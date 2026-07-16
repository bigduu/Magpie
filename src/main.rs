//! Magpie (鹊) — standalone IM connector for Bamboo.
//!
//! Phase 2: wires phase 1's Bamboo-facing layer (`bamboo::client`,
//! `bamboo::stream`) together with the ported platform adapters + bridge
//! (`bridge::ConnectBridge`, `platforms::{telegram, feishu}`). Mirrors
//! bamboo's own `connect::ConnectManager::start` startup sequence — same
//! per-platform validation, the same "one live bot per platform type" guard
//! (`multi_bot_guard`), and the same `dispatch_loop` shape — just retargeted
//! from in-process construction to `bridge::BambooEndpoint` (HTTP + WS
//! client pair) as the bridge's dependency. See ARCHITECTURE.md's `src/`
//! layout table.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tokio::sync::mpsc;
use tokio::time::Instant;

use magpie::bamboo::{BambooClient, BambooStream};
use magpie::bridge::{BambooApi, BambooEndpoint, ConnectBridge};
use magpie::config::{self, resolve_feishu_base_url, MagpieConfig, PlatformConfig};
use magpie::platform::{Inbound, Platform};
use magpie::platforms;

/// How often to re-check the config file while idling because it is
/// missing, unparseable/invalid, or has an empty `platforms: []` (issue #4).
/// A "modest poll" per the issue — frequent enough that fixing the config
/// takes effect quickly, far below any rate that would look like the old
/// 1s supervisor-restart hot loop.
const CONFIG_POLL_INTERVAL: Duration = Duration::from_secs(30);

/// How rarely to repeat the idle-state log line while nothing has changed,
/// so an operator who ignores the plugin for a while still sees occasional
/// evidence it's alive and waiting, without spamming a line every 30s.
const IDLE_REMINDER_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// Magpie — the standalone IM connector for Bamboo.
#[derive(Debug, Parser)]
#[command(
    name = "magpie",
    version,
    about = "Standalone IM connector for Bamboo (鹊桥)"
)]
struct Args {
    /// Path to magpie.json. Overrides $BAMBOO_PLUGIN_SERVICE_CONFIG and the
    /// ./magpie.json default (see config::resolve_config_path).
    #[arg(long, value_name = "PATH")]
    config: Option<PathBuf>,

    /// Smoke-test mode: call `GET /api/v1/execute/defaults` against the
    /// configured Bamboo instance, print the resolved model, and exit. Use
    /// this to confirm the device token + base_url are correct before
    /// wiring up a platform.
    #[arg(long)]
    check: bool,
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config_path = config::resolve_config_path(args.config.as_deref());
    tracing::info!("magpie: loading config from {}", config_path.display());

    // `--check` is an interactive, operator-invoked diagnostic (not the
    // supervised service loop bamboo's ServiceManager restarts) — it should
    // fail fast with a clear error rather than idle waiting for a config
    // that the operator is actively trying to debug.
    if args.check {
        let config: MagpieConfig = match config::load_config(&config_path) {
            Ok(config) => config,
            Err(error) => {
                tracing::error!("magpie: failed to load config: {error}");
                return std::process::ExitCode::FAILURE;
            }
        };
        let client = match BambooClient::new(&config.bamboo) {
            Ok(client) => client,
            Err(error) => {
                tracing::error!("magpie: failed to construct the bamboo HTTP client: {error}");
                return std::process::ExitCode::FAILURE;
            }
        };
        return run_check(&client).await;
    }

    // Issue #4: a missing/unparseable/invalid config file, or a valid one
    // with `platforms: []`, is a NORMAL state for a freshly installed and
    // not-yet-configured plugin — not a crash. Under bamboo's ServiceManager
    // (unlimited restart attempts), exiting here used to respawn every ~1s
    // forever. Idle-and-poll instead: `await_valid_config` returns as soon
    // as the config becomes valid AND non-empty (races against the shutdown
    // signal so SIGTERM/Ctrl-C still works immediately while idling).
    let config: MagpieConfig = tokio::select! {
        config = await_valid_config(&config_path, CONFIG_POLL_INTERVAL) => config,
        _ = shutdown_signal() => {
            tracing::info!("magpie: shutdown signal received while awaiting configuration");
            return std::process::ExitCode::SUCCESS;
        }
    };
    tracing::info!(
        "magpie: configuration ready — {} platform(s) configured",
        config.platforms.len()
    );

    let client = match BambooClient::new(&config.bamboo) {
        Ok(client) => client,
        Err(error) => {
            tracing::error!("magpie: failed to construct the bamboo HTTP client: {error}");
            return std::process::ExitCode::FAILURE;
        }
    };

    let stream = BambooStream::connect(config.bamboo.clone());
    let api: Arc<dyn BambooApi> = Arc::new(BambooEndpoint::new(client, stream));

    // Session map lives BESIDE the config file, per ARCHITECTURE.md ("Config
    // liveness" section) — `with_file_name` swaps only the final path
    // component, so `--config /etc/magpie/magpie.json` persists to
    // `/etc/magpie/magpie_sessions.json`.
    let map_path = config_path.with_file_name("magpie_sessions.json");
    let bridge = Arc::new(ConnectBridge::new(api, Some(map_path)));
    bridge.load_session_map().await;

    let mut tasks = Vec::new();
    let mut platforms_by_name: HashMap<String, Arc<dyn Platform>> = HashMap::new();
    let start_ok = multi_bot_guard(&config.platforms);

    for (index, platform_cfg) in config.platforms.iter().enumerate() {
        match platform_cfg {
            PlatformConfig::Telegram { token, allow_from } => {
                let token = token.clone().unwrap_or_default();
                if token.trim().is_empty() {
                    tracing::warn!(
                        "magpie: telegram platform configured without a token; skipping"
                    );
                    continue;
                }
                // Issue-#454-style guard (ported from bamboo's
                // ConnectManager::start): SessionKey has no per-bot
                // dimension, so two live telegram bots would collide on
                // `telegram:<chat_id>:<user_id>` — start only the first.
                if !start_ok[index] {
                    tracing::warn!(
                        "magpie: multiple telegram platform entries are configured; only the \
                         FIRST is started (a second bot would collide with the first on the \
                         same session-routing key). Remove the extra entry."
                    );
                    continue;
                }
                if allow_from.is_empty() {
                    tracing::warn!(
                        "magpie: telegram platform has an EMPTY allow_from list — every inbound \
                         message will be denied until you add allowed user ids"
                    );
                }

                let platform: Arc<dyn Platform> =
                    Arc::new(platforms::telegram::TelegramPlatform::new(token));
                platforms_by_name.insert("telegram".to_string(), platform.clone());
                spawn_platform_tasks(&mut tasks, &bridge, platform, allow_from.clone());
            }
            PlatformConfig::Feishu {
                app_id,
                app_secret,
                domain,
                allow_from,
            } => {
                let app_id = app_id.clone().unwrap_or_default();
                let app_secret = app_secret.clone().unwrap_or_default();
                if app_id.trim().is_empty() || app_secret.trim().is_empty() {
                    tracing::warn!(
                        "magpie: feishu platform configured without app_id/app_secret; skipping"
                    );
                    continue;
                }
                if !start_ok[index] {
                    tracing::warn!(
                        "magpie: multiple feishu platform entries are configured; only the \
                         FIRST is started (a second app would collide with the first on the \
                         same session-routing key and on inbound message dedup). Remove the \
                         extra entry."
                    );
                    continue;
                }
                let Some(base_url) = resolve_feishu_base_url(domain.as_deref()) else {
                    tracing::warn!(
                        domain = domain.as_deref().unwrap_or_default(),
                        "magpie: feishu platform has an invalid domain (expected \"feishu\", \
                         \"lark\", or an https:// base URL); skipping"
                    );
                    continue;
                };
                if allow_from.is_empty() {
                    tracing::warn!(
                        "magpie: feishu platform has an EMPTY allow_from list — every inbound \
                         message will be denied until you add allowed open ids"
                    );
                }

                let platform: Arc<dyn Platform> = Arc::new(platforms::feishu::FeishuPlatform::new(
                    app_id, app_secret, base_url,
                ));
                platforms_by_name.insert("feishu".to_string(), platform.clone());
                spawn_platform_tasks(&mut tasks, &bridge, platform, allow_from.clone());
            }
        }
    }

    if tasks.is_empty() {
        tracing::warn!("magpie: no platform adapter was started; exiting");
        return std::process::ExitCode::SUCCESS;
    }

    // Ask resync (ARCHITECTURE.md: "on startup ... optionally
    // client.respond_pending(session_id) to re-park a lost ask") — recovers
    // bookkeeping for any question left pending across a magpie restart.
    // Best-effort: never blocks startup on a slow/unreachable bamboo.
    bridge.resync_pending_asks(&platforms_by_name).await;

    tracing::info!("magpie: {} platform adapter(s) running", tasks.len());
    tokio::select! {
        _ = futures_util::future::join_all(tasks) => {
            tracing::warn!("magpie: every platform adapter task exited; shutting down");
        }
        _ = shutdown_signal() => {
            tracing::info!("magpie: shutdown signal received");
        }
    }

    // Magpie issue #12: `ChatState` (parked asks + queued messages) is
    // in-memory only and lost on exit — log anything still outstanding so
    // an operator sees it instead of a message silently vanishing.
    bridge.log_backlog_on_shutdown().await;

    std::process::ExitCode::SUCCESS
}

/// Waits for a graceful-shutdown request: Ctrl-C on every platform, plus
/// SIGTERM on unix (the signal bamboo-plugin's `graceful_shutdown` sends —
/// see `plugin/plugin.json`'s `services[].graceful_shutdown`).
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut sigterm =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(signal) => signal,
                Err(error) => {
                    tracing::warn!("magpie: failed to install SIGTERM handler: {error}");
                    // Fall back to Ctrl-C only.
                    let _ = tokio::signal::ctrl_c().await;
                    return;
                }
            };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Outcome of a single config-file check: either it's ready to run with
/// ([`ConfigState::Ready`]), or startup should idle for [`IdleReason`].
#[derive(Debug)]
enum ConfigState {
    Ready(MagpieConfig),
    Idle(IdleReason),
}

/// Why startup is idling instead of running — covers both halves of issue
/// #4 (missing/unparseable/invalid config file, and a valid config with no
/// platforms configured).
#[derive(Debug, Clone, PartialEq, Eq)]
enum IdleReason {
    /// Covers a missing file, a parse error, AND a failed `validate_config`
    /// (bad/empty credentials, invalid feishu domain, ...) — all of these
    /// are equally "not ready yet, try again", not a crash.
    ConfigNotReady(String),
    NoPlatformsConfigured,
}

impl std::fmt::Display for IdleReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IdleReason::ConfigNotReady(message) => write!(f, "config not ready: {message}"),
            IdleReason::NoPlatformsConfigured => {
                write!(
                    f,
                    "no platforms configured (magpie.json has \"platforms\": [])"
                )
            }
        }
    }
}

/// Reads + validates the config file at `path` once and classifies the
/// result. Pure sync wrapper around [`config::load_config`] so the
/// missing-file / parse-error / empty-platforms cases can be unit-tested
/// without touching the async idle loop below.
fn classify_config(path: &Path) -> ConfigState {
    match config::load_config(path) {
        Ok(config) if config.platforms.is_empty() => {
            ConfigState::Idle(IdleReason::NoPlatformsConfigured)
        }
        Ok(config) => ConfigState::Ready(config),
        Err(error) => ConfigState::Idle(IdleReason::ConfigNotReady(error.to_string())),
    }
}

/// Polls `path` at `poll_interval` until it holds a valid, non-empty
/// config, then returns it. This is issue #4's fix: previously magpie
/// exited (FAILURE on missing/invalid config, SUCCESS on empty
/// `platforms`) and relied on bamboo's ServiceManager to restart it —
/// which it does every ~1s (`max_attempts: 0` = unlimited), producing a
/// hot loop for a plugin that is simply not configured yet.
///
/// Never busy-spins: each iteration does one synchronous file read/parse
/// then sleeps for the full `poll_interval` (this also doubles as the
/// "hot-reload" path the issue asks to preserve — editing the config file
/// while idle is picked up on the next poll, same as an operator editing it
/// between the old restart-loop's exits). Logs once when idling begins (or
/// when the failure reason changes) and then at most once every
/// [`IDLE_REMINDER_INTERVAL`] while it persists, so a long-idle plugin
/// doesn't spam the log. Callers race this against [`shutdown_signal`] so
/// SIGTERM/Ctrl-C interrupts a sleeping poll immediately.
async fn await_valid_config(path: &Path, poll_interval: Duration) -> MagpieConfig {
    let mut last_reason: Option<IdleReason> = None;
    let mut last_logged_at: Option<Instant> = None;

    loop {
        match classify_config(path) {
            ConfigState::Ready(config) => return config,
            ConfigState::Idle(reason) => {
                let reason_changed = last_reason.as_ref() != Some(&reason);
                let reminder_due = last_logged_at
                    .map(|at| at.elapsed() >= IDLE_REMINDER_INTERVAL)
                    .unwrap_or(true);
                if reason_changed || reminder_due {
                    tracing::warn!(
                        "magpie: idling — {reason} (this is normal for a freshly installed, \
                         not-yet-configured plugin; re-checking {} every {}s)",
                        path.display(),
                        poll_interval.as_secs()
                    );
                    last_logged_at = Some(Instant::now());
                }
                last_reason = Some(reason);
            }
        }
        tokio::time::sleep(poll_interval).await;
    }
}

/// Spawns the pair of background tasks every platform entry needs — the
/// adapter's own `start()` loop and its [`dispatch_loop`] — and logs the
/// startup. Ported from bamboo's `connect::spawn_platform_tasks`.
fn spawn_platform_tasks(
    tasks: &mut Vec<tokio::task::JoinHandle<()>>,
    bridge: &Arc<ConnectBridge>,
    platform: Arc<dyn Platform>,
    allow_from: Vec<String>,
) {
    let name = platform.name().to_string();
    let (tx, rx) = mpsc::channel(64);

    let platform_for_start = platform.clone();
    let name_for_start = name.clone();
    tasks.push(tokio::spawn(async move {
        if let Err(error) = platform_for_start.start(tx).await {
            tracing::warn!("magpie: {name_for_start} platform loop exited: {error}");
        }
    }));

    tasks.push(tokio::spawn(dispatch_loop(
        bridge.clone(),
        platform,
        allow_from,
        rx,
    )));

    tracing::info!("magpie: started {name} platform");
}

/// Pulls inbound events (messages and button-press callbacks) off a single
/// platform's channel and hands each to the bridge. Kept as its OWN task per
/// platform (not merged into the platform's `start()` loop) so a slow/queued
/// chat can never stall the next poll/frame-read — `ConnectBridge::
/// handle_inbound`/`handle_callback` themselves return quickly (a message
/// spawns the actual run; a callback only ever acks+resolves). Ported from
/// bamboo's `connect::dispatch_loop`.
async fn dispatch_loop(
    bridge: Arc<ConnectBridge>,
    platform: Arc<dyn Platform>,
    allow_from: Vec<String>,
    mut rx: mpsc::Receiver<Inbound>,
) {
    while let Some(event) = rx.recv().await {
        match event {
            Inbound::Message(msg) => {
                ConnectBridge::handle_inbound(
                    bridge.clone(),
                    platform.clone(),
                    allow_from.clone(),
                    msg,
                )
                .await;
            }
            Inbound::Callback(callback) => {
                ConnectBridge::handle_callback(
                    bridge.clone(),
                    platform.clone(),
                    allow_from.clone(),
                    callback,
                )
                .await;
            }
        }
    }
}

/// For each entry in `platforms` (same order/length as the input), whether
/// [`main`] is allowed to start it as far as the "at most one live bot PER
/// PLATFORM TYPE" guard is concerned (see `spawn_platform_tasks`'s doc: two
/// live entries of the same type would collide on the same session-routing
/// key). Ported verbatim (semantics-wise) from bamboo's
/// `connect::multi_bot_guard`, retargeted to magpie's `PlatformConfig` enum.
fn multi_bot_guard(platforms: &[PlatformConfig]) -> Vec<bool> {
    let mut seen_valid: std::collections::HashSet<&'static str> = std::collections::HashSet::new();
    platforms
        .iter()
        .map(|platform_cfg| {
            let (type_name, valid) = match platform_cfg {
                PlatformConfig::Telegram { token, .. } => (
                    "telegram",
                    token.as_deref().is_some_and(|t| !t.trim().is_empty()),
                ),
                PlatformConfig::Feishu {
                    app_id, app_secret, ..
                } => (
                    "feishu",
                    app_id.as_deref().is_some_and(|v| !v.trim().is_empty())
                        && app_secret.as_deref().is_some_and(|v| !v.trim().is_empty()),
                ),
            };
            if !valid {
                return true;
            }
            seen_valid.insert(type_name)
        })
        .collect()
}

async fn run_check(client: &BambooClient) -> std::process::ExitCode {
    match client.execute_defaults().await {
        Ok(defaults) => {
            println!(
                "bamboo reachable — resolved model: {}",
                defaults.model.as_deref().unwrap_or("<none configured>")
            );
            if let Some(provider) = &defaults.provider {
                println!("provider: {provider}");
            }
            if let Some(fast) = &defaults.fast_model {
                println!("fast_model: {fast}");
            }
            std::process::ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("magpie --check failed: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn platform(platform_type: &str, token: Option<&str>) -> PlatformConfig {
        match platform_type {
            "telegram" => PlatformConfig::Telegram {
                token: token.map(str::to_string),
                allow_from: Vec::new(),
            },
            "feishu" => PlatformConfig::Feishu {
                app_id: token.map(str::to_string),
                app_secret: token.map(str::to_string),
                domain: None,
                allow_from: Vec::new(),
            },
            other => panic!("unexpected platform type in test helper: {other}"),
        }
    }

    fn feishu_platform(app_id: Option<&str>, app_secret: Option<&str>) -> PlatformConfig {
        PlatformConfig::Feishu {
            app_id: app_id.map(str::to_string),
            app_secret: app_secret.map(str::to_string),
            domain: None,
            allow_from: Vec::new(),
        }
    }

    #[test]
    fn multi_bot_guard_allows_a_single_telegram_entry() {
        let platforms = vec![platform("telegram", Some("tok-1"))];
        assert_eq!(multi_bot_guard(&platforms), vec![true]);
    }

    #[test]
    fn multi_bot_guard_rejects_every_telegram_entry_after_the_first() {
        let platforms = vec![
            platform("telegram", Some("tok-1")),
            platform("telegram", Some("tok-2")),
            platform("telegram", Some("tok-3")),
        ];
        assert_eq!(multi_bot_guard(&platforms), vec![true, false, false]);
    }

    #[test]
    fn multi_bot_guard_is_scoped_per_platform_type() {
        let platforms = vec![
            platform("telegram", Some("tok-1")),
            feishu_platform(Some("cli_a"), Some("secret-a")),
            platform("telegram", Some("tok-2")),
            feishu_platform(Some("cli_b"), Some("secret-b")),
        ];
        assert_eq!(multi_bot_guard(&platforms), vec![true, true, false, false]);
    }

    #[test]
    fn multi_bot_guard_does_not_count_a_credentialless_entry_against_the_budget() {
        let platforms = vec![
            platform("telegram", None),
            platform("telegram", Some("")),
            platform("telegram", Some("tok-real")),
            feishu_platform(Some("cli_a"), None),
            feishu_platform(Some("cli_b"), Some("secret-real")),
        ];
        assert_eq!(
            multi_bot_guard(&platforms),
            vec![true, true, true, true, true]
        );
    }

    #[test]
    fn multi_bot_guard_handles_an_empty_platform_list() {
        assert_eq!(multi_bot_guard(&[]), Vec::<bool>::new());
    }

    // ---- issue #4: idle-and-wait instead of exit when unconfigured ----

    fn valid_bamboo_only_json() -> String {
        serde_json::json!({
            "bamboo": {
                "base_url": "http://127.0.0.1:9560",
                "device_id": "bamboo_abc123",
                "token": "bd1_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
            }
        })
        .to_string()
    }

    fn valid_config_with_one_platform_json() -> String {
        serde_json::json!({
            "bamboo": {
                "base_url": "http://127.0.0.1:9560",
                "device_id": "bamboo_abc123",
                "token": "bd1_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
            },
            "platforms": [
                { "type": "telegram", "token": "123:abc", "allow_from": ["1"] }
            ]
        })
        .to_string()
    }

    #[test]
    fn classify_config_missing_file_is_idle_not_ready() {
        let path = Path::new("/nonexistent/definitely/not/here/magpie.json");
        match classify_config(path) {
            ConfigState::Idle(IdleReason::ConfigNotReady(message)) => {
                assert!(message.contains("failed to read"), "message: {message}");
            }
            other => panic!("expected Idle(ConfigNotReady), got {other:?}"),
        }
    }

    #[test]
    fn classify_config_parse_error_is_idle_not_ready() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"not json").unwrap();
        match classify_config(file.path()) {
            ConfigState::Idle(IdleReason::ConfigNotReady(_)) => {}
            other => panic!("expected Idle(ConfigNotReady), got {other:?}"),
        }
    }

    #[test]
    fn classify_config_validation_error_is_idle_not_ready() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        // platforms entry fails validate_config (telegram missing token) —
        // this is still an "idle and retry" case, not a crash.
        let json = serde_json::json!({
            "bamboo": {
                "base_url": "http://127.0.0.1:9560",
                "device_id": "bamboo_abc123",
                "token": "bd1_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
            },
            "platforms": [{ "type": "telegram", "allow_from": [] }]
        });
        file.write_all(json.to_string().as_bytes()).unwrap();
        match classify_config(file.path()) {
            ConfigState::Idle(IdleReason::ConfigNotReady(message)) => {
                assert!(message.contains("telegram"), "message: {message}");
            }
            other => panic!("expected Idle(ConfigNotReady), got {other:?}"),
        }
    }

    #[test]
    fn classify_config_empty_platforms_is_idle_no_platforms_configured() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(valid_bamboo_only_json().as_bytes()).unwrap();
        match classify_config(file.path()) {
            ConfigState::Idle(IdleReason::NoPlatformsConfigured) => {}
            other => panic!("expected Idle(NoPlatformsConfigured), got {other:?}"),
        }
    }

    #[test]
    fn classify_config_ready_when_a_platform_is_configured() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(valid_config_with_one_platform_json().as_bytes())
            .unwrap();
        match classify_config(file.path()) {
            ConfigState::Ready(config) => assert_eq!(config.platforms.len(), 1),
            other => panic!("expected Ready, got {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn await_valid_config_does_not_busy_spin_and_picks_up_a_later_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("magpie.json");
        let poll_interval = Duration::from_secs(30);

        let wait = await_valid_config(&path, poll_interval);
        tokio::pin!(wait);

        // The first check (config missing) happens immediately without
        // sleeping first; it must still be pending afterward.
        assert!(futures_util::poll!(&mut wait).is_pending());

        // Advancing LESS than a full poll interval must not trigger another
        // check — if it did, writing the config now would be picked up
        // immediately, which is exactly the busy-spin this test rules out.
        std::fs::write(&path, valid_config_with_one_platform_json()).unwrap();
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(
            futures_util::poll!(&mut wait).is_pending(),
            "await_valid_config must sleep for the full poll_interval between checks"
        );

        // Advancing past the poll interval boundary lets the next check
        // observe the config that's been sitting there since the write
        // above.
        tokio::time::advance(poll_interval).await;
        match futures_util::poll!(&mut wait) {
            std::task::Poll::Ready(config) => assert_eq!(config.platforms.len(), 1),
            std::task::Poll::Pending => {
                panic!("expected await_valid_config to resolve once a valid config appears")
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn await_valid_config_resolves_immediately_when_already_valid() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("magpie.json");
        std::fs::write(&path, valid_config_with_one_platform_json()).unwrap();

        let config = tokio::time::timeout(
            Duration::from_secs(1),
            await_valid_config(&path, Duration::from_secs(30)),
        )
        .await
        .expect("must not need to wait for an already-valid config");
        assert_eq!(config.platforms.len(), 1);
    }
}
