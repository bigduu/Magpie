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
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tokio::sync::mpsc;

use magpie::bamboo::{BambooClient, BambooStream};
use magpie::bridge::{BambooApi, BambooEndpoint, ConnectBridge};
use magpie::config::{self, resolve_feishu_base_url, MagpieConfig, PlatformConfig};
use magpie::platform::{Inbound, Platform};
use magpie::platforms;

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

    if args.check {
        return run_check(&client).await;
    }

    if config.platforms.is_empty() {
        tracing::warn!("magpie: no platforms configured in magpie.json — nothing to do");
        return std::process::ExitCode::SUCCESS;
    }

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
}
