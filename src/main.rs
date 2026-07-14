//! Magpie (鹊) — standalone IM connector for Bamboo.
//!
//! Phase 1 (this crate's current state): repo scaffold + the entire
//! Bamboo-facing layer (`config`, `bamboo::client`, `bamboo::stream`,
//! `bamboo::types`). Phase 2 ports the platform adapters + bridge
//! (`bridge.rs`, `render.rs`, `approvals.rs`, `platform.rs`,
//! `platforms/telegram.rs`, `platforms/feishu/`) on top of the interfaces
//! defined here — see ARCHITECTURE.md's `src/` layout table.

use std::path::PathBuf;

use clap::Parser;

use magpie::bamboo::{BambooClient, BambooStream};
use magpie::config::{self, MagpieConfig};

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

    if config.platforms.is_empty() {
        tracing::warn!(
            "magpie: no platforms configured — nothing to do besides --check (phase 2 wires up \
             the platform adapters + bridge here)"
        );
    }

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

    // Construct (but do not yet drive) the WS stream client — phase 2's
    // bridge subscribes to `agent.{session_id}` channels through this handle
    // before issuing each `POST /execute/{id}` (see `bamboo::stream`'s module
    // doc for the subscribe-before-execute ordering contract).
    let _stream: BambooStream = BambooStream::connect(config.bamboo.clone());

    // TODO(phase 2): mount `platforms/` (telegram long-poll, feishu WS
    // long-connection) + `bridge::ConnectBridge` here, wiring each adapter's
    // inbound messages through the bridge onto `client`/`_stream`. See
    // ARCHITECTURE.md's "Layout" and "Key mappings" tables for the exact
    // interfaces this phase hands off.
    tracing::info!(
        "magpie: phase 1 scaffold — {} platform(s) configured, no adapters wired up yet",
        config.platforms.len()
    );

    std::process::ExitCode::SUCCESS
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
