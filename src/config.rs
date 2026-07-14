//! `magpie.json` schema + load/validate.
//!
//! Load order (first hit wins): `--config <path>`, then
//! `$BAMBOO_PLUGIN_SERVICE_CONFIG`, then `./magpie.json` — mirrors how a
//! bamboo-plugin `services` artifact is handed its config path (bamboo #479)
//! while still being usable standalone via `--config`.
//!
//! Validation mirrors bamboo's own `connect::mod::ConnectManager::start` match
//! arms (see `bamboo/.claude/worktrees/magpie-ref/crates/app/bamboo-server/src/connect/mod.rs`):
//! telegram needs a non-empty `token`; feishu needs non-empty `app_id` +
//! `app_secret` plus a resolvable `domain`. An empty `allow_from` is valid but
//! denies every inbound message, so it is a startup *warning*, not an error
//! (same as bamboo's connect module).

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// The `$BAMBOO_PLUGIN_SERVICE_CONFIG` env var a bamboo-plugin `services`
/// artifact is launched with (bamboo #479) — the second-priority config
/// location, below `--config` and above `./magpie.json`.
pub const BAMBOO_PLUGIN_SERVICE_CONFIG_ENV: &str = "BAMBOO_PLUGIN_SERVICE_CONFIG";

/// Default config path when neither `--config` nor the env var is set.
pub const DEFAULT_CONFIG_FILENAME: &str = "magpie.json";

/// Top-level `magpie.json` schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MagpieConfig {
    pub bamboo: BambooConfig,
    #[serde(default)]
    pub platforms: Vec<PlatformConfig>,
}

/// The `bamboo` block: endpoint + device-token credential (see bamboo's
/// `POST /v2/pair` — device tokens are the v2-P2 per-device auth scheme,
/// `crates/app/bamboo-server/src/handlers/settings/access_control.rs`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BambooConfig {
    pub base_url: String,
    pub device_id: String,
    pub token: String,
}

/// One `platforms[]` entry, tagged by `"type"`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PlatformConfig {
    Telegram {
        #[serde(default)]
        token: Option<String>,
        #[serde(default)]
        allow_from: Vec<String>,
    },
    Feishu {
        #[serde(default)]
        app_id: Option<String>,
        #[serde(default)]
        app_secret: Option<String>,
        #[serde(default)]
        domain: Option<String>,
        #[serde(default)]
        allow_from: Vec<String>,
    },
}

impl PlatformConfig {
    pub fn type_name(&self) -> &'static str {
        match self {
            PlatformConfig::Telegram { .. } => "telegram",
            PlatformConfig::Feishu { .. } => "feishu",
        }
    }

    fn allow_from(&self) -> &[String] {
        match self {
            PlatformConfig::Telegram { allow_from, .. } => allow_from,
            PlatformConfig::Feishu { allow_from, .. } => allow_from,
        }
    }
}

/// Everything that can go wrong loading/validating `magpie.json`.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config file {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse config file {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("bamboo.base_url must not be empty")]
    EmptyBaseUrl,
    #[error("bamboo.device_id must not be empty")]
    EmptyDeviceId,
    #[error("bamboo.token must not be empty")]
    EmptyToken,
    #[error("platforms[{index}] (telegram) requires a non-empty token")]
    TelegramMissingToken { index: usize },
    #[error("platforms[{index}] (feishu) requires non-empty app_id and app_secret")]
    FeishuMissingCredentials { index: usize },
    #[error(
        "platforms[{index}] (feishu) has an invalid domain {domain:?} (expected \"feishu\", \"lark\", or an https:// base URL)"
    )]
    FeishuInvalidDomain { index: usize, domain: String },
}

/// Resolve the config path per the documented priority: `--config` flag >
/// `$BAMBOO_PLUGIN_SERVICE_CONFIG` > `./magpie.json`.
pub fn resolve_config_path(cli_flag: Option<&Path>) -> PathBuf {
    if let Some(path) = cli_flag {
        return path.to_path_buf();
    }
    if let Ok(env_path) = std::env::var(BAMBOO_PLUGIN_SERVICE_CONFIG_ENV) {
        if !env_path.trim().is_empty() {
            return PathBuf::from(env_path);
        }
    }
    PathBuf::from(DEFAULT_CONFIG_FILENAME)
}

/// Load + validate `magpie.json` from `path`. Warns (via `tracing::warn`) on
/// unix if the file is group/world readable, and for every platform entry
/// with an empty `allow_from` (deny-all is valid but almost certainly not
/// what the operator wants).
pub fn load_config(path: &Path) -> Result<MagpieConfig, ConfigError> {
    let raw = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
        path: path.to_path_buf(),
        source,
    })?;

    warn_if_insecure_perms(path);

    let config: MagpieConfig = serde_json::from_str(&raw).map_err(|source| ConfigError::Parse {
        path: path.to_path_buf(),
        source,
    })?;

    validate_config(&config)?;
    warn_empty_allow_from(&config);

    Ok(config)
}

/// Validate a parsed config, independent of where it came from (used directly
/// by tests without touching the filesystem).
pub fn validate_config(config: &MagpieConfig) -> Result<(), ConfigError> {
    if config.bamboo.base_url.trim().is_empty() {
        return Err(ConfigError::EmptyBaseUrl);
    }
    if config.bamboo.device_id.trim().is_empty() {
        return Err(ConfigError::EmptyDeviceId);
    }
    if config.bamboo.token.trim().is_empty() {
        return Err(ConfigError::EmptyToken);
    }

    for (index, platform) in config.platforms.iter().enumerate() {
        match platform {
            PlatformConfig::Telegram { token, .. } => {
                let non_empty = token.as_deref().is_some_and(|t| !t.trim().is_empty());
                if !non_empty {
                    return Err(ConfigError::TelegramMissingToken { index });
                }
            }
            PlatformConfig::Feishu {
                app_id,
                app_secret,
                domain,
                ..
            } => {
                let id_ok = app_id.as_deref().is_some_and(|v| !v.trim().is_empty());
                let secret_ok = app_secret.as_deref().is_some_and(|v| !v.trim().is_empty());
                if !id_ok || !secret_ok {
                    return Err(ConfigError::FeishuMissingCredentials { index });
                }
                if resolve_feishu_base_url(domain.as_deref()).is_none() {
                    return Err(ConfigError::FeishuInvalidDomain {
                        index,
                        domain: domain.clone().unwrap_or_default(),
                    });
                }
            }
        }
    }

    Ok(())
}

fn warn_empty_allow_from(config: &MagpieConfig) {
    for (index, platform) in config.platforms.iter().enumerate() {
        if platform.allow_from().is_empty() {
            tracing::warn!(
                platform = platform.type_name(),
                index,
                "magpie: platforms[{index}] ({}) has an EMPTY allow_from list — every inbound \
                 message will be denied until you add allowed user/open ids",
                platform.type_name()
            );
        }
    }
}

/// Resolves the `domain` field of a feishu platform entry to an API base URL.
///
/// Ported verbatim from bamboo's `connect::resolve_feishu_base_url`
/// (`crates/app/bamboo-server/src/connect/mod.rs`): absent/`"feishu"` →
/// open.feishu.cn, `"lark"` → open.larksuite.com (Lark international), any
/// `https://` value → private-deployment base used as-is (trailing slash
/// trimmed). Anything else (including a plain `http://` URL) is invalid.
pub fn resolve_feishu_base_url(domain: Option<&str>) -> Option<String> {
    match domain.map(str::trim).filter(|d| !d.is_empty()) {
        None | Some("feishu") => Some("https://open.feishu.cn".to_string()),
        Some("lark") => Some("https://open.larksuite.com".to_string()),
        Some(custom) if custom.starts_with("https://") => {
            Some(custom.trim_end_matches('/').to_string())
        }
        Some(_) => None,
    }
}

#[cfg(unix)]
fn warn_if_insecure_perms(path: &Path) {
    use std::os::unix::fs::PermissionsExt;

    let metadata = match std::fs::metadata(path) {
        Ok(m) => m,
        Err(error) => {
            tracing::debug!("magpie: could not stat config file {path:?} for perm check: {error}");
            return;
        }
    };
    let mode = metadata.permissions().mode();
    // Group or world read/write/execute bits set (mode & 0o077 != 0): the file
    // carries the bamboo device token + platform bot secrets in plaintext
    // (see ARCHITECTURE.md's "Config" section — v1 keeps secrets plaintext).
    if mode & 0o077 != 0 {
        tracing::warn!(
            "magpie: config file {path:?} is group/world readable (mode {mode:o}) — it \
             contains the bamboo device token and platform bot secrets in plaintext; run \
             `chmod 600 {path:?}`"
        );
    }
}

#[cfg(not(unix))]
fn warn_if_insecure_perms(_path: &Path) {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn bamboo_block() -> serde_json::Value {
        serde_json::json!({
            "base_url": "http://127.0.0.1:9560",
            "device_id": "bamboo_abc123",
            "token": "bd1_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
        })
    }

    #[test]
    fn parses_minimal_config_with_no_platforms() {
        let json = serde_json::json!({ "bamboo": bamboo_block() });
        let config: MagpieConfig = serde_json::from_value(json).unwrap();
        assert!(config.platforms.is_empty());
        assert_eq!(config.bamboo.base_url, "http://127.0.0.1:9560");
        validate_config(&config).unwrap();
    }

    #[test]
    fn parses_telegram_and_feishu_platforms() {
        let json = serde_json::json!({
            "bamboo": bamboo_block(),
            "platforms": [
                { "type": "telegram", "token": "123:abc", "allow_from": ["1"] },
                {
                    "type": "feishu",
                    "app_id": "cli_x",
                    "app_secret": "secret",
                    "domain": "lark",
                    "allow_from": ["ou_1"]
                }
            ]
        });
        let config: MagpieConfig = serde_json::from_value(json).unwrap();
        assert_eq!(config.platforms.len(), 2);
        assert_eq!(config.platforms[0].type_name(), "telegram");
        assert_eq!(config.platforms[1].type_name(), "feishu");
        validate_config(&config).unwrap();
    }

    #[test]
    fn rejects_empty_bamboo_fields() {
        let mut base = bamboo_block();
        base["base_url"] = serde_json::json!("");
        let json = serde_json::json!({ "bamboo": base });
        let config: MagpieConfig = serde_json::from_value(json).unwrap();
        assert!(matches!(
            validate_config(&config),
            Err(ConfigError::EmptyBaseUrl)
        ));
    }

    #[test]
    fn rejects_telegram_without_token() {
        let json = serde_json::json!({
            "bamboo": bamboo_block(),
            "platforms": [{ "type": "telegram", "allow_from": [] }]
        });
        let config: MagpieConfig = serde_json::from_value(json).unwrap();
        assert!(matches!(
            validate_config(&config),
            Err(ConfigError::TelegramMissingToken { index: 0 })
        ));
    }

    #[test]
    fn rejects_telegram_with_blank_token() {
        let json = serde_json::json!({
            "bamboo": bamboo_block(),
            "platforms": [{ "type": "telegram", "token": "   ", "allow_from": [] }]
        });
        let config: MagpieConfig = serde_json::from_value(json).unwrap();
        assert!(matches!(
            validate_config(&config),
            Err(ConfigError::TelegramMissingToken { index: 0 })
        ));
    }

    #[test]
    fn rejects_feishu_missing_credentials() {
        let json = serde_json::json!({
            "bamboo": bamboo_block(),
            "platforms": [{ "type": "feishu", "app_id": "cli_x", "allow_from": [] }]
        });
        let config: MagpieConfig = serde_json::from_value(json).unwrap();
        assert!(matches!(
            validate_config(&config),
            Err(ConfigError::FeishuMissingCredentials { index: 0 })
        ));
    }

    #[test]
    fn rejects_feishu_invalid_domain() {
        let json = serde_json::json!({
            "bamboo": bamboo_block(),
            "platforms": [{
                "type": "feishu",
                "app_id": "cli_x",
                "app_secret": "secret",
                "domain": "dingtalk",
                "allow_from": []
            }]
        });
        let config: MagpieConfig = serde_json::from_value(json).unwrap();
        assert!(matches!(
            validate_config(&config),
            Err(ConfigError::FeishuInvalidDomain { index: 0, .. })
        ));
    }

    // ---- resolve_feishu_base_url: ported semantics, pinned 1:1 with bamboo's
    // own test (`connect::mod::tests::resolve_feishu_base_url_covers_the_three_domain_forms`) ----

    #[test]
    fn resolve_feishu_base_url_covers_the_three_domain_forms() {
        assert_eq!(
            resolve_feishu_base_url(None).as_deref(),
            Some("https://open.feishu.cn")
        );
        assert_eq!(
            resolve_feishu_base_url(Some("feishu")).as_deref(),
            Some("https://open.feishu.cn")
        );
        assert_eq!(
            resolve_feishu_base_url(Some("")).as_deref(),
            Some("https://open.feishu.cn")
        );
        assert_eq!(
            resolve_feishu_base_url(Some("lark")).as_deref(),
            Some("https://open.larksuite.com")
        );
        assert_eq!(
            resolve_feishu_base_url(Some("https://feishu.example.corp/")).as_deref(),
            Some("https://feishu.example.corp")
        );
        assert_eq!(
            resolve_feishu_base_url(Some("http://insecure.example")),
            None
        );
        assert_eq!(resolve_feishu_base_url(Some("dingtalk")), None);
    }

    // ---- config path resolution priority ----

    #[test]
    fn resolve_config_path_prefers_cli_flag() {
        let path = resolve_config_path(Some(Path::new("/tmp/explicit.json")));
        assert_eq!(path, PathBuf::from("/tmp/explicit.json"));
    }

    #[test]
    fn resolve_config_path_falls_back_to_default_filename() {
        // Ensure the env var isn't set from a leaked prior test/process env.
        std::env::remove_var(BAMBOO_PLUGIN_SERVICE_CONFIG_ENV);
        let path = resolve_config_path(None);
        assert_eq!(path, PathBuf::from(DEFAULT_CONFIG_FILENAME));
    }

    // ---- load_config end-to-end over a real temp file ----

    #[test]
    fn load_config_reads_and_validates_a_real_file() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        let json = serde_json::json!({ "bamboo": bamboo_block() });
        file.write_all(serde_json::to_string(&json).unwrap().as_bytes())
            .unwrap();
        let config = load_config(file.path()).unwrap();
        assert_eq!(config.bamboo.device_id, "bamboo_abc123");
    }

    #[test]
    fn load_config_surfaces_a_read_error_for_a_missing_file() {
        let error = load_config(Path::new("/nonexistent/path/magpie.json")).unwrap_err();
        assert!(matches!(error, ConfigError::Read { .. }));
    }

    #[test]
    fn load_config_surfaces_a_parse_error_for_invalid_json() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"not json").unwrap();
        let error = load_config(file.path()).unwrap_err();
        assert!(matches!(error, ConfigError::Parse { .. }));
    }

    #[cfg(unix)]
    #[test]
    fn warn_if_insecure_perms_does_not_panic_on_world_readable_file() {
        use std::os::unix::fs::PermissionsExt;
        let mut file = tempfile::NamedTempFile::new().unwrap();
        let json = serde_json::json!({ "bamboo": bamboo_block() });
        file.write_all(serde_json::to_string(&json).unwrap().as_bytes())
            .unwrap();
        std::fs::set_permissions(file.path(), std::fs::Permissions::from_mode(0o644)).unwrap();
        // Just exercising the code path; the warning goes to tracing, not a
        // return value, so this test's job is "does not panic / still loads".
        let config = load_config(file.path()).unwrap();
        assert_eq!(config.bamboo.device_id, "bamboo_abc123");
    }
}
