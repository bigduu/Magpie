# Magpie (鹊)

Magpie is the standalone IM connector for [Bamboo](https://github.com/bigduu/Bamboo-agent). It
drives Bamboo agent sessions from IM platforms (Telegram, Feishu/Lark) exclusively over
Bamboo's public HTTP/WS API — never in-process internals — and ships as a Bamboo **service
plugin**: bamboo-server installs, spawns, supervises, and restarts it.

## Keep working from your chat app

Send a request from Telegram or Feishu/Lark to a running Bamboo agent and receive
its responses in the same conversation. Magpie is useful when you want an IM entry
point for an existing Bamboo setup; it does not replace the agent runtime or its
model-provider configuration. Restrict access with each platform's `allow_from` list.

You need a reachable Bamboo server, a paired Bamboo device credential, and a bot/app
credential for your chosen platform. Telegram uses polling and Feishu/Lark uses a
long-lived connection; this guide does not require publishing a webhook endpoint.

## Source versus releases

The inspected source is six commits ahead of tag `v0.1.1`, including fixes for
resuming pending questions after reconnects, queued messages, `/stop`, and waiting
for initial configuration. Do not assume those fixes are in the `v0.1.1` binaries.
See [audit notes](./docs/readme-audit.md) for exact revisions and verification limits.

Named after 鹊桥 (_què qiáo_, "magpie bridge") — the bridge of magpies that spans the Silver
River in the Qixi legend, connecting two separated worlds. Magpie spans the same gap between
a chat platform and a running Bamboo agent.

This repository is [Bamboo #477](https://github.com/bigduu/Bamboo-agent/issues/477)'s
extraction of bamboo-server's in-process `connect/` module into a standalone binary. See
[`ARCHITECTURE.md`](./ARCHITECTURE.md) for the full design, the Bamboo API surface Magpie
depends on, and the layout of `src/`.

## Install

[Magpie releases](https://github.com/bigduu/Magpie/releases/latest) publish two different
artifacts:

- `magpie-plugin-v<version>.tar.gz` is the Bamboo service-plugin bundle. With `bamboo serve`
  running, replace `<version>` with the latest numeric release version (without the leading
  `v`) and install it with:

  ```bash
  bamboo plugin install https://github.com/bigduu/Magpie/releases/download/v<version>/magpie-plugin-v<version>.tar.gz
  ```

  Bamboo verifies the signed release's `.sig` sidecar with the Magpie key in its default
  trust store, so no trust-bypass flags are needed. Bamboo then selects the platform binary
  and owns the service lifecycle. Put `magpie.json` at
  `<Bamboo data dir>/plugin_service_config/magpie/config.json` (normally
  `~/.bamboo/plugin_service_config/magpie/config.json`); Bamboo passes that path to Magpie.
- `magpie-v<version>-<target>.*` contains a standalone binary. Use it when another process
  manager owns Magpie, or build the same binary from source:

  ```bash
  git clone https://github.com/bigduu/Magpie.git
  cd Magpie
  cargo build --release
  ./target/release/magpie --config ./magpie.json --check   # smoke-test auth + connectivity
  ./target/release/magpie --config ./magpie.json            # run
  ```

`--check` calls `GET /api/v1/execute/defaults` against the configured Bamboo instance and
prints the resolved model, then exits — use it to confirm the device token and base URL are
correct before wiring up a platform. It does not validate Telegram/Feishu credentials,
message delivery, or a complete agent run.

## Config

Magpie reads `magpie.json` from (in priority order): the `--config` flag, then
`$BAMBOO_PLUGIN_SERVICE_CONFIG`, then `./magpie.json`. On Unix, a config file that is
group- or world-readable triggers a startup warning (it carries the bamboo device token and
platform bot secrets in plaintext; see `ARCHITECTURE.md`).

```json
{
  "bamboo": {
    "base_url": "http://127.0.0.1:9562",
    "device_id": "bamboo_abc123",
    "token": "bd1_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
  },
  "platforms": [
    { "type": "telegram", "token": "123456:ABC-DEF...", "allow_from": ["123456789"] },
    {
      "type": "feishu",
      "app_id": "cli_xxxxxxxx",
      "app_secret": "xxxxxxxxxxxxxxxx",
      "domain": "feishu",
      "allow_from": ["ou_xxxxxxxx"]
    }
  ]
}
```

`bamboo.device_id`/`bamboo.token` are a paired device credential minted by Bamboo (see
`POST /v2/pair` in the Bamboo server) — Magpie authenticates every request as that device.
An empty `allow_from` list denies every inbound message (logged as a startup warning), so a
freshly-configured platform entry never accidentally opens itself to the whole internet.

See [`ARCHITECTURE.md`](./ARCHITECTURE.md) for the full schema, the Bamboo API mapping table,
and the invariants carried over from bamboo's in-process `connect/` module.

For a private local config on Unix, run `chmod 600 magpie.json`. Protect the
config directory too: Magpie stores its conversation/session map beside the config.
Plugin builds target macOS, Linux, and Windows; this documentation review did not
exercise native platform behavior or send messages through real IM accounts.
