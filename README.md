# Magpie (鹊)

Magpie is the standalone IM connector for [Bamboo](https://github.com/bigduu/bamboo). It
drives Bamboo agent sessions from IM platforms (Telegram, Feishu/Lark) exclusively over
Bamboo's public HTTP/WS API — never in-process internals — and ships as a Bamboo **service
plugin**: bamboo-server installs, spawns, supervises, and restarts it.

Named after 鹊桥 (_què qiáo_, "magpie bridge") — the bridge of magpies that spans the Silver
River in the Qixi legend, connecting two separated worlds. Magpie spans the same gap between
a chat platform and a running Bamboo agent.

This repository is bamboo epic #477's extraction of bamboo-server's in-process `connect/`
module into a standalone binary. See [`ARCHITECTURE.md`](./ARCHITECTURE.md) for the full
design, the Bamboo API surface Magpie depends on, and the layout of `src/`.

## Quickstart

```bash
cargo build --release
./target/release/magpie --config ./magpie.json --check   # smoke-test auth + connectivity
./target/release/magpie --config ./magpie.json            # run
```

`--check` calls `GET /api/v1/execute/defaults` against the configured Bamboo instance and
prints the resolved model, then exits — use it to confirm the device token and base URL are
correct before wiring up a platform.

## Config

Magpie reads `magpie.json` from (in priority order): the `--config` flag, then
`$BAMBOO_PLUGIN_SERVICE_CONFIG`, then `./magpie.json`. On unix, a config file that is
group- or world-readable triggers a startup warning (it carries the bamboo device token and
platform bot secrets in plaintext — v1 scope, see `ARCHITECTURE.md`).

```json
{
  "bamboo": {
    "base_url": "http://127.0.0.1:9560",
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
