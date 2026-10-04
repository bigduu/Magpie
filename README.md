# Magpie (鹊)

[English](README.md) · [简体中文](README.zh-CN.md)

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

The latest release is [`v0.1.1`](https://github.com/bigduu/Magpie/releases/tag/v0.1.1).
`main` already carries the `0.1.2` version bump and unreleased fixes: replies lost after
answering a resumed question, resuming pending questions after reconnects or restarts,
queued messages and `/stop` while a question is pending, and waiting for initial
configuration instead of exit-restart looping. Do not assume those fixes are in the
`v0.1.1` binaries; see the [full comparison](https://github.com/bigduu/Magpie/compare/v0.1.1...main)
and the [audit notes](./docs/readme-audit.md).

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
  running, install the current release (`v0.1.1`) with:

  ```bash
  bamboo plugin install https://github.com/bigduu/Magpie/releases/download/v0.1.1/magpie-plugin-v0.1.1.tar.gz
  ```

  For a later release, replace both `0.1.1` values with that release's version.

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

`bamboo.device_id`/`bamboo.token` are a paired device credential minted by Bamboo —
Magpie authenticates every request as that device. Lotus Next does not have a device-pairing screen yet;
request the credential from Bamboo's `POST /v2/pair` endpoint. With a Bamboo access password
enabled:

```bash
curl -sS -X POST http://127.0.0.1:9562/v2/pair \
  -H 'Content-Type: application/json' \
  -d '{"root_password":"<your Bamboo access password>","label":"magpie"}'
```

Copy `device_id` into `bamboo.device_id` and `device_token` into `bamboo.token`; the token is
shown only once. An already-paired device can instead request a one-time code with
`POST /v2/pair/code` and redeem it with `{"code":"<code>","label":"magpie"}`. See Bamboo's
[pairing design](https://github.com/bigduu/Bamboo-agent/blob/dev/docs/design/api-v2-transport.md).
An empty `allow_from` list denies every inbound message (logged as a startup warning), so a
freshly-configured platform entry never accidentally opens itself to the whole internet.

See [`ARCHITECTURE.md`](./ARCHITECTURE.md) for the full schema, the Bamboo API mapping table,
and the invariants carried over from bamboo's in-process `connect/` module.

For a private local config on Unix, run `chmod 600 magpie.json`. Protect the
config directory too: Magpie stores its conversation/session map beside the config.
Plugin builds target macOS, Linux, and Windows; this documentation review did not
exercise native platform behavior or send messages through real IM accounts.

## Feishu/Lark setup

These steps follow what the Feishu adapter in `src/platforms/feishu/` implements:

1. In the [Feishu Open Platform](https://open.feishu.cn/app) (or
   [Lark Developer](https://open.larksuite.com/app)), create a custom app for your
   organization and enable its **bot** capability. Copy its App ID (`cli_…`) and
   App Secret into `app_id` / `app_secret`.
2. Under event subscriptions, choose **long connection** delivery (no public URL is
   needed) and subscribe to `im.message.receive_v1`. Grant the app the message
   permissions needed to receive messages and send messages as the bot.
3. To answer agent questions and approvals with buttons, also enable the card
   callback `card.action.trigger` over the long connection. Typed replies also work.
4. Set `"domain": "feishu"` for Feishu (China) or `"domain": "lark"` for Lark;
   an `https://` base URL is also accepted.
5. Add your `open_id` (`ou_…`) to `allow_from`. If you do not know it, start
   Magpie, send the bot a message and look for the warning
   `rejected inbound message — user not in allow_from`; it logs your `user_id`.
6. Publish the app version so it can be used in your organization, then message the
   bot directly. In group chats Magpie only responds when the bot is @-mentioned.

Chat commands: `/new` starts a new session, `/stop` stops the current run and
`/status` shows the current session. Replies are text cards; images and files are
not sent.

The console labels and permission names above may differ slightly between Feishu
and Lark versions; this review did not create a live app.
