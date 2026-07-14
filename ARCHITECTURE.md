# Magpie (鹊) — architecture

Magpie is the standalone IM connector for Bamboo (extraction of bamboo-server's
in-process `connect/` module, per bamboo epic #477). Named after 鹊桥 — the
magpie bridge that spans between worlds.

It drives Bamboo agent sessions from IM platforms (Telegram, Feishu/Lark)
exclusively over **Bamboo's public API** — never in-process internals — and
ships as a Bamboo **service plugin** (bamboo-plugin `services` artifact kind,
bamboo #479): bamboo-server installs, spawns, supervises, and restarts it.

## Layout

```
src/
  main.rs            — config load, client construction, platform startup
  config.rs          — magpie.json (bamboo endpoint+device token, platforms)
  bamboo/
    client.rs        — HTTP client: execute/defaults, chat, execute, respond
                       (+pending), stop, runs/active; device-token auth
    stream.rs        — /v2/stream WS client: hello, subscribe agent.{sid},
                       reconnect + resubscribe, terminal control frames
    types.rs         — wire types mirrored from bamboo's public API
  bridge.rs          — SessionKey → session-id map (magpie_sessions.json),
                       busy lock + FIFO queue, /new /stop /status, allow_from,
                       dedup, ask lifecycle          [ported from bamboo]
  render.rs          — AgentEvent JSON → platform messages, streaming
                       edit-in-place (1.5s/30-char throttle)  [ported]
  approvals.rs       — ask rendering/matching, callback nonces [ported]
  platform.rs        — Platform trait + Capabilities        [ported as-is]
  platforms/
    telegram.rs      — long-poll adapter                    [ported as-is]
    feishu/          — WS long-connection adapter           [ported as-is]
plugin/
  plugin.json        — bamboo-plugin manifest (services entry, per-platform
                       artifacts, sha256 filled by release CI)
```

## Key mappings from the in-proc module (bamboo #480 gap analysis)

| in-proc dependency | Magpie replacement |
|---|---|
| `resolve_connect_run_config` (Config+ProviderRegistry) | `GET /api/v1/execute/defaults` |
| `session.add_message` + `spawn_session_execution` | `POST /chat` (model omitted → server default) then `POST /execute/{id}` |
| `try_reserve_runner` / cancel_token | `POST /execute` returns AlreadyRunning; `POST /stop/{id}` by session id |
| broadcast `AgentEvent` subscription | `/v2/stream` `subscribe {ch:"agent.{sid}"}` — SUBSCRIBE BEFORE EXECUTE (replay covers critical events only, not tokens) |
| `EngineResponder` / `ConnectResumePort` | `POST /respond/{id}` (does grants + resume server-side); the ~180-line in-proc resume duplication is DELETED, not ported |
| ask resync | `GET /respond/{id}/pending` |
| connect.json secret round-trip | magpie owns its own config; as a plugin service the path arrives via `BAMBOO_PLUGIN_SERVICE_CONFIG` |

## Config

`magpie.json` (default: `$BAMBOO_PLUGIN_SERVICE_CONFIG` if set, else
`./magpie.json`, overridable with `--config`):

```json
{
  "bamboo": { "base_url": "http://127.0.0.1:9560", "device_id": "…", "token": "…" },
  "platforms": [
    { "type": "telegram", "token": "…", "allow_from": ["…"] },
    { "type": "feishu", "app_id": "…", "app_secret": "…", "domain": "feishu", "allow_from": ["…"] }
  ]
}
```

v1 keeps secrets plaintext in the file (0600 perms enforced at load, warn
otherwise) — bamboo's encrypted round-trip is coupled to bamboo's key store
and does not extract; revisit post-v1.

## Invariants carried over from bamboo connect/

- allow_from empty = deny-all (warn at startup)
- inbound dedup (platform:message_id) + drop messages older than process start
- adapter-side outbound rate limiting (blocking, never dropping)
- secrets never appear in error/log text
- one live bot per platform type (session keys have no bot dimension)
- buttons capability-gated; numbered-text answers always work

## Known limitations vs in-proc (documented, accepted)

- WS resubscribe replays critical events + last budget only — a mid-run
  reconnect misses tokens (terminal event still lands; next edit repaints).
- Two round trips per message (chat then execute) until/unless bamboo grows
  a combined /turn endpoint.
