# Magpie bamboo-plugin manifest

`plugin.json` declares Magpie as a bamboo-plugin `services` entry (bamboo
issue #479): once installed under `~/.bamboo/plugins/magpie/`, bamboo-server's
`ServiceManager` spawns, health-checks, and restarts the `magpie` binary
itself — no separate process manager needed.

## Config

Magpie's own `main.rs` resolves its config path in this order: `--config`
flag > `$BAMBOO_PLUGIN_SERVICE_CONFIG` env var > `./magpie.json`. As a plugin
service, bamboo-server sets `BAMBOO_PLUGIN_SERVICE_CONFIG` when it spawns the
process, so `plugin.json`'s `services[0].args` is intentionally empty — the
manifest does not need to pass `--config` explicitly. Point that env var (or
place a `magpie.json` beside the installed binary) at a config matching the
schema in `../ARCHITECTURE.md`'s "Config" section (bamboo endpoint +
device token, plus the `platforms[]` list).

## Checksums are placeholders

Every entry under `artifacts.<platform>.sha256` in `plugin.json` is the
literal string `"TBD-filled-by-release-CI"` — **not** a real hash. This
mirrors the "Zenith version placeholder strategy": the manifest ships with
`0.0.0`/placeholder values in the repo, and the release pipeline is
responsible for:

1. Building the per-platform `magpie` binary for `macos` / `windows` / `linux`.
2. Packaging each as the single-root-executable archive the manifest's
   artifact contract expects (`.tar.gz` on macos/linux with `magpie` at the
   archive root, `.zip` on windows with `magpie.exe` at the archive root) —
   see `bamboo-plugin`'s `manifest.rs` doc comment on `PluginArtifact` for the
   exact contract the installer expects.
3. Uploading each archive to the release's asset URL (matching
   `artifacts.<platform>.url` — update the URL too if the release tag or
   asset naming changes).
4. Computing the lowercase hex sha256 of each **raw archive** (not the
   unpacked binary) and rewriting the corresponding `sha256` field in
   `plugin.json` before it is published/registered as an installable plugin
   source.

Until that fill-in happens, `PluginManifest::validate()` will reject this
manifest (`sha256` must be exactly 64 lowercase hex characters) — that
rejection is deliberate: it is bamboo-plugin's guardrail against ever
installing a plugin whose binary hasn't been checksummed, so a
not-yet-released `plugin.json` failing validation is the *expected*,
correct state, not a bug to fix by relaxing the check.

Also bump `version` (both the top-level `plugin.json` field and the
`v0.0.0` release-tag segment baked into each `artifacts.*.url`) as part of
the same release step — the two must stay in lockstep or the installer will
fetch the wrong build.
