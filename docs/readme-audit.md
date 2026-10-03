# README source audit — 2026-10-03

- Zenith pin before documentation edits: `192de16ed7384b027a3ecd3260eeb2c126c84f17`.
- `git ls-remote origin HEAD` returned that same SHA. No newer default-branch source was observed; no Zenith gitlink was changed.
- Release evidence: https://github.com/bigduu/Magpie/releases . Public release page lists `v0.1.1` and `v0.1.0`. Source is six commits ahead of `v0.1.1`; reconnect/ask, queued-message, stop, and initial-config fixes must not be attributed to v0.1.1.
- GitHub API requests were blocked by this environment's proxy (403); the public release HTML was retrieved successfully instead.
- Source evidence: `src/main.rs`, `src/config.rs`, `src/bridge.rs`, platform adapters, `plugin/plugin.json`, and `.github/workflows/release.yml`.
- Scope: README files and this audit only. No product code, releases, credentials, remote branches, or deployment settings changed.
- Validation: local Markdown targets and `git diff --check`; command names and requirements compared with checked-in source. No real IM credentials, PostgreSQL service, provider calls, or native macOS/Windows behavior were exercised by this review.
