//! Magpie (鹊) — standalone IM connector for Bamboo.
//!
//! This crate is split lib+bin so the Bamboo-facing layer built in phase 1
//! (`bamboo::client`, `bamboo::stream`, `bamboo::types`, `config`) is a real
//! public API surface — `pub` items here are exempt from the `dead_code`
//! lint the way they would NOT be in a bin-only crate. Phase 2 adds the
//! platform adapters + bridge on top: `platform` (the `Platform` trait +
//! message types), `platforms::{telegram, feishu}` (the adapters), `render`
//! (`AgentEvent`/`StreamEvent` stream -> platform messages), `approvals`
//! (ask rendering/matching), and `bridge` (chat <-> bamboo-session routing).
//! See ARCHITECTURE.md's `src/` layout table.

pub mod approvals;
pub mod bamboo;
pub mod bridge;
pub mod config;
pub mod platform;
pub mod platforms;
pub mod render;
