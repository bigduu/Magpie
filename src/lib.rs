//! Magpie (鹊) — standalone IM connector for Bamboo.
//!
//! This crate is split lib+bin so the Bamboo-facing layer built in phase 1
//! (`bamboo::client`, `bamboo::stream`, `bamboo::types`, `config`) is a real
//! public API surface — `pub` items here are exempt from the `dead_code`
//! lint the way they would NOT be in a bin-only crate, since phase 2 (the
//! platform adapters + bridge) is the actual consumer and doesn't exist yet
//! in this repo. See ARCHITECTURE.md's `src/` layout table for what phase 2
//! adds on top.

pub mod bamboo;
pub mod config;
