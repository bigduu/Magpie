//! Wire types for Bamboo's public HTTP + `/v2/stream` WS API.
//!
//! Mirrored from bamboo's actual handler request/response structs (READ from
//! `bamboo/.claude/worktrees/magpie-ref`, NOT reimplemented from memory — see
//! the file:line citations on each type below). Only the fields Magpie
//! actually touches are carried; extra fields on the wire are silently
//! ignored by serde (no `deny_unknown_fields` anywhere in this module), so a
//! partial mirror is safe against additive server-side changes.
//!
//! [`AgentEvent`] is a deliberate SUBSET of bamboo's real
//! `bamboo_agent_core::AgentEvent` (see
//! `crates/core/bamboo-agent-core/src/agent/events.rs`) — it carries only the
//! variants bamboo's own `connect::render` module matches on
//! (`crates/app/bamboo-server/src/connect/render.rs`: `Token`, `ToolStart`,
//! `NeedClarification`, `Complete`, `Cancelled`, `Error`), plus a
//! `#[serde(other)]` catch-all so an event type bamboo adds later never
//! breaks deserialization — it just decodes to `Unknown` and callers ignore
//! it the same way `render.rs`'s `Ok(_) => continue` does today.

use serde::{Deserialize, Serialize};

// ── shared value types ──────────────────────────────────────────────────

/// Mirrors `bamboo_domain::ProviderModelRef`
/// (`crates/core/bamboo-domain/src/provider_model_ref.rs:11`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderModelRef {
    pub provider: String,
    pub model: String,
}

/// Mirrors `bamboo_domain::reasoning::ReasoningEffort`
/// (`crates/core/bamboo-domain/src/reasoning.rs:9`). `#[serde(rename_all =
/// "lowercase")]` on the source — pinned identically here.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ReasoningEffort {
    Low,
    Medium,
    High,
    Xhigh,
    Max,
}

/// Mirrors `bamboo_domain::TokenUsage`
/// (`crates/core/bamboo-domain/src/token_usage.rs:11`).
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TokenUsage {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
}

// ── GET /api/v1/execute/defaults ────────────────────────────────────────
// crates/app/bamboo-server/src/handlers/agent/execute/defaults.rs:34-61
// (`ExecuteDefaultsResponse`). `gold_config` is carried as an opaque `Value`
// — Magpie only ever displays/forwards it, never inspects its shape.

#[derive(Debug, Clone, Deserialize)]
pub struct ExecuteDefaultsResponse {
    pub model: Option<String>,
    pub provider: Option<String>,
    pub provider_type: Option<String>,
    pub reasoning_effort: Option<ReasoningEffort>,
    pub system_prompt: String,
    pub base_system_prompt: String,
    pub workspace_path: Option<String>,
    #[serde(default)]
    pub gold_config: Option<serde_json::Value>,
    pub fast_model: Option<String>,
    pub background_model: Option<String>,
    pub summarization_model: Option<String>,
}

// ── POST /api/v1/chat ───────────────────────────────────────────────────
// crates/app/bamboo-server/src/handlers/agent/chat/types.rs:19-73
// (`ChatRequest`/`ChatResponse`). `model` is optional (#480) — omitting it
// lets the server fall back to its resolved default (same resolution
// `GET /execute/defaults` reports).

#[derive(Debug, Clone, Default, Serialize)]
pub struct ChatRequest {
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enhance_prompt: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_ref: Option<ProviderModelRef>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChatResponse {
    pub session_id: String,
    pub stream_url: String,
    pub status: String,
    #[serde(default)]
    pub goal_command: Option<serde_json::Value>,
}

// ── POST /api/v1/sessions ───────────────────────────────────────────────
// crud/create.rs:24-97 (`CreateSessionRequest`/`CreateSessionResponse`);
// `SessionSummary` at handlers/agent/sessions/types.rs:11-63 (trimmed here to
// the fields Magpie needs — extra server fields are ignored on decode).

#[derive(Debug, Clone, Default, Serialize)]
pub struct CreateSessionRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_ref: Option<ProviderModelRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gold_config: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_path: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateSessionResponse {
    pub session: SessionSummary,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SessionSummary {
    pub id: String,
    pub title: String,
    pub model: String,
    #[serde(default)]
    pub is_running: bool,
    pub message_count: usize,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

// ── GET /api/v1/runs/active ─────────────────────────────────────────────
// crud/running_snapshot.rs + sessions/types.rs:167-187
// (`RunningSessionEntry`/`RunningSessionsResponse`).

#[derive(Debug, Clone, Deserialize)]
pub struct RunningSessionEntry {
    pub session_id: String,
    pub run_id: String,
    pub started_at: String,
    pub round_count: u32,
    #[serde(default)]
    pub last_tool_name: Option<String>,
    #[serde(default)]
    pub last_tool_phase: Option<String>,
    #[serde(default)]
    pub last_event_at: Option<String>,
    #[serde(default)]
    pub last_critical_events: Vec<AgentEvent>,
    #[serde(default)]
    pub running_child_session_ids: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RunningSessionsResponse {
    pub sessions: Vec<RunningSessionEntry>,
}

// ── POST /api/v1/execute/{session_id} ───────────────────────────────────
// execute/types.rs:26-141 (`ExecuteRequest`/`ExecuteResponse`/
// `ExecuteSyncInfo`/`ExecuteSyncReason`/`ExecuteClientSync`); status strings
// from execute/handler/response.rs: "completed" | "already_running" |
// "started" (202 Accepted).

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecuteSyncReason {
    MessageCountMismatch,
    LastMessageIdMismatch,
    PendingQuestionMismatch,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ExecuteClientSync {
    pub client_message_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_last_message_id: Option<String>,
    #[serde(default)]
    pub client_has_pending_question: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_pending_question_tool_call_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExecuteSyncInfo {
    pub need_sync: bool,
    #[serde(default)]
    pub reason: Option<ExecuteSyncReason>,
    pub server_message_count: usize,
    #[serde(default)]
    pub server_last_message_id: Option<String>,
    pub has_pending_question: bool,
    #[serde(default)]
    pub pending_question_tool_call_id: Option<String>,
    pub has_pending_user_message: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ExecuteRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_ref: Option<ProviderModelRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skill_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_sync: Option<ExecuteClientSync>,
    /// Headless connector run: Magpie has no interactive human approver on
    /// the Bamboo side of the wire (#74 semantics — see execute/types.rs:132-140).
    #[serde(default)]
    pub no_human_approver: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExecuteResponse {
    pub session_id: String,
    /// "started" | "completed" | "already_running".
    pub status: String,
    pub events_url: String,
    #[serde(default)]
    pub sync: Option<ExecuteSyncInfo>,
    #[serde(default)]
    pub run_id: Option<String>,
}

impl ExecuteResponse {
    pub fn is_already_running(&self) -> bool {
        self.status == "already_running"
    }

    pub fn is_started(&self) -> bool {
        self.status == "started"
    }
}

// ── POST /api/v1/stop/{session_id} ──────────────────────────────────────
// stop/types.rs:4-10 (`StopResponse`).

#[derive(Debug, Clone, Deserialize)]
pub struct StopResponse {
    pub success: bool,
    pub message: String,
}

// ── POST /api/v1/respond/{session_id} ───────────────────────────────────
// respond/types.rs:11-26 (`RespondRequest`); the submit handler's response is
// an ad hoc `serde_json::json!` object (respond/handlers/submit.rs:138-144),
// mirrored here as `RespondSubmitResponse`.

#[derive(Debug, Clone, Default, Serialize)]
pub struct RespondRequest {
    pub response: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_ref: Option<ProviderModelRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RespondSubmitResponse {
    pub success: bool,
    pub message: String,
    pub response: String,
    pub auto_resume_status: String,
    #[serde(default)]
    pub run_id: Option<String>,
}

// ── GET /api/v1/respond/{session_id}/pending ────────────────────────────
// respond/handlers/pending.rs:13-39 — two ad hoc `serde_json::json!` shapes
// (`has_pending_question: false` alone, or the full pending-question object).

#[derive(Debug, Clone, Deserialize)]
pub struct RespondPendingResponse {
    pub has_pending_question: bool,
    #[serde(default)]
    pub question: Option<String>,
    #[serde(default)]
    pub options: Option<Vec<String>>,
    #[serde(default)]
    pub allow_custom: Option<bool>,
    #[serde(default)]
    pub tool_call_id: Option<String>,
    #[serde(default)]
    pub tool_name: Option<String>,
    #[serde(default)]
    pub source: Option<serde_json::Value>,
}

// ── AgentEvent (subset — see module docs) ───────────────────────────────
// crates/core/bamboo-agent-core/src/agent/events.rs:97 —
// `#[serde(tag = "type", rename_all = "snake_case")]`, pinned identically.

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentEvent {
    Token {
        content: String,
    },
    ToolStart {
        tool_call_id: String,
        tool_name: String,
        arguments: serde_json::Value,
    },
    NeedClarification {
        question: String,
        options: Option<Vec<String>>,
        #[serde(default)]
        tool_call_id: Option<String>,
        #[serde(default)]
        tool_name: Option<String>,
        #[serde(default = "default_allow_custom")]
        allow_custom: bool,
    },
    Complete {
        usage: TokenUsage,
    },
    Cancelled {
        #[serde(default)]
        message: Option<String>,
    },
    Error {
        message: String,
    },
    /// Any `AgentEvent` variant bamboo emits that isn't one of the above.
    /// Render/bridge logic should treat this exactly like `render.rs`'s
    /// `Ok(_) => continue` — ignore it, never error.
    #[serde(other)]
    Unknown,
}

fn default_allow_custom() -> bool {
    true
}

impl AgentEvent {
    /// Whether this is one of the three terminal variants — mirrors
    /// `ws_v2::forwarders::is_terminal_event`
    /// (`crates/app/bamboo-server/src/handlers/agent/ws_v2/forwarders.rs:399-404`).
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            AgentEvent::Complete { .. } | AgentEvent::Cancelled { .. } | AgentEvent::Error { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_event_token_round_trips() {
        let json = serde_json::json!({ "type": "token", "content": "hi" });
        let event: AgentEvent = serde_json::from_value(json).unwrap();
        match event {
            AgentEvent::Token { content } => assert_eq!(content, "hi"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn agent_event_unknown_variant_decodes_to_unknown_not_error() {
        let json = serde_json::json!({
            "type": "task_list_updated",
            "task_list": { "session_id": "s1", "items": [] }
        });
        let event: AgentEvent = serde_json::from_value(json).unwrap();
        assert!(matches!(event, AgentEvent::Unknown));
    }

    #[test]
    fn agent_event_completely_unrecognized_type_also_decodes_to_unknown() {
        let json = serde_json::json!({ "type": "some_future_event", "whatever": 1 });
        let event: AgentEvent = serde_json::from_value(json).unwrap();
        assert!(matches!(event, AgentEvent::Unknown));
    }

    #[test]
    fn agent_event_terminal_predicate() {
        assert!(AgentEvent::Complete {
            usage: TokenUsage::default()
        }
        .is_terminal());
        assert!(AgentEvent::Cancelled { message: None }.is_terminal());
        assert!(AgentEvent::Error {
            message: "x".into()
        }
        .is_terminal());
        assert!(!AgentEvent::Token {
            content: "x".into()
        }
        .is_terminal());
        assert!(!AgentEvent::Unknown.is_terminal());
    }

    #[test]
    fn need_clarification_defaults_allow_custom_true_when_absent() {
        let json = serde_json::json!({
            "type": "need_clarification",
            "question": "Continue?",
            "options": ["Yes", "No"]
        });
        let event: AgentEvent = serde_json::from_value(json).unwrap();
        match event {
            AgentEvent::NeedClarification { allow_custom, .. } => assert!(allow_custom),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn execute_response_status_helpers() {
        let resp = ExecuteResponse {
            session_id: "s".into(),
            status: "already_running".into(),
            events_url: "/api/v1/events/s".into(),
            sync: None,
            run_id: None,
        };
        assert!(resp.is_already_running());
        assert!(!resp.is_started());
    }

    #[test]
    fn chat_request_omits_model_when_none() {
        let req = ChatRequest {
            message: "hi".into(),
            ..Default::default()
        };
        let json = serde_json::to_value(&req).unwrap();
        assert!(json.get("model").is_none());
        assert_eq!(json["message"], "hi");
    }

    #[test]
    fn respond_pending_response_false_shape_decodes() {
        let json = serde_json::json!({ "has_pending_question": false });
        let resp: RespondPendingResponse = serde_json::from_value(json).unwrap();
        assert!(!resp.has_pending_question);
        assert!(resp.question.is_none());
    }

    #[test]
    fn respond_pending_response_true_shape_decodes() {
        let json = serde_json::json!({
            "has_pending_question": true,
            "question": "Pick one",
            "options": ["A", "B"],
            "allow_custom": false,
            "tool_call_id": "call-1",
            "tool_name": "conclusion_with_options",
            "source": "gold"
        });
        let resp: RespondPendingResponse = serde_json::from_value(json).unwrap();
        assert!(resp.has_pending_question);
        assert_eq!(resp.question.as_deref(), Some("Pick one"));
        assert_eq!(resp.options, Some(vec!["A".to_string(), "B".to_string()]));
    }
}
