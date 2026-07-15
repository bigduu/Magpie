//! NeedsHuman/QuestionDialog → buttons/text replies → `POST /respond`
//! (ported from bamboo's `connect::approvals`, bamboo issue #458).
//!
//! Two concerns live here (a THIRD — the `Responder`/`EngineResponder`/
//! `ConnectResumePort` resolution seam — is deliberately NOT ported; see the
//! module-level "Port note" below):
//! - Rendering a [`PendingAsk`][render_pending_ask_type] as an outbound
//!   message (buttons when the platform supports them, always ALSO a
//!   numbered text list — text replies are first-class on every platform).
//! - Matching an inbound text reply or button `callback_data` against a
//!   [`ParkedAsk`], including the binary-ask keyword mapping
//!   (允许/yes/allow vs deny/no).
//!
//! ## Port note: no `Responder` seam here
//!
//! bamboo's in-proc `connect::approvals` also owns the resolution seam: a
//! `Responder` trait with a production `EngineResponder` that calls
//! `submit_pending_response` + `resume_session_execution` in-process
//! (`ConnectResumePort`), including ~180 lines re-executing a gated tool call
//! that was only a placeholder while awaiting approval. Magpie has no
//! in-process engine to call into — resolving an ask is just
//! `POST /api/v1/respond/{session_id}` (bamboo's `respond/handlers/submit.rs`
//! does the grant-recording + re-execution + resume server-side, exactly the
//! same code path `EngineResponder` called into). That single HTTP call is
//! made directly by `bridge::ConnectBridge::render_until_settled` via the
//! `bridge::BambooApi` seam — there is nothing left here to abstract over,
//! so `Responder`/`EngineResponder`/`ConnectResumePort` and the tool
//! re-execution block are DELETED, not ported. See ARCHITECTURE.md's in-proc
//! → API mapping table.
//!
//! [render_pending_ask_type]: crate::render::PendingAsk

use std::sync::Arc;

use crate::platform::{Button, MessageRef, OutboundMessage, Platform, PlatformResult, ReplyCtx};
use crate::render::PendingAsk;

/// Longest a button's visible label is allowed to be — Telegram (and most IM
/// platforms) truncate/reject very long inline-button text, so keep it well
/// under any known limit.
const BUTTON_LABEL_MAX_CHARS: usize = 48;

// ---------------------------------------------------------------------------
// ParkedAsk — the bridge's one-ask-per-chat state
// ---------------------------------------------------------------------------

/// A pending question rendered to a chat and awaiting resolution (button
/// press or text reply). One per chat at a time (bamboo issue #458: "one
/// parked ask per chat — session serializes asks").
#[derive(Debug, Clone)]
pub struct ParkedAsk {
    /// Short nonce embedded in every button's `callback_data`
    /// (`"{nonce}:{option_index}"`). Validated on every callback so
    /// forged/stale data is ignored.
    pub nonce: String,
    pub session_id: String,
    pub tool_call_id: String,
    pub tool_name: String,
    pub question: String,
    pub options: Vec<String>,
    pub allow_custom: bool,
}

impl ParkedAsk {
    pub fn new(nonce: String, session_id: String, ask: &PendingAsk) -> Self {
        Self {
            nonce,
            session_id,
            tool_call_id: ask.tool_call_id.clone(),
            tool_name: ask.tool_name.clone(),
            question: ask.question.clone(),
            options: ask.options.clone(),
            allow_custom: ask.allow_custom,
        }
    }
}

/// A short, hard-to-guess nonce for one parked ask. Not cryptographically
/// load-bearing on its own (it's paired with per-chat scoping + a single
/// live ask at a time) — just enough entropy that a stale/forged
/// `callback_data` from a different ask/session won't collide by accident.
pub fn new_nonce() -> String {
    let raw = uuid::Uuid::new_v4().to_string();
    raw.split('-').next().unwrap_or(&raw).to_string()
}

// ---------------------------------------------------------------------------
// Rendering an ask
// ---------------------------------------------------------------------------

fn truncate_label(text: &str) -> String {
    if text.chars().count() <= BUTTON_LABEL_MAX_CHARS {
        return text.to_string();
    }
    let mut out: String = text.chars().take(BUTTON_LABEL_MAX_CHARS - 1).collect();
    out.push('…');
    out
}

/// Format the ask's question + a numbered option list (text replies remain
/// first-class even when buttons are ALSO rendered).
pub fn format_ask_text(ask: &ParkedAsk) -> String {
    let mut text = ask.question.clone();
    if !ask.options.is_empty() {
        text.push_str("\n\n");
        for (index, option) in ask.options.iter().enumerate() {
            text.push_str(&format!("{}. {}\n", index + 1, option));
        }
    }
    if ask.allow_custom {
        text.push_str("\n(or reply with your own answer)");
    }
    text
}

/// Render `ask` to the chat: inline buttons (one per option, `callback_data =
/// "{nonce}:{index}"`) when `buttons_capable`, always alongside the numbered
/// text list — per bamboo issue #458, buttons are an enhancement, never a
/// requirement. Returns the sent message's [`MessageRef`] so the bridge can
/// edit the ask once answered (✅ + chosen answer, buttons dropped — issue
/// #6 follow-up); a send failure is returned for logging but does not itself
/// invalidate the parked ask (a text reply can still resolve it).
pub async fn render_ask(
    platform: &Arc<dyn Platform>,
    reply_ctx: &ReplyCtx,
    ask: &ParkedAsk,
    buttons_capable: bool,
) -> PlatformResult<MessageRef> {
    let text = format_ask_text(ask);
    let outbound = if buttons_capable && !ask.options.is_empty() {
        let rows: Vec<Vec<Button>> = ask
            .options
            .iter()
            .enumerate()
            .map(|(index, option)| {
                vec![Button::new(
                    truncate_label(option),
                    format!("{}:{index}", ask.nonce),
                )]
            })
            .collect();
        OutboundMessage::text(text).with_buttons(rows)
    } else {
        OutboundMessage::text(text)
    };
    platform.reply(reply_ctx, outbound).await
}

// ---------------------------------------------------------------------------
// Matching a text reply / callback against a ParkedAsk
// ---------------------------------------------------------------------------

const AFFIRMATIVE_KEYWORDS: &[&str] = &[
    "允许", "同意", "确定", "是", "yes", "allow", "approve", "ok",
];
/// "stay" comes from plan-mode's decline phrasing — ExitPlanMode's negative
/// option is literally "Stay in plan mode" on the bamboo side (see
/// `session_app::respond::is_exit_plan_mode_approved`), so a user typing
/// "stay" declines the plan approval. Safe to keep in this fallback list
/// because [`match_text_answer`] tries EXACT (case-insensitive) option-text
/// matching BEFORE the keyword fallback: an ask whose positive option is
/// literally titled "Stay" resolves on the exact match and never reaches
/// here.
const NEGATIVE_KEYWORDS: &[&str] = &["拒绝", "不", "否", "no", "deny", "reject", "stay"];

fn classify_intent(text: &str) -> Option<bool> {
    let lower = text.trim().to_lowercase();
    if AFFIRMATIVE_KEYWORDS.iter().any(|keyword| lower == *keyword) {
        return Some(true);
    }
    if NEGATIVE_KEYWORDS.iter().any(|keyword| lower == *keyword) {
        return Some(false);
    }
    None
}

/// "First-affirmative mapping": prefer an option whose OWN text already
/// reads as affirmative/negative (e.g. "Approve" / "Deny"); for a plain
/// 2-option ask with no such wording, fall back to treating the first option
/// as the affirmative one.
fn pick_option_by_intent(options: &[String], affirmative: bool) -> Option<String> {
    let keywords: &[&str] = if affirmative {
        AFFIRMATIVE_KEYWORDS
    } else {
        NEGATIVE_KEYWORDS
    };
    if let Some(option) = options.iter().find(|option| {
        let lower = option.to_lowercase();
        keywords.iter().any(|keyword| lower.contains(keyword))
    }) {
        return Some(option.clone());
    }
    if options.len() == 2 {
        return Some(if affirmative {
            options[0].clone()
        } else {
            options[1].clone()
        });
    }
    None
}

/// Match a text reply against `ask`, returning the answer to submit, or
/// `None` when it doesn't resolve the ask at all (bamboo issue #458: a
/// non-matching text on a CLOSED ask — no free text allowed — falls through
/// to the caller's normal busy-queue handling instead of being submitted as a
/// doomed-to-fail answer).
///
/// Tried in order: 1-based numeric option index, exact (case-insensitive)
/// option text, then — for a closed (non-`allow_custom`) ask — the
/// affirmative/negative keyword mapping. An OPEN ask (`allow_custom`) always
/// matches: any non-empty text IS the answer, verbatim (matching bamboo's
/// server-side `validate_pending_response` rule).
pub fn match_text_answer(ask: &ParkedAsk, text: &str) -> Option<String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(index) = trimmed.parse::<usize>() {
        if index >= 1 && index <= ask.options.len() {
            return Some(ask.options[index - 1].clone());
        }
    }
    if let Some(option) = ask
        .options
        .iter()
        .find(|option| option.eq_ignore_ascii_case(trimmed))
    {
        return Some(option.clone());
    }
    if !ask.allow_custom {
        if let Some(intent) = classify_intent(trimmed) {
            if let Some(option) = pick_option_by_intent(&ask.options, intent) {
                return Some(option);
            }
        }
    }
    if ask.allow_custom {
        return Some(trimmed.to_string());
    }
    None
}

/// Match a button press's `callback_data` (`"{nonce}:{index}"`) against
/// `ask`. Returns `None` for anything that doesn't EXACTLY match the parked
/// nonce and a valid option index — forged/stale data (bamboo issue #458:
/// "always answerCallbackQuery, even stale" — the caller acks regardless, but
/// never forwards a non-match as an answer).
pub fn match_callback_data(ask: &ParkedAsk, data: &str) -> Option<String> {
    let (nonce, index_str) = data.split_once(':')?;
    if nonce != ask.nonce {
        return None;
    }
    let index: usize = index_str.parse().ok()?;
    ask.options.get(index).cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ask(options: Vec<&str>, allow_custom: bool) -> ParkedAsk {
        ParkedAsk {
            nonce: "abc12345".to_string(),
            session_id: "sess-1".to_string(),
            tool_call_id: "call-1".to_string(),
            tool_name: "conclusion_with_options".to_string(),
            question: "Approve?".to_string(),
            options: options.into_iter().map(str::to_string).collect(),
            allow_custom,
        }
    }

    #[test]
    fn new_nonce_is_short_and_hex_like() {
        let nonce = new_nonce();
        assert!(!nonce.is_empty());
        assert!(nonce.len() <= 16);
        assert!(nonce.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn match_text_answer_numeric_index_selects_option() {
        let pending = ask(vec!["Approve", "Deny"], false);
        assert_eq!(
            match_text_answer(&pending, "1"),
            Some("Approve".to_string())
        );
        assert_eq!(match_text_answer(&pending, "2"), Some("Deny".to_string()));
        assert_eq!(match_text_answer(&pending, "3"), None);
    }

    #[test]
    fn match_text_answer_exact_text_is_case_insensitive() {
        let pending = ask(vec!["Approve", "Deny"], false);
        assert_eq!(
            match_text_answer(&pending, "approve"),
            Some("Approve".to_string())
        );
    }

    #[test]
    fn match_text_answer_binary_keyword_mapping() {
        let pending = ask(vec!["Approve", "Deny"], false);
        assert_eq!(
            match_text_answer(&pending, "允许"),
            Some("Approve".to_string())
        );
        assert_eq!(
            match_text_answer(&pending, "yes"),
            Some("Approve".to_string())
        );
        assert_eq!(
            match_text_answer(&pending, "deny"),
            Some("Deny".to_string())
        );
        assert_eq!(match_text_answer(&pending, "no"), Some("Deny".to_string()));
    }

    /// Ordering guarantee documented on [`NEGATIVE_KEYWORDS`]: an option
    /// literally titled "Stay" — even as the POSITIVE first choice — resolves
    /// via exact option-text matching BEFORE the keyword fallback, so the
    /// "stay"-is-negative heuristic can never misroute it.
    #[test]
    fn match_text_answer_exact_option_named_stay_beats_negative_keyword_fallback() {
        let pending = ask(vec!["Stay", "Leave"], false);
        assert_eq!(
            match_text_answer(&pending, "stay"),
            Some("Stay".to_string())
        );
        // And the fallback still works as intended for plan-mode phrasing,
        // where "stay" appears INSIDE the negative option's text.
        let plan_pending = ask(vec!["Approve", "Stay in plan mode"], false);
        assert_eq!(
            match_text_answer(&plan_pending, "stay"),
            Some("Stay in plan mode".to_string())
        );
    }

    #[test]
    fn match_text_answer_closed_ask_non_matching_text_falls_through() {
        let pending = ask(vec!["Approve", "Deny"], false);
        assert_eq!(match_text_answer(&pending, "banana"), None);
    }

    #[test]
    fn match_text_answer_open_question_accepts_any_free_text() {
        let pending = ask(vec!["OK", "Need changes"], true);
        assert_eq!(
            match_text_answer(&pending, "please add tests too"),
            Some("please add tests too".to_string())
        );
    }

    #[test]
    fn match_text_answer_empty_text_never_matches() {
        let pending = ask(vec!["OK", "Need changes"], true);
        assert_eq!(match_text_answer(&pending, "   "), None);
    }

    #[test]
    fn match_callback_data_requires_the_exact_nonce() {
        let pending = ask(vec!["Approve", "Deny"], false);
        assert_eq!(
            match_callback_data(&pending, "abc12345:0"),
            Some("Approve".to_string())
        );
        assert_eq!(match_callback_data(&pending, "stale-nonce:0"), None);
    }

    #[test]
    fn match_callback_data_rejects_out_of_range_index() {
        let pending = ask(vec!["Approve", "Deny"], false);
        assert_eq!(match_callback_data(&pending, "abc12345:9"), None);
    }

    #[test]
    fn match_callback_data_rejects_malformed_data() {
        let pending = ask(vec!["Approve", "Deny"], false);
        assert_eq!(match_callback_data(&pending, "not-a-valid-shape"), None);
        assert_eq!(match_callback_data(&pending, "abc12345:not-a-number"), None);
    }

    #[test]
    fn format_ask_text_numbers_every_option() {
        let pending = ask(vec!["Approve", "Deny"], false);
        let text = format_ask_text(&pending);
        assert!(text.contains("1. Approve"));
        assert!(text.contains("2. Deny"));
        assert!(!text.contains("reply with your own answer"));
    }

    #[test]
    fn format_ask_text_open_question_mentions_free_text() {
        let pending = ask(vec!["OK", "Need changes"], true);
        assert!(format_ask_text(&pending).contains("reply with your own answer"));
    }
}
