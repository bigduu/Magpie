//! Chat ⇄ bamboo-session routing, busy lock + FIFO queue, and execution
//! through Bamboo's public API (ported from bamboo's `connect::bridge`).
//!
//! ## Port note: in-proc engine calls → `BambooApi`
//!
//! bamboo's in-proc bridge runs a prompt through the canonical
//! `spawn_session_execution` path directly (agent/tools/session-repo all
//! live in the same process) and answers a pending question through the
//! `approvals::Responder` seam (`submit_pending_response` +
//! `resume_session_execution`, in-process). Magpie has neither — every one
//! of those becomes an HTTP (or WS-subscribe) call against Bamboo's public
//! `/api/v1` surface, per ARCHITECTURE.md's in-proc→API mapping table:
//!
//! | in-proc | magpie |
//! |---|---|
//! | `session.add_message` + `spawn_session_execution` | [`BambooApi::chat`] then [`BambooApi::execute`] |
//! | `try_reserve_runner` / cancel_token | `execute` returning `AlreadyRunning`; [`BambooApi::stop`] by session id |
//! | broadcast `AgentEvent` subscription | [`BambooApi::subscribe_session`] — SUBSCRIBE BEFORE EXECUTE |
//! | `Responder::respond_and_resume` | [`BambooApi::respond`] (grants + resume happen server-side) |
//! | ask resync after a restart/reconnect | [`BambooApi::respond_pending`] |
//!
//! [`BambooApi`] is a small trait over exactly those five calls (a seam
//! mirroring bamboo's own `approvals::Responder` — narrow enough that tests
//! inject a `FakeBambooApi` instead of standing up a live Bamboo server /
//! `AppState`). [`BambooEndpoint`] is the production implementation,
//! delegating the four HTTP methods 1:1 to [`crate::bamboo::BambooClient`]'s
//! own methods and `subscribe_session` to [`crate::bamboo::BambooStream`].
//!
//! There is no local `cancel_token`/`AgentRunner` reservation bookkeeping —
//! Bamboo's server is the sole source of truth for "is this session
//! running": `/stop/{id}` is just an HTTP call keyed by session id, and
//! `execute`'s `AlreadyRunning` status (rather than a local reservation
//! failure) is what tells us a run is already live server-side.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};

use chrono::{DateTime, Utc};
use tokio::sync::{mpsc, Mutex as AsyncMutex, RwLock as TokioRwLock};

use crate::approvals::{self, ParkedAsk};
use crate::bamboo::stream::StreamEvent;
use crate::bamboo::types::{
    ChatRequest, ChatResponse, ExecuteRequest, ExecuteResponse, RespondPendingResponse,
    RespondRequest, RespondSubmitResponse, StopResponse,
};
use crate::bamboo::{BambooClient, BambooStream, ClientError, StreamError};
use crate::platform::{
    CallbackQuery, InboundMessage, MessageRef, OutboundMessage, Platform, ReplyCtx,
};
use crate::render;

/// `platform:chat_id:user_id` — the chat-scoped routing key mapping to a
/// bamboo session id (see bamboo epic #447's "Bridge" design).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionKey {
    pub platform: String,
    pub chat_id: String,
    pub user_id: String,
}

impl SessionKey {
    pub fn as_string(&self) -> String {
        format!("{}:{}:{}", self.platform, self.chat_id, self.user_id)
    }
}

/// Max entries [`BoundedSeenSet`] retains before evicting the oldest
/// (bamboo issue #454 follow-up). This is defense-in-depth dedup, layered on
/// top of each adapter's own transport-level dedup (e.g. Telegram's offset
/// advance) — it only needs to cover the realistic in-flight
/// redelivery/retry window, not serve as a permanent audit log. A few
/// thousand entries comfortably covers any plausible burst across every
/// configured chat while keeping memory bounded for the life of the
/// process.
const DEDUP_CAPACITY: usize = 10_000;

/// A `HashSet` bounded to at most `capacity` entries via FIFO eviction:
/// once full, inserting a new key evicts the oldest still-tracked key.
/// Used for [`ConnectBridge::seen_message_ids`] — a plain `HashSet` there
/// would gain one entry per distinct `platform:message_id` for the life of
/// the process (bamboo issue #454 follow-up).
struct BoundedSeenSet {
    set: HashSet<String>,
    order: VecDeque<String>,
    capacity: usize,
}

impl BoundedSeenSet {
    fn new(capacity: usize) -> Self {
        Self {
            set: HashSet::new(),
            order: VecDeque::new(),
            capacity: capacity.max(1),
        }
    }

    /// Inserts `key`, evicting the oldest tracked key(s) if this pushes the
    /// set past capacity. Returns `true` if `key` was newly inserted (i.e.
    /// not a duplicate) — same contract as `HashSet::insert`.
    fn insert(&mut self, key: String) -> bool {
        if !self.set.insert(key.clone()) {
            return false;
        }
        self.order.push_back(key);
        while self.order.len() > self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.set.remove(&oldest);
            }
        }
        true
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.set.len()
    }
}

// ---------------------------------------------------------------------------
// BambooApi — the seam bridge.rs drives a run through
// ---------------------------------------------------------------------------

/// Everything the bridge needs from Bamboo's public API to drive a run:
/// the four HTTP calls plus subscribing to the `/v2/stream` WS channel. A
/// deliberately narrow seam (mirrors bamboo's own `connect::approvals::
/// Responder`) so tests inject a `FakeBambooApi` instead of a live Bamboo
/// server. See the module doc's mapping table for what in-proc call each
/// method replaces.
#[async_trait::async_trait]
pub trait BambooApi: Send + Sync {
    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse, ClientError>;
    async fn execute(
        &self,
        session_id: &str,
        request: ExecuteRequest,
    ) -> Result<ExecuteResponse, ClientError>;
    async fn stop(&self, session_id: &str) -> Result<StopResponse, ClientError>;
    async fn respond(
        &self,
        session_id: &str,
        request: RespondRequest,
    ) -> Result<RespondSubmitResponse, ClientError>;
    async fn respond_pending(
        &self,
        session_id: &str,
    ) -> Result<RespondPendingResponse, ClientError>;
    async fn subscribe_session(
        &self,
        session_id: &str,
    ) -> Result<mpsc::Receiver<StreamEvent>, StreamError>;
}

/// Production [`BambooApi`]: the four HTTP methods delegate 1:1 to
/// [`BambooClient`]'s own methods of the same name; `subscribe_session`
/// delegates to [`BambooStream`]. Two separate transports (REST vs the
/// persistent `/v2/stream` WS connection) are bundled behind one seam here
/// purely so `ConnectBridge` has a single dependency to hold and tests have
/// a single trait to fake — `BambooClient`/`BambooStream` themselves are
/// untouched (constructed exactly as `main.rs` already does per phase 1).
#[derive(Clone)]
pub struct BambooEndpoint {
    pub client: BambooClient,
    pub stream: BambooStream,
}

impl BambooEndpoint {
    pub fn new(client: BambooClient, stream: BambooStream) -> Self {
        Self { client, stream }
    }
}

#[async_trait::async_trait]
impl BambooApi for BambooEndpoint {
    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse, ClientError> {
        self.client.chat(&request).await
    }

    async fn execute(
        &self,
        session_id: &str,
        request: ExecuteRequest,
    ) -> Result<ExecuteResponse, ClientError> {
        self.client.execute(session_id, &request).await
    }

    async fn stop(&self, session_id: &str) -> Result<StopResponse, ClientError> {
        self.client.stop(session_id).await
    }

    async fn respond(
        &self,
        session_id: &str,
        request: RespondRequest,
    ) -> Result<RespondSubmitResponse, ClientError> {
        self.client.respond(session_id, &request).await
    }

    async fn respond_pending(
        &self,
        session_id: &str,
    ) -> Result<RespondPendingResponse, ClientError> {
        self.client.respond_pending(session_id).await
    }

    async fn subscribe_session(
        &self,
        session_id: &str,
    ) -> Result<mpsc::Receiver<StreamEvent>, StreamError> {
        self.stream.subscribe_session(session_id).await
    }
}

/// Per-chat runtime state: whether a run is currently executing, the FIFO
/// queue of messages that arrived while busy (drained at run end — mirrors
/// cc-connect engine.go's `queueMessageForBusySession`), and the chat's one
/// parked ask (if any) — bamboo issue #458's approval/question relay.
#[derive(Default)]
struct ChatState {
    busy: bool,
    queue: VecDeque<(Arc<dyn Platform>, InboundMessage)>,
    /// The chat's single in-flight pending question, if the current run is
    /// paused on one (bamboo issue #458: "one parked ask per chat").
    pending_ask: Option<ParkedAsk>,
    /// Resolver for `pending_ask`, held by the render task
    /// (`ConnectBridge::render_until_settled`) that's waiting on it.
    /// `handle_inbound`/`handle_callback` push a resolution here instead of
    /// queuing a matching reply — this is what lets an answer "jump" the
    /// busy queue while the run is genuinely suspended waiting for exactly
    /// this. Buffered at 1 so a resolver can send without the render task
    /// having reached its `recv().await` yet.
    ask_resolution: Option<mpsc::Sender<AskResolution>>,
}

/// What resolved (or invalidated) a chat's parked ask.
#[derive(Debug, Clone)]
enum AskResolution {
    /// A button press or text reply matched the parked ask; submit this as
    /// the answer.
    Answer(String),
    /// `/new`, session rotation, or an explicit clear invalidated the ask
    /// before it was answered — the waiting render task must stop rendering
    /// this (now-abandoned) run rather than hang forever.
    Invalidated,
}

/// What [`ConnectBridge::try_resolve_pending_ask`] found when an inbound
/// reply matched a chat's parked ask.
enum PendingAskMatch {
    /// The ordinary case: a live [`ConnectBridge::render_until_settled`]
    /// task is parked waiting on `ask_resolution` for exactly this ask —
    /// hand it the answer and it owns the rest of the resume.
    Live(mpsc::Sender<AskResolution>, String),
    /// The ask survived a magpie restart (`resync_pending_asks`) with no
    /// live render task to hand off to — `ask_resolution` was never set,
    /// because there is no waiting `render_until_settled` after a restart
    /// (bamboo issue #9). The caller must resolve it inline (see
    /// [`ConnectBridge::resolve_resynced_ask`]) instead of treating this as
    /// a non-match and falling through to the normal busy/queue routing,
    /// which would silently drop the answer and start an unrelated new run
    /// on a session that is still suspended server-side.
    Resynced(ParkedAsk, String),
}

/// Strips a Telegram-style `@BotName` command suffix (`/stop@MyBot` ->
/// `/stop`) so mention-qualified commands still match in group chats.
fn strip_command_suffix(text: &str) -> &str {
    text.split('@').next().unwrap_or(text)
}

async fn reply_text(platform: &Arc<dyn Platform>, ctx: &ReplyCtx, text: impl Into<String>) {
    if let Err(error) = platform.reply(ctx, OutboundMessage::text(text)).await {
        tracing::warn!("magpie bridge: failed to send reply: {error}");
    }
}

/// Routes inbound platform messages to bamboo sessions, enforces the
/// per-platform allow-list + dedup, and serializes execution per chat behind
/// a busy lock + FIFO queue.
pub struct ConnectBridge {
    api: Arc<dyn BambooApi>,
    /// `SessionKey::as_string()` -> bamboo session id. Persisted as JSON
    /// (atomic write) so a chat's session survives a process restart.
    session_map: TokioRwLock<HashMap<String, String>>,
    map_path: Option<PathBuf>,
    chat_state: AsyncMutex<HashMap<String, ChatState>>,
    /// `platform:message_id` seen so far — dedup defense-in-depth alongside
    /// each adapter's own transport-level dedup (e.g. Telegram's offset
    /// advance). Bounded (bamboo issue #454 follow-up: see
    /// [`BoundedSeenSet`]) so it never grows without limit for the life of
    /// the process. A `std::sync::Mutex` is fine here: only ever locked for
    /// a single insert, never held across an `.await`.
    seen_message_ids: StdMutex<BoundedSeenSet>,
    process_start: DateTime<Utc>,
}

impl ConnectBridge {
    pub fn new(api: Arc<dyn BambooApi>, map_path: Option<PathBuf>) -> Self {
        Self {
            api,
            session_map: TokioRwLock::new(HashMap::new()),
            map_path,
            chat_state: AsyncMutex::new(HashMap::new()),
            seen_message_ids: StdMutex::new(BoundedSeenSet::new(DEDUP_CAPACITY)),
            process_start: Utc::now(),
        }
    }

    /// Loads the persisted chat -> session map from disk, if a `map_path`
    /// was configured. Tolerates a missing or corrupt file (starts empty,
    /// logging a warning on corruption) — a fresh/lost map degrades to
    /// "every chat starts a new session," never a hard failure.
    pub async fn load_session_map(&self) {
        let Some(path) = &self.map_path else {
            return;
        };
        match tokio::fs::read(path).await {
            Ok(bytes) => match serde_json::from_slice::<HashMap<String, String>>(&bytes) {
                Ok(map) => *self.session_map.write().await = map,
                Err(error) => {
                    tracing::warn!(
                        "magpie bridge: session map at {path:?} is corrupt, starting empty: {error}"
                    );
                }
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                tracing::warn!("magpie bridge: failed to read session map at {path:?}: {error}");
            }
        }
    }

    pub async fn session_id_for_key(&self, key: &str) -> Option<String> {
        self.session_map.read().await.get(key).cloned()
    }

    async fn set_session_id_for_key(&self, key: &str, session_id: &str) {
        {
            let mut map = self.session_map.write().await;
            map.insert(key.to_string(), session_id.to_string());
        }
        self.persist_session_map().await;
    }

    /// Rotates the chat's session mapping (`/new`). Also invalidates any
    /// parked ask first (bamboo issue #458: "`/new` and session rotation
    /// invalidate parked asks") — an ask answered after its session has been
    /// rotated away would resolve a question nobody can see anymore.
    ///
    /// Returns `true` when the caller must drain `key`'s queue afterwards
    /// (magpie issue #12: a RESYNCED parked ask — see
    /// `invalidate_pending_ask`'s doc comment) — deliberately done AFTER the
    /// session map is cleared (not inside `invalidate_pending_ask` itself) so
    /// any queued message drains against the FRESH (post-rotation) session,
    /// matching what a user typing it after `/new` would get.
    async fn rotate_session(&self, key: &str) -> bool {
        let needs_drain = self.invalidate_pending_ask(key).await;
        {
            let mut map = self.session_map.write().await;
            map.remove(key);
        }
        self.persist_session_map().await;
        needs_drain
    }

    /// Whether `key`'s chat currently has a parked ask awaiting resolution.
    async fn has_pending_ask(&self, key: &str) -> bool {
        self.chat_state
            .lock()
            .await
            .get(key)
            .is_some_and(|state| state.pending_ask.is_some())
    }

    async fn is_busy(&self, key: &str) -> bool {
        self.chat_state
            .lock()
            .await
            .get(key)
            .map(|state| state.busy)
            .unwrap_or(false)
    }

    /// If `key` has a parked ask AND `resolve` matches it, atomically clears
    /// the parked ask (so a concurrent duplicate resolution — e.g. a button
    /// press racing a text reply — finds nothing left to match) and returns
    /// what to do with the answer: hand it to the live waiting render task,
    /// or — when this ask was re-parked by `resync_pending_asks` and has no
    /// live task waiting (`ask_resolution` is `None`, bamboo issue #9) —
    /// resolve it inline. In the resync case `busy` is set `true` in this
    /// SAME lock acquisition, atomically with clearing the ask, so a message
    /// racing in immediately behind this one queues instead of starting a
    /// second concurrent run on the same session (mirrors `handle_inbound`'s
    /// own busy-then-spawn dance for a fresh message). `resolve` runs while
    /// holding the chat-state lock, so it must be cheap and non-async (pure
    /// pattern matching against the parked ask — see
    /// `approvals::match_text_answer`/`match_callback_data`).
    async fn try_resolve_pending_ask(
        &self,
        key: &str,
        resolve: impl FnOnce(&ParkedAsk) -> Option<String>,
    ) -> Option<PendingAskMatch> {
        let mut guard = self.chat_state.lock().await;
        let state = guard.get_mut(key)?;
        let ask_ref = state.pending_ask.as_ref()?;
        let answer = resolve(ask_ref)?;
        let parked = state.pending_ask.take().expect("checked Some above");
        Some(match state.ask_resolution.take() {
            Some(sender) => PendingAskMatch::Live(sender, answer),
            None => {
                state.busy = true;
                PendingAskMatch::Resynced(parked, answer)
            }
        })
    }

    /// Clears `key`'s parked ask (if any) and wakes its waiting render task
    /// with [`AskResolution::Invalidated`] instead of an answer.
    ///
    /// Returns `true` when the cleared ask had NO live render task waiting
    /// on it — the `resync_pending_asks` case (bamboo issue #9) — because
    /// such an ask never set `busy = true` (see `handle_inbound`'s
    /// pending-ask queueing, added for magpie issue #12): any message that
    /// queued up behind it while parked would otherwise sit forever with
    /// nothing to drain it. This deliberately does NOT drain the queue
    /// itself — the caller may need to finish its own state changes first
    /// (e.g. `rotate_session` clearing the session map before a queued
    /// message replays against it) and must drain from a detached task
    /// rather than blocking (mirrors `drain_chat`'s own spawn) — so it just
    /// signals the need and leaves the "when"/"how" to the caller.
    async fn invalidate_pending_ask(&self, key: &str) -> bool {
        let (had_ask, sender) = {
            let mut guard = self.chat_state.lock().await;
            match guard.get_mut(key) {
                Some(state) => (
                    state.pending_ask.take().is_some(),
                    state.ask_resolution.take(),
                ),
                None => (false, None),
            }
        };
        match sender {
            Some(sender) => {
                let _ = sender.send(AskResolution::Invalidated).await;
                false
            }
            None => had_ask,
        }
    }

    /// Clears `key`'s parked ask + resolver without sending a resolution —
    /// used once a render task has already consumed one (whether an answer
    /// or an invalidation) so a stale entry never lingers.
    async fn clear_pending_ask(&self, key: &str) {
        let mut guard = self.chat_state.lock().await;
        if let Some(state) = guard.get_mut(key) {
            state.pending_ask = None;
            state.ask_resolution = None;
        }
    }

    async fn persist_session_map(&self) {
        let Some(path) = &self.map_path else {
            return;
        };
        let snapshot = self.session_map.read().await.clone();
        let json = match serde_json::to_vec_pretty(&snapshot) {
            Ok(json) => json,
            Err(error) => {
                tracing::warn!("magpie bridge: failed to serialize session map: {error}");
                return;
            }
        };
        if let Err(error) = atomic_write(path, &json).await {
            tracing::warn!("magpie bridge: failed to persist session map at {path:?}: {error}");
        }
    }

    /// Ask resync (ARCHITECTURE.md: "on startup or Gap, optionally
    /// `client.respond_pending(session_id)` to re-park a lost ask"). A
    /// magpie restart loses every in-memory `ChatState` (including any
    /// parked ask) while `session_map` survives on disk — if the underlying
    /// bamboo session was left paused on a question, this re-fetches it via
    /// `GET /respond/{id}/pending` for every known chat key and re-parks it
    /// (bookkeeping only — no `ReplyCtx` survives a restart to re-render the
    /// ask as a fresh message, see the loop body), so a user who answers
    /// after a magpie restart still resolves the SAME question instead of
    /// the answer landing nowhere. This deliberately re-parks with NO
    /// `ChatState::ask_resolution` sender (there is no live
    /// `render_until_settled` task after a restart to be one) —
    /// `try_resolve_pending_ask`/`handle_inbound`/`handle_callback` detect
    /// that (`PendingAskMatch::Resynced`) and resolve a matching answer
    /// inline via `resolve_resynced_ask` instead of falling through to the
    /// normal busy/queue routing and starting an unrelated new run (bamboo
    /// issue #9). Best-effort: a chat whose session has no pending question
    /// (the common case) or whose `respond_pending` call errors is silently
    /// skipped.
    pub async fn resync_pending_asks(
        self: &Arc<Self>,
        platforms: &HashMap<String, Arc<dyn Platform>>,
    ) {
        let snapshot = self.session_map.read().await.clone();
        for (key, session_id) in snapshot {
            let Some(platform_name) = key.split(':').next() else {
                continue;
            };
            let Some(platform) = platforms.get(platform_name).cloned() else {
                continue;
            };
            if self.has_pending_ask(&key).await {
                continue;
            }
            let pending = match self.api.respond_pending(&session_id).await {
                Ok(pending) => pending,
                Err(error) => {
                    tracing::debug!(
                        "magpie bridge: ask resync for {key} ({session_id}) failed, skipping: {error}"
                    );
                    continue;
                }
            };
            if !pending.has_pending_question {
                continue;
            }
            let ask = render::PendingAsk {
                tool_call_id: pending.tool_call_id.unwrap_or_default(),
                tool_name: pending.tool_name.unwrap_or_default(),
                question: pending.question.unwrap_or_default(),
                options: pending.options.unwrap_or_default(),
                allow_custom: pending.allow_custom.unwrap_or(true),
            };
            let parked = ParkedAsk::new(approvals::new_nonce(), session_id.clone(), &ask);
            // Best-effort chat_id recovery: chat_id is the middle segment of
            // `platform:chat_id:user_id` — reconstructing a `ReplyCtx` here
            // is platform-specific (each adapter decides its own opaque
            // shape), so this resync only re-parks the bookkeeping; it does
            // NOT re-render the ask as a fresh outbound message (no
            // `ReplyCtx` is recoverable from the session map alone), and
            // leaves `ask_resolution` unset (there is no live render task to
            // own it yet). The user's next message to the chat still
            // resolves it correctly via the ask-fast-path in
            // `handle_inbound`/`handle_callback` — a match against this
            // ask, with `ask_resolution` still `None`, now takes the
            // `PendingAskMatch::Resynced` path and resolves inline using
            // the ANSWERING message's own `ReplyCtx` (see
            // `resolve_resynced_ask`) — they just won't see a repeated
            // prompt after a restart.
            let mut guard = self.chat_state.lock().await;
            let state = guard.entry(key.clone()).or_default();
            state.pending_ask = Some(parked);
            drop(guard);
            let _ = &platform; // platform kept for a future re-render enhancement.
        }
    }

    /// Spawns a detached task to drain `key`'s queue — used after
    /// invalidating a RESYNCED parked ask (magpie issue #12:
    /// `invalidate_pending_ask` returned `true`, meaning no live render task
    /// ever set `busy = true` for it, so nothing else will drain whatever
    /// queued up behind it while parked). Detached rather than awaited
    /// inline so the (potentially long-running) drain never blocks the
    /// caller — `handle_inbound` must return quickly, see its own doc
    /// comment.
    fn spawn_drain_queue(self: &Arc<Self>, key: &str) {
        let bridge = self.clone();
        let key = key.to_string();
        tokio::spawn(async move {
            bridge.drain_queue(&key).await;
        });
    }

    /// Entry point for every inbound platform message. Enforces allow-list +
    /// dedup, answers `/stop` and `/status` immediately (bypassing the busy
    /// queue — a queued `/stop` could never reach a busy chat), and otherwise
    /// either runs the message right away or queues it behind the chat's
    /// current run.
    ///
    /// Takes `self: Arc<Self>` (not `&self`) so it can hand the bridge off to
    /// a detached `tokio::spawn` for the actual (potentially long-running)
    /// execution — this method itself returns as soon as the message is
    /// either answered inline or queued, so one slow chat can never block
    /// another chat's inbound dispatch loop.
    pub async fn handle_inbound(
        self: Arc<Self>,
        platform: Arc<dyn Platform>,
        allow_from: Vec<String>,
        msg: InboundMessage,
    ) {
        if !allow_from.iter().any(|allowed| allowed == &msg.user_id) {
            tracing::warn!(
                platform = %msg.platform,
                chat_id = %msg.chat_id,
                user_id = %msg.user_id,
                "magpie bridge: rejected inbound message — user not in allow_from"
            );
            return;
        }

        if msg.sent_at < self.process_start {
            tracing::debug!(
                platform = %msg.platform,
                message_id = %msg.message_id,
                "magpie bridge: dropping message older than process start"
            );
            return;
        }

        let dedup_key = format!("{}:{}", msg.platform, msg.message_id);
        {
            let mut seen = self.seen_message_ids.lock().unwrap();
            if !seen.insert(dedup_key) {
                tracing::debug!(
                    platform = %msg.platform,
                    message_id = %msg.message_id,
                    "magpie bridge: dropping duplicate message_id"
                );
                return;
            }
        }

        let key = SessionKey {
            platform: msg.platform.clone(),
            chat_id: msg.chat_id.clone(),
            user_id: msg.user_id.clone(),
        }
        .as_string();

        let command = strip_command_suffix(msg.text.trim());
        if command.eq_ignore_ascii_case("/stop") {
            if self.handle_stop(&key, &platform, &msg.reply_ctx).await {
                self.spawn_drain_queue(&key);
            }
            return;
        }
        if command.eq_ignore_ascii_case("/status") {
            self.handle_status(&key, &platform, &msg.reply_ctx).await;
            return;
        }

        // Ask-resolution fast path (bamboo issue #458): a parked ask takes
        // priority over normal busy/queue routing, even while `busy` is
        // still true — the run backing it is genuinely suspended waiting for
        // exactly this reply, so it must never sit behind the FIFO queue. A
        // non-matching reply on a CLOSED ask (no free text allowed) falls
        // through to the normal busy/queue handling below, exactly like any
        // other message.
        match self
            .try_resolve_pending_ask(&key, |ask| approvals::match_text_answer(ask, &msg.text))
            .await
        {
            Some(PendingAskMatch::Live(sender, answer)) => {
                let _ = sender.send(AskResolution::Answer(answer)).await;
                return;
            }
            // Resynced ask (issue #9): no live render task to hand this to
            // — resolve it inline instead of letting it fall through to a
            // new run.
            Some(PendingAskMatch::Resynced(parked, answer)) => {
                let bridge = self.clone();
                let reply_ctx = msg.reply_ctx.clone();
                tokio::spawn(async move {
                    bridge
                        .resolve_resynced_ask(key, platform, reply_ctx, parked, answer)
                        .await;
                });
                return;
            }
            None => {}
        }

        // `/new` is always an immediate escape hatch out of a parked ask
        // (bypassing the queue, which would never drain while the chat waits
        // on an answer nobody typed correctly) — the ordinary `/new` path in
        // `process_one` still handles the non-paused case unchanged.
        if command.eq_ignore_ascii_case("/new") && self.has_pending_ask(&key).await {
            let needs_drain = self.rotate_session(&key).await;
            reply_text(&platform, &msg.reply_ctx, "Started a new session.").await;
            if needs_drain {
                self.spawn_drain_queue(&key);
            }
            return;
        }

        // magpie issue #12: a parked ask — whether a LIVE one (`busy` is
        // already `true` for its whole paused-and-waiting window, see
        // `render_until_settled`) or a RESYNCED one with no live render task
        // yet (`resync_pending_asks`, bamboo issue #9 — `busy` is still
        // `false` here) — means the underlying session is suspended
        // server-side. A message that reaches this point already failed to
        // match the parked ask above, so it is UNRELATED to it: queue it
        // instead of starting a second concurrent run on a session that's
        // still waiting on an answer. `try_resolve_pending_ask`'s answer path
        // and `invalidate_pending_ask`'s `/stop`/`/new` paths are what
        // eventually drain this queue (see their doc comments).
        let mut guard = self.chat_state.lock().await;
        let state = guard.entry(key.clone()).or_default();
        if state.busy || state.pending_ask.is_some() {
            state.queue.push_back((platform, msg));
            return;
        }
        state.busy = true;
        drop(guard);

        let bridge = self.clone();
        tokio::spawn(async move {
            bridge.drain_chat(key, platform, msg).await;
        });
    }

    /// Processes `msg`, then keeps draining `chat_state`'s queue for `key`
    /// (FIFO) until it is empty, at which point the chat is marked idle
    /// again. Runs in its own spawned task (see [`Self::handle_inbound`]).
    async fn drain_chat(
        self: Arc<Self>,
        key: String,
        platform: Arc<dyn Platform>,
        msg: InboundMessage,
    ) {
        self.process_one(&key, platform, msg).await;
        self.drain_queue(&key).await;
    }

    /// Pops and processes `key`'s queued messages (FIFO) until the queue is
    /// empty, at which point the chat is marked idle (`busy = false`)
    /// again. The shared wind-down tail of both [`Self::drain_chat`] (a
    /// freshly-started run finishing) and [`Self::resolve_resynced_ask`] (a
    /// resumed post-restart run finishing, bamboo issue #9) — whichever one
    /// set `busy = true`, the queue it may have backed up behind must still
    /// get drained here rather than left orphaned.
    async fn drain_queue(&self, key: &str) {
        loop {
            let next = {
                let mut guard = self.chat_state.lock().await;
                match guard.get_mut(key) {
                    Some(state) => match state.queue.pop_front() {
                        Some(item) => Some(item),
                        None => {
                            state.busy = false;
                            None
                        }
                    },
                    None => None,
                }
            };

            match next {
                Some((platform, msg)) => self.process_one(key, platform, msg).await,
                None => break,
            }
        }
    }

    /// Entry point for every inbound button-press callback (bamboo issue
    /// #458). Unlike text messages, a callback NEVER queues and NEVER starts
    /// a run — it can only ever resolve (or fail to resolve) the chat's
    /// parked ask. Per the design constraint, the platform is ALWAYS acked
    /// (`answer_callback`), even for a stale/forged/non-matching one, and a
    /// non-match is dropped silently rather than ever being forwarded as
    /// user text.
    pub async fn handle_callback(
        self: Arc<Self>,
        platform: Arc<dyn Platform>,
        allow_from: Vec<String>,
        callback: CallbackQuery,
    ) {
        if !allow_from
            .iter()
            .any(|allowed| allowed == &callback.user_id)
        {
            tracing::warn!(
                platform = %callback.platform,
                chat_id = %callback.chat_id,
                user_id = %callback.user_id,
                "magpie bridge: rejected callback query — user not in allow_from"
            );
            let _ = platform
                .answer_callback(&callback.callback_query_id, None)
                .await;
            return;
        }

        let key = SessionKey {
            platform: callback.platform.clone(),
            chat_id: callback.chat_id.clone(),
            user_id: callback.user_id.clone(),
        }
        .as_string();

        let resolved = self
            .try_resolve_pending_ask(&key, |ask| {
                approvals::match_callback_data(ask, &callback.data)
            })
            .await;

        match resolved {
            Some(PendingAskMatch::Live(sender, answer)) => {
                let _ = platform
                    .answer_callback(&callback.callback_query_id, None)
                    .await;
                let _ = sender.send(AskResolution::Answer(answer)).await;
            }
            // Resynced ask (issue #9): no live render task to hand this to
            // — resolve it inline instead of dropping the press. In
            // practice a resync-parked ask's nonce is freshly regenerated
            // (never re-rendered as a message, so no stale pre-restart
            // button can ever match it) — this arm mainly gives
            // `PendingAskMatch` one consistent handler on both call sites.
            Some(PendingAskMatch::Resynced(parked, answer)) => {
                let _ = platform
                    .answer_callback(&callback.callback_query_id, None)
                    .await;
                let bridge = self.clone();
                let reply_ctx = callback.reply_ctx.clone();
                tokio::spawn(async move {
                    bridge
                        .resolve_resynced_ask(key, platform, reply_ctx, parked, answer)
                        .await;
                });
            }
            None => {
                tracing::debug!(
                    platform = %callback.platform,
                    chat_id = %callback.chat_id,
                    "magpie bridge: dropping stale/forged callback_data"
                );
                let _ = platform
                    .answer_callback(
                        &callback.callback_query_id,
                        Some("This action has expired."),
                    )
                    .await;
            }
        }
    }

    async fn process_one(&self, key: &str, platform: Arc<dyn Platform>, msg: InboundMessage) {
        let command = strip_command_suffix(msg.text.trim());
        if command.eq_ignore_ascii_case("/new") {
            // A message only ever reaches `process_one` via `drain_chat`
            // (the message that started this very run) or `drain_queue`
            // (something that queued behind it) — either way,
            // `handle_inbound`'s own `/new`-while-parked fast path already
            // intercepts a `/new` sent while a pending ask exists, before it
            // ever reaches the queue (see that fast path's doc comment). So
            // `rotate_session`'s `needs_drain` is always `false` here: there
            // is nothing left for it to have invalidated.
            let _needs_drain = self.rotate_session(key).await;
            debug_assert!(
                !_needs_drain,
                "a /new popped off the queue should never find a parked ask left to invalidate"
            );
            reply_text(&platform, &msg.reply_ctx, "Started a new session.").await;
            return;
        }

        let text = msg.text.trim();
        if text.is_empty() {
            return;
        }

        self.run_prompt(key, platform, &msg.reply_ctx, text).await;
    }

    /// `/stop`: if the chat is currently busy (a run is executing OR paused
    /// on a parked ask — both count as "busy" for the whole duration a
    /// `run_prompt` call is in flight, see [`Self::render_until_settled`]) OR
    /// has a parked ask that a magpie restart re-parked WITHOUT ever setting
    /// `busy` (`resync_pending_asks`, bamboo issue #9 / magpie issue #14 —
    /// such an ask has no live render task holding the session "busy", so
    /// `busy` alone under-reports it), tell the server to cancel the session
    /// via [`BambooApi::stop`] — harmless to call even if the session is
    /// only paused server-side (the server is the authority on whether
    /// there's anything to actually cancel).
    ///
    /// The server cancel is issued BEFORE any local state
    /// (`invalidate_pending_ask`) is cleared: `session_id_for_key` is read
    /// up front (unaffected by ask invalidation — only session rotation
    /// removes it, and `/stop` never rotates), then `api.stop` is awaited,
    /// and only THEN is the local ask cleared. If `api.stop` fails, the local
    /// pending-ask is still cleared afterwards (best-effort/log-only, same
    /// as the pre-existing busy-path precedent) rather than leaving the chat
    /// stuck reporting a stale parked ask — but clearing only happens after
    /// the cancel attempt so a slow/failed call never races a queued message
    /// into starting a second concurrent run on the same still-live session.
    ///
    /// Port note: bamboo's in-proc version distinguishes "a live task's
    /// `cancel_token` exists" from "only a parked ask, no live task" (a
    /// paused round has no in-proc task to cancel). Magpie has no local
    /// cancel token — `busy || pending_ask.is_some()` is what gates whether
    /// `/stop` calls the API, collapsing both in-proc cases into one magpie
    /// case (plus the resync-parked case, which is unique to magpie's
    /// restart-survives-the-session-map design); see the final report's
    /// judgment-call notes.
    ///
    /// Returns `true` when the caller must drain `key`'s queue afterwards
    /// (magpie issue #12: `invalidate_pending_ask` found a RESYNCED ask —
    /// see its doc comment).
    async fn handle_stop(
        &self,
        key: &str,
        platform: &Arc<dyn Platform>,
        reply_ctx: &ReplyCtx,
    ) -> bool {
        let had_pending_ask = self.has_pending_ask(key).await;
        let busy = self.is_busy(key).await;
        let session_id = self.session_id_for_key(key).await;

        // magpie issue #14: gate the server-side cancel on `busy` OR a
        // parked ask, not `busy` alone — a resync-parked ask never sets
        // `busy` (see `resync_pending_asks`'s doc comment) but still
        // corresponds to a live, server-side-suspended session that must be
        // told to cancel, not just forgotten locally.
        if busy || had_pending_ask {
            if let Some(session_id) = &session_id {
                if let Err(error) = self.api.stop(session_id).await {
                    tracing::warn!("magpie bridge: failed to stop session {session_id}: {error}");
                }
            }
        }

        // Only clear local state (and hand the caller a drain obligation)
        // AFTER the server has been asked to cancel — see this fn's doc
        // comment on ordering.
        let needs_drain = if had_pending_ask {
            self.invalidate_pending_ask(key).await
        } else {
            false
        };

        match (busy, session_id) {
            (true, Some(_)) => {
                reply_text(platform, reply_ctx, "Stopping the current run…").await;
            }
            _ if had_pending_ask => {
                reply_text(
                    platform,
                    reply_ctx,
                    "Stopped — the pending question was cancelled.",
                )
                .await;
            }
            _ => {
                reply_text(platform, reply_ctx, "Nothing is running.").await;
            }
        }
        needs_drain
    }

    async fn handle_status(&self, key: &str, platform: &Arc<dyn Platform>, reply_ctx: &ReplyCtx) {
        let session_id = self.session_id_for_key(key).await;
        let busy = self.is_busy(key).await;
        let text = match session_id {
            Some(id) => format!(
                "Session: {id}\nStatus: {}",
                if busy { "busy" } else { "idle" }
            ),
            None => "No session yet. Send a message to start one.".to_string(),
        };
        reply_text(platform, reply_ctx, text).await;
    }

    /// Runs `text` as a prompt for `key`'s session: `POST /chat` (creating a
    /// session server-side when `key` has none mapped yet) then
    /// `POST /execute/{id}` — see the module doc's mapping table. Subscribes
    /// to the session's `/v2/stream` channel BEFORE calling `execute`
    /// (ARCHITECTURE.md's documented ordering), so the very first events of a
    /// freshly-started run are never missed.
    ///
    /// Awaited inline by the caller (not detached) so the run's completion
    /// IS this call's completion — that is what lets [`Self::drain_chat`]
    /// serialize one run at a time per chat.
    async fn run_prompt(
        &self,
        key: &str,
        platform: Arc<dyn Platform>,
        reply_ctx: &ReplyCtx,
        text: &str,
    ) {
        let existing_id = self.session_id_for_key(key).await;
        let chat_request = ChatRequest {
            message: text.to_string(),
            session_id: existing_id.clone(),
            ..Default::default()
        };
        let chat_response = match self.api.chat(chat_request).await {
            Ok(response) => response,
            Err(error) => {
                reply_text(
                    &platform,
                    reply_ctx,
                    format!("Failed to send your message to bamboo: {error}"),
                )
                .await;
                return;
            }
        };
        let session_id = chat_response.session_id;
        if existing_id.as_deref() != Some(session_id.as_str()) {
            self.set_session_id_for_key(key, &session_id).await;
        }

        let rx = match self.api.subscribe_session(&session_id).await {
            Ok(rx) => rx,
            Err(error) => {
                reply_text(
                    &platform,
                    reply_ctx,
                    format!("Failed to subscribe to the run: {error}"),
                )
                .await;
                return;
            }
        };

        // `no_human_approver: false` — a magpie-bridged chat HAS a human
        // attached (this bridge, relaying questions to and from the chat
        // platform), so gated actions/pending questions should escalate
        // normally rather than being tagged "no interactive approver
        // available" (mirrors bamboo's `create_connect_session` doc comment).
        let execute_request = ExecuteRequest {
            no_human_approver: false,
            ..Default::default()
        };
        match self.api.execute(&session_id, execute_request).await {
            // Whether freshly started or already running server-side
            // (`AlreadyRunning` — e.g. a magpie restart raced a still-live
            // run), the subscription above is already live on
            // `agent.{session_id}` — either way we watch the SAME channel
            // through to a terminal event. JUDGMENT CALL: no separate
            // "queue and retry" handling for `AlreadyRunning` is needed
            // because subscribing before executing already guarantees we
            // observe whatever is/becomes live on that channel.
            Ok(_response) => {}
            Err(error) => {
                reply_text(
                    &platform,
                    reply_ctx,
                    format!("Failed to start the run: {error}"),
                )
                .await;
                return;
            }
        }

        self.render_until_settled(key, platform, reply_ctx.clone(), &session_id, rx)
            .await;
    }

    /// Renders one run to completion, looping back for as many
    /// pause/answer/resume cycles as it takes to reach a genuinely terminal
    /// state (bamboo issue #458). On [`render::RunOutcome::Paused`]: parks
    /// the ask, renders it (buttons when the platform supports them, always
    /// also a numbered text list), and waits for a resolution pushed by
    /// `handle_inbound`'s ask-resolution fast path or `handle_callback` — or
    /// an invalidation from `/new`/rotation/`/stop`.
    ///
    /// A resolved answer is submitted via `POST /respond/{id}`
    /// ([`BambooApi::respond`]) — the server performs the grant + resume
    /// server-side (see the module doc's mapping table: this is what
    /// replaces bamboo's in-proc `Responder::respond_and_resume`). Per
    /// ARCHITECTURE.md, the bridge then re-subscribes to the session's WS
    /// channel and keeps rendering with the fresh receiver — together with
    /// the streaming renderer's carried-over [`render::StreamState`] — so the
    /// SAME chat keeps watching the SAME (now-continuing) run in the SAME
    /// status message (one "⏳ Working…" bubble no matter how many times it
    /// pauses).
    async fn render_until_settled(
        &self,
        key: &str,
        platform: Arc<dyn Platform>,
        reply_ctx: ReplyCtx,
        session_id: &str,
        mut rx: mpsc::Receiver<StreamEvent>,
    ) {
        let mut stream_state: Option<Box<render::StreamState>> = None;
        loop {
            match render::stream_execution(
                platform.clone(),
                reply_ctx.clone(),
                rx,
                stream_state.take(),
            )
            .await
            {
                render::RunOutcome::Terminal => return,
                render::RunOutcome::Paused {
                    ask,
                    stream_state: paused_state,
                } => {
                    stream_state = paused_state;
                    let caps = platform.capabilities();
                    let parked =
                        ParkedAsk::new(approvals::new_nonce(), session_id.to_string(), &ask);
                    let ask_text = approvals::format_ask_text(&parked);

                    let ask_ref =
                        match approvals::render_ask(&platform, &reply_ctx, &parked, caps.buttons)
                            .await
                        {
                            Ok(msg_ref) => Some(msg_ref),
                            Err(error) => {
                                tracing::warn!(
                                    "magpie bridge: failed to render pending ask: {error}"
                                );
                                None
                            }
                        };

                    let (ask_tx, mut ask_rx) = mpsc::channel(1);
                    {
                        let mut guard = self.chat_state.lock().await;
                        let state = guard.entry(key.to_string()).or_default();
                        state.pending_ask = Some(parked);
                        state.ask_resolution = Some(ask_tx);
                    }

                    match ask_rx.recv().await {
                        Some(AskResolution::Answer(answer)) => {
                            let answered_ask =
                                ask_ref.as_ref().map(|msg_ref| (msg_ref, ask_text.as_str()));
                            match self
                                .respond_and_resubscribe(
                                    &platform,
                                    &reply_ctx,
                                    session_id,
                                    &answer,
                                    answered_ask,
                                )
                                .await
                            {
                                Some(new_rx) => {
                                    rx = new_rx;
                                    continue;
                                }
                                None => return,
                            }
                        }
                        Some(AskResolution::Invalidated) | None => {
                            // Already cleared by the invalidator in the
                            // common case; clear defensively so a stale entry
                            // never lingers if the sender was dropped instead
                            // (e.g. a bug elsewhere) rather than sending
                            // `Invalidated` explicitly.
                            self.clear_pending_ask(key).await;
                            return;
                        }
                    }
                }
            }
        }
    }

    /// Submits `answer` for `session_id`'s parked ask and returns the fresh
    /// receiver to keep watching the (resumed) run on — the shared core of
    /// resolving an ask, used both by [`Self::render_until_settled`]'s live
    /// pause branch and by [`Self::resolve_resynced_ask`] (bamboo issue #9).
    ///
    /// Subscribes BEFORE responding — the same subscribe-before-execute
    /// invariant the initial run relies on (ARCHITECTURE.md). A server-side
    /// resubscribe REPLACES the channel's forwarder with a fresh broadcast
    /// cut, so subscribing after `respond` leaves a window where the resumed
    /// run's events — or, if the WS is mid-reconnect while the Subscribe
    /// command waits, the entire run through `Complete` — are emitted into
    /// no subscription at all; the render task would then wait forever on an
    /// idle channel and the final reply never renders (issue #6). A failed
    /// subscribe still records the answer below (the old degradation path):
    /// dropping the user's decision is strictly worse than rendering
    /// nothing.
    ///
    /// `answered_ask`, when `Some((msg_ref, ask_text))`, marks that
    /// previously-rendered ask message answered — ✅ + the chosen answer,
    /// buttons dropped (an edit replaces the whole message body) — so stale
    /// buttons can't be pressed again and the chat shows WHAT was chosen.
    /// Best-effort: an edit failure never fails the resume. `None` (the
    /// resync case: no ask message was ever re-rendered after a restart, so
    /// there is nothing to edit) simply skips this step.
    ///
    /// On any failure an explicit reply is already sent to the chat and
    /// `None` is returned — callers must stop there, never silently fall
    /// through to treating the failure as "start a new run".
    async fn respond_and_resubscribe(
        &self,
        platform: &Arc<dyn Platform>,
        reply_ctx: &ReplyCtx,
        session_id: &str,
        answer: &str,
        answered_ask: Option<(&MessageRef, &str)>,
    ) -> Option<mpsc::Receiver<StreamEvent>> {
        let new_rx = self.api.subscribe_session(session_id).await;
        let respond_request = RespondRequest {
            response: answer.to_string(),
            ..Default::default()
        };
        match self.api.respond(session_id, respond_request).await {
            Ok(_response) => {
                if platform.capabilities().edit_message {
                    if let Some((msg_ref, ask_text)) = answered_ask {
                        let done = format!("{ask_text}\n\n✅ {answer}");
                        if let Err(error) =
                            platform.edit(msg_ref, OutboundMessage::text(done)).await
                        {
                            tracing::debug!(
                                "magpie bridge: answered-ask edit failed (non-fatal): {error}"
                            );
                        }
                    }
                }
                match new_rx {
                    Ok(new_rx) => Some(new_rx),
                    Err(error) => {
                        reply_text(
                            platform,
                            reply_ctx,
                            format!(
                                "Answer recorded, but failed to resume watching the run: {error}"
                            ),
                        )
                        .await;
                        None
                    }
                }
            }
            Err(error) => {
                reply_text(
                    platform,
                    reply_ctx,
                    format!("Failed to record your answer: {error}"),
                )
                .await;
                None
            }
        }
    }

    /// Resolves an answer to a RESYNCED ask (bamboo issue #9):
    /// `resync_pending_asks` re-parks a pending question after a restart
    /// with no live `render_until_settled` task waiting on it — nothing to
    /// hand the answer to via `ChatState::ask_resolution`. This runs the
    /// same subscribe-before-respond resume [`Self::respond_and_resubscribe`]
    /// uses on the live path, then keeps watching the resumed run through
    /// [`Self::render_until_settled`] (looping for as many further
    /// pause/answer cycles as it takes to reach a terminal state, exactly
    /// like any other run) using the ANSWERING message's `reply_ctx` —
    /// resync itself has no `ReplyCtx` to recover (see
    /// `resync_pending_asks`'s doc comment), but the message that answers
    /// the question always carries a fresh one. There is no `ask_ref` to
    /// edit (resync never re-rendered the ask as a message), so the ✅-edit
    /// step is skipped — there is nothing stale left visible to mark
    /// answered.
    ///
    /// `try_resolve_pending_ask` already set `busy = true` for this chat
    /// atomically with matching the answer (see its doc comment); this
    /// drains the chat's queue at the end exactly like [`Self::drain_chat`]
    /// does, so `busy` is correctly cleared and anything that queued up
    /// behind this resolution still gets processed.
    async fn resolve_resynced_ask(
        self: Arc<Self>,
        key: String,
        platform: Arc<dyn Platform>,
        reply_ctx: ReplyCtx,
        parked: ParkedAsk,
        answer: String,
    ) {
        let session_id = parked.session_id.clone();
        if let Some(rx) = self
            .respond_and_resubscribe(&platform, &reply_ctx, &session_id, &answer, None)
            .await
        {
            self.render_until_settled(&key, platform, reply_ctx, &session_id, rx)
                .await;
        }
        self.drain_queue(&key).await;
    }

    /// Best-effort operational visibility for a graceful shutdown (magpie
    /// issue #12): `ChatState` — a chat's parked ask and its queued
    /// messages — lives only in memory (unlike `session_map`, which is
    /// persisted to disk), so any chat still parked and/or backed up when
    /// the process exits loses that state with nothing to recover it on the
    /// next start beyond `resync_pending_asks` re-parking the ask itself
    /// (which does NOT recover queued messages — see its doc comment). This
    /// can't save them; it just logs a warning per affected chat so an
    /// operator sees why a user's message never got a reply instead of it
    /// silently vanishing. Call once, right before exiting.
    pub async fn log_backlog_on_shutdown(&self) {
        let guard = self.chat_state.lock().await;
        for (key, state) in guard.iter() {
            if state.pending_ask.is_none() && state.queue.is_empty() {
                continue;
            }
            tracing::warn!(
                chat = %key,
                has_pending_ask = state.pending_ask.is_some(),
                queued_messages = state.queue.len(),
                "magpie bridge: shutting down with a parked ask and/or queued messages still \
                 outstanding for this chat — queued messages will be lost (a parked ask itself \
                 survives restart via resync_pending_asks, but nothing queued behind it does)"
            );
        }
    }
}

/// Writes `bytes` to `path` atomically: temp file in the same directory,
/// fsync, rename over the target. Mirrors bamboo's
/// `handlers::settings::bamboo_config::config_endpoints::common::atomic_write`
/// (private there) so a crash mid-write leaves the old session map intact.
async fn atomic_write(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let tmp = path.with_extension(format!("tmp.{}", uuid::Uuid::new_v4()));
    {
        let mut file = tokio::fs::File::create(&tmp).await?;
        tokio::io::AsyncWriteExt::write_all(&mut file, bytes).await?;
        file.sync_all().await?;
    }
    tokio::fs::rename(&tmp, path).await?;
    if let Some(parent) = path.parent() {
        if let Ok(dir) = tokio::fs::File::open(parent).await {
            let _ = dir.sync_all().await;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bamboo::types::TokenUsage;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::Mutex as TokioMutex;

    /// mpsc-backed fake `Platform` (per bamboo issue #452's test spec):
    /// records every `reply()`/`edit()`/`answer_callback()` call instead of
    /// talking to a real IM API. `capabilities` is configurable (bamboo issue
    /// #458 tests need buttons+edit_message; the original #452 tests want
    /// the all-`false` default).
    struct FakePlatform {
        label: String,
        capabilities: crate::platform::Capabilities,
        sent: TokioMutex<Vec<String>>,
        edits: TokioMutex<Vec<String>>,
        answered_callbacks: TokioMutex<Vec<(String, Option<String>)>>,
    }

    impl FakePlatform {
        fn new(label: &str) -> Arc<Self> {
            Self::with_capabilities(label, Default::default())
        }

        fn with_capabilities(
            label: &str,
            capabilities: crate::platform::Capabilities,
        ) -> Arc<Self> {
            Arc::new(Self {
                label: label.to_string(),
                capabilities,
                sent: TokioMutex::new(Vec::new()),
                edits: TokioMutex::new(Vec::new()),
                answered_callbacks: TokioMutex::new(Vec::new()),
            })
        }

        async fn sent_texts(&self) -> Vec<String> {
            self.sent.lock().await.clone()
        }
    }

    #[async_trait::async_trait]
    impl Platform for FakePlatform {
        fn name(&self) -> &str {
            &self.label
        }
        fn capabilities(&self) -> crate::platform::Capabilities {
            self.capabilities
        }
        async fn start(
            &self,
            _inbound: mpsc::Sender<crate::platform::Inbound>,
        ) -> crate::platform::PlatformResult<()> {
            Ok(())
        }
        async fn reply(
            &self,
            _ctx: &ReplyCtx,
            msg: OutboundMessage,
        ) -> crate::platform::PlatformResult<crate::platform::MessageRef> {
            self.sent.lock().await.push(msg.text);
            Ok(crate::platform::MessageRef(serde_json::Value::Null))
        }
        async fn edit(
            &self,
            _msg_ref: &crate::platform::MessageRef,
            new: OutboundMessage,
        ) -> crate::platform::PlatformResult<()> {
            self.edits.lock().await.push(new.text);
            Ok(())
        }
        async fn answer_callback(
            &self,
            callback_query_id: &str,
            text: Option<&str>,
        ) -> crate::platform::PlatformResult<()> {
            self.answered_callbacks
                .lock()
                .await
                .push((callback_query_id.to_string(), text.map(str::to_string)));
            Ok(())
        }
        async fn stop(&self) -> crate::platform::PlatformResult<()> {
            Ok(())
        }
    }

    /// Fake [`BambooApi`]: records every call, hands back deterministic
    /// synthetic session ids (`sess-N`) for a fresh `chat` call, and lets a
    /// test grab the live `mpsc::Sender<StreamEvent>` for any session id it
    /// has subscribed (`sender_for`) to drive a run's event stream directly —
    /// the mpsc-channel counterpart of bridge.rs's source using a
    /// test-controlled `broadcast::Sender`.
    struct FakeBambooApi {
        chat_calls: TokioMutex<Vec<ChatRequest>>,
        execute_calls: TokioMutex<Vec<String>>,
        stop_calls: TokioMutex<Vec<String>>,
        respond_calls: TokioMutex<Vec<(String, String)>>,
        respond_pending_calls: TokioMutex<Vec<String>>,
        subscribe_calls: TokioMutex<Vec<String>>,
        /// Interleaved `"subscribe"`/`"respond"` markers in true call order —
        /// the per-method vecs above can't express cross-method ordering,
        /// which the resume path's subscribe-BEFORE-respond invariant
        /// (issue #6) needs asserted.
        ops: TokioMutex<Vec<&'static str>>,
        channels: TokioMutex<HashMap<String, mpsc::Sender<StreamEvent>>>,
        next_id: AtomicUsize,
        chat_error: Option<String>,
        respond_error: Option<String>,
        /// Canned `respond_pending` response (bamboo issue #9 tests):
        /// `resync_pending_asks` reads this to re-park an ask as if a
        /// restart had just happened. `None` (the default) means "no
        /// pending question" for every session id, matching every other
        /// existing test's assumption that resync is a no-op.
        resync_pending: TokioMutex<Option<RespondPendingResponse>>,
    }

    impl FakeBambooApi {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                chat_calls: TokioMutex::new(Vec::new()),
                execute_calls: TokioMutex::new(Vec::new()),
                stop_calls: TokioMutex::new(Vec::new()),
                respond_calls: TokioMutex::new(Vec::new()),
                respond_pending_calls: TokioMutex::new(Vec::new()),
                subscribe_calls: TokioMutex::new(Vec::new()),
                ops: TokioMutex::new(Vec::new()),
                channels: TokioMutex::new(HashMap::new()),
                next_id: AtomicUsize::new(1),
                chat_error: None,
                respond_error: None,
                resync_pending: TokioMutex::new(None),
            })
        }

        fn respond_failing(reason: &str) -> Arc<Self> {
            Arc::new(Self {
                chat_calls: TokioMutex::new(Vec::new()),
                execute_calls: TokioMutex::new(Vec::new()),
                stop_calls: TokioMutex::new(Vec::new()),
                respond_calls: TokioMutex::new(Vec::new()),
                respond_pending_calls: TokioMutex::new(Vec::new()),
                subscribe_calls: TokioMutex::new(Vec::new()),
                ops: TokioMutex::new(Vec::new()),
                channels: TokioMutex::new(HashMap::new()),
                next_id: AtomicUsize::new(1),
                chat_error: None,
                respond_error: Some(reason.to_string()),
                resync_pending: TokioMutex::new(None),
            })
        }

        /// Makes the next (and every subsequent) `respond_pending` call
        /// return `response`, as if that session were left paused on a
        /// question across a restart.
        async fn set_resync_pending(&self, response: RespondPendingResponse) {
            *self.resync_pending.lock().await = Some(response);
        }

        /// Fetch the currently-live sender for `session_id` (panics if no
        /// `subscribe_session` call has happened yet for it) — used by tests
        /// to push `StreamEvent`s directly onto a run in flight.
        async fn sender_for(&self, session_id: &str) -> mpsc::Sender<StreamEvent> {
            self.channels
                .lock()
                .await
                .get(session_id)
                .cloned()
                .unwrap_or_else(|| panic!("no live subscription for session {session_id}"))
        }
    }

    #[async_trait::async_trait]
    impl BambooApi for FakeBambooApi {
        async fn chat(&self, request: ChatRequest) -> Result<ChatResponse, ClientError> {
            self.chat_calls.lock().await.push(request.clone());
            if let Some(message) = &self.chat_error {
                return Err(ClientError::Api {
                    method: "POST",
                    path: "/api/v1/chat".to_string(),
                    status: 500,
                    body: message.clone(),
                });
            }
            let session_id = request
                .session_id
                .clone()
                .unwrap_or_else(|| format!("sess-{}", self.next_id.fetch_add(1, Ordering::SeqCst)));
            Ok(ChatResponse {
                session_id,
                stream_url: String::new(),
                status: "queued".to_string(),
                goal_command: None,
            })
        }

        async fn execute(
            &self,
            session_id: &str,
            _request: ExecuteRequest,
        ) -> Result<ExecuteResponse, ClientError> {
            self.execute_calls.lock().await.push(session_id.to_string());
            Ok(ExecuteResponse {
                session_id: session_id.to_string(),
                status: "started".to_string(),
                events_url: String::new(),
                sync: None,
                run_id: None,
            })
        }

        async fn stop(&self, session_id: &str) -> Result<StopResponse, ClientError> {
            self.stop_calls.lock().await.push(session_id.to_string());
            Ok(StopResponse {
                success: true,
                message: "stopped".to_string(),
            })
        }

        async fn respond(
            &self,
            session_id: &str,
            request: RespondRequest,
        ) -> Result<RespondSubmitResponse, ClientError> {
            self.respond_calls
                .lock()
                .await
                .push((session_id.to_string(), request.response.clone()));
            self.ops.lock().await.push("respond");
            if let Some(message) = &self.respond_error {
                return Err(ClientError::Api {
                    method: "POST",
                    path: format!("/api/v1/respond/{session_id}"),
                    status: 500,
                    body: message.clone(),
                });
            }
            Ok(RespondSubmitResponse {
                success: true,
                message: "ok".to_string(),
                response: request.response,
                auto_resume_status: "resumed".to_string(),
                run_id: None,
            })
        }

        async fn respond_pending(
            &self,
            session_id: &str,
        ) -> Result<RespondPendingResponse, ClientError> {
            self.respond_pending_calls
                .lock()
                .await
                .push(session_id.to_string());
            if let Some(pending) = self.resync_pending.lock().await.clone() {
                return Ok(pending);
            }
            Ok(RespondPendingResponse {
                has_pending_question: false,
                question: None,
                options: None,
                allow_custom: None,
                tool_call_id: None,
                tool_name: None,
                source: None,
            })
        }

        async fn subscribe_session(
            &self,
            session_id: &str,
        ) -> Result<mpsc::Receiver<StreamEvent>, StreamError> {
            self.subscribe_calls
                .lock()
                .await
                .push(session_id.to_string());
            self.ops.lock().await.push("subscribe");
            let (tx, rx) = mpsc::channel(16);
            self.channels
                .lock()
                .await
                .insert(session_id.to_string(), tx);
            Ok(rx)
        }
    }

    /// Polls `bridge`'s internal chat state until `key` has a parked ask (or
    /// panics past a 5s deadline) — used to synchronize with
    /// `render_until_settled`'s pause branch, which parks the ask
    /// asynchronously.
    async fn wait_for_parked_ask(bridge: &ConnectBridge, key: &str) -> ParkedAsk {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(ask) = bridge
                .chat_state
                .lock()
                .await
                .get(key)
                .and_then(|state| state.pending_ask.clone())
            {
                return ask;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "ask was never parked for {key}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Polls until `check` resolves `true` (or panics past a 5s deadline).
    /// `tokio::sync::Mutex`/`RwLock` only expose an async `lock()`/`read()` —
    /// their `blocking_*` variants panic inside a `#[tokio::test]`'s async
    /// context — so this takes a future-producing closure rather than a
    /// plain `FnMut() -> bool`.
    async fn wait_until<Fut>(mut check: impl FnMut() -> Fut)
    where
        Fut: std::future::Future<Output = bool>,
    {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if check().await {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "condition never became true"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn inbound(chat_id: &str, user_id: &str, message_id: &str, text: &str) -> InboundMessage {
        InboundMessage {
            platform: "fake".to_string(),
            chat_id: chat_id.to_string(),
            user_id: user_id.to_string(),
            message_id: message_id.to_string(),
            sent_at: Utc::now(),
            text: text.to_string(),
            reply_ctx: ReplyCtx(serde_json::json!({ "chat_id": chat_id })),
        }
    }

    fn key_for(chat_id: &str, user_id: &str) -> String {
        SessionKey {
            platform: "fake".to_string(),
            chat_id: chat_id.to_string(),
            user_id: user_id.to_string(),
        }
        .as_string()
    }

    fn usage() -> TokenUsage {
        TokenUsage {
            prompt_tokens: 1,
            completion_tokens: 1,
            total_tokens: 2,
        }
    }

    #[test]
    fn session_key_formats_as_platform_chat_user() {
        let key = SessionKey {
            platform: "telegram".to_string(),
            chat_id: "42".to_string(),
            user_id: "7".to_string(),
        };
        assert_eq!(key.as_string(), "telegram:42:7");
    }

    // ---- Bamboo issue #454 follow-up: bounded dedup set ----

    #[test]
    fn bounded_seen_set_evicts_the_oldest_entry_once_over_capacity() {
        let mut set = BoundedSeenSet::new(2);
        assert!(set.insert("a".to_string()));
        assert!(set.insert("b".to_string()));
        assert_eq!(set.len(), 2);

        assert!(set.insert("c".to_string()));
        assert_eq!(set.len(), 2);
        assert!(!set.insert("b".to_string()), "b must still be tracked");
        assert!(!set.insert("c".to_string()), "c must still be tracked");
    }

    #[test]
    fn bounded_seen_set_still_dedups_within_capacity() {
        let mut set = BoundedSeenSet::new(10);
        assert!(set.insert("a".to_string()));
        assert!(!set.insert("a".to_string()));
    }

    #[test]
    fn bounded_seen_set_never_grows_past_capacity() {
        let mut set = BoundedSeenSet::new(3);
        for i in 0..100 {
            set.insert(format!("k{i}"));
        }
        assert_eq!(set.len(), 3);
    }

    // ---- allow_from / dedup / stale-drop ----

    #[tokio::test]
    async fn allow_from_denies_users_not_in_the_list() {
        let api = FakeBambooApi::new();
        let bridge = Arc::new(ConnectBridge::new(api.clone(), None));
        let platform = FakePlatform::new("fake");

        bridge
            .clone()
            .handle_inbound(
                platform.clone() as Arc<dyn Platform>,
                vec!["allowed-user".to_string()],
                inbound("1", "someone-else", "m1", "hello"),
            )
            .await;

        assert!(platform.sent_texts().await.is_empty());
        assert!(api.chat_calls.lock().await.is_empty());
    }

    #[tokio::test]
    async fn dedup_drops_repeated_message_ids() {
        let api = FakeBambooApi::new();
        let bridge = Arc::new(ConnectBridge::new(api.clone(), None));
        let platform = FakePlatform::new("fake");
        let allow = vec!["u1".to_string()];

        bridge
            .clone()
            .handle_inbound(
                platform.clone() as Arc<dyn Platform>,
                allow.clone(),
                inbound("1", "u1", "dup-1", "/status"),
            )
            .await;
        bridge
            .clone()
            .handle_inbound(
                platform.clone() as Arc<dyn Platform>,
                allow,
                inbound("1", "u1", "dup-1", "/status"),
            )
            .await;

        // /status replies inline (no queueing); a second identical
        // message_id must never produce a second reply.
        assert_eq!(platform.sent_texts().await.len(), 1);
    }

    #[tokio::test]
    async fn older_than_process_start_messages_are_dropped() {
        let api = FakeBambooApi::new();
        let bridge = Arc::new(ConnectBridge::new(api.clone(), None));
        let platform = FakePlatform::new("fake");
        let mut msg = inbound("1", "u1", "m1", "/status");
        msg.sent_at = Utc::now() - chrono::Duration::hours(1);

        bridge
            .clone()
            .handle_inbound(
                platform.clone() as Arc<dyn Platform>,
                vec!["u1".to_string()],
                msg,
            )
            .await;

        assert!(platform.sent_texts().await.is_empty());
    }

    // ---- /status, /stop ----

    #[tokio::test]
    async fn status_command_reports_idle_with_no_session_yet() {
        let api = FakeBambooApi::new();
        let bridge = Arc::new(ConnectBridge::new(api, None));
        let platform = FakePlatform::new("fake");

        bridge
            .clone()
            .handle_inbound(
                platform.clone() as Arc<dyn Platform>,
                vec!["u1".to_string()],
                inbound("1", "u1", "m1", "/status"),
            )
            .await;

        let sent = platform.sent_texts().await;
        assert_eq!(sent.len(), 1);
        assert!(sent[0].contains("No session yet"));
    }

    #[tokio::test]
    async fn stop_with_nothing_running_replies_nothing_running() {
        let api = FakeBambooApi::new();
        let bridge = Arc::new(ConnectBridge::new(api.clone(), None));
        let platform = FakePlatform::new("fake");

        bridge
            .clone()
            .handle_inbound(
                platform.clone() as Arc<dyn Platform>,
                vec!["u1".to_string()],
                inbound("1", "u1", "m1", "/stop"),
            )
            .await;

        let sent = platform.sent_texts().await;
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0], "Nothing is running.");
        assert!(
            api.stop_calls.lock().await.is_empty(),
            "/stop with nothing pending must not call the API"
        );
    }

    // ---- prompt -> session mapping, /new, session-map persistence ----

    #[tokio::test]
    async fn prompt_creates_a_session_and_maps_it_to_the_chat_key() {
        let api = FakeBambooApi::new();
        let bridge = Arc::new(ConnectBridge::new(api.clone(), None));
        let platform = FakePlatform::new("fake");
        let key = key_for("1", "u1");

        bridge
            .clone()
            .handle_inbound(
                platform.clone() as Arc<dyn Platform>,
                vec!["u1".to_string()],
                inbound("1", "u1", "m1", "hello there"),
            )
            .await;

        wait_until(|| {
            let api = api.clone();
            async move { api.execute_calls.lock().await.len() == 1 }
        })
        .await;
        let session_id = bridge
            .session_id_for_key(&key)
            .await
            .expect("session mapped");
        assert_eq!(session_id, "sess-1");
        assert_eq!(api.execute_calls.lock().await.as_slice(), ["sess-1"]);

        // End the run so the background task doesn't linger past the test.
        api.sender_for("sess-1")
            .await
            .send(StreamEvent::Agent(
                crate::bamboo::types::AgentEvent::Complete { usage: usage() },
            ))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn new_command_rotates_the_session_mapping() {
        let api = FakeBambooApi::new();
        let bridge = Arc::new(ConnectBridge::new(api.clone(), None));
        let platform = FakePlatform::new("fake");
        let key = key_for("1", "u1");

        bridge
            .clone()
            .handle_inbound(
                platform.clone() as Arc<dyn Platform>,
                vec!["u1".to_string()],
                inbound("1", "u1", "m1", "hello"),
            )
            .await;
        wait_until(|| {
            let api = api.clone();
            async move { api.execute_calls.lock().await.len() == 1 }
        })
        .await;
        api.sender_for("sess-1")
            .await
            .send(StreamEvent::Agent(
                crate::bamboo::types::AgentEvent::Complete { usage: usage() },
            ))
            .await
            .unwrap();
        wait_until(|| {
            let bridge = bridge.clone();
            let key = key.clone();
            async move { bridge.session_map.read().await.contains_key(&key) }
        })
        .await;

        bridge
            .clone()
            .handle_inbound(
                platform.clone() as Arc<dyn Platform>,
                vec!["u1".to_string()],
                inbound("1", "u1", "m2", "/new"),
            )
            .await;
        wait_until(|| {
            let bridge = bridge.clone();
            let key = key.clone();
            async move { !bridge.session_map.read().await.contains_key(&key) }
        })
        .await;

        let sent = platform.sent_texts().await;
        assert!(sent.iter().any(|t| t == "Started a new session."));
    }

    #[tokio::test]
    async fn session_map_persists_and_reloads_across_bridge_instances() {
        let dir = tempfile::tempdir().unwrap();
        let map_path = dir.path().join("magpie_sessions.json");
        let api = FakeBambooApi::new();
        let key = key_for("1", "u1");

        {
            let bridge = Arc::new(ConnectBridge::new(api.clone(), Some(map_path.clone())));
            let platform = FakePlatform::new("fake");
            bridge
                .clone()
                .handle_inbound(
                    platform.clone() as Arc<dyn Platform>,
                    vec!["u1".to_string()],
                    inbound("1", "u1", "m1", "hello"),
                )
                .await;
            wait_until(|| {
                let api = api.clone();
                async move { api.execute_calls.lock().await.len() == 1 }
            })
            .await;
            api.sender_for("sess-1")
                .await
                .send(StreamEvent::Agent(
                    crate::bamboo::types::AgentEvent::Complete { usage: usage() },
                ))
                .await
                .unwrap();
            wait_until(|| {
                let bridge = bridge.clone();
                let key = key.clone();
                async move { bridge.session_map.read().await.contains_key(&key) }
            })
            .await;
        }

        let reloaded = ConnectBridge::new(api, Some(map_path));
        reloaded.load_session_map().await;
        assert_eq!(
            reloaded.session_id_for_key(&key).await,
            Some("sess-1".to_string())
        );
    }

    // ---- paused runs: buttons + callback / text resolution ----

    fn buttons_and_edit_capabilities() -> crate::platform::Capabilities {
        crate::platform::Capabilities {
            buttons: true,
            edit_message: true,
            images: false,
            files: false,
        }
    }

    async fn drive_to_paused(
        bridge: &Arc<ConnectBridge>,
        api: &Arc<FakeBambooApi>,
        platform: &Arc<FakePlatform>,
        key: &str,
        chat_id: &str,
        user_id: &str,
        message_id: &str,
    ) -> ParkedAsk {
        bridge
            .clone()
            .handle_inbound(
                platform.clone() as Arc<dyn Platform>,
                vec![user_id.to_string()],
                inbound(chat_id, user_id, message_id, "please approve something"),
            )
            .await;
        wait_until(|| {
            let api = api.clone();
            async move { !api.subscribe_calls.lock().await.is_empty() }
        })
        .await;
        let session_id = bridge.session_id_for_key(key).await.unwrap();
        api.sender_for(&session_id)
            .await
            .send(StreamEvent::Agent(
                crate::bamboo::types::AgentEvent::NeedClarification {
                    question: "Approve?".to_string(),
                    options: Some(vec!["Approve".to_string(), "Deny".to_string()]),
                    tool_call_id: Some("call-1".to_string()),
                    tool_name: Some("conclusion_with_options".to_string()),
                    allow_custom: false,
                },
            ))
            .await
            .unwrap();
        wait_for_parked_ask(bridge, key).await
    }

    #[tokio::test]
    async fn paused_run_renders_buttons_with_nonce_and_resolves_via_callback() {
        let api = FakeBambooApi::new();
        let bridge = Arc::new(ConnectBridge::new(api.clone(), None));
        let platform = FakePlatform::with_capabilities("fake", buttons_and_edit_capabilities());
        let key = key_for("1", "u1");

        let parked = drive_to_paused(&bridge, &api, &platform, &key, "1", "u1", "m1").await;

        let callback = CallbackQuery {
            platform: "fake".to_string(),
            chat_id: "1".to_string(),
            user_id: "u1".to_string(),
            callback_query_id: "cbq-1".to_string(),
            data: format!("{}:0", parked.nonce),
            reply_ctx: ReplyCtx(serde_json::json!({"chat_id": "1"})),
        };
        bridge
            .clone()
            .handle_callback(
                platform.clone() as Arc<dyn Platform>,
                vec!["u1".to_string()],
                callback,
            )
            .await;

        wait_until(|| {
            let api = api.clone();
            async move { api.respond_calls.lock().await.len() == 1 }
        })
        .await;
        assert_eq!(
            api.respond_calls.lock().await.as_slice(),
            [(
                bridge.session_id_for_key(&key).await.unwrap(),
                "Approve".to_string()
            )]
        );
        assert_eq!(platform.answered_callbacks.lock().await.len(), 1);

        // The resume leg re-subscribes BEFORE responding (issue #6): a
        // server-side resubscribe replaces the channel's forwarder with a
        // fresh broadcast cut, so subscribing after `respond` can lose the
        // resumed run's events entirely — only the ordered op log can see
        // the cross-method ordering.
        wait_until(|| {
            let api = api.clone();
            async move { api.subscribe_calls.lock().await.len() == 2 }
        })
        .await;
        assert_eq!(
            api.ops.lock().await.as_slice(),
            ["subscribe", "subscribe", "respond"],
            "resume must subscribe before respond"
        );

        // The answered ask message was edited to ✅ + the chosen answer, so
        // its now-stale buttons can't be pressed again (issue #6 follow-up).
        wait_until(|| {
            let platform = platform.clone();
            async move {
                platform
                    .edits
                    .lock()
                    .await
                    .iter()
                    .any(|edit| edit.contains("✅ Approve"))
            }
        })
        .await;

        // End the resumed run so the background task settles.
        let session_id = bridge.session_id_for_key(&key).await.unwrap();
        api.sender_for(&session_id)
            .await
            .send(StreamEvent::Agent(
                crate::bamboo::types::AgentEvent::Complete { usage: usage() },
            ))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn stale_callback_nonce_is_dropped_and_acked_without_resolving() {
        let api = FakeBambooApi::new();
        let bridge = Arc::new(ConnectBridge::new(api.clone(), None));
        let platform = FakePlatform::with_capabilities("fake", buttons_and_edit_capabilities());
        let key = key_for("1", "u1");

        drive_to_paused(&bridge, &api, &platform, &key, "1", "u1", "m1").await;

        let callback = CallbackQuery {
            platform: "fake".to_string(),
            chat_id: "1".to_string(),
            user_id: "u1".to_string(),
            callback_query_id: "cbq-1".to_string(),
            data: "stale-nonce:0".to_string(),
            reply_ctx: ReplyCtx(serde_json::json!({"chat_id": "1"})),
        };
        bridge
            .clone()
            .handle_callback(
                platform.clone() as Arc<dyn Platform>,
                vec!["u1".to_string()],
                callback,
            )
            .await;

        let answered = platform.answered_callbacks.lock().await;
        assert_eq!(answered.len(), 1);
        assert_eq!(answered[0].1.as_deref(), Some("This action has expired."));
        assert!(api.respond_calls.lock().await.is_empty());
    }

    #[tokio::test]
    async fn text_answer_resolves_an_open_question() {
        let api = FakeBambooApi::new();
        let bridge = Arc::new(ConnectBridge::new(api.clone(), None));
        let platform = FakePlatform::new("fake");
        let key = key_for("1", "u1");

        bridge
            .clone()
            .handle_inbound(
                platform.clone() as Arc<dyn Platform>,
                vec!["u1".to_string()],
                inbound("1", "u1", "m1", "what should I do"),
            )
            .await;
        wait_until(|| {
            let api = api.clone();
            async move { !api.subscribe_calls.lock().await.is_empty() }
        })
        .await;
        let session_id = bridge.session_id_for_key(&key).await.unwrap();
        api.sender_for(&session_id)
            .await
            .send(StreamEvent::Agent(
                crate::bamboo::types::AgentEvent::NeedClarification {
                    question: "Anything else?".to_string(),
                    options: Some(vec!["OK".to_string(), "Need changes".to_string()]),
                    tool_call_id: Some("call-1".to_string()),
                    tool_name: Some("conclusion_with_options".to_string()),
                    allow_custom: true,
                },
            ))
            .await
            .unwrap();
        wait_for_parked_ask(&bridge, &key).await;

        bridge
            .clone()
            .handle_inbound(
                platform.clone() as Arc<dyn Platform>,
                vec!["u1".to_string()],
                inbound("1", "u1", "m2", "please also add tests"),
            )
            .await;

        wait_until(|| {
            let api = api.clone();
            async move { api.respond_calls.lock().await.len() == 1 }
        })
        .await;
        assert_eq!(api.respond_calls.lock().await[0].1, "please also add tests");

        wait_until(|| {
            let api = api.clone();
            async move { api.subscribe_calls.lock().await.len() == 2 }
        })
        .await;
        api.sender_for(&session_id)
            .await
            .send(StreamEvent::Agent(
                crate::bamboo::types::AgentEvent::Complete { usage: usage() },
            ))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn binary_ask_keyword_mapping_resolves_via_text() {
        let api = FakeBambooApi::new();
        let bridge = Arc::new(ConnectBridge::new(api.clone(), None));
        let platform = FakePlatform::new("fake");
        let key = key_for("1", "u1");

        drive_to_paused(&bridge, &api, &platform, &key, "1", "u1", "m1").await;

        bridge
            .clone()
            .handle_inbound(
                platform.clone() as Arc<dyn Platform>,
                vec!["u1".to_string()],
                inbound("1", "u1", "m2", "yes"),
            )
            .await;

        wait_until(|| {
            let api = api.clone();
            async move { api.respond_calls.lock().await.len() == 1 }
        })
        .await;
        assert_eq!(api.respond_calls.lock().await[0].1, "Approve");

        wait_until(|| {
            let api = api.clone();
            async move { api.subscribe_calls.lock().await.len() == 2 }
        })
        .await;
        let session_id = bridge.session_id_for_key(&key).await.unwrap();
        api.sender_for(&session_id)
            .await
            .send(StreamEvent::Agent(
                crate::bamboo::types::AgentEvent::Complete { usage: usage() },
            ))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn new_command_invalidates_a_parked_ask_instead_of_answering_it() {
        let api = FakeBambooApi::new();
        let bridge = Arc::new(ConnectBridge::new(api.clone(), None));
        let platform = FakePlatform::new("fake");
        let key = key_for("1", "u1");

        drive_to_paused(&bridge, &api, &platform, &key, "1", "u1", "m1").await;

        bridge
            .clone()
            .handle_inbound(
                platform.clone() as Arc<dyn Platform>,
                vec!["u1".to_string()],
                inbound("1", "u1", "m2", "/new"),
            )
            .await;

        wait_until(|| {
            let bridge = bridge.clone();
            let key = key.clone();
            async move { !bridge.has_pending_ask(&key).await }
        })
        .await;
        assert!(api.respond_calls.lock().await.is_empty());
        let sent = platform.sent_texts().await;
        assert!(sent.iter().any(|t| t == "Started a new session."));
    }

    #[tokio::test]
    async fn respond_error_reports_to_the_chat_without_hanging() {
        let api = FakeBambooApi::respond_failing("boom");
        let bridge = Arc::new(ConnectBridge::new(api.clone(), None));
        let platform = FakePlatform::new("fake");
        let key = key_for("1", "u1");

        drive_to_paused(&bridge, &api, &platform, &key, "1", "u1", "m1").await;

        bridge
            .clone()
            .handle_inbound(
                platform.clone() as Arc<dyn Platform>,
                vec!["u1".to_string()],
                inbound("1", "u1", "m2", "yes"),
            )
            .await;

        wait_until(|| {
            let platform = platform.clone();
            async move {
                platform
                    .sent
                    .lock()
                    .await
                    .iter()
                    .any(|t| t.contains("Failed to record your answer"))
            }
        })
        .await;
    }

    #[tokio::test]
    async fn stop_while_paused_cancels_the_pending_question_and_calls_stop() {
        let api = FakeBambooApi::new();
        let bridge = Arc::new(ConnectBridge::new(api.clone(), None));
        let platform = FakePlatform::new("fake");
        let key = key_for("1", "u1");

        drive_to_paused(&bridge, &api, &platform, &key, "1", "u1", "m1").await;

        bridge
            .clone()
            .handle_inbound(
                platform.clone() as Arc<dyn Platform>,
                vec!["u1".to_string()],
                inbound("1", "u1", "m2", "/stop"),
            )
            .await;

        wait_until(|| {
            let bridge = bridge.clone();
            let key = key.clone();
            async move { !bridge.has_pending_ask(&key).await }
        })
        .await;
        // Port note: magpie's `busy` flag stays true for the whole
        // paused-and-waiting window (unlike bamboo's in-proc `cancel_token`,
        // which only exists while a round is actively executing) — so
        // `/stop` while paused always calls the API's stop endpoint too, not
        // just the local invalidation. See `handle_stop`'s doc comment.
        wait_until(|| {
            let api = api.clone();
            async move { !api.stop_calls.lock().await.is_empty() }
        })
        .await;
    }

    // ---- resync path: answering a re-parked ask after a restart (bamboo issue #9) ----

    fn resync_pending_question(options: Vec<&str>, allow_custom: bool) -> RespondPendingResponse {
        RespondPendingResponse {
            has_pending_question: true,
            question: Some("Approve?".to_string()),
            options: Some(options.into_iter().map(str::to_string).collect()),
            allow_custom: Some(allow_custom),
            tool_call_id: Some("call-1".to_string()),
            tool_name: Some("conclusion_with_options".to_string()),
            source: None,
        }
    }

    /// Seeds `bridge`'s session map with `key` -> `session_id` directly
    /// (bypassing a `chat`/`execute` call) and runs `resync_pending_asks` —
    /// simulating exactly what a real magpie restart leaves behind: a
    /// persisted session map entry whose underlying bamboo session is
    /// paused on a question, with a brand-new (in-memory) `ChatState`.
    async fn seed_and_resync(
        bridge: &Arc<ConnectBridge>,
        platform: &Arc<FakePlatform>,
        key: &str,
        session_id: &str,
    ) {
        bridge.set_session_id_for_key(key, session_id).await;
        let mut platforms: HashMap<String, Arc<dyn Platform>> = HashMap::new();
        platforms.insert("fake".to_string(), platform.clone() as Arc<dyn Platform>);
        bridge.resync_pending_asks(&platforms).await;
    }

    #[tokio::test]
    async fn resync_parks_the_ask_with_no_live_resolver() {
        let api = FakeBambooApi::new();
        api.set_resync_pending(resync_pending_question(vec!["Approve", "Deny"], false))
            .await;
        let bridge = Arc::new(ConnectBridge::new(api.clone(), None));
        let platform = FakePlatform::new("fake");
        let key = key_for("1", "u1");

        seed_and_resync(&bridge, &platform, &key, "sess-1").await;

        assert!(bridge.has_pending_ask(&key).await);
        assert_eq!(
            api.respond_pending_calls.lock().await.as_slice(),
            ["sess-1"]
        );
        // The gap issue #9 describes: re-parked with no live render task
        // waiting, so no `ask_resolution` sender exists yet.
        assert!(bridge
            .chat_state
            .lock()
            .await
            .get(&key)
            .unwrap()
            .ask_resolution
            .is_none());
    }

    #[tokio::test]
    async fn resync_text_answer_resumes_the_parked_run_instead_of_starting_a_new_one() {
        let api = FakeBambooApi::new();
        api.set_resync_pending(resync_pending_question(vec!["Approve", "Deny"], false))
            .await;
        let bridge = Arc::new(ConnectBridge::new(api.clone(), None));
        let platform = FakePlatform::new("fake");
        let key = key_for("1", "u1");

        seed_and_resync(&bridge, &platform, &key, "sess-1").await;
        assert!(bridge.has_pending_ask(&key).await);

        bridge
            .clone()
            .handle_inbound(
                platform.clone() as Arc<dyn Platform>,
                vec!["u1".to_string()],
                inbound("1", "u1", "m1", "yes"),
            )
            .await;

        wait_until(|| {
            let api = api.clone();
            async move { api.respond_calls.lock().await.len() == 1 }
        })
        .await;
        assert_eq!(
            api.respond_calls.lock().await.as_slice(),
            [("sess-1".to_string(), "Approve".to_string())]
        );
        assert_eq!(
            api.ops.lock().await.as_slice(),
            ["subscribe", "respond"],
            "resync resume must subscribe before respond, exactly like the live path"
        );
        // The core regression: the matched answer must resolve the parked
        // run inline, never fall through to `process_one`/`run_prompt` and
        // start a brand new session on top of the still-suspended one.
        assert!(
            api.chat_calls.lock().await.is_empty(),
            "a resync-matched answer must never start a new run via POST /chat"
        );
        assert!(api.execute_calls.lock().await.is_empty());
        assert!(!bridge.has_pending_ask(&key).await);

        // Let the resumed run finish so `busy` settles back to idle.
        api.sender_for("sess-1")
            .await
            .send(StreamEvent::Agent(
                crate::bamboo::types::AgentEvent::Complete { usage: usage() },
            ))
            .await
            .unwrap();
        wait_until(|| {
            let bridge = bridge.clone();
            let key = key.clone();
            async move { !bridge.is_busy(&key).await }
        })
        .await;
    }

    #[tokio::test]
    async fn resync_callback_answer_resumes_the_parked_run_instead_of_starting_a_new_one() {
        let api = FakeBambooApi::new();
        api.set_resync_pending(resync_pending_question(vec!["Approve", "Deny"], false))
            .await;
        let bridge = Arc::new(ConnectBridge::new(api.clone(), None));
        let platform = FakePlatform::with_capabilities("fake", buttons_and_edit_capabilities());
        let key = key_for("1", "u1");

        seed_and_resync(&bridge, &platform, &key, "sess-1").await;
        let parked = bridge
            .chat_state
            .lock()
            .await
            .get(&key)
            .unwrap()
            .pending_ask
            .clone()
            .expect("ask parked by resync");

        let callback = CallbackQuery {
            platform: "fake".to_string(),
            chat_id: "1".to_string(),
            user_id: "u1".to_string(),
            callback_query_id: "cbq-1".to_string(),
            data: format!("{}:0", parked.nonce),
            reply_ctx: ReplyCtx(serde_json::json!({"chat_id": "1"})),
        };
        bridge
            .clone()
            .handle_callback(
                platform.clone() as Arc<dyn Platform>,
                vec!["u1".to_string()],
                callback,
            )
            .await;

        assert_eq!(platform.answered_callbacks.lock().await.len(), 1);
        wait_until(|| {
            let api = api.clone();
            async move { api.respond_calls.lock().await.len() == 1 }
        })
        .await;
        assert_eq!(api.respond_calls.lock().await[0].1, "Approve");
        assert!(api.chat_calls.lock().await.is_empty());
        assert!(api.execute_calls.lock().await.is_empty());

        api.sender_for("sess-1")
            .await
            .send(StreamEvent::Agent(
                crate::bamboo::types::AgentEvent::Complete { usage: usage() },
            ))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn resync_pre_restart_button_nonce_is_stale_and_reports_expired() {
        // A resync always mints a FRESH nonce (bridge.rs never recovers the
        // pre-restart message to re-render), so a press on the OLD button
        // still visible in the chat can never match post-restart — the
        // callback fast path's existing "This action has expired." handling
        // covers it; this asserts that stays true for the resync case too,
        // and that it never starts a stray new run either.
        let api = FakeBambooApi::new();
        api.set_resync_pending(resync_pending_question(vec!["Approve", "Deny"], false))
            .await;
        let bridge = Arc::new(ConnectBridge::new(api.clone(), None));
        let platform = FakePlatform::with_capabilities("fake", buttons_and_edit_capabilities());
        let key = key_for("1", "u1");

        seed_and_resync(&bridge, &platform, &key, "sess-1").await;

        let callback = CallbackQuery {
            platform: "fake".to_string(),
            chat_id: "1".to_string(),
            user_id: "u1".to_string(),
            callback_query_id: "cbq-1".to_string(),
            data: "pre-restart-nonce:0".to_string(),
            reply_ctx: ReplyCtx(serde_json::json!({"chat_id": "1"})),
        };
        bridge
            .clone()
            .handle_callback(
                platform.clone() as Arc<dyn Platform>,
                vec!["u1".to_string()],
                callback,
            )
            .await;

        let answered = platform.answered_callbacks.lock().await;
        assert_eq!(answered.len(), 1);
        assert_eq!(answered[0].1.as_deref(), Some("This action has expired."));
        assert!(api.respond_calls.lock().await.is_empty());
        assert!(api.chat_calls.lock().await.is_empty());
        // The genuinely-parked ask (fresh nonce) must still be there,
        // untouched by the stale press.
        assert!(bridge.has_pending_ask(&key).await);
    }

    #[tokio::test]
    async fn resync_answer_marks_the_chat_busy_and_drains_a_message_queued_behind_it() {
        let api = FakeBambooApi::new();
        api.set_resync_pending(resync_pending_question(vec!["Approve", "Deny"], false))
            .await;
        let bridge = Arc::new(ConnectBridge::new(api.clone(), None));
        let platform = FakePlatform::new("fake");
        let key = key_for("1", "u1");

        seed_and_resync(&bridge, &platform, &key, "sess-1").await;
        assert!(!bridge.is_busy(&key).await, "merely parked, not yet busy");

        bridge
            .clone()
            .handle_inbound(
                platform.clone() as Arc<dyn Platform>,
                vec!["u1".to_string()],
                inbound("1", "u1", "m1", "yes"),
            )
            .await;

        // The resolving answer atomically marks the chat busy (mirrors
        // `handle_inbound`'s own busy-then-spawn dance for a fresh
        // message) — an unrelated message arriving right behind it must
        // queue rather than race a second run onto the same session.
        wait_until(|| {
            let bridge = bridge.clone();
            let key = key.clone();
            async move { bridge.is_busy(&key).await }
        })
        .await;
        bridge
            .clone()
            .handle_inbound(
                platform.clone() as Arc<dyn Platform>,
                vec!["u1".to_string()],
                inbound("1", "u1", "m2", "a follow-up message"),
            )
            .await;

        // Finish the resumed (resync) run.
        wait_until(|| {
            let api = api.clone();
            async move { api.respond_calls.lock().await.len() == 1 }
        })
        .await;
        api.sender_for("sess-1")
            .await
            .send(StreamEvent::Agent(
                crate::bamboo::types::AgentEvent::Complete { usage: usage() },
            ))
            .await
            .unwrap();

        // The queued follow-up message must still get processed — as its
        // own new run on the same (now-idle) session — instead of being
        // orphaned in the queue forever.
        wait_until(|| {
            let api = api.clone();
            async move { !api.chat_calls.lock().await.is_empty() }
        })
        .await;
        assert_eq!(
            api.chat_calls.lock().await[0].message,
            "a follow-up message"
        );

        // End the follow-up run too so its background task doesn't linger
        // past the test.
        wait_until(|| {
            let api = api.clone();
            async move { api.execute_calls.lock().await.len() == 1 }
        })
        .await;
        api.sender_for("sess-1")
            .await
            .send(StreamEvent::Agent(
                crate::bamboo::types::AgentEvent::Complete { usage: usage() },
            ))
            .await
            .unwrap();
        wait_until(|| {
            let bridge = bridge.clone();
            let key = key.clone();
            async move { !bridge.is_busy(&key).await }
        })
        .await;
    }

    // ---- magpie issue #12: queue (don't run) while a resync-parked ask has no live task yet ----

    #[tokio::test]
    async fn unrelated_message_in_the_resync_parked_window_queues_instead_of_starting_a_run() {
        let api = FakeBambooApi::new();
        api.set_resync_pending(resync_pending_question(vec!["Approve", "Deny"], false))
            .await;
        let bridge = Arc::new(ConnectBridge::new(api.clone(), None));
        let platform = FakePlatform::new("fake");
        let key = key_for("1", "u1");

        seed_and_resync(&bridge, &platform, &key, "sess-1").await;
        assert!(bridge.has_pending_ask(&key).await);
        assert!(
            !bridge.is_busy(&key).await,
            "the exact window issue #12 describes: parked but not yet busy"
        );

        // An unrelated message — doesn't match "Approve"/"Deny" by index,
        // exact text, or the binary keyword mapping — arrives BEFORE any
        // answer. Before the fix this fell through to normal busy/queue
        // routing (busy == false) and started a brand new concurrent run on
        // top of the still-suspended session.
        bridge
            .clone()
            .handle_inbound(
                platform.clone() as Arc<dyn Platform>,
                vec!["u1".to_string()],
                inbound("1", "u1", "m1", "what's the weather like"),
            )
            .await;

        // It must be queued, not run: no new POST /chat, ask still parked,
        // still not busy (queueing alone never flips `busy`).
        assert!(
            api.chat_calls.lock().await.is_empty(),
            "an unrelated message during the parked-but-not-yet-busy window must never start a \
             concurrent run"
        );
        assert!(bridge.has_pending_ask(&key).await);
        assert_eq!(
            bridge
                .chat_state
                .lock()
                .await
                .get(&key)
                .unwrap()
                .queue
                .len(),
            1
        );

        // Now the real answer arrives and resolves the parked ask inline
        // (the resync path, bamboo issue #9).
        bridge
            .clone()
            .handle_inbound(
                platform.clone() as Arc<dyn Platform>,
                vec!["u1".to_string()],
                inbound("1", "u1", "m2", "yes"),
            )
            .await;

        wait_until(|| {
            let api = api.clone();
            async move { api.respond_calls.lock().await.len() == 1 }
        })
        .await;
        assert_eq!(
            api.respond_calls.lock().await.as_slice(),
            [("sess-1".to_string(), "Approve".to_string())]
        );
        assert!(!bridge.has_pending_ask(&key).await);

        // Finish the resumed run — its own `drain_queue` tail must now pick
        // up the earlier queued message rather than orphaning it.
        api.sender_for("sess-1")
            .await
            .send(StreamEvent::Agent(
                crate::bamboo::types::AgentEvent::Complete { usage: usage() },
            ))
            .await
            .unwrap();

        wait_until(|| {
            let api = api.clone();
            async move { !api.chat_calls.lock().await.is_empty() }
        })
        .await;
        assert_eq!(
            api.chat_calls.lock().await[0].message,
            "what's the weather like"
        );

        // Let the drained follow-up run finish too, so no task lingers past
        // the test.
        wait_until(|| {
            let api = api.clone();
            async move { api.execute_calls.lock().await.len() == 1 }
        })
        .await;
        api.sender_for("sess-1")
            .await
            .send(StreamEvent::Agent(
                crate::bamboo::types::AgentEvent::Complete { usage: usage() },
            ))
            .await
            .unwrap();
        wait_until(|| {
            let bridge = bridge.clone();
            let key = key.clone();
            async move { !bridge.is_busy(&key).await }
        })
        .await;
    }

    #[tokio::test]
    async fn stop_while_resync_parked_and_not_yet_busy_still_drains_the_queued_backlog() {
        let api = FakeBambooApi::new();
        api.set_resync_pending(resync_pending_question(vec!["Approve", "Deny"], false))
            .await;
        let bridge = Arc::new(ConnectBridge::new(api.clone(), None));
        let platform = FakePlatform::new("fake");
        let key = key_for("1", "u1");

        seed_and_resync(&bridge, &platform, &key, "sess-1").await;

        // Queue an unrelated message behind the parked (not-yet-busy) ask.
        bridge
            .clone()
            .handle_inbound(
                platform.clone() as Arc<dyn Platform>,
                vec!["u1".to_string()],
                inbound("1", "u1", "m1", "unrelated backlog message"),
            )
            .await;
        assert_eq!(
            bridge
                .chat_state
                .lock()
                .await
                .get(&key)
                .unwrap()
                .queue
                .len(),
            1
        );

        // The ask never gets answered — the user gives up and sends /stop
        // instead ("ask expired/stale nonce" resolution path). Before the
        // fix nothing would ever drain the backlog: `busy` was never set
        // `true` for a resync-parked ask, and `invalidate_pending_ask` used
        // to just drop the pending ask on the floor.
        bridge
            .clone()
            .handle_inbound(
                platform.clone() as Arc<dyn Platform>,
                vec!["u1".to_string()],
                inbound("1", "u1", "m2", "/stop"),
            )
            .await;

        wait_until(|| {
            let bridge = bridge.clone();
            let key = key.clone();
            async move { !bridge.has_pending_ask(&key).await }
        })
        .await;
        let sent = platform.sent_texts().await;
        assert!(sent
            .iter()
            .any(|t| t == "Stopped — the pending question was cancelled."));
        assert!(
            api.respond_calls.lock().await.is_empty(),
            "/stop must never submit an answer"
        );
        // magpie issue #14: a resync-parked ask never set `busy`, so
        // `handle_stop` used to skip `BambooApi::stop` entirely for this
        // exact case, clearing local state without ever telling the server
        // to cancel the still-suspended session.
        assert_eq!(
            *api.stop_calls.lock().await,
            vec!["sess-1".to_string()],
            "/stop on a resync-parked (never-busy) ask must still cancel the server session"
        );

        // The queued backlog message must still get drained (against the
        // untouched session — /stop doesn't rotate it) rather than sitting
        // forever with `busy` stuck reporting idle while the queue is
        // secretly non-empty.
        wait_until(|| {
            let api = api.clone();
            async move { !api.chat_calls.lock().await.is_empty() }
        })
        .await;
        assert_eq!(
            api.chat_calls.lock().await[0].message,
            "unrelated backlog message"
        );
        assert_eq!(
            api.chat_calls.lock().await[0].session_id.as_deref(),
            Some("sess-1")
        );

        wait_until(|| {
            let api = api.clone();
            async move { api.execute_calls.lock().await.len() == 1 }
        })
        .await;
        api.sender_for("sess-1")
            .await
            .send(StreamEvent::Agent(
                crate::bamboo::types::AgentEvent::Complete { usage: usage() },
            ))
            .await
            .unwrap();
        wait_until(|| {
            let bridge = bridge.clone();
            let key = key.clone();
            async move { !bridge.is_busy(&key).await }
        })
        .await;
    }

    #[tokio::test]
    async fn new_command_while_resync_parked_drains_the_queued_backlog_onto_the_fresh_session() {
        let api = FakeBambooApi::new();
        api.set_resync_pending(resync_pending_question(vec!["Approve", "Deny"], false))
            .await;
        let bridge = Arc::new(ConnectBridge::new(api.clone(), None));
        let platform = FakePlatform::new("fake");
        let key = key_for("1", "u1");

        seed_and_resync(&bridge, &platform, &key, "sess-1").await;

        bridge
            .clone()
            .handle_inbound(
                platform.clone() as Arc<dyn Platform>,
                vec!["u1".to_string()],
                inbound("1", "u1", "m1", "unrelated backlog message"),
            )
            .await;
        assert_eq!(
            bridge
                .chat_state
                .lock()
                .await
                .get(&key)
                .unwrap()
                .queue
                .len(),
            1
        );

        // `/new` always takes the immediate-escape-hatch fast path while a
        // parked ask exists (bamboo issue #458), regardless of `busy`.
        bridge
            .clone()
            .handle_inbound(
                platform.clone() as Arc<dyn Platform>,
                vec!["u1".to_string()],
                inbound("1", "u1", "m2", "/new"),
            )
            .await;

        wait_until(|| {
            let bridge = bridge.clone();
            let key = key.clone();
            async move { !bridge.has_pending_ask(&key).await }
        })
        .await;
        assert!(bridge.session_id_for_key(&key).await.is_none());

        // The queued backlog message must drain against the FRESH
        // (post-rotation) session, not the abandoned one — i.e. as a
        // brand-new `POST /chat` with no `session_id`.
        wait_until(|| {
            let api = api.clone();
            async move { !api.chat_calls.lock().await.is_empty() }
        })
        .await;
        let chat_calls = api.chat_calls.lock().await;
        assert_eq!(chat_calls[0].message, "unrelated backlog message");
        assert_eq!(chat_calls[0].session_id, None);
        drop(chat_calls);

        wait_until(|| {
            let api = api.clone();
            async move { api.execute_calls.lock().await.len() == 1 }
        })
        .await;
        // `FakeBambooApi::chat` mints new ids from its own counter — which
        // was never advanced by `seed_and_resync`'s manual seeding, so the
        // fresh id can coincidentally collide with the string "sess-1";
        // what actually matters (checked above) is that the drained
        // message's `POST /chat` carried NO `session_id`, i.e. a genuinely
        // new session, not a reuse of the abandoned one.
        let new_session_id = bridge.session_id_for_key(&key).await.unwrap();
        api.sender_for(&new_session_id)
            .await
            .send(StreamEvent::Agent(
                crate::bamboo::types::AgentEvent::Complete { usage: usage() },
            ))
            .await
            .unwrap();
        wait_until(|| {
            let bridge = bridge.clone();
            let key = key.clone();
            async move { !bridge.is_busy(&key).await }
        })
        .await;
    }

    #[tokio::test]
    async fn log_backlog_on_shutdown_is_a_noop_when_nothing_is_outstanding() {
        // No tracing-output assertion (would need a custom subscriber); this
        // just pins down that the method is safe to call unconditionally at
        // shutdown, including against an empty/never-touched bridge.
        let api = FakeBambooApi::new();
        let bridge = Arc::new(ConnectBridge::new(api, None));
        bridge.log_backlog_on_shutdown().await;
    }
}
