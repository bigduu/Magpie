//! `/v2/stream` WebSocket client.
//!
//! Protocol read from
//! `crates/app/bamboo-server/src/handlers/agent/ws_v2/{mod,envelope,forwarders}.rs`:
//!
//! - Client frames (`ClientFrame` on the server, `envelope.rs:179-202`):
//!   `{"type":"hello","device_id":..,"token":..}`,
//!   `{"type":"subscribe","ch":"agent.{sid}","since":null}`,
//!   `{"type":"unsubscribe","ch":..}`, `{"type":"stop","session_id":..}`.
//! - Server envelopes (`ServerEnvelope`, `envelope.rs:58-97`):
//!   `{"ch":..,"seq":N,"event":<AgentEvent JSON>}` or
//!   `{"ch":..,"seq":N,"control":{"type":"terminal"|"feed_reset",..}}`.
//! - Auth (`mod.rs:36-54`, #189): `/v2/stream` is on the PUBLIC route
//!   whitelist so the upgrade itself opens unauthenticated; the handler is
//!   the authoritative gate. A connection carrying the same
//!   `Authorization`/`X-Device-Id` headers the REST client uses is
//!   `pre_authorized` on the upgrade (mirrors `request_is_authorized`).
//!   Magpie ALSO sends a `hello` frame immediately after connecting, for two
//!   reasons: (1) belt-and-braces in case a proxy ever strips upgrade
//!   headers, and (2) it matches the wire contract a browser device-token
//!   client must use (headers can't be set on a browser WS upgrade), so this
//!   code path is exercised the same way in every deployment.
//! - Keepalive: the server pings every ~15s (`mod.rs:87-88`); this client
//!   answers every `Ping` with a matching `Pong` explicitly (defensive —
//!   correct regardless of whether the underlying library auto-replies).
//! - Subscribe ack: the protocol has **no explicit subscribe-ack frame** —
//!   `subscribe` just spawns a forwarder task server-side
//!   (`mod.rs:568-661`). [`BambooStream::subscribe_session`] resolves
//!   "established" to "the `subscribe` frame's WS write completed" (the
//!   strongest signal the protocol offers); see the method doc for why this
//!   is the documented, deliberate interpretation of the phase-1 spec's
//!   "guaranteed subscribed before execute" requirement.

use std::collections::HashMap;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;

use super::types::AgentEvent;
use crate::config::BambooConfig;

/// HTTP header carrying the device id, mirrored from `client.rs` /
/// bamboo's `DEVICE_ID_HEADER`.
const DEVICE_ID_HEADER: &str = "X-Device-Id";

/// Bound on each per-session outbound channel handed to a subscriber. Small:
/// a slow consumer should feel backpressure quickly rather than let Magpie
/// buffer an unbounded amount of a bamboo run's token stream.
const SUBSCRIBER_BUFFER: usize = 256;

/// Reconnect backoff schedule (seconds), capped at the last entry.
const BACKOFF_SCHEDULE_SECS: &[u64] = &[0, 1, 2, 5, 10, 30];

#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    #[error("invalid bamboo base_url for the WS stream: {0}")]
    InvalidBaseUrl(String),
    #[error("failed to connect to /v2/stream: {0}")]
    Connect(String),
    #[error("the stream connection task is no longer running")]
    Closed,
}

// ── wire protocol (pure encode/decode) ──────────────────────────────────

/// A client→server frame. Mirrors `ClientFrame` in
/// `ws_v2/envelope.rs:179-202` (`#[serde(tag = "type", rename_all =
/// "snake_case")]`, pinned identically). Magpie always sends a fully
/// credentialed `Hello` (unlike the server's `Option<String>` fields, which
/// also tolerate a token-less hello for other clients).
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientFrame {
    Hello {
        device_id: String,
        token: String,
    },
    Subscribe {
        ch: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        since: Option<u64>,
    },
    Unsubscribe {
        ch: String,
    },
    Stop {
        session_id: String,
    },
}

/// Encode a [`ClientFrame`] to the JSON text sent over the WS text frame.
pub fn encode_client_frame(frame: &ClientFrame) -> String {
    serde_json::to_string(frame).expect("ClientFrame always serializes")
}

/// A server→client envelope. Mirrors `ServerEnvelope` in
/// `ws_v2/envelope.rs:58-97`: `{ch, seq}` plus a mutually-exclusive
/// `event`/`control` payload (untagged + flatten on the server; decoded the
/// same way here).
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct ServerEnvelope {
    pub ch: String,
    pub seq: u64,
    #[serde(flatten)]
    pub body: EnvelopeBody,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum EnvelopeBody {
    Event { event: serde_json::Value },
    Control { control: serde_json::Value },
}

/// Parse one WS text frame's body into a [`ServerEnvelope`].
pub fn parse_server_envelope(text: &str) -> Result<ServerEnvelope, serde_json::Error> {
    serde_json::from_str(text)
}

/// A `control` payload's `type` field. Mirrors `envelope.rs`'s
/// `terminal_control`/`feed_reset_control` helpers (`:161-170`).
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlPayload {
    Terminal {
        reason: String,
    },
    FeedReset {
        from_seq: u64,
    },
    #[serde(other)]
    Unknown,
}

/// Parse a `control` JSON value into a [`ControlPayload`].
pub fn parse_control_payload(
    value: &serde_json::Value,
) -> Result<ControlPayload, serde_json::Error> {
    serde_json::from_value(value.clone())
}

/// Rewrite an `http(s)://…` `base_url` into the `ws(s)://…/v2/stream` upgrade
/// URL. Pure function, independent of the actual connect attempt.
pub fn derive_ws_url(base_url: &str) -> Result<url::Url, StreamError> {
    let mut url = url::Url::parse(base_url)
        .map_err(|error| StreamError::InvalidBaseUrl(error.to_string()))?;
    let ws_scheme = match url.scheme() {
        "http" => "ws",
        "https" => "wss",
        other => {
            return Err(StreamError::InvalidBaseUrl(format!(
                "unsupported scheme {other:?} (expected http or https)"
            )))
        }
    };
    url.set_scheme(ws_scheme)
        .map_err(|_| StreamError::InvalidBaseUrl("failed to rewrite URL scheme".to_string()))?;
    url.set_path("/v2/stream");
    url.set_query(None);
    Ok(url)
}

// ── consumer-facing event type ──────────────────────────────────────────

/// What a [`BambooStream`] subscriber receives for one `agent.{sid}` channel.
#[derive(Debug, Clone)]
pub enum StreamEvent {
    /// A decoded `AgentEvent` (subset — see `bamboo::types`).
    Agent(AgentEvent),
    /// The channel's forwarder reached a terminal state server-side; no more
    /// events will arrive on THIS subscription (a new run needs a fresh
    /// `subscribe_session` call before the next `execute`).
    Terminal { reason: String },
    /// Emitted once right after a reconnect + resubscribe cycle completes.
    /// Per ARCHITECTURE.md's documented limitation, a resubscribe only
    /// replays critical events + the last budget event, never the token
    /// stream — so a consumer that receives this should treat any
    /// in-flight rendering as possibly stale until the next terminal event.
    Gap,
}

// ── connection manager ───────────────────────────────────────────────────

enum Command {
    Subscribe {
        channel: String,
        ack: oneshot::Sender<mpsc::Receiver<StreamEvent>>,
    },
    Unsubscribe {
        channel: String,
    },
    Stop {
        session_id: String,
    },
}

/// A running `/v2/stream` connection: owns a background task that connects,
/// reconnects with backoff, and multiplexes `agent.{sid}` channels out to
/// per-session subscriber queues.
#[derive(Clone)]
pub struct BambooStream {
    cmd_tx: mpsc::UnboundedSender<Command>,
}

impl BambooStream {
    /// Spawn the background connection task. Returns immediately; the actual
    /// WS handshake happens asynchronously and reconnects on its own.
    pub fn connect(config: BambooConfig) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        // A probe clone lets `run`'s backoff sleep detect "every BambooStream
        // handle was dropped" (via `UnboundedSender::closed()`) without
        // consuming a real command off `cmd_rx`.
        let probe_tx = cmd_tx.clone();
        tokio::spawn(run(config, cmd_rx, probe_tx));
        Self { cmd_tx }
    }

    /// Subscribe to `agent.{session_id}`, returning a receiver of
    /// [`StreamEvent`]s for that channel.
    ///
    /// Resolves once the `subscribe` frame's WS write has completed — the
    /// strongest "established" signal the protocol offers (there is no
    /// server-side subscribe ack; see the module doc). Callers that need the
    /// documented "subscribe before execute" ordering (ARCHITECTURE.md) should
    /// `await` this BEFORE issuing the `POST /execute/{id}` call: because the
    /// WS write is flushed to the OS socket before this future resolves, and
    /// the server drives its single WS connection's frame-processing loop
    /// strictly in receive order, the subscribe is virtually always
    /// established before a subsequently-issued HTTP request reaches the
    /// server. This is a best-effort ordering guarantee, not a cross-protocol
    /// transactional one — see the "Known limitations" section of
    /// ARCHITECTURE.md for the accepted gap (a resubscribe after a reconnect
    /// only replays critical + budget events, never tokens).
    pub async fn subscribe_session(
        &self,
        session_id: &str,
    ) -> Result<mpsc::Receiver<StreamEvent>, StreamError> {
        let channel = format!("agent.{session_id}");
        let (ack_tx, ack_rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Subscribe {
                channel,
                ack: ack_tx,
            })
            .map_err(|_| StreamError::Closed)?;
        ack_rx.await.map_err(|_| StreamError::Closed)
    }

    /// Unsubscribe from `agent.{session_id}`. Fire-and-forget — a missing
    /// connection or already-torn-down channel is a silent no-op.
    pub fn unsubscribe_session(&self, session_id: &str) {
        let channel = format!("agent.{session_id}");
        let _ = self.cmd_tx.send(Command::Unsubscribe { channel });
    }

    /// Send the WS `stop` control frame for `session_id` (cancels a running
    /// session — reuses the same discipline as `POST /stop/{id}`).
    pub fn stop_session(&self, session_id: &str) {
        let _ = self.cmd_tx.send(Command::Stop {
            session_id: session_id.to_string(),
        });
    }
}

/// The background task: connect, drive one connection until it drops, then
/// reconnect with backoff — forever (or until every [`BambooStream`] handle
/// and its `cmd_tx` are dropped, at which point `cmd_rx` closes and this
/// task exits).
async fn run(
    config: BambooConfig,
    mut cmd_rx: mpsc::UnboundedReceiver<Command>,
    probe_tx: mpsc::UnboundedSender<Command>,
) {
    let mut subscribers: HashMap<String, mpsc::Sender<StreamEvent>> = HashMap::new();
    let mut backoff_index = 0usize;
    let mut first_connect = true;

    loop {
        let ws_url = match derive_ws_url(&config.base_url) {
            Ok(url) => url,
            Err(error) => {
                tracing::error!("magpie stream: {error}; giving up (no valid base_url)");
                return;
            }
        };

        match connect_once(&config, &ws_url).await {
            Ok(stream) => {
                backoff_index = 0;
                if !first_connect {
                    // Reconnected after a drop: resubscribe every live
                    // channel and tell each subscriber a gap may have
                    // occurred.
                    for (channel, tx) in &subscribers {
                        let _ = tx.try_send(StreamEvent::Gap);
                        tracing::debug!("magpie stream: resubscribing {channel} after reconnect");
                    }
                }
                first_connect = false;
                drive(stream, &config, &mut subscribers, &mut cmd_rx).await;
                // `drive` returns when the command channel closes (shut down)
                // or the WS connection drops (reconnect) or unexpectedly.
                if probe_tx.is_closed() {
                    return;
                }
            }
            Err(error) => {
                tracing::warn!("magpie stream: connect failed: {error}");
            }
        }

        let delay = backoff_delay(backoff_index);
        backoff_index = (backoff_index + 1).min(BACKOFF_SCHEDULE_SECS.len() - 1);
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = probe_tx.closed() => return,
        }
    }
}

fn backoff_delay(index: usize) -> Duration {
    let secs = BACKOFF_SCHEDULE_SECS[index.min(BACKOFF_SCHEDULE_SECS.len() - 1)];
    Duration::from_secs(secs)
}

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect_once(config: &BambooConfig, ws_url: &url::Url) -> Result<WsStream, StreamError> {
    let mut request = ws_url
        .as_str()
        .into_client_request()
        .map_err(|error| StreamError::Connect(error.to_string()))?;

    let auth_value = HeaderValue::from_str(&format!("Bearer {}", config.token))
        .map_err(|error| StreamError::Connect(error.to_string()))?;
    let device_value = HeaderValue::from_str(&config.device_id)
        .map_err(|error| StreamError::Connect(error.to_string()))?;
    request.headers_mut().insert(
        tokio_tungstenite::tungstenite::http::header::AUTHORIZATION,
        auth_value,
    );
    request.headers_mut().insert(DEVICE_ID_HEADER, device_value);

    let (stream, _response) = tokio_tungstenite::connect_async(request)
        .await
        .map_err(|error| StreamError::Connect(error.to_string()))?;
    Ok(stream)
}

/// Drive one live connection: send the `hello` frame, then loop over
/// commands + inbound WS messages until the connection drops or the command
/// channel closes.
async fn drive(
    stream: WsStream,
    config: &BambooConfig,
    subscribers: &mut HashMap<String, mpsc::Sender<StreamEvent>>,
    cmd_rx: &mut mpsc::UnboundedReceiver<Command>,
) {
    let (mut write, mut read) = stream.split();

    let hello = ClientFrame::Hello {
        device_id: config.device_id.clone(),
        token: config.token.clone(),
    };
    if write
        .send(Message::Text(encode_client_frame(&hello)))
        .await
        .is_err()
    {
        return;
    }

    // Resubscribe every channel already tracked from a prior connection.
    for channel in subscribers.keys() {
        let frame = ClientFrame::Subscribe {
            ch: channel.clone(),
            since: None,
        };
        if write
            .send(Message::Text(encode_client_frame(&frame)))
            .await
            .is_err()
        {
            return;
        }
    }

    loop {
        tokio::select! {
            command = cmd_rx.recv() => {
                match command {
                    Some(Command::Subscribe { channel, ack }) => {
                        let (tx, rx) = mpsc::channel(SUBSCRIBER_BUFFER);
                        let frame = ClientFrame::Subscribe { ch: channel.clone(), since: None };
                        if write.send(Message::Text(encode_client_frame(&frame))).await.is_err() {
                            // Connection is dead; drop the ack so the caller's
                            // await surfaces StreamError::Closed, and bail out
                            // to let the outer loop reconnect.
                            return;
                        }
                        subscribers.insert(channel, tx);
                        let _ = ack.send(rx);
                    }
                    Some(Command::Unsubscribe { channel }) => {
                        subscribers.remove(&channel);
                        let frame = ClientFrame::Unsubscribe { ch: channel };
                        if write.send(Message::Text(encode_client_frame(&frame))).await.is_err() {
                            return;
                        }
                    }
                    Some(Command::Stop { session_id }) => {
                        let frame = ClientFrame::Stop { session_id };
                        if write.send(Message::Text(encode_client_frame(&frame))).await.is_err() {
                            return;
                        }
                    }
                    None => {
                        let _ = write.send(Message::Close(None)).await;
                        return;
                    }
                }
            }
            message = read.next() => {
                match message {
                    Some(Ok(Message::Text(text))) => {
                        handle_server_text(&text, subscribers).await;
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        if write.send(Message::Pong(payload)).await.is_err() {
                            return;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => return,
                    Some(Ok(_)) => {} // Pong / Binary / Frame: ignore.
                    Some(Err(error)) => {
                        tracing::debug!("magpie stream: WS read error: {error}");
                        return;
                    }
                }
            }
        }
    }
}

async fn handle_server_text(
    text: &str,
    subscribers: &mut HashMap<String, mpsc::Sender<StreamEvent>>,
) {
    let envelope = match parse_server_envelope(text) {
        Ok(envelope) => envelope,
        Err(error) => {
            tracing::debug!("magpie stream: ignoring malformed envelope: {error}");
            return;
        }
    };
    let Some(tx) = subscribers.get(&envelope.ch) else {
        return; // Not a channel we're tracking (e.g. `feed`, or already unsubscribed).
    };

    match envelope.body {
        EnvelopeBody::Event { event } => match serde_json::from_value::<AgentEvent>(event) {
            Ok(agent_event) => {
                let _ = tx.send(StreamEvent::Agent(agent_event)).await;
            }
            Err(error) => {
                tracing::debug!("magpie stream: ignoring undecodable agent event: {error}");
            }
        },
        EnvelopeBody::Control { control } => match parse_control_payload(&control) {
            Ok(ControlPayload::Terminal { reason }) => {
                let _ = tx.send(StreamEvent::Terminal { reason }).await;
            }
            Ok(ControlPayload::FeedReset { .. }) | Ok(ControlPayload::Unknown) => {}
            Err(error) => {
                tracing::debug!("magpie stream: ignoring undecodable control payload: {error}");
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── pure encode ──────────────────────────────────────────────────────

    #[test]
    fn encode_hello_frame() {
        let frame = ClientFrame::Hello {
            device_id: "bamboo_abc".to_string(),
            token: "bd1_xyz".to_string(),
        };
        let json: serde_json::Value = serde_json::from_str(&encode_client_frame(&frame)).unwrap();
        assert_eq!(json["type"], "hello");
        assert_eq!(json["device_id"], "bamboo_abc");
        assert_eq!(json["token"], "bd1_xyz");
    }

    #[test]
    fn encode_subscribe_frame_omits_since_when_none() {
        let frame = ClientFrame::Subscribe {
            ch: "agent.sess-1".to_string(),
            since: None,
        };
        let json: serde_json::Value = serde_json::from_str(&encode_client_frame(&frame)).unwrap();
        assert_eq!(json["type"], "subscribe");
        assert_eq!(json["ch"], "agent.sess-1");
        assert!(json.get("since").is_none());
    }

    #[test]
    fn encode_unsubscribe_and_stop_frames() {
        let frame = ClientFrame::Unsubscribe {
            ch: "agent.sess-1".to_string(),
        };
        let json: serde_json::Value = serde_json::from_str(&encode_client_frame(&frame)).unwrap();
        assert_eq!(json["type"], "unsubscribe");

        let frame = ClientFrame::Stop {
            session_id: "sess-1".to_string(),
        };
        let json: serde_json::Value = serde_json::from_str(&encode_client_frame(&frame)).unwrap();
        assert_eq!(json["type"], "stop");
        assert_eq!(json["session_id"], "sess-1");
    }

    // ── pure decode ──────────────────────────────────────────────────────

    #[test]
    fn parse_event_envelope() {
        let text = r#"{"ch":"agent.sess-1","seq":42,"event":{"type":"token","content":"Hi"}}"#;
        let envelope = parse_server_envelope(text).unwrap();
        assert_eq!(envelope.ch, "agent.sess-1");
        assert_eq!(envelope.seq, 42);
        match envelope.body {
            EnvelopeBody::Event { event } => {
                let agent_event: AgentEvent = serde_json::from_value(event).unwrap();
                match agent_event {
                    AgentEvent::Token { content } => assert_eq!(content, "Hi"),
                    other => panic!("unexpected: {other:?}"),
                }
            }
            other => panic!("expected Event body, got {other:?}"),
        }
    }

    #[test]
    fn parse_control_envelope_terminal() {
        let text =
            r#"{"ch":"agent.sess-1","seq":43,"control":{"type":"terminal","reason":"complete"}}"#;
        let envelope = parse_server_envelope(text).unwrap();
        match envelope.body {
            EnvelopeBody::Control { control } => {
                let payload = parse_control_payload(&control).unwrap();
                assert_eq!(
                    payload,
                    ControlPayload::Terminal {
                        reason: "complete".to_string()
                    }
                );
            }
            other => panic!("expected Control body, got {other:?}"),
        }
    }

    #[test]
    fn parse_control_envelope_feed_reset() {
        let text = r#"{"ch":"feed","seq":0,"control":{"type":"feed_reset","from_seq":1006}}"#;
        let envelope = parse_server_envelope(text).unwrap();
        match envelope.body {
            EnvelopeBody::Control { control } => {
                let payload = parse_control_payload(&control).unwrap();
                assert_eq!(payload, ControlPayload::FeedReset { from_seq: 1006 });
            }
            other => panic!("expected Control body, got {other:?}"),
        }
    }

    #[test]
    fn parse_control_envelope_unknown_type_does_not_error() {
        let control = serde_json::json!({ "type": "some_future_control" });
        let payload = parse_control_payload(&control).unwrap();
        assert_eq!(payload, ControlPayload::Unknown);
    }

    #[test]
    fn parse_server_envelope_rejects_malformed_json() {
        assert!(parse_server_envelope("not json{").is_err());
    }

    #[test]
    fn parse_server_envelope_rejects_a_body_with_neither_event_nor_control() {
        let text = r#"{"ch":"agent.sess-1","seq":1}"#;
        assert!(parse_server_envelope(text).is_err());
    }

    // ── ws url derivation ────────────────────────────────────────────────

    #[test]
    fn derive_ws_url_rewrites_http_to_ws() {
        let url = derive_ws_url("http://127.0.0.1:9560").unwrap();
        assert_eq!(url.as_str(), "ws://127.0.0.1:9560/v2/stream");
    }

    #[test]
    fn derive_ws_url_rewrites_https_to_wss() {
        let url = derive_ws_url("https://bamboo.example.com").unwrap();
        assert_eq!(url.as_str(), "wss://bamboo.example.com/v2/stream");
    }

    #[test]
    fn derive_ws_url_replaces_any_existing_path_and_query() {
        let url = derive_ws_url("http://127.0.0.1:9560/some/path?x=1").unwrap();
        assert_eq!(url.as_str(), "ws://127.0.0.1:9560/v2/stream");
    }

    #[test]
    fn derive_ws_url_rejects_unsupported_scheme() {
        let error = derive_ws_url("ftp://example.com").unwrap_err();
        assert!(matches!(error, StreamError::InvalidBaseUrl(_)));
    }

    #[test]
    fn derive_ws_url_rejects_unparseable_base_url() {
        let error = derive_ws_url("not a url").unwrap_err();
        assert!(matches!(error, StreamError::InvalidBaseUrl(_)));
    }

    // ── envelope routing (pure, no live socket) ─────────────────────────

    #[tokio::test]
    async fn handle_server_text_routes_event_to_the_matching_subscriber_only() {
        let mut subscribers = HashMap::new();
        let (tx, mut rx) = mpsc::channel(8);
        subscribers.insert("agent.sess-1".to_string(), tx);

        let text = r#"{"ch":"agent.sess-1","seq":1,"event":{"type":"token","content":"hi"}}"#;
        handle_server_text(text, &mut subscribers).await;

        let event = rx.try_recv().unwrap();
        match event {
            StreamEvent::Agent(AgentEvent::Token { content }) => assert_eq!(content, "hi"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn handle_server_text_ignores_events_for_untracked_channels() {
        let mut subscribers = HashMap::new();
        let (tx, mut rx) = mpsc::channel(8);
        subscribers.insert("agent.sess-1".to_string(), tx);

        let text = r#"{"ch":"agent.sess-OTHER","seq":1,"event":{"type":"token","content":"hi"}}"#;
        handle_server_text(text, &mut subscribers).await;

        assert!(
            rx.try_recv().is_err(),
            "must not receive an event for another channel"
        );
    }

    #[tokio::test]
    async fn handle_server_text_routes_terminal_control() {
        let mut subscribers = HashMap::new();
        let (tx, mut rx) = mpsc::channel(8);
        subscribers.insert("agent.sess-1".to_string(), tx);

        let text =
            r#"{"ch":"agent.sess-1","seq":2,"control":{"type":"terminal","reason":"complete"}}"#;
        handle_server_text(text, &mut subscribers).await;

        match rx.try_recv().unwrap() {
            StreamEvent::Terminal { reason } => assert_eq!(reason, "complete"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn handle_server_text_ignores_malformed_json_without_panicking() {
        let mut subscribers = HashMap::new();
        let (tx, mut rx) = mpsc::channel(8);
        subscribers.insert("agent.sess-1".to_string(), tx);

        handle_server_text("not json{", &mut subscribers).await;
        assert!(rx.try_recv().is_err());
    }

    // ── backoff schedule ─────────────────────────────────────────────────

    #[test]
    fn backoff_delay_starts_at_zero_and_grows_then_caps() {
        assert_eq!(backoff_delay(0), Duration::from_secs(0));
        assert_eq!(backoff_delay(1), Duration::from_secs(1));
        assert_eq!(
            backoff_delay(BACKOFF_SCHEDULE_SECS.len() - 1),
            Duration::from_secs(30)
        );
        // Out-of-range index clamps to the last (cap) entry.
        assert_eq!(backoff_delay(999), Duration::from_secs(30));
    }
}
