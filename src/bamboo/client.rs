//! Async HTTP client over Bamboo's public `/api/v1` surface.
//!
//! Auth: every request carries `Authorization: Bearer <token>` +
//! `X-Device-Id: <device_id>` — device-token auth (v2-P2, bamboo #181),
//! verified against
//! `crates/app/bamboo-server/src/handlers/settings/access_control.rs:271-273`
//! (`DEVICE_TOKEN_PREFIX = "bd1_"`, `DEVICE_ID_HEADER = "x-device-id"`) and
//! `:367-387` (`presented_device_token`: `Authorization: Bearer <token>` +
//! the device id header, both required).
//!
//! Errors never carry the token: [`ClientError`] messages are built from the
//! response status/body and a sanitized network-error string (see
//! [`sanitize`]) — the token is masked out of any error text before it is
//! returned or logged, mirroring bamboo's own outbound masking discipline
//! (`bamboo-memory: Bamboo outbound masking scan`).
//!
//! Retry policy: a single retry on a *transient network* error only (connect
//! failure, request timeout) — never on an HTTP error response (4xx/5xx),
//! which is the server's authoritative answer and must not be retried
//! blindly (e.g. retrying a `POST /chat` could double-send a message).

use std::time::Duration;

use reqwest::Method;
use serde::Serialize;

use super::types::{
    ChatRequest, ChatResponse, CreateSessionRequest, CreateSessionResponse,
    ExecuteDefaultsResponse, ExecuteRequest, ExecuteResponse, RespondPendingResponse,
    RespondRequest, RespondSubmitResponse, RunningSessionsResponse, StopResponse,
};
use crate::config::BambooConfig;

/// Request timeout applied to every call.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// HTTP header carrying the device id companion for the bearer device token.
/// Verified against `access_control.rs:273` (`DEVICE_ID_HEADER = "x-device-id"`).
const DEVICE_ID_HEADER: &str = "X-Device-Id";

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("network error calling {method} {path}: {message}")]
    Network {
        method: &'static str,
        path: String,
        message: String,
    },
    #[error("bamboo {method} {path} returned {status}: {body}")]
    Api {
        method: &'static str,
        path: String,
        status: u16,
        body: String,
    },
    #[error("failed to decode response from {method} {path}: {message}")]
    Decode {
        method: &'static str,
        path: String,
        message: String,
    },
    #[error("invalid bamboo base_url {base_url:?}: {message}")]
    InvalidBaseUrl { base_url: String, message: String },
}

/// Replace every occurrence of `secret` in `text` with a fixed mask. Used to
/// keep the device token out of error/log text even if some underlying
/// library ever echoes a header value back into an error message.
fn sanitize(text: &str, secret: &str) -> String {
    if secret.is_empty() {
        return text.to_string();
    }
    text.replace(secret, "***REDACTED***")
}

/// Whether a `reqwest::Error` is a *transient* network failure eligible for
/// the single-retry policy (connect refused/reset, DNS blip, timeout) as
/// opposed to an application-level failure (a decode error, a redirect
/// policy violation, a builder error) which retrying can never fix.
fn is_transient(error: &reqwest::Error) -> bool {
    error.is_connect() || error.is_timeout()
}

/// Bamboo HTTP client. Cheap to clone (holds an `Arc`-backed `reqwest::Client`
/// internally); construct once and share.
#[derive(Debug, Clone)]
pub struct BambooClient {
    http: reqwest::Client,
    base_url: String,
    device_id: String,
    token: String,
}

impl BambooClient {
    pub fn new(config: &BambooConfig) -> Result<Self, ClientError> {
        let base_url = config.base_url.trim_end_matches('/').to_string();
        // Fail fast on a malformed base_url rather than surfacing an opaque
        // reqwest error on the first request.
        reqwest::Url::parse(&base_url).map_err(|error| ClientError::InvalidBaseUrl {
            base_url: base_url.clone(),
            message: error.to_string(),
        })?;

        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|error| ClientError::InvalidBaseUrl {
                base_url: base_url.clone(),
                message: error.to_string(),
            })?;

        Ok(Self {
            http,
            base_url,
            device_id: config.device_id.clone(),
            token: config.token.clone(),
        })
    }

    /// The base URL this client was constructed with (already trimmed of a
    /// trailing slash). Used by [`crate::bamboo::stream`] to derive the WS URL.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn device_id(&self) -> &str {
        &self.device_id
    }

    pub fn token(&self) -> &str {
        &self.token
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    /// Sanitize `text` against this client's token — see the module doc.
    fn sanitize(&self, text: &str) -> String {
        sanitize(text, &self.token)
    }

    async fn send_json<B: Serialize + ?Sized, R: serde::de::DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<&B>,
    ) -> Result<R, ClientError> {
        let method_str = method_name(&method);
        let mut attempt = 0u8;
        loop {
            attempt += 1;
            let mut request = self
                .http
                .request(method.clone(), self.url(path))
                .header(
                    reqwest::header::AUTHORIZATION,
                    format!("Bearer {}", self.token),
                )
                .header(DEVICE_ID_HEADER, &self.device_id);
            if let Some(body) = body {
                request = request.json(body);
            }

            match request.send().await {
                Ok(response) => return self.handle_response(method_str, path, response).await,
                Err(error) => {
                    let transient = is_transient(&error);
                    let message = self.sanitize(&error.to_string());
                    if transient && attempt == 1 {
                        tracing::debug!(
                            "bamboo client: transient network error on {method_str} {path}, \
                             retrying once: {message}"
                        );
                        continue;
                    }
                    return Err(ClientError::Network {
                        method: method_str,
                        path: path.to_string(),
                        message,
                    });
                }
            }
        }
    }

    async fn handle_response<R: serde::de::DeserializeOwned>(
        &self,
        method: &'static str,
        path: &str,
        response: reqwest::Response,
    ) -> Result<R, ClientError> {
        let status = response.status();
        let body_bytes = response
            .bytes()
            .await
            .map_err(|error| ClientError::Decode {
                method,
                path: path.to_string(),
                message: self.sanitize(&error.to_string()),
            })?;

        if !status.is_success() {
            let body_text = String::from_utf8_lossy(&body_bytes).to_string();
            return Err(ClientError::Api {
                method,
                path: path.to_string(),
                status: status.as_u16(),
                body: self.sanitize(&body_text),
            });
        }

        serde_json::from_slice(&body_bytes).map_err(|error| ClientError::Decode {
            method,
            path: path.to_string(),
            message: self.sanitize(&error.to_string()),
        })
    }

    /// `GET /api/v1/execute/defaults`
    /// (execute/defaults.rs:68 — `pub async fn handler`).
    pub async fn execute_defaults(&self) -> Result<ExecuteDefaultsResponse, ClientError> {
        self.send_json::<(), _>(Method::GET, "/api/v1/execute/defaults", None)
            .await
    }

    /// `POST /api/v1/chat` (routes/agent.rs:80).
    pub async fn chat(&self, request: &ChatRequest) -> Result<ChatResponse, ClientError> {
        self.send_json(Method::POST, "/api/v1/chat", Some(request))
            .await
    }

    /// `POST /api/v1/sessions` (routes/agent.rs:103; crud/create.rs:24).
    pub async fn create_session(
        &self,
        request: &CreateSessionRequest,
    ) -> Result<CreateSessionResponse, ClientError> {
        self.send_json(Method::POST, "/api/v1/sessions", Some(request))
            .await
    }

    /// `POST /api/v1/execute/{session_id}` (routes/agent.rs:204;
    /// execute/handler/mod.rs:24).
    pub async fn execute(
        &self,
        session_id: &str,
        request: &ExecuteRequest,
    ) -> Result<ExecuteResponse, ClientError> {
        let path = format!("/api/v1/execute/{session_id}");
        self.send_json(Method::POST, &path, Some(request)).await
    }

    /// `POST /api/v1/stop/{session_id}` (routes/agent.rs:214; stop/handler.rs:9).
    pub async fn stop(&self, session_id: &str) -> Result<StopResponse, ClientError> {
        let path = format!("/api/v1/stop/{session_id}");
        self.send_json::<(), _>(Method::POST, &path, None).await
    }

    /// `POST /api/v1/respond/{session_id}` (routes/agent.rs:234;
    /// respond/handlers/submit.rs:18).
    pub async fn respond(
        &self,
        session_id: &str,
        request: &RespondRequest,
    ) -> Result<RespondSubmitResponse, ClientError> {
        let path = format!("/api/v1/respond/{session_id}");
        self.send_json(Method::POST, &path, Some(request)).await
    }

    /// `GET /api/v1/respond/{session_id}/pending` (routes/agent.rs:238;
    /// respond/handlers/pending.rs:13).
    pub async fn respond_pending(
        &self,
        session_id: &str,
    ) -> Result<RespondPendingResponse, ClientError> {
        let path = format!("/api/v1/respond/{session_id}/pending");
        self.send_json::<(), _>(Method::GET, &path, None).await
    }

    /// `GET /api/v1/runs/active` (routes/agent.rs:99; crud/running_snapshot.rs:12).
    pub async fn runs_active(&self) -> Result<RunningSessionsResponse, ClientError> {
        self.send_json::<(), _>(Method::GET, "/api/v1/runs/active", None)
            .await
    }
}

fn method_name(method: &Method) -> &'static str {
    match *method {
        Method::GET => "GET",
        Method::POST => "POST",
        Method::PUT => "PUT",
        Method::PATCH => "PATCH",
        Method::DELETE => "DELETE",
        _ => "OTHER",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn client_for(server: &MockServer, token: &str) -> BambooClient {
        BambooClient::new(&BambooConfig {
            base_url: server.uri(),
            device_id: "bamboo_test123".to_string(),
            token: token.to_string(),
        })
        .unwrap()
    }

    #[tokio::test]
    async fn execute_defaults_round_trips_and_sends_auth_headers() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/execute/defaults"))
            .and(header("Authorization", "Bearer bd1_secret"))
            .and(header("X-Device-Id", "bamboo_test123"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "model": "gpt-5",
                "provider": "openai",
                "provider_type": "openai",
                "reasoning_effort": "medium",
                "system_prompt": "You are Bamboo.",
                "base_system_prompt": "You are Bamboo.",
                "workspace_path": null,
                "gold_config": null,
                "fast_model": "gpt-5-mini",
                "background_model": null,
                "summarization_model": null
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = client_for(&server, "bd1_secret");
        let response = client.execute_defaults().await.unwrap();
        assert_eq!(response.model.as_deref(), Some("gpt-5"));
        assert_eq!(response.fast_model.as_deref(), Some("gpt-5-mini"));
    }

    #[tokio::test]
    async fn chat_posts_body_and_round_trips_response() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/chat"))
            .and(body_json(serde_json::json!({ "message": "hi" })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "sess-1",
                "stream_url": "/api/v1/events/sess-1",
                "status": "streaming"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = client_for(&server, "bd1_secret");
        let request = ChatRequest {
            message: "hi".to_string(),
            ..Default::default()
        };
        let response = client.chat(&request).await.unwrap();
        assert_eq!(response.session_id, "sess-1");
        assert_eq!(response.status, "streaming");
    }

    #[tokio::test]
    async fn create_session_round_trips() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/sessions"))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                "session": {
                    "id": "sess-2",
                    "title": "New session",
                    "model": "gpt-5",
                    "is_running": false,
                    "message_count": 0,
                    "created_at": "2026-07-14T00:00:00Z",
                    "updated_at": "2026-07-14T00:00:00Z"
                }
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = client_for(&server, "bd1_secret");
        let response = client
            .create_session(&CreateSessionRequest::default())
            .await
            .unwrap();
        assert_eq!(response.session.id, "sess-2");
    }

    #[tokio::test]
    async fn execute_round_trips_already_running_status() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/execute/sess-3"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "session_id": "sess-3",
                "status": "already_running",
                "events_url": "/api/v1/events/sess-3",
                "sync": null,
                "run_id": "run-1"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = client_for(&server, "bd1_secret");
        let response = client
            .execute("sess-3", &ExecuteRequest::default())
            .await
            .unwrap();
        assert!(response.is_already_running());
        assert_eq!(response.run_id.as_deref(), Some("run-1"));
    }

    #[tokio::test]
    async fn stop_round_trips() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/stop/sess-4"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "success": true,
                "message": "Agent execution stopped"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = client_for(&server, "bd1_secret");
        let response = client.stop("sess-4").await.unwrap();
        assert!(response.success);
    }

    #[tokio::test]
    async fn respond_round_trips() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/respond/sess-5"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "success": true,
                "message": "Response recorded. Agent loop will continue.",
                "response": "yes",
                "auto_resume_status": "resumed",
                "run_id": "run-2"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = client_for(&server, "bd1_secret");
        let request = RespondRequest {
            response: "yes".to_string(),
            ..Default::default()
        };
        let response = client.respond("sess-5", &request).await.unwrap();
        assert_eq!(response.auto_resume_status, "resumed");
    }

    #[tokio::test]
    async fn respond_pending_round_trips_false_shape() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/respond/sess-6/pending"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({ "has_pending_question": false })),
            )
            .expect(1)
            .mount(&server)
            .await;

        let client = client_for(&server, "bd1_secret");
        let response = client.respond_pending("sess-6").await.unwrap();
        assert!(!response.has_pending_question);
    }

    #[tokio::test]
    async fn runs_active_round_trips() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/runs/active"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "sessions": [{
                    "session_id": "sess-7",
                    "run_id": "run-3",
                    "started_at": "2026-07-14T00:00:00Z",
                    "round_count": 2,
                    "last_critical_events": [],
                    "running_child_session_ids": []
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = client_for(&server, "bd1_secret");
        let response = client.runs_active().await.unwrap();
        assert_eq!(response.sessions.len(), 1);
        assert_eq!(response.sessions[0].session_id, "sess-7");
    }

    #[tokio::test]
    async fn api_error_never_includes_the_token_in_the_error_text() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/execute/defaults"))
            .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
                "error": {
                    "message": "invalid device token bd1_supersecrettoken",
                    "type": "api_error"
                }
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = client_for(&server, "bd1_supersecrettoken");
        let error = client.execute_defaults().await.unwrap_err();
        let text = error.to_string();
        assert!(
            !text.contains("bd1_supersecrettoken"),
            "error text must never contain the raw token: {text}"
        );
        assert!(
            text.contains("REDACTED"),
            "expected a redaction marker: {text}"
        );
    }

    #[tokio::test]
    async fn network_error_is_sanitized_and_never_includes_the_token() {
        // Point at a server that isn't listening (immediate connect refusal —
        // no retry-delay flakiness) to exercise the Network error path.
        let client = BambooClient::new(&BambooConfig {
            base_url: "http://127.0.0.1:1".to_string(),
            device_id: "bamboo_test123".to_string(),
            token: "bd1_supersecrettoken".to_string(),
        })
        .unwrap();

        let error = client.execute_defaults().await.unwrap_err();
        let text = error.to_string();
        assert!(!text.contains("bd1_supersecrettoken"));
        assert!(matches!(error, ClientError::Network { .. }));
    }

    #[test]
    fn invalid_base_url_is_rejected_at_construction() {
        let error = BambooClient::new(&BambooConfig {
            base_url: "not a url".to_string(),
            device_id: "d".to_string(),
            token: "t".to_string(),
        })
        .unwrap_err();
        assert!(matches!(error, ClientError::InvalidBaseUrl { .. }));
    }

    #[test]
    fn sanitize_masks_every_occurrence() {
        let text = "token bd1_x leaked, again bd1_x here";
        assert_eq!(
            sanitize(text, "bd1_x"),
            "token ***REDACTED*** leaked, again ***REDACTED*** here"
        );
    }

    #[test]
    fn sanitize_is_a_no_op_for_an_empty_secret() {
        assert_eq!(sanitize("hello", ""), "hello");
    }
}
