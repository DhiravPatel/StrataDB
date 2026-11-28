//! Studio errors and their JSON form.
//!
//! Every error the studio returns to the browser is a JSON object:
//! `{"error": kind, "message": text, "retryable": bool, "source": "studio" | "upstream", ...}`.
//! Upstream (StrataDB) errors additionally carry `upstream_status` and, when
//! the server sent them, `code` and `retry_after_ms` (docs/studio/README.md).

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{json, Value};

/// An error produced by the studio itself (not by a StrataDB server).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StudioError {
    pub status: StatusCode,
    pub kind: &'static str,
    pub message: String,
}

pub type Result<T> = std::result::Result<T, StudioError>;

impl StudioError {
    pub fn new(status: StatusCode, kind: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            kind,
            message: message.into(),
        }
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "invalid_input", message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", message)
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "already_exists", message)
    }

    pub fn unauthenticated(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "unauthenticated", message)
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, "permission_denied", message)
    }

    pub fn exhausted(message: impl Into<String>) -> Self {
        Self::new(StatusCode::TOO_MANY_REQUESTS, "resource_exhausted", message)
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(StatusCode::SERVICE_UNAVAILABLE, "unavailable", message)
    }

    pub fn bad_gateway(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_GATEWAY, "unavailable", message)
    }

    pub fn timeout(message: impl Into<String>) -> Self {
        Self::new(StatusCode::GATEWAY_TIMEOUT, "timeout", message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", message)
    }

    pub fn io(context: &str, e: &std::io::Error) -> Self {
        Self::internal(format!("{context}: {e}"))
    }

    fn retryable(&self) -> bool {
        matches!(
            self.status,
            StatusCode::TOO_MANY_REQUESTS
                | StatusCode::SERVICE_UNAVAILABLE
                | StatusCode::BAD_GATEWAY
                | StatusCode::GATEWAY_TIMEOUT
        )
    }

    pub fn body(&self) -> Value {
        json!({
            "error": self.kind,
            "message": self.message,
            "retryable": self.retryable(),
            "source": "studio",
        })
    }
}

impl std::fmt::Display for StudioError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.kind, self.message)
    }
}

impl std::error::Error for StudioError {}

impl IntoResponse for StudioError {
    fn into_response(self) -> Response {
        (self.status, Json(self.body())).into_response()
    }
}
