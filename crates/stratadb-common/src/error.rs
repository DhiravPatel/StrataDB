//! Error model (ticket A-002).
//!
//! Every error carries a [`ErrorKind`] so that API layers can map it to a
//! transport status and tell clients whether a retry is safe. Variants keep the
//! original context; errors are never flattened into generic strings at the
//! storage layer.

use std::fmt;

/// Stable error category. Exposed through APIs; do not renumber or repurpose.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    /// Caller supplied an invalid request or record. Not retryable as-is.
    InvalidInput,
    /// A requested object does not exist.
    NotFound,
    /// An object that must be unique already exists.
    AlreadyExists,
    /// Persistent data failed validation (checksum, framing, invariants).
    Corruption,
    /// Operating-system I/O failure.
    Io,
    /// A bounded resource (queue, memory budget, disk) is exhausted. Retryable later.
    ResourceExhausted,
    /// The component is temporarily unable to serve (shutting down, recovering). Retryable.
    Unavailable,
    /// The outcome of a write is unknown: it may or may not have been committed.
    /// Clients should retry with the same idempotency key.
    Indeterminate,
    /// Feature, syntax, or format version not supported by this build.
    Unsupported,
    /// Caller is authenticated but lacks permission.
    PermissionDenied,
    /// Caller identity could not be established.
    Unauthenticated,
    /// Operation cancelled by request.
    Cancelled,
    /// Operation exceeded its deadline.
    Timeout,
    /// Invariant violation inside StrataDB. Always a bug.
    Internal,
}

impl ErrorKind {
    /// Whether retrying the identical request may succeed.
    pub fn is_retryable(self) -> bool {
        matches!(
            self,
            ErrorKind::ResourceExhausted
                | ErrorKind::Unavailable
                | ErrorKind::Indeterminate
                | ErrorKind::Timeout
        )
    }

    pub fn as_str(self) -> &'static str {
        match self {
            ErrorKind::InvalidInput => "invalid_input",
            ErrorKind::NotFound => "not_found",
            ErrorKind::AlreadyExists => "already_exists",
            ErrorKind::Corruption => "corruption",
            ErrorKind::Io => "io",
            ErrorKind::ResourceExhausted => "resource_exhausted",
            ErrorKind::Unavailable => "unavailable",
            ErrorKind::Indeterminate => "indeterminate",
            ErrorKind::Unsupported => "unsupported",
            ErrorKind::PermissionDenied => "permission_denied",
            ErrorKind::Unauthenticated => "unauthenticated",
            ErrorKind::Cancelled => "cancelled",
            ErrorKind::Timeout => "timeout",
            ErrorKind::Internal => "internal",
        }
    }
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The StrataDB error type.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error while {context}: {source}")]
    Io {
        context: String,
        #[source]
        source: std::io::Error,
    },
    #[error("corruption detected in {location}: {detail}")]
    Corruption { location: String, detail: String },
    #[error("unsupported {what} format version {found} (supported: {supported})")]
    FormatVersion {
        what: &'static str,
        found: u32,
        supported: String,
    },
    #[error("{kind}: {message}")]
    Other { kind: ErrorKind, message: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    pub fn kind(&self) -> ErrorKind {
        match self {
            Error::Io { source, .. } => match source.kind() {
                // Disk full must surface as a resource problem, not a generic I/O failure,
                // so ingestion can apply backpressure (spec §7.4, §28.4).
                std::io::ErrorKind::StorageFull => ErrorKind::ResourceExhausted,
                _ => ErrorKind::Io,
            },
            Error::Corruption { .. } => ErrorKind::Corruption,
            Error::FormatVersion { .. } => ErrorKind::Unsupported,
            Error::Other { kind, .. } => *kind,
        }
    }

    pub fn is_retryable(&self) -> bool {
        self.kind().is_retryable()
    }

    pub fn io(context: impl Into<String>, source: std::io::Error) -> Self {
        Error::Io {
            context: context.into(),
            source,
        }
    }

    pub fn corruption(location: impl Into<String>, detail: impl Into<String>) -> Self {
        Error::Corruption {
            location: location.into(),
            detail: detail.into(),
        }
    }

    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Error::Other {
            kind,
            message: message.into(),
        }
    }

    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidInput, message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::NotFound, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Internal, message)
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Unavailable, message)
    }

    pub fn resource_exhausted(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::ResourceExhausted, message)
    }

    pub fn indeterminate(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Indeterminate, message)
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Unsupported, message)
    }
}

/// Attach context to `std::io::Result` values.
pub trait IoResultExt<T> {
    fn ctx(self, context: impl FnOnce() -> String) -> Result<T>;
}

impl<T> IoResultExt<T> for std::io::Result<T> {
    fn ctx(self, context: impl FnOnce() -> String) -> Result<T> {
        self.map_err(|e| Error::io(context(), e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryability_is_explicit() {
        assert!(ErrorKind::ResourceExhausted.is_retryable());
        assert!(ErrorKind::Indeterminate.is_retryable());
        assert!(!ErrorKind::Corruption.is_retryable());
        assert!(!ErrorKind::InvalidInput.is_retryable());
    }

    #[test]
    fn disk_full_maps_to_resource_exhausted() {
        let e = Error::io(
            "appending",
            std::io::Error::from(std::io::ErrorKind::StorageFull),
        );
        assert_eq!(e.kind(), ErrorKind::ResourceExhausted);
        assert!(e.to_string().contains("appending"));
    }
}
