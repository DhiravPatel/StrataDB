//! Canonical StrataDB event model (spec §2.2, §6).
//!
//! * [`Value`] — typed attribute values; attributes are *not* stringly typed.
//! * [`Event`] — the canonical log event.
//! * [`codec`] — versioned binary encoding used by the WAL and segments.
//! * [`validate`] — ingestion limits and validation rules.
//! * [`json`] — mapping from JSON/NDJSON payloads to canonical events.

pub mod codec;
pub mod event;
pub mod json;
pub mod otlp;
pub mod otlp_proto;
pub mod validate;
pub mod value;

pub use event::{Event, Severity, EVENT_SCHEMA_VERSION};
pub use validate::Limits;
pub use value::Value;
