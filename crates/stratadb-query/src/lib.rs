//! StrataDB query engine (spec §13, epic E; ADR-0006).
//!
//! `parse` → [`plan::Plan`] (validation, projection, pruning) → [`exec::execute`]
//! over a tenant-scoped [`stratadb_storage::Snapshot`]. The language reference
//! is in `docs/query-language/`.

pub mod ast;
pub mod eval;
pub mod exec;
pub mod lexer;
pub mod parser;
pub mod plan;

pub use eval::Datum;
pub use exec::{execute, QueryOptions, QueryOutput, QueryStats};
