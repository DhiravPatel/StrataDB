//! StrataDB query engine (spec §13, epic E; ADR-0006).
//!
//! `parse` → [`plan::Plan`] (validation, projection, pruning) → [`exec::execute`]
//! over a tenant-scoped [`stratadb_storage::Snapshot`]. The language reference
//! is in `docs/query-language/`.

pub mod ast;
pub mod eval;
pub mod exec;
pub mod fingerprint;
mod groups;
pub mod lexer;
pub mod page;
pub mod parser;
pub mod plan;
pub mod pool;
mod vector;

pub use ast::Table;
pub use eval::Datum;
pub use exec::{
    execute, execute_page, Catalog, Page, PageRequest, QueryOptions, QueryOutput, QueryStats,
};
pub use fingerprint::{fingerprint, Fingerprint};
pub use pool::{configure_scan_pool, scan_pool, PoolStats};
