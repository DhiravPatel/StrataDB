//! Shared foundations for StrataDB crates.
//!
//! This crate must not depend on any server, network, or storage-engine code
//! (spec §5.2). Everything here is small, deterministic, and heavily tested
//! because every persistent format builds on it.

pub mod cache;
pub mod checksum;
pub mod codec;
pub mod cpu;
pub mod error;
pub mod failpoint;
pub mod fsutil;
pub mod ids;
pub mod io;
pub mod memory;
pub mod metrics;
#[cfg(feature = "simfs")]
pub mod simfs;
pub mod state;
pub mod threads;
pub mod time;
pub mod workload;

pub use error::{Error, ErrorKind, IoResultExt, Result};
pub use ids::TenantId;
