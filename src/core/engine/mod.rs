//! Core storage engine module.
//!
//! The MidgeEngine implementation is organized across multiple files:
//! - `types.rs` - Result enums (InsertResult, CasResult)
//! - `column_family.rs` - Column family management (ColumnFamily, ColumnFamilySet)
//! - `factory.rs` - Engine construction and initialization helpers
//! - `engine.rs` - Main MidgeEngine implementation
//!
//! The engine.rs file is large but logically organized into sections:
//! - Construction & initialization (delegated to factory.rs)
//! - Internal helpers (memtable access, manifest, WAL)
//! - Flush & compaction
//! - Column family management
//! - Read operations (get, multi_get, scan)
//! - Write operations (put, delete, write_batch)
//! - Advanced operations (merge, CAS, insert, batch mutations)
//! - Transactions
//! - Snapshots
//! - Checkpoints & lifecycle
//! - Observability (metrics, cache stats)

mod column_family;
#[allow(clippy::module_inception)]
mod engine;
pub(crate) mod factory;
pub mod types;

// Re-export public types
pub use types::{CasResult, InsertResult};

// Re-export the engine and its public API
pub use engine::*;

// Re-export Query and Snapshot from API
pub use crate::api::query::Query;
pub use crate::api::snapshot::Snapshot;
