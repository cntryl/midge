//! Core storage engine module.
//!
//! The MidgeEngine implementation is organized across multiple files:
//! - `types.rs` - Result enums (InsertResult, CasResult)
//! - `column_family.rs` - Column family management (ColumnFamily, ColumnFamilySet)
//! - `factory.rs` - Engine construction and initialization helpers
//! - `engine.rs` - Main MidgeEngine struct and internal helpers
//! - `operations/` - Focused operation modules:
//!   - `reads.rs` - Point reads and range scans
//!   - (more to be added: writes, mutations, transactions, etc.)

mod cf_manager;
mod column_family;
mod coordination;
#[allow(clippy::module_inception)]
mod engine;
pub(crate) mod factory;
pub mod types;

// Re-export public types
pub use types::{CasResult, InsertResult};

pub(crate) mod operations;

// Re-export the engine and its public API
pub use engine::*;

// Re-export Query and Snapshot from API
pub use crate::api::query::Query;
pub use crate::api::snapshot::Snapshot;
