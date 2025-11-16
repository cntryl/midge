//! Core storage engine module.
//!
//! The MidgeEngine implementation is organized across multiple files:
//! - `types.rs` - Result enums (InsertResult, CasResult)
//! - `column_family.rs` - Column family management (ColumnFamily, ColumnFamilySet)
//! - `factory.rs` - Engine construction and initialization helpers
//! - `engine.rs` - Main MidgeEngine struct and internal helpers
//! - `state.rs` - Engine state management and initialization
//! - `operations/` - Focused operation modules (reads, writes, transactions, etc.)
//! - `adapters/` - Trait adapters for external API compatibility (KvStore, etc.)

mod adapters;
mod cf_manager;
pub(crate) mod column_family;
mod coordination;
mod core;
pub(crate) mod factory;
pub mod state;
pub mod types;

// Re-export adapters for public use
pub use adapters::KvStoreAdapter;

// Re-export public types
pub use types::{CasResult, InsertResult};

pub(crate) mod operations;

// Re-export the engine and its public API
pub use core::*;

// Re-export Query and Snapshot from API
pub use crate::api::query::Query;
pub use crate::api::snapshot::Snapshot;
