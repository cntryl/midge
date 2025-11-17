//! Core storage engine module.
//!
//! The MidgeEngine implementation is organized across multiple files:
//! - `types.rs` - Result enums (InsertResult, CasResult)
//! - `column_family.rs` - Column family management (ColumnFamily, ColumnFamilySet)
//! - `factory.rs` - Engine construction and initialization helpers
//! - `core.rs` - Main MidgeEngine struct and internal helpers
//! - `state.rs` - Engine state management and initialization
//! - `operations/` - Focused operation modules (reads, writes, transactions, etc.)
//! - `kv_store_adapter.rs` - KvStore trait adapter for external API compatibility
//! - `flush_manager.rs` - Memtable flushing coordination

mod cf_manager;
pub(crate) mod column_family;
mod core;
pub(crate) mod factory;
mod flush_manager;
mod kv_store_adapter;
pub mod state;
pub mod types;

// Re-export adapters for public use
pub use kv_store_adapter::KvStoreAdapter;

// Re-export public types
pub use types::{CasResult, InsertResult};

pub(crate) mod operations;

// Re-export the engine and its public API
pub use core::*;

// Re-export Query and Snapshot from API
pub use crate::api::query::Query;
pub use crate::api::snapshot::Snapshot;
