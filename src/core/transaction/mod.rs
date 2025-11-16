//! Transaction management for Midge
//!
//! This module contains all transaction-related functionality including:
//! - Optimistic concurrency control (OCC) via `TransactionManager`
//! - Transaction-aware engine operations via `EngineTransaction`
//! - Conflict detection and deadlock prevention
//! - Spill-to-disk for large transactions via `SpillManager`
//! - Conflict tracking for read/write sets via `ConflictTracker`
//!
//! The transaction system uses optimistic locking with conflict detection at commit time.

pub mod conflict_tracking;
pub(crate) mod core;
pub mod engine_transaction;
pub mod manager;
pub mod spill;

// Re-export public types
pub use conflict_tracking::ConflictTracker;
pub use engine_transaction::EngineTransaction;
pub use manager::{Key, TransactionController};
pub use spill::SpillManager;

// Internal re-export for use within core module
pub(crate) use core::Transaction;
