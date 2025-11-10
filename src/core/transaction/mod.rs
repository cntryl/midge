//! Transaction management for Midge
//!
//! This module contains all transaction-related functionality including:
//! - Optimistic concurrency control (OCC) via `TransactionManager`
//! - Transaction-aware engine operations via `EngineTransaction`
//! - Conflict detection and deadlock prevention
//!
//! The transaction system uses optimistic locking with conflict detection at commit time.

pub mod engine_transaction;
pub mod manager;

// Re-export public types
pub use engine_transaction::EngineTransaction;
pub use manager::{Key, TransactionManager};
