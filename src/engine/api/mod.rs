//! Engine API - canonical public interfaces
//!
//! IMPORTANT: This module intentionally exposes a *minimal* surface.
//! All data operations happen on `Transaction`. The `Engine` must not provide
//! alternate entry points, helpers, or convenience APIs.

// Canonical modules
mod kv;
mod options;
mod transaction;
mod write_options;

// Scan API - required public types
pub mod iterator;
pub mod query;

pub use crate::types::ConflictPolicy;
pub use iterator::{Direction, Iterator as ScanIterator};
pub use kv::{Key, Value};
pub use options::{
    BlockCachePolicy, CloudWritePolicy, Goal, MemoryBudget, OpenOptions, OpenOptionsBuilder,
    RecoveryPolicy, Storage, WorkloadProfile,
};
pub use query::Query;
pub(crate) use transaction::TransactionInit;
pub use transaction::{Transaction, TransactionMode};
pub use write_options::WriteOptions;
