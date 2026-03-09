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

pub use iterator::{Direction, Iterator as ScanIterator};
pub use kv::{Key, Value};
pub use options::{Goal, MemoryBudget, OpenOptions, RecoveryPolicy, Storage, WorkloadProfile};
pub use query::Query;
pub use transaction::{Transaction, TransactionMode, WriteIntent};
pub use write_options::WriteOptions;

// Internal types needed by runtime but not part of public API
pub(crate) use options::Durability;
pub(crate) use write_options::DurabilityPolicy;
