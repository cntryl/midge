//! Database locking to prevent concurrent writers.
//!
//! This module provides exclusive access control for database directories,
//! preventing multiple writers from corrupting the same database.
//!
//! Two implementations:
//! - `LocalFileLock`: File-based lock for local/memory storage modes
//! - `CloudLeaseLock`: Distributed lease for cloud-backed storage mode
//!
//! Both use the same semantics:
//! - Acquisition with exponential backoff
//! - Heartbeat renewal (every ttl/2)
//! - Automatic read-only fallback on renewal failure
//! - Graceful release on shutdown

mod traits;
mod meta;
mod renewal;
mod local;
mod cloud;

// Re-export public API
pub use traits::DbLock;
pub use meta::LockMeta;
pub use local::LocalFileLock;
pub use cloud::CloudLeaseLock;
