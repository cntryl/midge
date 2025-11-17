//! Database locking to prevent concurrent writers.
//!
//! This module provides exclusive access control for database directories,
//! preventing multiple writers from corrupting the same database.
//!
//! The locking behavior is abstracted behind the `DbLock` trait, with
//! implementations for different storage backends (local file, cloud lease).
//! Use the factory functions to create appropriate locks for your storage mode.

mod cloud;
mod local;
mod meta;
mod renewal;
mod traits;

// Re-export public API
pub use meta::LockMeta;
pub use traits::DbLock;

// Factory functions for creating locks (implementation details hidden)
use crate::cloud::StorageBackend;
use std::path::Path;
use std::sync::Arc;

/// Create a local file-based database lock.
///
/// This is appropriate for local storage modes where file system
/// locking provides sufficient exclusivity.
pub fn create_local_lock(db_path: &Path, ttl_ms: u32) -> Box<dyn DbLock> {
    Box::new(local::LocalFileLock::new(db_path, ttl_ms))
}

/// Create a cloud-based distributed database lock.
///
/// This is appropriate for cloud storage modes where distributed
/// locking is required across multiple instances.
pub fn create_cloud_lock(
    backend: Arc<dyn StorageBackend>,
    lock_key: String,
    ttl_ms: u32,
) -> Box<dyn DbLock> {
    Box::new(cloud::CloudLeaseLock::new(backend, lock_key, ttl_ms))
}
