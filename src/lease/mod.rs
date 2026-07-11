//! Primary instance exclusivity via distributed leases.
//!
//! This module enforces the critical safety invariant:
//! **"At most one Midge instance holds the primary lease at any time."**
//!
//! Prevents split-brain scenarios where multiple instances write to the same storage,
//! which could lead to data corruption or inconsistent state.
//!
//! ## Design
//!
//! - **Fencing lease**: Not advisory—enforced by the storage backend
//! - **TTL-based**: Lease expires if not renewed (handles crashes gracefully)
//! - **Heartbeat loop**: Continuous renewal during normal operation
//! - **Fail-stop semantics**: Loss of lease immediately stops accepting writes
//!
//! ## Backends
//!
//! - **Cloud storage**: Preferred for distributed deployments (blob leases, conditional writes)
//! - **Filesystem**: Local-only fallback using exclusive file locks (`flock`)
//!
//! ## Usage
//!
//! Lease acquisition MUST occur before engine initialization:

mod cloud;
mod filesystem;
pub mod fs_leader_store;
mod heartbeat;
mod traits;

pub use cloud::{CloudLeaseConfig, CloudStorageLease};
pub use filesystem::FileSystemLease;
pub use heartbeat::LeaseHeartbeat;
pub use traits::{LeaderStore, LeaseError, LeaseGuard, PrimaryLease};

use crate::config::Storage;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

static INMEM_LEASE_COUNTER: AtomicU64 = AtomicU64::new(0);

// Keep the dependency direction common <- lease: the higher lease layer owns
// conversion of its error into the shared public error type.
impl From<LeaseError> for crate::common::MidgeError {
    fn from(error: LeaseError) -> Self {
        Self::Fenced(error.to_string())
    }
}

/// Create a lease implementation appropriate for the given storage backend.
pub fn create_lease(storage: &Storage) -> Result<Arc<dyn PrimaryLease>, LeaseError> {
    match storage {
        Storage::InMemory => {
            // In-memory mode: use filesystem lease on temp directory (no disk I/O)
            // Generate a unique temp path for lease coordination without actually
            // creating the directory (memory mode must not touch filesystem).
            // NOTE: On some platforms (notably Windows) `SystemTime` resolution is not truly
            // nanosecond-granular, so concurrent callers can collide. Add a counter to ensure
            // uniqueness even under heavy parallel test load.
            let unique = INMEM_LEASE_COUNTER.fetch_add(1, Ordering::SeqCst);
            let temp_path = std::env::temp_dir().join(format!(
                "midge_inmem_{}_{}_{}",
                std::process::id(),
                unique,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
            ));
            // Memory mode: use MockFs for lease coordination (no disk I/O)
            Ok(Arc::new(FileSystemLease::new(&temp_path, true)?))
        }
        Storage::Local { path } => {
            // Local storage: use filesystem lease with RealFs
            Ok(Arc::new(FileSystemLease::new(path.as_path(), false)?))
        }
        Storage::Cloud {
            local_cache_path,
            provider,
            prefix,
        } => {
            // Cloud storage: use cloud lease with TTL-based coordination
            let config = CloudLeaseConfig {
                bucket: provider.bucket_or_container().to_string(),
                prefix: prefix.clone(),
            };
            let cloud = crate::storage::providers::build_cloud_storage(provider, prefix)
                .map_err(|error| LeaseError::IoError(format!("cloud lease backend: {error}")))?;
            Ok(Arc::new(CloudStorageLease::new_provider_backed(
                config,
                local_cache_path.clone(),
                cloud,
            )))
        }
        Storage::CloudSimulated {
            local_cache_path,
            bucket,
            prefix,
        } => {
            let config = CloudLeaseConfig {
                bucket: bucket.clone(),
                prefix: prefix.clone(),
            };
            Ok(Arc::new(CloudStorageLease::new(
                config,
                local_cache_path.clone(),
            )))
        }
    }
}
