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
//! - **Fail-closed semantics**: Loss of lease immediately stops accepting
//!   writes. The engine stays open for reads/diagnostics and can notify the
//!   embedder exactly once through `OpenOptionsBuilder::on_lease_loss`.
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
#[cfg(test)]
pub(crate) use traits::LeaderRecord;
pub(crate) use traits::LeaseValidity;
pub use traits::{LeaderStore, LeaseError, LeaseGuard, PrimaryLease};

use crate::config::Storage;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

static INMEM_LEASE_COUNTER: AtomicU64 = AtomicU64::new(0);

// Keep the dependency direction common <- lease: the higher lease layer owns
// conversion of its error into the shared public error type.
impl From<LeaseError> for crate::common::MidgeError {
    fn from(error: LeaseError) -> Self {
        match error {
            LeaseError::AcquisitionFailed(message) => Self::LeaseHeld(message),
            LeaseError::IoError(message) => Self::LeaseUnavailable(message),
            LeaseError::RenewalFailed(message) => Self::Fenced(message),
            LeaseError::AlreadyReleased => Self::Fenced("lease already released".to_string()),
            LeaseError::Indeterminate(message) => Self::LeaseIndeterminate(message),
            LeaseError::EpochExhausted => Self::LeaseEpochExhausted,
            LeaseError::AlreadyAcquired(message) => Self::Busy(message),
            LeaseError::Internal(message) => Self::Internal(message),
        }
    }
}

pub(crate) struct CreatedLease {
    pub(crate) lease: Arc<dyn PrimaryLease>,
    pub(crate) validity: Option<Arc<LeaseValidity>>,
}

pub(crate) fn create_lease_with_validity(
    storage: &Storage,
    clock_skew_tolerance: std::time::Duration,
) -> Result<CreatedLease, LeaseError> {
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
            Ok(CreatedLease {
                lease: Arc::new(FileSystemLease::new(&temp_path, true)?),
                validity: None,
            })
        }
        Storage::Local { path } => {
            // Local storage: use filesystem lease with RealFs
            let lease = Arc::new(FileSystemLease::new_with_clock_skew_tolerance(
                path.as_path(),
                false,
                clock_skew_tolerance,
            )?);
            Ok(CreatedLease {
                validity: Some(lease.lease_validity()),
                lease,
            })
        }
        Storage::Cloud {
            local_cache_path,
            topology,
        } => {
            // Cloud storage: use cloud lease with TTL-based coordination
            let control = topology.control();
            let lease_provider = control.provider();
            let lease_prefix = control.prefix();
            let config = CloudLeaseConfig {
                bucket: lease_provider.bucket_or_container().to_string(),
                prefix: lease_prefix.to_string(),
            };
            let cloud =
                crate::storage::providers::build_cloud_storage(lease_provider, lease_prefix)
                    .map_err(|error| {
                        LeaseError::IoError(format!("cloud lease backend: {error}"))
                    })?;
            let lease = Arc::new(
                CloudStorageLease::new_provider_backed_with_clock_skew_tolerance(
                    config,
                    local_cache_path.clone(),
                    cloud,
                    clock_skew_tolerance,
                ),
            );
            Ok(CreatedLease {
                validity: Some(lease.lease_validity()),
                lease,
            })
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
            let lease = Arc::new(CloudStorageLease::new_with_clock_skew_tolerance(
                config,
                local_cache_path.clone(),
                clock_skew_tolerance,
            ));
            Ok(CreatedLease {
                validity: Some(lease.lease_validity()),
                lease,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_attach_monotonic_watchdog_validity_to_local_filesystem_lease() {
        // Arrange
        let directory = tempfile::tempdir().expect("create local lease directory");
        let storage = Storage::Local {
            path: directory.path().to_path_buf(),
        };

        // Act
        let created = create_lease_with_validity(&storage, std::time::Duration::from_secs(15))
            .expect("create local lease");

        // Assert
        assert!(created.validity.is_some());
    }
}
