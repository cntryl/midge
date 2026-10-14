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
//! - **Filesystem**: Persistent `.midge_leader` epoch record with CAS-via-rename
//!
//! ## Usage
//!
//! Lease acquisition MUST occur before engine initialization:

mod clock;
pub(crate) mod cloud;
mod filesystem;
pub mod fs_leader_store;
mod heartbeat;
mod scoped_leader_store;
mod traits;

pub use crate::types::SharedGenerationId;
pub(crate) use clock::LeaseClock;
pub use cloud::{CloudLeaseConfig, CloudStorageLease};
pub use filesystem::FileSystemLease;
pub use heartbeat::LeaseHeartbeat;
pub(crate) use scoped_leader_store::scoped_leader_store;
#[cfg(test)]
pub(crate) use traits::LeaderRecord;
pub(crate) use traits::LeaseError;
pub(crate) use traits::LeaseValidity;
pub use traits::{
    CloudMetadataGeneration, CloudMetadataHead, CloudMetadataObject, LeaderStore, LeaseGuard,
    PrimaryLease,
};

use crate::config::Storage;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub(crate) struct LeaseLossHook(pub(crate) Arc<dyn Fn() + Send + Sync>);

impl std::fmt::Debug for LeaseLossHook {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("LeaseLossHook(..)")
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum LeaseStorageKind {
    Cloud,
    Local,
}

impl From<&Storage> for LeaseStorageKind {
    fn from(storage: &Storage) -> Self {
        if matches!(
            storage,
            Storage::Cloud { .. } | Storage::CloudSimulated { .. }
        ) {
            Self::Cloud
        } else {
            Self::Local
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct LeaseConfig {
    pub(crate) ttl: Duration,
    pub(crate) clock_skew_tolerance: Option<Duration>,
    pub(crate) loss_hook: Option<LeaseLossHook>,
    pub(crate) clock: LeaseClock,
}

impl LeaseConfig {
    pub(crate) fn new(
        ttl: Duration,
        clock_skew_tolerance: Option<Duration>,
        loss_hook: Option<LeaseLossHook>,
    ) -> Self {
        Self {
            ttl,
            clock_skew_tolerance,
            loss_hook,
            clock: LeaseClock::default(),
        }
    }

    pub(crate) fn resolved_clock_skew_tolerance(&self) -> Duration {
        self.clock_skew_tolerance.unwrap_or(self.ttl / 2)
    }

    pub(crate) fn validate(
        &self,
        storage_kind: LeaseStorageKind,
    ) -> crate::common::MidgeResult<()> {
        use crate::common::MidgeError;
        if self.ttl.is_zero() {
            return Err(MidgeError::InvalidArgument(
                "lease TTL must be greater than zero".to_string(),
            ));
        }
        if self.resolved_clock_skew_tolerance() > self.ttl {
            return Err(MidgeError::InvalidArgument(
                "lease clock-skew tolerance must not exceed the lease TTL".to_string(),
            ));
        }
        // A cloud lease renews with two thirds of its TTL left and needs
        // enough time for its conditional provider write.
        if matches!(storage_kind, LeaseStorageKind::Cloud)
            && self.ttl.saturating_mul(2) / 3 <= cloud::RENEWAL_WRITE_DEADLINE_MARGIN
        {
            return Err(MidgeError::InvalidArgument(format!(
                "cloud lease TTL {:?} is too short to renew; two thirds of it must exceed the {:?} provider write margin",
                self.ttl,
                cloud::RENEWAL_WRITE_DEADLINE_MARGIN
            )));
        }
        Ok(())
    }
}

// Keep the dependency direction common <- lease: the higher lease layer owns
// conversion of its error into the shared public error type.
impl From<LeaseError> for crate::common::MidgeError {
    fn from(error: LeaseError) -> Self {
        match error {
            LeaseError::AcquisitionFailed(message) => Self::LeaseHeld(message),
            LeaseError::IoError(message) => Self::LeaseUnavailable(message),
            LeaseError::RenewalFailed(message) => Self::Fenced(message),
            LeaseError::Indeterminate(message) => Self::LeaseIndeterminate(message),
            LeaseError::EpochExhausted => Self::LeaseEpochExhausted,
            LeaseError::AlreadyAcquired(message) => Self::Busy(message),
            LeaseError::Internal(message) => Self::Internal(message),
            LeaseError::Timeout(message) => Self::Timeout(message),
        }
    }
}

impl LeaseError {
    /// How a failed writer-authority check reaches the runtime. Only a proven
    /// loss of ownership fences. A store that did not answer leaves authority
    /// unknown, which callers retry, and the lease's own validity still
    /// fences the writer if that lasts until it expires.
    pub(crate) fn into_validation_error(self, context: &str) -> crate::common::MidgeError {
        match self {
            LeaseError::IoError(_) | LeaseError::Indeterminate(_) => {
                crate::common::MidgeError::Busy(format!(
                    "{context}: writer lease authority is unavailable: {self}"
                ))
            }
            LeaseError::Timeout(_) => {
                crate::common::MidgeError::Timeout(format!("{context}: {self}"))
            }
            other => crate::common::MidgeError::Fenced(format!("{context}: {other}")),
        }
    }

    /// A writer storage effect may have reached durable storage before its
    /// authority check failed. Preserve unknown authority as a lease failure
    /// so commit handling reports an unknown outcome instead of retryable
    /// local I/O poisoning.
    pub(crate) fn into_writer_effect_error(self, context: &str) -> crate::common::MidgeError {
        match self {
            LeaseError::IoError(_) | LeaseError::Timeout(_) => {
                crate::common::MidgeError::LeaseUnavailable(format!(
                    "{context}: writer lease authority is unknown: {self}"
                ))
            }
            LeaseError::Indeterminate(_) => crate::common::MidgeError::LeaseIndeterminate(format!(
                "{context}: writer lease authority is indeterminate: {self}"
            )),
            other => other.into_validation_error(context),
        }
    }
}

pub(crate) struct CreatedLease {
    pub(crate) lease: Arc<dyn PrimaryLease>,
    pub(crate) validity: Option<Arc<LeaseValidity>>,
}

/// The database root a test lease coordinates through: in memory for
/// in-memory storage, otherwise the host directory.
#[cfg(test)]
pub(crate) fn test_lease_root(storage: &Storage) -> Arc<dyn crate::io::Fs> {
    match storage {
        Storage::InMemory => Arc::new(crate::io::MockFs::new()),
        Storage::Local { path } => crate::io::real::test_root(path),
        Storage::Cloud {
            local_cache_path, ..
        }
        | Storage::CloudSimulated {
            local_cache_path, ..
        } => crate::io::real::test_root(local_cache_path),
    }
}

#[cfg(test)]
pub(crate) fn create_lease_with_validity(
    storage: &Storage,
    clock_skew_tolerance: std::time::Duration,
) -> Result<CreatedLease, LeaseError> {
    create_lease_with_validity_and_timeout_and_ttl(
        storage,
        &test_lease_root(storage),
        None,
        clock_skew_tolerance,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
        std::time::Duration::from_secs(30),
        &LeaseClock::default(),
    )
}

/// Create the primary lease for `storage`. Cloud and simulated-cloud leases
/// keep their pointer and fencing document in the shared object store. Every
/// lease reads wall time from `clock`.
pub(crate) fn create_lease_with_validity_and_timeout_and_ttl(
    storage: &Storage,
    root: &Arc<dyn crate::io::Fs>,
    object_store: Option<Arc<dyn crate::storage::cloud::CloudBackend>>,
    clock_skew_tolerance: std::time::Duration,
    storage_io_timeout: std::time::Duration,
    lease_ttl: std::time::Duration,
    clock: &LeaseClock,
) -> Result<CreatedLease, LeaseError> {
    match storage {
        Storage::InMemory => Ok(CreatedLease {
            lease: Arc::new(FileSystemLease::with_root_and_timeout(
                Arc::clone(root),
                lease_ttl,
                clock_skew_tolerance,
                clock.clone(),
                storage_io_timeout,
            )),
            validity: None,
        }),
        Storage::Local { .. } => {
            let lease = Arc::new(FileSystemLease::with_root_and_timeout(
                Arc::clone(root),
                lease_ttl,
                clock_skew_tolerance,
                clock.clone(),
                storage_io_timeout,
            ));
            Ok(CreatedLease {
                validity: Some(lease.lease_validity()),
                lease,
            })
        }
        Storage::Cloud { topology, .. } => {
            // Cloud storage: use cloud lease with TTL-based coordination
            let control = topology.control();
            let lease_provider = control.provider();
            let lease_prefix = control.prefix();
            let config = CloudLeaseConfig {
                bucket: lease_provider.bucket_or_container().to_string(),
                prefix: lease_prefix.to_string(),
            };
            let cloud = crate::storage::providers::build_class_store(
                lease_provider,
                lease_prefix,
                storage_io_timeout,
                object_store,
            )
            .map_err(|error| LeaseError::IoError(format!("cloud lease backend: {error}")))?;
            let lease = Arc::new(
                CloudStorageLease::new_provider_backed_with_clock_skew_tolerance_and_ttl(
                    config,
                    cloud,
                    clock_skew_tolerance,
                    lease_ttl,
                    clock.clone(),
                ),
            );
            Ok(CreatedLease {
                validity: Some(lease.lease_validity()),
                lease,
            })
        }
        Storage::CloudSimulated { bucket, prefix, .. } => {
            let config = CloudLeaseConfig {
                bucket: bucket.clone(),
                prefix: prefix.clone(),
            };
            let store = crate::storage::simulated::SimulatedCloud::locate(
                root.as_ref(),
                object_store,
                storage_io_timeout,
            )
            .and_then(|location| location.open())
            .map_err(|error| {
                LeaseError::IoError(format!("simulated shared lease backend: {error}"))
            })?;
            let cloud = Arc::new(crate::storage::cloud::CloudStorage::from_object_store(
                store,
                storage_io_timeout,
            ));
            let lease = Arc::new(
                CloudStorageLease::new_provider_backed_with_clock_skew_tolerance_and_ttl(
                    config,
                    cloud,
                    clock_skew_tolerance,
                    lease_ttl,
                    clock.clone(),
                ),
            );
            Ok(CreatedLease {
                validity: Some(lease.lease_validity()),
                lease,
            })
        }
    }
}

#[cfg(test)]
mod config_tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn should_reject_short_cloud_lease_ttl_when_lease_config_validates() {
        // Arrange
        let config = LeaseConfig::new(Duration::from_secs(15), None, None);

        // Act
        let result = config.validate(LeaseStorageKind::Cloud);

        // Assert
        assert!(matches!(
            result,
            Err(crate::common::MidgeError::InvalidArgument(_))
        ));
    }

    #[test]
    fn should_allow_short_local_lease_ttl_when_lease_config_validates() {
        // Arrange
        let config = LeaseConfig::new(Duration::from_secs(15), None, None);

        // Act
        let result = config.validate(LeaseStorageKind::Local);

        // Assert
        assert!(result.is_ok());
        assert_eq!(
            config.resolved_clock_skew_tolerance(),
            Duration::from_millis(7500)
        );
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

    #[test]
    fn should_share_simulated_metadata_authority_across_distinct_cache_roots() {
        // Arrange
        let first_cache = tempfile::tempdir().unwrap();
        let second_cache = tempfile::tempdir().unwrap();
        let backend: Arc<dyn crate::storage::cloud::CloudBackend> =
            Arc::new(crate::storage::cloud::MockCloudBackend::new());
        let make = |directory: &std::path::Path| {
            create_lease_with_validity_and_timeout_and_ttl(
                &Storage::CloudSimulated {
                    local_cache_path: directory.into(),
                    bucket: "shared".into(),
                    prefix: String::new(),
                },
                &crate::io::real::test_root(directory),
                Some(Arc::clone(&backend)),
                Duration::ZERO,
                Duration::from_secs(5),
                Duration::from_secs(30),
                &LeaseClock::default(),
            )
            .unwrap()
        };
        let first = make(first_cache.path());
        let guard = Arc::clone(&first.lease).try_acquire().unwrap();
        let first_epoch = first.lease.epoch();
        let first_store = first.lease.get_leader_store().unwrap();
        let generation = CloudMetadataGeneration {
            id: SharedGenerationId::FIRST,
            manifest_horizon: 10,
            objects: [
                crate::metadata::files::FORMAT,
                crate::metadata::files::MANIFEST_SNAPSHOT,
            ]
            .into_iter()
            .map(|file_name| CloudMetadataObject {
                file_name: file_name.into(),
                object_key: format!("metadata/generations/{}/{file_name}", uuid::Uuid::new_v4()),
                len: 1,
                crc32c: 1,
            })
            .collect(),
        };
        first_store
            .publish_committed_metadata(
                &first.lease.holder_id(),
                first.lease.epoch(),
                None,
                generation.clone(),
                Duration::from_secs(5),
            )
            .unwrap();
        guard.release();
        first.lease.release().unwrap();
        // Act
        let second = make(second_cache.path());
        let _guard = Arc::clone(&second.lease).try_acquire().unwrap();
        let current = second
            .lease
            .get_leader_store()
            .unwrap()
            .read_committed_metadata(Duration::from_secs(5))
            .unwrap();
        // Assert
        assert_eq!(current, CloudMetadataHead::Committed(generation));
        assert!(second.lease.epoch() > first_epoch);
    }
}
