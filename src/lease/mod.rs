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

pub(crate) mod cloud;
mod filesystem;
pub mod fs_leader_store;
mod heartbeat;
mod traits;

pub use cloud::{CloudLeaseConfig, CloudStorageLease};
pub use filesystem::FileSystemLease;
pub use heartbeat::LeaseHeartbeat;
#[cfg(test)]
pub(crate) use traits::LeaderRecord;
pub(crate) use traits::LeaseError;
pub(crate) use traits::LeaseValidity;
pub use traits::{LeaderStore, LeaseGuard, PrimaryLease};

use crate::config::Storage;
use std::sync::atomic::{AtomicU64, Ordering};
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
        if matches!(storage, Storage::Cloud { .. }) {
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

static INMEM_LEASE_COUNTER: AtomicU64 = AtomicU64::new(0);

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
                    "{context}: writer lease authority is unknown: {self}"
                ))
            }
            LeaseError::Timeout(_) => {
                crate::common::MidgeError::Timeout(format!("{context}: {self}"))
            }
            other => crate::common::MidgeError::Fenced(format!("{context}: {other}")),
        }
    }
}

pub(crate) struct CreatedLease {
    pub(crate) lease: Arc<dyn PrimaryLease>,
    pub(crate) validity: Option<Arc<LeaseValidity>>,
}

#[cfg(test)]
pub(crate) fn create_lease_with_validity(
    storage: &Storage,
    clock_skew_tolerance: std::time::Duration,
) -> Result<CreatedLease, LeaseError> {
    create_lease_with_validity_and_timeout(
        storage,
        clock_skew_tolerance,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )
}

#[cfg(test)]
pub(crate) fn create_lease_with_validity_and_timeout(
    storage: &Storage,
    clock_skew_tolerance: std::time::Duration,
    storage_io_timeout: std::time::Duration,
) -> Result<CreatedLease, LeaseError> {
    create_lease_with_validity_and_timeout_and_ttl(
        storage,
        clock_skew_tolerance,
        storage_io_timeout,
        std::time::Duration::from_secs(30),
    )
}

pub(crate) fn create_lease_with_validity_and_timeout_and_ttl(
    storage: &Storage,
    clock_skew_tolerance: std::time::Duration,
    storage_io_timeout: std::time::Duration,
    lease_ttl: std::time::Duration,
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
                lease: Arc::new(FileSystemLease::new_with_ttl_and_clock_skew_tolerance(
                    &temp_path,
                    true,
                    lease_ttl,
                    clock_skew_tolerance,
                )?),
                validity: None,
            })
        }
        Storage::Local { path } => {
            // Local storage: use filesystem lease with RealFs
            let lease = Arc::new(FileSystemLease::new_with_ttl_and_clock_skew_tolerance(
                path.as_path(),
                false,
                lease_ttl,
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
            let cloud = crate::storage::providers::build_cloud_storage_with_timeout(
                lease_provider,
                lease_prefix,
                storage_io_timeout,
            )
            .map_err(|error| LeaseError::IoError(format!("cloud lease backend: {error}")))?;
            let lease = Arc::new(
                CloudStorageLease::new_provider_backed_with_clock_skew_tolerance_and_ttl(
                    config,
                    local_cache_path.clone(),
                    cloud,
                    clock_skew_tolerance,
                    lease_ttl,
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
            let lease = Arc::new(CloudStorageLease::new_with_clock_skew_tolerance_and_ttl(
                config,
                local_cache_path.clone(),
                clock_skew_tolerance,
                lease_ttl,
            ));
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
}
