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
pub mod cloud;
mod filesystem;
pub mod fs_leader_store;
mod heartbeat;
mod scoped_leader_store;
mod traits;
mod writer_effect_groups;

pub use clock::LeaseClock;
pub use cloud::{CloudLeaseConfig, CloudStorageLease};
pub use cntryl_midge_core::types::SharedGenerationId;
pub use filesystem::FileSystemLease;

#[cfg(test)]
mod auto_trait_tests {
    #[test]
    fn should_derive_thread_safety_for_lease_fields_without_unsafe_implementations() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<super::FileSystemLease>();
        assert_send_sync::<super::CloudStorageLease>();
    }
}
pub use heartbeat::LeaseHeartbeat;
pub use scoped_leader_store::scoped_leader_store;
pub use traits::LeaderRecord;
pub use traits::{
    CloudFormatMigrationCandidate, CloudFormatMigrationGate, CloudFormatMigrationPending,
    CloudMetadataGeneration, CloudMetadataGenerationIdentity, CloudMetadataHead,
    CloudMetadataObject, LeaderStore, LeaseGuard, PrimaryLease,
};
pub use traits::{LeaseError, LeaseValidity};
pub use writer_effect_groups::{
    ActiveWriterEffect, WriterEffectGroup, WriterEffectGroupContext, WriterEffectGroups,
    WriterEffectTicket,
};

use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct LeaseLossHook(pub Arc<dyn Fn() + Send + Sync>);

impl std::fmt::Debug for LeaseLossHook {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("LeaseLossHook(..)")
    }
}
#[derive(Debug, Clone, Copy)]
pub enum LeaseStorageKind {
    Cloud,
    Local,
}

#[derive(Debug, Clone)]
pub struct LeaseConfig {
    pub ttl: Duration,
    pub clock_skew_tolerance: Option<Duration>,
    pub loss_hook: Option<LeaseLossHook>,
    pub clock: LeaseClock,
}

impl LeaseConfig {
    #[must_use]
    pub fn new(
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

    #[must_use]
    pub fn resolved_clock_skew_tolerance(&self) -> Duration {
        self.clock_skew_tolerance.unwrap_or(self.ttl / 2)
    }

    pub fn validate(
        &self,
        storage_kind: LeaseStorageKind,
    ) -> cntryl_midge_core::common::MidgeResult<()> {
        use cntryl_midge_core::common::MidgeError;
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
        if std::time::Instant::now().checked_add(self.ttl).is_none() {
            return Err(MidgeError::InvalidArgument(
                "lease TTL exceeds the monotonic clock range".to_string(),
            ));
        }
        if matches!(storage_kind, LeaseStorageKind::Cloud)
            && cloud::lease_expiry(self.clock.now(), self.ttl).is_none()
        {
            return Err(MidgeError::InvalidArgument(
                "cloud lease TTL exceeds the wall-clock expiry range".to_string(),
            ));
        }
        if matches!(storage_kind, LeaseStorageKind::Cloud)
            && self.ttl.saturating_mul(2) / 3 <= cloud::RENEWAL_WRITE_DEADLINE_MARGIN
        {
            return Err(MidgeError::InvalidArgument(format!(
                "cloud lease TTL {:?} is too short to renew; two thirds of it must exceed the {:?} provider write margin",
                self.ttl, cloud::RENEWAL_WRITE_DEADLINE_MARGIN
            )));
        }
        Ok(())
    }
}

// Keep the dependency direction common <- lease: the higher lease layer owns
// conversion of its error into the shared public error type.
impl From<LeaseError> for cntryl_midge_core::common::MidgeError {
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
    pub fn into_validation_error(self, context: &str) -> cntryl_midge_core::common::MidgeError {
        match self {
            LeaseError::IoError(_) | LeaseError::Indeterminate(_) => {
                cntryl_midge_core::common::MidgeError::Busy(format!(
                    "{context}: writer lease authority is unavailable: {self}"
                ))
            }
            LeaseError::Timeout(_) => {
                cntryl_midge_core::common::MidgeError::Timeout(format!("{context}: {self}"))
            }
            other => cntryl_midge_core::common::MidgeError::Fenced(format!("{context}: {other}")),
        }
    }

    /// A writer storage effect may have reached durable storage before its
    /// authority check failed. Preserve unknown authority as a lease failure
    /// so commit handling reports an unknown outcome instead of retryable
    /// local I/O poisoning.
    pub fn into_writer_effect_error(self, context: &str) -> cntryl_midge_core::common::MidgeError {
        match self {
            LeaseError::IoError(_) | LeaseError::Timeout(_) => {
                cntryl_midge_core::common::MidgeError::LeaseUnavailable(format!(
                    "{context}: writer lease authority is unknown: {self}"
                ))
            }
            LeaseError::Indeterminate(_) => {
                cntryl_midge_core::common::MidgeError::LeaseIndeterminate(format!(
                    "{context}: writer lease authority is indeterminate: {self}"
                ))
            }
            other => other.into_validation_error(context),
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
            Err(cntryl_midge_core::common::MidgeError::InvalidArgument(_))
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
