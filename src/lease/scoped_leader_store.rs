//! Startup authority checks share one scope; fencing cleanup keeps the original store.

use super::traits::{scope_lease_error, LeaderRecord};
use super::{CloudMetadataGeneration, CloudMetadataHead, LeaderStore, LeaseError};
use crate::common::DeadlineScope;
use std::sync::Arc;
use std::time::Duration;

struct StartupLeaderStore {
    inner: Arc<dyn LeaderStore>,
    scope: DeadlineScope,
    per_io_timeout: Duration,
}

pub(crate) fn scoped_leader_store(
    inner: Arc<dyn LeaderStore>,
    scope: DeadlineScope,
    per_io_timeout: Duration,
) -> Arc<dyn LeaderStore> {
    Arc::new(StartupLeaderStore {
        inner,
        scope,
        per_io_timeout,
    })
}

impl StartupLeaderStore {
    fn within<T>(
        &self,
        timeout: Duration,
        context: &str,
        operation: impl FnOnce(Duration) -> Result<T, LeaseError>,
    ) -> Result<T, LeaseError> {
        let timeout = if self.scope.deadline().is_bounded() {
            timeout.min(self.per_io_timeout)
        } else {
            // Admission disarms startup limits; preserve ordinary explicit budgets.
            timeout
        };
        self.within_operation(timeout, context, operation)
    }

    fn within_operation<T>(
        &self,
        timeout: Duration,
        context: &str,
        operation: impl FnOnce(Duration) -> Result<T, LeaseError>,
    ) -> Result<T, LeaseError> {
        self.scope.check(context).map_err(scope_lease_error)?;
        let timeout = self.scope.clamp(timeout);
        if timeout.is_zero() {
            return Err(LeaseError::Timeout(format!(
                "startup authority budget exhausted during {context}"
            )));
        }
        let result = operation(timeout);
        // Preserve definite terminal authority errors rather than replacing
        // their classification with an unrelated deadline observation.
        if result.is_ok() {
            self.scope.check(context).map_err(scope_lease_error)?;
        }
        result
    }
}

impl LeaderStore for StartupLeaderStore {
    fn acquire_leadership(&self, holder_id: &str) -> Result<LeaderRecord, LeaseError> {
        self.acquire_leadership_with_minimum_epoch(holder_id, 0)
    }

    fn acquire_leadership_with_minimum_epoch(
        &self,
        holder_id: &str,
        minimum_epoch: u64,
    ) -> Result<LeaderRecord, LeaseError> {
        self.within(self.per_io_timeout, "leader acquisition", |_| {
            let result = self.inner.acquire_leadership_with_minimum_epoch_within(
                holder_id,
                minimum_epoch,
                &self.scope,
                &mut |_| {},
            );
            if result.is_ok() {
                self.scope.resolve_ambiguous_mutation();
            }
            result
        })
    }

    fn acquire_leadership_with_minimum_epoch_within(
        &self,
        holder_id: &str,
        minimum_epoch: u64,
        scope: &DeadlineScope,
        on_cleanup_epoch: &mut dyn FnMut(u64),
    ) -> Result<LeaderRecord, LeaseError> {
        self.within(self.per_io_timeout, "scoped leader acquisition", |_| {
            self.inner.acquire_leadership_with_minimum_epoch_within(
                holder_id,
                minimum_epoch,
                scope,
                on_cleanup_epoch,
            )
        })
    }

    fn read_current(&self) -> Result<Option<LeaderRecord>, LeaseError> {
        self.read_current_with_timeout(self.per_io_timeout)
    }

    fn read_current_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Option<LeaderRecord>, LeaseError> {
        self.within(timeout, "current leader read", |timeout| {
            self.inner.read_current_with_timeout(timeout)
        })
    }

    fn read_committed_metadata(&self, timeout: Duration) -> Result<CloudMetadataHead, LeaseError> {
        self.within(timeout, "committed metadata read", |timeout| {
            self.inner.read_committed_metadata(timeout)
        })
    }

    fn publish_committed_metadata(
        &self,
        holder_id: &str,
        expected_epoch: u64,
        expected_previous: Option<&CloudMetadataGeneration>,
        generation: CloudMetadataGeneration,
        timeout: Duration,
    ) -> Result<(), LeaseError> {
        self.within_operation(timeout, "metadata publication", |timeout| {
            self.inner.publish_committed_metadata(
                holder_id,
                expected_epoch,
                expected_previous,
                generation,
                timeout,
            )
        })
    }

    fn validate_epoch(&self, holder_id: &str, epoch: u64) -> Result<(), LeaseError> {
        self.validate_epoch_with_timeout(holder_id, epoch, self.per_io_timeout)
    }

    fn validate_epoch_with_timeout(
        &self,
        holder_id: &str,
        epoch: u64,
        timeout: Duration,
    ) -> Result<(), LeaseError> {
        self.within(timeout, "epoch validation", |timeout| {
            self.inner
                .validate_epoch_with_timeout(holder_id, epoch, timeout)
        })
    }

    // Renewal and release are fencing lifecycle operations. The startup
    // owner retains the original store; its cleanup/heartbeat budget survives
    // cancellation even if a caller keeps this private authority view.
    fn renew_leadership(&self, holder_id: &str, epoch: u64) -> Result<(), LeaseError> {
        self.inner.renew_leadership(holder_id, epoch)
    }

    fn release_leadership(&self, holder_id: &str, epoch: u64) -> Result<(), LeaseError> {
        self.inner.release_leadership(holder_id, epoch)
    }

    fn set_clock_skew_tolerance(&self, tolerance: Duration) -> Result<(), LeaseError> {
        self.inner.set_clock_skew_tolerance(tolerance)
    }

    #[cfg(test)]
    fn read_test_coordination_document(&self) -> Result<Option<String>, LeaseError> {
        self.inner.read_test_coordination_document()
    }

    #[cfg(test)]
    fn write_test_coordination_document(&self, content: &str) -> Result<(), LeaseError> {
        self.inner.write_test_coordination_document(content)
    }

    #[cfg(test)]
    fn remove_test_coordination_document(&self) -> Result<(), LeaseError> {
        self.inner.remove_test_coordination_document()
    }
}
