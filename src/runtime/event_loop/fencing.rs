use std::sync::Arc;

/// Write-authority state owned independently from message dispatch.
pub struct RuntimeFence {
    pub(super) lease_healthy: Option<Arc<std::sync::atomic::AtomicBool>>,
    pub(super) ddl_authority_ambiguous: bool,
    pub(super) writer_epoch: u64,
    pub(super) leader_store: Option<Arc<dyn crate::lease::LeaderStore>>,
    pub(super) leader_holder_id: Option<String>,
}

impl RuntimeFence {
    pub(super) fn check_health(&self) -> crate::common::MidgeResult<()> {
        if self.ddl_authority_ambiguous {
            return Err(crate::common::MidgeError::Fenced(
                "DDL authority is ambiguous; refusing writes until prepared DDL is reconciled"
                    .into(),
            ));
        }
        if let Some(healthy) = &self.lease_healthy {
            if !healthy.load(std::sync::atomic::Ordering::Acquire) {
                return Err(crate::common::MidgeError::Fenced(
                    "lease heartbeat reports unhealthy — refusing writes".into(),
                ));
            }
        }
        Ok(())
    }

    pub(super) fn validate_within(
        &self,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<()> {
        self.check_health()?;
        if deadline.is_expired() {
            return Err(crate::common::MidgeError::Timeout(
                "operation deadline exhausted before writer lease validation".to_string(),
            ));
        }
        let Some(store) = &self.leader_store else {
            return Ok(());
        };
        let holder_id = self.leader_holder_id.as_deref().unwrap_or_default();
        let result = if deadline.is_bounded() {
            store.validate_epoch_with_timeout(holder_id, self.writer_epoch, deadline.remaining())
        } else {
            store.validate_epoch(holder_id, self.writer_epoch)
        };
        result.map_err(|error| {
            let error = if deadline.is_bounded() && deadline.is_expired() {
                crate::common::MidgeError::Timeout(format!(
                    "writer lease validation exceeded the operation deadline: {error}"
                ))
            } else {
                error.into_validation_error("writer lease validation failed")
            };
            if matches!(error, crate::common::MidgeError::Fenced(_)) {
                if let Some(healthy) = &self.lease_healthy {
                    healthy.store(false, std::sync::atomic::Ordering::Release);
                }
                tracing::error!(%error, "writer lease validation failed; runtime fenced");
            }
            error
        })
    }
}

#[cfg(test)]
mod tests {
    use super::RuntimeFence;
    use crate::common::{MidgeError, OperationDeadline};
    use crate::lease::{LeaderRecord, LeaderStore, LeaseError};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    /// Leader store whose every read fails with the error `make` builds.
    struct FailingLeaderStore {
        make: fn() -> LeaseError,
    }

    impl LeaderStore for FailingLeaderStore {
        fn acquire_leadership(&self, _holder_id: &str) -> Result<LeaderRecord, LeaseError> {
            Err(LeaseError::Internal("not used by fence tests".to_string()))
        }

        fn read_current(&self) -> Result<Option<LeaderRecord>, LeaseError> {
            Err((self.make)())
        }
    }

    fn fence(make: fn() -> LeaseError) -> (RuntimeFence, Arc<AtomicBool>) {
        let healthy = Arc::new(AtomicBool::new(true));
        let fence = RuntimeFence {
            lease_healthy: Some(Arc::clone(&healthy)),
            ddl_authority_ambiguous: false,
            writer_epoch: 1,
            leader_store: Some(Arc::new(FailingLeaderStore { make })),
            leader_holder_id: Some("holder".to_string()),
        };
        (fence, healthy)
    }

    #[test]
    fn should_not_poison_lease_health_when_leader_store_read_fails_transiently() {
        // Arrange
        let (fence, healthy) = fence(|| LeaseError::IoError("503".to_string()));

        // Act
        let result = fence.validate_within(&OperationDeadline::from_budget(Duration::from_secs(5)));

        // Assert
        assert!(matches!(result, Err(MidgeError::Busy(_))), "{result:?}");
        assert!(healthy.load(Ordering::Acquire));
        assert!(fence.check_health().is_ok());
    }

    #[test]
    fn should_report_timeout_without_poisoning_when_lease_validation_times_out() {
        // Arrange
        let (fence, healthy) = fence(|| LeaseError::Timeout("deadline".to_string()));

        // Act
        let result = fence.validate_within(&OperationDeadline::from_budget(Duration::from_secs(5)));

        // Assert
        assert!(matches!(result, Err(MidgeError::Timeout(_))), "{result:?}");
        assert!(healthy.load(Ordering::Acquire));
    }

    #[test]
    fn should_poison_lease_health_when_ownership_is_lost() {
        // Arrange
        let (fence, healthy) = fence(|| LeaseError::RenewalFailed("newer holder".to_string()));

        // Act
        let result = fence.validate_within(&OperationDeadline::unbounded());

        // Assert
        assert!(matches!(result, Err(MidgeError::Fenced(_))), "{result:?}");
        assert!(!healthy.load(Ordering::Acquire));
    }
}
