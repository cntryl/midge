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
            let error = if deadline.is_bounded()
                && (deadline.is_expired() || error.to_string().contains("timed out"))
            {
                crate::common::MidgeError::Timeout(format!(
                    "writer lease validation exceeded the operation deadline: {error}"
                ))
            } else {
                crate::common::MidgeError::Fenced(error.to_string())
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
