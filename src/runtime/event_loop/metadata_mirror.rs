use super::EventLoop;

impl EventLoop {
    pub(super) fn mirror_metadata_to_authoritative_cloud(&self) -> crate::common::MidgeResult<()> {
        self.mirror_metadata_to_authoritative_cloud_within(&self.event_loop_cloud_deadline())
    }

    pub(super) fn mirror_metadata_to_authoritative_cloud_within(
        &self,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<()> {
        self.cloud_coordinator.mirror_metadata_within(
            self.state.fs.as_ref(),
            &self.metadata_publication_lock,
            self.state.manifest.last_persisted_sequence,
            deadline,
            self.fencing.leader_store.as_deref().map(|store| {
                crate::runtime::hybrid_persistence::CloudMetadataMirrorAuthority {
                    store,
                    holder_id: self.fencing.leader_holder_id.as_deref().unwrap_or_default(),
                    writer_epoch: self.fencing.writer_epoch,
                }
            }),
            |deadline| self.validate_runtime_writer_lease_within(deadline),
        )
    }

    pub(super) fn mirror_metadata_after_local_commit(
        &mut self,
        context: &str,
    ) -> crate::common::MidgeResult<()> {
        let deadline = self.event_loop_cloud_deadline();
        self.mirror_metadata_after_local_commit_within(context, &deadline)
    }

    pub(super) fn mirror_metadata_after_local_commit_within(
        &mut self,
        context: &str,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<()> {
        match self.mirror_metadata_to_authoritative_cloud_within(deadline) {
            Ok(()) => Ok(()),
            Err(error)
                if self.state.recovery_policy() == crate::config::RecoveryPolicy::Salvage =>
            {
                self.state.mark_persistence_anomaly();
                tracing::warn!(%error, context, "cloud metadata mirror failed during salvage-capable operation");
                Ok(())
            }
            Err(error) => Err(error),
        }
    }
}
