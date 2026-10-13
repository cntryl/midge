//! Retire the authoritative sealed WAL prefix covered by durable family frontiers.

use super::EventLoop;

mod cloud;

impl EventLoop {
    pub(super) fn prune_local_wal_segments_covered_by_manifest(&mut self) {
        if self.state.is_memory_mode() || self.wal_actor.is_cloud_async() {
            return;
        }
        if let Err(error) = self.validate_runtime_lease_for_wal_prune() {
            tracing::warn!(%error, "retaining local WAL because the writer lease is no longer valid");
            return;
        }
        if !self.seal_active_wal_for_prune() {
            return;
        }
        let frontiers = self.state.manifest.flush_frontiers();
        let to = self
            .state
            .wal
            .history
            .ledger()
            .retirable_prefix(|cf, sequence| match frontiers.get(cf) {
                Ok(crate::metadata::FamilyFrontier::Live { through_sequence }) => {
                    sequence <= through_sequence
                }
                Ok(crate::metadata::FamilyFrontier::Dropped) => true,
                Err(_) => false,
            });
        if let Some(to) = to {
            self.retire_local_wal_below(to);
        }
    }

    /// Retires every sealed segment below `to` through the ledger, after a
    /// fresh writer-lease check, so a fenced writer stops retiring at once.
    fn retire_local_wal_below(&mut self, to: u64) {
        if let Err(error) = self.validate_runtime_lease_for_wal_prune() {
            tracing::warn!(%error, "stopped local WAL pruning after lease validation failed");
            return;
        }
        self.retire_wal_below_or_retain(to);
    }

    /// Retire every WAL segment below `to`, or retain the WAL and record the
    /// anomaly when the ledger cannot. A file left behind is an anomaly too:
    /// it holds disk space until the next open sweeps it.
    pub(super) fn retire_wal_below_or_retain(
        &mut self,
        to: u64,
    ) -> Option<crate::runtime::wal_history::RetiredWal> {
        crate::failpoints::fail_point!("midge::cloud::before_local_wal_retirement");
        match self.wal_actor.retire_wal_below(&mut self.state, to) {
            Ok(retired) => {
                crate::failpoints::fail_point!("midge::cloud::before_local_wal_unlink");
                self.gc_actor.enqueue_retired_wal(&retired);
                self.gc_actor.retry_retired_wal(
                    &mut self.state,
                    self.cloud_coordinator.hybrid_storage.as_deref(),
                );
                tracing::debug!(?retired, "retired WAL segments");
                Some(retired)
            }
            Err(error) => {
                self.state.mark_persistence_anomaly();
                tracing::warn!(%error, to, "retaining WAL because the ledger could not retire it");
                None
            }
        }
    }

    /// Seals the active WAL segment so it can be retired later. In local
    /// mode this is the only place segments are sealed, so it must happen
    /// even while some memtable is unflushed, or the open segment would grow
    /// without bound. An active segment holding no record has nothing to
    /// seal (#490): rotating it would only create a sealed segment holding
    /// nothing for the loop below to delete.
    ///
    /// The writer's tail is a sound test: an append returns only after the
    /// writer thread has written its bytes and counted its records, and
    /// appends run on this thread, so none is in flight here. Returns false
    /// when pruning must stop.
    pub(super) fn seal_active_wal_for_prune(&mut self) -> bool {
        if self.wal_actor.open_tail().record_count == 0 {
            return true;
        }
        if let Err(error) = self.sync_local_wal_before_prune_rotation() {
            self.state.mark_persistence_anomaly();
            tracing::warn!(%error, "retaining local WAL because buffered records could not be synced");
            return false;
        }
        if let Err(error) = self.rotate_local_wal_transition() {
            self.state.mark_persistence_anomaly();
            tracing::warn!(%error, "retaining local WAL because the active segment could not be sealed");
            return false;
        }
        true
    }

    pub(super) fn validate_runtime_lease_for_wal_prune(&self) -> crate::common::MidgeResult<()> {
        self.check_lease_health()?;
        if let Some(store) = &self.fencing.leader_store {
            store
                .validate_epoch(
                    self.fencing.leader_holder_id.as_deref().unwrap_or_default(),
                    self.fencing.writer_epoch,
                )
                .map_err(|error| error.into_validation_error("local WAL prune"))?;
        }
        Ok(())
    }
}
