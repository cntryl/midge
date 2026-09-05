//! Fair turns for cloud maintenance sharing the local working-space budget.

use super::EventLoop;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum MaintenanceTask {
    #[default]
    Flush,
    Compaction,
    WalRetirement,
}

impl MaintenanceTask {
    fn following(self) -> Self {
        match self {
            Self::Flush => Self::Compaction,
            Self::Compaction => Self::WalRetirement,
            Self::WalRetirement => Self::Flush,
        }
    }
}

#[derive(Default)]
pub(super) struct CloudMaintenance {
    pub next: MaintenanceTask,
    pub dispatching: bool,
}

impl EventLoop {
    pub(super) fn cloud_maintenance_enabled(&self) -> bool {
        self.wal_actor.is_cloud_async()
            && !self.state.is_memory_mode()
            && self
                .hybrid_storage
                .as_ref()
                .is_some_and(|storage| storage.ephemeral_sst_cache_enabled())
    }

    /// Dispatch at most one ready worker. A missing or retry-delayed task does
    /// not hold the turn; a successful launch advances the preferred task.
    pub(super) fn schedule_cloud_maintenance(&mut self) -> Option<MaintenanceTask> {
        if self.cloud_maintenance.dispatching
            || self.pending_msg.is_some()
            || !self.publication_gate.deferred_messages.is_empty()
            || self.publication_gate.active
            || self.flush_actor.is_inflight()
            || self.cloud_wal_prune_worker.is_some()
            || self
                .state
                .active_compactions
                .load(std::sync::atomic::Ordering::Acquire)
                > 0
            || !self.state.compaction.compacting_ssts.is_empty()
        {
            return None;
        }
        self.cloud_maintenance.dispatching = true;
        let mut task = self.cloud_maintenance.next;
        let mut started = None;
        for _ in 0..3 {
            let launched = match task {
                MaintenanceTask::Flush => {
                    self.schedule_next_flush_worker();
                    self.flush_actor.is_inflight()
                }
                MaintenanceTask::Compaction => {
                    if self.shutting_down
                        || self
                            .state
                            .ingest_active
                            .load(std::sync::atomic::Ordering::Acquire)
                    {
                        false
                    } else {
                        match self
                            .schedule_one_background_compaction_if_needed("cloud maintenance turn")
                        {
                            Ok(started) => started,
                            Err(error) => {
                                tracing::warn!(%error, "Cloud maintenance compaction was not admitted");
                                false
                            }
                        }
                    }
                }
                MaintenanceTask::WalRetirement => {
                    self.prune_cloud_wal_segments_covered_by_manifest();
                    self.cloud_wal_prune_worker.is_some()
                }
            };
            if launched {
                self.cloud_maintenance.next = task.following();
                started = Some(task);
                break;
            }
            task = task.following();
        }
        self.cloud_maintenance.dispatching = false;
        started
    }
}
