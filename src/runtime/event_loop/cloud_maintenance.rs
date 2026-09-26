//! Fair turns for cloud maintenance sharing the local working-space budget.

use super::cloud_coordinator::CloudCoordinator;
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

impl CloudCoordinator {
    pub(super) fn begin_maintenance_turn(&mut self, blocked: bool) -> Option<MaintenanceTask> {
        if self.cloud_maintenance.dispatching || blocked || self.cloud_wal_prune_worker.is_some() {
            return None;
        }
        self.cloud_maintenance.dispatching = true;
        Some(self.cloud_maintenance.next)
    }

    pub(super) fn complete_maintenance_turn(&mut self, started: Option<MaintenanceTask>) {
        if let Some(task) = started {
            self.cloud_maintenance.next = task.following();
        }
        self.cloud_maintenance.dispatching = false;
    }
}

impl EventLoop {
    pub(super) fn cloud_maintenance_enabled(&self) -> bool {
        self.cloud_coordinator
            .maintenance_enabled(self.wal_actor.is_cloud_async(), self.state.is_memory_mode())
    }

    /// Dispatch at most one ready worker. A missing or retry-delayed task does
    /// not hold the turn; a successful launch advances the preferred task.
    pub(super) fn schedule_cloud_maintenance(&mut self) -> Option<MaintenanceTask> {
        let blocked = self.pending_msg.is_some()
            || !self.publication_gate.deferred_messages_is_empty()
            || self.publication_gate.is_active()
            || self.flush_actor.is_inflight()
            || self
                .state
                .active_compactions
                .load(std::sync::atomic::Ordering::Acquire)
                > 0
            || !self.state.compaction.compacting_ssts.is_empty();
        let mut task = self.cloud_coordinator.begin_maintenance_turn(blocked)?;
        let mut started = None;
        for _ in 0..3 {
            let launched = match task {
                MaintenanceTask::Flush => {
                    self.schedule_next_flush_worker();
                    self.flush_actor.is_inflight()
                }
                MaintenanceTask::Compaction => {
                    if self.shutting_down {
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
                    self.cloud_coordinator.cloud_wal_prune_worker.is_some()
                }
            };
            if launched {
                started = Some(task);
                break;
            }
            task = task.following();
        }
        self.cloud_coordinator.complete_maintenance_turn(started);
        started
    }
}
