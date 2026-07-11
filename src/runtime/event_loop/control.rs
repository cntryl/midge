use super::{EventLoop, HandleOutcome};
use crate::runtime::RuntimeResponse;

pub(super) struct RuntimeConfigUpdate {
    pub request_id: u64,
    pub memtable_size_limit: Option<usize>,
    pub memtable_flush_threshold: Option<usize>,
    pub enable_compaction: Option<bool>,
    pub l0_compaction_trigger: Option<usize>,
    pub wal_durability_policy: Option<crate::wal::DurabilityPolicy>,
    pub wal_batch_config: Option<crate::wal::policy::BatchConfig>,
}

impl EventLoop {
    #[cfg(test)]
    pub(super) fn handle_noop(&self, request_id: u64) {
        self.respond(request_id, RuntimeResponse::Ok { request_id });
    }

    #[cfg(test)]
    pub(super) fn handle_startup_ping(&self, request_id: u64) {
        self.respond(request_id, RuntimeResponse::Ok { request_id });
    }

    pub(super) fn handle_check_write_stall(
        &self,
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
    ) {
        let is_stalled = self.should_stall_writes(cf_id);
        self.respond(
            request_id,
            RuntimeResponse::WriteStallStatus {
                request_id,
                is_stalled,
            },
        );
    }

    pub(super) fn handle_wait_for_write_stall_clear(
        &mut self,
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
    ) {
        if self.should_stall_writes(cf_id) {
            self.write_stall_waiters.insert(request_id, cf_id);
            self.write_stall_waiter_queues
                .entry(cf_id)
                .or_default()
                .push_back(request_id);
        } else {
            self.respond(request_id, RuntimeResponse::Ok { request_id });
        }

        self.drain_auto_flush_memtables();
    }

    pub(super) fn handle_cancel_wait_for_write_stall_clear(&mut self, wait_request_id: u64) {
        let Some(cf_id) = self.write_stall_waiters.remove(&wait_request_id) else {
            return;
        };

        let remove_queue = if let Some(queue) = self.write_stall_waiter_queues.get_mut(&cf_id) {
            queue.retain(|request_id| *request_id != wait_request_id);
            queue.is_empty()
        } else {
            false
        };
        if remove_queue {
            self.write_stall_waiter_queues.remove(&cf_id);
        }
    }

    pub(super) fn handle_get_read_amp_metrics(&self, request_id: u64) {
        let metrics = &self.state.read_amp_metrics;
        self.respond(
            request_id,
            RuntimeResponse::ReadAmpMetricsSnapshot {
                request_id,
                reads_total: metrics.reads_total(),
                ssts_touched_total: metrics.ssts_touched_total(),
                l0_ssts_touched_total: metrics.l0_ssts_touched_total(),
                blocks_read_total: metrics.blocks_read_total(),
                avg_ssts_per_read: metrics.avg_ssts_per_read(),
                avg_l0_ssts_per_read: metrics.avg_l0_ssts_per_read(),
                avg_blocks_per_read: metrics.avg_blocks_per_read(),
                l0_overlap_rate: metrics.l0_overlap_rate(),
                sst_budget_violation_rate: metrics.sst_budget_violation_rate(),
                block_budget_violation_rate: metrics.block_budget_violation_rate(),
            },
        );
    }

    pub(super) fn handle_get_recovery_metrics(&self, request_id: u64) {
        self.respond(
            request_id,
            RuntimeResponse::RecoveryMetricsSnapshot {
                request_id,
                wal_recovery_records_replayed: self.state.wal_recovery_records_replayed,
                wal_recovery_bytes_replayed: self.state.wal_recovery_bytes_replayed,
                intent_log_replay_runs: self.state.intent_log_replay_runs,
                intent_log_entries_replayed: self.state.intent_log_entries_replayed,
            },
        );
    }

    pub(super) fn handle_get_runtime_metrics(&self, request_id: u64) {
        let mut snapshot = self.state.runtime_metrics_snapshot();
        if let Some(storage) = &self.hybrid_storage {
            let budget = storage.budget_snapshot();
            snapshot.hybrid_max_local_bytes = budget.max_local_bytes;
            snapshot.hybrid_total_committed_bytes = budget.total_committed_bytes;
            snapshot.hybrid_free_bytes = budget.free_bytes;
            snapshot.hybrid_usage_percent = budget.usage_percent;
            snapshot.hybrid_pending_evictions = budget.pending_evictions;
        }
        self.respond(
            request_id,
            RuntimeResponse::RuntimeMetricsSnapshot {
                request_id,
                snapshot: Box::new(snapshot),
            },
        );
    }

    pub(super) fn handle_get_storage_layout(&self, request_id: u64) {
        self.respond(
            request_id,
            RuntimeResponse::StorageLayoutSnapshot {
                request_id,
                snapshot: self.state.storage_layout_snapshot(),
            },
        );
    }

    pub(super) fn handle_set_runtime_config(
        &mut self,
        update: &RuntimeConfigUpdate,
    ) -> HandleOutcome {
        if let Some(ms) = update.memtable_size_limit {
            self.state.memtable_size_limit = ms;
        }
        if let Some(th) = update.memtable_flush_threshold {
            self.state.memtable_flush_threshold = th;
        }
        if let Some(ec) = update.enable_compaction {
            self.state.set_compaction_enabled(ec);
        }
        if let Some(trigger) = update.l0_compaction_trigger {
            self.compaction_actor.set_l0_file_count_threshold(trigger);
        }

        self.wake_write_stall_waiters();

        if update.wal_durability_policy.is_some() || update.wal_batch_config.is_some() {
            let policy = update
                .wal_durability_policy
                .as_ref()
                .copied()
                .unwrap_or(self.wal_actor.durability_policy());
            let batch_cfg = update
                .wal_batch_config
                .as_ref()
                .copied()
                .unwrap_or(self.wal_actor.batch_config());
            self.wal_actor.set_durability(policy, batch_cfg);
        }

        self.respond(
            update.request_id,
            RuntimeResponse::Ok {
                request_id: update.request_id,
            },
        );
        HandleOutcome::Continue
    }

    #[cfg(test)]
    pub(super) fn handle_get_runtime_config(&self, request_id: u64) {
        self.respond(
            request_id,
            RuntimeResponse::RuntimeConfigSnapshot {
                request_id,
                memtable_size_limit: self.state.memtable_size_limit,
                memtable_flush_threshold: self.state.memtable_flush_threshold,
                enable_compaction: self.state.compaction_enabled(),
                l0_compaction_trigger: self.compaction_actor.l0_file_count_threshold(),
                wal_durability_policy: self.wal_actor.durability_policy(),
                wal_batch_config: self.wal_actor.batch_config(),
            },
        );
    }

    #[cfg(test)]
    pub(super) fn handle_get_current_sequence(&self, request_id: u64) {
        self.respond(
            request_id,
            RuntimeResponse::CurrentSequence {
                request_id,
                sequence: self.state.sequence,
            },
        );
    }
}
