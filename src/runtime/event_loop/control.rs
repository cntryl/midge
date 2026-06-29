use super::{EventLoop, HandleOutcome};
use crate::runtime::RuntimeResponse;

impl EventLoop {
    pub(super) fn handle_noop(&self, request_id: u64) {
        self.respond(request_id, RuntimeResponse::Ok { request_id });
    }

    pub(super) fn handle_startup_ping(&self, request_id: u64) {
        self.respond(request_id, RuntimeResponse::Ok { request_id });
    }

    pub(super) fn handle_check_write_stall(
        &self,
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
    ) {
        let is_stalled = self.state.should_stall_writes(cf_id);
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
        if !self.state.should_stall_writes(cf_id) {
            self.respond(request_id, RuntimeResponse::Ok { request_id });
        } else {
            self.write_stall_waiters.insert(request_id, cf_id);
            self.write_stall_waiter_queues
                .entry(cf_id)
                .or_default()
                .push_back(request_id);
        }

        self.drain_auto_flush_memtables();
    }

    pub(super) fn handle_cancel_wait_for_write_stall_clear(&mut self, wait_request_id: u64) {
        let _ = self.write_stall_waiters.remove(&wait_request_id);
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
        self.respond(
            request_id,
            RuntimeResponse::RuntimeMetricsSnapshot {
                request_id,
                snapshot: Box::new(self.state.runtime_metrics_snapshot()),
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

    #[allow(clippy::too_many_arguments)]
    pub(super) fn handle_set_runtime_config(
        &mut self,
        request_id: u64,
        memtable_size_limit: Option<usize>,
        memtable_flush_threshold: Option<usize>,
        enable_compaction: Option<bool>,
        l0_compaction_trigger: Option<usize>,
        wal_durability_policy: Option<crate::wal::DurabilityPolicy>,
        wal_batch_config: Option<crate::wal::policy::BatchConfig>,
    ) -> HandleOutcome {
        if let Some(ms) = memtable_size_limit {
            self.state.memtable_size_limit = ms;
        }
        if let Some(th) = memtable_flush_threshold {
            self.state.memtable_flush_threshold = th;
        }
        if let Some(ec) = enable_compaction {
            self.state.enable_compaction = ec;
        }
        if let Some(trigger) = l0_compaction_trigger {
            self.compaction_actor.set_l0_file_count_threshold(trigger);
        }

        self.wake_write_stall_waiters();

        if wal_durability_policy.is_some() || wal_batch_config.is_some() {
            let policy = wal_durability_policy.unwrap_or(self.wal_actor.durability_policy());
            let batch_cfg = wal_batch_config.unwrap_or(self.wal_actor.batch_config());
            if let Err(error) = self.wal_actor.set_durability(policy, batch_cfg) {
                self.respond(
                    request_id,
                    RuntimeResponse::Error {
                        request_id,
                        error: crate::common::MidgeError::Internal(error.to_string()),
                    },
                );
                return HandleOutcome::Continue;
            }
        }

        self.respond(request_id, RuntimeResponse::Ok { request_id });
        HandleOutcome::Continue
    }

    pub(super) fn handle_get_runtime_config(&self, request_id: u64) {
        self.respond(
            request_id,
            RuntimeResponse::RuntimeConfigSnapshot {
                request_id,
                memtable_size_limit: self.state.memtable_size_limit,
                memtable_flush_threshold: self.state.memtable_flush_threshold,
                enable_compaction: self.state.enable_compaction,
                l0_compaction_trigger: self.compaction_actor.l0_file_count_threshold(),
                wal_durability_policy: self.wal_actor.durability_policy(),
                wal_batch_config: self.wal_actor.batch_config(),
            },
        );
    }

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
