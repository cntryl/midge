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
            self.write_stall_waiters.register(request_id, cf_id);
        } else {
            self.respond(request_id, RuntimeResponse::Ok { request_id });
        }

        self.drain_auto_flush_memtables();
    }

    pub(super) fn handle_cancel_wait_for_write_stall_clear(&mut self, wait_request_id: u64) {
        self.write_stall_waiters.cancel(wait_request_id);
    }

    pub(super) fn handle_get_read_amp_metrics(&self, request_id: u64) {
        let metrics = self.state.diagnostics.read_amp_metrics();
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
        crate::failpoints::fail_point!("midge::runtime::before_get_runtime_metrics_response");
        let mut snapshot = self.state.runtime_metrics_snapshot();
        let read_path = self.state.diagnostics.snapshot();
        snapshot.durability_waiters_fanned_out_total = self.durability.waiters_fanned_out();
        snapshot.abandoned_runtime_requests_total = self.router.abandoned_requests_total();
        snapshot.late_runtime_responses_total = self.router.late_responses_total();
        snapshot.sst_bloom_rejects_total = read_path.bloom_rejects;
        snapshot.sst_bloom_checks_total = read_path.bloom_checks;
        snapshot.sst_data_blocks_read_total = read_path.data_blocks_read;
        snapshot.remote_range_requests_total = read_path.remote_range_requests_total;
        snapshot.remote_range_bytes_total = read_path.remote_range_bytes_total;
        snapshot.remote_range_failures_total = read_path.remote_range_failures_total;
        snapshot.remote_range_latency_ns_total = read_path.remote_range_latency_ns_total;
        snapshot.remote_range_latency_ns_max = read_path.remote_range_latency_ns_max;
        if let Some(storage) = &self.hybrid_storage {
            let budget = storage.budget_snapshot();
            snapshot.hybrid_max_local_bytes = budget.max_local_bytes;
            snapshot.hybrid_total_committed_bytes = budget.total_committed_bytes;
            snapshot.hybrid_free_bytes = budget.free_bytes;
            snapshot.hybrid_usage_percent = budget.usage_percent;
            snapshot.hybrid_pending_evictions = budget.pending_evictions;
            snapshot.local_storage = Some(budget);
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
        let candidate_memtable_size_limit = update
            .memtable_size_limit
            .unwrap_or(self.state.memtable_size_limit);
        let candidate_memtable_flush_threshold = update
            .memtable_flush_threshold
            .unwrap_or(self.state.memtable_flush_threshold);
        if let Err(error) = crate::config::validate_memtable_limits(
            candidate_memtable_size_limit,
            candidate_memtable_flush_threshold,
        ) {
            self.respond(
                update.request_id,
                RuntimeResponse::Error {
                    request_id: update.request_id,
                    error,
                },
            );
            return HandleOutcome::Continue;
        }

        let candidate_wal_policy = update
            .wal_durability_policy
            .unwrap_or(self.wal_actor.durability_policy());
        // The durability coordinator keys waiters by generation in local
        // modes and by segment id in `CloudAsync`. Switching between the two
        // at runtime would let the actor and coordinator disagree about what
        // a generation means. Reject the complete update before mutating any
        // runtime field so `SetRuntimeConfig` remains atomic.
        let switches_cloud_mode = matches!(
            candidate_wal_policy,
            crate::wal::DurabilityPolicy::CloudAsync
        ) != self.wal_actor.is_cloud_async();
        if switches_cloud_mode {
            self.respond(
                update.request_id,
                RuntimeResponse::Error {
                    request_id: update.request_id,
                    error: crate::common::MidgeError::InvalidArgument(format!(
                        "cannot switch WAL durability policy to {candidate_wal_policy:?} at runtime; cloud-backed and local modes key durability generations differently"
                    )),
                },
            );
            return HandleOutcome::Continue;
        }

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
            let trigger = trigger.max(1);
            self.state.l0_compaction_trigger = trigger;
            self.compaction_actor.set_l0_file_count_threshold(trigger);
        }

        self.wake_write_stall_waiters();

        if update.wal_durability_policy.is_some() || update.wal_batch_config.is_some() {
            let batch_cfg = update
                .wal_batch_config
                .as_ref()
                .copied()
                .unwrap_or(self.wal_actor.batch_config());
            self.wal_actor
                .set_durability(candidate_wal_policy, batch_cfg);
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

#[cfg(test)]
mod tests {
    use super::super::tests::{create_test_cloud_event_loop, create_test_local_event_loop};
    use super::RuntimeConfigUpdate;
    use crate::runtime::RuntimeResponse;

    fn policy_update(request_id: u64, policy: crate::wal::DurabilityPolicy) -> RuntimeConfigUpdate {
        RuntimeConfigUpdate {
            request_id,
            memtable_size_limit: None,
            memtable_flush_threshold: None,
            enable_compaction: None,
            l0_compaction_trigger: None,
            wal_durability_policy: Some(policy),
            wal_batch_config: None,
        }
    }

    #[test]
    fn should_reject_cross_mode_wal_policy_switches_when_runtime_config_changes(
    ) -> crate::common::MidgeResult<()> {
        // Arrange
        let mut local = create_test_local_event_loop()?;
        let mut cloud = create_test_cloud_event_loop(
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        )?;
        let local_response = local.router.register(1, "SetRuntimeConfig");
        let cloud_response = cloud.router.register(2, "SetRuntimeConfig");
        let same_mode_response = local.router.register(3, "SetRuntimeConfig");

        // Act
        local
            .handle_set_runtime_config(&policy_update(1, crate::wal::DurabilityPolicy::CloudAsync));
        cloud.handle_set_runtime_config(&policy_update(2, crate::wal::DurabilityPolicy::Batched));
        local.handle_set_runtime_config(&policy_update(3, crate::wal::DurabilityPolicy::Strict));

        // Assert
        assert!(matches!(
            local_response.recv_timeout(std::time::Duration::from_secs(1)),
            Ok(RuntimeResponse::Error {
                error: crate::common::MidgeError::InvalidArgument(_),
                ..
            })
        ));
        assert!(matches!(
            cloud_response.recv_timeout(std::time::Duration::from_secs(1)),
            Ok(RuntimeResponse::Error {
                error: crate::common::MidgeError::InvalidArgument(_),
                ..
            })
        ));
        assert!(matches!(
            same_mode_response.recv_timeout(std::time::Duration::from_secs(1)),
            Ok(RuntimeResponse::Ok { request_id: 3 })
        ));
        assert!(!local.wal_actor.is_cloud_async());
        assert!(cloud.wal_actor.is_cloud_async());
        assert_eq!(
            local.wal_actor.durability_policy(),
            crate::wal::DurabilityPolicy::Strict
        );
        Ok(())
    }
}
