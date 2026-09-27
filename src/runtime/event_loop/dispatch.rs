#[cfg(test)]
use super::gc::GcCoordinator;
#[cfg(test)]
use super::wal::AppendRequest;
use super::{
    compaction::{CompactionCompleteRequest, CompactionCoordinator},
    control::RuntimeConfigUpdate,
    flush::FlushCoordinator,
    manifest::ManifestCoordinator,
    snapshot::SnapshotCoordinator,
    wal::{ApplyTransactionRequest, SpilledTransactionRequest, WalCoordinator},
    EventLoop, HandleOutcome,
};
use crate::runtime::RuntimeMsg;
use crate::runtime::TestRuntimeMsg;
use crossbeam::channel::Receiver;

pub(super) struct RuntimeDispatcher;

impl RuntimeDispatcher {
    pub(super) fn handle(
        event_loop: &mut EventLoop,
        msg: RuntimeMsg,
        msg_rx: &Receiver<RuntimeMsg>,
    ) -> HandleOutcome {
        if let Some(outcome) = Self::handle_early_control(event_loop, &msg) {
            return outcome;
        }
        let Some(msg) = event_loop.gate_message_for_storage_verification(msg) else {
            return HandleOutcome::Continue;
        };
        let Some(msg) = event_loop.gate_message_for_flush_publication(msg) else {
            return HandleOutcome::Continue;
        };

        match msg {
            RuntimeMsg::CheckWriteStall { request_id, cf_id } => {
                event_loop.handle_check_write_stall(request_id, cf_id);
                HandleOutcome::Continue
            }
            RuntimeMsg::WaitForWriteStallClear { request_id, cf_id } => {
                event_loop.handle_wait_for_write_stall_clear(request_id, cf_id);
                HandleOutcome::Continue
            }
            RuntimeMsg::CancelWaitForWriteStallClear { wait_request_id } => {
                event_loop.handle_cancel_wait_for_write_stall_clear(wait_request_id);
                HandleOutcome::Continue
            }
            RuntimeMsg::GetReadAmpMetrics { request_id } => {
                event_loop.handle_get_read_amp_metrics(request_id);
                HandleOutcome::Continue
            }
            RuntimeMsg::GetRecoveryMetrics { request_id } => {
                event_loop.handle_get_recovery_metrics(request_id);
                HandleOutcome::Continue
            }
            RuntimeMsg::GetRuntimeMetrics { request_id } => {
                event_loop.handle_get_runtime_metrics(request_id);
                HandleOutcome::Continue
            }
            RuntimeMsg::GetStorageLayout { request_id } => {
                event_loop.handle_get_storage_layout(request_id);
                HandleOutcome::Continue
            }
            RuntimeMsg::WalSync { request_id } => WalCoordinator::sync(event_loop, request_id),
            RuntimeMsg::SealWalForCloud {
                request_id,
                sequence,
                wait_for_ack,
            } => WalCoordinator::seal_for_cloud(event_loop, request_id, sequence, wait_for_ack),
            RuntimeMsg::CompactAll { request_id } => {
                CompactionCoordinator::compact_all(event_loop, request_id)
            }
            RuntimeMsg::ManifestPersist { request_id } => {
                ManifestCoordinator::persist(event_loop, request_id)
            }
            msg @ RuntimeMsg::SetRuntimeConfig { .. } => Self::set_runtime_config(event_loop, &msg),
            RuntimeMsg::BeginTransaction { request_id, cf_id } => {
                SnapshotCoordinator::begin_transaction(event_loop, request_id, cf_id)
            }
            msg @ (RuntimeMsg::ApplyTransaction { .. }
            | RuntimeMsg::ApplySpilledTransaction { .. }) => {
                Self::apply_transaction(event_loop, msg, msg_rx)
            }
            RuntimeMsg::FlushMemtable { request_id, cf_id } => {
                FlushCoordinator::flush_memtable(event_loop, request_id, cf_id)
            }
            RuntimeMsg::CompactionComplete {
                request_id,
                input_ssts,
                output_ssts,
                cf_id,
                target_level,
                succeeded,
            } => CompactionCoordinator::complete(
                event_loop,
                CompactionCompleteRequest {
                    request_id,
                    input_ssts,
                    output_ssts,
                    cf_id,
                    target_level,
                    succeeded,
                },
            ),
            RuntimeMsg::ManifestCreateColumnFamily { request_id, name } => {
                ManifestCoordinator::create_column_family(event_loop, msg_rx, request_id, &name)
            }
            msg @ RuntimeMsg::ManifestDropColumnFamily { .. } => {
                Self::drop_column_family(event_loop, msg, msg_rx)
            }
            RuntimeMsg::RetryGc => event_loop.retry_gc(),
            RuntimeMsg::Test(msg) => Self::handle_test_message(event_loop, msg, msg_rx),
            _ => unreachable!("message handled before dispatch routing"),
        }
    }

    fn handle_early_control(event_loop: &mut EventLoop, msg: &RuntimeMsg) -> Option<HandleOutcome> {
        match msg {
            RuntimeMsg::Shutdown => Some(event_loop.handle_shutdown_request(None)),
            &RuntimeMsg::ShutdownWithResponse { request_id } => {
                Some(event_loop.handle_shutdown_request(Some(request_id)))
            }
            &RuntimeMsg::BeginStorageVerification { request_id } => {
                Some(event_loop.begin_storage_verification(request_id))
            }
            &RuntimeMsg::BeginBackupCapture { request_id } => {
                Some(event_loop.begin_backup_capture(request_id))
            }
            &RuntimeMsg::EndStorageVerification { request_id, token } => {
                Some(event_loop.end_storage_verification(request_id, token))
            }
            _ => None,
        }
    }

    fn set_runtime_config(event_loop: &mut EventLoop, msg: &RuntimeMsg) -> HandleOutcome {
        let &RuntimeMsg::SetRuntimeConfig {
            request_id,
            memtable_size_limit,
            memtable_flush_threshold,
            enable_compaction,
            l0_compaction_trigger,
            wal_durability_policy,
            wal_batch_config,
        } = msg
        else {
            unreachable!("non-config message routed to set_runtime_config");
        };
        event_loop.handle_set_runtime_config(&RuntimeConfigUpdate {
            request_id,
            memtable_size_limit,
            memtable_flush_threshold,
            enable_compaction,
            l0_compaction_trigger,
            wal_durability_policy,
            wal_batch_config,
        })
    }

    fn apply_transaction(
        event_loop: &mut EventLoop,
        msg: RuntimeMsg,
        msg_rx: &Receiver<RuntimeMsg>,
    ) -> HandleOutcome {
        match msg {
            RuntimeMsg::ApplyTransaction {
                request_id,
                ops,
                assertions,
                durability_policy,
                start_sequence,
                conflict_policy,
                response_tx,
            } => WalCoordinator::apply_transaction(
                event_loop,
                msg_rx,
                ApplyTransactionRequest {
                    request_id,
                    ops,
                    assertions,
                    durability_policy,
                    start_sequence,
                    conflict_policy,
                },
                response_tx,
            ),
            RuntimeMsg::ApplySpilledTransaction {
                request_id,
                source,
                assertions,
                durability_policy,
                start_sequence,
                conflict_policy,
                response_tx,
            } => WalCoordinator::apply_spilled_transaction(
                event_loop,
                msg_rx,
                SpilledTransactionRequest {
                    request_id,
                    source,
                    assertions,
                    durability_policy,
                    start_sequence,
                    conflict_policy,
                },
                response_tx,
            ),
            _ => unreachable!("non-transaction message routed to apply_transaction"),
        }
    }

    /// Dropping a family waits until its publication pipeline drains.
    fn drop_column_family(
        event_loop: &mut EventLoop,
        msg: RuntimeMsg,
        msg_rx: &Receiver<RuntimeMsg>,
    ) -> HandleOutcome {
        let RuntimeMsg::ManifestDropColumnFamily {
            request_id,
            cf_id,
            discard_unflushed,
        } = msg
        else {
            unreachable!("non-drop message routed to drop_column_family");
        };
        if event_loop.column_family_publication_pipeline_active(cf_id) {
            event_loop.publication_gate.defer(msg);
            return HandleOutcome::Continue;
        }
        ManifestCoordinator::drop_column_family(
            event_loop,
            msg_rx,
            request_id,
            cf_id,
            discard_unflushed,
        )
    }
}

/// Release-build counterpart of the test-only dispatch extension below.
///
/// `TestRuntimeMsg` is uninhabited outside `cfg(test)`, so this arm is
/// statically unreachable and the production `match` above needs no
/// `#[cfg(test)]` arm to stay well formed.
#[cfg(not(test))]
impl RuntimeDispatcher {
    // `msg` is uninhabited here, so the empty `match` diverges without reading it.
    #[allow(clippy::needless_pass_by_value)]
    fn handle_test_message(
        _event_loop: &mut EventLoop,
        msg: TestRuntimeMsg,
        _msg_rx: &Receiver<RuntimeMsg>,
    ) -> HandleOutcome {
        match msg {}
    }
}

/// Test-only dispatch extension.
///
/// Every `TestRuntimeMsg` hook is routed here, and only here. Keeping it out of
/// the production `match` above means a test hook cannot be added to, or removed
/// from, a production routing table by accident: the tables a test exercises are
/// byte-for-byte the tables a release build runs.
#[cfg(test)]
impl RuntimeDispatcher {
    fn handle_test_message(
        event_loop: &mut EventLoop,
        msg: TestRuntimeMsg,
        msg_rx: &Receiver<RuntimeMsg>,
    ) -> HandleOutcome {
        match msg {
            TestRuntimeMsg::Noop { .. }
            | TestRuntimeMsg::StartupPing { .. }
            | TestRuntimeMsg::GetRuntimeConfig { .. }
            | TestRuntimeMsg::GetCurrentSequence { .. }
            | TestRuntimeMsg::WalRotate { .. }
            | TestRuntimeMsg::CheckCompaction { .. }
            | TestRuntimeMsg::CheckGc { .. } => Self::handle_test_control_message(event_loop, &msg),
            TestRuntimeMsg::CaptureReadSnapshot { .. }
            | TestRuntimeMsg::RegisterSnapshot { .. }
            | TestRuntimeMsg::UnregisterSnapshot { .. } => {
                Self::handle_test_snapshot_message(event_loop, msg)
            }
            TestRuntimeMsg::WalAppend { .. } | TestRuntimeMsg::WalAppendDeleteRange { .. } => {
                Self::handle_test_wal_message(event_loop, msg, msg_rx)
            }
            TestRuntimeMsg::FlushComplete { .. }
            | TestRuntimeMsg::RunCompaction { .. }
            | TestRuntimeMsg::DeleteObsoleteSsts { .. }
            | TestRuntimeMsg::ManifestAddSst { .. }
            | TestRuntimeMsg::ManifestCompactionComplete { .. } => {
                Self::handle_test_storage_message(event_loop, msg)
            }
            TestRuntimeMsg::Read { .. } | TestRuntimeMsg::RangeScan { .. } => {
                Self::handle_test_read_message(event_loop, msg)
            }
        }
    }

    fn handle_test_control_message(
        event_loop: &mut EventLoop,
        msg: &TestRuntimeMsg,
    ) -> HandleOutcome {
        match *msg {
            TestRuntimeMsg::Noop { request_id } => event_loop.handle_noop(request_id),
            TestRuntimeMsg::StartupPing { request_id } => {
                event_loop.handle_startup_ping(request_id);
            }
            TestRuntimeMsg::GetRuntimeConfig { request_id } => {
                event_loop.handle_get_runtime_config(request_id);
            }
            TestRuntimeMsg::GetCurrentSequence { request_id } => {
                event_loop.handle_get_current_sequence(request_id);
            }
            TestRuntimeMsg::WalRotate { request_id } => {
                return WalCoordinator::rotate(event_loop, request_id)
            }
            TestRuntimeMsg::CheckCompaction { request_id } => {
                return CompactionCoordinator::check(event_loop, request_id)
            }
            TestRuntimeMsg::CheckGc { request_id } => {
                return GcCoordinator::check(event_loop, request_id)
            }
            _ => unreachable!("non-control hook routed to handle_test_control_message"),
        }
        HandleOutcome::Continue
    }

    fn handle_test_snapshot_message(
        event_loop: &mut EventLoop,
        msg: TestRuntimeMsg,
    ) -> HandleOutcome {
        match msg {
            TestRuntimeMsg::CaptureReadSnapshot {
                request_id,
                cf_id,
                sequence,
            } => SnapshotCoordinator::capture(event_loop, request_id, cf_id, sequence),
            TestRuntimeMsg::RegisterSnapshot {
                request_id,
                snapshot_id,
                sequence,
                pinned_sst_names,
            } => SnapshotCoordinator::register(
                event_loop,
                request_id,
                snapshot_id,
                sequence,
                pinned_sst_names,
            ),
            TestRuntimeMsg::UnregisterSnapshot { snapshot_id } => {
                SnapshotCoordinator::unregister(event_loop, snapshot_id)
            }
            _ => unreachable!("non-snapshot hook routed to handle_test_snapshot_message"),
        }
    }

    fn handle_test_wal_message(
        event_loop: &mut EventLoop,
        msg: TestRuntimeMsg,
        msg_rx: &Receiver<RuntimeMsg>,
    ) -> HandleOutcome {
        match msg {
            TestRuntimeMsg::WalAppend {
                request_id,
                cf_id,
                key,
                value,
                ttl_seconds,
                insert_only,
            } => WalCoordinator::append(
                event_loop,
                msg_rx,
                AppendRequest {
                    request_id,
                    cf_id,
                    key,
                    value,
                    ttl_seconds,
                    insert_only,
                },
            ),
            TestRuntimeMsg::WalAppendDeleteRange {
                request_id,
                cf_id,
                start_key,
                end_key,
                durability_policy,
            } => WalCoordinator::append_delete_range(
                event_loop,
                msg_rx,
                request_id,
                cf_id,
                start_key,
                end_key,
                durability_policy,
            ),
            _ => unreachable!("non-WAL hook routed to handle_test_wal_message"),
        }
    }

    fn handle_test_storage_message(
        event_loop: &mut EventLoop,
        msg: TestRuntimeMsg,
    ) -> HandleOutcome {
        match msg {
            TestRuntimeMsg::FlushComplete {
                request_id,
                cf_id,
                sst_name,
                sequence,
            } => {
                FlushCoordinator::flush_complete(event_loop, request_id, cf_id, &sst_name, sequence)
            }
            TestRuntimeMsg::RunCompaction { request_id, plan } => {
                CompactionCoordinator::run(event_loop, request_id, plan)
            }
            TestRuntimeMsg::DeleteObsoleteSsts {
                request_id,
                sst_names,
            } => GcCoordinator::delete_obsolete_ssts(event_loop, request_id, &sst_names),
            TestRuntimeMsg::ManifestAddSst {
                request_id,
                file_meta,
            } => ManifestCoordinator::add_sst(event_loop, request_id, &file_meta),
            TestRuntimeMsg::ManifestCompactionComplete {
                request_id,
                removed,
                added,
            } => ManifestCoordinator::compaction_complete(event_loop, request_id, &removed, &added),
            _ => unreachable!("non-storage hook routed to handle_test_storage_message"),
        }
    }

    fn handle_test_read_message(event_loop: &mut EventLoop, msg: TestRuntimeMsg) -> HandleOutcome {
        match msg {
            TestRuntimeMsg::Read {
                request_id,
                cf_id,
                key,
                sequence,
                requested_durability,
            } => {
                event_loop.handle_msg_read(request_id, cf_id, key, sequence, requested_durability);
            }
            TestRuntimeMsg::RangeScan {
                request_id,
                cf_id,
                start,
                end,
                sequence,
                requested_durability,
            } => {
                event_loop.handle_msg_range_scan(
                    request_id,
                    cf_id,
                    start,
                    end,
                    sequence,
                    requested_durability,
                );
            }
            _ => unreachable!("non-read hook routed to handle_test_read_message"),
        }
        HandleOutcome::Continue
    }
}
