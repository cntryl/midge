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
use crate::runtime::TestRuntimeMsg;
use crate::runtime::{RuntimeMsg, RuntimeResponse};
use crossbeam::channel::{Receiver, Sender};

#[derive(Clone, Copy)]
enum RuntimeRoute {
    CheckWriteStall {
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
    },
    WaitForWriteStallClear {
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
    },
    CancelWaitForWriteStallClear {
        wait_request_id: u64,
    },
    GetReadAmpMetrics {
        request_id: u64,
    },
    GetRecoveryMetrics {
        request_id: u64,
    },
    GetRuntimeMetrics {
        request_id: u64,
    },
    GetStorageLayout {
        request_id: u64,
    },
}

#[derive(Clone, Copy)]
enum SnapshotRoute {
    BeginTransaction {
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
    },
}

enum WalRoute {
    ApplyTransaction {
        request: ApplyTransactionRequest,
        response_tx: Option<Sender<RuntimeResponse>>,
    },
    ApplySpilledTransaction {
        request: SpilledTransactionRequest,
        response_tx: Option<Sender<RuntimeResponse>>,
    },
}

#[derive(Clone, Copy)]
enum FlushRoute {
    FlushMemtable {
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
    },
}

enum CompactionRoute {
    Complete(CompactionCompleteRequest),
}

enum ManifestRoute {
    CreateColumnFamily {
        request_id: u64,
        name: String,
    },
    DropColumnFamily {
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
        discard_unflushed: bool,
    },
}

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
            RuntimeMsg::CheckWriteStall { .. }
            | RuntimeMsg::WaitForWriteStallClear { .. }
            | RuntimeMsg::CancelWaitForWriteStallClear { .. }
            | RuntimeMsg::GetReadAmpMetrics { .. }
            | RuntimeMsg::GetRecoveryMetrics { .. }
            | RuntimeMsg::GetRuntimeMetrics { .. }
            | RuntimeMsg::GetStorageLayout { .. }
            | RuntimeMsg::WalSync { .. }
            | RuntimeMsg::SealWalForCloud { .. }
            | RuntimeMsg::CompactAll { .. }
            | RuntimeMsg::ManifestPersist { .. }
            | RuntimeMsg::SetRuntimeConfig { .. } => Self::handle_control_message(event_loop, &msg),
            RuntimeMsg::BeginTransaction { .. } => Self::handle_snapshot_message(event_loop, &msg),
            RuntimeMsg::ApplyTransaction { .. } | RuntimeMsg::ApplySpilledTransaction { .. } => {
                Self::handle_wal_message(event_loop, msg, msg_rx)
            }
            RuntimeMsg::FlushMemtable { .. } => Self::handle_flush_message(event_loop, &msg),
            RuntimeMsg::CompactionComplete { .. } => {
                Self::handle_compaction_message(event_loop, msg)
            }
            RuntimeMsg::ManifestCreateColumnFamily { .. }
            | RuntimeMsg::ManifestDropColumnFamily { .. } => {
                Self::handle_manifest_message(event_loop, msg, msg_rx)
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

    fn handle_control_message(event_loop: &mut EventLoop, msg: &RuntimeMsg) -> HandleOutcome {
        match *msg {
            RuntimeMsg::CheckWriteStall { request_id, cf_id } => Self::dispatch_runtime(
                event_loop,
                RuntimeRoute::CheckWriteStall { request_id, cf_id },
            ),
            RuntimeMsg::WaitForWriteStallClear { request_id, cf_id } => Self::dispatch_runtime(
                event_loop,
                RuntimeRoute::WaitForWriteStallClear { request_id, cf_id },
            ),
            RuntimeMsg::CancelWaitForWriteStallClear { wait_request_id } => Self::dispatch_runtime(
                event_loop,
                RuntimeRoute::CancelWaitForWriteStallClear { wait_request_id },
            ),
            RuntimeMsg::GetReadAmpMetrics { request_id } => {
                Self::dispatch_runtime(event_loop, RuntimeRoute::GetReadAmpMetrics { request_id })
            }
            RuntimeMsg::GetRecoveryMetrics { request_id } => {
                Self::dispatch_runtime(event_loop, RuntimeRoute::GetRecoveryMetrics { request_id })
            }
            RuntimeMsg::GetRuntimeMetrics { request_id } => {
                Self::dispatch_runtime(event_loop, RuntimeRoute::GetRuntimeMetrics { request_id })
            }
            RuntimeMsg::GetStorageLayout { request_id } => {
                Self::dispatch_runtime(event_loop, RuntimeRoute::GetStorageLayout { request_id })
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
            RuntimeMsg::SetRuntimeConfig {
                request_id,
                memtable_size_limit,
                memtable_flush_threshold,
                enable_compaction,
                l0_compaction_trigger,
                wal_durability_policy,
                wal_batch_config,
            } => {
                let update = RuntimeConfigUpdate {
                    request_id,
                    memtable_size_limit,
                    memtable_flush_threshold,
                    enable_compaction,
                    l0_compaction_trigger,
                    wal_durability_policy,
                    wal_batch_config,
                };
                Self::dispatch_config(event_loop, &update)
            }
            _ => unreachable!("non-control message routed to handle_control_message"),
        }
    }

    fn handle_snapshot_message(event_loop: &mut EventLoop, msg: &RuntimeMsg) -> HandleOutcome {
        match *msg {
            RuntimeMsg::BeginTransaction { request_id, cf_id } => Self::dispatch_snapshot(
                event_loop,
                SnapshotRoute::BeginTransaction { request_id, cf_id },
            ),
            _ => unreachable!("non-snapshot message routed to handle_snapshot_message"),
        }
    }

    fn handle_wal_message(
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
            } => Self::dispatch_wal(
                event_loop,
                WalRoute::ApplyTransaction {
                    request: ApplyTransactionRequest {
                        request_id,
                        ops,
                        assertions,
                        durability_policy,
                        start_sequence,
                        conflict_policy,
                    },
                    response_tx,
                },
                msg_rx,
            ),
            RuntimeMsg::ApplySpilledTransaction {
                request_id,
                source,
                assertions,
                durability_policy,
                start_sequence,
                conflict_policy,
                response_tx,
            } => Self::dispatch_wal(
                event_loop,
                WalRoute::ApplySpilledTransaction {
                    request: SpilledTransactionRequest {
                        request_id,
                        source,
                        assertions,
                        durability_policy,
                        start_sequence,
                        conflict_policy,
                    },
                    response_tx,
                },
                msg_rx,
            ),
            _ => unreachable!("non-WAL message routed to handle_wal_message"),
        }
    }

    fn handle_flush_message(event_loop: &mut EventLoop, msg: &RuntimeMsg) -> HandleOutcome {
        match *msg {
            RuntimeMsg::FlushMemtable { request_id, cf_id } => {
                Self::dispatch_flush(event_loop, FlushRoute::FlushMemtable { request_id, cf_id })
            }
            _ => unreachable!("non-flush message routed to handle_flush_message"),
        }
    }

    fn handle_compaction_message(event_loop: &mut EventLoop, msg: RuntimeMsg) -> HandleOutcome {
        match msg {
            RuntimeMsg::CompactionComplete {
                request_id,
                input_ssts,
                output_ssts,
                cf_id,
                target_level,
                succeeded,
            } => Self::dispatch_compaction(
                event_loop,
                CompactionRoute::Complete(CompactionCompleteRequest {
                    request_id,
                    input_ssts,
                    output_ssts,
                    cf_id,
                    target_level,
                    succeeded,
                }),
            ),
            _ => unreachable!("non-compaction message routed to handle_compaction_message"),
        }
    }

    fn handle_manifest_message(
        event_loop: &mut EventLoop,
        msg: RuntimeMsg,
        msg_rx: &Receiver<RuntimeMsg>,
    ) -> HandleOutcome {
        if let RuntimeMsg::ManifestDropColumnFamily { cf_id, .. } = &msg {
            if event_loop.column_family_publication_pipeline_active(*cf_id) {
                event_loop.publication_gate.defer(msg);
                return HandleOutcome::Continue;
            }
        }
        match msg {
            RuntimeMsg::ManifestCreateColumnFamily { request_id, name } => Self::dispatch_manifest(
                event_loop,
                msg_rx,
                ManifestRoute::CreateColumnFamily { request_id, name },
            ),
            RuntimeMsg::ManifestDropColumnFamily {
                request_id,
                cf_id,
                discard_unflushed,
            } => Self::dispatch_manifest(
                event_loop,
                msg_rx,
                ManifestRoute::DropColumnFamily {
                    request_id,
                    cf_id,
                    discard_unflushed,
                },
            ),
            _ => unreachable!("non-manifest message routed to handle_manifest_message"),
        }
    }

    fn dispatch_runtime(event_loop: &mut EventLoop, route: RuntimeRoute) -> HandleOutcome {
        match route {
            RuntimeRoute::CheckWriteStall { request_id, cf_id } => {
                event_loop.handle_check_write_stall(request_id, cf_id);
            }
            RuntimeRoute::WaitForWriteStallClear { request_id, cf_id } => {
                event_loop.handle_wait_for_write_stall_clear(request_id, cf_id);
            }
            RuntimeRoute::CancelWaitForWriteStallClear { wait_request_id } => {
                event_loop.handle_cancel_wait_for_write_stall_clear(wait_request_id);
            }
            RuntimeRoute::GetReadAmpMetrics { request_id } => {
                event_loop.handle_get_read_amp_metrics(request_id);
            }
            RuntimeRoute::GetRecoveryMetrics { request_id } => {
                event_loop.handle_get_recovery_metrics(request_id);
            }
            RuntimeRoute::GetRuntimeMetrics { request_id } => {
                event_loop.handle_get_runtime_metrics(request_id);
            }
            RuntimeRoute::GetStorageLayout { request_id } => {
                event_loop.handle_get_storage_layout(request_id);
            }
        }
        HandleOutcome::Continue
    }

    fn dispatch_config(event_loop: &mut EventLoop, update: &RuntimeConfigUpdate) -> HandleOutcome {
        event_loop.handle_set_runtime_config(update)
    }

    fn dispatch_snapshot(event_loop: &mut EventLoop, route: SnapshotRoute) -> HandleOutcome {
        match route {
            SnapshotRoute::BeginTransaction { request_id, cf_id } => {
                SnapshotCoordinator::begin_transaction(event_loop, request_id, cf_id)
            }
        }
    }

    fn dispatch_wal(
        event_loop: &mut EventLoop,
        route: WalRoute,
        msg_rx: &Receiver<RuntimeMsg>,
    ) -> HandleOutcome {
        match route {
            WalRoute::ApplyTransaction {
                request,
                response_tx,
            } => WalCoordinator::apply_transaction(event_loop, msg_rx, request, response_tx),
            WalRoute::ApplySpilledTransaction {
                request,
                response_tx,
            } => {
                WalCoordinator::apply_spilled_transaction(event_loop, msg_rx, request, response_tx)
            }
        }
    }

    fn dispatch_flush(event_loop: &mut EventLoop, route: FlushRoute) -> HandleOutcome {
        match route {
            FlushRoute::FlushMemtable { request_id, cf_id } => {
                FlushCoordinator::flush_memtable(event_loop, request_id, cf_id)
            }
        }
    }

    fn dispatch_compaction(event_loop: &mut EventLoop, route: CompactionRoute) -> HandleOutcome {
        match route {
            CompactionRoute::Complete(request) => {
                CompactionCoordinator::complete(event_loop, request)
            }
        }
    }

    fn dispatch_manifest(
        event_loop: &mut EventLoop,
        msg_rx: &Receiver<RuntimeMsg>,
        route: ManifestRoute,
    ) -> HandleOutcome {
        match route {
            ManifestRoute::CreateColumnFamily { request_id, name } => {
                ManifestCoordinator::create_column_family(event_loop, msg_rx, request_id, &name)
            }
            ManifestRoute::DropColumnFamily {
                request_id,
                cf_id,
                discard_unflushed,
            } => ManifestCoordinator::drop_column_family(
                event_loop,
                msg_rx,
                request_id,
                cf_id,
                discard_unflushed,
            ),
        }
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
