#[cfg(test)]
use super::cloud::CloudCoordinator;
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
use crate::runtime::{RuntimeMsg, RuntimeResponse};
use crossbeam::channel::{Receiver, Sender};

#[derive(Clone, Copy)]
enum RuntimeRoute {
    #[cfg(test)]
    Noop {
        request_id: u64,
    },
    #[cfg(test)]
    StartupPing {
        request_id: u64,
    },
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
    #[cfg(test)]
    GetRuntimeConfig {
        request_id: u64,
    },
    #[cfg(test)]
    GetIngestState {
        request_id: u64,
    },
    #[cfg(test)]
    BeginIngest {
        request_id: u64,
    },
    #[cfg(test)]
    EndIngest {
        request_id: u64,
    },
    #[cfg(test)]
    GetCurrentSequence {
        request_id: u64,
    },
}

#[cfg_attr(not(test), derive(Clone, Copy))]
enum SnapshotRoute {
    #[cfg(test)]
    CaptureReadSnapshot {
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
        sequence: u64,
    },
    BeginTransaction {
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
    },
    #[cfg(test)]
    RegisterSnapshot {
        request_id: u64,
        snapshot_id: u64,
        sequence: u64,
        pinned_sst_names: Vec<String>,
    },
    #[cfg(test)]
    UnregisterSnapshot { snapshot_id: u64 },
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
    #[cfg(test)]
    Append(AppendRequest),
    #[cfg(test)]
    AppendDeleteRange {
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        durability_policy: Option<crate::wal::DurabilityPolicy>,
    },
    #[cfg(test)]
    SyncComplete { request_id: u64, segment_id: u64 },
}

#[cfg_attr(not(test), derive(Clone, Copy))]
enum FlushRoute {
    FlushMemtable {
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
    },
    #[cfg(test)]
    FlushComplete {
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
        sst_name: String,
        sequence: u64,
    },
}

enum CompactionRoute {
    #[cfg(test)]
    Run {
        request_id: u64,
        plan: crate::runtime::CompactionPlan,
    },
    Complete(CompactionCompleteRequest),
}

#[cfg(test)]
enum CloudRoute {
    Sst { request_id: u64, sst_name: String },
    Wal { request_id: u64, segment_id: u64 },
    Complete { request_id: u64, resource: String },
}

#[cfg(test)]
enum GcRoute {
    DeleteObsoleteSsts {
        request_id: u64,
        sst_names: Vec<String>,
    },
}

enum ManifestRoute {
    #[cfg(test)]
    AddSst {
        request_id: u64,
        file_meta: crate::runtime::FileMeta,
    },
    #[cfg(test)]
    CompactionComplete {
        request_id: u64,
        removed: Vec<String>,
        added: Vec<crate::runtime::FileMeta>,
    },
    CreateColumnFamily {
        request_id: u64,
        name: String,
    },
    DropColumnFamily {
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
    },
}

#[cfg(test)]
enum ReadRoute {
    Read {
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
        key: Vec<u8>,
        sequence: u64,
        requested_durability: crate::types::ReadDurability,
    },
    RangeScan {
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
        start: Vec<u8>,
        end: Vec<u8>,
        sequence: u64,
        requested_durability: crate::types::ReadDurability,
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
            #[cfg(test)]
            RuntimeMsg::Noop { .. } => Self::handle_control_message(event_loop, &msg),
            #[cfg(test)]
            RuntimeMsg::StartupPing { .. } => Self::handle_control_message(event_loop, &msg),
            #[cfg(test)]
            RuntimeMsg::GetRuntimeConfig { .. } => Self::handle_control_message(event_loop, &msg),
            #[cfg(test)]
            RuntimeMsg::GetIngestState { .. } => Self::handle_control_message(event_loop, &msg),
            #[cfg(test)]
            RuntimeMsg::BeginIngest { .. } => Self::handle_control_message(event_loop, &msg),
            #[cfg(test)]
            RuntimeMsg::EndIngest { .. } => Self::handle_control_message(event_loop, &msg),
            #[cfg(test)]
            RuntimeMsg::GetCurrentSequence { .. } => Self::handle_control_message(event_loop, &msg),
            #[cfg(test)]
            RuntimeMsg::WalRotate { .. } => Self::handle_control_message(event_loop, &msg),
            #[cfg(test)]
            RuntimeMsg::CheckCompaction { .. } => Self::handle_control_message(event_loop, &msg),
            #[cfg(test)]
            RuntimeMsg::CheckGc { .. } => Self::handle_control_message(event_loop, &msg),
            #[cfg(test)]
            RuntimeMsg::CaptureReadSnapshot { .. } => {
                Self::handle_snapshot_message(event_loop, &msg)
            }
            #[cfg(test)]
            RuntimeMsg::RegisterSnapshot { .. } => Self::handle_snapshot_message(event_loop, &msg),
            #[cfg(test)]
            RuntimeMsg::UnregisterSnapshot { .. } => {
                Self::handle_snapshot_message(event_loop, &msg)
            }
            #[cfg(test)]
            RuntimeMsg::WalAppend { .. } => Self::handle_wal_message(event_loop, msg, msg_rx),
            #[cfg(test)]
            RuntimeMsg::WalAppendDeleteRange { .. } => {
                Self::handle_wal_message(event_loop, msg, msg_rx)
            }
            #[cfg(test)]
            RuntimeMsg::WalSyncComplete { .. } => Self::handle_wal_message(event_loop, msg, msg_rx),
            #[cfg(test)]
            RuntimeMsg::FlushComplete { .. } => Self::handle_flush_message(event_loop, &msg),
            #[cfg(test)]
            RuntimeMsg::RunCompaction { .. } => Self::handle_compaction_message(event_loop, msg),
            #[cfg(test)]
            RuntimeMsg::CloudUploadSst { .. } => Self::handle_cloud_message(event_loop, msg),
            #[cfg(test)]
            RuntimeMsg::CloudUploadWal { .. } => Self::handle_cloud_message(event_loop, msg),
            #[cfg(test)]
            RuntimeMsg::CloudUploadComplete { .. } => Self::handle_cloud_message(event_loop, msg),
            #[cfg(test)]
            RuntimeMsg::DeleteObsoleteSsts { .. } => Self::handle_gc_message(event_loop, msg),
            #[cfg(test)]
            RuntimeMsg::ManifestAddSst { .. } => {
                Self::handle_manifest_message(event_loop, msg, msg_rx)
            }
            #[cfg(test)]
            RuntimeMsg::ManifestCompactionComplete { .. } => {
                Self::handle_manifest_message(event_loop, msg, msg_rx)
            }
            #[cfg(test)]
            RuntimeMsg::Read { .. } | RuntimeMsg::RangeScan { .. } => {
                Self::handle_read_message(event_loop, msg)
            }
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
            &RuntimeMsg::EndStorageVerification { request_id, token } => {
                Some(event_loop.end_storage_verification(request_id, token))
            }
            _ => None,
        }
    }

    fn handle_control_message(event_loop: &mut EventLoop, msg: &RuntimeMsg) -> HandleOutcome {
        match msg {
            &RuntimeMsg::CheckWriteStall { request_id, cf_id } => Self::dispatch_runtime(
                event_loop,
                RuntimeRoute::CheckWriteStall { request_id, cf_id },
            ),
            &RuntimeMsg::WaitForWriteStallClear { request_id, cf_id } => Self::dispatch_runtime(
                event_loop,
                RuntimeRoute::WaitForWriteStallClear { request_id, cf_id },
            ),
            &RuntimeMsg::CancelWaitForWriteStallClear { wait_request_id } => {
                Self::dispatch_runtime(
                    event_loop,
                    RuntimeRoute::CancelWaitForWriteStallClear { wait_request_id },
                )
            }
            &RuntimeMsg::GetReadAmpMetrics { request_id } => {
                Self::dispatch_runtime(event_loop, RuntimeRoute::GetReadAmpMetrics { request_id })
            }
            &RuntimeMsg::GetRecoveryMetrics { request_id } => {
                Self::dispatch_runtime(event_loop, RuntimeRoute::GetRecoveryMetrics { request_id })
            }
            &RuntimeMsg::GetRuntimeMetrics { request_id } => {
                Self::dispatch_runtime(event_loop, RuntimeRoute::GetRuntimeMetrics { request_id })
            }
            &RuntimeMsg::GetStorageLayout { request_id } => {
                Self::dispatch_runtime(event_loop, RuntimeRoute::GetStorageLayout { request_id })
            }
            #[cfg(test)]
            &RuntimeMsg::Noop { request_id } => {
                Self::dispatch_runtime(event_loop, RuntimeRoute::Noop { request_id })
            }
            #[cfg(test)]
            &RuntimeMsg::StartupPing { request_id } => {
                Self::dispatch_runtime(event_loop, RuntimeRoute::StartupPing { request_id })
            }
            #[cfg(test)]
            &RuntimeMsg::GetRuntimeConfig { request_id } => {
                Self::dispatch_runtime(event_loop, RuntimeRoute::GetRuntimeConfig { request_id })
            }
            #[cfg(test)]
            &RuntimeMsg::GetIngestState { request_id } => {
                Self::dispatch_runtime(event_loop, RuntimeRoute::GetIngestState { request_id })
            }
            #[cfg(test)]
            &RuntimeMsg::BeginIngest { request_id } => {
                Self::dispatch_runtime(event_loop, RuntimeRoute::BeginIngest { request_id })
            }
            #[cfg(test)]
            &RuntimeMsg::EndIngest { request_id } => {
                Self::dispatch_runtime(event_loop, RuntimeRoute::EndIngest { request_id })
            }
            #[cfg(test)]
            &RuntimeMsg::GetCurrentSequence { request_id } => {
                Self::dispatch_runtime(event_loop, RuntimeRoute::GetCurrentSequence { request_id })
            }
            &RuntimeMsg::WalSync { request_id } => WalCoordinator::sync(event_loop, request_id),
            #[cfg(test)]
            &RuntimeMsg::WalRotate { request_id } => WalCoordinator::rotate(event_loop, request_id),
            &RuntimeMsg::SealWalForCloud {
                request_id,
                sequence,
                wait_for_ack,
            } => WalCoordinator::seal_for_cloud(event_loop, request_id, sequence, wait_for_ack),
            #[cfg(test)]
            &RuntimeMsg::CheckCompaction { request_id } => {
                CompactionCoordinator::check(event_loop, request_id)
            }
            &RuntimeMsg::CompactAll { request_id } => {
                CompactionCoordinator::compact_all(event_loop, request_id)
            }
            &RuntimeMsg::ManifestPersist { request_id } => {
                ManifestCoordinator::persist(event_loop, request_id)
            }
            #[cfg(test)]
            &RuntimeMsg::CheckGc { request_id } => GcCoordinator::check(event_loop, request_id),
            &RuntimeMsg::SetRuntimeConfig {
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
        match msg {
            &RuntimeMsg::BeginTransaction { request_id, cf_id } => Self::dispatch_snapshot(
                event_loop,
                SnapshotRoute::BeginTransaction { request_id, cf_id },
            ),
            #[cfg(test)]
            &RuntimeMsg::CaptureReadSnapshot {
                request_id,
                cf_id,
                sequence,
            } => Self::dispatch_snapshot(
                event_loop,
                SnapshotRoute::CaptureReadSnapshot {
                    request_id,
                    cf_id,
                    sequence,
                },
            ),
            #[cfg(test)]
            RuntimeMsg::RegisterSnapshot {
                request_id,
                snapshot_id,
                sequence,
                pinned_sst_names,
            } => Self::dispatch_snapshot(
                event_loop,
                SnapshotRoute::RegisterSnapshot {
                    request_id: *request_id,
                    snapshot_id: *snapshot_id,
                    sequence: *sequence,
                    pinned_sst_names: pinned_sst_names.clone(),
                },
            ),
            #[cfg(test)]
            &RuntimeMsg::UnregisterSnapshot { snapshot_id } => Self::dispatch_snapshot(
                event_loop,
                SnapshotRoute::UnregisterSnapshot { snapshot_id },
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
                        durability_policy,
                        start_sequence,
                        conflict_policy,
                    },
                    response_tx,
                },
                msg_rx,
            ),
            #[cfg(test)]
            RuntimeMsg::WalAppend {
                request_id,
                cf_id,
                key,
                value,
                ttl_seconds,
                insert_only,
            } => Self::dispatch_wal(
                event_loop,
                WalRoute::Append(AppendRequest {
                    request_id,
                    cf_id,
                    key,
                    value,
                    ttl_seconds,
                    insert_only,
                }),
                msg_rx,
            ),
            #[cfg(test)]
            RuntimeMsg::WalAppendDeleteRange {
                request_id,
                cf_id,
                start_key,
                end_key,
                durability_policy,
            } => Self::dispatch_wal(
                event_loop,
                WalRoute::AppendDeleteRange {
                    request_id,
                    cf_id,
                    start_key,
                    end_key,
                    durability_policy,
                },
                msg_rx,
            ),
            #[cfg(test)]
            RuntimeMsg::WalSyncComplete {
                request_id,
                segment_id,
            } => Self::dispatch_wal(
                event_loop,
                WalRoute::SyncComplete {
                    request_id,
                    segment_id,
                },
                msg_rx,
            ),
            _ => unreachable!("non-WAL message routed to handle_wal_message"),
        }
    }

    fn handle_flush_message(event_loop: &mut EventLoop, msg: &RuntimeMsg) -> HandleOutcome {
        match msg {
            &RuntimeMsg::FlushMemtable { request_id, cf_id } => {
                Self::dispatch_flush(event_loop, FlushRoute::FlushMemtable { request_id, cf_id })
            }
            #[cfg(test)]
            RuntimeMsg::FlushComplete {
                request_id,
                cf_id,
                sst_name,
                sequence,
            } => Self::dispatch_flush(
                event_loop,
                FlushRoute::FlushComplete {
                    request_id: *request_id,
                    cf_id: *cf_id,
                    sst_name: sst_name.clone(),
                    sequence: *sequence,
                },
            ),
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
            #[cfg(test)]
            RuntimeMsg::RunCompaction { request_id, plan } => {
                Self::dispatch_compaction(event_loop, CompactionRoute::Run { request_id, plan })
            }
            _ => unreachable!("non-compaction message routed to handle_compaction_message"),
        }
    }

    #[cfg(test)]
    fn handle_cloud_message(event_loop: &mut EventLoop, msg: RuntimeMsg) -> HandleOutcome {
        match msg {
            #[cfg(test)]
            RuntimeMsg::CloudUploadSst {
                request_id,
                sst_name,
            } => Self::dispatch_cloud(
                event_loop,
                CloudRoute::Sst {
                    request_id,
                    sst_name,
                },
            ),
            #[cfg(test)]
            RuntimeMsg::CloudUploadWal {
                request_id,
                segment_id,
            } => Self::dispatch_cloud(
                event_loop,
                CloudRoute::Wal {
                    request_id,
                    segment_id,
                },
            ),
            #[cfg(test)]
            RuntimeMsg::CloudUploadComplete {
                request_id,
                resource,
            } => Self::dispatch_cloud(
                event_loop,
                CloudRoute::Complete {
                    request_id,
                    resource,
                },
            ),
            _ => unreachable!("non-cloud message routed to handle_cloud_message"),
        }
    }

    #[cfg(test)]
    fn handle_gc_message(event_loop: &mut EventLoop, msg: RuntimeMsg) -> HandleOutcome {
        match msg {
            #[cfg(test)]
            RuntimeMsg::DeleteObsoleteSsts {
                request_id,
                sst_names,
            } => Self::dispatch_gc(
                event_loop,
                GcRoute::DeleteObsoleteSsts {
                    request_id,
                    sst_names,
                },
            ),
            _ => unreachable!("non-GC message routed to handle_gc_message"),
        }
    }

    fn handle_manifest_message(
        event_loop: &mut EventLoop,
        msg: RuntimeMsg,
        msg_rx: &Receiver<RuntimeMsg>,
    ) -> HandleOutcome {
        match msg {
            RuntimeMsg::ManifestCreateColumnFamily { request_id, name } => Self::dispatch_manifest(
                event_loop,
                msg_rx,
                ManifestRoute::CreateColumnFamily { request_id, name },
            ),
            RuntimeMsg::ManifestDropColumnFamily { request_id, cf_id } => Self::dispatch_manifest(
                event_loop,
                msg_rx,
                ManifestRoute::DropColumnFamily { request_id, cf_id },
            ),
            #[cfg(test)]
            RuntimeMsg::ManifestAddSst {
                request_id,
                file_meta,
            } => Self::dispatch_manifest(
                event_loop,
                msg_rx,
                ManifestRoute::AddSst {
                    request_id,
                    file_meta,
                },
            ),
            #[cfg(test)]
            RuntimeMsg::ManifestCompactionComplete {
                request_id,
                removed,
                added,
            } => Self::dispatch_manifest(
                event_loop,
                msg_rx,
                ManifestRoute::CompactionComplete {
                    request_id,
                    removed,
                    added,
                },
            ),
            _ => unreachable!("non-manifest message routed to handle_manifest_message"),
        }
    }

    #[cfg(test)]
    fn handle_read_message(event_loop: &mut EventLoop, msg: RuntimeMsg) -> HandleOutcome {
        match msg {
            #[cfg(test)]
            RuntimeMsg::Read {
                request_id,
                cf_id,
                key,
                sequence,
                requested_durability,
            } => Self::dispatch_read(
                event_loop,
                ReadRoute::Read {
                    request_id,
                    cf_id,
                    key,
                    sequence,
                    requested_durability,
                },
            ),
            #[cfg(test)]
            RuntimeMsg::RangeScan {
                request_id,
                cf_id,
                start,
                end,
                sequence,
                requested_durability,
            } => Self::dispatch_read(
                event_loop,
                ReadRoute::RangeScan {
                    request_id,
                    cf_id,
                    start,
                    end,
                    sequence,
                    requested_durability,
                },
            ),
            _ => unreachable!("non-read message routed to handle_read_message"),
        }
    }

    fn dispatch_runtime(event_loop: &mut EventLoop, route: RuntimeRoute) -> HandleOutcome {
        match route {
            #[cfg(test)]
            RuntimeRoute::Noop { request_id } => event_loop.handle_noop(request_id),
            #[cfg(test)]
            RuntimeRoute::StartupPing { request_id } => event_loop.handle_startup_ping(request_id),
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
            #[cfg(test)]
            RuntimeRoute::GetRuntimeConfig { request_id } => {
                event_loop.handle_get_runtime_config(request_id);
            }
            #[cfg(test)]
            RuntimeRoute::GetIngestState { request_id } => {
                event_loop.handle_get_ingest_state(request_id);
            }
            #[cfg(test)]
            RuntimeRoute::BeginIngest { request_id } => event_loop.handle_begin_ingest(request_id),
            #[cfg(test)]
            RuntimeRoute::EndIngest { request_id } => event_loop.handle_end_ingest(request_id),
            #[cfg(test)]
            RuntimeRoute::GetCurrentSequence { request_id } => {
                event_loop.handle_get_current_sequence(request_id);
            }
        }
        HandleOutcome::Continue
    }

    fn dispatch_config(event_loop: &mut EventLoop, update: &RuntimeConfigUpdate) -> HandleOutcome {
        event_loop.handle_set_runtime_config(update)
    }

    fn dispatch_snapshot(event_loop: &mut EventLoop, route: SnapshotRoute) -> HandleOutcome {
        match route {
            #[cfg(test)]
            SnapshotRoute::CaptureReadSnapshot {
                request_id,
                cf_id,
                sequence,
            } => SnapshotCoordinator::capture(event_loop, request_id, cf_id, sequence),
            SnapshotRoute::BeginTransaction { request_id, cf_id } => {
                SnapshotCoordinator::begin_transaction(event_loop, request_id, cf_id)
            }
            #[cfg(test)]
            SnapshotRoute::RegisterSnapshot {
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
            #[cfg(test)]
            SnapshotRoute::UnregisterSnapshot { snapshot_id } => {
                SnapshotCoordinator::unregister(event_loop, snapshot_id)
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
            #[cfg(test)]
            WalRoute::Append(request) => WalCoordinator::append(event_loop, msg_rx, request),
            #[cfg(test)]
            WalRoute::AppendDeleteRange {
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
            #[cfg(test)]
            WalRoute::SyncComplete {
                request_id,
                segment_id,
            } => WalCoordinator::sync_complete(event_loop, request_id, segment_id),
        }
    }

    fn dispatch_flush(event_loop: &mut EventLoop, route: FlushRoute) -> HandleOutcome {
        match route {
            FlushRoute::FlushMemtable { request_id, cf_id } => {
                FlushCoordinator::flush_memtable(event_loop, request_id, cf_id)
            }
            #[cfg(test)]
            FlushRoute::FlushComplete {
                request_id,
                cf_id,
                sst_name,
                sequence,
            } => {
                FlushCoordinator::flush_complete(event_loop, request_id, cf_id, &sst_name, sequence)
            }
        }
    }

    fn dispatch_compaction(event_loop: &mut EventLoop, route: CompactionRoute) -> HandleOutcome {
        match route {
            #[cfg(test)]
            CompactionRoute::Run { request_id, plan } => {
                CompactionCoordinator::run(event_loop, request_id, plan)
            }
            CompactionRoute::Complete(request) => {
                CompactionCoordinator::complete(event_loop, request)
            }
        }
    }

    #[cfg(test)]
    fn dispatch_cloud(event_loop: &mut EventLoop, route: CloudRoute) -> HandleOutcome {
        match route {
            CloudRoute::Sst {
                request_id,
                sst_name,
            } => CloudCoordinator::upload_sst(event_loop, request_id, &sst_name),
            CloudRoute::Wal {
                request_id,
                segment_id,
            } => CloudCoordinator::upload_wal(event_loop, request_id, segment_id),
            CloudRoute::Complete {
                request_id,
                resource,
            } => CloudCoordinator::upload_complete(event_loop, request_id, &resource),
        }
    }

    #[cfg(test)]
    fn dispatch_gc(event_loop: &mut EventLoop, route: GcRoute) -> HandleOutcome {
        match route {
            GcRoute::DeleteObsoleteSsts {
                request_id,
                sst_names,
            } => GcCoordinator::delete_obsolete_ssts(event_loop, request_id, &sst_names),
        }
    }

    fn dispatch_manifest(
        event_loop: &mut EventLoop,
        msg_rx: &Receiver<RuntimeMsg>,
        route: ManifestRoute,
    ) -> HandleOutcome {
        match route {
            #[cfg(test)]
            ManifestRoute::AddSst {
                request_id,
                file_meta,
            } => ManifestCoordinator::add_sst(event_loop, request_id, file_meta),
            #[cfg(test)]
            ManifestRoute::CompactionComplete {
                request_id,
                removed,
                added,
            } => ManifestCoordinator::compaction_complete(event_loop, request_id, &removed, &added),
            ManifestRoute::CreateColumnFamily { request_id, name } => {
                ManifestCoordinator::create_column_family(event_loop, msg_rx, request_id, &name)
            }
            ManifestRoute::DropColumnFamily { request_id, cf_id } => {
                ManifestCoordinator::drop_column_family(event_loop, msg_rx, request_id, cf_id)
            }
        }
    }

    #[cfg(test)]
    fn dispatch_read(event_loop: &mut EventLoop, route: ReadRoute) -> HandleOutcome {
        match route {
            ReadRoute::Read {
                request_id,
                cf_id,
                key,
                sequence,
                requested_durability,
            } => {
                event_loop.handle_msg_read(request_id, cf_id, key, sequence, requested_durability);
                HandleOutcome::Continue
            }
            ReadRoute::RangeScan {
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
                HandleOutcome::Continue
            }
        }
    }
}
