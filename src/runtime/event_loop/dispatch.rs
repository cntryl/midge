use super::{
    cloud::CloudCoordinator, compaction::CompactionCoordinator, flush::FlushCoordinator,
    gc::GcCoordinator, manifest::ManifestCoordinator, snapshot::SnapshotCoordinator,
    wal::WalCoordinator, EventLoop, HandleOutcome,
};
use crate::runtime::RuntimeMsg;
use crossbeam::channel::Receiver;

pub(super) struct RuntimeDispatcher;

impl RuntimeDispatcher {
    pub(super) fn handle(
        event_loop: &mut EventLoop,
        msg: RuntimeMsg,
        msg_rx: &Receiver<RuntimeMsg>,
    ) -> HandleOutcome {
        match msg {
            RuntimeMsg::Shutdown => event_loop.handle_shutdown(),

            RuntimeMsg::Noop { request_id } => {
                event_loop.handle_noop(request_id);
                HandleOutcome::Continue
            }
            RuntimeMsg::StartupPing { request_id } => {
                event_loop.handle_startup_ping(request_id);
                HandleOutcome::Continue
            }
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
            RuntimeMsg::SetRuntimeConfig {
                request_id,
                memtable_size_limit,
                memtable_flush_threshold,
                enable_compaction,
                l0_compaction_trigger,
                wal_durability_policy,
                wal_batch_config,
            } => event_loop.handle_set_runtime_config(
                request_id,
                memtable_size_limit,
                memtable_flush_threshold,
                enable_compaction,
                l0_compaction_trigger,
                wal_durability_policy,
                wal_batch_config,
            ),
            RuntimeMsg::GetRuntimeConfig { request_id } => {
                event_loop.handle_get_runtime_config(request_id);
                HandleOutcome::Continue
            }
            RuntimeMsg::GetIngestState { request_id } => {
                event_loop.handle_get_ingest_state(request_id);
                HandleOutcome::Continue
            }
            RuntimeMsg::BeginIngest { request_id } => {
                event_loop.handle_begin_ingest(request_id);
                HandleOutcome::Continue
            }
            RuntimeMsg::EndIngest { request_id } => {
                event_loop.handle_end_ingest(request_id);
                HandleOutcome::Continue
            }
            RuntimeMsg::GetCurrentSequence { request_id } => {
                event_loop.handle_get_current_sequence(request_id);
                HandleOutcome::Continue
            }

            RuntimeMsg::CaptureReadSnapshot {
                request_id,
                cf_id,
                sequence,
            } => SnapshotCoordinator::capture(event_loop, request_id, cf_id, sequence),
            RuntimeMsg::BeginTransaction { request_id, cf_id } => {
                SnapshotCoordinator::begin_transaction(event_loop, request_id, cf_id)
            }
            RuntimeMsg::RegisterSnapshot {
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
            RuntimeMsg::UnregisterSnapshot { snapshot_id } => {
                SnapshotCoordinator::unregister(event_loop, snapshot_id)
            }

            RuntimeMsg::ApplyTransaction {
                request_id,
                ops,
                durability_policy,
                start_sequence,
                isolation_policy,
            } => WalCoordinator::apply_transaction(
                event_loop,
                msg_rx,
                request_id,
                ops,
                durability_policy,
                start_sequence,
                isolation_policy,
            ),
            RuntimeMsg::WalAppend {
                request_id,
                cf_id,
                key,
                value,
                ttl_seconds,
                insert_only,
            } => WalCoordinator::append(
                event_loop,
                msg_rx,
                request_id,
                cf_id,
                key,
                value,
                ttl_seconds,
                insert_only,
            ),
            RuntimeMsg::WalAppendDeleteRange {
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
            RuntimeMsg::WalSync { request_id } => WalCoordinator::sync(event_loop, request_id),
            RuntimeMsg::WalRotate { request_id } => WalCoordinator::rotate(event_loop, request_id),
            RuntimeMsg::SealWalForCloud {
                request_id,
                sequence,
                wait_for_ack,
            } => WalCoordinator::seal_for_cloud(event_loop, request_id, sequence, wait_for_ack),
            RuntimeMsg::WalSyncComplete {
                request_id,
                segment_id,
            } => WalCoordinator::sync_complete(event_loop, request_id, segment_id),

            RuntimeMsg::FlushMemtable { request_id, cf_id } => {
                FlushCoordinator::flush_memtable(event_loop, request_id, cf_id)
            }
            RuntimeMsg::FlushComplete {
                request_id,
                cf_id,
                sst_name,
                sequence,
            } => {
                FlushCoordinator::flush_complete(event_loop, request_id, cf_id, sst_name, sequence)
            }

            RuntimeMsg::CheckCompaction { request_id } => {
                CompactionCoordinator::check(event_loop, request_id)
            }
            RuntimeMsg::RunCompaction { request_id, plan } => {
                CompactionCoordinator::run(event_loop, request_id, plan)
            }
            RuntimeMsg::CompactAll { request_id } => {
                CompactionCoordinator::compact_all(event_loop, request_id)
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
                request_id,
                input_ssts,
                output_ssts,
                cf_id,
                target_level,
                succeeded,
            ),

            RuntimeMsg::CloudUploadSst {
                request_id,
                sst_name,
            } => CloudCoordinator::upload_sst(event_loop, request_id, sst_name),
            RuntimeMsg::CloudUploadWal {
                request_id,
                segment_id,
            } => CloudCoordinator::upload_wal(event_loop, request_id, segment_id),
            RuntimeMsg::CloudUploadComplete {
                request_id,
                resource,
            } => CloudCoordinator::upload_complete(event_loop, request_id, resource),

            RuntimeMsg::CheckGc { request_id } => GcCoordinator::check(event_loop, request_id),
            RuntimeMsg::DeleteObsoleteSsts {
                request_id,
                sst_names,
            } => GcCoordinator::delete_obsolete_ssts(event_loop, request_id, sst_names),

            RuntimeMsg::ManifestAddSst {
                request_id,
                file_meta,
            } => ManifestCoordinator::add_sst(event_loop, request_id, file_meta),
            RuntimeMsg::ManifestCompactionComplete {
                request_id,
                removed,
                added,
            } => ManifestCoordinator::compaction_complete(event_loop, request_id, removed, added),
            RuntimeMsg::ManifestPersist { request_id } => {
                ManifestCoordinator::persist(event_loop, request_id)
            }
            RuntimeMsg::ManifestCreateColumnFamily { request_id, name } => {
                ManifestCoordinator::create_column_family(event_loop, msg_rx, request_id, name)
            }
            RuntimeMsg::ManifestDropColumnFamily { request_id, cf_id } => {
                ManifestCoordinator::drop_column_family(event_loop, msg_rx, request_id, cf_id)
            }

            RuntimeMsg::Read {
                request_id,
                cf_id,
                key,
                sequence,
                requested_durability,
            } => {
                event_loop.handle_msg_read(request_id, cf_id, key, sequence, requested_durability);
                HandleOutcome::Continue
            }
            RuntimeMsg::RangeScan {
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
