//! Event loop — central message dispatcher
//!
//! Receives messages from RuntimeHandle and routes them to the correct actor.
//!
//! Copilot note:
//! - Per-request routing is done exclusively via `router.complete()`.
//! - EventLoop never touches pending_responses directly.
//! - All read paths are local (memtables → SST later).
//! - All actor responses flow through `respond()`.
//!
//! # Module structure
//!
//! The event loop is split across domain-specific files:
//!
//! - `read_path` — point reads, range scans, durability-aware message handlers
//! - `durability_sync` — WAL sync, group commit, durability waiter completion
//! - `cloud_integration` — CloudFirst WAL flush, cloud ack/fail handling
//! - `write_batch` — group commit write draining, backpressure / write stall

mod cloud_integration;
mod durability_sync;
mod read_path;
mod write_batch;

use crossbeam::channel::Receiver;
use crossbeam::channel::RecvTimeoutError;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use super::actors::{
    CloudActor, CompactionActor, EvictionActor, FlushActor, GcActor, ManifestActor, WalActor,
};
use super::durability::{DurabilityCoordinator, DurabilityWaiter};
use super::read_snapshot::ReadSnapshot;
use super::state::RuntimeState;
use super::{ResponseRouter, RuntimeMsg, RuntimeResponse};

/// Main synchronous event loop for the runtime.
///
/// Owns all actors and is responsible for routing inbound messages.
pub struct EventLoop {
    pub(super) state: RuntimeState,

    // Actors
    pub(super) flush_actor: FlushActor,
    pub(super) compaction_actor: CompactionActor,
    pub(super) wal_actor: WalActor,
    pub(super) cloud_actor: CloudActor,
    gc_actor: GcActor,
    manifest_actor: ManifestActor,
    pub(super) eviction_actor: Option<EvictionActor>,

    pub(super) hybrid_storage: Option<Arc<crate::storage::HybridStorage>>,
    pub(super) hybrid_storage_events:
        Option<crossbeam::channel::Receiver<crate::storage::StorageEvent>>,
    pub(super) trace_enabled: bool,

    // Durability coordination (extracted to reduce EventLoop cognitive load)
    pub(super) durability: DurabilityCoordinator,

    /// Per-request router (oneshot channels)
    pub(super) router: Arc<ResponseRouter>,

    /// One buffered message we pulled from the channel while draining writes.
    ///
    /// This preserves FIFO semantics when we opportunistically `try_recv()` to batch writes:
    /// if we encounter a non-write message, we stash it here and handle it next.
    pub(super) pending_msg: Option<RuntimeMsg>,

    /// Sender that worker threads can use to post back completion messages
    /// (compaction threads will use this to report completion).
    pub(super) worker_msg_tx: Option<crossbeam::channel::Sender<RuntimeMsg>>,

    /// Waiters blocked on write stall clearing (request_id -> cf_id).
    pub(super) write_stall_waiters: HashMap<u64, crate::engine::ColumnFamilyId>,
    /// FIFO queues of waiters per CF.
    pub(super) write_stall_waiter_queues: HashMap<crate::engine::ColumnFamilyId, VecDeque<u64>>,
}

impl EventLoop {
    pub(crate) fn new(
        state: RuntimeState,
        trace_enabled: bool,
        router: Arc<ResponseRouter>,
        config: super::RuntimeConfig,
        worker_msg_tx: Option<crossbeam::channel::Sender<super::RuntimeMsg>>,
    ) -> crate::common::MidgeResult<Self> {
        let wal_dir = state.wal_dir.clone();
        let sst_dir = state.sst_dir.clone();
        let memory_mode = state.memory_mode;
        let initial_segment_id = state.wal.current_segment_id;

        let sst_factory = if memory_mode {
            // Use in-memory MockFs for SST factory in memory mode
            let fs = Arc::new(crate::io::MockFs::new());
            Arc::new(
                crate::sst::FsSstFactoryIo::new(fs, 64 * 1024)
                    .with_compression_policy(config.compression_policy.clone()),
            )
        } else {
            let fs = Arc::new(crate::io::RealFs::new(&sst_dir)?);
            Arc::new(
                crate::sst::FsSstFactoryIo::new(fs, 64 * 1024)
                    .with_compression_policy(config.compression_policy.clone()),
            )
        };

        // Create actors - they handle memory_mode internally
        let flush_actor =
            FlushActor::new(&sst_dir, memory_mode, config.compression_policy.clone())?;
        let wal_actor = WalActor::new(
            wal_dir,
            config.wal_durability_policy,
            config.wal_batch_config,
            memory_mode,
        )?;

        // 🔑 CRITICAL: Use the correct key for durability_waiters based on mode
        // - CloudFirst: key is segment_id (for rotate_to/complete calls)
        // - Batched: key is flush_generation (returned from wal_actor.sync())
        let is_cloud_first = wal_actor.is_cloud_first();
        let initial_durability_key = if is_cloud_first {
            initial_segment_id
        } else {
            wal_actor.current_flush_generation()
        };

        let mut event_loop = Self {
            state,
            flush_actor,
            compaction_actor: CompactionActor::new(sst_factory),
            wal_actor,
            cloud_actor: CloudActor::new(),
            gc_actor: GcActor::new(),
            manifest_actor: ManifestActor::new(),
            eviction_actor: None,
            hybrid_storage: None,
            hybrid_storage_events: config.hybrid_storage_events.clone(),
            trace_enabled,
            durability: DurabilityCoordinator::new(initial_durability_key, is_cloud_first),
            router,
            pending_msg: None,
            worker_msg_tx,

            write_stall_waiters: HashMap::new(),
            write_stall_waiter_queues: HashMap::new(),
        };

        if let Some(storage) = config.hybrid_storage {
            event_loop.set_hybrid_storage(storage);
        }

        Ok(event_loop)
    }

    pub fn set_hybrid_storage(&mut self, storage: Arc<crate::storage::HybridStorage>) {
        self.eviction_actor = Some(EvictionActor::new(storage.clone()));
        self.hybrid_storage = Some(storage);
    }

    /// Helper: deliver a RuntimeResponse to the requester via the router.
    #[inline]
    pub(super) fn respond(&self, request_id: u64, resp: RuntimeResponse) {
        self.router.complete(resp);

        // Optional trace
        if self.trace_enabled {
            tracing::trace!(request_id, "response routed");
        }
    }

    /// Main event loop — runs until Shutdown message or channel close.
    pub fn run(&mut self, msg_rx: Receiver<RuntimeMsg>) {
        loop {
            // CloudFirst needs fast ticking while callers are blocked waiting for CloudAck.
            // A fixed 5ms recv_timeout creates a hard floor of ~5ms per write.
            let cloudfirst_draining =
                self.wal_actor.is_cloud_first() && self.wal_actor.has_pending_cloud_writes();
            let timeout = if cloudfirst_draining {
                // Drain quickly, but avoid a 0-timeout busy-spin.
                Duration::from_millis(1)
            } else {
                Duration::from_millis(5)
            };

            // Prefer reacting to storage events immediately (no polling floor).
            //
            // If we previously pulled a non-write message while draining writes, handle it first to
            // preserve FIFO semantics.
            let msg = if let Some(pending) = self.pending_msg.take() {
                Some(pending)
            } else {
                let storage_rx_opt = self.hybrid_storage_events.clone();
                if let Some(storage_rx) = storage_rx_opt {
                    crossbeam::channel::select! {
                        recv(msg_rx) -> msg => msg.ok(),
                        recv(storage_rx) -> ev => {
                            if let Ok(ev) = ev {
                                self.handle_storage_event(ev);
                            }
                            // Continue the loop; we didn't consume a RuntimeMsg.
                            continue;
                        },
                        default(timeout) => {
                            // 🔑 CRITICAL: Drive durability progress on idle ticks
                            // If waiters exist but no new messages, sync them now
                            self.sync_batched_wal_if_needed(&msg_rx);

                            self.maybe_flush_cloudfirst_wal();
                            self.tick_hybrid_storage();
                            // Drain any push-channel events that arrived between ticks.
                            self.drain_hybrid_storage_events();
                            continue;
                        }
                    }
                } else {
                    match msg_rx.recv_timeout(timeout) {
                        Ok(msg) => Some(msg),
                        Err(RecvTimeoutError::Timeout) => {
                            // 🔑 CRITICAL: Drive durability progress on idle ticks
                            // If waiters exist but no new messages, sync them now
                            self.sync_batched_wal_if_needed(&msg_rx);

                            self.maybe_flush_cloudfirst_wal();
                            self.tick_hybrid_storage();
                            continue;
                        }
                        Err(RecvTimeoutError::Disconnected) => None,
                    }
                }
            };

            let Some(msg) = msg else {
                break;
            };

            self.maybe_flush_cloudfirst_wal();
            self.tick_hybrid_storage();
            self.drain_hybrid_storage_events();

            match msg {
                RuntimeMsg::Shutdown => {
                    tracing::info!("Runtime shutting down");

                    // CRITICAL: Flush and wait for pending CloudFirst segments before shutdown.
                    // This ensures all writes are cloud-durable for recovery.
                    if self.wal_actor.is_cloud_first() && self.state.wal.pending_writes > 0 {
                        if let Err(e) = self.wal_actor.flush_for_cloud_upload(&mut self.state) {
                            tracing::error!(error = %e, "Failed to flush CloudFirst segments during shutdown");
                        } else if let Err(e) = self.wal_actor.rotate(&mut self.state) {
                            tracing::error!(error = %e, "Failed to rotate WAL during shutdown");
                        } else if let Some(storage) = &self.hybrid_storage {
                            let segment_id = self.state.wal.current_segment_id.saturating_sub(1);
                            let max_sequence = self.state.wal.local_durable_seq;
                            let local_path = self.state.wal_dir.join(format!("{segment_id}.wal"));
                            storage.enqueue_wal_segment(segment_id, local_path, max_sequence);
                            tracing::info!(
                                segment_id,
                                "Enqueued final CloudFirst segment on shutdown"
                            );
                        }
                    }

                    // Wait for pending uploads to complete (with timeout)
                    if self.wal_actor.is_cloud_first() {
                        if let Some(storage) = &self.hybrid_storage {
                            let storage_arc = storage.clone();
                            let shutdown_start = std::time::Instant::now();
                            let shutdown_timeout = std::time::Duration::from_secs(30);
                            while storage_arc.pending_upload_count() > 0
                                && shutdown_start.elapsed() < shutdown_timeout
                            {
                                // Process uploads
                                self.tick_hybrid_storage();
                                self.drain_hybrid_storage_events();

                                // Small sleep to avoid busy-waiting
                                std::thread::sleep(std::time::Duration::from_millis(10));
                            }

                            if storage_arc.pending_upload_count() > 0 {
                                tracing::warn!(
                                    pending = storage_arc.pending_upload_count(),
                                    "Shutdown timeout: {} pending CloudFirst uploads not completed",
                                    storage_arc.pending_upload_count()
                                );
                            } else {
                                tracing::info!("All CloudFirst uploads completed on shutdown");
                            }
                        }
                    }

                    break;
                }

                RuntimeMsg::Noop { request_id } => {
                    self.respond(request_id, RuntimeResponse::Ok { request_id });
                }

                RuntimeMsg::StartupPing { request_id } => {
                    self.respond(request_id, RuntimeResponse::Ok { request_id });
                }

                RuntimeMsg::CheckWriteStall { request_id, cf_id } => {
                    let is_stalled = self.state.should_stall_writes(cf_id);
                    self.respond(
                        request_id,
                        RuntimeResponse::WriteStallStatus {
                            request_id,
                            is_stalled,
                        },
                    );
                }

                RuntimeMsg::WaitForWriteStallClear { request_id, cf_id } => {
                    if !self.state.should_stall_writes(cf_id) {
                        self.respond(request_id, RuntimeResponse::Ok { request_id });
                    } else {
                        self.write_stall_waiters.insert(request_id, cf_id);
                        self.write_stall_waiter_queues
                            .entry(cf_id)
                            .or_default()
                            .push_back(request_id);
                    }
                }

                RuntimeMsg::CancelWaitForWriteStallClear { wait_request_id } => {
                    let _ = self.write_stall_waiters.remove(&wait_request_id);
                }

                RuntimeMsg::GetReadAmpMetrics { request_id } => {
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

                RuntimeMsg::SetRuntimeConfig {
                    request_id,
                    memtable_size_limit,
                    memtable_flush_threshold,
                    enable_compaction,
                    wal_durability_policy,
                    wal_batch_config,
                } => {
                    if let Some(ms) = memtable_size_limit {
                        self.state.memtable_size_limit = ms;
                    }
                    if let Some(th) = memtable_flush_threshold {
                        self.state.memtable_flush_threshold = th;
                    }
                    if let Some(ec) = enable_compaction {
                        self.state.enable_compaction = ec;
                    }

                    // Apply WAL changes to the wal actor if requested
                    if wal_durability_policy.is_some() || wal_batch_config.is_some() {
                        let policy =
                            wal_durability_policy.unwrap_or(self.wal_actor.durability_policy());
                        let batch_cfg = wal_batch_config.unwrap_or(self.wal_actor.batch_config());
                        if let Err(e) = self.wal_actor.set_durability(policy, batch_cfg) {
                            self.respond(
                                request_id,
                                RuntimeResponse::Error {
                                    request_id,
                                    error: crate::common::MidgeError::Internal(e.to_string()),
                                },
                            );
                            continue;
                        }
                    }

                    self.respond(request_id, RuntimeResponse::Ok { request_id });
                }

                RuntimeMsg::GetRuntimeConfig { request_id } => {
                    self.respond(
                        request_id,
                        RuntimeResponse::RuntimeConfigSnapshot {
                            request_id,
                            memtable_size_limit: self.state.memtable_size_limit,
                            memtable_flush_threshold: self.state.memtable_flush_threshold,
                            enable_compaction: self.state.enable_compaction,
                            wal_durability_policy: self.wal_actor.durability_policy(),
                            wal_batch_config: self.wal_actor.batch_config(),
                        },
                    );
                }

                RuntimeMsg::GetIngestState { request_id } => {
                    let active = self
                        .state
                        .ingest_active
                        .load(std::sync::atomic::Ordering::SeqCst);
                    self.respond(
                        request_id,
                        RuntimeResponse::IngestState {
                            request_id,
                            ingest_active: active,
                        },
                    );
                }

                RuntimeMsg::BeginIngest { request_id } => {
                    // ─────────────────────────────────────────────────────────────────────
                    // INGEST BARRIER — scope: active_compactions ONLY
                    //
                    // We wait ONLY for active compaction jobs to drain. We do NOT wait for:
                    //   - WAL sync (batched writes flush on their own schedule)
                    //   - memtable flush (handled separately on EndIngest)
                    //   - stats, maintenance, or background monitoring
                    //
                    // The invariant: once begin_ingest returns, no compaction work is
                    // running or can be scheduled until end_ingest is called.
                    // ─────────────────────────────────────────────────────────────────────

                    let active = self
                        .state
                        .active_compactions
                        .load(std::sync::atomic::Ordering::SeqCst);
                    let current_epoch = self
                        .state
                        .ingest_epoch
                        .load(std::sync::atomic::Ordering::SeqCst);

                    tracing::info!(
                        component = "ingest",
                        invariant = "begin_ingest_barrier",
                        ingest_epoch = current_epoch,
                        "ingest: begin_ingest requested"
                    );
                    tracing::info!(
                        component = "ingest",
                        invariant = "begin_ingest_barrier",
                        active_compactions = active,
                        ingest_epoch = current_epoch,
                        "ingest: active_compactions={} (will wait for drain)",
                        active
                    );

                    // Prevent new compactions and mark ingest active
                    self.state.enable_compaction = false;
                    self.state
                        .ingest_active
                        .store(true, std::sync::atomic::Ordering::SeqCst);

                    // Bump epoch so in-flight compactions will see the change and abort cooperatively
                    let new_epoch = self
                        .state
                        .ingest_epoch
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                        + 1;

                    // ─────────────────────────────────────────────────────────────────────
                    // Wait for active compactions to drain (ONLY compactions).
                    // This is a blocking wait with timed logging thresholds.
                    // ─────────────────────────────────────────────────────────────────────
                    let (lock, cvar) = &*self.state.active_compactions_notify;
                    let mut guard = lock.lock();

                    let wait_start = std::time::Instant::now();
                    let mut warned_500ms = false;
                    let mut warned_5s = false;

                    while self
                        .state
                        .active_compactions
                        .load(std::sync::atomic::Ordering::SeqCst)
                        > 0
                    {
                        // Wait with timeout so we can log progress thresholds
                        let timeout = std::time::Duration::from_millis(100);
                        // parking_lot::Condvar::wait_for will block up to `timeout` and return
                        // a boolean that indicates whether the wait ended because of notify.
                        let _notified = cvar.wait_for(&mut guard, timeout);

                        let elapsed = wait_start.elapsed();

                        if !warned_500ms && elapsed > std::time::Duration::from_millis(500) {
                            warned_500ms = true;
                            let a = self
                                .state
                                .active_compactions
                                .load(std::sync::atomic::Ordering::SeqCst);
                            tracing::warn!(
                                component = "ingest",
                                invariant = "begin_ingest_barrier",
                                active_compactions = a,
                                ingest_epoch = new_epoch,
                                elapsed_ms = elapsed.as_millis(),
                                "ingest: still waiting after 500ms for compactions to drain (active_compactions={})", a
                            );
                        }

                        if !warned_5s && elapsed > std::time::Duration::from_secs(5) {
                            warned_5s = true;
                            let a = self
                                .state
                                .active_compactions
                                .load(std::sync::atomic::Ordering::SeqCst);
                            tracing::error!(
                                component = "ingest",
                                invariant = "begin_ingest_barrier",
                                active_compactions = a,
                                ingest_epoch = new_epoch,
                                elapsed_ms = elapsed.as_millis(),
                                "ingest: begin_ingest blocked >5s — likely misuse: entering ingest during active workload (active_compactions={}). \
                                 Correct ordering: warmup/probe BEFORE begin_ingest, not during.", a
                            );
                        }
                    }

                    tracing::info!(
                        component = "ingest",
                        invariant = "begin_ingest_barrier",
                        ingest_epoch = new_epoch,
                        "ingest: ingestion barrier enabled — all compactions drained"
                    );

                    self.respond(request_id, RuntimeResponse::Ok { request_id });
                }

                RuntimeMsg::EndIngest { request_id } => {
                    tracing::info!("EndIngest: flushing memtables and restoring scheduling");

                    // Trigger flush for each column family to ensure memtables are persisted
                    let cf_ids: Vec<u32> = self.state.column_families.keys().cloned().collect();
                    for cf_id in cf_ids {
                        // Flush and complete bookkeeping
                        if let Ok(sst_name) = self.flush_actor.handle_flush(
                            &mut self.state,
                            cf_id,
                            self.hybrid_storage.as_ref(),
                        ) {
                            let sequence = self.state.sequence;
                            self.flush_actor.handle_flush_complete(
                                &mut self.state,
                                cf_id,
                                &sst_name,
                                sequence,
                            );
                        }
                    }
                    self.wake_write_stall_waiters();

                    // Bump epoch again to invalidate any in-flight compactions that missed the earlier epoch
                    let new_epoch = self
                        .state
                        .ingest_epoch
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                        + 1;
                    tracing::info!(
                        ingest_epoch = new_epoch,
                        "EndIngest: ingestion barrier lifted"
                    );

                    // Clear ingest active before resuming compaction scheduling
                    self.state
                        .ingest_active
                        .store(false, std::sync::atomic::Ordering::SeqCst);

                    // Optionally kick compaction checks once so background compaction resumes
                    if let Some(plan) = self.compaction_actor.check_compaction(&self.state) {
                        let _ = self.compaction_actor.run_compaction(
                            &mut self.state,
                            plan,
                            self.hybrid_storage.as_ref(),
                            self.worker_msg_tx.clone(),
                        );
                    }

                    self.respond(request_id, RuntimeResponse::Ok { request_id });
                }

                RuntimeMsg::GetCurrentSequence { request_id } => {
                    self.respond(
                        request_id,
                        RuntimeResponse::CurrentSequence {
                            request_id,
                            sequence: self.state.sequence,
                        },
                    );
                }

                RuntimeMsg::CaptureReadSnapshot {
                    request_id,
                    cf_id,
                    sequence: _,
                } => {
                    if let Some(cf_state) = self.state.column_families.get(&cf_id) {
                        // Collect SST files for this CF
                        let cf_files: Vec<_> = self
                            .state
                            .manifest
                            .files
                            .iter()
                            .filter(|f| f.cf_id == cf_id)
                            .cloned()
                            .collect();

                        let snapshot = Arc::new(ReadSnapshot::new(
                            cf_state.memtable.clone(),
                            cf_state.immutable_memtables.clone(),
                            cf_files,
                            self.state.sst_dir.clone(),
                        ));
                        self.respond(
                            request_id,
                            RuntimeResponse::ReadSnapshot {
                                request_id,
                                snapshot,
                            },
                        );
                    } else {
                        self.respond(
                            request_id,
                            RuntimeResponse::Error {
                                request_id,
                                error: crate::common::MidgeError::Internal(format!(
                                    "Column family {} not found",
                                    cf_id
                                )),
                            },
                        );
                    }
                }

                RuntimeMsg::ApplyTransaction { request_id, ops } => {
                    if self.wal_actor.is_cloud_first() && self.hybrid_storage.is_none() {
                        self.respond(
                            request_id,
                            RuntimeResponse::Error {
                                request_id,
                                error: crate::common::MidgeError::Internal(
                                    "CloudFirst requires HybridStorage".to_string(),
                                ),
                            },
                        );
                    } else {
                        match self
                            .wal_actor
                            .append_transaction(&mut self.state, request_id, ops)
                        {
                            Ok((last_sequence, op_count, deferred)) => {
                                if self.should_ack_immediately(deferred) {
                                    if self.wal_actor.is_cloud_first() {
                                        // Accepted but not yet cloud-durable; confirm later on CloudAck.
                                        self.durability.queue_waiter(
                                            DurabilityWaiter::ConfirmTransactionApply {
                                                request_id,
                                            },
                                        );
                                    } else if deferred {
                                        self.maybe_queue_confirm_only_waiter(
                                            deferred, request_id, true,
                                        );
                                    } else {
                                        self.state.clear_pending_transaction_barrier();
                                        self.state.confirm_sequences(request_id);
                                    }

                                    self.respond(
                                        request_id,
                                        RuntimeResponse::TransactionApplied {
                                            request_id,
                                            last_sequence,
                                            op_count,
                                            write_stall_hint: self.state.should_stall_writes(0),
                                        },
                                    );
                                } else {
                                    self.durability.queue_waiter(
                                        DurabilityWaiter::TransactionApply {
                                            request_id,
                                            last_sequence,
                                            op_count,
                                        },
                                    );
                                }
                            }
                            Err(e) => {
                                self.respond(
                                    request_id,
                                    RuntimeResponse::Error {
                                        request_id,
                                        error: crate::common::MidgeError::Internal(e.to_string()),
                                    },
                                );
                            }
                        }
                    }
                    // Auto-sync batched writes if needed (group commit completes all waiters).
                    // Do this for local durability mode; CloudFirst uses rotate/upload logic.
                    if !self.wal_actor.is_cloud_first() {
                        const MAX_DRAIN_WRITES_AFTER_BATCH: usize = 1024;
                        let _ = self.drain_pending_writes(&msg_rx, MAX_DRAIN_WRITES_AFTER_BATCH);
                        self.sync_batched_wal_if_needed(&msg_rx);
                    }

                    // Check if any memtable needs flushing (backpressure mechanism)
                    // Flushing is critical to accumulating immutable memtables and triggering backpressure
                    if let Some(cf_id_to_flush) = self.state.needs_flush() {
                        match self.flush_actor.handle_flush(
                            &mut self.state,
                            cf_id_to_flush,
                            self.hybrid_storage.as_ref(),
                        ) {
                            Ok(sst_name) => {
                                // Complete the flush by updating bookkeeping
                                let sequence = self.state.sequence;
                                self.flush_actor.handle_flush_complete(
                                    &mut self.state,
                                    cf_id_to_flush,
                                    &sst_name,
                                    sequence,
                                );
                                self.wake_write_stall_waiters();
                                tracing::debug!(cf_id = cf_id_to_flush, sst_name = %sst_name, "Auto-flushed memtable to enable backpressure");
                            }
                            Err(e) => {
                                tracing::trace!(cf_id = cf_id_to_flush, error = %e, "Flush failed (backpressure or internal error)");
                            }
                        }
                    }

                    self.maybe_flush_cloudfirst_wal();
                }

                // =============================================================
                // Flush
                // =============================================================
                RuntimeMsg::FlushMemtable { request_id, cf_id } => {
                    let resp = self
                        .flush_actor
                        .handle_flush(&mut self.state, cf_id, self.hybrid_storage.as_ref())
                        .map(|sst_name| RuntimeResponse::FlushComplete {
                            request_id,
                            sst_name,
                        })
                        .unwrap_or_else(|e| RuntimeResponse::Error {
                            request_id,
                            error: crate::common::MidgeError::Internal(e.to_string()),
                        });

                    self.respond(request_id, resp);
                }

                RuntimeMsg::FlushComplete {
                    request_id,
                    cf_id,
                    sst_name,
                    sequence,
                } => {
                    self.flush_actor.handle_flush_complete(
                        &mut self.state,
                        cf_id,
                        &sst_name,
                        sequence,
                    );
                    self.wake_write_stall_waiters();
                    self.respond(request_id, RuntimeResponse::Ok { request_id });
                }

                // =============================================================
                // Compaction
                // =============================================================
                RuntimeMsg::CheckCompaction { request_id } => {
                    // ─────────────────────────────────────────────────────────────────────
                    // HARD INVARIANT: No compaction scheduling while ingest is active.
                    // This is a programmer error — the caller should not have reached here.
                    // ─────────────────────────────────────────────────────────────────────
                    if self
                        .state
                        .ingest_active
                        .load(std::sync::atomic::Ordering::SeqCst)
                    {
                        let epoch = self
                            .state
                            .ingest_epoch
                            .load(std::sync::atomic::Ordering::SeqCst);
                        tracing::error!(
                            component = "compaction",
                            invariant = "no_compaction_during_ingest",
                            ingest_epoch = epoch,
                            "BUG: CheckCompaction called while ingest mode is active. \
                             Violated invariant: compaction must not be scheduled during ingest. \
                             Correct ordering: complete all compactions BEFORE begin_ingest."
                        );
                        // Return error (panic would kill the runtime; use error response for recoverability)
                        self.respond(
                            request_id,
                            RuntimeResponse::Error {
                                request_id,
                                error: crate::common::MidgeError::Internal("BUG: compaction scheduling attempted during ingest mode — violated invariant".to_string()),
                            },
                        );
                        continue;
                    }

                    if let Some(plan) = self.compaction_actor.check_compaction(&self.state) {
                        // Schedule compaction to run in background and respond immediately.
                        // The compaction worker will send a `CompactionComplete` message back
                        // when finished which will be handled below.
                        let schedule_res = self.compaction_actor.run_compaction(
                            &mut self.state,
                            plan,
                            self.hybrid_storage.as_ref(),
                            self.worker_msg_tx.clone(),
                        );

                        // Return immediate response (ack)
                        match schedule_res {
                            Ok(_) => self.respond(request_id, RuntimeResponse::Ok { request_id }),
                            Err(e) => self.respond(
                                request_id,
                                RuntimeResponse::Error {
                                    request_id,
                                    error: crate::common::MidgeError::Internal(e.to_string()),
                                },
                            ),
                        }
                    } else {
                        self.respond(request_id, RuntimeResponse::Ok { request_id });
                    }
                }

                RuntimeMsg::RunCompaction { request_id, plan } => {
                    // ─────────────────────────────────────────────────────────────────────
                    // HARD INVARIANT: No compaction execution while ingest is active.
                    // ─────────────────────────────────────────────────────────────────────
                    if self
                        .state
                        .ingest_active
                        .load(std::sync::atomic::Ordering::SeqCst)
                    {
                        let epoch = self
                            .state
                            .ingest_epoch
                            .load(std::sync::atomic::Ordering::SeqCst);
                        tracing::error!(
                            component = "compaction",
                            invariant = "no_compaction_during_ingest",
                            ingest_epoch = epoch,
                            input_files = ?plan.input_files,
                            "BUG: RunCompaction called while ingest mode is active. \
                             Violated invariant: compaction must not run during ingest. \
                             Correct ordering: complete all compactions BEFORE begin_ingest."
                        );
                        self.respond(
                            request_id,
                            RuntimeResponse::Error {
                                request_id,
                                error: crate::common::MidgeError::Internal("BUG: compaction execution attempted during ingest mode — violated invariant".to_string()),
                            },
                        );
                        continue;
                    }

                    let cplan = crate::compaction::CompactionPlan {
                        input_files: plan.input_files,
                        output_files: Vec::new(),
                        source_level: plan.source_level,
                        target_level: plan.target_level,
                        cf_id: plan.cf_id,
                        output_seq: self.state.next_sequence(),
                    };

                    let schedule_res = self.compaction_actor.run_compaction(
                        &mut self.state,
                        cplan,
                        self.hybrid_storage.as_ref(),
                        self.worker_msg_tx.clone(),
                    );
                    let resp = match schedule_res {
                        Ok(_) => RuntimeResponse::Ok { request_id },
                        Err(e) => RuntimeResponse::Error {
                            request_id,
                            error: crate::common::MidgeError::Internal(e.to_string()),
                        },
                    };

                    self.respond(request_id, resp);
                }

                RuntimeMsg::CompactAll { request_id } => {
                    // Do not allow explicit compaction while ingest barrier is active
                    if self
                        .state
                        .ingest_active
                        .load(std::sync::atomic::Ordering::SeqCst)
                    {
                        let epoch = self
                            .state
                            .ingest_epoch
                            .load(std::sync::atomic::Ordering::SeqCst);
                        tracing::error!(
                            component = "compaction",
                            invariant = "no_compaction_during_ingest",
                            ingest_epoch = epoch,
                            "BUG: CompactAll called while ingest mode is active."
                        );
                        self.respond(
                            request_id,
                            RuntimeResponse::Error {
                                request_id,
                                error: crate::common::MidgeError::Internal("BUG: compact_all attempted during ingest mode — violated invariant".to_string()),
                            },
                        );
                        continue;
                    }

                    // Schedule as many compactions as CheckCompaction suggests
                    let mut scheduled = 0usize;
                    loop {
                        if let Some(plan) = self.compaction_actor.check_compaction(&self.state) {
                            let schedule_res = self.compaction_actor.run_compaction(
                                &mut self.state,
                                plan,
                                self.hybrid_storage.as_ref(),
                                self.worker_msg_tx.clone(),
                            );

                            match schedule_res {
                                Ok(_) => scheduled += 1,
                                Err(e) => {
                                    self.respond(
                                        request_id,
                                        RuntimeResponse::Error {
                                            request_id,
                                            error: crate::common::MidgeError::Internal(
                                                e.to_string(),
                                            ),
                                        },
                                    );
                                    break;
                                }
                            }

                            // Continue trying to pick more plans
                            continue;
                        }

                        // No more plans to schedule right now
                        break;
                    }

                    if scheduled == 0 {
                        // Nothing to do
                        self.respond(request_id, RuntimeResponse::Ok { request_id });
                        continue;
                    }

                    // Wait for all scheduled compactions to complete (active_compactions -> 0)
                    {
                        let (lock, cvar) = &*self.state.active_compactions_notify;
                        let mut guard = lock.lock();

                        let wait_start = std::time::Instant::now();
                        let mut warned_500ms = false;
                        let mut warned_5s = false;

                        while self
                            .state
                            .active_compactions
                            .load(std::sync::atomic::Ordering::SeqCst)
                            > 0
                        {
                            let timeout = std::time::Duration::from_millis(100);
                            let _notified = cvar.wait_for(&mut guard, timeout);

                            let elapsed = wait_start.elapsed();

                            if !warned_500ms && elapsed > std::time::Duration::from_millis(500) {
                                warned_500ms = true;
                                let a = self
                                    .state
                                    .active_compactions
                                    .load(std::sync::atomic::Ordering::SeqCst);
                                tracing::warn!(
                                    component = "compaction",
                                    active_compactions = a,
                                    elapsed_ms = elapsed.as_millis(),
                                    "compact_all: waiting after 500ms for scheduled compactions to drain (active_compactions={})",
                                    a
                                );
                            }

                            if !warned_5s && elapsed > std::time::Duration::from_secs(5) {
                                warned_5s = true;
                                let a = self
                                    .state
                                    .active_compactions
                                    .load(std::sync::atomic::Ordering::SeqCst);
                                tracing::error!(
                                    component = "compaction",
                                    active_compactions = a,
                                    elapsed_ms = elapsed.as_millis(),
                                    "compact_all: still waiting >5s for scheduled compactions to drain (active_compactions={})",
                                    a
                                );
                            }
                        }
                    }

                    // Final sanity pass: attempt to schedule any emergent compactions
                    while let Some(plan) = self.compaction_actor.check_compaction(&self.state) {
                        let _ = self.compaction_actor.run_compaction(
                            &mut self.state,
                            plan,
                            self.hybrid_storage.as_ref(),
                            self.worker_msg_tx.clone(),
                        );
                    }

                    // Wait again if we scheduled any emergent compactions
                    {
                        let (lock, cvar) = &*self.state.active_compactions_notify;
                        let mut guard = lock.lock();
                        while self
                            .state
                            .active_compactions
                            .load(std::sync::atomic::Ordering::SeqCst)
                            > 0
                        {
                            let timeout = std::time::Duration::from_millis(100);
                            let _notified = cvar.wait_for(&mut guard, timeout);
                        }
                    }

                    self.respond(request_id, RuntimeResponse::Ok { request_id });
                }

                RuntimeMsg::CompactionComplete {
                    request_id,
                    input_ssts,
                    output_ssts,
                } => {
                    // Decrement active compactions and notify any waiters
                    let prev = self
                        .state
                        .active_compactions
                        .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                    if prev <= 1 {
                        // notify waiters that active_compactions may be zero now
                        let (lock, cvar) = &*self.state.active_compactions_notify;
                        let _guard = lock.lock();
                        cvar.notify_all();
                    }

                    self.compaction_actor
                        .handle_complete(&mut self.state, input_ssts, output_ssts);
                    self.respond(request_id, RuntimeResponse::Ok { request_id });
                }

                // WAL
                RuntimeMsg::WalAppend {
                    request_id,
                    cf_id,
                    key,
                    value,
                    ttl_seconds,
                    insert_only,
                } => {
                    if self.wal_actor.is_cloud_first() && self.hybrid_storage.is_none() {
                        self.respond(
                            request_id,
                            RuntimeResponse::Error {
                                request_id,
                                error: crate::common::MidgeError::Internal(
                                    "CloudFirst requires HybridStorage".to_string(),
                                ),
                            },
                        );
                        continue;
                    }

                    let result = self.wal_actor.append(
                        &mut self.state,
                        crate::runtime::actors::wal::AppendParams {
                            request_id,
                            cf_id,
                            key: bytes::Bytes::from(key),
                            value: value.map(bytes::Bytes::from),
                            insert_only,
                            ttl_seconds,
                        },
                    );
                    match result {
                        Ok((seq, deferred)) => {
                            if self.should_ack_immediately(deferred) {
                                if self.wal_actor.is_cloud_first() {
                                    // Background CloudFirst: confirm sequences immediately after local WAL write.
                                    // Cloud upload runs asynchronously; no need to queue waiters since we
                                    // respond immediately and make data visible for reads.
                                    self.state.confirm_sequences(request_id);
                                } else if deferred {
                                    self.maybe_queue_confirm_only_waiter(
                                        deferred, request_id, false,
                                    );
                                } else {
                                    self.state.confirm_sequences(request_id);
                                }

                                self.respond(
                                    request_id,
                                    RuntimeResponse::WalAppended {
                                        request_id,
                                        sequence: seq,
                                    },
                                );
                            } else {
                                self.durability.queue_waiter(DurabilityWaiter::WalAppend {
                                    request_id,
                                    sequence: seq,
                                });
                            }
                        }
                        Err(e) => {
                            self.respond(
                                request_id,
                                RuntimeResponse::Error {
                                    request_id,
                                    error: crate::common::MidgeError::Internal(e.to_string()),
                                },
                            );
                        }
                    }

                    // Auto-sync batched writes if needed (group commit completes all waiters)
                    self.sync_batched_wal_if_needed(&msg_rx);

                    self.maybe_flush_cloudfirst_wal();
                }

                RuntimeMsg::WalAppendDeleteRange {
                    request_id,
                    cf_id,
                    start_key,
                    end_key,
                } => {
                    if self.wal_actor.is_cloud_first() && self.hybrid_storage.is_none() {
                        self.respond(
                            request_id,
                            RuntimeResponse::Error {
                                request_id,
                                error: crate::common::MidgeError::Internal(
                                    "CloudFirst requires HybridStorage".to_string(),
                                ),
                            },
                        );
                        continue;
                    }

                    let result = self.wal_actor.append_delete_range(
                        &mut self.state,
                        request_id,
                        cf_id,
                        bytes::Bytes::from(start_key),
                        bytes::Bytes::from(end_key),
                    );
                    match result {
                        Ok((seq, deferred)) => {
                            if self.should_ack_immediately(deferred) {
                                if self.wal_actor.is_cloud_first() {
                                    // Background CloudFirst: confirm sequences immediately after local WAL write.
                                    self.state.confirm_sequences(request_id);
                                } else if deferred {
                                    self.maybe_queue_confirm_only_waiter(
                                        deferred, request_id, false,
                                    );
                                } else {
                                    self.state.confirm_sequences(request_id);
                                }

                                self.respond(
                                    request_id,
                                    RuntimeResponse::WalAppended {
                                        request_id,
                                        sequence: seq,
                                    },
                                );
                            } else {
                                self.durability.queue_waiter(DurabilityWaiter::WalAppend {
                                    request_id,
                                    sequence: seq,
                                });
                            }
                        }
                        Err(e) => {
                            self.respond(
                                request_id,
                                RuntimeResponse::Error {
                                    request_id,
                                    error: crate::common::MidgeError::Internal(e.to_string()),
                                },
                            );
                        }
                    }

                    self.sync_batched_wal_if_needed(&msg_rx);
                    self.maybe_flush_cloudfirst_wal();
                }

                RuntimeMsg::WalSync { request_id } => {
                    let result = self.wal_actor.sync(&mut self.state);
                    let resp = result
                        .map(|_| RuntimeResponse::Ok { request_id })
                        .unwrap_or_else(|e| RuntimeResponse::Error {
                            request_id,
                            error: crate::common::MidgeError::Internal(e.to_string()),
                        });
                    self.respond(request_id, resp);
                }

                RuntimeMsg::WalRotate { request_id } => {
                    let result = self.wal_actor.rotate(&mut self.state);
                    let resp = result
                        .map(|_| RuntimeResponse::Ok { request_id })
                        .unwrap_or_else(|e| RuntimeResponse::Error {
                            request_id,
                            error: crate::common::MidgeError::Internal(e.to_string()),
                        });
                    self.respond(request_id, resp);
                }

                RuntimeMsg::WalSyncComplete {
                    request_id,
                    segment_id,
                } => {
                    self.wal_actor
                        .handle_sync_complete(&mut self.state, segment_id);
                    self.respond(request_id, RuntimeResponse::Ok { request_id });
                }

                // Cloud
                RuntimeMsg::CloudUploadSst {
                    request_id,
                    sst_name,
                } => {
                    let result = self.cloud_actor.upload_sst(
                        &mut self.state,
                        &sst_name,
                        self.hybrid_storage.as_ref(),
                    );
                    let resp = result
                        .map(|_| RuntimeResponse::Ok { request_id })
                        .unwrap_or_else(|e| RuntimeResponse::Error {
                            request_id,
                            error: crate::common::MidgeError::Internal(e.to_string()),
                        });
                    self.respond(request_id, resp);
                }

                RuntimeMsg::CloudUploadWal {
                    request_id,
                    segment_id,
                } => {
                    let result = self.cloud_actor.upload_wal(&mut self.state, segment_id);
                    let resp = result
                        .map(|_| RuntimeResponse::Ok { request_id })
                        .unwrap_or_else(|e| RuntimeResponse::Error {
                            request_id,
                            error: crate::common::MidgeError::Internal(e.to_string()),
                        });
                    self.respond(request_id, resp);
                }

                RuntimeMsg::CloudUploadComplete {
                    request_id,
                    resource,
                } => {
                    self.cloud_actor
                        .handle_upload_complete(&mut self.state, &resource);
                    self.respond(request_id, RuntimeResponse::Ok { request_id });
                }

                // GC
                RuntimeMsg::CheckGc { request_id } => {
                    self.gc_actor.check(&self.state);
                    self.respond(request_id, RuntimeResponse::Ok { request_id });
                }

                RuntimeMsg::DeleteObsoleteSsts {
                    request_id,
                    sst_names,
                } => {
                    let result = self.gc_actor.delete_ssts(&mut self.state, &sst_names);
                    let resp = result
                        .map(|_| RuntimeResponse::Ok { request_id })
                        .unwrap_or_else(|e| RuntimeResponse::Error {
                            request_id,
                            error: crate::common::MidgeError::Internal(e.to_string()),
                        });
                    self.respond(request_id, resp);
                }

                // Manifest
                RuntimeMsg::ManifestAddSst {
                    request_id,
                    file_meta,
                } => {
                    let result = self.manifest_actor.add_sst(&mut self.state, file_meta);
                    let resp = result
                        .map(|_| RuntimeResponse::Ok { request_id })
                        .unwrap_or_else(|e| RuntimeResponse::Error {
                            request_id,
                            error: crate::common::MidgeError::Internal(e.to_string()),
                        });
                    self.respond(request_id, resp);
                }

                RuntimeMsg::ManifestCompactionComplete {
                    request_id,
                    removed,
                    added,
                } => {
                    let result =
                        self.manifest_actor
                            .compaction_complete(&mut self.state, removed, added);
                    let resp = result
                        .map(|_| RuntimeResponse::Ok { request_id })
                        .unwrap_or_else(|e| RuntimeResponse::Error {
                            request_id,
                            error: crate::common::MidgeError::Internal(e.to_string()),
                        });
                    self.respond(request_id, resp);
                }

                RuntimeMsg::ManifestPersist { request_id } => {
                    let result = self.manifest_actor.persist(&self.state);
                    let resp = result
                        .map(|_| RuntimeResponse::Ok { request_id })
                        .unwrap_or_else(|e| RuntimeResponse::Error {
                            request_id,
                            error: crate::common::MidgeError::Internal(e.to_string()),
                        });
                    self.respond(request_id, resp);
                }

                RuntimeMsg::ManifestCreateColumnFamily { request_id, name } => {
                    // Block DDL while ingest active
                    if self
                        .state
                        .ingest_active
                        .load(std::sync::atomic::Ordering::SeqCst)
                    {
                        tracing::error!("ingest: attempted DDL (create CF) during ingest mode");
                        self.respond(
                            request_id,
                            RuntimeResponse::Error {
                                request_id,
                                error: crate::common::MidgeError::Internal(
                                    "ingest: DDL forbidden during ingest mode".to_string(),
                                ),
                            },
                        );
                        continue;
                    }

                    // DDL durability barrier: ensure WAL is durable before CF creation
                    self.force_wal_sync(&msg_rx);

                    let result = self
                        .manifest_actor
                        .create_column_family(&mut self.state, name.clone());
                    let resp = result
                        .map(|cf_id| RuntimeResponse::ColumnFamilyCreated { request_id, cf_id })
                        .unwrap_or_else(|e| RuntimeResponse::Error {
                            request_id,
                            error: crate::common::MidgeError::Internal(e.to_string()),
                        });
                    self.respond(request_id, resp);
                }

                RuntimeMsg::ManifestDropColumnFamily { request_id, cf_id } => {
                    // Block DDL while ingest active
                    if self
                        .state
                        .ingest_active
                        .load(std::sync::atomic::Ordering::SeqCst)
                    {
                        tracing::error!("ingest: attempted DDL (drop CF) during ingest mode");
                        self.respond(
                            request_id,
                            RuntimeResponse::Error {
                                request_id,
                                error: crate::common::MidgeError::Internal(
                                    "ingest: DDL forbidden during ingest mode".to_string(),
                                ),
                            },
                        );
                        continue;
                    }

                    // DDL durability barrier: ensure WAL is durable before CF drop
                    self.force_wal_sync(&msg_rx);

                    let result = self
                        .manifest_actor
                        .drop_column_family(&mut self.state, cf_id);
                    let resp = result
                        .map(|_| RuntimeResponse::Ok { request_id })
                        .unwrap_or_else(|e| RuntimeResponse::Error {
                            request_id,
                            error: crate::common::MidgeError::Internal(e.to_string()),
                        });
                    self.respond(request_id, resp);
                }

                // Read path: local memtable lookup
                RuntimeMsg::Read {
                    request_id,
                    cf_id,
                    key,
                    sequence,
                    requested_durability,
                } => {
                    self.handle_msg_read(request_id, cf_id, key, sequence, requested_durability);
                }

                RuntimeMsg::RangeScan {
                    request_id,
                    cf_id,
                    start,
                    end,
                    sequence,
                    requested_durability,
                } => {
                    self.handle_msg_range_scan(
                        request_id,
                        cf_id,
                        start,
                        end,
                        sequence,
                        requested_durability,
                    );
                }
            }
        }

        tracing::debug!("Runtime message channel closed — exiting event loop");
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::runtime::{state::RuntimeState, ResponseRouter};
    use std::sync::Arc;

    // Helper to create a minimal runtime state for testing
    pub(in crate::runtime::event_loop) fn create_test_state() -> RuntimeState {
        RuntimeState::new("/tmp/test_event_loop".into(), true) // Memory mode
    }

    // Helper to create a new event loop
    pub(in crate::runtime::event_loop) fn create_test_event_loop(
    ) -> crate::common::MidgeResult<EventLoop> {
        let state = create_test_state();
        let router = Arc::new(ResponseRouter::new());
        EventLoop::new(
            state,
            false,
            router,
            crate::runtime::RuntimeConfig::default(),
            None,
        )
    }

    // =========== EventLoop Creation Tests ===========

    #[test]
    fn should_create_event_loop_in_memory_mode() {
        // Arrange
        // Act
        let result = create_test_event_loop();

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_initialize_all_actors() {
        // Arrange
        // Act
        let event_loop = create_test_event_loop().expect("Should create event loop");

        // Assert - Just verify construction doesn't panic and has all actors
        // We can't directly inspect actors, but we verify no errors during init
        drop(event_loop);
    }

    #[test]
    fn should_initialize_with_tracing_disabled() {
        // Arrange
        let state = create_test_state();
        let router = Arc::new(ResponseRouter::new());

        // Act
        let event_loop = EventLoop::new(
            state,
            false,
            router,
            crate::runtime::RuntimeConfig::default(),
            None,
        );

        // Assert
        assert!(event_loop.is_ok());
    }

    #[test]
    fn should_initialize_with_tracing_enabled() {
        // Arrange
        let state = create_test_state();
        let router = Arc::new(ResponseRouter::new());

        // Act
        let event_loop = EventLoop::new(
            state,
            true,
            router,
            crate::runtime::RuntimeConfig::default(),
            None,
        );

        // Assert
        assert!(event_loop.is_ok());
    }

    // =========== Response Routing Tests ===========

    #[test]
    fn should_route_responses_via_router() {
        // Arrange
        let state = create_test_state();
        let router = Arc::new(ResponseRouter::new());

        // Act
        let event_loop = EventLoop::new(
            state,
            false,
            router.clone(),
            crate::runtime::RuntimeConfig::default(),
            None,
        );

        // Assert
        assert!(event_loop.is_ok());
        // Response routing happens through the router, which we test separately
    }

    // =========== State Management Tests ===========

    #[test]
    fn should_maintain_runtime_state_invariants() {
        // Arrange
        let state = create_test_state();

        // Act - Create event loop (should not modify state invariants during init)
        let router = Arc::new(ResponseRouter::new());
        let result = EventLoop::new(
            state,
            false,
            router,
            crate::runtime::RuntimeConfig::default(),
            None,
        );

        // Assert
        assert!(result.is_ok());
        // State is moved into EventLoop, can't inspect after, but construction validates it
    }

    // =========== Trace Flag Tests ===========

    #[test]
    fn should_respect_trace_enabled_flag() {
        // Arrange
        let state1 = create_test_state();
        let state2 = create_test_state();
        let router1 = Arc::new(ResponseRouter::new());
        let router2 = Arc::new(ResponseRouter::new());

        // Act
        let event_loop1 = EventLoop::new(
            state1,
            false,
            router1,
            crate::runtime::RuntimeConfig::default(),
            None,
        )
        .expect("Should create");
        let event_loop2 = EventLoop::new(
            state2,
            true,
            router2,
            crate::runtime::RuntimeConfig::default(),
            None,
        )
        .expect("Should create");

        // Assert
        assert!(!event_loop1.trace_enabled);
        assert!(event_loop2.trace_enabled);
    }

    // =========== Actor Initialization Tests ===========

    #[test]
    fn should_initialize_flush_actor() {
        // Arrange
        // Act
        let event_loop = create_test_event_loop().expect("Should create event loop");

        // Assert - Verify through construction success
        // FlushActor is initialized and owned by EventLoop
        assert!(event_loop.hybrid_storage.is_none()); // Related check
    }

    #[test]
    fn should_initialize_compaction_actor() {
        // Arrange
        // Act
        let event_loop = create_test_event_loop().expect("Should create event loop");

        // Assert - CompactionActor is initialized
        // We verify this indirectly through successful construction
        drop(event_loop);
    }

    #[test]
    fn should_initialize_wal_actor() {
        // Arrange
        // Act
        let event_loop = create_test_event_loop().expect("Should create event loop");

        // Assert - WalActor is initialized
        drop(event_loop);
    }

    #[test]
    fn should_initialize_cloud_actor() {
        // Arrange
        // Act
        let event_loop = create_test_event_loop().expect("Should create event loop");

        // Assert
        drop(event_loop);
    }

    #[test]
    fn should_initialize_gc_actor() {
        // Arrange
        // Act
        let event_loop = create_test_event_loop().expect("Should create event loop");

        // Assert
        drop(event_loop);
    }

    #[test]
    fn should_initialize_manifest_actor() {
        // Arrange
        // Act
        let event_loop = create_test_event_loop().expect("Should create event loop");

        // Assert
        drop(event_loop);
    }

    // =========== Invariant Tests ===========

    #[test]
    fn should_maintain_actor_ownership() {
        // Arrange
        // Act
        let event_loop = create_test_event_loop().expect("Should create event loop");

        // Assert - All actors should be owned by event loop
        // They're private fields, so we verify through successful construction
        // and that event_loop doesn't expose uninitialized actors
        assert!(event_loop.eviction_actor.is_none()); // Eviction actor is optional
    }

    #[test]
    fn should_maintain_router_reference() {
        // Arrange
        let state = create_test_state();
        let router = Arc::new(ResponseRouter::new());
        let router_clone = router.clone();

        // Act
        let _event_loop = EventLoop::new(
            state,
            false,
            router,
            crate::runtime::RuntimeConfig::default(),
            None,
        )
        .expect("Should create");

        // Assert - Router is properly stored
        // The router's methods can be called independently
        let _rx = router_clone.register(1);
    }

    // =========== Memory Mode Tests ===========

    #[test]
    fn should_handle_memory_mode_initialization() {
        // Arrange
        let state = RuntimeState::new("/tmp/test_memory".into(), true);

        // Act
        let router = Arc::new(ResponseRouter::new());
        let result = EventLoop::new(
            state,
            false,
            router,
            crate::runtime::RuntimeConfig::default(),
            None,
        );

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_handle_filesystem_mode_initialization() {
        // Arrange - Create state in filesystem mode
        let state = RuntimeState::new("/tmp/test_filesystem".into(), false);

        // Act
        let router = Arc::new(ResponseRouter::new());
        let result = EventLoop::new(
            state,
            false,
            router,
            crate::runtime::RuntimeConfig::default(),
            None,
        );

        // Assert
        assert!(result.is_ok());
    }

    // =========== Actor Factory Tests ===========

    #[test]
    fn should_create_sst_factory_for_compaction_actor() {
        // Arrange
        // Act
        let event_loop = create_test_event_loop().expect("Should create event loop");

        // Assert - SST factory is created and passed to CompactionActor
        // This is verified by successful construction
        drop(event_loop);
    }

    #[test]
    fn should_use_correct_block_size_for_sst_factory() {
        // Arrange - Create event loop which creates SST factory with 64KB block size
        let event_loop = create_test_event_loop().expect("Should create event loop");

        // Act

        // Assert - The 64KB block size is hardcoded in EventLoop::new
        // This test documents that invariant
        drop(event_loop);
    }
}
