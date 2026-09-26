//! Event loop — central message dispatcher
//!
//! Receives messages from `RuntimeHandle` and routes them to the correct actor.
//!
//! Maintainer note:
//! - Per-request routing flows through `respond()`.
//! - `EventLoop` never touches `pending_responses` directly.
//! - All read paths are local (memtables → SST later).
//! - All actor responses flow through `respond()`.
//!
//! # Module structure
//!
//! The event loop is split across domain-specific files:
//!
//! - `read_path` — point reads, range scans, durability-aware message handlers
//! - `durability_sync` — WAL sync, group commit, durability waiter completion
//! - `cloud_integration` — `CloudAsync` WAL flush, cloud ack/fail handling
//! - `write_batch` — group commit write draining, backpressure / write stall

mod cloud_coordinator;
mod cloud_integration;
mod cloud_maintenance;
mod cloud_memtable_admission;
mod compaction;
mod control;
mod coordination;
mod dispatch;
mod durability_sync;
mod fencing;
mod flush;
mod flush_pipeline;
mod gc;
mod manifest;
mod metadata_mirror;
mod read_path;
mod resources;
mod scheduler;
mod shutdown;
mod snapshot;
mod sst_names;
mod verification;
mod wal;
mod wal_retention;
mod wal_transition;
mod write_batch;

use crossbeam::channel::Sender;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// How flush work is executed for one `EventLoop`.
///
/// This is an explicit construction-time choice rather than a `cfg!(test)`
/// branch, so the scheduling semantics a test exercises are the ones it asked
/// for and are reproducible in a production build.
pub(crate) enum FlushWorkerMode {
    /// Flush jobs run on the flush actor's worker threads; completions are
    /// observed asynchronously by the event loop, and background coordinators
    /// post follow-up work back through the supplied runtime channel.
    Background(Sender<RuntimeMsg>),
    /// Flush jobs are drained to completion inline, before the call that
    /// scheduled them returns. There is no runtime channel for background
    /// workers to post back through.
    ///
    /// Only test fixtures construct the loop this way today. The variant stays
    /// in the production enum so flush scheduling is chosen the same way, from
    /// the same table, in every build.
    #[cfg_attr(not(test), allow(dead_code))]
    Inline,
}

impl FlushWorkerMode {
    fn worker_msg_tx(self) -> Option<Sender<RuntimeMsg>> {
        match self {
            FlushWorkerMode::Background(tx) => Some(tx),
            FlushWorkerMode::Inline => None,
        }
    }

    const fn is_inline(&self) -> bool {
        matches!(self, FlushWorkerMode::Inline)
    }
}

/// SST names reserved per durable reservation (#491).
const BACKGROUND_COMPACTION_CHECK_INTERVAL: Duration = Duration::from_secs(30);
const STARTUP_CLOUD_MAINTENANCE_DELAY: Duration = Duration::from_millis(100);
const HYBRID_STORAGE_POLL_INTERVAL: Duration = Duration::from_millis(5);

use super::actors::{CompactionActor, FlushActor, GcActor, ManifestActor, WalActor};
use super::durability::DurabilityCoordinator;
use super::read_resources::ReadResources;
use super::read_snapshot::ReadSnapshot;
use super::snapshot_cache::{CfSnapshotData, PublishedSnapshot, SnapshotCache};
use super::state::RuntimeState;
use super::{MetadataPublicationLock, ResponseRouter, RuntimeMsg, RuntimeResponse};
use crate::runtime::actors::compaction::publication::{
    CompactionPublishActor, CompactionPublishCompletion,
};
use crate::runtime::actors::flush::FlushWorkerResult;

struct RecoveredCloudWalConfig {
    remote_segments: BTreeMap<u64, crate::runtime::RecoveredCloudWalSegment>,
    local_segments: BTreeMap<u64, crate::runtime::RecoveredCloudWalSegment>,
    active_wal: Option<crate::runtime::RecoveredCloudActiveWal>,
}

impl From<&super::RuntimeConfig> for RecoveredCloudWalConfig {
    fn from(config: &super::RuntimeConfig) -> Self {
        let with_epochs = |segments: &BTreeMap<u64, u64>, epochs: &BTreeMap<u64, u64>| {
            segments
                .iter()
                .map(|(segment_id, max_sequence)| {
                    (
                        *segment_id,
                        crate::runtime::RecoveredCloudWalSegment {
                            max_sequence: *max_sequence,
                            writer_epoch: epochs.get(segment_id).copied().unwrap_or_default(),
                        },
                    )
                })
                .collect()
        };
        Self {
            remote_segments: with_epochs(
                &config.recovered_cloud_wal_segments,
                &config.recovered_cloud_wal_segment_epochs,
            ),
            local_segments: with_epochs(
                &config.recovered_local_wal_segments,
                &config.recovered_local_wal_segment_epochs,
            ),
            active_wal: config.recovered_cloud_active_wal,
        }
    }
}

use cloud_coordinator::CloudCoordinator;
use coordination::{ManifestPublicationGate, VerificationBarrier, WriteStallWaiters};

/// Main synchronous event loop for the runtime.
///
/// Owns all actors and is responsible for routing inbound messages.
#[allow(clippy::struct_excessive_bools)]
pub struct EventLoop {
    pub(super) state: RuntimeState,

    // Actors
    pub(super) flush_actor: FlushActor,
    pub(super) compaction_actor: CompactionActor,
    pub(super) compaction_publish_actor: CompactionPublishActor,
    pub(super) wal_actor: WalActor,
    gc_actor: GcActor,
    manifest_actor: ManifestActor,
    pub(super) cloud_coordinator: CloudCoordinator,
    pub(super) metadata_publication_lock: MetadataPublicationLock,
    pub(super) trace_enabled: bool,
    pub(super) loop_debug: bool,
    pub(super) loop_debug_wakes: u64,
    pub(super) loop_debug_batch_total: u64,
    next_background_compaction_check: Instant,

    // Durability coordination (extracted to reduce EventLoop cognitive load)
    pub(super) durability: DurabilityCoordinator,
    wal_transition: crate::runtime::wal_transition::WalTransitionProtocol,

    /// Per-request router (oneshot channels)
    pub(super) router: Arc<ResponseRouter>,
    /// Direct response channels for hot runtime-internal requests.
    pub(super) inline_responses: RefCell<HashMap<u64, Sender<RuntimeResponse>>>,

    /// One buffered message we pulled from the channel while draining writes.
    ///
    /// This preserves FIFO semantics when we opportunistically `try_recv()` to batch writes:
    /// if we encounter a non-write message, we stash it here and handle it next.
    pub(super) pending_msg: Option<RuntimeMsg>,

    verification_barrier: VerificationBarrier,
    legacy_bound_backfill: read_path::LegacyBoundBackfill,
    publication_gate: ManifestPublicationGate,

    pub(super) flush_worker_result_rx: crossbeam::channel::Receiver<FlushWorkerResult>,
    pub(super) compaction_publish_result_rx:
        crossbeam::channel::Receiver<CompactionPublishCompletion>,
    pub(super) compaction_publication: Option<compaction::PendingCompactionPublication>,
    flush_barrier_waiters: HashMap<crate::types::ColumnFamilyId, Vec<flush::FlushBarrierWaiter>>,
    inline_flush_worker: bool,
    shutting_down: bool,
    shutdown_cloud_drain_timeout: Duration,

    /// Sender that worker threads can use to post back completion messages
    /// (compaction threads will use this to report completion).
    pub(super) worker_msg_tx: Option<crossbeam::channel::Sender<RuntimeMsg>>,

    pub(super) write_stall_waiters: WriteStallWaiters,
    /// Lock-free snapshot cache shared with Engine for read-path bypass.
    pub(super) snapshot_cache: Option<Arc<SnapshotCache>>,
    /// Shared SST readers and block cache used by runtime read snapshots.
    pub(super) read_resources: Option<Arc<ReadResources>>,

    /// Shared flag from the lease heartbeat. When `false`, the event loop
    /// rejects new write operations with `MidgeError::Fenced`.
    fencing: fencing::RuntimeFence,
    /// A compaction manifest authority switch completed, but its publication
    /// intent could not be advanced or settled. Further compaction could
    /// consume that output and make restart recovery ambiguous, so compaction
    /// remains fenced until reopen replays the durable intent.
    compaction_publication_degraded: bool,
    /// Terminal publication error for the just-completed compaction. This is
    /// forwarded to a pending `compact_all()` waiter after authority handling.
    last_compaction_publication_error: Option<crate::common::MidgeError>,
    /// Response budget a caller is given for one runtime request. Cloud work
    /// performed on a caller's behalf shares this budget rather than restarting
    /// a fresh `storage_io_timeout` per round trip.
    runtime_response_timeout: std::time::Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HandleOutcome {
    Continue,
    Break,
}

impl EventLoop {
    /// Route components that record on their own threads into this engine's
    /// counters, so its metrics snapshot reflects this engine alone.
    /// Also applies the WAL actor's replay limit, set at the same point.
    fn attach_engine_counters(
        state: &RuntimeState,
        wal_actor: &mut WalActor,
        config: &super::RuntimeConfig,
    ) {
        wal_actor.set_max_replayable_txn_bytes(config.max_replayable_txn_bytes);
        wal_actor.attach_counters(&state.diagnostics);
        if let Some(storage) = &config.hybrid_storage {
            state.diagnostics.attach(storage.counters());
        }
    }

    pub(crate) fn new(
        mut state: RuntimeState,
        trace_enabled: bool,
        router: Arc<ResponseRouter>,
        config: super::RuntimeConfig,
        flush_worker_mode: FlushWorkerMode,
    ) -> crate::common::MidgeResult<Self> {
        let (inline_flush_worker, worker_msg_tx) = (
            flush_worker_mode.is_inline(),
            flush_worker_mode.worker_msg_tx(),
        );
        let (wal_dir, sst_dir, memory_mode, initial_segment_id) = (
            state.wal_dir.clone(),
            state.sst_dir.clone(),
            state.is_memory_mode(),
            state.wal.current_segment_id,
        );
        let recovered_cloud_wal = RecoveredCloudWalConfig::from(&config);
        Self::apply_runtime_state_config(&mut state, &config);

        let resources = resources::assemble_sst_resources(&state, &sst_dir, memory_mode, &config)?;
        let sst_factory = resources.factory;
        let read_resources = resources.reads;
        state.writer_epoch = config.writer_epoch;
        let (flush_completion_tx, flush_worker_result_rx) =
            crossbeam::channel::unbounded::<FlushWorkerResult>();
        let (compaction_publish_actor, compaction_publish_result_rx) =
            Self::create_compaction_publish_actor(inline_flush_worker)?;

        // Create actors - they handle memory_mode internally
        let flush_actor = FlushActor::new_with_memory_limit(
            &sst_dir,
            memory_mode,
            config.compression_policy.clone(),
            flush_completion_tx,
            config.flush_memory_limit,
        )?;
        let mut wal_actor = WalActor::new(
            wal_dir,
            config.wal_durability_policy,
            config.wal_batch_config,
            memory_mode,
            config.writer_epoch,
            config.storage_io_timeout,
        )?;

        Self::configure_wal_actor(&state, &mut wal_actor, &config, read_resources.as_ref());

        // CloudAsync waiters are keyed by segment id. Local waiters start at
        // generation zero; the durability coordinator is the sole owner of
        // that logical generation.
        let mut gc_actor = GcActor::new();
        gc_actor.set_retry_notifier(worker_msg_tx.clone());
        let durability =
            Self::create_durability_coordinator(&wal_actor, initial_segment_id, &config);

        let mut event_loop = Self {
            state,
            flush_actor,
            compaction_actor: Self::create_compaction_actor(sst_factory, &config),
            compaction_publish_actor,
            wal_actor,
            gc_actor,
            manifest_actor: ManifestActor::new(),
            cloud_coordinator: CloudCoordinator::new(&config),
            metadata_publication_lock: config.metadata_publication_lock.clone(),
            trace_enabled,
            loop_debug: std::env::var_os("MIDGE_LOOP_DEBUG").is_some(),
            loop_debug_wakes: 0,
            loop_debug_batch_total: 0,
            next_background_compaction_check: Instant::now() + BACKGROUND_COMPACTION_CHECK_INTERVAL,
            durability,
            wal_transition: crate::runtime::wal_transition::WalTransitionProtocol::new(),
            router,
            inline_responses: RefCell::new(HashMap::new()),
            pending_msg: None,
            verification_barrier: VerificationBarrier::default(),
            legacy_bound_backfill: read_path::LegacyBoundBackfill::default(),
            publication_gate: ManifestPublicationGate::default(),
            flush_worker_result_rx,
            compaction_publish_result_rx,
            compaction_publication: None,
            flush_barrier_waiters: HashMap::new(),
            inline_flush_worker,
            shutting_down: false,
            shutdown_cloud_drain_timeout: config.shutdown_cloud_drain_timeout,
            worker_msg_tx,
            write_stall_waiters: WriteStallWaiters::default(),
            snapshot_cache: None,
            read_resources,
            fencing: fencing::RuntimeFence {
                lease_healthy: config.lease_healthy.clone(),
                ddl_authority_ambiguous: false,
                writer_epoch: config.writer_epoch,
                leader_store: config.leader_store.clone(),
                leader_holder_id: config.leader_holder_id.clone(),
            },
            compaction_publication_degraded: false,
            last_compaction_publication_error: None,
            runtime_response_timeout: config.runtime_response_timeout,
        };

        event_loop.finish_initialization(
            config.hybrid_storage,
            config.compaction_memory_limit,
            &recovered_cloud_wal,
        )?;

        Ok(event_loop)
    }

    fn finish_initialization(
        &mut self,
        hybrid_storage: Option<Arc<crate::storage::HybridStorage>>,
        compaction_memory_limit: usize,
        recovered_cloud_wal: &RecoveredCloudWalConfig,
    ) -> crate::common::MidgeResult<()> {
        if let Some(storage) = hybrid_storage {
            storage.configure_maintenance_memory(compaction_memory_limit);
            self.set_hybrid_storage(storage);
        }
        self.initialize_recovered_cloud_wal(recovered_cloud_wal)
    }

    /// The compaction publisher shares the flush worker's inline mode so
    /// deterministic tests run every publication phase on the calling thread.
    fn create_compaction_publish_actor(
        inline: bool,
    ) -> crate::common::MidgeResult<(
        CompactionPublishActor,
        crossbeam::channel::Receiver<CompactionPublishCompletion>,
    )> {
        let (completion_tx, completion_rx) = crossbeam::channel::unbounded();
        Ok((
            CompactionPublishActor::new(completion_tx, inline)?,
            completion_rx,
        ))
    }

    fn create_durability_coordinator(
        wal_actor: &WalActor,
        initial_segment_id: u64,
        config: &super::RuntimeConfig,
    ) -> DurabilityCoordinator {
        let is_cloud_async = wal_actor.is_cloud_async();
        let initial_durability_key = if is_cloud_async {
            initial_segment_id
        } else {
            0
        };
        DurabilityCoordinator::new(
            initial_durability_key,
            is_cloud_async,
            config.cloud_runtime_policy.clone(),
        )
    }

    fn create_compaction_actor(
        sst_factory: Arc<dyn crate::sst::SstFactory>,
        config: &super::RuntimeConfig,
    ) -> CompactionActor {
        let compaction_config = crate::compaction::LeveledCompactionConfig {
            l0_file_count_threshold: config.l0_compaction_trigger.max(1),
            ..Default::default()
        };
        let mut actor = CompactionActor::new_with_config(sst_factory, compaction_config);
        actor.set_execution_limits(config.target_sst_size, config.compaction_memory_limit);
        actor
    }

    /// Wires the WAL actor to the engine: its counters, the read caches its
    /// transaction validation reads through, and the leader store it
    /// validates the writer epoch against at sync boundaries.
    fn configure_wal_actor(
        state: &RuntimeState,
        wal_actor: &mut WalActor,
        config: &super::RuntimeConfig,
        read_resources: Option<&Arc<ReadResources>>,
    ) {
        Self::attach_engine_counters(state, wal_actor, config);
        wal_actor.set_read_resources(read_resources.cloned());
        if let Some(store) = config.leader_store.clone() {
            wal_actor.set_leader_store(store, config.leader_holder_id.clone().unwrap_or_default());
        }
    }

    fn apply_runtime_state_config(state: &mut RuntimeState, config: &super::RuntimeConfig) {
        state.install_ttl_clock(Arc::clone(&config.ttl_clock));
        state.limits.eventual_flush_segment_gap =
            config.cloud_runtime_policy.eventual_flush_segment_gap;
        state.set_compaction_enabled(config.background_compaction);
        state.limits.l0_compaction_trigger = config.l0_compaction_trigger.max(1);
    }

    fn initialize_recovered_cloud_wal(
        &mut self,
        config: &RecoveredCloudWalConfig,
    ) -> crate::common::MidgeResult<()> {
        let remote_segments = &config.remote_segments;
        let local_segments = &config.local_segments;
        let active_wal = config.active_wal;
        if remote_segments.is_empty() && local_segments.is_empty() && active_wal.is_none() {
            return Ok(());
        }
        if !self.wal_actor.is_cloud_async() {
            return Err(crate::common::MidgeError::RecoveryFailed(
                "cloud WAL recovery obligations installed outside CloudAsync mode".to_string(),
            ));
        }
        self.cloud_coordinator
            .hybrid_storage
            .as_ref()
            .ok_or_else(|| {
                crate::common::MidgeError::RecoveryFailed(
                    "cloud WAL recovery requires hybrid storage".to_string(),
                )
            })?;

        let mut recovered_segments = remote_segments.clone();
        for (&segment_id, &segment) in local_segments {
            if let Some(remote_segment) = recovered_segments.insert(segment_id, segment) {
                return Err(crate::common::MidgeError::RecoveryFailed(format!(
                    "WAL segment {segment_id} is both remote and local-only during recovery: remote max {}, local max {}",
                    remote_segment.max_sequence,
                    segment.max_sequence
                )));
            }
        }
        for (&segment_id, segment) in &recovered_segments {
            self.durability
                .record_cloud_segment_inflight(segment_id, segment.max_sequence);
            self.wal_transition.register_recovered(
                segment_id,
                segment.max_sequence,
                remote_segments.contains_key(&segment_id),
            );
        }

        let initially_durable = self
            .durability
            .contiguous_acked_cloud_segments(&self.cloud_coordinator.cloud_wal.acked_segments)
            .map_err(crate::common::MidgeError::RecoveryFailed)?;
        if let Some((_, max_sequence)) = initially_durable.last() {
            self.state.wal.frontiers.advance_cloud_to(*max_sequence);
        }
        for (segment_id, _) in initially_durable {
            self.wal_transition.note_cloud_durable(segment_id)?;
            self.durability.retire_cloud_segment(segment_id);
            if self.remove_cloud_durable_local_wal_segment(segment_id) {
                self.wal_transition.retire_cloud_durable(segment_id)?;
            }
        }

        for (&segment_id, segment) in local_segments {
            self.cloud_coordinator
                .cloud_wal
                .upload_backlog
                .insert(segment_id, segment.max_sequence);
        }

        if let Some(active_wal) = active_wal {
            self.wal_actor
                .restore_recovered_cloud_active_wal(&mut self.state, active_wal)?;
            if self.seal_recovered_cloud_active_segment()?.is_none() {
                return Err(crate::common::MidgeError::RecoveryFailed(
                    "recovered active cloud WAL was not sealed for resumed upload".to_string(),
                ));
            }
        }

        Ok(())
    }

    pub fn set_hybrid_storage(&mut self, storage: Arc<crate::storage::HybridStorage>) {
        storage.configure_maintenance_memory(self.compaction_actor.compaction_memory_limit());
        self.wal_actor.set_storage_budget(Arc::clone(&storage));
        self.cloud_coordinator.hybrid_storage = Some(storage);
    }

    /// Returns an error if the lease has been lost (heartbeat detected failure).
    fn check_lease_health(&self) -> crate::common::MidgeResult<()> {
        self.fencing.check_health()
    }

    fn validate_runtime_writer_lease_within(
        &self,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<()> {
        self.fencing.validate_within(deadline)
    }

    /// Deadline for a routed request that began waiting in `RuntimeHandle`.
    ///
    /// A missing route means the caller already abandoned the request, not
    /// that accepted caller-owned work should receive an unbounded budget.
    pub(super) fn registered_request_deadline(
        &self,
        request_id: u64,
    ) -> crate::common::OperationDeadline {
        self.router.registered_at(request_id).map_or_else(
            || crate::common::OperationDeadline::from_budget(Duration::ZERO),
            |registered_at| {
                crate::common::OperationDeadline::from_start(
                    registered_at,
                    self.runtime_response_timeout,
                )
            },
        )
    }

    /// Budget for cloud work the event loop performs on its own thread.
    ///
    /// Anything this thread waits for blocks every read, write, ack and
    /// shutdown behind it, so it gets the same budget the runtime promises
    /// its callers rather than waiting indefinitely.
    pub(super) fn event_loop_cloud_deadline(&self) -> crate::common::OperationDeadline {
        crate::common::OperationDeadline::from_budget(self.runtime_response_timeout)
    }

    /// Set the snapshot cache for read-path bypass.
    pub fn set_snapshot_cache(&mut self, cache: Arc<SnapshotCache>) {
        self.snapshot_cache = Some(cache);
        // Publish initial snapshot from current state
        self.publish_snapshot();
    }

    /// Publish current state to the lock-free snapshot cache.
    ///
    /// Called after writes, flushes, and CF lifecycle events so that
    /// `begin_tx` can capture snapshots without event loop round-trip.
    #[inline]
    pub(super) fn publish_snapshot(&self) {
        let Some(cache) = &self.snapshot_cache else {
            return;
        };

        let mut cf_snapshots = std::collections::HashMap::new();
        for (&cf_id, cf_state) in &self.state.column_families {
            let sst_view = self.state.manifest.read_view_for(cf_id);
            let sst_path_prefix = self
                .state
                .sst_dir
                .strip_prefix(&self.state.db_path)
                .unwrap_or_else(|_| std::path::Path::new("sst"))
                .to_path_buf();
            cf_snapshots.insert(
                cf_id,
                CfSnapshotData {
                    snapshot: Arc::new(ReadSnapshot::new_with_view_resources(
                        cf_id,
                        cf_state.memtable.clone(),
                        cf_state.immutable_memtables.clone(),
                        sst_view,
                        Arc::clone(&self.state.fs),
                        sst_path_prefix,
                        self.state.is_memory_mode(),
                        self.state.observed_time_millis(),
                        self.read_resources.clone(),
                    )),
                },
            );
        }

        cache.publish(PublishedSnapshot {
            sequence: self.state.sequence,
            cf_snapshots,
        });

        if let Some(read_resources) = &self.read_resources {
            // Live names change only when the manifest is rebuilt into the
            // view cache; plain write batches never need to prune.
            let rebuilt = self.state.manifest.take_rebuilt_live_names();
            if let Some(live_names) = rebuilt {
                read_resources.prune_to_live_ssts(&live_names);
            }
        }
    }

    #[cfg(test)]
    fn publish_flushed_sst(
        &mut self,
        cf_id: crate::types::ColumnFamilyId,
        sst_name: &str,
        sequence: u64,
        file_meta: Option<crate::runtime::FileMeta>,
        _frozen_memtable: Option<&std::sync::Arc<crate::memtable::SkipListMemtable>>,
    ) -> crate::common::MidgeResult<()> {
        let Some(file_meta) = file_meta else {
            return Ok(());
        };
        if !self.state.manifest_has_file(sst_name) {
            self.manifest_actor.add_sst(&mut self.state, &file_meta)?;
        }
        self.state.transition_flush_publication_intent(
            sst_name,
            crate::runtime::PublicationPhase::ManifestPublished,
        )?;
        self.state.manifest.last_persisted_sequence =
            self.state.manifest.last_persisted_sequence.max(sequence);
        crate::runtime::actors::ManifestActor::persist(&mut self.state)?;
        self.state.clear_flush_publication_intent(sst_name)?;
        self.mirror_metadata_after_local_commit("test flush publication")?;
        self.publish_snapshot();
        self.schedule_compaction_after_flush_publication(sst_name);
        self.prune_cloud_wal_segments_covered_by_manifest();
        tracing::debug!(cf_id, sst_name, "test flush publication completed");
        Ok(())
    }

    pub(super) fn register_inline_response(
        &self,
        request_id: u64,
        response_tx: Sender<RuntimeResponse>,
    ) {
        self.inline_responses
            .borrow_mut()
            .insert(request_id, response_tx);
    }

    /// Helper: deliver a `RuntimeResponse` to the requester.
    #[inline]
    pub(super) fn respond(&self, request_id: u64, resp: RuntimeResponse) {
        let inline_tx = self.inline_responses.borrow_mut().remove(&request_id);
        if let Some(response_tx) = inline_tx {
            // A failed send means the inline caller already timed out and
            // dropped its receiver. That is the late-response signal for this
            // route, which the router's pending table never sees.
            if response_tx.send(resp).is_err() {
                self.router.record_late_response();
            }
        } else {
            self.router.complete(resp);
        }

        // Optional trace
        if self.trace_enabled {
            tracing::trace!(request_id, "response routed");
        }
    }

    pub(super) fn retry_gc(&mut self) -> HandleOutcome {
        let deadline = crate::common::OperationDeadline::from_budget(self.runtime_response_timeout);
        gc::GcCoordinator::retry_within(self, &deadline)
    }

    pub(super) fn retry_gc_within(
        &mut self,
        deadline: &crate::common::OperationDeadline,
    ) -> HandleOutcome {
        gc::GcCoordinator::retry_within(self, deadline)
    }
}

#[cfg(test)]
pub(super) mod tests;
