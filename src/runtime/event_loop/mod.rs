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
mod ingest;
mod manifest;
mod read_path;
mod resources;
mod shutdown;
mod snapshot;
mod verification;
mod wal;
mod wal_transition;
mod write_batch;

use crossbeam::channel::{Receiver, Sender, TryRecvError};
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
const SST_NAME_RESERVATION_BLOCK: u64 = 16;
const BACKGROUND_COMPACTION_CHECK_INTERVAL: Duration = Duration::from_secs(30);
const STARTUP_CLOUD_MAINTENANCE_DELAY: Duration = Duration::from_millis(100);
const HYBRID_STORAGE_POLL_INTERVAL: Duration = Duration::from_millis(5);

use super::actors::{CompactionActor, FlushActor, GcActor, ManifestActor, WalActor};
use super::durability::DurabilityCoordinator;
use super::read_resources::ReadResources;
use super::read_snapshot::ReadSnapshot;
use super::snapshot_cache::{CfSnapshotData, PublishedSnapshot, SnapshotCache};
use super::sst_read_view::SstReadViewCache;
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

use crate::runtime::hybrid_persistence::CloudWalPruneProgress;
use coordination::{
    CloudWalUploadTracker, ManifestPublicationGate, VerificationBarrier, WriteStallWaiters,
};

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
    pub(super) hybrid_storage: Option<Arc<crate::storage::HybridStorage>>,
    pub(super) hybrid_storage_events:
        Option<crossbeam::channel::Receiver<crate::storage::StorageEvent>>,
    pub(super) cloud_metadata_storage: Option<Arc<crate::storage::cloud::CloudStorage>>,
    pub(super) metadata_publication_lock: MetadataPublicationLock,
    pub(super) trace_enabled: bool,
    pub(super) loop_debug: bool,
    pub(super) loop_debug_wakes: u64,
    pub(super) loop_debug_batch_total: u64,
    pub(super) cloud_wal: CloudWalUploadTracker,
    cloud_wal_prune_worker: Option<std::thread::JoinHandle<()>>,
    cloud_wal_prune_progress: CloudWalPruneProgress,
    cloud_maintenance: cloud_maintenance::CloudMaintenance,
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
    /// Immutable per-CF SST indexes, rebuilt only after manifest mutations.
    sst_read_views: RefCell<SstReadViewCache>,

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
            hybrid_storage: None,
            hybrid_storage_events: config.hybrid_storage_events.clone(),
            cloud_metadata_storage: config.cloud_metadata_storage.clone(),
            metadata_publication_lock: config.metadata_publication_lock.clone(),
            trace_enabled,
            loop_debug: std::env::var_os("MIDGE_LOOP_DEBUG").is_some(),
            loop_debug_wakes: 0,
            loop_debug_batch_total: 0,
            cloud_wal: CloudWalUploadTracker::new(config.recovered_cloud_wal_segments.clone()),
            cloud_wal_prune_worker: None,
            cloud_wal_prune_progress: CloudWalPruneProgress::default(),
            cloud_maintenance: cloud_maintenance::CloudMaintenance::default(),
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
            sst_read_views: RefCell::new(SstReadViewCache::new()),
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
        state.eventual_flush_segment_gap = config.cloud_runtime_policy.eventual_flush_segment_gap;
        state.set_compaction_enabled(config.background_compaction);
        state.l0_compaction_trigger = config.l0_compaction_trigger.max(1);
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
        self.hybrid_storage.as_ref().ok_or_else(|| {
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
            .contiguous_acked_cloud_segments(&self.cloud_wal.acked_segments)
            .map_err(crate::common::MidgeError::RecoveryFailed)?;
        if let Some((_, max_sequence)) = initially_durable.last() {
            self.state.wal.cloud_durable_seq = self.state.wal.cloud_durable_seq.max(*max_sequence);
        }
        for (segment_id, _) in initially_durable {
            self.wal_transition.note_cloud_durable(segment_id)?;
            self.durability.retire_cloud_segment(segment_id);
            if self.remove_cloud_durable_local_wal_segment(segment_id) {
                self.wal_transition.retire_cloud_durable(segment_id)?;
            }
        }

        for (&segment_id, segment) in local_segments {
            self.cloud_wal
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
        self.hybrid_storage = Some(storage);
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
            let sst_view = self
                .sst_read_views
                .borrow_mut()
                .view_for(&self.state.manifest, cf_id);
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
            let rebuilt = self
                .sst_read_views
                .borrow_mut()
                .take_rebuilt_live_names(&self.state.manifest);
            if let Some(live_names) = rebuilt {
                read_resources.prune_to_live_ssts(&live_names);
            }
        }
    }

    fn invalidate_sst_read_views(&self) {
        self.sst_read_views.borrow_mut().invalidate();
    }

    fn assign_compaction_output_sequence(
        &mut self,
        mut plan: crate::compaction::CompactionPlan,
    ) -> crate::common::MidgeResult<crate::compaction::CompactionPlan> {
        if plan.output_seq == 0 {
            plan.output_seq = self.state.next_compaction_output_generation()?;
        }
        if self.hybrid_storage.is_some() {
            // Early remote output staging can leave harmless orphans after a
            // crash. Persist the filename allocation before any such object
            // is uploaded so a cold replacement never reuses its identity.
            self.reserve_sst_name_durably(plan.cf_id, plan.output_seq)?;
        }
        Ok(plan)
    }

    /// Persist an SST filename allocation before the object can exist
    /// remotely. After a crash between upload and publication, a replacement
    /// must never reuse the orphan's name: immutable publication rejects an
    /// existing object with different bytes, which would wedge it forever.
    ///
    /// A name is covered only once its reservation is both journaled and
    /// mirrored by this session (`SstNameAllocation::reserved_through`).
    /// `manifest.next_sst_seqs` alone is not enough: a failed mirror leaves it
    /// raised locally while cloud metadata never saw it. Otherwise reserve a
    /// block of names with one journal append and one mirror, so the next
    /// flushes reserve nothing. The journal replays on open, so no snapshot is
    /// needed (#491).
    pub(super) fn reserve_sst_name_durably(
        &mut self,
        cf_id: crate::types::ColumnFamilyId,
        sst_seq: u64,
    ) -> crate::common::MidgeResult<()> {
        let reserved_through = self
            .state
            .sst_names
            .reserved_through
            .get(&cf_id)
            .copied()
            .unwrap_or(0);
        if sst_seq < reserved_through {
            return Ok(());
        }
        // The mirror below publishes local metadata files; never publish
        // them while memory is known to be behind disk (#500). Reload first,
        // so the counter read next is the reloaded one.
        self.state.retry_metadata_reload()?;
        let durable_next = self
            .state
            .manifest
            .next_sst_seqs
            .get(&cf_id)
            .copied()
            .unwrap_or(1);
        // Never lower the counter: the edit replays as a max, and memory must
        // match what the journal replays.
        let next_seq = sst_seq
            .checked_add(SST_NAME_RESERVATION_BLOCK)
            .ok_or_else(|| {
                crate::common::MidgeError::ResourceLimit("SST filename allocation exhausted".into())
            })?
            .max(durable_next);
        let edit_id = self
            .state
            .manifest_store
            .append(&crate::metadata::ManifestEdit::BumpNextSstSeq { cf_id, next_seq })?;
        self.state.manifest.next_sst_seqs.insert(cf_id, next_seq);
        self.state.manifest.note_applied_journal_edit(edit_id);
        self.mirror_metadata_to_authoritative_cloud()?;
        self.state
            .sst_names
            .reserved_through
            .insert(cf_id, next_seq);
        Ok(())
    }

    fn prepare_compaction_plan_for_launch(
        &mut self,
        plan: crate::compaction::CompactionPlan,
    ) -> crate::common::MidgeResult<crate::compaction::CompactionPlan> {
        let memory_limit = self.available_compaction_memory()?;
        let mut plan = self.assign_compaction_output_sequence(plan)?;
        plan.snapshot_horizon = self.state.oldest_active_snapshot_sequence();
        plan.target_sst_size = self.compaction_actor.target_sst_size();
        plan.compaction_memory_limit = memory_limit;

        if plan.output_seq == 0 {
            return Err(crate::common::MidgeError::Internal(
                "BUG: compaction output sequence was not assigned before actor launch".to_string(),
            ));
        }

        Ok(plan)
    }

    fn available_compaction_memory(&self) -> crate::common::MidgeResult<usize> {
        if self.cloud_wal_prune_worker.is_some() || self.publication_gate.is_active() {
            return Err(crate::common::MidgeError::Busy(
                "compaction memory is owned by an active publication turn".into(),
            ));
        }
        let retained = self
            .cloud_wal_prune_progress
            .retained_bytes()
            .ok_or_else(|| {
                crate::common::MidgeError::Busy("WAL retirement proof is still active".into())
            })?;
        // Paused checksum/cursor proofs outlive their worker. Execution and
        // publication share the remaining configured allowance without
        // discarding the proof work that the next retirement turn will resume.
        let available = self
            .compaction_actor
            .compaction_memory_limit()
            .saturating_sub(retained);
        if available == 0 {
            return Err(crate::common::MidgeError::ResourceLimit(
                "retained WAL retirement proofs leave no memory for compaction".into(),
            ));
        }
        Ok(available)
    }

    fn launch_compaction(
        &mut self,
        plan: crate::compaction::CompactionPlan,
    ) -> crate::common::MidgeResult<()> {
        if self.publication_gate.is_active() {
            return Err(crate::common::MidgeError::Busy(
                "manifest publication is already in progress".to_string(),
            ));
        }
        if self.compaction_publication_degraded {
            return Err(crate::common::MidgeError::Fenced(
                "compaction publication is unsettled; refusing another compaction until recovery"
                    .into(),
            ));
        }
        self.state.retry_metadata_reload()?;
        let plan = self.prepare_compaction_plan_for_launch(plan)?;

        let compaction_storage = self.hybrid_storage.as_ref().map(|storage| {
            Arc::clone(storage) as Arc<dyn super::actors::compaction::CompactionStorage>
        });
        self.compaction_actor
            .run_compaction(
                &mut self.state,
                &plan,
                compaction_storage.as_ref(),
                self.worker_msg_tx.clone(),
            )
            .map(|_| ())
            .map_err(|error| crate::common::MidgeError::Internal(error.to_string()))
    }

    fn schedule_one_background_compaction_if_needed(
        &mut self,
        operation: &str,
    ) -> crate::common::MidgeResult<bool> {
        if self.cloud_maintenance_enabled() && !self.cloud_maintenance.dispatching {
            return Ok(self.schedule_cloud_maintenance()
                == Some(cloud_maintenance::MaintenanceTask::Compaction));
        }
        let manual = self.cloud_maintenance_enabled()
            && compaction::CompactionCoordinator::has_manual_compaction_waiters(self);
        let result = self.schedule_background_compaction_plan(operation, manual);
        if manual {
            match &result {
                Ok(false) => {
                    compaction::CompactionCoordinator::complete_idle_compaction_waits(self, true);
                }
                Err(error) => {
                    compaction::CompactionCoordinator::fail_pending_compaction_waits(self, error);
                }
                Ok(true) => {}
            }
        }
        result
    }

    fn schedule_background_compaction_plan(
        &mut self,
        operation: &str,
        manual: bool,
    ) -> crate::common::MidgeResult<bool> {
        // Disabling ordinary background work must not permanently wedge L0
        // admission. Use the same authority, ingest, and worker gates for
        // pressure recovery at startup, after flush, and during live maintenance.
        let background_enabled = self.state.compaction_enabled();
        if !background_enabled && !manual && !self.state.has_any_critical_l0_debt() {
            return Ok(false);
        }
        if self.fencing.ddl_authority_ambiguous {
            return Err(crate::common::MidgeError::Fenced(
                "DDL authority is ambiguous; refusing compaction until reconciliation".into(),
            ));
        }
        let timed_out = self.state.warn_timed_out_snapshots();
        if timed_out > 0 {
            tracing::warn!(
                timed_out,
                operation,
                "Observed timed-out snapshots before compaction check; retaining pins"
            );
        }

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
                operation,
                "BUG: compaction scheduling attempted while ingest mode is active. \
                 Violated invariant: compaction must not be scheduled during ingest. \
                 Correct ordering: complete all compactions BEFORE begin_ingest."
            );
            return Err(crate::common::MidgeError::Internal(
                "BUG: compaction scheduling attempted during ingest mode — violated invariant"
                    .to_string(),
            ));
        }

        let planned = if background_enabled && !manual {
            self.compaction_actor.check_compaction(&self.state)
        } else {
            self.compaction_actor.check_manual_compaction(&self.state)
        };
        let Some(plan) = planned.inspect_err(|_| self.state.mark_persistence_anomaly())? else {
            return Ok(false);
        };

        self.launch_compaction(plan)?;
        Ok(true)
    }

    fn schedule_compaction_after_flush_publication(&mut self, sst_name: &str) {
        match self.schedule_one_background_compaction_if_needed("flush publication") {
            Ok(true) => tracing::debug!(
                sst_name,
                "Scheduled background compaction after flush publication"
            ),
            Ok(false) => {}
            Err(error) => tracing::warn!(
                %error,
                sst_name,
                "Skipping automatic compaction after flush publication"
            ),
        }
    }

    fn background_maintenance_timeout(&self) -> Duration {
        self.next_background_compaction_check
            .saturating_duration_since(Instant::now())
    }

    fn run_background_compaction_maintenance_if_due(&mut self) {
        if self.background_maintenance_timeout() != Duration::ZERO {
            return;
        }

        self.next_background_compaction_check =
            Instant::now() + BACKGROUND_COMPACTION_CHECK_INTERVAL;
        match self.backfill_one_legacy_sst_bounds() {
            Ok(true) => {
                // Continue migrating one file per event-loop turn without
                // making one maintenance invocation proportional to catalog
                // size.
                self.next_background_compaction_check =
                    Instant::now() + STARTUP_CLOUD_MAINTENANCE_DELAY;
            }
            Ok(false) => {}
            Err(error) => {
                tracing::warn!(%error, "SST key-bound backfill maintenance failed; retaining conservative read fallback");
            }
        }
        if !self
            .state
            .ingest_active
            .load(std::sync::atomic::Ordering::Acquire)
        {
            match self.schedule_one_background_compaction_if_needed("periodic maintenance") {
                Ok(true) => tracing::debug!("Scheduled background compaction during maintenance"),
                Ok(false) => {}
                Err(error) => {
                    tracing::warn!(%error, "Background compaction maintenance check failed");
                }
            }
        }
        self.prune_cloud_wal_segments_covered_by_manifest();
    }

    pub(super) fn schedule_background_compaction_on_startup(&mut self) {
        self.next_background_compaction_check = Instant::now() + STARTUP_CLOUD_MAINTENANCE_DELAY;
        match self.schedule_one_background_compaction_if_needed("runtime startup") {
            Ok(true) => tracing::debug!("Scheduled compaction during runtime startup"),
            Ok(false) => {}
            Err(error) => tracing::warn!(%error, "Startup background compaction check failed"),
        }
    }

    /// Local copies are disposable only after the remote manifest publication
    /// has completed. Snapshot readers pin remote objects, not these files.
    fn evict_published_sst_cache(&self, names: &[String]) {
        let Some(storage) = &self.hybrid_storage else {
            return;
        };
        for name in names {
            let path = self.state.sst_dir.join(name);
            let size = std::fs::metadata(&path).ok().map(|metadata| metadata.len());
            match std::fs::remove_file(&path) {
                Ok(()) => storage.release_local_sst_bytes(size.unwrap_or(0)),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    tracing::warn!(%error, sst_name = name, "retaining local SST cache after failed eviction");
                }
            }
            if let Err(error) =
                storage.evict_local_object_cache(&crate::cloud_layout::object_key(name))
            {
                tracing::warn!(%error, sst_name = name, "retaining secondary SST cache after failed eviction");
            }
        }
    }

    fn mirror_metadata_to_authoritative_cloud(&self) -> crate::common::MidgeResult<()> {
        self.mirror_metadata_to_authoritative_cloud_within(&self.event_loop_cloud_deadline())
    }

    pub(super) fn mirror_metadata_to_authoritative_cloud_within(
        &self,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<()> {
        let Some(cloud) = self.cloud_metadata_storage.as_ref() else {
            return Ok(());
        };
        crate::runtime::hybrid_persistence::mirror_control_metadata_within(
            cloud,
            self.state.fs.as_ref(),
            &self.metadata_publication_lock,
            std::time::Duration::ZERO,
            self.state.manifest.last_persisted_sequence,
            deadline,
            |deadline| self.validate_runtime_writer_lease_within(deadline),
        )
    }

    fn mirror_metadata_after_local_commit(
        &mut self,
        context: &str,
    ) -> crate::common::MidgeResult<()> {
        let deadline = self.event_loop_cloud_deadline();
        self.mirror_metadata_after_local_commit_within(context, &deadline)
    }

    fn mirror_metadata_after_local_commit_within(
        &mut self,
        context: &str,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<()> {
        match self.mirror_metadata_to_authoritative_cloud_within(deadline) {
            Ok(()) => Ok(()),
            Err(error)
                if self.state.recovery_policy() == crate::config::RecoveryPolicy::Salvage =>
            {
                self.state.mark_persistence_anomaly();
                tracing::warn!(%error, context, "cloud metadata mirror failed during salvage-capable operation");
                Ok(())
            }
            Err(error) => Err(error),
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
            self.manifest_actor.add_sst(&mut self.state, file_meta)?;
            self.invalidate_sst_read_views();
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

    pub(super) fn defer_verification_message(&mut self, message: RuntimeMsg) {
        let is_duplicate_maintenance = matches!(message, RuntimeMsg::RetryGc)
            && self
                .verification_barrier
                .deferred_messages
                .iter()
                .any(|pending| matches!(pending, RuntimeMsg::RetryGc));
        let is_duplicate_drop_shutdown = matches!(message, RuntimeMsg::Shutdown)
            && self
                .verification_barrier
                .deferred_messages
                .iter()
                .any(|pending| matches!(pending, RuntimeMsg::Shutdown));
        if !is_duplicate_maintenance && !is_duplicate_drop_shutdown {
            self.verification_barrier
                .deferred_messages
                .push_back(message);
        }
    }

    pub(super) fn begin_storage_verification(&mut self, request_id: u64) -> HandleOutcome {
        let active_compactions = self
            .state
            .active_compactions
            .load(std::sync::atomic::Ordering::Acquire);
        let layout_is_changing = active_compactions > 0
            || !self.state.compaction.compacting_ssts.is_empty()
            || self.flush_actor.is_inflight()
            || self.publication_gate.is_active();
        if self.verification_barrier.token.is_some() || layout_is_changing {
            self.respond(
                request_id,
                RuntimeResponse::Error {
                    request_id,
                    error: crate::common::MidgeError::Busy(
                        "storage layout is busy or already being verified".to_string(),
                    ),
                },
            );
            return HandleOutcome::Continue;
        }

        let activated = self.verification_barrier.activate(request_id);
        debug_assert!(activated);
        crate::failpoints::fail_point!("midge::verification::before_barrier_response");
        self.respond(
            request_id,
            RuntimeResponse::StorageVerificationBarrier {
                request_id,
                token: request_id,
                health: self.state.health(),
            },
        );
        HandleOutcome::Continue
    }

    pub(super) fn end_storage_verification(
        &mut self,
        request_id: u64,
        token: u64,
    ) -> HandleOutcome {
        if self.verification_barrier.token != Some(token) {
            self.respond(
                request_id,
                RuntimeResponse::Error {
                    request_id,
                    error: crate::common::MidgeError::InvalidArgument(
                        "storage verification barrier token does not match".to_string(),
                    ),
                },
            );
            return HandleOutcome::Continue;
        }

        let deferred = self.verification_barrier.release(token);
        let retirements = self.verification_barrier.take_deferred_wal_retirements();
        if !retirements.is_empty() {
            self.retire_acked_local_wal_segments(&retirements);
        }
        for event in self.verification_barrier.take_deferred_storage_events() {
            self.handle_storage_event(event);
        }
        if self.pending_msg.is_none() {
            self.pending_msg = deferred;
        }
        self.respond(request_id, RuntimeResponse::Ok { request_id });
        HandleOutcome::Continue
    }

    fn has_actionable_work(&self) -> bool {
        if self.pending_msg.is_some() {
            return true;
        }

        if self.verification_barrier.token.is_some() {
            // Verification freezes layout maintenance, but group-commit fsync
            // does not change the layout being verified.
            let due_sync = self.wal_actor.should_sync_batch()
                && (self.wal_actor.has_pending_data() || self.durability.has_pending_waiters());
            return due_sync
                || self
                    .hybrid_storage_events
                    .as_ref()
                    .is_some_and(|rx| !rx.is_empty());
        }

        if self
            .cloud_wal_prune_worker
            .as_ref()
            .is_some_and(std::thread::JoinHandle::is_finished)
        {
            return true;
        }

        if !self.flush_worker_result_rx.is_empty() {
            return true;
        }

        if !self.compaction_publish_result_rx.is_empty() {
            return true;
        }

        if self.wal_actor.should_sync_batch() {
            return true;
        }

        if self.durability.cloud_seal_retry_due() && self.state.wal.pending_writes > 0 {
            return true;
        }

        if self.cloud_wal.uploads_ready() {
            return true;
        }

        if self.background_maintenance_timeout() == Duration::ZERO {
            return true;
        }

        if self.state.has_due_immutable_flush() && !self.flush_start_blocked(false) {
            return true;
        }

        if let Some(rx) = &self.hybrid_storage_events {
            if !rx.is_empty() {
                return true;
            }
        }

        false
    }

    fn idle_progress_timeout(&self) -> Option<Duration> {
        if self.verification_barrier.is_active() {
            // Verification deliberately freezes maintenance. Ignoring due
            // retry deadlines here makes the run loop block for the release
            // message instead of repeatedly timing out at zero duration. The
            // batched WAL sync deadline still applies.
            return self.wal_actor.sync_deadline_timeout();
        }

        [
            self.wal_actor.sync_deadline_timeout(),
            self.durability
                .cloud_seal_deadline_timeout(self.state.wal.pending_writes),
            self.gc_actor.retry_deadline_timeout(),
            self.cloud_wal.upload_retry_deadline_timeout(),
            self.hybrid_storage.as_ref().and_then(|storage| {
                (storage.pending_upload_count() > 0).then_some(HYBRID_STORAGE_POLL_INTERVAL)
            }),
            // Terminal storage events can arrive before the prune thread
            // exits. Keep observing the handle after its last event so the
            // publication gate and queued manual requests cannot lose a wake.
            self.cloud_wal_prune_worker
                .as_ref()
                .map(|_| HYBRID_STORAGE_POLL_INTERVAL),
            self.state.flush_retry_deadline_timeout(),
            self.flush_actor
                .is_inflight()
                .then_some(Duration::from_millis(1)),
            self.compaction_publish_actor
                .is_inflight()
                .then_some(Duration::from_millis(1)),
            Some(self.background_maintenance_timeout()),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    fn progress_pass(&mut self, msg_rx: &Receiver<RuntimeMsg>) {
        if self.verification_barrier.token.is_some() {
            // Mutations stay deferred behind the barrier, so sync only what
            // is already in the WAL; do not drain queued writes into it.
            // Cloud acknowledgements still complete their waiters.
            self.sync_batched_wal_without_draining();
            self.drain_hybrid_storage_events();
            return;
        }
        self.background_progress(Some(msg_rx));
    }

    /// The one list of background progress steps. The idle pass and the
    /// request fairness slot both run it, so a busy queue cannot starve a
    /// step the idle pass would have run. The idle pass may drain queued
    /// writes into its batched sync; the fairness slot runs after dispatch
    /// and leaves the queue in order.
    fn background_progress(&mut self, drain_writes_from: Option<&Receiver<RuntimeMsg>>) {
        self.drain_compaction_publish_results();
        self.drain_flush_worker_results();
        match drain_writes_from {
            Some(msg_rx) => self.sync_batched_wal_if_needed(msg_rx),
            None => self.sync_batched_wal_without_draining(),
        }
        self.maybe_flush_cloud_async_wal();
        self.drain_cloud_wal_upload_backlog();
        self.tick_hybrid_storage();
        self.drain_hybrid_storage_events();
        self.drain_cloud_wal_upload_backlog();
        let hybrid_storage = self.hybrid_storage.clone();
        self.gc_actor
            .retry_failed_cloud_deletes_if_due(&mut self.state, hybrid_storage);
        self.retry_manifest_reclamation_if_due();
        self.drain_auto_flush_memtables();
        self.run_background_compaction_maintenance_if_due();
    }

    fn retry_manifest_reclamation_if_due(&mut self) {
        if !self.gc_actor.manifest_reclamation_retry_due() {
            return;
        }

        // Do not interleave with a flush/prune publication snapshot. Deferring
        // re-arms the idle wakeup instead of turning a busy publication gate
        // into a spin loop.
        if self.publication_gate.is_active() {
            self.gc_actor.defer_manifest_reclamation_retry();
            return;
        }

        let deadline = crate::common::OperationDeadline::from_budget(self.runtime_response_timeout);
        let _ = self.retry_gc_within(&deadline);
    }

    fn record_wake_batch(&mut self, batch: usize) {
        self.state.diagnostics.record(|m| {
            m.record_event_loop_wake();
            m.record_event_loop_batch(batch as u64);
        });

        if self.loop_debug {
            const LOOP_DEBUG_EVERY: u64 = 256;
            self.loop_debug_wakes += 1;
            self.loop_debug_batch_total += batch as u64;

            if self.loop_debug_wakes.is_multiple_of(LOOP_DEBUG_EVERY) {
                let avg_batch = self
                    .loop_debug_batch_total
                    .to_string()
                    .parse::<f64>()
                    .unwrap_or(0.0)
                    / self
                        .loop_debug_wakes
                        .to_string()
                        .parse::<f64>()
                        .unwrap_or(1.0);
                eprintln!(
                    "[midge] loop_stats wakes={} avg_batch={:.2}",
                    self.loop_debug_wakes, avg_batch
                );
            }
        }
    }

    fn process_one(&mut self, msg: RuntimeMsg, msg_rx: &Receiver<RuntimeMsg>) -> HandleOutcome {
        if self.verification_barrier.token.is_none() && msg.is_mutation() {
            self.drain_compaction_publish_results();
            self.drain_flush_worker_results();
            self.maybe_flush_cloud_async_wal();
            self.tick_hybrid_storage();
            self.drain_hybrid_storage_events();
            self.run_background_compaction_maintenance_if_due();
        }
        let outcome = self.handle_runtime_msg(msg, msg_rx);
        if outcome == HandleOutcome::Continue && self.verification_barrier.token.is_none() {
            self.run_request_fairness_slot();
        }
        outcome
    }

    fn process_restored_one(
        &mut self,
        msg: RuntimeMsg,
        msg_rx: &Receiver<RuntimeMsg>,
    ) -> HandleOutcome {
        // A restored message owns the publication turn that just became
        // available. Running maintenance before dispatch can start another
        // flush and re-defer the same request forever under steady flush debt.
        let outcome = self.handle_runtime_msg(msg, msg_rx);
        if outcome == HandleOutcome::Continue && self.verification_barrier.token.is_none() {
            self.run_request_fairness_slot();
        }
        outcome
    }

    pub(super) fn run_request_fairness_slot(&mut self) {
        // A continuously non-empty request queue must not starve background
        // durability and storage progress. Run this bounded slot only after
        // dispatch so a restored control request keeps the publication turn
        // that made it eligible.
        self.background_progress(None);
    }

    pub(super) fn handle_runtime_msg(
        &mut self,
        msg: RuntimeMsg,
        msg_rx: &Receiver<RuntimeMsg>,
    ) -> HandleOutcome {
        dispatch::RuntimeDispatcher::handle(self, msg, msg_rx)
    }

    fn process_wake_msg(
        &mut self,
        msg: RuntimeMsg,
        msg_rx: &Receiver<RuntimeMsg>,
        max_drain: usize,
    ) -> HandleOutcome {
        let mut batch = 1usize;
        let outcome = self.process_one(msg, msg_rx);

        if outcome == HandleOutcome::Break {
            self.record_wake_batch(batch);
            return outcome;
        }

        let drained = if self.verification_barrier.token.is_some() || self.pending_msg.is_some() {
            0
        } else {
            self.drain_pending_writes(msg_rx, max_drain)
        };
        batch += drained;
        self.record_wake_batch(batch);
        outcome
    }

    fn restore_verification_deferred_message(&mut self) {
        if !self.shutting_down
            && self.verification_barrier.token.is_none()
            && self.pending_msg.is_none()
        {
            self.pending_msg = self.verification_barrier.deferred_messages.pop_front();
        }
    }

    pub(super) fn restore_publication_deferred_message(&mut self) {
        if !self.shutting_down
            && !self.publication_gate.is_active()
            && self.verification_barrier.token.is_none()
            && self.pending_msg.is_none()
        {
            if let Some(index) = self.publication_gate.next_restorable_index(|cf_id| {
                self.column_family_publication_pipeline_active(cf_id)
            }) {
                self.pending_msg = self.publication_gate.finish_at(index);
            }
        }
    }

    fn run_actionable_pass(
        &mut self,
        msg_rx: &Receiver<RuntimeMsg>,
        max_drain_writes: usize,
    ) -> Option<HandleOutcome> {
        if !self.has_actionable_work() {
            return None;
        }
        match msg_rx.try_recv() {
            Ok(msg) => return Some(self.process_wake_msg(msg, msg_rx, max_drain_writes)),
            Err(TryRecvError::Disconnected) => return Some(HandleOutcome::Break),
            Err(TryRecvError::Empty) => {}
        }
        if let Some(storage_rx) = &self.hybrid_storage_events {
            if let Ok(event) = storage_rx.try_recv() {
                self.handle_storage_event(event);
                return Some(HandleOutcome::Continue);
            }
        }
        self.progress_pass(msg_rx);
        std::thread::sleep(Duration::from_micros(50));
        Some(HandleOutcome::Continue)
    }

    /// Main event loop — runs until Shutdown message or channel close.
    pub fn run(&mut self, msg_rx: &Receiver<RuntimeMsg>, worker_msg_rx: &Receiver<RuntimeMsg>) {
        // Bound write coalescing by a fairness quantum. Thousands of local
        // writes are cheap, but the same wake on cloud durability can consume
        // an entire control-request deadline before yielding.
        const MAX_DRAIN_WRITES_ON_WAKE: usize = 64;

        loop {
            if let Ok(message) = worker_msg_rx.try_recv() {
                if dispatch::RuntimeDispatcher::handle(self, message, msg_rx)
                    == HandleOutcome::Break
                {
                    break;
                }
                continue;
            }
            self.restore_verification_deferred_message();
            self.restore_publication_deferred_message();
            if let Some(pending) = self.pending_msg.take() {
                let outcome = self.process_restored_one(pending, msg_rx);
                if outcome == HandleOutcome::Break {
                    break;
                }
                continue;
            }

            if let Some(outcome) = self.run_actionable_pass(msg_rx, MAX_DRAIN_WRITES_ON_WAKE) {
                if outcome == HandleOutcome::Break {
                    break;
                }
                continue;
            }

            let idle_timeout = self.idle_progress_timeout();
            // Storage events are selected under the barrier too: the handler
            // takes only the ones that cannot change the verified layout and
            // defers the rest.
            let selectable_storage_rx = self.hybrid_storage_events.clone();
            let msg = if let Some(storage_rx) = selectable_storage_rx {
                if let Some(timeout) = idle_timeout {
                    crossbeam::channel::select! {
                        recv(worker_msg_rx) -> msg => msg.ok(),
                        recv(msg_rx) -> msg => msg.ok(),
                        recv(storage_rx) -> ev => {
                            match ev {
                                Ok(ev) => {
                                    self.handle_storage_event(ev);
                                }
                                Err(_) => {
                                    self.hybrid_storage_events = None;
                                }
                            }
                            continue;
                        }
                        default(timeout) => {
                            self.progress_pass(msg_rx);
                            continue;
                        }
                    }
                } else {
                    crossbeam::channel::select! {
                        recv(worker_msg_rx) -> msg => msg.ok(),
                        recv(msg_rx) -> msg => msg.ok(),
                        recv(storage_rx) -> ev => {
                            match ev {
                                Ok(ev) => {
                                    self.handle_storage_event(ev);
                                }
                                Err(_) => {
                                    self.hybrid_storage_events = None;
                                }
                            }
                            continue;
                        }
                    }
                }
            } else if let Some(timeout) = idle_timeout {
                crossbeam::channel::select! {
                    recv(worker_msg_rx) -> msg => msg.ok(),
                    recv(msg_rx) -> msg => msg.ok(),
                    default(timeout) => {
                        self.progress_pass(msg_rx);
                        continue;
                    }
                }
            } else {
                crossbeam::channel::select! {
                    recv(worker_msg_rx) -> msg => msg.ok(),
                    recv(msg_rx) -> msg => msg.ok(),
                }
            };

            let Some(msg) = msg else {
                break;
            };

            let outcome = self.process_wake_msg(msg, msg_rx, MAX_DRAIN_WRITES_ON_WAKE);
            if outcome == HandleOutcome::Break {
                break;
            }
        }

        tracing::debug!("Runtime message channel closed — exiting event loop");
    }
}

#[cfg(test)]
pub(super) mod tests;
