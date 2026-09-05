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

use crate::runtime::hybrid_persistence::HybridPersistence;

#[cfg(test)]
mod cloud;
mod cloud_integration;
mod cloud_memtable_admission;
mod compaction;
mod control;
mod coordination;
mod dispatch;
mod durability_sync;
mod flush;
mod flush_pipeline;
mod gc;
mod ingest;
mod manifest;
mod read_path;
mod shutdown;
mod snapshot;
mod verification;
mod wal;
mod write_batch;

use crossbeam::channel::{Receiver, Sender, TryRecvError};
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

const BACKGROUND_COMPACTION_CHECK_INTERVAL: Duration = Duration::from_secs(30);
const STARTUP_CLOUD_MAINTENANCE_DELAY: Duration = Duration::from_millis(100);
const HYBRID_STORAGE_POLL_INTERVAL: Duration = Duration::from_millis(5);

#[cfg(test)]
use super::actors::CloudActor;
use super::actors::{CompactionActor, FlushActor, GcActor, ManifestActor, WalActor};
use super::durability::DurabilityCoordinator;
use super::read_resources::ReadResources;
use super::read_snapshot::ReadSnapshot;
use super::snapshot_cache::{CfSnapshotData, PublishedSnapshot, SnapshotCache};
use super::sst_read_view::SstReadViewCache;
use super::state::RuntimeState;
use super::{ResponseRouter, RuntimeMsg, RuntimeResponse};
use crate::runtime::actors::flush::FlushWorkerResult;

type SstRuntimeResources = (Arc<dyn crate::sst::SstFactory>, Option<Arc<ReadResources>>);

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

use coordination::{CloudWalUploadTracker, ManifestPublicationGate, VerificationBarrier};

/// Main synchronous event loop for the runtime.
///
/// Owns all actors and is responsible for routing inbound messages.
#[allow(clippy::struct_excessive_bools)]
pub struct EventLoop {
    pub(super) state: RuntimeState,

    // Actors
    pub(super) flush_actor: FlushActor,
    pub(super) compaction_actor: CompactionActor,
    pub(super) wal_actor: WalActor,
    #[cfg(test)]
    pub(super) cloud_actor: CloudActor,
    gc_actor: GcActor,
    manifest_actor: ManifestActor,
    pub(super) hybrid_storage: Option<Arc<crate::storage::HybridStorage>>,
    pub(super) hybrid_storage_events:
        Option<crossbeam::channel::Receiver<crate::storage::StorageEvent>>,
    pub(super) cloud_metadata_storage: Option<Arc<crate::storage::cloud::CloudStorage>>,
    pub(super) trace_enabled: bool,
    pub(super) loop_debug: bool,
    pub(super) loop_debug_wakes: u64,
    pub(super) loop_debug_batch_total: u64,
    pub(super) cloud_wal: CloudWalUploadTracker,
    cloud_wal_prune_worker: Option<std::thread::JoinHandle<()>>,
    next_background_compaction_check: Instant,

    // Durability coordination (extracted to reduce EventLoop cognitive load)
    pub(super) durability: DurabilityCoordinator,

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
    publication_gate: ManifestPublicationGate,

    pub(super) flush_worker_result_rx: crossbeam::channel::Receiver<FlushWorkerResult>,
    flush_barrier_waiters: HashMap<crate::types::ColumnFamilyId, Vec<flush::FlushBarrierWaiter>>,
    inline_flush_worker: bool,
    shutting_down: bool,
    shutdown_cloud_drain_timeout: Duration,

    /// Sender that worker threads can use to post back completion messages
    /// (compaction threads will use this to report completion).
    pub(super) worker_msg_tx: Option<crossbeam::channel::Sender<RuntimeMsg>>,

    /// Waiters blocked on write stall clearing (`request_id` -> `cf_id`).
    pub(super) write_stall_waiters: HashMap<u64, crate::types::ColumnFamilyId>,
    /// FIFO queues of waiters per CF.
    pub(super) write_stall_waiter_queues: HashMap<crate::types::ColumnFamilyId, VecDeque<u64>>,
    /// Lock-free snapshot cache shared with Engine for read-path bypass.
    pub(super) snapshot_cache: Option<Arc<SnapshotCache>>,
    /// Shared SST readers and block cache used by runtime read snapshots.
    pub(super) read_resources: Option<Arc<ReadResources>>,
    /// Immutable per-CF SST indexes, rebuilt only after manifest mutations.
    sst_read_views: RefCell<SstReadViewCache>,

    /// Shared flag from the lease heartbeat. When `false`, the event loop
    /// rejects new write operations with `MidgeError::Fenced`.
    lease_healthy: Option<Arc<std::sync::atomic::AtomicBool>>,
    /// A remote DDL CAS may have committed even when its response and the
    /// authority re-read both fail. Fence writes/publication until the durable
    /// prepare can be reconciled instead of accepting work into a stale CF.
    ddl_authority_ambiguous: bool,
    /// A compaction manifest authority switch completed, but its publication
    /// intent could not be advanced or settled. Further compaction could
    /// consume that output and make restart recovery ambiguous, so compaction
    /// remains fenced until reopen replays the durable intent.
    compaction_publication_degraded: bool,
    /// Terminal publication error for the just-completed compaction. This is
    /// forwarded to a pending `compact_all()` waiter after authority handling.
    last_compaction_publication_error: Option<crate::common::MidgeError>,
    writer_epoch: u64,
    /// Response budget a caller is given for one runtime request. Cloud work
    /// performed on a caller's behalf shares this budget rather than restarting
    /// a fresh `storage_io_timeout` per round trip.
    runtime_response_timeout: std::time::Duration,
    leader_store: Option<Arc<dyn crate::lease::LeaderStore>>,
    leader_holder_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HandleOutcome {
    Continue,
    Break,
}

impl EventLoop {
    fn initialize_sst_resources(
        state: &RuntimeState,
        sst_dir: &std::path::Path,
        memory_mode: bool,
        config: &super::RuntimeConfig,
    ) -> crate::common::MidgeResult<SstRuntimeResources> {
        let sst_factory: Arc<dyn crate::sst::SstFactory> = if memory_mode {
            let fs = Arc::new(crate::io::MockFs::new());
            Arc::new(
                crate::sst::FsSstFactoryIo::new(fs, 64 * 1024)
                    .with_compression_policy(config.compression_policy.clone()),
            )
        } else {
            let fs: Arc<dyn crate::io::Fs> = match &config.sst_read_fs {
                Some(fs) => Arc::clone(fs),
                None => Arc::new(crate::io::RealFs::new(sst_dir)?),
            };
            Arc::new(
                crate::sst::FsSstFactoryIo::new(fs, 64 * 1024)
                    .with_compaction_scratch_directory(sst_dir.join(".flush-staging"))
                    .with_compression_policy(config.compression_policy.clone()),
            )
        };
        let read_resources = if memory_mode {
            None
        } else {
            let sst_path_prefix = sst_dir
                .strip_prefix(&state.db_path)
                .unwrap_or_else(|_| std::path::Path::new("sst"))
                .to_path_buf();
            Some(Arc::new(ReadResources::new_with_diagnostics(
                config
                    .sst_read_fs
                    .clone()
                    .unwrap_or_else(|| Arc::clone(&state.fs)),
                sst_path_prefix,
                config.block_cache_size,
                config.block_cache_policy,
                Arc::clone(&state.diagnostics),
            )))
        };
        Ok((sst_factory, read_resources))
    }

    pub(crate) fn new(
        mut state: RuntimeState,
        trace_enabled: bool,
        router: Arc<ResponseRouter>,
        config: super::RuntimeConfig,
        worker_msg_tx: Option<crossbeam::channel::Sender<super::RuntimeMsg>>,
    ) -> crate::common::MidgeResult<Self> {
        let wal_dir = state.wal_dir.clone();
        let sst_dir = state.sst_dir.clone();
        let memory_mode = state.is_memory_mode();
        let initial_segment_id = state.wal.current_segment_id;
        let recovered_cloud_wal = RecoveredCloudWalConfig::from(&config);
        Self::apply_runtime_state_config(&mut state, &config);

        let (sst_factory, read_resources) =
            Self::initialize_sst_resources(&state, &sst_dir, memory_mode, &config)?;
        state.writer_epoch = config.writer_epoch;
        let (flush_completion_tx, flush_worker_result_rx) =
            crossbeam::channel::unbounded::<FlushWorkerResult>();

        // Create actors - they handle memory_mode internally
        let flush_actor = FlushActor::new(
            &sst_dir,
            memory_mode,
            config.compression_policy.clone(),
            flush_completion_tx,
        )?;
        let mut wal_actor = WalActor::new(
            wal_dir,
            config.wal_durability_policy,
            config.wal_batch_config,
            memory_mode,
            config.writer_epoch,
            config.storage_io_timeout,
        )?;

        // Wire leader store for epoch validation at sync boundaries.
        if let Some(store) = config.leader_store.clone() {
            wal_actor.set_leader_store(store, config.leader_holder_id.clone().unwrap_or_default());
        }

        // 🔑 CRITICAL: Use the correct key for durability_waiters based on mode
        // - CloudAsync: key is segment_id (for rotate_to/complete calls)
        // - Batched: key is flush_generation (returned from wal_actor.sync())
        let is_cloud_async = wal_actor.is_cloud_async();
        let initial_durability_key = if is_cloud_async {
            initial_segment_id
        } else {
            wal_actor.current_flush_generation()
        };

        let mut gc_actor = GcActor::new();
        gc_actor.set_retry_notifier(worker_msg_tx.clone());

        let mut event_loop = Self {
            state,
            flush_actor,
            compaction_actor: {
                let compaction_config = crate::compaction::LeveledCompactionConfig {
                    l0_file_count_threshold: config.l0_compaction_trigger.max(1),
                    ..Default::default()
                };
                let mut actor = CompactionActor::new_with_config(sst_factory, compaction_config);
                actor.set_execution_limits(config.target_sst_size, config.compaction_memory_limit);
                actor
            },
            wal_actor,
            #[cfg(test)]
            cloud_actor: CloudActor::new(),
            gc_actor,
            manifest_actor: ManifestActor::new(),
            hybrid_storage: None,
            hybrid_storage_events: config.hybrid_storage_events.clone(),
            cloud_metadata_storage: config.cloud_metadata_storage.clone(),
            trace_enabled,
            loop_debug: std::env::var_os("MIDGE_LOOP_DEBUG").is_some(),
            loop_debug_wakes: 0,
            loop_debug_batch_total: 0,
            cloud_wal: CloudWalUploadTracker::new(config.recovered_cloud_wal_segments.clone()),
            cloud_wal_prune_worker: None,
            next_background_compaction_check: Instant::now() + BACKGROUND_COMPACTION_CHECK_INTERVAL,
            durability: DurabilityCoordinator::new(
                initial_durability_key,
                is_cloud_async,
                config.cloud_runtime_policy.clone(),
            ),
            router,
            inline_responses: RefCell::new(HashMap::new()),
            pending_msg: None,
            verification_barrier: VerificationBarrier::default(),
            publication_gate: ManifestPublicationGate::default(),
            flush_worker_result_rx,
            flush_barrier_waiters: HashMap::new(),
            inline_flush_worker: cfg!(test) && worker_msg_tx.is_none(),
            shutting_down: false,
            shutdown_cloud_drain_timeout: config.shutdown_cloud_drain_timeout,
            worker_msg_tx,

            write_stall_waiters: HashMap::new(),
            write_stall_waiter_queues: HashMap::new(),
            snapshot_cache: None,
            read_resources,
            sst_read_views: RefCell::new(SstReadViewCache::new()),
            lease_healthy: config.lease_healthy.clone(),
            ddl_authority_ambiguous: false,
            compaction_publication_degraded: false,
            last_compaction_publication_error: None,
            writer_epoch: config.writer_epoch,
            runtime_response_timeout: config.runtime_response_timeout,
            leader_store: config.leader_store.clone(),
            leader_holder_id: config.leader_holder_id.clone(),
        };

        if let Some(storage) = config.hybrid_storage {
            event_loop.set_hybrid_storage(storage);
        }
        event_loop.initialize_recovered_cloud_wal(&recovered_cloud_wal)?;

        Ok(event_loop)
    }

    fn apply_runtime_state_config(state: &mut RuntimeState, config: &super::RuntimeConfig) {
        state.install_ttl_clock(Arc::clone(&config.ttl_clock));
        state.cloud_eventual_flush_segment_gap =
            config.cloud_runtime_policy.eventual_flush_segment_gap;
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
        }

        let initially_durable = self
            .durability
            .take_contiguous_acked_cloud_segments(&self.cloud_wal.acked_segments)
            .map_err(crate::common::MidgeError::RecoveryFailed)?;
        if let Some((_, max_sequence)) = initially_durable.last() {
            self.state.wal.cloud_durable_seq = self.state.wal.cloud_durable_seq.max(*max_sequence);
        }
        for (segment_id, _) in initially_durable {
            self.remove_cloud_durable_local_wal_segment(segment_id);
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
        self.wal_actor.set_storage_budget(Arc::clone(&storage));
        self.hybrid_storage = Some(storage);
    }

    /// Returns an error if the lease has been lost (heartbeat detected failure).
    fn check_lease_health(&self) -> crate::common::MidgeResult<()> {
        if self.ddl_authority_ambiguous {
            return Err(crate::common::MidgeError::Fenced(
                "DDL authority is ambiguous; refusing writes until prepared DDL is reconciled"
                    .into(),
            ));
        }
        if let Some(healthy) = &self.lease_healthy {
            if !healthy.load(std::sync::atomic::Ordering::Acquire) {
                return Err(crate::common::MidgeError::Fenced(
                    "lease heartbeat reports unhealthy — refusing writes".into(),
                ));
            }
        }
        Ok(())
    }

    fn validate_runtime_writer_lease_within(
        &self,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<()> {
        self.check_lease_health()?;
        if deadline.is_expired() {
            return Err(crate::common::MidgeError::Timeout(
                "operation deadline exhausted before writer lease validation".to_string(),
            ));
        }
        let Some(store) = &self.leader_store else {
            return Ok(());
        };
        let holder_id = self.leader_holder_id.as_deref().unwrap_or_default();
        let result = if deadline.is_bounded() {
            store.validate_epoch_with_timeout(holder_id, self.writer_epoch, deadline.remaining())
        } else {
            store.validate_epoch(holder_id, self.writer_epoch)
        };
        result.map_err(|error| {
            let error = if deadline.is_bounded()
                && (deadline.is_expired() || error.to_string().contains("timed out"))
            {
                crate::common::MidgeError::Timeout(format!(
                    "writer lease validation exceeded the operation deadline: {error}"
                ))
            } else {
                crate::common::MidgeError::Fenced(error.to_string())
            };
            if matches!(error, crate::common::MidgeError::Fenced(_)) {
                if let Some(healthy) = &self.lease_healthy {
                    healthy.store(false, std::sync::atomic::Ordering::Release);
                }
                tracing::error!(%error, "writer lease validation failed; runtime fenced");
            }
            error
        })
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
            let live_names = self
                .sst_read_views
                .borrow_mut()
                .live_names(&self.state.manifest);
            read_resources.prune_to_live_ssts(&live_names);
        }
    }

    fn invalidate_sst_read_views(&self) {
        self.sst_read_views.borrow_mut().invalidate();
    }

    fn build_sst_file_meta(
        &self,
        cf_id: crate::types::ColumnFamilyId,
        level: u32,
        sst_name: &str,
        budget: &crate::common::resource_budget::ResourceBudget,
    ) -> crate::common::MidgeResult<crate::runtime::FileMeta> {
        if let Some((meta, proof)) = self.compaction_actor.prepared_remote_output(sst_name) {
            if meta.cf_id != cf_id || meta.level != level || meta.name != sst_name {
                return Err(crate::common::MidgeError::Corruption(
                    "remote compaction output identity mismatch".into(),
                ));
            }
            let storage = self.hybrid_storage.as_ref().ok_or_else(|| {
                crate::common::MidgeError::Internal(
                    "remote compaction output without cloud storage".into(),
                )
            })?;
            storage.verify_remote_object_guards_within(
                &[proof],
                &crate::common::OperationDeadline::unbounded(),
            )?;
            return Ok(meta);
        }
        let path = self.state.sst_dir.join(sst_name);
        let summary = crate::sst::fs::SstFileIo::summarize_with_real_fs_for_compaction(
            &path,
            budget.clone(),
        )?;

        Ok(crate::runtime::FileMeta {
            name: sst_name.to_string(),
            level,
            size_bytes: summary.size_bytes,
            content_crc32c: Some(Self::checksummed_file_crc(&path, budget)?),
            cf_id,
            smallest_key: Some(summary.smallest_key),
            largest_key: Some(summary.largest_key),
            smallest_seq: Some(summary.smallest_seq),
            largest_seq: Some(summary.largest_seq),
            key_bounds_complete: true,
        })
    }

    fn checksummed_file_crc(
        path: &std::path::Path,
        budget: &crate::common::resource_budget::ResourceBudget,
    ) -> crate::common::MidgeResult<u32> {
        use std::io::Read;

        const CRC_BUFFER_SIZE: usize = 64 * 1024;
        let _reservation = budget.reserve(CRC_BUFFER_SIZE, "SST checksum buffer")?;
        let mut file = std::fs::File::open(path)?;
        let mut buffer = vec![0u8; CRC_BUFFER_SIZE].into_boxed_slice();
        let mut crc = 0u32;
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                return Ok(crc);
            }
            crc = crc32c::crc32c_append(crc, &buffer[..read]);
        }
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
            let next = plan.output_seq.checked_add(1).ok_or_else(|| {
                crate::common::MidgeError::ResourceLimit("SST filename allocation exhausted".into())
            })?;
            let counter = self
                .state
                .manifest
                .next_sst_seqs
                .entry(plan.cf_id)
                .or_insert(1);
            *counter = (*counter).max(next);
            crate::metadata::append_edit(
                &self.state.db_path,
                &crate::metadata::ManifestEdit::BumpNextSstSeq {
                    cf_id: plan.cf_id,
                    next_seq: *counter,
                },
            )?;
            crate::runtime::actors::ManifestActor::persist(&self.state)?;
            self.mirror_metadata_to_authoritative_cloud()?;
        }
        Ok(plan)
    }

    fn prepare_compaction_plan_for_launch(
        &mut self,
        plan: crate::compaction::CompactionPlan,
    ) -> crate::common::MidgeResult<crate::compaction::CompactionPlan> {
        let mut plan = self.assign_compaction_output_sequence(plan)?;
        plan.snapshot_horizon = self.state.oldest_active_snapshot_sequence();
        plan.target_sst_size = self.compaction_actor.target_sst_size();
        plan.compaction_memory_limit = self.compaction_actor.compaction_memory_limit();

        if plan.output_seq == 0 {
            return Err(crate::common::MidgeError::Internal(
                "BUG: compaction output sequence was not assigned before actor launch".to_string(),
            ));
        }

        Ok(plan)
    }

    fn launch_compaction(
        &mut self,
        plan: crate::compaction::CompactionPlan,
    ) -> crate::common::MidgeResult<()> {
        if self.publication_gate.active {
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
        let plan = self.prepare_compaction_plan_for_launch(plan)?;

        self.compaction_actor
            .run_compaction(
                &mut self.state,
                &plan,
                self.hybrid_storage.as_ref(),
                self.worker_msg_tx.clone(),
            )
            .map(|_| ())
            .map_err(|error| crate::common::MidgeError::Internal(error.to_string()))
    }

    fn schedule_one_background_compaction_if_needed(
        &mut self,
        operation: &str,
    ) -> crate::common::MidgeResult<bool> {
        // Disabling ordinary background work must not permanently wedge L0
        // admission. Use the same authority, ingest, and worker gates for
        // pressure recovery at startup, after flush, and during live maintenance.
        let background_enabled = self.state.compaction_enabled();
        if !background_enabled && !self.state.has_any_critical_l0_debt() {
            return Ok(false);
        }
        if self.ddl_authority_ambiguous {
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

        let planned = if background_enabled {
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

    fn mirror_ssts_to_authoritative_cloud(
        &self,
        sst_names: &[String],
        budget: &crate::common::resource_budget::ResourceBudget,
    ) -> crate::common::MidgeResult<()> {
        let Some(hybrid) = self.hybrid_storage.as_ref() else {
            return Ok(());
        };

        for sst_name in sst_names {
            if let Some((_metadata, proof)) = self.compaction_actor.prepared_remote_output(sst_name)
            {
                hybrid.verify_remote_object_guards_within(
                    &[proof],
                    &crate::common::OperationDeadline::unbounded(),
                )?;
                continue;
            }
            let path = self.state.sst_dir.join(sst_name);
            let (data, _reservation) = Self::read_file_with_budget(&path, budget)?;
            hybrid.write_sst_object(sst_name, data)?;
        }

        Ok(())
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
            if let Err(error) = storage.evict_local_object_cache(&crate::sst::object_key(name)) {
                tracing::warn!(%error, sst_name = name, "retaining secondary SST cache after failed eviction");
            }
        }
    }

    fn read_file_with_budget(
        path: &std::path::Path,
        budget: &crate::common::resource_budget::ResourceBudget,
    ) -> crate::common::MidgeResult<(Vec<u8>, crate::common::resource_budget::ResourceReservation)>
    {
        let file_size = std::fs::metadata(path)?.len();
        let retained_bytes = usize::try_from(file_size).map_err(|_| {
            crate::common::MidgeError::ResourceLimit(format!(
                "compaction output '{}' exceeds addressable memory",
                path.display()
            ))
        })?;
        let reservation = budget.reserve(retained_bytes, "cloud SST upload buffer")?;
        let bytes = std::fs::read(path)?;
        if bytes.len() != retained_bytes {
            return Err(crate::common::MidgeError::Corruption(format!(
                "compaction output '{}' changed size before cloud upload",
                path.display()
            )));
        }
        Ok((bytes, reservation))
    }

    fn cloud_metadata_timeout(
        key: &str,
        operation: &str,
        per_operation_timeout: std::time::Duration,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<std::time::Duration> {
        deadline
            .clamp_nonzero(per_operation_timeout)
            .ok_or_else(|| {
                crate::common::MidgeError::Timeout(format!(
                    "operation deadline exhausted before cloud metadata {operation} for '{key}'"
                ))
            })
    }

    fn cloud_metadata_get_optional(
        cloud: &crate::storage::cloud::CloudStorage,
        key: &str,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<Option<Vec<u8>>> {
        let timeout = Self::cloud_metadata_timeout(key, "GET", cloud.callback_timeout(), deadline)?;
        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_get(key, tx);
        match rx.recv_timeout(timeout) {
            Ok(crate::storage::cloud::CloudEvent::Get {
                result: crate::storage::cloud::CloudOutcome::Ok(data),
                ..
            }) => Ok(Some(data)),
            Ok(crate::storage::cloud::CloudEvent::Get {
                result: crate::storage::cloud::CloudOutcome::Err(error),
                ..
            }) if crate::storage::cloud::is_not_found_error(&error) => Ok(None),
            Ok(crate::storage::cloud::CloudEvent::Get {
                result: crate::storage::cloud::CloudOutcome::Err(error),
                ..
            }) => Err(crate::storage::cloud::contextualize_operation_error(
                &error,
                format_args!("cloud metadata get '{key}' failed"),
                deadline,
            )),
            Ok(other) => Err(crate::common::MidgeError::Internal(format!(
                "unexpected cloud metadata get response for '{key}': {other:?}"
            ))),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                Err(crate::common::MidgeError::Timeout(format!(
                    "cloud metadata get '{key}' exceeded the operation deadline"
                )))
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                Err(crate::common::MidgeError::Internal(format!(
                    "cloud metadata get callback closed for '{key}'"
                )))
            }
        }
    }

    fn cloud_metadata_head_optional(
        cloud: &crate::storage::cloud::CloudStorage,
        key: &str,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<Option<crate::storage::cloud::ObjectMetadata>> {
        let timeout =
            Self::cloud_metadata_timeout(key, "HEAD", cloud.callback_timeout(), deadline)?;
        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_head(key, tx);
        match rx.recv_timeout(timeout) {
            Ok(crate::storage::cloud::CloudEvent::Head {
                result: crate::storage::cloud::CloudOutcome::Ok(metadata),
                ..
            }) => Ok(Some(metadata)),
            Ok(crate::storage::cloud::CloudEvent::Head {
                result: crate::storage::cloud::CloudOutcome::Err(error),
                ..
            }) if crate::storage::cloud::is_not_found_error(&error) => Ok(None),
            Ok(crate::storage::cloud::CloudEvent::Head {
                result: crate::storage::cloud::CloudOutcome::Err(error),
                ..
            }) => Err(crate::storage::cloud::contextualize_operation_error(
                &error,
                format_args!("cloud metadata head '{key}' failed"),
                deadline,
            )),
            Ok(other) => Err(crate::common::MidgeError::Internal(format!(
                "unexpected cloud metadata head response for '{key}': {other:?}"
            ))),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                Err(crate::common::MidgeError::Timeout(format!(
                    "cloud metadata head '{key}' exceeded the operation deadline"
                )))
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                Err(crate::common::MidgeError::Internal(format!(
                    "cloud metadata head callback closed for '{key}'"
                )))
            }
        }
    }

    fn remote_manifest_sequence_from_metadata(
        file_name: &str,
        data: &[u8],
    ) -> Result<Option<u64>, String> {
        match file_name {
            "manifest.json" | "manifest.snapshot.json" => {
                let manifest: crate::metadata::Manifest = serde_json::from_slice(data)
                    .map_err(|error| format!("cloud metadata '{file_name}' is invalid: {error}"))?;
                Ok(Some(manifest.last_persisted_sequence))
            }
            _ => Ok(None),
        }
    }

    fn ensure_remote_manifest_metadata_not_ahead(
        &self,
        cloud: &crate::storage::cloud::CloudStorage,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<()> {
        let local_sequence = self.state.manifest.last_persisted_sequence;
        for file_name in ["manifest.snapshot.json", "manifest.json"] {
            let key = crate::storage::cloud::cloud_metadata_key(file_name);
            let Some(data) = Self::cloud_metadata_get_optional(cloud, &key, deadline)? else {
                continue;
            };
            let Some(remote_sequence) =
                Self::remote_manifest_sequence_from_metadata(file_name, &data)
                    .map_err(crate::common::MidgeError::Internal)?
            else {
                continue;
            };
            if remote_sequence > local_sequence {
                return Err(crate::common::MidgeError::Internal(format!(
                    "stale cloud metadata mirror rejected: remote {file_name} is ahead of local manifest ({remote_sequence} > {local_sequence})"
                )));
            }
        }
        Ok(())
    }

    fn submit_conditional_metadata_put(
        cloud: &crate::storage::cloud::CloudStorage,
        file_name: &str,
        key: &str,
        data: Vec<u8>,
        local_manifest_sequence: u64,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<()> {
        let headers = match Self::cloud_metadata_head_optional(cloud, key, deadline)? {
            Some(metadata) => {
                let headers = crate::storage::cloud::object_match_precondition_headers(
                    &metadata.etag,
                    metadata.generation.as_deref(),
                )
                .ok_or_else(|| {
                    crate::common::MidgeError::Internal(format!(
                        "cloud metadata '{key}' cannot be conditionally updated without an identity token"
                    ))
                })?;
                let current =
                    Self::cloud_metadata_get_optional(cloud, key, deadline)?.ok_or_else(|| {
                        crate::common::MidgeError::Internal(format!(
                            "cloud metadata '{key}' disappeared after HEAD precondition"
                        ))
                    })?;
                if let Some(remote_sequence) =
                    Self::remote_manifest_sequence_from_metadata(file_name, &current)
                        .map_err(crate::common::MidgeError::Internal)?
                {
                    if remote_sequence > local_manifest_sequence {
                        return Err(crate::common::MidgeError::Internal(format!(
                            "stale cloud metadata mirror rejected: remote {file_name} is ahead of local manifest ({remote_sequence} > {local_manifest_sequence})"
                        )));
                    }
                }
                if current == data {
                    return Ok(());
                }
                headers
            }
            None => vec![("If-None-Match".to_string(), "*".to_string())],
        };

        let timeout = Self::cloud_metadata_timeout(key, "PUT", cloud.callback_timeout(), deadline)?;
        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_put(key, data, headers, tx);

        match rx.recv_timeout(timeout) {
            Ok(crate::storage::cloud::CloudEvent::Put { result, .. }) => match result {
                crate::storage::cloud::CloudOutcome::Ok(()) => Ok(()),
                crate::storage::cloud::CloudOutcome::Err(error) => {
                    Err(crate::storage::cloud::contextualize_operation_error(
                        &error,
                        format_args!("cloud metadata mirror failed for '{key}'"),
                        deadline,
                    ))
                }
            },
            Ok(other) => Err(crate::common::MidgeError::Internal(format!(
                "unexpected cloud metadata mirror response for '{key}': {other:?}"
            ))),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                Err(crate::common::MidgeError::Timeout(format!(
                    "cloud metadata mirror put '{key}' exceeded the operation deadline"
                )))
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                Err(crate::common::MidgeError::Internal(format!(
                    "cloud metadata mirror callback closed for '{key}'"
                )))
            }
        }
    }

    fn mirror_metadata_to_authoritative_cloud(&self) -> crate::common::MidgeResult<()> {
        self.mirror_metadata_to_authoritative_cloud_within(
            &crate::common::OperationDeadline::unbounded(),
        )
    }

    pub(super) fn mirror_metadata_to_authoritative_cloud_within(
        &self,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<()> {
        let Some(cloud) = self.cloud_metadata_storage.as_ref() else {
            return Ok(());
        };
        let _publication_guard = cloud.try_lock_metadata_publication().ok_or_else(|| {
            crate::common::MidgeError::Busy(
                "cloud metadata publication is already in progress".to_string(),
            )
        })?;

        self.ensure_remote_manifest_metadata_not_ahead(cloud, deadline)?;
        let local_manifest_sequence = self.state.manifest.last_persisted_sequence;

        for file_name in crate::storage::cloud::CLOUD_METADATA_FILES {
            if deadline.is_expired() {
                return Err(crate::common::MidgeError::Timeout(format!(
                    "operation deadline exhausted before cloud metadata local mirror preparation for '{file_name}'"
                )));
            }
            let local_path = self.state.db_path.join(file_name);
            if !local_path.exists() {
                continue;
            }

            let data = std::fs::read(&local_path)?;
            let key = crate::storage::cloud::cloud_metadata_key(file_name);
            Self::submit_conditional_metadata_put(
                cloud,
                file_name,
                &key,
                data,
                local_manifest_sequence,
                deadline,
            )?;
        }

        Ok(())
    }

    fn mirror_metadata_after_local_commit(
        &mut self,
        context: &str,
    ) -> crate::common::MidgeResult<()> {
        self.mirror_metadata_after_local_commit_within(
            context,
            &crate::common::OperationDeadline::unbounded(),
        )
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
        _frozen_memtable: Option<&std::sync::Arc<crate::sst::SkipListMemtable>>,
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
        crate::runtime::actors::ManifestActor::persist(&self.state)?;
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
            || self.state.compaction.pending_tasks > 0
            || !self.state.compaction.compacting_ssts.is_empty()
            || self.flush_actor.is_inflight()
            || self.publication_gate.active;
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
            return false;
        }

        if !self.flush_worker_result_rx.is_empty() {
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

        if self.state.compaction.pending_tasks > 0 {
            return true;
        }

        if self.background_maintenance_timeout() == Duration::ZERO {
            return true;
        }

        if self.state.has_due_immutable_flush() {
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
            // message instead of repeatedly timing out at zero duration.
            return None;
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
            self.state.flush_retry_deadline_timeout(),
            self.flush_actor
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
            return;
        }
        self.drain_flush_worker_results();
        self.sync_batched_wal_if_needed(msg_rx);
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
        if self.publication_gate.active {
            self.gc_actor.defer_manifest_reclamation_retry();
            return;
        }

        let deadline = crate::common::OperationDeadline::from_budget(self.runtime_response_timeout);
        let _ = self.retry_gc_within(&deadline);
    }

    fn record_wake_batch(&mut self, batch: usize) {
        if let Some(telemetry) = crate::telemetry::Telemetry::global() {
            telemetry.metrics().record_event_loop_wake();
            telemetry.metrics().record_event_loop_batch(batch as u64);
        }

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

    fn run_request_fairness_slot(&mut self) {
        // A continuously non-empty request queue must not starve background
        // durability and storage progress. Run this bounded slot only after
        // dispatch so a restored control request keeps the publication turn
        // that made it eligible.
        self.drain_flush_worker_results();
        self.maybe_flush_cloud_async_wal();
        self.drain_cloud_wal_upload_backlog();
        self.tick_hybrid_storage();
        self.drain_hybrid_storage_events();
        self.drain_cloud_wal_upload_backlog();
        self.drain_auto_flush_memtables();
        self.run_background_compaction_maintenance_if_due();
        self.retry_manifest_reclamation_if_due();
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
            && !self.publication_gate.active
            && self.verification_barrier.token.is_none()
            && self.pending_msg.is_none()
        {
            let eligible =
                self.publication_gate.deferred_messages.front().is_some_and(
                    |message| match message {
                        RuntimeMsg::ManifestDropColumnFamily { cf_id, .. } => {
                            !self.column_family_publication_pipeline_active(*cf_id)
                        }
                        _ => true,
                    },
                );
            if eligible {
                self.pending_msg = self.publication_gate.finish();
            }
        }
    }

    /// Main event loop — runs until Shutdown message or channel close.
    pub fn run(&mut self, msg_rx: &Receiver<RuntimeMsg>) {
        // Bound write coalescing by a fairness quantum. Thousands of local
        // writes are cheap, but the same wake on cloud durability can consume
        // an entire control-request deadline before yielding.
        const MAX_DRAIN_WRITES_ON_WAKE: usize = 64;
        const ACTIONABLE_IDLE_BACKOFF: Duration = Duration::from_micros(50);

        loop {
            self.restore_verification_deferred_message();
            self.restore_publication_deferred_message();
            if let Some(pending) = self.pending_msg.take() {
                let outcome = self.process_restored_one(pending, msg_rx);
                if outcome == HandleOutcome::Break {
                    break;
                }
                continue;
            }

            if self.has_actionable_work() {
                match msg_rx.try_recv() {
                    Ok(msg) => {
                        let outcome = self.process_wake_msg(msg, msg_rx, MAX_DRAIN_WRITES_ON_WAKE);
                        if outcome == HandleOutcome::Break {
                            break;
                        }
                        continue;
                    }
                    Err(TryRecvError::Disconnected) => break,
                    Err(TryRecvError::Empty) => {}
                }

                if let Some(storage_rx) = &self.hybrid_storage_events {
                    match storage_rx.try_recv() {
                        Ok(event) => {
                            self.handle_storage_event(event);
                            continue;
                        }
                        Err(TryRecvError::Disconnected | TryRecvError::Empty) => {}
                    }
                }

                self.progress_pass(msg_rx);
                std::thread::sleep(ACTIONABLE_IDLE_BACKOFF);
                continue;
            }

            let idle_timeout = self.idle_progress_timeout();
            let selectable_storage_rx = (!self.verification_barrier.is_active())
                .then(|| self.hybrid_storage_events.clone())
                .flatten();
            let msg = if let Some(storage_rx) = selectable_storage_rx {
                if let Some(timeout) = idle_timeout {
                    crossbeam::channel::select! {
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
                match msg_rx.recv_timeout(timeout) {
                    Ok(msg) => Some(msg),
                    Err(crossbeam::channel::RecvTimeoutError::Timeout) => {
                        self.progress_pass(msg_rx);
                        continue;
                    }
                    Err(crossbeam::channel::RecvTimeoutError::Disconnected) => None,
                }
            } else {
                msg_rx.recv().ok()
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
