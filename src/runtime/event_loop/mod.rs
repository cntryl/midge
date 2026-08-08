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
mod compaction;
mod control;
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
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

const BACKGROUND_COMPACTION_CHECK_INTERVAL: Duration = Duration::from_secs(30);
const STARTUP_CLOUD_MAINTENANCE_DELAY: Duration = Duration::from_millis(100);

#[cfg(test)]
use super::actors::CloudActor;
use super::actors::{CompactionActor, FlushActor, GcActor, ManifestActor, WalActor};
use super::durability::DurabilityCoordinator;
use super::read_resources::ReadResources;
use super::read_snapshot::ReadSnapshot;
use super::snapshot_cache::{CfSnapshotData, PublishedSnapshot, SnapshotCache};
use super::state::RuntimeState;
use super::{ResponseRouter, RuntimeMsg, RuntimeResponse};
use crate::runtime::actors::flush::FlushWorkerResult;

type SstRuntimeResources = (Arc<dyn crate::sst::SstFactory>, Option<Arc<ReadResources>>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct MetadataCleanupProof {
    len: u64,
    crc32c: u32,
    remote: crate::storage::StorageObjectMetadata,
}

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
    pub(super) cloud_acked_wal_segments: BTreeMap<u64, u64>,
    pub(super) cloud_wal_prune_inflight: HashSet<u64>,
    cloud_wal_prune_retries: HashMap<u64, (u32, Instant)>,
    pub(super) cloud_metadata_cleanup_proofs: HashMap<String, MetadataCleanupProof>,
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

    /// Active runtime-wide barrier token held by online storage verification.
    verification_barrier_token: Option<u64>,
    /// Internal completion messages held until the verification barrier releases.
    verification_deferred_messages: VecDeque<RuntimeMsg>,
    /// Manifest-mutating messages held while a flush publication owns the
    /// single authority-changing I/O slot.
    publication_deferred_messages: VecDeque<RuntimeMsg>,
    manifest_publication_active: bool,

    pub(super) flush_worker_result_rx: crossbeam::channel::Receiver<FlushWorkerResult>,
    flush_barrier_waiters: HashMap<crate::types::ColumnFamilyId, Vec<flush::FlushBarrierWaiter>>,
    inline_flush_worker: bool,
    shutting_down: bool,

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

    /// Shared flag from the lease heartbeat. When `false`, the event loop
    /// rejects new write operations with `MidgeError::Fenced`.
    lease_healthy: Option<Arc<std::sync::atomic::AtomicBool>>,
    writer_epoch: u64,
    leader_store: Option<Arc<dyn crate::lease::LeaderStore>>,
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
            let fs = Arc::new(crate::io::RealFs::new(sst_dir)?);
            Arc::new(
                crate::sst::FsSstFactoryIo::new(fs, 64 * 1024)
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
                Arc::clone(&state.fs),
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
            wal_actor.set_leader_store(store);
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

        state.cloud_eventual_flush_segment_gap =
            config.cloud_runtime_policy.eventual_flush_segment_gap;
        state.set_compaction_enabled(config.background_compaction);

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
                CompactionActor::new_with_config(sst_factory, compaction_config)
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
            cloud_acked_wal_segments: config.recovered_cloud_wal_segments.clone(),
            cloud_wal_prune_inflight: HashSet::new(),
            cloud_wal_prune_retries: HashMap::new(),
            cloud_metadata_cleanup_proofs: HashMap::new(),
            next_background_compaction_check: Instant::now() + BACKGROUND_COMPACTION_CHECK_INTERVAL,
            durability: DurabilityCoordinator::new(
                initial_durability_key,
                is_cloud_async,
                config.cloud_runtime_policy.clone(),
            ),
            router,
            inline_responses: RefCell::new(HashMap::new()),
            pending_msg: None,
            verification_barrier_token: None,
            verification_deferred_messages: VecDeque::new(),
            publication_deferred_messages: VecDeque::new(),
            manifest_publication_active: false,
            flush_worker_result_rx,
            flush_barrier_waiters: HashMap::new(),
            inline_flush_worker: cfg!(test) && worker_msg_tx.is_none(),
            shutting_down: false,
            worker_msg_tx,

            write_stall_waiters: HashMap::new(),
            write_stall_waiter_queues: HashMap::new(),
            snapshot_cache: None,
            read_resources,
            lease_healthy: config.lease_healthy.clone(),
            writer_epoch: config.writer_epoch,
            leader_store: config.leader_store.clone(),
        };

        if let Some(storage) = config.hybrid_storage {
            event_loop.set_hybrid_storage(storage);
        }

        Ok(event_loop)
    }

    pub fn set_hybrid_storage(&mut self, storage: Arc<crate::storage::HybridStorage>) {
        self.hybrid_storage = Some(storage);
    }

    /// Returns an error if the lease has been lost (heartbeat detected failure).
    fn check_lease_health(&self) -> crate::common::MidgeResult<()> {
        if let Some(healthy) = &self.lease_healthy {
            if !healthy.load(std::sync::atomic::Ordering::Acquire) {
                return Err(crate::common::MidgeError::Fenced(
                    "lease heartbeat reports unhealthy — refusing writes".into(),
                ));
            }
        }
        Ok(())
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
            let cf_files: Vec<_> = self
                .state
                .manifest
                .files
                .iter()
                .filter(|f| f.cf_id == cf_id)
                .cloned()
                .collect();
            let sst_path_prefix = self
                .state
                .sst_dir
                .strip_prefix(&self.state.db_path)
                .unwrap_or_else(|_| std::path::Path::new("sst"))
                .to_path_buf();
            cf_snapshots.insert(
                cf_id,
                CfSnapshotData {
                    snapshot: Arc::new(ReadSnapshot::new_with_resources(
                        cf_state.memtable.clone(),
                        cf_state.immutable_memtables.clone(),
                        cf_files,
                        Arc::clone(&self.state.fs),
                        sst_path_prefix,
                        self.state.is_memory_mode(),
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
                .state
                .manifest
                .files
                .iter()
                .map(|file| file.name.clone())
                .collect();
            read_resources.prune_to_live_ssts(&live_names);
        }
    }

    fn build_sst_file_meta(
        &self,
        cf_id: crate::types::ColumnFamilyId,
        level: u32,
        sst_name: &str,
    ) -> crate::common::MidgeResult<crate::runtime::FileMeta> {
        let path = self.state.sst_dir.join(sst_name);
        let summary = crate::sst::fs::SstFileIo::summarize_with_real_fs(&path)?;

        Ok(crate::runtime::FileMeta {
            name: sst_name.to_string(),
            level,
            size_bytes: summary.size_bytes,
            content_crc32c: Some(crc32c::crc32c(&std::fs::read(&path)?)),
            cf_id,
            smallest_key: Some(summary.smallest_key),
            largest_key: Some(summary.largest_key),
            smallest_seq: Some(summary.smallest_seq),
            largest_seq: Some(summary.largest_seq),
        })
    }

    fn assign_compaction_output_sequence(
        &mut self,
        mut plan: crate::compaction::CompactionPlan,
    ) -> crate::compaction::CompactionPlan {
        if plan.output_seq == 0 {
            plan.output_seq = self.state.next_sequence();
        }
        plan
    }

    fn prepare_compaction_plan_for_launch(
        &mut self,
        plan: crate::compaction::CompactionPlan,
    ) -> crate::common::MidgeResult<crate::compaction::CompactionPlan> {
        let mut plan = self.assign_compaction_output_sequence(plan);
        plan.snapshot_horizon = self.state.oldest_active_snapshot_sequence();

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

        let Some(plan) = self.compaction_actor.check_compaction(&self.state) else {
            return Ok(false);
        };

        self.launch_compaction(plan)?;
        Ok(true)
    }

    fn schedule_compaction_after_flush_publication(&mut self, sst_name: &str) {
        if !self.state.compaction_enabled() {
            return;
        }

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
        if self.state.compaction_enabled()
            && !self
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
        self.rearm_wal_prune_retry_deadline();
    }

    /// Fold any still-pending cloud WAL-prune retry deadline back into the
    /// background maintenance timer. Without this, resetting the timer above
    /// to the full 30s interval would silently discard a tighter exponential
    /// backoff deadline set by a failed prune (see ack.rs), stalling that
    /// segment's retry on an otherwise-idle system.
    fn rearm_wal_prune_retry_deadline(&mut self) {
        if let Some(earliest_retry_at) = self
            .cloud_wal_prune_retries
            .values()
            .map(|(_, retry_at)| *retry_at)
            .min()
        {
            self.next_background_compaction_check =
                self.next_background_compaction_check.min(earliest_retry_at);
        }
    }

    pub(super) fn schedule_background_compaction_on_startup(&mut self) {
        self.next_background_compaction_check = Instant::now() + STARTUP_CLOUD_MAINTENANCE_DELAY;
        if self.state.compaction_enabled() {
            match self.schedule_one_background_compaction_if_needed("runtime startup") {
                Ok(true) => {
                    tracing::debug!("Scheduled background compaction during runtime startup");
                }
                Ok(false) => {}
                Err(error) => tracing::warn!(%error, "Startup background compaction check failed"),
            }
        }
    }

    fn mirror_ssts_to_authoritative_cloud(
        &self,
        sst_names: &[String],
    ) -> crate::common::MidgeResult<()> {
        let Some(hybrid) = self.hybrid_storage.as_ref() else {
            return Ok(());
        };

        for sst_name in sst_names {
            let path = self.state.sst_dir.join(sst_name);
            let data = std::fs::read(&path)?;
            hybrid.write_sst_object(sst_name, data)?;
        }

        Ok(())
    }

    fn cloud_metadata_get_optional(
        cloud: &crate::storage::cloud::CloudStorage,
        key: &str,
    ) -> Result<Option<Vec<u8>>, String> {
        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_get(key, tx);
        match rx.recv_timeout(std::time::Duration::from_secs(30)) {
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
            }) => Err(format!("cloud metadata get '{key}' failed: {error}")),
            Ok(other) => Err(format!(
                "unexpected cloud metadata get response for '{key}': {other:?}"
            )),
            Err(error) => Err(format!("cloud metadata get '{key}' timed out: {error}")),
        }
    }

    fn cloud_metadata_head_optional(
        cloud: &crate::storage::cloud::CloudStorage,
        key: &str,
    ) -> Result<Option<crate::storage::cloud::ObjectMetadata>, String> {
        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_head(key, tx);
        match rx.recv_timeout(std::time::Duration::from_secs(30)) {
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
            }) => Err(format!("cloud metadata head '{key}' failed: {error}")),
            Ok(other) => Err(format!(
                "unexpected cloud metadata head response for '{key}': {other:?}"
            )),
            Err(error) => Err(format!("cloud metadata head '{key}' timed out: {error}")),
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
    ) -> crate::common::MidgeResult<()> {
        let local_sequence = self.state.manifest.last_persisted_sequence;
        for file_name in ["manifest.snapshot.json", "manifest.json"] {
            let key = crate::storage::cloud::cloud_metadata_key(file_name);
            let Some(data) = Self::cloud_metadata_get_optional(cloud, &key)
                .map_err(crate::common::MidgeError::Internal)?
            else {
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
    ) -> crate::common::MidgeResult<()> {
        let headers = match Self::cloud_metadata_head_optional(cloud, key)
            .map_err(crate::common::MidgeError::Internal)?
        {
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
                let current = Self::cloud_metadata_get_optional(cloud, key)
                    .map_err(crate::common::MidgeError::Internal)?
                    .ok_or_else(|| {
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

        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_put(key, data, headers, tx);

        match rx.recv_timeout(std::time::Duration::from_secs(30)) {
            Ok(crate::storage::cloud::CloudEvent::Put { result, .. }) => match result {
                crate::storage::cloud::CloudOutcome::Ok(()) => Ok(()),
                crate::storage::cloud::CloudOutcome::Err(error) => {
                    Err(crate::common::MidgeError::Internal(format!(
                        "cloud metadata mirror failed for '{key}': {error}"
                    )))
                }
            },
            Ok(other) => Err(crate::common::MidgeError::Internal(format!(
                "unexpected cloud metadata mirror response for '{key}': {other:?}"
            ))),
            Err(error) => Err(crate::common::MidgeError::Internal(format!(
                "cloud metadata mirror timed out for '{key}': {error}"
            ))),
        }
    }

    fn mirror_metadata_to_authoritative_cloud(&self) -> crate::common::MidgeResult<()> {
        let Some(cloud) = self.cloud_metadata_storage.as_ref() else {
            return Ok(());
        };

        self.ensure_remote_manifest_metadata_not_ahead(cloud)?;
        let local_manifest_sequence = self.state.manifest.last_persisted_sequence;

        for file_name in crate::storage::cloud::CLOUD_METADATA_FILES {
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
            )?;
        }

        Ok(())
    }

    fn mirror_metadata_after_local_commit(
        &mut self,
        context: &str,
    ) -> crate::common::MidgeResult<()> {
        match self.mirror_metadata_to_authoritative_cloud() {
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
            let _ = response_tx.send(resp);
        } else {
            self.router.complete(resp);
        }

        // Optional trace
        if self.trace_enabled {
            tracing::trace!(request_id, "response routed");
        }
    }

    pub(super) fn retry_gc(&mut self) -> HandleOutcome {
        gc::GcCoordinator::retry(self)
    }

    pub(super) fn defer_verification_message(&mut self, message: RuntimeMsg) {
        let is_duplicate_maintenance = matches!(message, RuntimeMsg::RetryGc)
            && self
                .verification_deferred_messages
                .iter()
                .any(|pending| matches!(pending, RuntimeMsg::RetryGc));
        let is_duplicate_drop_shutdown = matches!(message, RuntimeMsg::Shutdown)
            && self
                .verification_deferred_messages
                .iter()
                .any(|pending| matches!(pending, RuntimeMsg::Shutdown));
        if !is_duplicate_maintenance && !is_duplicate_drop_shutdown {
            self.verification_deferred_messages.push_back(message);
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
            || self.manifest_publication_active;
        if self.verification_barrier_token.is_some() || layout_is_changing {
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

        self.verification_barrier_token = Some(request_id);
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
        if self.verification_barrier_token != Some(token) {
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

        self.verification_barrier_token = None;
        if self.pending_msg.is_none() {
            self.pending_msg = self.verification_deferred_messages.pop_front();
        }
        self.respond(request_id, RuntimeResponse::Ok { request_id });
        HandleOutcome::Continue
    }

    fn has_actionable_work(&self) -> bool {
        if self.pending_msg.is_some() {
            return true;
        }

        if self.verification_barrier_token.is_some() {
            return false;
        }

        if !self.flush_worker_result_rx.is_empty() {
            return true;
        }

        if self.wal_actor.should_sync_batch() || self.wal_actor.has_pending_cloud_writes() {
            return true;
        }

        if self.durability.cloud_seal_retry_needed() && self.state.wal.pending_writes > 0 {
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

        if let Some(storage) = &self.hybrid_storage {
            if storage.pending_upload_count() > 0 {
                return true;
            }
        }

        if let Some(rx) = &self.hybrid_storage_events {
            if !rx.is_empty() {
                return true;
            }
        }

        false
    }

    fn idle_progress_timeout(&self) -> Option<Duration> {
        [
            self.wal_actor.sync_deadline_timeout(),
            self.gc_actor.retry_deadline_timeout(),
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
        if self.verification_barrier_token.is_some() {
            return;
        }
        self.drain_flush_worker_results();
        self.sync_batched_wal_if_needed(msg_rx);
        self.maybe_flush_cloud_async_wal();
        self.tick_hybrid_storage();
        self.drain_hybrid_storage_events();
        let hybrid_storage = self.hybrid_storage.clone();
        self.gc_actor
            .retry_failed_cloud_deletes_if_due(&mut self.state, hybrid_storage);
        self.drain_auto_flush_memtables();
        self.run_background_compaction_maintenance_if_due();
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
        if self.verification_barrier_token.is_none() {
            self.drain_flush_worker_results();
            self.maybe_flush_cloud_async_wal();
            self.tick_hybrid_storage();
            self.drain_hybrid_storage_events();
            self.run_background_compaction_maintenance_if_due();
        }
        self.handle_runtime_msg(msg, msg_rx)
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

        let drained = if self.verification_barrier_token.is_some() || self.pending_msg.is_some() {
            0
        } else {
            self.drain_pending_writes(msg_rx, max_drain)
        };
        batch += drained;
        self.record_wake_batch(batch);
        outcome
    }

    fn restore_verification_deferred_message(&mut self) {
        if self.verification_barrier_token.is_none() && self.pending_msg.is_none() {
            self.pending_msg = self.verification_deferred_messages.pop_front();
        }
    }

    pub(super) fn restore_publication_deferred_message(&mut self) {
        if !self.manifest_publication_active
            && self.verification_barrier_token.is_none()
            && self.pending_msg.is_none()
        {
            let eligible = self
                .publication_deferred_messages
                .front()
                .is_some_and(|message| match message {
                    RuntimeMsg::ManifestDropColumnFamily { cf_id, .. } => {
                        !self.column_family_flush_pipeline_active(*cf_id)
                    }
                    _ => true,
                });
            if eligible {
                self.pending_msg = self.publication_deferred_messages.pop_front();
            }
        }
    }

    /// Main event loop — runs until Shutdown message or channel close.
    pub fn run(&mut self, msg_rx: &Receiver<RuntimeMsg>) {
        const MAX_DRAIN_WRITES_ON_WAKE: usize = 4096;
        const ACTIONABLE_IDLE_BACKOFF: Duration = Duration::from_micros(50);

        loop {
            self.restore_verification_deferred_message();
            self.restore_publication_deferred_message();
            if let Some(pending) = self.pending_msg.take() {
                let outcome = self.process_one(pending, msg_rx);
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
            let selectable_storage_rx = self
                .verification_barrier_token
                .is_none()
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
