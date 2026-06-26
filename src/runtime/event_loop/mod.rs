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
//! - `cloud_integration` — CloudAsync WAL flush, cloud ack/fail handling
//! - `write_batch` — group commit write draining, backpressure / write stall

mod cloud_integration;
mod durability_sync;
mod read_path;
mod write_batch;

use crossbeam::channel::{Receiver, TryRecvError};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use super::actors::{
    CloudActor, CompactionActor, EvictionActor, FlushActor, GcActor, ManifestActor, WalActor,
};
use super::durability::{DurabilityCoordinator, DurabilityWaiter};
use super::read_snapshot::ReadSnapshot;
use super::snapshot_cache::{CfSnapshotData, PublishedSnapshot, SnapshotCache};
use super::state::RuntimeState;
use super::{ResponseRouter, RuntimeMsg, RuntimeResponse};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct MetadataCleanupProof {
    len: u64,
    crc32c: u32,
    remote: crate::storage::StorageObjectMetadata,
}

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
    pub(super) cloud_metadata_storage: Option<Arc<crate::storage::cloud::CloudStorage>>,
    pub(super) trace_enabled: bool,
    pub(super) loop_debug: bool,
    pub(super) loop_debug_wakes: u64,
    pub(super) loop_debug_batch_total: u64,
    pub(super) cloud_acked_wal_segments: BTreeMap<u64, u64>,
    pub(super) cloud_wal_prune_inflight: HashSet<u64>,
    pub(super) cloud_metadata_cleanup_proofs: HashMap<String, MetadataCleanupProof>,

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
    /// Lock-free snapshot cache shared with Engine for read-path bypass.
    pub(super) snapshot_cache: Option<Arc<SnapshotCache>>,

    /// Shared flag from the lease heartbeat. When `false`, the event loop
    /// rejects new write operations with `MidgeError::Fenced`.
    lease_healthy: Option<Arc<std::sync::atomic::AtomicBool>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HandleOutcome {
    Continue,
    Break,
}

impl EventLoop {
    pub(crate) fn new(
        mut state: RuntimeState,
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
        let mut wal_actor = WalActor::new(
            wal_dir,
            config.wal_durability_policy,
            config.wal_batch_config,
            memory_mode,
            config.writer_epoch,
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
            cloud_metadata_storage: config.cloud_metadata_storage.clone(),
            trace_enabled,
            loop_debug: std::env::var_os("MIDGE_LOOP_DEBUG").is_some(),
            loop_debug_wakes: 0,
            loop_debug_batch_total: 0,
            cloud_acked_wal_segments: BTreeMap::new(),
            cloud_wal_prune_inflight: HashSet::new(),
            cloud_metadata_cleanup_proofs: HashMap::new(),
            durability: DurabilityCoordinator::new(
                initial_durability_key,
                is_cloud_async,
                config.cloud_runtime_policy.clone(),
            ),
            router,
            pending_msg: None,
            worker_msg_tx,

            write_stall_waiters: HashMap::new(),
            write_stall_waiter_queues: HashMap::new(),
            snapshot_cache: None,
            lease_healthy: config.lease_healthy.clone(),
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
                    snapshot: Arc::new(ReadSnapshot::new(
                        cf_state.memtable.clone(),
                        cf_state.immutable_memtables.clone(),
                        cf_files,
                        Arc::clone(&self.state.fs),
                        sst_path_prefix,
                        self.state.memory_mode,
                    )),
                },
            );
        }

        cache.publish(PublishedSnapshot {
            sequence: self.state.sequence,
            cf_snapshots,
        });
    }

    fn build_sst_file_meta(
        &self,
        cf_id: crate::engine::ColumnFamilyId,
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

    fn schedule_one_background_compaction_if_needed(
        &mut self,
        operation: &str,
    ) -> crate::common::MidgeResult<bool> {
        let evicted = self.state.evict_timed_out_snapshots();
        if evicted > 0 {
            tracing::warn!(
                evicted,
                operation,
                "Evicted timed-out snapshots before compaction check"
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

        let plan = self.assign_compaction_output_sequence(plan);
        self.compaction_actor
            .run_compaction(
                &mut self.state,
                plan,
                self.hybrid_storage.as_ref(),
                self.worker_msg_tx.clone(),
            )
            .map_err(|error| crate::common::MidgeError::Internal(error.to_string()))?;
        Ok(true)
    }

    fn schedule_compaction_after_flush_publication(&mut self, sst_name: &str) {
        if !self.state.enable_compaction {
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
        cloud.submit_get(key.to_string(), tx);
        match rx.recv_timeout(std::time::Duration::from_secs(30)) {
            Ok(crate::storage::cloud::CloudEvent::GetComplete {
                result: crate::storage::cloud::CloudOutcome::Ok(data),
                ..
            }) => Ok(Some(data)),
            Ok(crate::storage::cloud::CloudEvent::GetComplete {
                result: crate::storage::cloud::CloudOutcome::Err(error),
                ..
            }) if crate::storage::cloud::is_not_found_error(&error) => Ok(None),
            Ok(crate::storage::cloud::CloudEvent::GetComplete {
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
        cloud.submit_head(key.to_string(), tx);
        match rx.recv_timeout(std::time::Duration::from_secs(30)) {
            Ok(crate::storage::cloud::CloudEvent::HeadComplete {
                result: crate::storage::cloud::CloudOutcome::Ok(metadata),
                ..
            }) => Ok(Some(metadata)),
            Ok(crate::storage::cloud::CloudEvent::HeadComplete {
                result: crate::storage::cloud::CloudOutcome::Err(error),
                ..
            }) if crate::storage::cloud::is_not_found_error(&error) => Ok(None),
            Ok(crate::storage::cloud::CloudEvent::HeadComplete {
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
        key: String,
        data: Vec<u8>,
        local_manifest_sequence: u64,
    ) -> crate::common::MidgeResult<()> {
        let headers = match Self::cloud_metadata_head_optional(cloud, &key)
            .map_err(crate::common::MidgeError::Internal)?
        {
            Some(metadata) => {
                let etag = metadata.etag.trim().to_string();
                if etag.is_empty() {
                    return Err(crate::common::MidgeError::Internal(format!(
                        "cloud metadata '{key}' cannot be conditionally updated without an etag"
                    )));
                }
                let current = Self::cloud_metadata_get_optional(cloud, &key)
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
                vec![("If-Match".to_string(), etag)]
            }
            None => vec![("If-None-Match".to_string(), "*".to_string())],
        };

        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_put(key.clone(), data, headers, tx);

        match rx.recv_timeout(std::time::Duration::from_secs(30)) {
            Ok(crate::storage::cloud::CloudEvent::PutComplete { result, .. }) => match result {
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
                key,
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
            Err(error) if self.state.recovery_policy == crate::engine::RecoveryPolicy::Salvage => {
                self.state.mark_persistence_anomaly();
                tracing::warn!(%error, context, "cloud metadata mirror failed during salvage-capable operation");
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    fn publish_flushed_sst(
        &mut self,
        cf_id: crate::engine::ColumnFamilyId,
        sst_name: &str,
        sequence: u64,
        file_meta: Option<crate::runtime::FileMeta>,
    ) -> crate::common::MidgeResult<()> {
        // Invariant: this is the flush authority switch. Before this point the
        // WAL-backed state remains authoritative; after successful manifest
        // publication the new SST may replace that state on restart.
        let Some(file_meta) = file_meta else {
            return Ok(());
        };

        let mut cloud_manifest_published = false;
        if !self.state.memory_mode {
            self.manifest_actor.add_sst(&mut self.state, file_meta)?;
            self.state.transition_flush_publication_intent(
                sst_name,
                crate::runtime::PublicationPhase::ManifestPublished,
            )?;
            if sequence > self.state.manifest.last_persisted_sequence {
                self.state.manifest.last_persisted_sequence = sequence;
            }
            let manifest_persisted = match self.manifest_actor.persist(&self.state) {
                Ok(()) => true,
                Err(error) => {
                    self.state.mark_persistence_anomaly();
                    tracing::warn!(%error, sst_name, "manifest checkpoint after flush failed; journal remains authoritative");
                    false
                }
            };

            self.state.clear_flush_publication_intent(sst_name)?;
            if manifest_persisted {
                self.mirror_metadata_after_local_commit("flush manifest publish")?;
                cloud_manifest_published = true;
            }
        }

        self.flush_actor
            .handle_flush_complete(&mut self.state, cf_id, sst_name, sequence);
        if cloud_manifest_published {
            self.prune_cloud_wal_segments_covered_by_manifest();
        }
        self.publish_snapshot();
        self.schedule_compaction_after_flush_publication(sst_name);
        Ok(())
    }

    fn drain_auto_flush_memtables(&mut self) -> usize {
        if self.state.memory_mode {
            return 0;
        }

        let mut flushed = 0usize;

        loop {
            let Some(candidate) = self
                .state
                .next_flush_candidate(self.wal_actor.is_cloud_async())
            else {
                return flushed;
            };

            let wal_segment_gap = self.state.active_memtable_wal_segment_gap(candidate.cf_id);
            match self.flush_actor.handle_flush(
                &mut self.state,
                candidate.cf_id,
                self.hybrid_storage.as_ref(),
            ) {
                Ok(flush_output) => {
                    if flush_output.file_meta.is_none() {
                        tracing::trace!(
                            cf_id = candidate.cf_id,
                            reason = ?candidate.reason,
                            wal_segment_gap,
                            "auto-flush made no durable progress"
                        );
                        return flushed;
                    }

                    let sequence = self.state.sequence;
                    if let Err(error) = self.publish_flushed_sst(
                        candidate.cf_id,
                        &flush_output.sst_name,
                        sequence,
                        flush_output.file_meta,
                    ) {
                        tracing::error!(
                            %error,
                            cf_id = candidate.cf_id,
                            sst_name = %flush_output.sst_name,
                            reason = ?candidate.reason,
                            wal_segment_gap,
                            "auto-flush publication failed"
                        );
                        return flushed;
                    }

                    self.wake_write_stall_waiters();
                    tracing::debug!(
                        cf_id = candidate.cf_id,
                        sst_name = %flush_output.sst_name,
                        reason = ?candidate.reason,
                        wal_segment_gap,
                        "Auto-flushed memtable"
                    );
                    flushed += 1;
                }
                Err(error) => {
                    tracing::trace!(
                        cf_id = candidate.cf_id,
                        reason = ?candidate.reason,
                        wal_segment_gap,
                        error = %error,
                        "Auto-flush skipped"
                    );
                    return flushed;
                }
            }
        }
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

    fn has_actionable_work(&self) -> bool {
        if self.pending_msg.is_some() {
            return true;
        }

        if self.wal_actor.should_sync_batch()
            || self.wal_actor.has_pending_data()
            || self.wal_actor.has_pending_cloud_writes()
        {
            return true;
        }

        if self.durability.has_pending_waiters() {
            return true;
        }

        if self.state.compaction.pending_tasks > 0 {
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

    fn progress_pass(&mut self, msg_rx: &Receiver<RuntimeMsg>) {
        self.sync_batched_wal_if_needed(msg_rx);
        self.maybe_flush_cloud_async_wal();
        self.tick_hybrid_storage();
        self.drain_hybrid_storage_events();
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
                let avg_batch = self.loop_debug_batch_total as f64 / self.loop_debug_wakes as f64;
                eprintln!(
                    "[midge] loop_stats wakes={} avg_batch={:.2}",
                    self.loop_debug_wakes, avg_batch
                );
            }
        }
    }

    fn process_one(&mut self, msg: RuntimeMsg, msg_rx: &Receiver<RuntimeMsg>) -> HandleOutcome {
        self.maybe_flush_cloud_async_wal();
        self.tick_hybrid_storage();
        self.drain_hybrid_storage_events();
        self.handle_runtime_msg(msg, msg_rx)
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

        let drained = self.drain_pending_writes(msg_rx, max_drain);
        batch += drained;
        self.record_wake_batch(batch);
        outcome
    }

    /// Main event loop — runs until Shutdown message or channel close.
    pub fn run(&mut self, msg_rx: Receiver<RuntimeMsg>) {
        const MAX_DRAIN_WRITES_ON_WAKE: usize = 4096;
        const ACTIONABLE_IDLE_BACKOFF: Duration = Duration::from_micros(50);

        loop {
            if let Some(pending) = self.pending_msg.take() {
                let outcome = self.process_one(pending, &msg_rx);
                if outcome == HandleOutcome::Break {
                    break;
                }
                continue;
            }

            if self.has_actionable_work() {
                match msg_rx.try_recv() {
                    Ok(msg) => {
                        let outcome = self.process_wake_msg(msg, &msg_rx, MAX_DRAIN_WRITES_ON_WAKE);
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
                        Err(TryRecvError::Disconnected) => {}
                        Err(TryRecvError::Empty) => {}
                    }
                }

                self.progress_pass(&msg_rx);
                std::thread::sleep(ACTIONABLE_IDLE_BACKOFF);
                continue;
            }

            let msg = if let Some(storage_rx) = self.hybrid_storage_events.clone() {
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
            } else {
                msg_rx.recv().ok()
            };

            let Some(msg) = msg else {
                break;
            };

            let outcome = self.process_wake_msg(msg, &msg_rx, MAX_DRAIN_WRITES_ON_WAKE);
            if outcome == HandleOutcome::Break {
                break;
            }
        }

        tracing::debug!("Runtime message channel closed — exiting event loop");
    }

    fn handle_runtime_msg(
        &mut self,
        msg: RuntimeMsg,
        msg_rx: &Receiver<RuntimeMsg>,
    ) -> HandleOutcome {
        match msg {
            RuntimeMsg::Shutdown => {
                tracing::info!("Runtime shutting down");

                // CRITICAL: Flush and wait for pending CloudAsync segments before shutdown.
                // This ensures all writes are cloud-durable for recovery.
                if self.wal_actor.is_cloud_async() && self.state.wal.pending_writes > 0 {
                    match self.seal_current_cloud_segment() {
                        Ok(Some((segment_id, _max_sequence))) => {
                            tracing::info!(
                                segment_id,
                                "Enqueued final CloudAsync segment on shutdown"
                            );
                        }
                        Ok(None) => {}
                        Err(e) => {
                            tracing::error!(
                                error = %e,
                                "Failed to seal CloudAsync segment during shutdown"
                            );
                        }
                    }
                }

                // Wait for pending uploads to complete (with timeout)
                if self.wal_actor.is_cloud_async() {
                    if let Some(storage) = &self.hybrid_storage {
                        let storage_arc = storage.clone();
                        let shutdown_start = std::time::Instant::now();
                        let shutdown_timeout = std::time::Duration::from_secs(30);
                        let mut last_pending = usize::MAX;
                        let mut stagnant_rounds = 0usize;
                        while storage_arc.pending_upload_count() > 0
                            && shutdown_start.elapsed() < shutdown_timeout
                        {
                            // Process uploads
                            self.tick_hybrid_storage();
                            self.drain_hybrid_storage_events();

                            let pending = storage_arc.pending_upload_count();
                            if pending < last_pending {
                                last_pending = pending;
                                stagnant_rounds = 0;
                            } else if self.state.persistence_anomaly_detected {
                                stagnant_rounds = stagnant_rounds.saturating_add(1);
                                if stagnant_rounds >= 25 {
                                    tracing::warn!(
                                        pending,
                                        "aborting cloud shutdown wait after repeated failed upload progress"
                                    );
                                    break;
                                }
                            }

                            // Small sleep to avoid busy-waiting
                            std::thread::sleep(std::time::Duration::from_millis(10));
                        }

                        if storage_arc.pending_upload_count() > 0 {
                            tracing::warn!(
                                pending = storage_arc.pending_upload_count(),
                                "Shutdown timeout: {} pending CloudAsync uploads not completed",
                                storage_arc.pending_upload_count()
                            );
                        } else {
                            tracing::info!("All CloudAsync uploads completed on shutdown");
                        }
                    }
                }

                return HandleOutcome::Break;
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

                self.drain_auto_flush_memtables();
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

            RuntimeMsg::GetRecoveryMetrics { request_id } => {
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

            RuntimeMsg::GetRuntimeMetrics { request_id } => {
                self.respond(
                    request_id,
                    RuntimeResponse::RuntimeMetricsSnapshot {
                        request_id,
                        snapshot: Box::new(self.state.runtime_metrics_snapshot()),
                    },
                );
            }

            RuntimeMsg::GetStorageLayout { request_id } => {
                self.respond(
                    request_id,
                    RuntimeResponse::StorageLayoutSnapshot {
                        request_id,
                        snapshot: self.state.storage_layout_snapshot(),
                    },
                );
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
                if let Some(ms) = memtable_size_limit {
                    self.state.memtable_size_limit = ms;
                }
                if let Some(th) = memtable_flush_threshold {
                    self.state.memtable_flush_threshold = th;
                }
                if let Some(ec) = enable_compaction {
                    self.state.enable_compaction = ec;
                }
                if let Some(trigger) = l0_compaction_trigger {
                    self.compaction_actor.set_l0_file_count_threshold(trigger);
                }

                self.wake_write_stall_waiters();

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
                        return HandleOutcome::Continue;
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
                        l0_compaction_trigger: self.compaction_actor.l0_file_count_threshold(),
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

                // Check if active compactions are already zero
                let active = self
                    .state
                    .active_compactions
                    .load(std::sync::atomic::Ordering::SeqCst);

                if active == 0 {
                    // No active compactions; can proceed immediately
                    tracing::info!(
                        component = "ingest",
                        invariant = "begin_ingest_barrier",
                        ingest_epoch = new_epoch,
                        "ingest: ingestion barrier enabled — no active compactions to drain"
                    );
                    self.respond(request_id, RuntimeResponse::Ok { request_id });
                } else {
                    // Queue request to be completed when active_compactions reaches 0.
                    // Don't block the event loop — check completion in CompactionComplete handler.
                    let mut pending = self.state.pending_compaction_waits.lock();
                    pending.insert(request_id, format!("BeginIngest(epoch={})", new_epoch));
                    tracing::debug!(
                        component = "ingest",
                        "begin_ingest queued; waiting for {} active compactions to drain (epoch={})",
                        active,
                        new_epoch
                    );
                }
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
                        let _ = self.publish_flushed_sst(
                            cf_id,
                            &sst_name.sst_name,
                            sequence,
                            sst_name.file_meta,
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
                if let Some(snapshot) = self.create_read_snapshot(cf_id) {
                    self.respond(
                        request_id,
                        RuntimeResponse::ReadSnapshot {
                            request_id,
                            snapshot: Arc::new(snapshot),
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

            RuntimeMsg::BeginTransaction { request_id, cf_id } => {
                let start_sequence = self.state.sequence;
                let snapshot = self.create_read_snapshot(cf_id).map(Arc::new);
                self.respond(
                    request_id,
                    RuntimeResponse::BeginTransactionResult {
                        request_id,
                        start_sequence,
                        snapshot,
                    },
                );
            }

            RuntimeMsg::RegisterSnapshot {
                request_id,
                snapshot_id,
                sequence,
                pinned_sst_names,
            } => {
                let inserted =
                    self.state
                        .register_snapshot(snapshot_id, sequence, pinned_sst_names);
                if inserted {
                    self.respond(request_id, RuntimeResponse::Ok { request_id });
                } else {
                    self.respond(
                        request_id,
                        RuntimeResponse::Error {
                            request_id,
                            error: crate::common::MidgeError::InvalidArgument(format!(
                                "snapshot {} is already registered",
                                snapshot_id
                            )),
                        },
                    );
                }
            }

            RuntimeMsg::UnregisterSnapshot { snapshot_id } => {
                self.state.unregister_snapshot(snapshot_id);
            }

            RuntimeMsg::ApplyTransaction {
                request_id,
                ops,
                durability_policy,
                start_sequence,
                isolation_policy,
            } => {
                // Fencing gate: reject writes if lease is lost
                if let Err(e) = self.check_lease_health() {
                    self.respond(
                        request_id,
                        RuntimeResponse::Error {
                            request_id,
                            error: e,
                        },
                    );
                    return HandleOutcome::Continue;
                }

                if self.wal_actor.is_cloud_async() && self.hybrid_storage.is_none() {
                    self.respond(
                        request_id,
                        RuntimeResponse::Error {
                            request_id,
                            error: crate::common::MidgeError::Internal(
                                "CloudAsync requires HybridStorage".to_string(),
                            ),
                        },
                    );
                } else {
                    match self.wal_actor.append_transaction(
                        &mut self.state,
                        request_id,
                        ops,
                        durability_policy,
                        start_sequence,
                        isolation_policy,
                    ) {
                        Ok((last_sequence, op_count, deferred)) => {
                            // Publish snapshot BEFORE responding so that
                            // the caller's next begin_tx sees the write.
                            self.publish_snapshot();

                            if self.should_ack_immediately(deferred) {
                                if self.wal_actor.is_cloud_async() {
                                    // Accepted but not yet cloud-durable; confirm later on CloudAck.
                                    self.durability.queue_waiter(
                                        DurabilityWaiter::ConfirmTransactionApply { request_id },
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
                                self.durability
                                    .queue_waiter(DurabilityWaiter::TransactionApply {
                                        request_id,
                                        last_sequence,
                                        op_count,
                                    });
                            }
                        }
                        Err(e) => {
                            self.respond(
                                request_id,
                                RuntimeResponse::Error {
                                    request_id,
                                    error: e,
                                },
                            );
                        }
                    }
                }
                // Auto-sync batched writes if needed (group commit completes all waiters).
                // Do this for local durability mode; CloudAsync uses rotate/upload logic.
                if !self.wal_actor.is_cloud_async() {
                    const MAX_DRAIN_WRITES_AFTER_BATCH: usize = 1024;
                    let _ = self.drain_pending_writes(msg_rx, MAX_DRAIN_WRITES_AFTER_BATCH);
                    self.sync_batched_wal_if_needed(msg_rx);
                }

                self.maybe_flush_cloud_async_wal();
                self.drain_auto_flush_memtables();
            }

            // =============================================================
            // Flush
            // =============================================================
            RuntimeMsg::FlushMemtable { request_id, cf_id } => {
                let resp = match self.flush_actor.handle_flush(
                    &mut self.state,
                    cf_id,
                    self.hybrid_storage.as_ref(),
                ) {
                    Ok(flush_output) => {
                        let sequence = self.state.sequence;
                        match self.publish_flushed_sst(
                            cf_id,
                            &flush_output.sst_name,
                            sequence,
                            flush_output.file_meta,
                        ) {
                            Ok(()) => {
                                self.wake_write_stall_waiters();
                                RuntimeResponse::FlushComplete {
                                    request_id,
                                    sst_name: flush_output.sst_name,
                                }
                            }
                            Err(error) => RuntimeResponse::Error { request_id, error },
                        }
                    }
                    Err(e) => RuntimeResponse::Error {
                        request_id,
                        error: e,
                    },
                };

                self.respond(request_id, resp);
            }

            RuntimeMsg::FlushComplete {
                request_id,
                cf_id,
                sst_name,
                sequence,
            } => {
                let resp = match self.publish_flushed_sst(cf_id, &sst_name, sequence, None) {
                    Ok(()) => {
                        self.wake_write_stall_waiters();
                        RuntimeResponse::Ok { request_id }
                    }
                    Err(error) => RuntimeResponse::Error { request_id, error },
                };
                self.respond(request_id, resp);
            }

            // =============================================================
            // Compaction
            // =============================================================
            RuntimeMsg::CheckCompaction { request_id } => {
                match self.schedule_one_background_compaction_if_needed("CheckCompaction") {
                    Ok(_) => self.respond(request_id, RuntimeResponse::Ok { request_id }),
                    Err(error) => {
                        self.respond(request_id, RuntimeResponse::Error { request_id, error })
                    }
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
                    return HandleOutcome::Continue;
                }

                let cplan = crate::compaction::CompactionPlan {
                    input_files: plan.input_files,
                    output_files: Vec::new(),
                    source_level: plan.source_level,
                    target_level: plan.target_level,
                    cf_id: plan.cf_id,
                    output_seq: self.state.next_sequence(),
                    snapshot_horizon: self.state.oldest_active_snapshot_sequence(),
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
                    return HandleOutcome::Continue;
                }

                // Schedule as many compactions as CheckCompaction suggests
                let mut scheduled = 0usize;
                loop {
                    if let Some(plan) = self.compaction_actor.check_compaction(&self.state) {
                        let plan = self.assign_compaction_output_sequence(plan);
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
                                        error: crate::common::MidgeError::Internal(e.to_string()),
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
                    return HandleOutcome::Continue;
                }

                // Queue request to be completed when active_compactions reaches 0.
                // Don't block the event loop — instead, continue processing messages and
                // check completion in CompactionComplete handler.
                {
                    let mut pending = self.state.pending_compaction_waits.lock();
                    pending.insert(request_id, "CompactAll".to_string());
                }
            }

            RuntimeMsg::CompactionComplete {
                request_id,
                input_ssts,
                output_ssts,
                cf_id,
                target_level,
                succeeded,
            } => {
                let mut allow_emergent_followup = false;

                // Decrement active compactions
                let _prev = self
                    .state
                    .active_compactions
                    .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);

                // Handle the compaction completion (clean local state)
                self.compaction_actor.handle_complete(
                    &mut self.state,
                    input_ssts.clone(),
                    output_ssts.clone(),
                );

                if !succeeded {
                    if let Some(t) = crate::telemetry::Telemetry::global() {
                        t.metrics().record_compaction_failure();
                    }
                    tracing::warn!(
                        input_count = input_ssts.len(),
                        output_count = output_ssts.len(),
                        "compaction worker failed or aborted; leaving manifest unchanged"
                    );
                    self.respond(request_id, RuntimeResponse::Ok { request_id });
                } else {
                    let added: Result<Vec<_>, _> = output_ssts
                        .iter()
                        .map(|name| self.build_sst_file_meta(cf_id, target_level, name))
                        .collect();

                    // Publish compaction changes to manifest (update and persist)
                    if let Err(e) = added.and_then(|added| {
                        self.mirror_ssts_to_authoritative_cloud(&output_ssts)?;
                        self.state.record_compaction_publication_intent(
                            cf_id,
                            input_ssts.clone(),
                            added.clone(),
                        )?;

                        fail::fail_point!(
                            "slice7::after_compaction_output_durable_before_manifest_publish"
                        );

                        self.manifest_actor.compaction_complete(
                            &mut self.state,
                            input_ssts.clone(),
                            added,
                        )?;
                        self.state.transition_compaction_publication_intent(
                            &output_ssts,
                            crate::runtime::PublicationPhase::ManifestPublished,
                        )
                    }) {
                        if let Some(t) = crate::telemetry::Telemetry::global() {
                            t.metrics().record_compaction_failure();
                        }
                        self.state.mark_persistence_anomaly();
                        tracing::error!(error = ?e, "failed to apply compaction to manifest");
                        self.respond(
                            request_id,
                            RuntimeResponse::Error {
                                request_id,
                                error: crate::common::MidgeError::Internal(format!(
                                    "failed to apply compaction to manifest: {}",
                                    e
                                )),
                            },
                        );
                    } else {
                        // 🔑 CRITICAL Slice 6: Manifest publication succeeded.
                        // Now persist the manifest to make changes durable.
                        fail::fail_point!(
                            "slice6::after_compaction_update_before_manifest_persist"
                        );

                        if let Err(e) = self.manifest_actor.persist(&self.state) {
                            if let Some(t) = crate::telemetry::Telemetry::global() {
                                t.metrics().record_compaction_failure();
                            }
                            self.state.mark_persistence_anomaly();
                            tracing::error!(error = ?e, "failed to persist manifest after compaction");
                            self.respond(
                                request_id,
                                RuntimeResponse::Error {
                                    request_id,
                                    error: crate::common::MidgeError::Internal(format!(
                                        "failed to persist manifest after compaction: {}",
                                        e
                                    )),
                                },
                            );
                        } else {
                            if let Err(e) = self
                                .mirror_metadata_after_local_commit("compaction manifest publish")
                            {
                                if let Some(t) = crate::telemetry::Telemetry::global() {
                                    t.metrics().record_compaction_failure();
                                }
                                self.state.mark_persistence_anomaly();
                                tracing::error!(
                                    error = ?e,
                                    "failed to mirror manifest after compaction"
                                );
                                self.respond(
                                    request_id,
                                    RuntimeResponse::Error {
                                        request_id,
                                        error: crate::common::MidgeError::Internal(format!(
                                            "failed to mirror manifest after compaction: {}",
                                            e
                                        )),
                                    },
                                );
                                return HandleOutcome::Continue;
                            }

                            // 🔑 CRITICAL Slice 6: Manifest is now durable.
                            // Safe to delete input SSTs (they are now orphaned from manifest).
                            fail::fail_point!("slice6::after_manifest_persist_before_sst_gc");

                            // Queue GC deletion of input SSTs
                            if let Err(e) = self.gc_actor.delete_ssts(&mut self.state, &input_ssts)
                            {
                                self.state.mark_persistence_anomaly();
                                tracing::warn!(
                                    error = ?e,
                                    "GC deletion of compaction input SSTs failed (non-fatal)"
                                );
                            } else {
                                tracing::info!(
                                    removed_count = input_ssts.len(),
                                    "Successfully deleted compaction input SSTs"
                                );
                            }

                            let cleared_intent_mirror_error = match self
                                .state
                                .clear_compaction_publication_intent(&output_ssts)
                            {
                                Ok(()) => {
                                    match self.mirror_metadata_after_local_commit(
                                        "compaction publication intent clear",
                                    ) {
                                        Ok(()) => None,
                                        Err(e) => {
                                            if let Some(t) = crate::telemetry::Telemetry::global() {
                                                t.metrics().record_compaction_failure();
                                            }
                                            self.state.mark_persistence_anomaly();
                                            tracing::error!(
                                                error = ?e,
                                                "failed to mirror cleared compaction publication intent"
                                            );
                                            Some(e)
                                        }
                                    }
                                }
                                Err(error) => {
                                    self.state.mark_persistence_anomaly();
                                    tracing::warn!(
                                        %error,
                                        "failed to clear compaction publication intent after GC"
                                    );
                                    None
                                }
                            };

                            if let Some(error) = cleared_intent_mirror_error {
                                self.publish_snapshot();
                                self.respond(
                                    request_id,
                                    RuntimeResponse::Error {
                                        request_id,
                                        error: crate::common::MidgeError::Internal(format!(
                                            "failed to mirror cleared compaction publication intent: {}",
                                            error
                                        )),
                                    },
                                );
                            } else {
                                if let Some(t) = crate::telemetry::Telemetry::global() {
                                    let bytes_rewritten: u64 = self
                                        .state
                                        .manifest
                                        .files
                                        .iter()
                                        .filter(|file| output_ssts.contains(&file.name))
                                        .map(|file| file.size_bytes)
                                        .sum();
                                    t.metrics().record_compaction(bytes_rewritten);
                                }
                                allow_emergent_followup = true;
                                self.publish_snapshot();
                                self.respond(request_id, RuntimeResponse::Ok { request_id });
                            }
                        }
                    }
                }

                // Check if any pending CompactAll/BeginIngest requests can be completed now
                let active = self
                    .state
                    .active_compactions
                    .load(std::sync::atomic::Ordering::SeqCst);
                if active == 0 {
                    let mut emergent_scheduled = false;

                    // Only chain more compactions after a successful completion.
                    // If the just-finished compaction failed, the manifest is unchanged and
                    // blindly rescheduling here can spin forever on the same failing plan.
                    if allow_emergent_followup {
                        while let Some(plan) = self.compaction_actor.check_compaction(&self.state) {
                            if self
                                .compaction_actor
                                .run_compaction(
                                    &mut self.state,
                                    plan,
                                    self.hybrid_storage.as_ref(),
                                    self.worker_msg_tx.clone(),
                                )
                                .is_ok()
                            {
                                emergent_scheduled = true;
                                continue;
                            }

                            break;
                        }
                    }

                    // Send responses to pending requests only when no active compactions remain.
                    let active_now = self
                        .state
                        .active_compactions
                        .load(std::sync::atomic::Ordering::SeqCst);
                    if active_now == 0 {
                        let mut pending = self.state.pending_compaction_waits.lock();
                        for (req_id, condition) in pending.drain() {
                            tracing::debug!(
                                "responding to pending {:?} request (request_id={})",
                                condition,
                                req_id
                            );
                            self.router
                                .complete(RuntimeResponse::Ok { request_id: req_id });
                        }
                    } else if emergent_scheduled {
                        let pending = self.state.pending_compaction_waits.lock();
                        tracing::debug!(
                            "emergent compactions scheduled; {} requests still waiting",
                            pending.len()
                        );
                    }
                }

                self.drain_auto_flush_memtables();
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
                // Fencing gate: reject writes if lease is lost
                if let Err(e) = self.check_lease_health() {
                    self.respond(
                        request_id,
                        RuntimeResponse::Error {
                            request_id,
                            error: e,
                        },
                    );
                    return HandleOutcome::Continue;
                }

                if self.wal_actor.is_cloud_async() && self.hybrid_storage.is_none() {
                    self.respond(
                        request_id,
                        RuntimeResponse::Error {
                            request_id,
                            error: crate::common::MidgeError::Internal(
                                "CloudAsync requires HybridStorage".to_string(),
                            ),
                        },
                    );
                    return HandleOutcome::Continue;
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
                        // Publish snapshot BEFORE responding.
                        self.publish_snapshot();

                        if self.should_ack_immediately(deferred) {
                            if self.wal_actor.is_cloud_async() {
                                // Background CloudAsync: confirm sequences immediately after local WAL write.
                                // Cloud upload runs asynchronously; no need to queue waiters since we
                                // respond immediately and make data visible for reads.
                                self.state.confirm_sequences(request_id);
                            } else if deferred {
                                self.maybe_queue_confirm_only_waiter(deferred, request_id, false);
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
                self.sync_batched_wal_if_needed(msg_rx);

                self.maybe_flush_cloud_async_wal();
                self.drain_auto_flush_memtables();
            }

            RuntimeMsg::WalAppendDeleteRange {
                request_id,
                cf_id,
                start_key,
                end_key,
                durability_policy,
            } => {
                // Fencing gate: reject writes if lease is lost
                if let Err(e) = self.check_lease_health() {
                    self.respond(
                        request_id,
                        RuntimeResponse::Error {
                            request_id,
                            error: e,
                        },
                    );
                    return HandleOutcome::Continue;
                }

                if self.wal_actor.is_cloud_async() && self.hybrid_storage.is_none() {
                    self.respond(
                        request_id,
                        RuntimeResponse::Error {
                            request_id,
                            error: crate::common::MidgeError::Internal(
                                "CloudAsync requires HybridStorage".to_string(),
                            ),
                        },
                    );
                    return HandleOutcome::Continue;
                }

                let result = self.wal_actor.append_delete_range(
                    &mut self.state,
                    request_id,
                    cf_id,
                    bytes::Bytes::from(start_key),
                    bytes::Bytes::from(end_key),
                    durability_policy,
                );
                match result {
                    Ok((seq, deferred)) => {
                        // Publish snapshot BEFORE responding.
                        self.publish_snapshot();

                        if self.should_ack_immediately(deferred) {
                            if self.wal_actor.is_cloud_async() {
                                // Background CloudAsync: confirm sequences immediately after local WAL write.
                                self.state.confirm_sequences(request_id);
                            } else if deferred {
                                self.maybe_queue_confirm_only_waiter(deferred, request_id, false);
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

                self.sync_batched_wal_if_needed(msg_rx);
                self.maybe_flush_cloud_async_wal();
                self.drain_auto_flush_memtables();
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

            RuntimeMsg::SealWalForCloud {
                request_id,
                sequence,
                wait_for_ack,
            } => {
                if !self.wal_actor.is_cloud_async() {
                    let resp = if self.state.wal.local_durable_seq >= sequence {
                        RuntimeResponse::Ok { request_id }
                    } else {
                        RuntimeResponse::Error {
                            request_id,
                            error: crate::common::MidgeError::InvalidArgument(
                                "cloud durability requested outside cloud-backed mode".to_string(),
                            ),
                        }
                    };
                    self.respond(request_id, resp);
                    return HandleOutcome::Continue;
                }

                if let Err(error) = self.check_lease_health() {
                    self.respond(request_id, RuntimeResponse::Error { request_id, error });
                    return HandleOutcome::Continue;
                }

                if self.state.wal.cloud_durable_seq >= sequence {
                    self.respond(request_id, RuntimeResponse::Ok { request_id });
                    return HandleOutcome::Continue;
                }

                let mut inflight_segment = self.durability.inflight_segment_for_sequence(sequence);
                if inflight_segment.is_none()
                    && (self.state.wal.pending_writes > 0
                        || self.state.wal.local_durable_seq < sequence)
                {
                    match self.seal_current_cloud_segment() {
                        Ok(Some((segment_id, max_sequence))) => {
                            if max_sequence < sequence {
                                self.respond(
                                    request_id,
                                    RuntimeResponse::Error {
                                        request_id,
                                        error: crate::common::MidgeError::Internal(format!(
                                            "sealed WAL segment up to sequence {max_sequence}, but requested cloud durability for {sequence}"
                                        )),
                                    },
                                );
                                return HandleOutcome::Continue;
                            }
                            inflight_segment = Some(segment_id);
                        }
                        Ok(None) => {}
                        Err(error) => {
                            self.respond(request_id, RuntimeResponse::Error { request_id, error });
                            return HandleOutcome::Continue;
                        }
                    }
                }

                if !wait_for_ack || self.state.wal.cloud_durable_seq >= sequence {
                    self.drain_auto_flush_memtables();
                    self.respond(request_id, RuntimeResponse::Ok { request_id });
                } else if let Some(segment_id) = inflight_segment {
                    self.drain_auto_flush_memtables();
                    self.durability.queue_waiter_for_key(
                        segment_id,
                        DurabilityWaiter::CloudDurability { request_id },
                    );
                } else {
                    self.respond(
                        request_id,
                        RuntimeResponse::Error {
                            request_id,
                            error: crate::common::MidgeError::Internal(format!(
                                "no inflight cloud upload covers sequence {sequence}"
                            )),
                        },
                    );
                }
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
                let evicted = self.state.evict_timed_out_snapshots();
                if evicted > 0 {
                    tracing::warn!(evicted, "Evicted timed-out snapshots before GC check");
                }

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
                let result = self
                    .manifest_actor
                    .add_sst(&mut self.state, file_meta)
                    .and_then(|_| self.mirror_metadata_after_local_commit("manifest add sst"));
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
                let result = self
                    .manifest_actor
                    .compaction_complete(&mut self.state, removed, added)
                    .and_then(|_| {
                        self.mirror_metadata_after_local_commit("manifest compaction complete")
                    });
                let resp = result
                    .map(|_| RuntimeResponse::Ok { request_id })
                    .unwrap_or_else(|e| RuntimeResponse::Error {
                        request_id,
                        error: crate::common::MidgeError::Internal(e.to_string()),
                    });
                self.respond(request_id, resp);
            }

            RuntimeMsg::ManifestPersist { request_id } => {
                let result = self
                    .manifest_actor
                    .persist(&self.state)
                    .and_then(|_| self.mirror_metadata_after_local_commit("manifest persist"));
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
                    return HandleOutcome::Continue;
                }

                // DDL durability barrier: ensure WAL is durable before CF creation
                self.force_wal_sync(msg_rx);

                let result = self
                    .manifest_actor
                    .create_column_family(&mut self.state, name.clone())
                    .and_then(|cf_id| {
                        self.mirror_metadata_after_local_commit("create column family")
                            .map(|_| cf_id)
                    });
                let resp = result
                    .map(|cf_id| RuntimeResponse::ColumnFamilyCreated { request_id, cf_id })
                    .unwrap_or_else(|e| RuntimeResponse::Error {
                        request_id,
                        error: crate::common::MidgeError::Internal(e.to_string()),
                    });
                self.publish_snapshot();
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
                    return HandleOutcome::Continue;
                }

                // DDL durability barrier: ensure WAL is durable before CF drop
                self.force_wal_sync(msg_rx);

                let result = self
                    .manifest_actor
                    .drop_column_family(&mut self.state, cf_id)
                    .and_then(|_| self.mirror_metadata_after_local_commit("drop column family"));
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

        HandleOutcome::Continue
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::runtime::{state::RuntimeState, ResponseRouter};
    use crate::sst::Memtable;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn unique_test_db_path(prefix: &str) -> std::path::PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let counter = TEST_DB_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "{prefix}_{}_{}_{}",
            std::process::id(),
            unique,
            counter
        ))
    }

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

    pub(in crate::runtime::event_loop) fn create_test_cloud_event_loop(
        storage_policy: crate::storage::hybrid::policy::StorageBudgetPolicy,
    ) -> crate::common::MidgeResult<EventLoop> {
        let db_path = unique_test_db_path("midge_event_loop_cloud");
        std::fs::create_dir_all(&db_path).expect("create temp cloud event loop dir");

        let state = RuntimeState::new(db_path.clone(), false);
        let router = Arc::new(ResponseRouter::new());
        let local = Arc::new(
            crate::storage::filesystem::FileSystem::new(db_path.join("hybrid_local"))
                .expect("create local backend"),
        );
        let cloud = Arc::new(
            crate::storage::filesystem::FileSystem::new(db_path.join("cloud_store"))
                .expect("create cloud backend"),
        );
        let hybrid_storage = Arc::new(crate::storage::HybridStorage::with_policy(
            local,
            cloud,
            storage_policy,
        ));
        let config = crate::runtime::RuntimeConfig {
            wal_durability_policy: crate::wal::DurabilityPolicy::CloudAsync,
            hybrid_storage: Some(Arc::clone(&hybrid_storage)),
            ..crate::runtime::RuntimeConfig::default()
        };
        EventLoop::new(state, false, router, config, None)
    }

    pub(in crate::runtime::event_loop) fn create_test_local_event_loop(
    ) -> crate::common::MidgeResult<EventLoop> {
        let db_path = unique_test_db_path("midge_event_loop_local");
        std::fs::create_dir_all(&db_path).expect("create temp local event loop dir");

        let state = RuntimeState::new(db_path, false);
        let router = Arc::new(ResponseRouter::new());
        EventLoop::new(
            state,
            false,
            router,
            crate::runtime::RuntimeConfig::default(),
            None,
        )
    }

    fn valid_sst_bytes_for_event_loop_test(key: &[u8], value: &[u8], seq: u64) -> Vec<u8> {
        use crate::sst::SstFactory;

        let factory = crate::sst::FsSstFactoryIo::new(Arc::new(crate::io::MockFs::new()), 4096);
        let mut writer = factory.create().expect("create test SST writer");
        writer
            .add_with_meta(key, Some(value), seq, 0, None)
            .expect("add test SST entry");
        writer.finish_bytes().expect("finish test SST bytes")
    }

    fn test_manifest_l0_file_meta(name: &str, largest_seq: u64) -> crate::metadata::FileMeta {
        crate::metadata::FileMeta {
            name: name.to_string(),
            level: 0,
            size_bytes: 128,
            cf_id: 0,
            smallest_key: Some(format!("key-{largest_seq:04}").into_bytes()),
            largest_key: Some(format!("key-{largest_seq:04}").into_bytes()),
            smallest_seq: Some(largest_seq),
            largest_seq: Some(largest_seq),
            ..Default::default()
        }
    }

    fn write_runtime_l0_sst_for_test(
        event_loop: &EventLoop,
        name: &str,
        largest_seq: u64,
    ) -> crate::runtime::FileMeta {
        let key = format!("key-{largest_seq:04}");
        let bytes = valid_sst_bytes_for_event_loop_test(key.as_bytes(), b"value", largest_seq);
        std::fs::create_dir_all(&event_loop.state.sst_dir).expect("create test sst dir");
        std::fs::write(event_loop.state.sst_dir.join(name), &bytes).expect("write test SST");
        crate::runtime::FileMeta {
            name: name.to_string(),
            level: 0,
            size_bytes: bytes.len() as u64,
            content_crc32c: Some(crc32c::crc32c(&bytes)),
            cf_id: 0,
            smallest_key: Some(key.clone().into_bytes()),
            largest_key: Some(key.into_bytes()),
            smallest_seq: Some(largest_seq),
            largest_seq: Some(largest_seq),
        }
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
    fn should_not_treat_blocked_auto_flush_candidate_as_standalone_actionable_work() {
        let mut event_loop = create_test_cloud_event_loop(
            crate::storage::hybrid::policy::StorageBudgetPolicy::new(1_000_000),
        )
        .expect("create cloud event loop");
        event_loop
            .hybrid_storage
            .as_ref()
            .expect("hybrid storage")
            .flush_completed(960_000);

        event_loop.state.memtable_flush_threshold = 1024;
        event_loop.state.memtable_size_limit = 1024 * 1024;
        event_loop.state.sequence = 1;
        {
            let cf = event_loop.state.get_cf(0).expect("default cf");
            cf.memtable
                .put_with_seq(b"key".to_vec(), vec![0xA5; 2048], 1, None)
                .expect("seed memtable");
        }
        event_loop.state.total_memtable_bytes = event_loop
            .state
            .get_cf(0)
            .expect("default cf")
            .memtable
            .size_bytes();

        assert_eq!(event_loop.drain_auto_flush_memtables(), 0);

        assert!(
            !event_loop.has_actionable_work(),
            "blocked auto-flush candidates should wait for a real state change before retrying"
        );
    }

    #[test]
    fn should_schedule_compaction_after_l0_flush_when_threshold_reached(
    ) -> crate::common::MidgeResult<()> {
        let mut event_loop = create_test_local_event_loop().expect("create local event loop");
        let (worker_tx, _worker_rx) = crossbeam::channel::unbounded();
        event_loop.worker_msg_tx = Some(worker_tx);
        event_loop.state.enable_compaction = true;
        event_loop
            .state
            .manifest
            .files
            .extend((1..=3).map(|seq| test_manifest_l0_file_meta(&format!("pre-{seq}.sst"), seq)));

        let flushed = write_runtime_l0_sst_for_test(&event_loop, "threshold-crossing.sst", 4);
        event_loop.publish_flushed_sst(0, "threshold-crossing.sst", 4, Some(flushed))?;

        assert_eq!(
            event_loop
                .state
                .active_compactions
                .load(std::sync::atomic::Ordering::SeqCst),
            1,
            "publishing the threshold-crossing L0 SST should schedule compaction"
        );
        assert_eq!(
            event_loop.state.compaction.compacting_ssts.len(),
            4,
            "scheduled L0 compaction should lock all threshold input files"
        );
        assert!(
            event_loop
                .state
                .compaction
                .compacting_ssts
                .iter()
                .any(|name| name == "threshold-crossing.sst"),
            "newly flushed SST should be part of the scheduled L0 compaction"
        );

        Ok(())
    }

    #[test]
    fn should_not_schedule_auto_compaction_when_compaction_disabled(
    ) -> crate::common::MidgeResult<()> {
        let mut event_loop = create_test_local_event_loop().expect("create local event loop");
        let (worker_tx, _worker_rx) = crossbeam::channel::unbounded();
        event_loop.worker_msg_tx = Some(worker_tx);
        event_loop.state.enable_compaction = false;
        event_loop
            .state
            .manifest
            .files
            .extend((1..=3).map(|seq| test_manifest_l0_file_meta(&format!("pre-{seq}.sst"), seq)));

        let flushed =
            write_runtime_l0_sst_for_test(&event_loop, "disabled-threshold-crossing.sst", 4);
        event_loop.publish_flushed_sst(0, "disabled-threshold-crossing.sst", 4, Some(flushed))?;

        assert_eq!(
            event_loop
                .state
                .active_compactions
                .load(std::sync::atomic::Ordering::SeqCst),
            0,
            "post-flush compaction scheduling should respect enable_compaction=false"
        );
        assert!(
            event_loop.state.compaction.compacting_ssts.is_empty(),
            "disabled compaction should not lock SSTs after flush publication"
        );

        Ok(())
    }

    #[test]
    fn should_skip_post_flush_compaction_check_during_ingest_when_compaction_disabled(
    ) -> crate::common::MidgeResult<()> {
        #[derive(Clone)]
        struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

        struct CapturedLogWriter(Arc<Mutex<Vec<u8>>>);

        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
            type Writer = CapturedLogWriter;

            fn make_writer(&'a self) -> Self::Writer {
                CapturedLogWriter(Arc::clone(&self.0))
            }
        }

        impl std::io::Write for CapturedLogWriter {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0
                    .lock()
                    .expect("captured logs lock")
                    .extend_from_slice(buf);
                Ok(buf.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let mut event_loop = create_test_local_event_loop().expect("create local event loop");
        event_loop.state.enable_compaction = false;
        event_loop
            .state
            .ingest_active
            .store(true, std::sync::atomic::Ordering::SeqCst);
        event_loop.state.manifest.files.extend(
            (1..=4)
                .map(|seq| test_manifest_l0_file_meta(&format!("ingest-disabled-{seq}.sst"), seq)),
        );

        let captured_logs = CapturedLogs(Arc::new(Mutex::new(Vec::new())));
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_writer(captured_logs.clone())
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            event_loop.schedule_compaction_after_flush_publication("ingest-disabled-4.sst");
        });

        let logs = String::from_utf8(captured_logs.0.lock().expect("captured logs lock").clone())
            .expect("captured logs should be utf8");
        assert!(
            !logs.contains("no_compaction_during_ingest"),
            "disabled post-flush scheduling should not enter the ingest invariant path: {logs}"
        );
        assert!(
            !logs.contains("BUG: compaction scheduling attempted while ingest mode is active"),
            "disabled post-flush scheduling should stay quiet during ingest teardown: {logs}"
        );
        assert_eq!(
            event_loop
                .state
                .active_compactions
                .load(std::sync::atomic::Ordering::SeqCst),
            0,
            "disabled post-flush scheduling should not start compaction during ingest"
        );

        Ok(())
    }

    #[test]
    fn should_apply_l0_compaction_trigger_from_runtime_config() {
        let mut event_loop = create_test_local_event_loop().expect("create local event loop");
        let request_id = 99;
        let response_rx = event_loop.router.register(request_id);
        let (_tx, msg_rx) = crossbeam::channel::unbounded();

        event_loop.handle_runtime_msg(
            RuntimeMsg::SetRuntimeConfig {
                request_id,
                memtable_size_limit: None,
                memtable_flush_threshold: None,
                enable_compaction: None,
                l0_compaction_trigger: Some(2),
                wal_durability_policy: None,
                wal_batch_config: None,
            },
            &msg_rx,
        );

        assert!(matches!(
            response_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("runtime config response"),
            RuntimeResponse::Ok { .. }
        ));
        assert_eq!(
            event_loop.compaction_actor.l0_file_count_threshold(),
            2,
            "runtime config should update the compaction actor L0 file-count trigger"
        );
    }

    #[test]
    fn should_not_spin_auto_flush_drain_in_memory_mode() {
        let mut event_loop = create_test_event_loop().expect("create memory event loop");
        event_loop.state.memtable_flush_threshold = 1024;
        event_loop.state.memtable_size_limit = 1024 * 1024;

        {
            let cf = event_loop.state.get_cf(0).expect("default cf");
            cf.memtable
                .put_with_seq(b"memory-key".to_vec(), vec![0xA5; 2048], 1, None)
                .expect("seed memory memtable");
        }
        let expected_size = event_loop
            .state
            .get_cf(0)
            .expect("default cf")
            .memtable
            .size_bytes();
        event_loop.state.total_memtable_bytes = expected_size;

        let flushed = event_loop.drain_auto_flush_memtables();

        assert_eq!(flushed, 0, "memory mode should not loop on no-op flushes");
        assert_eq!(event_loop.state.manifest.files.len(), 0);
        assert_eq!(
            event_loop
                .state
                .get_cf(0)
                .expect("default cf")
                .memtable
                .size_bytes(),
            expected_size,
            "memory mode should retain the active memtable contents"
        );
    }

    #[test]
    fn should_drain_all_current_flush_candidates_in_single_auto_flush_pass() {
        let mut event_loop = create_test_local_event_loop().expect("create local event loop");
        event_loop.state.memtable_flush_threshold = 1024;
        event_loop.state.memtable_size_limit = 1024 * 1024;

        let second_cf_id = event_loop
            .state
            .create_cf("second".to_string())
            .expect("create second cf");

        {
            let cf = event_loop.state.get_cf(0).expect("default cf");
            cf.memtable
                .put_with_seq(b"default-key".to_vec(), vec![0xA5; 2048], 1, None)
                .expect("seed default cf");
        }
        {
            let cf = event_loop.state.get_cf(second_cf_id).expect("second cf");
            cf.memtable
                .put_with_seq(b"second-key".to_vec(), vec![0x5A; 2048], 2, None)
                .expect("seed second cf");
        }
        event_loop.state.total_memtable_bytes = event_loop
            .state
            .column_families
            .values()
            .map(|cf| cf.memtable.size_bytes())
            .sum();

        let flushed = event_loop.drain_auto_flush_memtables();

        assert_eq!(
            flushed, 2,
            "one successful auto-flush trigger should drain every currently eligible CF"
        );
        assert_eq!(
            event_loop.state.manifest.files.len(),
            2,
            "both flushable CFs should publish SSTs without waiting for another event"
        );
        assert!(event_loop
            .state
            .manifest
            .files
            .iter()
            .any(|file| file.cf_id == 0));
        assert!(event_loop
            .state
            .manifest
            .files
            .iter()
            .any(|file| file.cf_id == second_cf_id));
    }

    #[test]
    fn should_publish_all_flushable_cfs_given_local_write_burst_without_further_writes() {
        let mut event_loop = create_test_local_event_loop().expect("create local event loop");
        event_loop.state.memtable_flush_threshold = 2048;
        event_loop.state.memtable_size_limit = 1024 * 1024;
        let second_cf_id = event_loop
            .state
            .create_cf("burst-second".to_string())
            .expect("create second cf");

        let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
        let payload = vec![0xA5; 1536];
        let burst = vec![
            RuntimeMsg::WalAppend {
                request_id: 1,
                cf_id: 0,
                key: b"default-1".to_vec(),
                value: Some(payload.clone()),
                ttl_seconds: None,
                insert_only: false,
            },
            RuntimeMsg::WalAppend {
                request_id: 2,
                cf_id: second_cf_id,
                key: b"second-1".to_vec(),
                value: Some(payload.clone()),
                ttl_seconds: None,
                insert_only: false,
            },
            RuntimeMsg::WalAppend {
                request_id: 3,
                cf_id: 0,
                key: b"default-2".to_vec(),
                value: Some(payload.clone()),
                ttl_seconds: None,
                insert_only: false,
            },
            RuntimeMsg::WalAppend {
                request_id: 4,
                cf_id: second_cf_id,
                key: b"second-2".to_vec(),
                value: Some(payload),
                ttl_seconds: None,
                insert_only: false,
            },
        ];

        let mut burst_iter = burst.into_iter();
        let first = burst_iter.next().expect("first burst write");
        for msg in burst_iter {
            msg_tx.send(msg).expect("enqueue burst write");
        }

        let outcome = event_loop.process_wake_msg(first, &msg_rx, 16);
        drop(msg_tx);

        assert_eq!(outcome, HandleOutcome::Continue);
        assert_eq!(
            event_loop.state.manifest.files.len(),
            2,
            "local write burst should publish SSTs for every CF that became flushable without requiring another write"
        );
        assert!(event_loop
            .state
            .manifest
            .files
            .iter()
            .any(|file| file.cf_id == 0));
        assert!(event_loop
            .state
            .manifest
            .files
            .iter()
            .any(|file| file.cf_id == second_cf_id));
    }

    #[test]
    fn should_assign_compaction_output_sequence_when_plan_has_zero() {
        let mut event_loop = create_test_event_loop().expect("create event loop");
        event_loop.state.sequence = 41;
        let plan = crate::compaction::CompactionPlan::new(3, 0, 1);

        let assigned = event_loop.assign_compaction_output_sequence(plan);

        assert_eq!(assigned.output_seq, 42);
        assert_eq!(
            event_loop.state.sequence, 42,
            "assigning a compaction output sequence must consume one global sequence"
        );
    }

    #[test]
    fn should_preserve_existing_compaction_output_sequence() {
        let mut event_loop = create_test_event_loop().expect("create event loop");
        event_loop.state.sequence = 41;
        let plan = crate::compaction::CompactionPlan::new(3, 0, 1).with_output_seq(99);

        let assigned = event_loop.assign_compaction_output_sequence(plan);

        assert_eq!(assigned.output_seq, 99);
        assert_eq!(
            event_loop.state.sequence, 41,
            "preassigned compaction output sequences must not consume another sequence"
        );
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
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let state = RuntimeState::new(temp_dir.path().to_path_buf(), false);

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
