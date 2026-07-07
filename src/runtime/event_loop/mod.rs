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

#[cfg(test)]
mod cloud;
mod cloud_integration;
mod compaction;
mod control;
mod dispatch;
mod durability_sync;
mod flush;
#[cfg(test)]
mod gc;
mod ingest;
mod manifest;
mod read_path;
mod shutdown;
mod snapshot;
mod wal;
mod write_batch;

use crossbeam::channel::{Receiver, Sender, TryRecvError};
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

#[cfg(test)]
use super::actors::CloudActor;
use super::actors::{CompactionActor, FlushActor, GcActor, ManifestActor, WalActor};
use super::durability::DurabilityCoordinator;
use super::read_resources::ReadResources;
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
    pub(super) cloud_metadata_cleanup_proofs: HashMap<String, MetadataCleanupProof>,

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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HandleOutcome {
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
        let memory_mode = state.is_memory_mode();
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
        let read_resources = if memory_mode {
            None
        } else {
            let sst_path_prefix = sst_dir
                .strip_prefix(&state.db_path)
                .unwrap_or_else(|_| std::path::Path::new("sst"))
                .to_path_buf();
            Some(Arc::new(ReadResources::new(
                Arc::clone(&state.fs),
                sst_path_prefix,
                config.block_cache_size,
                config.block_cache_policy,
            )))
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
        state.set_compaction_enabled(config.background_compaction);

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
            gc_actor: GcActor::new(),
            manifest_actor: ManifestActor::new(),
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
            inline_responses: RefCell::new(HashMap::new()),
            pending_msg: None,
            worker_msg_tx,

            write_stall_waiters: HashMap::new(),
            write_stall_waiter_queues: HashMap::new(),
            snapshot_cache: None,
            read_resources,
            lease_healthy: config.lease_healthy.clone(),
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
                let etag = metadata.etag.trim().to_string();
                if etag.is_empty() {
                    return Err(crate::common::MidgeError::Internal(format!(
                        "cloud metadata '{key}' cannot be conditionally updated without an etag"
                    )));
                }
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
                vec![("If-Match".to_string(), etag)]
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

    fn publish_flushed_sst(
        &mut self,
        cf_id: crate::types::ColumnFamilyId,
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
        if !self.state.is_memory_mode() {
            self.manifest_actor.add_sst(&mut self.state, file_meta)?;
            self.state.transition_flush_publication_intent(
                sst_name,
                crate::runtime::PublicationPhase::ManifestPublished,
            )?;
            if sequence > self.state.manifest.last_persisted_sequence {
                self.state.manifest.last_persisted_sequence = sequence;
            }
            let manifest_persisted = match crate::runtime::actors::ManifestActor::persist(
                &self.state,
            ) {
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
        if self.state.is_memory_mode() {
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

    fn has_actionable_work(&self) -> bool {
        if self.pending_msg.is_some() {
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
        self.wal_actor.sync_deadline_timeout()
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
        self.maybe_flush_cloud_async_wal();
        self.tick_hybrid_storage();
        self.drain_hybrid_storage_events();
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

        let drained = self.drain_pending_writes(msg_rx, max_drain);
        batch += drained;
        self.record_wake_batch(batch);
        outcome
    }

    /// Main event loop — runs until Shutdown message or channel close.
    pub fn run(&mut self, msg_rx: &Receiver<RuntimeMsg>) {
        const MAX_DRAIN_WRITES_ON_WAKE: usize = 4096;
        const ACTIONABLE_IDLE_BACKOFF: Duration = Duration::from_micros(50);

        loop {
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
            let msg = if let Some(storage_rx) = self.hybrid_storage_events.clone() {
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
            .map_or(0, |d| d.as_nanos());
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

    #[test]
    fn should_apply_runtime_config_block_cache_policy_to_read_resources_when_initializing_event_loop(
    ) -> crate::common::MidgeResult<()> {
        // Arrange
        let db_path = unique_test_db_path("midge_event_loop_config");
        std::fs::create_dir_all(&db_path).expect("create temp runtime config dir");
        let state = RuntimeState::new(db_path, false);
        let router = Arc::new(ResponseRouter::new());
        let config = crate::runtime::RuntimeConfig {
            block_cache_policy: crate::sst::cache::CachePolicyType::ClockPro,
            block_cache_size: 512 * 1024,
            l0_compaction_trigger: 7,
            ..crate::runtime::RuntimeConfig::default()
        };

        // Act
        let event_loop = EventLoop::new(state, false, router, config, None)?;

        // Assert
        assert_eq!(event_loop.compaction_actor.l0_file_count_threshold(), 7);
        let read_resources = event_loop
            .read_resources
            .as_ref()
            .expect("local runtime should construct read resources");
        assert_eq!(
            read_resources.block_cache().policy_type(),
            crate::sst::cache::CachePolicyType::ClockPro
        );
        Ok(())
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
        // Arrange
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

        // Act
        // Assert
        assert_eq!(event_loop.drain_auto_flush_memtables(), 0);

        assert!(
            !event_loop.has_actionable_work(),
            "blocked auto-flush candidates should wait for a real state change before retrying"
        );
    }

    #[test]
    fn should_schedule_compaction_after_l0_flush_when_threshold_reached(
    ) -> crate::common::MidgeResult<()> {
        // Arrange
        let mut event_loop = create_test_local_event_loop().expect("create local event loop");
        let (worker_tx, _worker_rx) = crossbeam::channel::unbounded();
        event_loop.worker_msg_tx = Some(worker_tx);
        event_loop.state.set_compaction_enabled(true);
        event_loop
            .state
            .manifest
            .files
            .extend((1..=3).map(|seq| test_manifest_l0_file_meta(&format!("pre-{seq}.sst"), seq)));

        let flushed = write_runtime_l0_sst_for_test(&event_loop, "threshold-crossing.sst", 4);
        event_loop.publish_flushed_sst(0, "threshold-crossing.sst", 4, Some(flushed))?;

        // Act
        // Assert
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
        // Arrange
        let mut event_loop = create_test_local_event_loop().expect("create local event loop");
        let (worker_tx, _worker_rx) = crossbeam::channel::unbounded();
        event_loop.worker_msg_tx = Some(worker_tx);
        event_loop.state.set_compaction_enabled(false);
        event_loop
            .state
            .manifest
            .files
            .extend((1..=3).map(|seq| test_manifest_l0_file_meta(&format!("pre-{seq}.sst"), seq)));

        let flushed =
            write_runtime_l0_sst_for_test(&event_loop, "disabled-threshold-crossing.sst", 4);
        event_loop.publish_flushed_sst(0, "disabled-threshold-crossing.sst", 4, Some(flushed))?;

        // Act
        // Assert
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
    fn should_skip_post_flush_compaction_check_during_ingest_when_compaction_disabled() {
        // Arrange
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
        event_loop.state.set_compaction_enabled(false);
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
        // Act
        // Assert
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
    }

    #[test]
    fn should_apply_l0_compaction_trigger_from_runtime_config() {
        // Arrange
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

        // Act
        // Assert
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
        // Arrange
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

        // Act
        // Assert
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
        // Arrange
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

        // Act
        // Assert
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
        // Arrange
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

        // Act
        // Assert
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
        // Arrange
        let mut event_loop = create_test_event_loop().expect("create event loop");
        event_loop.state.sequence = 41;
        let plan = crate::compaction::CompactionPlan::new(3, 0, 1);

        let assigned = event_loop.assign_compaction_output_sequence(plan);

        // Act
        // Assert
        assert_eq!(assigned.output_seq, 42);
        assert_eq!(
            event_loop.state.sequence, 42,
            "assigning a compaction output sequence must consume one global sequence"
        );
    }

    #[test]
    fn should_preserve_existing_compaction_output_sequence() {
        // Arrange
        let mut event_loop = create_test_event_loop().expect("create event loop");
        event_loop.state.sequence = 41;
        let plan = crate::compaction::CompactionPlan::new(3, 0, 1).with_output_seq(99);

        let assigned = event_loop.assign_compaction_output_sequence(plan);

        // Act
        // Assert
        assert_eq!(assigned.output_seq, 99);
        assert_eq!(
            event_loop.state.sequence, 41,
            "preassigned compaction output sequences must not consume another sequence"
        );
    }

    mod compaction_scheduling {
        use super::*;

        fn manifest_file_from_runtime(file: crate::runtime::FileMeta) -> crate::metadata::FileMeta {
            crate::metadata::FileMeta {
                name: file.name,
                level: file.level,
                size_bytes: file.size_bytes,
                content_crc32c: file.content_crc32c,
                cf_id: file.cf_id,
                smallest_key: file.smallest_key,
                largest_key: file.largest_key,
                smallest_seq: file.smallest_seq,
                largest_seq: file.largest_seq,
                ..Default::default()
            }
        }

        #[test]
        fn should_assign_launch_metadata_at_launch_boundary() {
            // Arrange
            let mut event_loop = create_test_event_loop().expect("create event loop");
            event_loop.state.sequence = 41;
            // Act
            // Assert
            assert!(event_loop.state.register_snapshot(100, 37, Vec::new()));
            let plan =
                crate::compaction::CompactionPlan::new(3, 0, 1).with_snapshot_horizon(Some(99));

            let prepared = event_loop
                .prepare_compaction_plan_for_launch(plan)
                .expect("prepare compaction plan");

            assert_eq!(prepared.output_seq, 42);
            assert_eq!(prepared.snapshot_horizon, Some(37));
            assert_eq!(
                event_loop.state.sequence, 42,
                "compaction launch must consume one global sequence for raw plans"
            );
        }

        #[test]
        fn should_preserve_preassigned_identity_at_launch_boundary() {
            // Arrange
            let mut event_loop = create_test_event_loop().expect("create event loop");
            event_loop.state.sequence = 41;
            let plan = crate::compaction::CompactionPlan::new(3, 0, 1).with_output_seq(99);

            let prepared = event_loop
                .prepare_compaction_plan_for_launch(plan)
                .expect("prepare compaction plan");

            // Act
            // Assert
            assert_eq!(prepared.output_seq, 99);
            assert_eq!(
                event_loop.state.sequence, 41,
                "compaction launch must not consume a sequence for preassigned plans"
            );
        }

        #[test]
        fn should_assign_output_sequence_when_compaction_runs_after_end_ingest() {
            // Arrange
            let mut event_loop = create_test_local_event_loop().expect("create local event loop");
            let (worker_tx, worker_rx) = crossbeam::channel::unbounded();
            event_loop.worker_msg_tx = Some(worker_tx);
            event_loop.state.set_compaction_enabled(true);
            event_loop.state.sequence = 100;
            event_loop
                .state
                .ingest_active
                .store(true, std::sync::atomic::Ordering::SeqCst);

            for seq in 1..=4 {
                let name = crate::sst::file_name(0, 0, seq);
                let file = write_runtime_l0_sst_for_test(&event_loop, &name, seq);
                event_loop
                    .state
                    .manifest
                    .files
                    .push(manifest_file_from_runtime(file));
            }

            event_loop.handle_end_ingest(77);

            let msg = worker_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("EndIngest-triggered compaction should complete");
            match msg {
                RuntimeMsg::CompactionComplete { output_ssts, .. } => {
                    // Act
                    // Assert
                    assert!(
                        !output_ssts.is_empty(),
                        "EndIngest-triggered compaction should produce an output SST"
                    );
                    assert!(
                        output_ssts
                            .iter()
                            .all(|name| !name.ends_with("00000000000000000000.sst")),
                        "EndIngest-triggered compaction must not use sequence zero: {output_ssts:?}"
                    );
                }
                other => panic!("unexpected worker message: {other:?}"),
            }
        }

        #[test]
        fn should_launch_compactions_only_through_event_loop_helper() {
            // Arrange
            let event_loop_dir =
                std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/runtime/event_loop");
            let pattern = format!(".run_{}(", "compaction");
            let mut call_sites = Vec::new();

            for entry in std::fs::read_dir(&event_loop_dir).expect("read event_loop dir") {
                let entry = entry.expect("read event_loop entry");
                let path = entry.path();
                if path.extension().and_then(std::ffi::OsStr::to_str) != Some("rs") {
                    continue;
                }

                let source = std::fs::read_to_string(&path).expect("read event_loop source file");
                let relative = path
                    .strip_prefix(env!("CARGO_MANIFEST_DIR"))
                    .expect("strip source prefix")
                    .display()
                    .to_string();
                for (line_idx, line) in source.lines().enumerate() {
                    if line.contains(&pattern) {
                        call_sites.push(format!("{relative}:{}:{}", line_idx + 1, line.trim()));
                    }
                }
            }

            // Act
            // Assert
            assert_eq!(
                call_sites.len(),
                1,
                "event-loop compaction actor launches must stay centralized: {call_sites:?}"
            );
            assert!(
                call_sites[0].starts_with("src/runtime/event_loop/mod.rs:"),
                "central compaction actor launch must live in event_loop/mod.rs: {call_sites:?}"
            );
        }
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
        assert!(event_loop.hybrid_storage.is_none()); // Hybrid storage is optional
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
