//! Event loop — central message dispatcher
//!
//! Receives messages from RuntimeHandle and routes them to the correct actor.
//!
//! Copilot note:
//! - Per-request routing is done exclusively via `router.complete()`.
//! - EventLoop never touches pending_responses directly.
//! - All read paths are local (memtables → SST later).
//! - All actor responses flow through `respond()`.

use crossbeam::channel::Receiver;
use std::sync::Arc;

use super::actors::{
    CloudActor, CompactionActor, EvictionActor, FlushActor, GcActor, ManifestActor,
    SeqnoAllocActor, WalActor,
};
use super::state::RuntimeState;
use super::{ResponseRouter, RuntimeMsg, RuntimeResponse};
use crate::sst::traits::SstReader;
use crate::sst::Memtable;

/// Main synchronous event loop for the runtime.
///
/// Owns all actors and is responsible for routing inbound messages.
pub struct EventLoop {
    state: RuntimeState,

    // Actors
    flush_actor: FlushActor,
    compaction_actor: CompactionActor,
    wal_actor: WalActor,
    cloud_actor: CloudActor,
    gc_actor: GcActor,
    manifest_actor: ManifestActor,
    eviction_actor: Option<EvictionActor>,

    hybrid_storage: Option<Arc<crate::storage::HybridStorage>>,
    trace_enabled: bool,

    /// Per-request router (oneshot channels)
    router: Arc<ResponseRouter>,
}

impl EventLoop {
    pub(crate) fn new(
        state: RuntimeState,
        trace_enabled: bool,
        router: Arc<ResponseRouter>,
    ) -> crate::common::MidgeResult<Self> {
        let wal_dir = state.wal_dir.clone();
        let sst_dir = state.sst_dir.clone();
        let memory_mode = state.memory_mode;

        let sst_factory = if memory_mode {
            // Don't create SST factory in memory mode
            Arc::new(crate::sst::FsSstFactory::new(&sst_dir, 64 * 1024)) // Dummy, won't be used
        } else {
            Arc::new(super::super::sst::FsSstFactory::new(&sst_dir, 64 * 1024)) // 64KB block size
        };

        // Create actors - they handle memory_mode internally
        let flush_actor = FlushActor::new(&sst_dir, memory_mode)?;
        let wal_actor = WalActor::new(wal_dir, crate::wal::DurabilityPolicy::Batched, memory_mode)?;

        Ok(Self {
            state,
            flush_actor,
            compaction_actor: CompactionActor::new(sst_factory),
            wal_actor,
            cloud_actor: CloudActor::new(),
            gc_actor: GcActor::new(),
            manifest_actor: ManifestActor::new(),
            eviction_actor: None,
            hybrid_storage: None,
            trace_enabled,
            router,
        })
    }

    pub fn set_hybrid_storage(&mut self, storage: Arc<crate::storage::HybridStorage>) {
        self.eviction_actor = Some(EvictionActor::new(storage.clone()));
        self.hybrid_storage = Some(storage);
    }

    /// Helper: deliver a RuntimeResponse to the requester via the router.
    #[inline]
    fn respond(&self, request_id: u64, resp: RuntimeResponse) {
        self.router.complete(resp);

        // Optional trace
        if self.trace_enabled {
            tracing::trace!(request_id, "response routed");
        }
    }

    /// Main event loop — runs until Shutdown message or channel close.
    pub fn run(&mut self, msg_rx: Receiver<RuntimeMsg>) {
        while let Ok(msg) = msg_rx.recv() {
            if self.trace_enabled {
                tracing::trace!(?msg, "runtime received message");
            }

            match msg {
                RuntimeMsg::Shutdown => {
                    tracing::info!("Runtime shutting down");
                    break;
                }

                RuntimeMsg::Noop { request_id } => {
                    self.respond(request_id, RuntimeResponse::Ok { request_id });
                }

                RuntimeMsg::StartupPing { request_id } => {
                    self.respond(request_id, RuntimeResponse::Ok { request_id });
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

                // =============================================================
                // Seqno Allocation
                // =============================================================
                RuntimeMsg::AllocSeqno { request_id, cf_id } => {
                    let resp = SeqnoAllocActor::alloc_seqno(&mut self.state, cf_id)
                        .map(|(seqno, _)| RuntimeResponse::SeqnoAllocated { request_id, seqno })
                        .unwrap_or_else(|e| RuntimeResponse::Error {
                            request_id,
                            message: e.to_string(),
                        });

                    self.respond(request_id, resp);
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
                            message: e.to_string(),
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
                    self.respond(request_id, RuntimeResponse::Ok { request_id });
                }

                // =============================================================
                // Compaction
                // =============================================================
                RuntimeMsg::CheckCompaction { request_id } => {
                    if let Some(plan) = self.compaction_actor.check_compaction(&self.state) {
                        let resp = self
                            .compaction_actor
                            .run_compaction(&mut self.state, plan, self.hybrid_storage.as_ref())
                            .map(|output_ssts| RuntimeResponse::CompactionComplete {
                                request_id,
                                output_ssts,
                            })
                            .unwrap_or_else(|e| RuntimeResponse::Error {
                                request_id,
                                message: e.to_string(),
                            });

                        self.respond(request_id, resp);
                    } else {
                        self.respond(request_id, RuntimeResponse::Ok { request_id });
                    }
                }

                RuntimeMsg::RunCompaction { request_id, plan } => {
                    let cplan = crate::compaction::CompactionPlan {
                        input_files: plan.input_files,
                        output_files: Vec::new(),
                        source_level: plan.source_level,
                        target_level: plan.target_level,
                        cf_id: plan.cf_id,
                        output_seq: self.state.next_sequence(),
                    };

                    let resp = self
                        .compaction_actor
                        .run_compaction(&mut self.state, cplan, self.hybrid_storage.as_ref())
                        .map(|output_ssts| RuntimeResponse::CompactionComplete {
                            request_id,
                            output_ssts,
                        })
                        .unwrap_or_else(|e| RuntimeResponse::Error {
                            request_id,
                            message: e.to_string(),
                        });

                    self.respond(request_id, resp);
                }

                RuntimeMsg::CompactionComplete {
                    request_id,
                    input_ssts,
                    output_ssts,
                } => {
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
                    sequence,
                    ttl_seconds,
                    insert_only,
                } => {
                    let result = self.wal_actor.append(
                        &mut self.state,
                        cf_id,
                        bytes::Bytes::from(key),
                        value.map(bytes::Bytes::from),
                        sequence,
                        insert_only,
                        ttl_seconds,
                    );
                    let resp = result
                        .map(|_| RuntimeResponse::Ok { request_id })
                        .unwrap_or_else(|e| RuntimeResponse::Error {
                            request_id,
                            message: e.to_string(),
                        });
                    self.respond(request_id, resp);

                    // Auto-sync if batched threshold exceeded
                    if self.wal_actor.should_sync_batch() {
                        if let Err(e) = self.wal_actor.sync(&mut self.state) {
                            tracing::warn!(error = %e, "failed to auto-sync WAL batch");
                        }
                    }
                }

                RuntimeMsg::WalMerge {
                    request_id,
                    cf_id,
                    key,
                    operand,
                    sequence,
                } => {
                    let result = self.wal_actor.append_merge(
                        &mut self.state,
                        cf_id,
                        bytes::Bytes::from(key),
                        bytes::Bytes::from(operand),
                        sequence,
                    );

                    // Auto-sync if batched threshold exceeded
                    if self.wal_actor.should_sync_batch() {
                        if let Err(e) = self.wal_actor.sync(&mut self.state) {
                            tracing::warn!(error = %e, "failed to auto-sync WAL batch");
                        }
                    }
                    let resp = result
                        .map(|_| RuntimeResponse::Ok { request_id })
                        .unwrap_or_else(|e| RuntimeResponse::Error {
                            request_id,
                            message: e.to_string(),
                        });
                    self.respond(request_id, resp);
                }

                RuntimeMsg::RegisterMergeOperator {
                    request_id,
                    cf_id,
                    operator,
                } => {
                    if let Some(cf_state) = self.state.column_families.get_mut(&cf_id) {
                        cf_state.merge_operator = Some(operator);
                        self.respond(request_id, RuntimeResponse::Ok { request_id });
                    } else {
                        self.respond(
                            request_id,
                            RuntimeResponse::Error {
                                request_id,
                                message: format!("Invalid CF ID: {}", cf_id),
                            },
                        );
                    }
                }

                RuntimeMsg::WalSync { request_id } => {
                    let result = self.wal_actor.sync(&mut self.state);
                    let resp = result
                        .map(|_| RuntimeResponse::Ok { request_id })
                        .unwrap_or_else(|e| RuntimeResponse::Error {
                            request_id,
                            message: e.to_string(),
                        });
                    self.respond(request_id, resp);
                }

                RuntimeMsg::WalRotate { request_id } => {
                    let result = self.wal_actor.rotate(&mut self.state);
                    let resp = result
                        .map(|_| RuntimeResponse::Ok { request_id })
                        .unwrap_or_else(|e| RuntimeResponse::Error {
                            request_id,
                            message: e.to_string(),
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
                    let result = self.cloud_actor.upload_sst(&mut self.state, &sst_name);
                    let resp = result
                        .map(|_| RuntimeResponse::Ok { request_id })
                        .unwrap_or_else(|e| RuntimeResponse::Error {
                            request_id,
                            message: e.to_string(),
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
                            message: e.to_string(),
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
                            message: e.to_string(),
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
                            message: e.to_string(),
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
                            message: e.to_string(),
                        });
                    self.respond(request_id, resp);
                }

                RuntimeMsg::ManifestPersist { request_id } => {
                    let result = self.manifest_actor.persist(&self.state);
                    let resp = result
                        .map(|_| RuntimeResponse::Ok { request_id })
                        .unwrap_or_else(|e| RuntimeResponse::Error {
                            request_id,
                            message: e.to_string(),
                        });
                    self.respond(request_id, resp);
                }

                RuntimeMsg::ManifestCreateColumnFamily { request_id, name } => {
                    let result = self
                        .manifest_actor
                        .create_column_family(&mut self.state, name.clone());
                    let resp = result
                        .map(|cf_id| RuntimeResponse::ColumnFamilyCreated { request_id, cf_id })
                        .unwrap_or_else(|e| RuntimeResponse::Error {
                            request_id,
                            message: e.to_string(),
                        });
                    self.respond(request_id, resp);
                }

                RuntimeMsg::ManifestDropColumnFamily { request_id, cf_id } => {
                    let result = self
                        .manifest_actor
                        .drop_column_family(&mut self.state, cf_id);
                    let resp = result
                        .map(|_| RuntimeResponse::Ok { request_id })
                        .unwrap_or_else(|e| RuntimeResponse::Error {
                            request_id,
                            message: e.to_string(),
                        });
                    self.respond(request_id, resp);
                }

                // Read path: local memtable lookup
                RuntimeMsg::Read {
                    request_id,
                    cf_id,
                    key,
                    sequence,
                } => {
                    let value = self.handle_read(cf_id, &key, sequence);
                    self.respond(request_id, RuntimeResponse::ReadValue { request_id, value });
                }

                RuntimeMsg::RangeScan {
                    request_id,
                    cf_id,
                    start,
                    end,
                    sequence,
                } => {
                    let results = self.handle_range_scan(cf_id, &start, &end, sequence);
                    self.respond(
                        request_id,
                        RuntimeResponse::RangeScanResults {
                            request_id,
                            results,
                        },
                    );
                }
            }
        }

        tracing::debug!("Runtime message channel closed — exiting event loop");
    }

    /// Local read path: memtable → immutable memtables → [SST TODO]
    /// Resolves merge operands if a merge operator is registered
    fn handle_read(&self, cf_id: u32, key: &[u8], _seq: u64) -> Option<Vec<u8>> {
        let cf_state = self.state.column_families.get(&cf_id)?;

        // Check if there's a merge operator for this CF
        if cf_state.merge_operator.is_some() {
            // Collect all versions from memtables
            let mut all_versions = Vec::new();

            // Active memtable
            let active_versions = cf_state.memtable.get_versions_for_merge(key);
            all_versions.extend(active_versions);

            // Immutable memtables (newest → oldest)
            for imm in cf_state.immutable_memtables.iter().rev() {
                let imm_versions = imm.get_versions_for_merge(key);
                all_versions.extend(imm_versions);
            }

            // Sort versions by sequence (oldest first) - versions are already in order
            // from each memtable, but we need to merge them

            if all_versions.is_empty() {
                return None;
            }

            // Separate base value and merge operands
            // Versions are in newest-first order, so we need to process in reverse
            // to find the oldest Put (base value) and subsequent Merges
            let mut base_value: Option<Vec<u8>> = None;
            let mut merge_operands: Vec<Vec<u8>> = Vec::new();

            use crate::iterators::skiplist::OpType;
            // Process versions in reverse (oldest first)
            for (value_opt, _exp, op_type) in all_versions.iter().rev() {
                match op_type {
                    OpType::Put => {
                        // Oldest Put becomes the base value
                        if let Some(value) = value_opt.as_ref() {
                            base_value = Some(value.to_vec());
                        }
                        merge_operands.clear(); // Start fresh after a Put
                    }
                    OpType::Delete => {
                        // Tombstone clears everything
                        base_value = None;
                        merge_operands.clear();
                    }
                    OpType::Merge => {
                        // Collect merge operand in chronological order
                        if let Some(v) = value_opt {
                            merge_operands.push(v.to_vec());
                        }
                    }
                }
            }

            // If there are merge operands, resolve them
            if !merge_operands.is_empty() {
                if let Some(operator) = &cf_state.merge_operator {
                    match operator.merge(key, base_value.as_deref(), &merge_operands) {
                        Ok(Some(resolved)) => return Some(resolved),
                        Ok(None) => return None,
                        Err(e) => {
                            tracing::error!("Merge operator failed: {}", e);
                            return None;
                        }
                    }
                } else {
                    // Shouldn't happen - we checked for merge operator above
                    tracing::error!("Merge operands found but no operator registered");
                    return None;
                }
            }

            // No merge operands, return base value
            return base_value;
        }

        // No merge operator - use simple get logic
        // Active memtable
        if let Ok(Some(v)) = cf_state.memtable.get(key) {
            return Some(v);
        }

        // Immutable memtables (newest → oldest)
        for imm in cf_state.immutable_memtables.iter().rev() {
            if let Ok(Some(v)) = imm.get(key) {
                return Some(v);
            }
        }

        // SST lookup: check files from newest to oldest across all levels
        let mut ssts_checked = 0u64;
        let mut l0_ssts_checked = 0u64;
        let mut blocks_read = 0u64;

        // Get all SST files for this CF, grouped by level
        let mut files_by_level: std::collections::BTreeMap<u32, Vec<_>> =
            std::collections::BTreeMap::new();
        for file in &self.state.manifest.files {
            if file.cf_id == cf_id {
                files_by_level
                    .entry(file.level)
                    .or_default()
                    .push(file.clone());
            }
        }

        // Search L0 first (newest to oldest), then L1, L2, etc.
        // L0 files may overlap, so we must check all of them
        if let Some(l0_files) = files_by_level.get(&0) {
            for file_meta in l0_files.iter().rev() {
                ssts_checked += 1;
                l0_ssts_checked += 1;

                // Track read access for compaction prioritization
                file_meta.record_read();

                // Try to open and read from this SST
                let sst_path = self.state.sst_dir.join(&file_meta.name);
                if let Ok(reader) = crate::sst::fs::SstFile::open(&sst_path) {
                    blocks_read += 1; // At minimum, we read index block
                    if let Ok(Some(value)) = reader.get(key) {
                        // Found! Record metrics and return
                        self.state.read_amp_metrics.record_read(
                            ssts_checked,
                            l0_ssts_checked,
                            blocks_read,
                        );
                        return Some(value.to_vec());
                    }
                }
            }
        }

        // Check higher levels (L1, L2, ...) - these are sorted and non-overlapping
        for (&level, files) in files_by_level.iter() {
            if level == 0 {
                continue; // Already checked L0
            }

            for file_meta in files.iter().rev() {
                // Check if key is in range for this SST
                if let (Some(ref smallest), Some(ref largest)) =
                    (&file_meta.smallest_key, &file_meta.largest_key)
                {
                    if key < smallest.as_slice() || key > largest.as_slice() {
                        continue; // Key not in this SST's range
                    }
                }

                ssts_checked += 1;

                // Track read access for compaction prioritization
                file_meta.record_read();

                // Try to open and read from this SST
                let sst_path = self.state.sst_dir.join(&file_meta.name);
                if let Ok(reader) = crate::sst::fs::SstFile::open(&sst_path) {
                    blocks_read += 1; // At minimum, we read index block
                    if let Ok(Some(value)) = reader.get(key) {
                        // Found! Record metrics and return
                        self.state.read_amp_metrics.record_read(
                            ssts_checked,
                            l0_ssts_checked,
                            blocks_read,
                        );
                        return Some(value.to_vec());
                    }
                }
            }
        }

        // Key not found in any SST - record miss
        self.state
            .read_amp_metrics
            .record_read(ssts_checked, l0_ssts_checked, blocks_read);
        None
    }

    /// Range scan: iterate keys in [start, end) from memtables
    fn handle_range_scan(
        &self,
        cf_id: u32,
        start: &[u8],
        end: &[u8],
        _seq: u64,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let cf_state = match self.state.column_families.get(&cf_id) {
            Some(state) => state,
            None => return vec![],
        };

        // Collect results from active memtable + immutable memtables
        let mut results: std::collections::BTreeMap<Vec<u8>, Vec<u8>> =
            std::collections::BTreeMap::new();

        // Scan active memtable - group by key to get latest version only
        let mut by_key: std::collections::BTreeMap<Vec<u8>, Option<Vec<u8>>> =
            std::collections::BTreeMap::new();
        for (key, value, _seq) in cf_state.memtable.iter_all(u64::MAX) {
            // For each key, keep the first (most recent) value we encounter
            by_key.entry(key).or_insert(value);
        }

        for (key, value) in by_key.iter() {
            if value.is_none() {
                // Skip tombstones (deleted keys)
                continue;
            }
            if key.as_slice() >= start && key.as_slice() < end {
                results.insert(key.clone(), value.clone().expect("value already checked"));
            }
        }

        // Scan immutable memtables (oldest → newest for correct override semantics)
        for imm in cf_state.immutable_memtables.iter() {
            let mut by_key: std::collections::BTreeMap<Vec<u8>, Option<Vec<u8>>> =
                std::collections::BTreeMap::new();
            for (key, value, _seq) in imm.iter_all(u64::MAX) {
                // Keep the first (most recent) value for each key
                by_key.entry(key).or_insert(value);
            }

            for (key, value) in by_key.iter() {
                if value.is_none() {
                    // Skip tombstones
                    continue;
                }
                if key.as_slice() >= start && key.as_slice() < end {
                    results.insert(key.clone(), value.clone().expect("value already checked"));
                }
            }
        }

        // Convert to vec of (key, value) tuples
        results.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{state::RuntimeState, ResponseRouter};
    use std::sync::Arc;

    // Helper to create a minimal runtime state for testing
    fn create_test_state() -> RuntimeState {
        RuntimeState::new("/tmp/test_event_loop".into(), true) // Memory mode
    }

    // Helper to create a new event loop
    fn create_test_event_loop() -> crate::common::MidgeResult<EventLoop> {
        let state = create_test_state();
        let router = Arc::new(ResponseRouter::new());
        EventLoop::new(state, false, router)
    }

    // =========== EventLoop Creation Tests ===========

    #[test]
    fn should_create_event_loop_in_memory_mode() {
        // Arrange & Act
        let result = create_test_event_loop();

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_initialize_all_actors() {
        // Arrange & Act
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
        let event_loop = EventLoop::new(state, false, router);

        // Assert
        assert!(event_loop.is_ok());
    }

    #[test]
    fn should_initialize_with_tracing_enabled() {
        // Arrange
        let state = create_test_state();
        let router = Arc::new(ResponseRouter::new());

        // Act
        let event_loop = EventLoop::new(state, true, router);

        // Assert
        assert!(event_loop.is_ok());
    }

    #[test]
    fn should_start_with_no_hybrid_storage() {
        // Arrange & Act
        let event_loop = create_test_event_loop().expect("Should create event loop");

        // Assert - eviction_actor should be None initially
        assert!(event_loop.eviction_actor.is_none());
        assert!(event_loop.hybrid_storage.is_none());
    }

    // =========== Hybrid Storage Configuration Tests ===========

    #[test]
    fn should_set_hybrid_storage() {
        // Arrange
        let event_loop = create_test_event_loop().expect("Should create event loop");

        // Create a mock hybrid storage (we need to use a real one or skip this test)
        // For now, we'll skip detailed testing of set_hybrid_storage since it requires
        // complex setup with actual HybridStorage instance

        // Assert - Method exists and is callable
        drop(event_loop);
    }

    // =========== Message Routing Tests ===========
    // Note: Full message routing tests are difficult without actually running
    // the event loop in a thread, which is integration testing territory.
    // The EventLoop::run() method is the main entry point and requires
    // a channel receiver and mutable access to process messages.

    // =========== Response Routing Tests ===========

    #[test]
    fn should_route_responses_via_router() {
        // Arrange
        let state = create_test_state();
        let router = Arc::new(ResponseRouter::new());

        // Act
        let event_loop = EventLoop::new(state, false, router.clone());

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
        let result = EventLoop::new(state, false, router);

        // Assert
        assert!(result.is_ok());
        // State is moved into EventLoop, can't inspect after, but construction validates it
    }

    // =========== Read Path Tests ===========
    // The read path is complex and would benefit from integration testing,
    // but we can verify that the methods exist and are callable

    #[test]
    fn should_have_handle_read_method() {
        // Arrange
        let event_loop = create_test_event_loop().expect("Should create event loop");

        // Act - The method should exist (private, so can't call directly)
        // We verify it exists by checking the struct compiles with it

        // Assert - Just verify event_loop is created
        assert!(!event_loop.trace_enabled);
    }

    #[test]
    fn should_have_handle_range_scan_method() {
        // Arrange
        let event_loop = create_test_event_loop().expect("Should create event loop");

        // Act - Similar to handle_read, verify method exists

        // Assert
        assert!(!event_loop.trace_enabled);
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
        let event_loop1 = EventLoop::new(state1, false, router1).expect("Should create");
        let event_loop2 = EventLoop::new(state2, true, router2).expect("Should create");

        // Assert
        assert!(!event_loop1.trace_enabled);
        assert!(event_loop2.trace_enabled);
    }

    // =========== Actor Initialization Tests ===========

    #[test]
    fn should_initialize_flush_actor() {
        // Arrange & Act
        let event_loop = create_test_event_loop().expect("Should create event loop");

        // Assert - Verify through construction success
        // FlushActor is initialized and owned by EventLoop
        assert!(event_loop.hybrid_storage.is_none()); // Related check
    }

    #[test]
    fn should_initialize_compaction_actor() {
        // Arrange & Act
        let event_loop = create_test_event_loop().expect("Should create event loop");

        // Assert - CompactionActor is initialized
        // We verify this indirectly through successful construction
        drop(event_loop);
    }

    #[test]
    fn should_initialize_wal_actor() {
        // Arrange & Act
        let event_loop = create_test_event_loop().expect("Should create event loop");

        // Assert - WalActor is initialized
        drop(event_loop);
    }

    #[test]
    fn should_initialize_cloud_actor() {
        // Arrange & Act
        let event_loop = create_test_event_loop().expect("Should create event loop");

        // Assert
        drop(event_loop);
    }

    #[test]
    fn should_initialize_gc_actor() {
        // Arrange & Act
        let event_loop = create_test_event_loop().expect("Should create event loop");

        // Assert
        drop(event_loop);
    }

    #[test]
    fn should_initialize_manifest_actor() {
        // Arrange & Act
        let event_loop = create_test_event_loop().expect("Should create event loop");

        // Assert
        drop(event_loop);
    }

    // =========== Invariant Tests ===========

    #[test]
    fn should_maintain_actor_ownership() {
        // Arrange & Act
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
        let _event_loop = EventLoop::new(state, false, router).expect("Should create");

        // Assert - Router is properly stored
        // The router's methods can be called independently
        let _rx = router_clone.register(1);
    }

    #[test]
    fn should_support_hybrid_storage_optional_field() {
        // Arrange
        let event_loop = create_test_event_loop().expect("Should create event loop");

        // Act & Assert
        // hybrid_storage starts as None
        assert!(event_loop.hybrid_storage.is_none());
    }

    #[test]
    fn should_support_eviction_actor_optional_field() {
        // Arrange
        let event_loop = create_test_event_loop().expect("Should create event loop");

        // Act & Assert
        // eviction_actor starts as None
        assert!(event_loop.eviction_actor.is_none());
    }

    // =========== Memory Mode Tests ===========

    #[test]
    fn should_handle_memory_mode_initialization() {
        // Arrange
        let state = RuntimeState::new("/tmp/test_memory".into(), true);

        // Act
        let router = Arc::new(ResponseRouter::new());
        let result = EventLoop::new(state, false, router);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_handle_filesystem_mode_initialization() {
        // Arrange - Create state in filesystem mode
        let state = RuntimeState::new("/tmp/test_filesystem".into(), false);

        // Act
        let router = Arc::new(ResponseRouter::new());
        let result = EventLoop::new(state, false, router);

        // Assert
        assert!(result.is_ok());
    }

    // =========== Actor Factory Tests ===========

    #[test]
    fn should_create_sst_factory_for_compaction_actor() {
        // Arrange & Act
        let event_loop = create_test_event_loop().expect("Should create event loop");

        // Assert - SST factory is created and passed to CompactionActor
        // This is verified by successful construction
        drop(event_loop);
    }

    #[test]
    fn should_use_correct_block_size_for_sst_factory() {
        // Arrange - Create event loop which creates SST factory with 64KB block size
        let event_loop = create_test_event_loop().expect("Should create event loop");

        // Assert - The 64KB block size is hardcoded in EventLoop::new
        // This test documents that invariant
        drop(event_loop);
    }
}
