//! Background compaction management for LSM-tree maintenance
//!
//! Manages the background compaction process that maintains LSM-tree performance
//! by merging overlapping SST files, removing deleted data, and optimizing storage.
//! Handles compaction job scheduling, worker thread lifecycle, and coordination
//! with the main database operations to ensure minimal impact on read/write performance.

use super::strategy::{CompactionPlan, Compactor};
use crate::common::test_hooks::CompactionGatePoint;
use crate::error::{MidgeError, MidgeResult};
use crate::manifest::Manifest;
use crossbeam::channel;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tracing::debug;

/// Messages for compaction worker communication
#[derive(Debug)]
pub enum CompactionMsg {
    /// Request compaction of a specific level
    CompactLevel { cf_id: u32, level: u32 },

    /// Request compaction of a key range
    CompactRange {
        cf_id: u32,
        start_key: Option<Vec<u8>>,
        end_key: Option<Vec<u8>>,
    },

    /// Signal worker to shutdown
    Shutdown,

    /// Barrier: requester wants to be notified when the worker is idle
    Barrier {
        /// reply channel to notify the waiter when idle
        reply: channel::Sender<()>,
    },
}

/// Configuration for the compaction worker
pub struct CompactionWorkerConfig {
    pub db_path: PathBuf,
    pub sst_dir: PathBuf,
    pub sst_factory: Arc<dyn crate::sst::SstFactory>,
    pub sst_reader_factory: Arc<dyn crate::sst::traits::SstReaderFactory>,
    pub snapshot_registry: Arc<crate::api::snapshot::SnapshotRegistry>,
    pub metrics: Arc<crate::metrics::Metrics>,
    pub compression: crate::common::codec::CompressionType,
    pub block_size: usize,
    pub ttl_seconds: Option<u64>,
    pub tombstone_density_threshold: f64,
    pub max_tombstone_compaction_files: usize,
    pub check_interval_ms: u64,
    pub cloud_sst_manager: Option<Arc<crate::sst::cloud::CloudSstManager>>,
    pub compactor: Compactor,
    pub cf_set: Arc<crate::core::engine::column_family::ColumnFamilySet>,
    pub test_hooks: Option<crate::common::test_hooks::TestHooks>,
    pub version_manager: Arc<crate::core::manifest::VersionManager>,
    /// Optional shared background error container used to report errors back to the engine.
    pub background_error: Option<Arc<parking_lot::RwLock<Option<crate::error::MidgeError>>>>,
}

/// Internal work item representing a single compaction plan.
///
/// We don't currently differentiate manual vs automatic behavior here – both
/// are just "plans to execute".
#[derive(Debug)]
struct WorkItem {
    plan: CompactionPlan,
}

/// Coordinates background compaction of LSM-tree levels.
///
/// Encapsulates the compaction worker thread lifecycle and provides a clean API
/// for requesting manual compactions and shutting down gracefully.
pub struct CompactionController {
    /// Channel for sending compaction requests to the background worker
    tx: channel::Sender<CompactionMsg>,
    /// Handle to the background compaction worker thread
    handle: Option<JoinHandle<()>>,
}

impl CompactionController {
    /// Spawn a new background compaction worker thread.
    ///
    /// Creates a dedicated thread that monitors LSM-tree levels and performs
    /// automatic compactions, as well as handling manual compaction requests.
    pub fn spawn(config: CompactionWorkerConfig) -> MidgeResult<Self> {
        let (tx, rx) = channel::unbounded::<CompactionMsg>();

        let db_path = config.db_path.clone();
        let sst_dir = config.sst_dir.clone();
        let sst_factory = config.sst_factory.clone();
        let sst_reader_factory = config.sst_reader_factory.clone();
        let snapshot_registry = config.snapshot_registry.clone();
        let _metrics = config.metrics.clone();
        let compression = config.compression;
        let block_size = config.block_size;
        let _ttl_seconds = config.ttl_seconds;
        let _tombstone_threshold = config.tombstone_density_threshold;
        let _max_tombstone_files = config.max_tombstone_compaction_files;
        let interval = Duration::from_millis(config.check_interval_ms);
        let cloud_sst_manager = config.cloud_sst_manager.clone();
        let compactor = config.compactor;
        let cf_set = config.cf_set.clone();
        let test_hooks = config.test_hooks.clone();
        let version_manager = config.version_manager.clone();
        let background_error = config.background_error.clone();

        let handle = thread::Builder::new()
            .name("midge-compaction-worker".to_string())
            .spawn(move || {
                tracing::info!(interval_ms = interval.as_millis(), "Compaction worker started");
                // Pending work items (manual + automatic).
                let mut work_queue: VecDeque<WorkItem> = VecDeque::new();
                // Number of compactions that are queued or in-flight.
                let mut inflight: usize = 0;
                // Barrier waiters that should be notified once we become idle.
                let mut barrier_waiters: Vec<channel::Sender<()>> = Vec::new();

                loop {
                    let tick_start = std::time::Instant::now();
                    // Step 1: Block until the next message or timeout (interval). This
                    // reduces busy polling and ensures immediate responsiveness to messages.
                    match rx.recv_timeout(interval) {
                        Ok(msg) => {
                            tracing::trace!(wait_ms = %tick_start.elapsed().as_millis(), "compaction received message after wait (ms)");
                            // We received a control message. Process the message and
                            // drain any additional messages available to batch work.
                            if Self::handle_compaction_msg(msg, &db_path, &compactor, &mut work_queue, &mut inflight, &mut barrier_waiters) {
                                // Shutdown requested
                                return;
                            }
                            while let Ok(msg) = rx.try_recv() {
                                if Self::handle_compaction_msg(msg, &db_path, &compactor, &mut work_queue, &mut inflight, &mut barrier_waiters) {
                                    return;
                                }
                            }
                        }
                        Err(channel::RecvTimeoutError::Timeout) => {
                            tracing::trace!(wait_ms = %tick_start.elapsed().as_millis(), "compaction recv timeout tick (ms)");
                            // Periodic tick: if we are idle, attempt automatic compaction.
                            if work_queue.is_empty() {
                                let manifest = Manifest::load(&db_path).unwrap_or_default();
                                let default_cf_config = manifest
                                    .column_families
                                    .first()
                                    .and_then(|cf| cf.config.clone())
                                    .unwrap_or_default();
                                if let Some(plan) = compactor.pick_leveled_compaction(
                                    &manifest.files, 0, default_cf_config.level_size_multiplier, default_cf_config.target_file_size,
                                ) {
                                    tracing::info!(cf_id = plan.cf_id, source_level = plan.source_level, target_level = plan.target_level, input_count = plan.input_files.len(), "automatic compaction plan selected");
                                    work_queue.push_back(WorkItem { plan });
                                    inflight += 1;
                                }
                            }
                        }
                        Err(channel::RecvTimeoutError::Disconnected) => {
                            tracing::warn!("compaction worker channel disconnected unexpectedly");
                            debug!("Compaction receiver disconnected");
                            return;
                        }
                    }

                    // 2) If we currently have no work queued, attempt automatic compaction.
                    if work_queue.is_empty() {
                        let manifest = Manifest::load(&db_path).unwrap_or_default();
                        tracing::debug!(
                            sst_count = manifest.ssts.len(),
                            file_count = manifest.files.len(),
                            "loaded manifest for automatic compaction check"
                        );

                        let default_cf_config = manifest
                            .column_families
                            .first()
                            .and_then(|cf| cf.config.clone())
                            .unwrap_or_default();

                        if let Some(plan) = compactor.pick_leveled_compaction(
                            &manifest.files,
                            0, // default CF id
                            default_cf_config.level_size_multiplier,
                            default_cf_config.target_file_size,
                        ) {
                            tracing::info!(
                                cf_id = plan.cf_id,
                                source_level = plan.source_level,
                                target_level = plan.target_level,
                                input_count = plan.input_files.len(),
                                "automatic compaction plan selected"
                            );
                            work_queue.push_back(WorkItem { plan });
                            inflight += 1;
                        } else {
                            tracing::trace!("no compaction plan selected this iteration");
                        }
                    }

                    // 3) If there is still no work, we are idle.
                    if work_queue.is_empty() {
                        if inflight == 0 && !barrier_waiters.is_empty() {
                            // Satisfy all barrier waiters on idle transition.
                            for waiter in barrier_waiters.drain(..) {
                                let _ = waiter.send(());
                            }
                        }

                        // No queued work: loop back and wait on the channel (recv_timeout)
                        continue;
                    }

                    // 4) Execute a single compaction task.
                    let WorkItem { plan } = match work_queue.pop_front() {
                        Some(item) => item,
                        None => {
                            // Defensive: if something else cleared the queue, just continue.
                            continue;
                        }
                    };

                    tracing::info!(
                        cf_id = plan.cf_id,
                        source_level = plan.source_level,
                        target_level = plan.target_level,
                        input_files = ?plan.input_files,
                        "executing compaction plan"
                    );

                    // Call test hook before compaction starts (returns true if should fail).
                    let should_fail = if let Some(ref hooks) = test_hooks {
                        hooks.maybe_pause_compaction(CompactionGatePoint::BeforeExecution);
                        hooks.before_compaction()
                    } else {
                        false
                    };

                        let result = (|| -> Result<(), crate::error::MidgeError> {
                        if should_fail {
                            return Err(crate::error::MidgeError::internal(
                                "Compaction failed midway (test hook)",
                            ));
                        }

                        // Collect versions from input files.
                        let mut versions = super::executor::collect_compaction_versions(
                            &sst_reader_factory,
                            &sst_dir,
                            &plan.input_files,
                        );

                        if versions.is_empty() {
                            return Ok(());
                        }

                        let range_tombs = super::executor::collect_compaction_range_tombstones(
                            &sst_reader_factory,
                            &sst_dir,
                            &plan.input_files,
                        );

                        // Sort, filter tombstones, apply compaction filter, dedupe.
                        super::executor::sort_versions_for_output(&mut versions);

                        let versions = super::executor::filter_versions_with_range_tombstones(&versions, &range_tombs);
                        let min_snapshot_seq = snapshot_registry.min_active_seq();
                        let (versions_after_filter, _removed) =
                            super::executor::filter_safe_tombstones(&versions, min_snapshot_seq);

                        // Retrieve compaction filter for this CF, or use NoOpFilter as fallback.
                        let filter_arc: Option<Arc<dyn super::filter::CompactionFilter>> =
                            cf_set.cfs.get(&plan.cf_id).and_then(|cf| {
                                let guard = cf.compaction_filter.read();
                                guard.as_ref().map(Arc::clone)
                            });

                        let versions_after_cf = if let Some(filter) = filter_arc {
                            super::executor::apply_compaction_filter(
                                &versions_after_filter,
                                filter.as_ref(),
                                plan.target_level,
                            )
                        } else {
                            let noop = super::filter::NoOpFilter;
                            super::executor::apply_compaction_filter(
                                &versions_after_filter,
                                &noop,
                                plan.target_level,
                            )
                        };

                        let deduped = super::executor::deduplicate_versions(
                            &versions_after_cf,
                            min_snapshot_seq,
                        );

                        // Write compacted SST.
                        let ctx = super::executor::SstWriterContext {
                            sst_factory: &sst_factory,
                            compression,
                            block_size,
                            sst_dir: &sst_dir,
                            cloud_sst_manager: cloud_sst_manager.as_ref(),
                        };

                        let write_res =
                            super::executor::write_compacted_sst(&ctx, &deduped, plan.cf_id)?;

                            if let Some((_path, meta)) = write_res {
                            // Update manifest and version_set atomically via VersionManager.
                            if let Some(ref hooks) = test_hooks {
                                hooks.maybe_pause_compaction(
                                    CompactionGatePoint::BeforeManifestUpdate,
                                );
                            }

                            let combined = crate::core::manifest::VersionEdit::CombinedAddRemove { add: Box::new(meta.clone()), remove: plan.input_files.clone() };
                            {
                                let vm_start = std::time::Instant::now();
                                version_manager.apply_edit_sync(combined)?;
                                tracing::trace!(dur_ms = %vm_start.elapsed().as_millis(), "version_manager.apply_edit_sync duration (ms)");
                            }
                            // Also update the manifest's persisted sequence to reflect the
                            // largest sequence present in the newly written compacted SST.
                                if let Some(lg) = meta.largest_seq {
                                let current_seq = Manifest::load_with_retry(&db_path, 5, std::time::Duration::from_millis(10)).unwrap_or_default().last_persisted_sequence;
                                let seq_to_set = std::cmp::max(current_seq, lg);
                                let seq_edit = crate::core::manifest::VersionEdit::UpdateSequence { sequence: seq_to_set };
                                {
                                    let vm2_start = std::time::Instant::now();
                                    version_manager.apply_edit_sync(seq_edit)?;
                                    tracing::trace!(dur_ms = %vm2_start.elapsed().as_millis(), "version_manager.apply_edit_sync UpdateSequence duration (ms)");
                                }
                            }

                            if let Some(ref hooks) = test_hooks {
                                hooks.maybe_pause_compaction(
                                    CompactionGatePoint::AfterManifestUpdate,
                                );
                            }

                            // Delete old SST files only after manifest persistence is confirmed
                            // Use FileManager's grace period mechanism if available to prevent race conditions
                            for old_sst in &plan.input_files {
                                let old_path = sst_dir.join(old_sst);
                                if old_path.exists() {
                                    // Delete immediately after manifest update to prevent stale reads
                                    if let Err(e) = std::fs::remove_file(&old_path) {
                                        tracing::warn!(path = %old_path.display(), error = %e, "failed to remove old SST during compaction");
                                    } else {
                                        tracing::debug!(path = %old_path.display(), "removed old SST during compaction");
                                    }
                                }
                            }
                        }

                        Ok(())
                    })();

                    match result {
                        Ok(()) => {
                            tracing::info!("compaction executed successfully");
                            if let Some(ref hooks) = test_hooks {
                                hooks.after_compaction();
                            }
                            // Clear background error on successful compaction run
                            if let Some(bg) = &background_error {
                                *bg.write() = None;
                            }
                        }
                        Err(e) => {
                            tracing::warn!(error = ?e, "compaction execution failed");
                            if let Some(ref hooks) = test_hooks {
                                hooks.compaction_failed();
                            }
                            // Set background error indicator if provided
                            if let Some(bg) = &background_error {
                                *bg.write() = Some(crate::error::MidgeError::internal(e.to_string()));
                            }
                        }
                    }

                    // 5) We just finished one compaction.
                    inflight = inflight.saturating_sub(1);

                    // If this brought us to idle (no queued work, no in-flight),
                    // satisfy any barriers immediately.
                    if inflight == 0 && work_queue.is_empty() && !barrier_waiters.is_empty() {
                        for waiter in barrier_waiters.drain(..) {
                            let _ = waiter.send(());
                        }
                    }
                }
            })
            .map_err(|e| {
                MidgeError::internal(format!("Failed to spawn compaction worker: {}", e))
            })?;

        Ok(Self {
            tx,
            handle: Some(handle),
        })
    }

    /// Synchronously run a single compaction plan using the same logic as the
    /// background worker. This is intended for deterministic tests that want
    /// to drive compaction end-to-end without relying on background threads.
    pub fn run_plan_sync(
        &self,
        db_path: &Path,
        cf_set: &Arc<crate::core::engine::column_family::ColumnFamilySet>,
        sst_dir: &Path,
        sst_factory: &Arc<dyn crate::sst::SstFactory>,
        sst_reader_factory: &Arc<dyn crate::sst::traits::SstReaderFactory>,
        snapshot_registry: &Arc<crate::api::snapshot::SnapshotRegistry>,
        compression: crate::common::codec::CompressionType,
        block_size: usize,
        cloud_sst_manager: &Option<Arc<crate::sst::cloud::CloudSstManager>>,
        test_hooks: &Option<crate::common::test_hooks::TestHooks>,
        version_manager: &Arc<crate::core::manifest::VersionManager>,
        background_error: &Option<Arc<parking_lot::RwLock<Option<crate::error::MidgeError>>>>,
        plan: CompactionPlan,
    ) -> MidgeResult<()> {
        // This mirrors the inner portion of the worker loop that executes a
        // single CompactionPlan, but runs on the caller's thread.
        let mut versions = super::executor::collect_compaction_versions(
            sst_reader_factory,
            sst_dir,
            &plan.input_files,
        );

        let range_tombs = super::executor::collect_compaction_range_tombstones(
            sst_reader_factory,
            sst_dir,
            &plan.input_files,
        );

        super::executor::sort_versions_for_output(&mut versions);

        let versions =
            super::executor::filter_versions_with_range_tombstones(&versions, &range_tombs);
        let min_snapshot_seq = snapshot_registry.min_active_seq();
        let (versions_after_filter, _removed) =
            super::executor::filter_safe_tombstones(&versions, min_snapshot_seq);

        let filter_arc: Option<Arc<dyn super::filter::CompactionFilter>> =
            cf_set.cfs.get(&plan.cf_id).and_then(|cf| {
                let guard = cf.compaction_filter.read();
                guard.as_ref().map(Arc::clone)
            });

        let versions_after_cf = if let Some(filter) = filter_arc {
            super::executor::apply_compaction_filter(
                &versions_after_filter,
                filter.as_ref(),
                plan.target_level,
            )
        } else {
            let noop = super::filter::NoOpFilter;
            super::executor::apply_compaction_filter(
                &versions_after_filter,
                &noop,
                plan.target_level,
            )
        };

        let deduped = super::executor::deduplicate_versions(&versions_after_cf, min_snapshot_seq);

        let ctx = super::executor::SstWriterContext {
            sst_factory,
            compression,
            block_size,
            sst_dir,
            cloud_sst_manager: cloud_sst_manager.as_ref(),
        };

        let write_res = super::executor::write_compacted_sst(&ctx, &deduped, plan.cf_id)?;

        if let Some((_path, meta)) = write_res {
            if let Some(ref hooks) = test_hooks {
                hooks.maybe_pause_compaction(CompactionGatePoint::BeforeManifestUpdate);
            }

            let combined = crate::core::manifest::VersionEdit::CombinedAddRemove {
                add: Box::new(meta.clone()),
                remove: plan.input_files.clone(),
            };
            version_manager.apply_edit_sync(combined)?;

            if let Some(lg) = meta.largest_seq {
                let current_seq =
                    Manifest::load_with_retry(db_path, 5, std::time::Duration::from_millis(10))
                        .unwrap_or_default()
                        .last_persisted_sequence;
                let seq_to_set = std::cmp::max(current_seq, lg);
                let seq_edit = crate::core::manifest::VersionEdit::UpdateSequence {
                    sequence: seq_to_set,
                };
                version_manager.apply_edit_sync(seq_edit)?;
            }

            if let Some(ref hooks) = test_hooks {
                hooks.maybe_pause_compaction(CompactionGatePoint::AfterManifestUpdate);
            }

            for old_sst in &plan.input_files {
                let old_path = sst_dir.join(old_sst);
                if old_path.exists() {
                    if let Err(e) = std::fs::remove_file(&old_path) {
                        tracing::warn!(path = %old_path.display(), error = %e, "failed to remove old SST during compaction");
                    } else {
                        tracing::debug!(path = %old_path.display(), "removed old SST during compaction");
                    }
                }
            }

            if let Some(ref hooks) = test_hooks {
                hooks.after_compaction();
            }

            if let Some(bg) = background_error {
                *bg.write() = None;
            }
        }

        Ok(())
    }

    /// Internal helper to process a single compaction control message. Returns
    /// true if the worker should shut down immediately (Shutdown received).
    fn handle_compaction_msg(
        msg: CompactionMsg,
        db_path: &Path,
        compactor: &Compactor,
        work_queue: &mut VecDeque<WorkItem>,
        inflight: &mut usize,
        barrier_waiters: &mut Vec<channel::Sender<()>>,
    ) -> bool {
        match msg {
            CompactionMsg::CompactLevel { cf_id, level } => {
                let manifest = Manifest::load(db_path).unwrap_or_default();
                let cf_config = manifest
                    .column_families
                    .iter()
                    .find(|cf| cf.id == cf_id)
                    .and_then(|cf| cf.config.clone())
                    .unwrap_or_default();
                if let Some(plan) = compactor.pick_manual_compaction_level(
                    &manifest.files,
                    cf_id,
                    level,
                    cf_config.level_size_multiplier,
                    cf_config.target_file_size,
                ) {
                    work_queue.push_back(WorkItem { plan });
                    *inflight += 1;
                }
            }
            CompactionMsg::CompactRange {
                cf_id,
                start_key,
                end_key,
            } => {
                let manifest = Manifest::load(db_path).unwrap_or_default();
                let cf_config = manifest
                    .column_families
                    .iter()
                    .find(|cf| cf.id == cf_id)
                    .and_then(|cf| cf.config.clone())
                    .unwrap_or_default();
                if let Some(plan) = compactor.pick_manual_compaction_range(
                    &manifest.files,
                    cf_id,
                    start_key.as_deref(),
                    end_key.as_deref(),
                    cf_config.level_size_multiplier,
                    cf_config.target_file_size,
                ) {
                    work_queue.push_back(WorkItem { plan });
                    *inflight += 1;
                }
            }
            CompactionMsg::Barrier { reply } => {
                barrier_waiters.push(reply);
            }
            CompactionMsg::Shutdown => {
                debug!("Compaction shutdown requested");
                return true;
            }
        }

        false
    }

    /// Request compaction of a specific level in a column family.
    ///
    /// Sends a manual compaction request to the background worker. This is non-blocking
    /// and returns immediately after queueing the request.
    pub fn compact_level(&self, cf_id: u32, level: u32) -> MidgeResult<()> {
        self.tx
            .send(CompactionMsg::CompactLevel { cf_id, level })
            .map_err(|_| MidgeError::internal("Compaction worker channel closed"))
    }

    /// Request compaction of a key range in a column family.
    ///
    /// Sends a manual compaction request to the background worker. This is non-blocking
    /// and returns immediately after queueing the request.
    pub fn compact_range(
        &self,
        cf_id: u32,
        start_key: Option<Vec<u8>>,
        end_key: Option<Vec<u8>>,
    ) -> MidgeResult<()> {
        self.tx
            .send(CompactionMsg::CompactRange {
                cf_id,
                start_key,
                end_key,
            })
            .map_err(|_| MidgeError::internal("Compaction worker channel closed"))
    }

    /// Wait until the compaction worker is idle (processed prior requests and finished
    /// any in-flight compaction). This sends a Barrier message and waits for the
    /// worker to acknowledge. A timeout is required to avoid hanging tests if the
    /// worker is deadlocked.
    ///
    /// This waits for stability - the worker must be idle for a short period to
    /// ensure cascading compactions have also completed.
    pub fn wait_until_idle(&self, timeout: std::time::Duration) -> MidgeResult<()> {
        let start_time = std::time::Instant::now();
        let stability_duration = std::time::Duration::from_millis(100);

        loop {
            let (s, r) = channel::bounded::<()>(1);
            self.tx
                .send(CompactionMsg::Barrier { reply: s })
                .map_err(|_| MidgeError::internal("Compaction worker channel closed"))?;

            match r.recv_timeout(timeout.saturating_sub(start_time.elapsed())) {
                Ok(()) => {
                    // Worker is currently idle. Wait a bit and check again to ensure stability.
                    std::thread::sleep(stability_duration);
                    let (s2, r2) = channel::bounded::<()>(1);
                    self.tx
                        .send(CompactionMsg::Barrier { reply: s2 })
                        .map_err(|_| MidgeError::internal("Compaction worker channel closed"))?;

                    match r2.recv_timeout(std::time::Duration::from_millis(50)) {
                        Ok(()) => {
                            // Still idle after stability period - we're done
                            tracing::trace!(wait_ms = %start_time.elapsed().as_millis(), "Compaction wait_until_idle completed (ms)");
                            return Ok(());
                        }
                        Err(channel::RecvTimeoutError::Timeout) => {
                            // Worker became busy again, continue waiting
                            continue;
                        }
                        Err(channel::RecvTimeoutError::Disconnected) => {
                            return Err(MidgeError::internal("Compaction worker disconnected"));
                        }
                    }
                }
                Err(channel::RecvTimeoutError::Timeout) => {
                    return Err(MidgeError::internal(
                        "Timed out waiting for compaction worker to become idle",
                    ));
                }
                Err(channel::RecvTimeoutError::Disconnected) => {
                    return Err(MidgeError::internal("Compaction worker disconnected"));
                }
            }
        }
    }

    /// Gracefully shutdown the compaction worker and wait for completion.
    ///
    /// Sends a shutdown signal and joins the worker thread. Consumes self
    /// to ensure the coordinator cannot be used after shutdown.
    pub fn shutdown(mut self) -> MidgeResult<()> {
        // Send shutdown signal (ignore error if receiver already dropped).
        let _ = self.tx.send(CompactionMsg::Shutdown);

        // Wait for worker thread to finish.
        if let Some(handle) = self.handle.take() {
            handle.join().map_err(|_| {
                MidgeError::internal("Compaction worker thread panicked during shutdown")
            })?;
        }

        Ok(())
    }

    /// Check if the compaction worker is still running.
    ///
    /// Returns false if the worker thread has terminated or shutdown was called.
    #[cfg(test)]
    pub fn is_running(&self) -> bool {
        self.handle.is_some()
    }
}

impl Drop for CompactionController {
    fn drop(&mut self) {
        // Best-effort shutdown signal.
        let _ = self.tx.send(CompactionMsg::Shutdown);

        // Wait for thread to finish if handle still exists.
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::codec::CompressionType;
    use crate::core::compaction::LeveledCompactionConfig;
    use std::sync::Arc;
    use tempfile::TempDir;

    struct TestGuard {
        _version_mgr: Arc<crate::core::manifest::VersionManager>,
    }

    impl Drop for TestGuard {
        fn drop(&mut self) {
            // Explicit shutdown to avoid warnings
            self._version_mgr.shutdown();
        }
    }

    fn create_test_config(temp_dir: &TempDir) -> (CompactionWorkerConfig, TestGuard) {
        let db_path = temp_dir.path().to_path_buf();
        let sst_dir = temp_dir.path().join("sst");

        // Create manifest file
        let manifest = Manifest::default();
        let _ = manifest.save_atomic(&db_path);

        let version_manager = Arc::new(crate::core::manifest::VersionManager::new(
            crate::core::manifest::AtomicVersionSet::new(crate::core::manifest::VersionSet::new(
                manifest,
            )),
            db_path.clone(),
            None,  // No test hooks in this test
            false, // Not memory mode
        ));

        let config = CompactionWorkerConfig {
            db_path: db_path.clone(),
            sst_dir,
            sst_factory: Arc::new(crate::sst::mem::MemSstFactory),
            sst_reader_factory: Arc::new(crate::sst::mem::MemSstReaderFactory::new(false)),
            snapshot_registry: Arc::new(crate::api::snapshot::SnapshotRegistry::new()),
            metrics: Arc::new(crate::metrics::Metrics::new()),
            compression: CompressionType::None,
            block_size: 4096,
            ttl_seconds: None,
            tombstone_density_threshold: 0.3,
            max_tombstone_compaction_files: 10,
            check_interval_ms: 10,
            cloud_sst_manager: None,
            compactor: Compactor::with_config(LeveledCompactionConfig {
                l0_compaction_threshold: 4 * 1024 * 1024,
                level_multiplier: 10,
                l1_target_size: 10 * 1024 * 1024,
                max_levels: 7,
            }),
            cf_set: Arc::new(crate::core::engine::column_family::ColumnFamilySet::new()),
            test_hooks: None,
            version_manager: Arc::clone(&version_manager),
            background_error: None,
        };

        let guard = TestGuard {
            _version_mgr: version_manager,
        };

        (config, guard)
    }

    #[test]
    fn should_spawn_coordinator_successfully() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(temp_dir.path().join("sst")).unwrap();
        let (config, _version_mgr) = create_test_config(&temp_dir);

        // Act
        let coordinator = CompactionController::spawn(config).unwrap();

        // Assert
        assert!(coordinator.is_running());
        coordinator.shutdown().unwrap();
    }

    #[test]
    fn should_accept_manual_compaction_level_request() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(temp_dir.path().join("sst")).unwrap();
        let (config, _version_mgr) = create_test_config(&temp_dir);
        let coordinator = CompactionController::spawn(config).unwrap();

        // Act
        let result = coordinator.compact_level(0, 0);
        let wait_result = coordinator.wait_until_idle(Duration::from_secs(5));

        // Assert
        assert!(result.is_ok());
        assert!(wait_result.is_ok());
        coordinator.shutdown().unwrap();
    }

    #[test]
    fn should_accept_manual_compaction_range_request() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(temp_dir.path().join("sst")).unwrap();
        let (config, _version_mgr) = create_test_config(&temp_dir);
        let coordinator = CompactionController::spawn(config).unwrap();

        // Act
        let result = coordinator.compact_range(0, Some(b"a".to_vec()), Some(b"z".to_vec()));
        let wait_result = coordinator.wait_until_idle(Duration::from_secs(5));

        // Assert
        assert!(result.is_ok());
        assert!(wait_result.is_ok());
        coordinator.shutdown().unwrap();
    }

    #[test]
    fn should_shutdown_gracefully() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(temp_dir.path().join("sst")).unwrap();
        let (config, _version_mgr) = create_test_config(&temp_dir);
        let coordinator = CompactionController::spawn(config).unwrap();

        // Act
        let result = coordinator.shutdown();

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_process_requests_before_shutdown() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(temp_dir.path().join("sst")).unwrap();
        let (config, _version_mgr) = create_test_config(&temp_dir);
        let coordinator = CompactionController::spawn(config).unwrap();

        // Act
        coordinator.compact_level(0, 0).unwrap();
        coordinator.compact_range(0, None, None).unwrap();
        coordinator.wait_until_idle(Duration::from_secs(5)).unwrap();
        let result = coordinator.shutdown();

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_cleanup_on_drop() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(temp_dir.path().join("sst")).unwrap();
        let (config, _version_mgr) = create_test_config(&temp_dir);
        let coordinator = CompactionController::spawn(config).unwrap();
        coordinator.compact_level(0, 0).unwrap();
        coordinator.wait_until_idle(Duration::from_secs(5)).unwrap();

        // Act
        drop(coordinator);

        // Assert
        // Thread should terminate gracefully (no panic)
    }

    #[test]
    fn should_handle_multiple_requests() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(temp_dir.path().join("sst")).unwrap();
        let (config, _version_mgr) = create_test_config(&temp_dir);
        let coordinator = CompactionController::spawn(config).unwrap();

        // Act
        for level in 0..3 {
            coordinator.compact_level(0, level).unwrap();
        }
        let wait_result = coordinator.wait_until_idle(Duration::from_secs(5));

        // Assert
        assert!(wait_result.is_ok());
        assert!(coordinator.is_running());
        coordinator.shutdown().unwrap();
    }
}
