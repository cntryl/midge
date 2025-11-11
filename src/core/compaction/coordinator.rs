//! Compaction coordinator for background LSM-tree compaction
//!
//! Manages the lifecycle of the background compaction worker thread, including
//! spawning, job submission, and graceful shutdown.

use super::strategy::Compactor;
use crate::error::{MidgeError, MidgeResult};
use crate::manifest::Manifest;
use crossbeam::channel;
use std::path::PathBuf;
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
    pub metrics: Arc<crate::core::metrics::Metrics>,
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
}

/// Coordinates background compaction of LSM-tree levels.
///
/// Encapsulates the compaction worker thread lifecycle and provides a clean API
/// for requesting manual compactions and shutting down gracefully.
pub struct CompactionCoordinator {
    /// Channel for sending compaction requests to the background worker
    tx: channel::Sender<CompactionMsg>,
    /// Handle to the background compaction worker thread
    handle: Option<JoinHandle<()>>,
}

impl CompactionCoordinator {
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

        let handle = thread::Builder::new()
            .name("midge-compaction-worker".to_string())
            .spawn(move || {
                // Track barrier waiters that want an ack when the worker becomes idle
                let mut barrier_waiters: Vec<channel::Sender<()>> = Vec::new();
                loop {
                    // Check for manual compaction requests (non-blocking)
                    let manual_plan = match rx.try_recv() {
                        Ok(CompactionMsg::CompactLevel { cf_id, level }) => {
                            let manifest = Manifest::load(&db_path).unwrap_or_default();

                            // Get CF config for compaction settings
                            let cf_config = manifest
                                .column_families
                                .iter()
                                .find(|cf| cf.id == cf_id)
                                .and_then(|cf| cf.config.clone())
                                .unwrap_or_default();

                            compactor.pick_manual_compaction_level(
                                &manifest.files,
                                cf_id,
                                level,
                                cf_config.level_size_multiplier,
                                cf_config.target_file_size,
                            )
                        }
                        Ok(CompactionMsg::CompactRange {
                            cf_id,
                            start_key,
                            end_key,
                        }) => {
                            let manifest = Manifest::load(&db_path).unwrap_or_default();

                            // Get CF config for compaction settings
                            let cf_config = manifest
                                .column_families
                                .iter()
                                .find(|cf| cf.id == cf_id)
                                .and_then(|cf| cf.config.clone())
                                .unwrap_or_default();

                            compactor.pick_manual_compaction_range(
                                &manifest.files,
                                cf_id,
                                start_key.as_deref(),
                                end_key.as_deref(),
                                cf_config.level_size_multiplier,
                                cf_config.target_file_size,
                            )
                        }
                        Ok(CompactionMsg::Shutdown) => {
                            debug!("Compaction shutdown requested");
                            return;
                        }
                        Ok(CompactionMsg::Barrier { reply }) => {
                            // Enqueue barrier waiter; worker will reply when it becomes idle
                            barrier_waiters.push(reply);
                            None
                        }
                        Err(channel::TryRecvError::Empty) => None,
                        Err(channel::TryRecvError::Disconnected) => {
                            debug!("Compaction channel disconnected, exiting worker");
                            return;
                        }
                    };

                    // If there is no plan and there are barrier waiters and no pending
                    // messages, notify waiters that we're idle.
                    if manual_plan.is_none() && !barrier_waiters.is_empty() && rx.is_empty() {
                        for waiter in barrier_waiters.drain(..) {
                            let _ = waiter.send(());
                        }
                    }

                    // Execute manual compaction if one was requested
                    let plan = if manual_plan.is_some() {
                        manual_plan
                    } else {
                        // No manual compaction, try automatic compaction
                        thread::sleep(interval);

                        let manifest = Manifest::load(&db_path).unwrap_or_default();

                        // Get default CF config for compaction settings
                        let default_cf_config = manifest
                            .column_families
                            .first()
                            .and_then(|cf| cf.config.clone())
                            .unwrap_or_default();

                        compactor.pick_leveled_compaction(
                            &manifest.files,
                            0, // Default CF
                            default_cf_config.level_size_multiplier,
                            default_cf_config.target_file_size,
                        )
                    };

                    // Execute compaction plan if we have one
                    if let Some(plan) = plan {
                        debug!("Compaction plan selected");

                        // Call test hook before compaction starts (returns true if should fail)
                        let should_fail = if let Some(ref hooks) = test_hooks {
                            hooks.before_compaction()
                        } else {
                            false
                        };

                        // Perform a simple compaction execution using the compaction executor
                        // Steps: collect versions -> sort -> tombstone filter -> apply filter -> dedupe -> write SST -> update manifest
                        match (|| -> Result<(), crate::error::MidgeError> {
                            // Check for FailMidway test hook behavior
                            if should_fail {
                                return Err(crate::error::MidgeError::internal(
                                    "Compaction failed midway (test hook)",
                                ));
                            }

                            // Collect versions from input files
                            let mut versions = super::executor::collect_compaction_versions(
                                &sst_reader_factory,
                                &sst_dir,
                                &plan.input_files,
                            );

                            if versions.is_empty() {
                                return Ok(());
                            }

                            // Sort, filter tombstones, apply compaction filter, dedupe
                            super::executor::sort_versions_for_output(&mut versions);

                            let min_snapshot_seq = snapshot_registry.min_active_seq();
                            let (versions_after_filter, _removed) =
                                super::executor::filter_safe_tombstones(
                                    &versions,
                                    min_snapshot_seq,
                                );

                            // Retrieve compaction filter for this CF, or use NoOpFilter as fallback
                            let filter_arc: Option<Arc<dyn super::filter::CompactionFilter>> = cf_set
                                .cfs
                                .get(&plan.cf_id)
                                .and_then(|cf| {
                                    let guard = cf.compaction_filter.read();
                                    if let Some(ref arc) = *guard {
                                        Some(Arc::clone(arc))
                                    } else {
                                        None
                                    }
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

                            let deduped = super::executor::deduplicate_versions(&versions_after_cf);

                            // Write compacted SST
                            let ctx = super::executor::SstWriterContext {
                                sst_factory: &sst_factory,
                                compression,
                                block_size,
                                sst_dir: &sst_dir,
                                cloud_sst_manager: cloud_sst_manager.as_ref(),
                            };
                            let write_res = super::executor::write_compacted_sst(
                                &ctx,
                                &deduped,
                                plan.cf_id,
                            )?;

                            if let Some((_path, meta)) = write_res {
                                // Update manifest on disk: remove inputs, add new file meta
                                let mut m = crate::manifest::Manifest::load_with_retry(
                                    &db_path,
                                    10,
                                    Duration::from_millis(10),
                                )?;

                                // Remove input files from manifest.ssts and files
                                for fname in &plan.input_files {
                                    m.ssts.retain(|n| n != fname);
                                    m.files.retain(|f| &f.name != fname);
                                }

                                // Add new SST to manifest
                                m.ssts.push(meta.name.clone());
                                m.files.push(meta);

                                // Persist manifest
                                m.save_atomic(&db_path)?;
                            }
                            // After finishing a compaction, if there are any barrier waiters
                            // and no pending messages, notify them now (worker is idle).
                            if !barrier_waiters.is_empty() && rx.is_empty() {
                                for waiter in barrier_waiters.drain(..) {
                                    let _ = waiter.send(());
                                }
                            }

                            Ok(())
                        })() {
                            Ok(()) => {
                                debug!("Compaction executed successfully");
                                // Call hook after successful compaction
                                if let Some(ref hooks) = test_hooks {
                                    hooks.after_compaction();
                                }
                            }
                            Err(e) => {
                                debug!(error = ?e, "Compaction execution failed");
                                // Call hook after failed compaction
                                if let Some(ref hooks) = test_hooks {
                                    hooks.compaction_failed();
                                }
                            }
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
    pub fn wait_until_idle(&self, timeout: std::time::Duration) -> MidgeResult<()> {
        let (s, r) = channel::bounded::<()>(1);
        self.tx
            .send(CompactionMsg::Barrier { reply: s })
            .map_err(|_| MidgeError::internal("Compaction worker channel closed"))?;

        match r.recv_timeout(timeout) {
            Ok(()) => Ok(()),
            Err(channel::RecvTimeoutError::Timeout) => Err(MidgeError::internal(
                "Timed out waiting for compaction worker to become idle",
            )),
            Err(channel::RecvTimeoutError::Disconnected) => {
                Err(MidgeError::internal("Compaction worker disconnected"))
            }
        }
    }

    /// Gracefully shutdown the compaction worker and wait for completion.
    ///
    /// Sends a shutdown signal and joins the worker thread. Consumes self
    /// to ensure the coordinator cannot be used after shutdown.
    pub fn shutdown(mut self) -> MidgeResult<()> {
        // Send shutdown signal (ignore error if receiver already dropped)
        let _ = self.tx.send(CompactionMsg::Shutdown);

        // Wait for worker thread to finish
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

impl Drop for CompactionCoordinator {
    fn drop(&mut self) {
        // Best-effort shutdown signal
        let _ = self.tx.send(CompactionMsg::Shutdown);

        // Wait for thread to finish if handle still exists
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::codec::CompressionType;
    use crate::compactor::LeveledCompactionConfig;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_config(temp_dir: &TempDir) -> CompactionWorkerConfig {
        let db_path = temp_dir.path().to_path_buf();
        let sst_dir = temp_dir.path().join("sst");

        // Create manifest file
        let manifest = Manifest::default();
        let _ = manifest.save_atomic(&db_path);

        CompactionWorkerConfig {
            db_path: db_path.clone(),
            sst_dir,
            sst_factory: Arc::new(crate::sst::mem::MemSstFactory),
            sst_reader_factory: Arc::new(crate::sst::mem::MemSstReaderFactory::new(false)),
            snapshot_registry: Arc::new(crate::snapshot_compat::SnapshotRegistry::new()),
            metrics: Arc::new(crate::core::metrics::Metrics::new()),
            compression: CompressionType::None,
            block_size: 4096,
            ttl_seconds: None,
            tombstone_density_threshold: 0.3,
            max_tombstone_compaction_files: 10,
            check_interval_ms: 100,
            cloud_sst_manager: None,
            compactor: Compactor::with_config(LeveledCompactionConfig {
                l0_compaction_threshold: 4 * 1024 * 1024,
                level_multiplier: 10,
                l1_target_size: 10 * 1024 * 1024,
                max_levels: 7,
            }),
            cf_set: Arc::new(crate::core::engine::column_family::ColumnFamilySet::new()),
            test_hooks: None,
        }
    }

    #[test]
    fn should_spawn_coordinator_successfully() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(temp_dir.path().join("sst")).unwrap();
        let config = create_test_config(&temp_dir);

        // Act
        let coordinator = CompactionCoordinator::spawn(config);

        // Assert
        assert!(coordinator.is_ok());
        let coord = coordinator.unwrap();
        assert!(coord.is_running());
    }

    #[test]
    fn should_accept_manual_compaction_level_request() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(temp_dir.path().join("sst")).unwrap();
        let config = create_test_config(&temp_dir);
        let coordinator = CompactionCoordinator::spawn(config).unwrap();

        // Act
        let result = coordinator.compact_level(0, 0);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_accept_manual_compaction_range_request() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(temp_dir.path().join("sst")).unwrap();
        let config = create_test_config(&temp_dir);
        let coordinator = CompactionCoordinator::spawn(config).unwrap();

        // Act
        let result = coordinator.compact_range(0, Some(b"a".to_vec()), Some(b"z".to_vec()));

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_shutdown_gracefully() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(temp_dir.path().join("sst")).unwrap();
        let config = create_test_config(&temp_dir);
        let coordinator = CompactionCoordinator::spawn(config).unwrap();

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
        let config = create_test_config(&temp_dir);
        let coordinator = CompactionCoordinator::spawn(config).unwrap();

        // Act
        coordinator.compact_level(0, 0).unwrap();
        coordinator.compact_range(0, None, None).unwrap();
        let result = coordinator.shutdown();

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_cleanup_on_drop() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(temp_dir.path().join("sst")).unwrap();
        let config = create_test_config(&temp_dir);
        let coordinator = CompactionCoordinator::spawn(config).unwrap();

        coordinator.compact_level(0, 0).unwrap();

        // Act - Drop coordinator without explicit shutdown
        drop(coordinator);

        // Assert - Thread should terminate gracefully (no panic)
        std::thread::sleep(Duration::from_millis(200));
    }

    #[test]
    fn should_handle_multiple_requests() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(temp_dir.path().join("sst")).unwrap();
        let config = create_test_config(&temp_dir);
        let coordinator = CompactionCoordinator::spawn(config).unwrap();

        // Act
        for level in 0..3 {
            let result = coordinator.compact_level(0, level);
            assert!(result.is_ok());
        }

        // Assert
        std::thread::sleep(Duration::from_millis(200));
        assert!(coordinator.is_running());
    }
}
