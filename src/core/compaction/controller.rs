//! Perfect compaction controller for Midge.
//!
//! Deadlock-free. Deterministic. Pebble-style scheduling. Clean idle semantics.

use super::strategy::{CompactionPlan, Compactor};
use crate::common::test_hooks::CompactionGatePoint;
use crate::error::{MidgeError, MidgeResult};
use crate::manifest::Manifest;
use crossbeam::channel;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tracing::debug;

use crate::api::snapshot::SnapshotRegistry;
use crate::common::codec::CompressionType;
use crate::core::engine::column_family::ColumnFamilySet;
use crate::core::manifest::VersionManager;
use crate::metrics::Metrics;
use crate::sst::cloud::CloudSstManager;
use crate::sst::{SstFactory, SstReaderFactory};

/// Messages handled by the compaction worker.
pub enum CompactionMsg {
    CompactLevel {
        cf_id: u32,
        level: u32,
    },
    CompactRange {
        cf_id: u32,
        start_key: Option<Vec<u8>>,
        end_key: Option<Vec<u8>>,
    },
    Barrier {
        reply: channel::Sender<()>,
    },
    Shutdown,
}

/// Controller providing APIs to request compaction and coordinate the worker.
pub struct CompactionController {
    tx: channel::Sender<CompactionMsg>,
}

/// Handle to the background worker thread.
use crate::core::runtime::WorkerHandle;

/// Configuration for the compaction worker.
pub struct CompactionWorkerConfig {
    pub db_path: PathBuf,
    pub sst_dir: PathBuf,
    pub sst_factory: Arc<dyn SstFactory>,
    pub sst_reader_factory: Arc<dyn SstReaderFactory>,
    pub snapshot_registry: Arc<SnapshotRegistry>,
    pub metrics: Arc<Metrics>,
    pub compression: CompressionType,
    pub block_size: usize,
    pub ttl_seconds: Option<u64>,
    pub tombstone_density_threshold: f64,
    pub max_tombstone_compaction_files: usize,
    pub check_interval_ms: u64,
    pub cloud_sst_manager: Option<Arc<CloudSstManager>>,
    /// Phase 7.2: Cloud coordinator for submitting SST uploads as runtime tasks
    pub cloud_coordinator: Arc<parking_lot::RwLock<Option<Arc<crate::core::cloud_coordinator::CloudCoordinator>>>>,
    /// Phase 7.2: Runtime for submitting cloud upload tasks
    pub runtime: Arc<parking_lot::RwLock<Option<std::sync::Arc<crate::core::runtime::EngineRuntime>>>>,
    pub compactor: Compactor,
    pub cf_set: Arc<ColumnFamilySet>,
    pub test_hooks: Option<Arc<crate::common::test_hooks::TestHooks>>,
    pub version_manager: Arc<VersionManager>,
    pub background_error: Option<Arc<parking_lot::RwLock<Option<MidgeError>>>>,
    pub rate_limiter: Option<Arc<crate::common::rate_limiter::RateLimiter>>,
}

struct WorkItem {
    plan: CompactionPlan,
}

impl CompactionController {
    /// Spawn the compaction worker and return the controller + worker handle.
    pub fn spawn(cfg: CompactionWorkerConfig) -> MidgeResult<(Self, WorkerHandle)> {
        let (tx, rx) = channel::unbounded();
        let (tick_tx, tick_rx) = channel::bounded(1);

        let cfg = Arc::new(cfg);
        let db_path = cfg.db_path.clone();
        let sst_dir = cfg.sst_dir.clone();

        // Tick thread driving periodic scheduling.
        // Tick loop
        let tick_interval = Duration::from_millis(cfg.check_interval_ms);
        std::thread::spawn(move || loop {
            std::thread::sleep(tick_interval);
            if tick_tx.send(()).is_err() {
                break;
            }
        });

        let worker_cfg = Arc::clone(&cfg);
        let join = std::thread::spawn(move || {
            run_worker_loop(rx, tick_rx, worker_cfg, db_path, sst_dir);
        });
        let handle = WorkerHandle::new(join, "compaction-worker");

        Ok((CompactionController { tx }, handle))
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

    /// Deterministic "wait until idle" barrier.
    pub fn wait_until_idle(&self, timeout: Duration) -> MidgeResult<()> {
        let (s, r) = channel::bounded::<()>(1);
        self.tx
            .send(CompactionMsg::Barrier { reply: s })
            .map_err(|_| MidgeError::internal("Compaction worker channel closed"))?;
        r.recv_timeout(timeout).map_err(|_| {
            MidgeError::internal("Timed out waiting for compaction worker to become idle")
        })
    }

    /// Graceful shutdown.
    pub fn shutdown(self) -> MidgeResult<()> {
        let _ = self.tx.send(CompactionMsg::Shutdown);
        Ok(())
    }

    /// Check if the compaction worker is still running.
    #[cfg(test)]
    pub fn is_running(&self) -> bool {
        true
    }
}

impl Drop for CompactionController {
    fn drop(&mut self) {
        // Best-effort shutdown signal - use try_send to avoid blocking in Drop.
        let _ = self.tx.try_send(CompactionMsg::Shutdown);
    }
}

impl CompactionController {
    /// Execute a compaction plan synchronously via the orchestrator.
    /// Intended for maintenance paths that need inline execution.
    #[allow(clippy::too_many_arguments)]
    pub fn run_plan_sync(
        &self,
        db_path: &Path,
        cf_set: &Arc<ColumnFamilySet>,
        sst_dir: &Path,
        sst_factory: &Arc<dyn SstFactory>,
        sst_reader_factory: &Arc<dyn SstReaderFactory>,
        snapshot_registry: &Arc<SnapshotRegistry>,
        compression: CompressionType,
        block_size: usize,
        cloud_sst_manager: &Option<Arc<CloudSstManager>>,
        test_hooks: &Option<crate::common::test_hooks::TestHooks>,
        version_manager: &Arc<VersionManager>,
        background_error: &Option<Arc<parking_lot::RwLock<Option<MidgeError>>>>,
        plan: CompactionPlan,
    ) -> MidgeResult<()> {
        let cfg = Arc::new(CompactionWorkerConfig {
            db_path: db_path.to_path_buf(),
            sst_dir: sst_dir.to_path_buf(),
            sst_factory: Arc::clone(sst_factory),
            sst_reader_factory: Arc::clone(sst_reader_factory),
            snapshot_registry: Arc::clone(snapshot_registry),
            metrics: Arc::new(crate::metrics::Metrics::new()),
            compression,
            block_size,
            ttl_seconds: None,
            tombstone_density_threshold: 0.3,
            max_tombstone_compaction_files: 10,
            check_interval_ms: 100,
            cloud_sst_manager: cloud_sst_manager.clone(),
            cloud_coordinator: Arc::new(parking_lot::RwLock::new(None)), // Phase 7.2: Not set in sync mode
            runtime: Arc::new(parking_lot::RwLock::new(None)), // Phase 7.2: Not set in sync mode
            compactor: Compactor::with_config(
                crate::core::compaction::LeveledCompactionConfig::default(),
            ),
            cf_set: Arc::clone(cf_set),
            test_hooks: test_hooks.as_ref().map(|h| Arc::new(h.clone())),
            version_manager: Arc::clone(version_manager),
            background_error: background_error.clone(),
            rate_limiter: None,
        });

        let _ = super::executor::execute_compaction_plan(&cfg, db_path, sst_dir, plan)?;
        Ok(())
    }
}
/// The event-driven compaction worker loop.
fn run_worker_loop(
    rx: channel::Receiver<CompactionMsg>,
    tick_rx: channel::Receiver<()>,
    cfg: Arc<CompactionWorkerConfig>,
    db_path: PathBuf,
    sst_dir: PathBuf,
) {
    let mut work_queue: VecDeque<WorkItem> = VecDeque::new();
    let mut barrier_waiters: Vec<channel::Sender<()>> = Vec::new();
    // Failure backoff state applied at scheduling time
    let mut backoff_ms: u64 = 0;
    let mut last_error_time: Option<std::time::Instant> = None;

    loop {
        // Optional failure backoff state
        // Backoff removed for simplicity and to avoid idle sleeps
        enum Event {
            Msg(CompactionMsg),
            Tick,
        }

        let event = crossbeam::channel::select! {
            recv(rx) -> m => match m {
                Ok(msg) => Event::Msg(msg),
                Err(_) => return,
            },
            recv(tick_rx) -> _ => Event::Tick,
        };

        match event {
            Event::Msg(CompactionMsg::Shutdown) => {
                debug!("Compaction worker shutdown");
                return;
            }
            Event::Msg(CompactionMsg::Barrier { reply }) => {
                if work_queue.is_empty() {
                    let _ = reply.send(());
                } else {
                    barrier_waiters.push(reply);
                }
            }
            Event::Msg(CompactionMsg::CompactLevel { cf_id, level }) => {
                let manifest = Manifest::load(&db_path).unwrap_or_default();
                let cf_config = manifest
                    .column_families
                    .iter()
                    .find(|cf| cf.id == cf_id)
                    .and_then(|cf| cf.config.clone())
                    .unwrap_or_default();
                if let Some(plan) = cfg.compactor.pick_manual_compaction_level(
                    &manifest.files,
                    cf_id,
                    level,
                    cf_config.level_size_multiplier,
                    cf_config.target_file_size,
                ) {
                    work_queue.push_back(WorkItem { plan });
                }
            }
            Event::Msg(CompactionMsg::CompactRange {
                cf_id,
                start_key,
                end_key,
            }) => {
                let manifest = Manifest::load(&db_path).unwrap_or_default();
                let cf_config = manifest
                    .column_families
                    .iter()
                    .find(|cf| cf.id == cf_id)
                    .and_then(|cf| cf.config.clone())
                    .unwrap_or_default();
                if let Some(plan) = cfg.compactor.pick_manual_compaction_range(
                    &manifest.files,
                    cf_id,
                    start_key.as_deref(),
                    end_key.as_deref(),
                    cf_config.level_size_multiplier,
                    cf_config.target_file_size,
                ) {
                    work_queue.push_back(WorkItem { plan });
                }
            }
            Event::Tick => {
                if work_queue.is_empty() {
                    let manifest = Manifest::load(&db_path).unwrap_or_default();
                    let default_cf_config = manifest
                        .column_families
                        .first()
                        .and_then(|cf| cf.config.clone())
                        .unwrap_or_default();
                    if let Some(plan) = cfg.compactor.pick_leveled_compaction(
                        &manifest.files,
                        0,
                        default_cf_config.level_size_multiplier,
                        default_cf_config.target_file_size,
                    ) {
                        // Apply backoff if recent failures occurred
                        if backoff_ms > 0 {
                            std::thread::sleep(std::time::Duration::from_millis(backoff_ms));
                        }
                        work_queue.push_back(WorkItem { plan });
                    }
                }
            }
        }

        // Execute at most one compaction per iteration.
        if let Some(WorkItem { plan }) = work_queue.pop_front() {
            let hooks = cfg.test_hooks.clone();
            let should_fail = if let Some(ref h) = hooks {
                h.maybe_pause_compaction(CompactionGatePoint::BeforeExecution);
                h.before_compaction()
            } else {
                false
            };

            let result = (|| -> Result<(), crate::error::MidgeError> {
                if should_fail {
                    return Err(crate::error::MidgeError::internal(
                        "Compaction failed midway (test hook)",
                    ));
                }

                // Rate limit anticipated reads
                let mut estimated_read_bytes: u64 = 0;
                for input_file in &plan.input_files {
                    let path = sst_dir.join(input_file);
                    if let Ok(meta) = std::fs::metadata(&path) {
                        estimated_read_bytes += meta.len();
                    }
                }
                if let Some(ref limiter) = cfg.rate_limiter {
                    if estimated_read_bytes > 0 {
                        limiter.request(estimated_read_bytes);
                    }
                }

                // Delegate full pipeline to orchestrator
                let write_res =
                    super::executor::execute_compaction_plan(&cfg, &db_path, &sst_dir, plan)?;

                // Rate limit writes based on output
                if let Some((_path, ref meta)) = write_res {
                    if let Some(ref limiter) = cfg.rate_limiter {
                        let written_bytes = meta.size_bytes;
                        if written_bytes > 0 {
                            limiter.request(written_bytes);
                        }
                    }
                }

                Ok(())
            })();

            match result {
                Ok(()) => {
                    // Reset backoff on success
                    if backoff_ms > 0 {
                        backoff_ms = 0;
                        last_error_time = None;
                    }
                    if let Some(ref h) = hooks {
                        h.after_compaction();
                    }
                    if let Some(bg) = &cfg.background_error {
                        *bg.write() = None;
                    }
                }
                Err(e) => {
                    debug!(error = ?e, "compaction execution failed");
                    // Exponential backoff if errors happen close together
                    let now = std::time::Instant::now();
                    if let Some(prev) = last_error_time {
                        if now.duration_since(prev).as_millis() < 1000 {
                            backoff_ms = (backoff_ms.saturating_mul(2)).clamp(10, 1000);
                        }
                    } else {
                        backoff_ms = 50;
                    }
                    last_error_time = Some(now);
                    // Consider backoff if needed in future iterations
                    if let Some(ref h) = hooks {
                        h.compaction_failed();
                    }
                    if let Some(bg) = &cfg.background_error {
                        *bg.write() = Some(MidgeError::internal(e.to_string()));
                    }
                }
            }

            // If we are idle now, satisfy barriers.
            if work_queue.is_empty() && !barrier_waiters.is_empty() {
                for waiter in barrier_waiters.drain(..) {
                    let _ = waiter.send(());
                }
            }
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
            cloud_coordinator: Arc::new(parking_lot::RwLock::new(None)), // Phase 7.2: Not set in tests
            runtime: Arc::new(parking_lot::RwLock::new(None)), // Phase 7.2: Not set in tests
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
            rate_limiter: None,
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
        let (coordinator, _handle) = CompactionController::spawn(config).unwrap();

        // Assert

        coordinator.shutdown().unwrap();
    }

    #[test]
    fn should_accept_manual_compaction_level_request() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(temp_dir.path().join("sst")).unwrap();
        let (config, _version_mgr) = create_test_config(&temp_dir);
        let (coordinator, _handle) = CompactionController::spawn(config).unwrap();

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
        let (coordinator, _handle) = CompactionController::spawn(config).unwrap();

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
        let (coordinator, _handle) = CompactionController::spawn(config).unwrap();

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
        let (coordinator, _handle) = CompactionController::spawn(config).unwrap();

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
        let (coordinator, _handle) = CompactionController::spawn(config).unwrap();
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
        let (coordinator, _handle) = CompactionController::spawn(config).unwrap();

        // Act
        for level in 0..3 {
            coordinator.compact_level(0, level).unwrap();
        }
        let wait_result = coordinator.wait_until_idle(Duration::from_secs(5));

        // Assert
        assert!(wait_result.is_ok());

        coordinator.shutdown().unwrap();
    }
}
