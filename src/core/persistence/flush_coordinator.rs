//! Flush coordinator for background memtable flushing
//!
//! Manages the lifecycle of the background flush worker thread, including
//! spawning, job submission, and graceful shutdown.

use crate::core::persistence::flush::{spawn_flush_worker, FlushJob, FlushMsg, FlushWorkerConfig};
use crate::error::{MidgeError, MidgeResult};
use crossbeam::channel;
use std::thread::JoinHandle;

/// Coordinates background flushing of memtable contents to SST files.
///
/// Encapsulates the flush worker thread lifecycle and provides a clean API
/// for requesting flushes and shutting down gracefully.
pub struct FlushCoordinator {
    /// Channel for sending flush requests to the background worker
    tx: channel::Sender<FlushMsg>,
    /// Handle to the background flush worker thread
    handle: Option<JoinHandle<()>>,
}

impl FlushCoordinator {
    /// Spawn a new background flush worker thread.
    ///
    /// Creates a dedicated thread that processes flush jobs in the background,
    /// allowing writes to continue with minimal latency.
    pub fn spawn(config: FlushWorkerConfig) -> MidgeResult<Self> {
        let (tx, handle) = spawn_flush_worker(config)?;
        Ok(Self {
            tx,
            handle: Some(handle),
        })
    }

    /// Request a flush of memtable entries to an SST file.
    ///
    /// Sends a flush job to the background worker. This is non-blocking
    /// and returns immediately after queueing the job.
    pub fn request_flush(&self, job: FlushJob) -> MidgeResult<()> {
        self.tx
            .send(FlushMsg::Entries(job))
            .map_err(|_| MidgeError::internal("Flush worker channel closed"))
    }

    /// Wait until the flush worker has processed all prior flush jobs and is idle.
    ///
    /// This sends a Barrier message and waits for an acknowledgment. A timeout is
    /// required to avoid indefinite blocking if the worker is deadlocked.
    pub fn wait_until_idle(&self, timeout: std::time::Duration) -> MidgeResult<()> {
        let start = std::time::Instant::now();
        let (s, r) = channel::bounded::<()>(1);
        self.tx
            .send(FlushMsg::Barrier { reply: s })
            .map_err(|_| MidgeError::internal("Flush worker channel closed"))?;

        match r.recv_timeout(timeout) {
            Ok(()) => {
                tracing::trace!(wait_ms = %start.elapsed().as_millis(), "FlushCoordinator.wait_until_idle completed (ms)");
                Ok(())
            }
            Err(channel::RecvTimeoutError::Timeout) => Err(MidgeError::internal(
                "Timed out waiting for flush worker to become idle",
            )),
            Err(channel::RecvTimeoutError::Disconnected) => {
                Err(MidgeError::internal("Flush worker disconnected"))
            }
        }
    }

    /// Gracefully shutdown the flush worker and wait for completion.
    ///
    /// Sends a shutdown signal and joins the worker thread. Consumes self
    /// to ensure the coordinator cannot be used after shutdown.
    pub fn shutdown(mut self) -> MidgeResult<()> {
        // Send shutdown signal (ignore error if receiver already dropped)
        let _ = self.tx.send(FlushMsg::Shutdown);

        // Wait for worker thread to finish
        if let Some(handle) = self.handle.take() {
            handle.join().map_err(|_| {
                MidgeError::internal("Flush worker thread panicked during shutdown")
            })?;
        }

        Ok(())
    }

    /// Check if the flush worker is still running.
    ///
    /// Returns false if the worker thread has terminated or shutdown was called.
    #[cfg(test)]
    pub fn is_running(&self) -> bool {
        self.handle.is_some()
    }
}

impl Drop for FlushCoordinator {
    fn drop(&mut self) {
        // Best-effort shutdown signal
        let _ = self.tx.send(FlushMsg::Shutdown);

        // Wait for thread to finish if handle still exists
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::column_family::ColumnFamilyId;
    use crate::core::EntryMeta;
    use crate::metrics::Metrics;
    use crate::sst::mem::MemSstFactory;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_config() -> FlushWorkerConfig {
        let temp_dir = TempDir::new().unwrap();
        let sst_dir = temp_dir.path().join("sst");
        let wal_dir = temp_dir.path().join("wal");
        let db_path = temp_dir.path().to_path_buf();
        std::fs::create_dir_all(&sst_dir).unwrap();
        std::fs::create_dir_all(&wal_dir).unwrap();

        FlushWorkerConfig {
            sst_factory: Arc::new(MemSstFactory {}),
            sst_dir,
            wal_dir,
            db_path,
            compression: crate::common::codec::CompressionType::None,
            block_size: 4096,
            mem_mode: true,
            cloud_sst_manager: None,
            metrics: Arc::new(Metrics::new()),
            test_hooks: None,
            manifest_update_callback: None,
            background_error: None,
        }
    }

    #[test]
    fn should_spawn_coordinator_successfully() {
        // Arrange
        let config = create_test_config();

        // Act
        let result = FlushCoordinator::spawn(config);

        // Assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_running());
    }

    #[test]
    fn should_request_flush_without_blocking() {
        // Arrange
        let config = create_test_config();
        let coordinator = FlushCoordinator::spawn(config).unwrap();
        let job = FlushJob {
            cf_id: ColumnFamilyId::new(0),
            seq: 1,
            entries: vec![],
            range_tombstones: vec![],
        };

        // Act
        let result = coordinator.request_flush(job);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_wait_until_idle_successfully() {
        // Arrange
        let config = create_test_config();
        let coordinator = FlushCoordinator::spawn(config).unwrap();

        // Act
        let result = coordinator.wait_until_idle(std::time::Duration::from_secs(1));

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_timeout_when_waiting_too_short() {
        // Arrange
        let config = create_test_config();
        let coordinator = FlushCoordinator::spawn(config).unwrap();
        // Queue a job that won't complete immediately
        let mut entries = vec![];
        for i in 0..1000 {
            entries.push(EntryMeta {
                key: format!("key_{}", i).into_bytes(),
                value: Some(vec![0u8; 1024]),
                sequence: i,
                is_tombstone: false,
                expiration_millis: None,
                op_type: crate::core::skiplist::OpType::Put,
            });
        }
        let job = FlushJob {
            cf_id: ColumnFamilyId::new(0),
            seq: 1,
            entries,
            range_tombstones: vec![],
        };
        coordinator.request_flush(job).unwrap();

        // Act
        let result = coordinator.wait_until_idle(std::time::Duration::from_nanos(1));

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_shutdown_gracefully() {
        // Arrange
        let config = create_test_config();
        let coordinator = FlushCoordinator::spawn(config).unwrap();

        // Act
        let result = coordinator.shutdown();

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_process_multiple_flush_jobs() {
        // Arrange
        let config = create_test_config();
        let coordinator = FlushCoordinator::spawn(config).unwrap();

        // Act
        for i in 0..5 {
            let job = FlushJob {
                cf_id: ColumnFamilyId::new(0),
                seq: i,
                entries: vec![],
                range_tombstones: vec![],
            };
            coordinator.request_flush(job).unwrap();
        }
        let result = coordinator.wait_until_idle(std::time::Duration::from_secs(2));

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_not_panic_when_dropped_without_shutdown() {
        // Arrange
        let config = create_test_config();
        let coordinator = FlushCoordinator::spawn(config).unwrap();

        // Act
        drop(coordinator);

        // Assert
        // no panic means success
    }
}
