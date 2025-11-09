//! Flush coordinator for background memtable flushing
//!
//! Manages the lifecycle of the background flush worker thread, including
//! spawning, job submission, and graceful shutdown.

use crate::core::flush::{spawn_flush_worker, FlushJob, FlushMsg, FlushWorkerConfig};
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
        let (s, r) = channel::bounded::<()>(1);
        self.tx
            .send(FlushMsg::Barrier { reply: s })
            .map_err(|_| MidgeError::internal("Flush worker channel closed"))?;

        match r.recv_timeout(timeout) {
            Ok(()) => Ok(()),
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
    use crate::common::codec::CompressionType;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_config(temp_dir: &TempDir) -> FlushWorkerConfig {
        FlushWorkerConfig {
            sst_factory: Arc::new(crate::sst::mem::MemSstFactory),
            sst_dir: temp_dir.path().join("sst"),
            wal_dir: temp_dir.path().join("wal"),
            db_path: temp_dir.path().to_path_buf(),
            compression: CompressionType::None,
            block_size: 4096,
            mem_mode: false,
            cloud_sst_manager: None,
            metrics: Arc::new(crate::core::metrics::Metrics::new()),
        }
    }

    fn create_test_flush_job(seq: u64, num_entries: usize) -> FlushJob {
        let entries: Vec<_> = (0..num_entries)
            .map(|i| crate::EntryMeta {
                key: format!("key{:03}", i).into_bytes(),
                value: Some(format!("value{}", i).into_bytes()),
                sequence: seq + i as u64,
                is_tombstone: false,
                expiration_millis: None,
            })
            .collect();

        FlushJob {
            cf_id: crate::column_family::DEFAULT_CF_ID,
            seq,
            entries,
            range_tombstones: vec![],
        }
    }

    #[test]
    fn should_spawn_coordinator_successfully() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(temp_dir.path().join("sst")).unwrap();
        let config = create_test_config(&temp_dir);

        // Act
        let coordinator = FlushCoordinator::spawn(config);

        // Assert
        assert!(coordinator.is_ok());
        let coord = coordinator.unwrap();
        assert!(coord.is_running());
    }

    #[test]
    fn should_process_flush_job_successfully() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(temp_dir.path().join("sst")).unwrap();
        let config = create_test_config(&temp_dir);
        let coordinator = FlushCoordinator::spawn(config).unwrap();

        let job = create_test_flush_job(100, 10);

        // Act
        let result = coordinator.request_flush(job);

        // Assert
        assert!(result.is_ok());

        // Give worker time to process
        std::thread::sleep(std::time::Duration::from_millis(100));

        // Verify SST file was created
        let sst_dir = temp_dir.path().join("sst");
        let sst_files: Vec<_> = std::fs::read_dir(&sst_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("sst"))
            .collect();

        assert_eq!(sst_files.len(), 1, "Should create one SST file");
    }

    #[test]
    fn should_process_multiple_flush_jobs() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(temp_dir.path().join("sst")).unwrap();
        let config = create_test_config(&temp_dir);
        let coordinator = FlushCoordinator::spawn(config).unwrap();

        // Act
        for i in 0..3 {
            let job = create_test_flush_job(i * 100, 5);
            coordinator.request_flush(job).unwrap();
        }

        // Assert
        std::thread::sleep(std::time::Duration::from_millis(200));

        let sst_dir = temp_dir.path().join("sst");
        let sst_count = std::fs::read_dir(&sst_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("sst"))
            .count();

        assert_eq!(sst_count, 3, "Should create three SST files");
    }

    #[test]
    fn should_shutdown_gracefully() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(temp_dir.path().join("sst")).unwrap();
        let config = create_test_config(&temp_dir);
        let coordinator = FlushCoordinator::spawn(config).unwrap();

        // Act
        let result = coordinator.shutdown();

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_process_jobs_before_shutdown() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(temp_dir.path().join("sst")).unwrap();
        let config = create_test_config(&temp_dir);
        let coordinator = FlushCoordinator::spawn(config).unwrap();

        let job = create_test_flush_job(200, 10);
        coordinator.request_flush(job).unwrap();

        // Act
        let result = coordinator.shutdown();

        // Assert
        assert!(result.is_ok());

        // Verify job was processed before shutdown
        let sst_dir = temp_dir.path().join("sst");
        let sst_count = std::fs::read_dir(&sst_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("sst"))
            .count();

        assert_eq!(sst_count, 1, "Should process job before shutdown");
    }

    #[test]
    fn should_cleanup_on_drop() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(temp_dir.path().join("sst")).unwrap();
        let config = create_test_config(&temp_dir);
        let coordinator = FlushCoordinator::spawn(config).unwrap();

        let job = create_test_flush_job(300, 5);
        coordinator.request_flush(job).unwrap();

        // Act - Drop coordinator without explicit shutdown
        drop(coordinator);

        // Assert - Give time for cleanup
        std::thread::sleep(std::time::Duration::from_millis(100));

        // Verify job was processed
        let sst_dir = temp_dir.path().join("sst");
        let sst_count = std::fs::read_dir(&sst_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("sst"))
            .count();

        assert_eq!(sst_count, 1, "Should process job even when dropped");
    }

    #[test]
    fn should_handle_empty_flush_job() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(temp_dir.path().join("sst")).unwrap();
        let config = create_test_config(&temp_dir);
        let coordinator = FlushCoordinator::spawn(config).unwrap();

        let empty_job = FlushJob {
            cf_id: crate::column_family::DEFAULT_CF_ID,
            seq: 500,
            entries: vec![],
            range_tombstones: vec![],
        };

        // Act
        let result = coordinator.request_flush(empty_job);

        // Assert
        assert!(result.is_ok());
        std::thread::sleep(std::time::Duration::from_millis(100));

        // Empty job should not create SST file
        let sst_dir = temp_dir.path().join("sst");
        let sst_count = std::fs::read_dir(&sst_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("sst"))
            .count();

        assert_eq!(sst_count, 0, "Empty job should not create SST");
    }
}
