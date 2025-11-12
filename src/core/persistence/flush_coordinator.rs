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
