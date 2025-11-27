//! Engine runtime for managing background worker threads.
//!
//! Provides centralized lifecycle management for all background workers
//! (WAL uploader, compaction, manifest sync) with deterministic shutdown.

use crossbeam::channel;
use std::thread::JoinHandle;

/// Handle to a background worker thread with shutdown capability.
pub struct WorkerHandle {
    /// Optional join handle for the worker thread
    join: Option<JoinHandle<()>>,
    /// Optional name for debugging
    name: &'static str,
}

impl WorkerHandle {
    /// Create a new worker handle.
    pub fn new(join: JoinHandle<()>, name: &'static str) -> Self {
        Self {
            join: Some(join),
            name,
        }
    }

    /// Wait for the worker to exit, consuming the handle.
    pub fn join(mut self) {
        if let Some(handle) = self.join.take() {
            if handle.join().is_err() {
                tracing::warn!("Worker '{}' panicked during shutdown", self.name);
            }
        }
    }
}

impl Drop for WorkerHandle {
    fn drop(&mut self) {
        if let Some(handle) = self.join.take() {
            // Best-effort join on drop - don't wait forever
            if handle.join().is_err() {
                tracing::warn!("Worker '{}' panicked during drop", self.name);
            }
        }
    }
}

/// Central runtime managing all engine background workers.
///
/// Owns worker thread handles and provides deterministic shutdown.
pub struct EngineRuntime {
    /// Flush coordinator worker
    flush_coordinator: Option<WorkerHandle>,
    /// WAL uploader worker (optional - only present in cloud-backed mode)
    wal_uploader: Option<WorkerHandle>,
    /// Compaction worker (optional - may be disabled)
    compaction: Option<WorkerHandle>,
    /// Manifest sync worker (optional - TBD if needed)
    manifest_sync: Option<WorkerHandle>,
    /// Hybrid storage workers (upload + eviction)
    hybrid_storage_workers: Vec<WorkerHandle>,
    /// Shutdown signal broadcaster
    shutdown_tx: channel::Sender<()>,
}

impl EngineRuntime {
    /// Create a new runtime with the given shutdown channel.
    pub fn new(shutdown_tx: channel::Sender<()>) -> Self {
        Self {
            flush_coordinator: None,
            wal_uploader: None,
            compaction: None,
            manifest_sync: None,
            hybrid_storage_workers: Vec::new(),
            shutdown_tx,
        }
    }

    /// Register the WAL uploader worker.
    pub fn set_wal_uploader(&mut self, handle: WorkerHandle) {
        self.wal_uploader = Some(handle);
    }

    /// Register the flush coordinator worker.
    pub fn set_flush_coordinator(&mut self, handle: WorkerHandle) {
        self.flush_coordinator = Some(handle);
    }

    /// Register the compaction worker.
    pub fn set_compaction(&mut self, handle: WorkerHandle) {
        self.compaction = Some(handle);
    }

    /// Register a hybrid storage worker.
    pub fn add_hybrid_storage_worker(&mut self, handle: WorkerHandle) {
        self.hybrid_storage_workers.push(handle);
    }

    /// Shutdown all workers gracefully.
    ///
    /// Sends shutdown signal and waits for all workers to exit.
    pub fn shutdown(mut self) {
        // Broadcast shutdown signal
        let _ = self.shutdown_tx.send(());

        // Wait for all workers to exit in reverse dependency order
        if let Some(flush) = self.flush_coordinator.take() {
            flush.join();
        }
        if let Some(wal) = self.wal_uploader.take() {
            wal.join();
        }
        if let Some(compaction) = self.compaction.take() {
            compaction.join();
        }
        if let Some(manifest) = self.manifest_sync.take() {
            manifest.join();
        }
        for worker in self.hybrid_storage_workers.drain(..) {
            worker.join();
        }
    }
}

impl Drop for EngineRuntime {
    fn drop(&mut self) {
        // Broadcast shutdown signal (best-effort)
        let _ = self.shutdown_tx.send(());

        // Wait for all workers to exit
        if let Some(flush) = self.flush_coordinator.take() {
            flush.join();
        }
        if let Some(wal) = self.wal_uploader.take() {
            wal.join();
        }
        if let Some(compaction) = self.compaction.take() {
            compaction.join();
        }
        if let Some(manifest) = self.manifest_sync.take() {
            manifest.join();
        }
        for worker in self.hybrid_storage_workers.drain(..) {
            worker.join();
        }
    }
}
