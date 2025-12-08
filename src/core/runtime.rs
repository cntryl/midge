//! Engine runtime for managing background worker threads.
//!
//! Provides centralized lifecycle management for all background workers
//! (WAL uploader, compaction, manifest sync) with deterministic shutdown.

use crate::error::{MidgeError, MidgeResult};
use crossbeam::channel;
use std::env;
use std::thread::{self, JoinHandle};

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

type TaskFn = Box<dyn FnOnce() + Send + 'static>;

/// Task kind for instrumentation and tracing.
#[derive(Debug, Clone, Copy)]
pub enum RuntimeTaskKind {
    Flush,
    Compaction,
    Maintenance,
}

/// Work item submitted to the engine runtime executor.
pub struct RuntimeTask {
    pub kind: RuntimeTaskKind,
    pub description: String,
    action: TaskFn,
    completion: Option<channel::Sender<()>>,
}

impl RuntimeTask {
    /// Create a new runtime task.
    pub fn new(kind: RuntimeTaskKind, description: impl Into<String>, action: TaskFn) -> Self {
        Self {
            kind,
            description: description.into(),
            action,
            completion: None,
        }
    }

    fn with_completion(mut self, completion: channel::Sender<()>) -> Self {
        self.completion = Some(completion);
        self
    }

    fn execute(mut self) {
        (self.action)();
        if let Some(done) = self.completion.take() {
            let _ = done.send(());
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
    /// Task submission channel for runtime executor
    task_tx: Option<channel::Sender<RuntimeTask>>,
    /// Handle to the executor thread
    task_handle: Option<JoinHandle<()>>,
    /// Whether runtime tracing is enabled
    trace_runtime: bool,
    /// Shutdown signal broadcaster
    shutdown_tx: channel::Sender<()>,
}

impl EngineRuntime {
    /// Create a new runtime with the given shutdown channel.
    pub fn new(shutdown_tx: channel::Sender<()>, shutdown_rx: channel::Receiver<()>) -> Self {
        let trace_runtime = should_trace_runtime();
        let (task_tx, task_rx) = channel::unbounded::<RuntimeTask>();
        let task_handle =
            thread::spawn(move || run_runtime_loop(task_rx, shutdown_rx, trace_runtime));

        Self {
            flush_coordinator: None,
            wal_uploader: None,
            compaction: None,
            manifest_sync: None,
            hybrid_storage_workers: Vec::new(),
            task_tx: Some(task_tx),
            task_handle: Some(task_handle),
            trace_runtime,
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

    /// Submit a work item to the runtime executor queue.
    pub fn submit(&self, task: RuntimeTask) -> MidgeResult<()> {
        if self.trace_runtime {
            tracing::trace!(task = %task.description, kind = ?task.kind, "runtime submitting task");
        }
        let tx = self
            .task_tx
            .as_ref()
            .ok_or_else(|| MidgeError::internal("Engine runtime executor is already shut down"))?;
        tx.send(task)
            .map_err(|_| MidgeError::internal("Engine runtime task channel closed"))
    }

    /// Submit a task and block until it has been executed.
    pub fn submit_and_wait(&self, task: RuntimeTask) -> MidgeResult<()> {
        let (tx, rx) = channel::bounded::<()>(1);
        let task = task.with_completion(tx);
        self.submit(task)?;
        rx.recv()
            .map_err(|_| MidgeError::internal("Engine runtime task was cancelled"))
    }

    /// Shutdown all workers gracefully.
    ///
    /// Sends shutdown signal and waits for all workers to exit.
    pub fn shutdown(mut self) {
        // Broadcast shutdown signal
        let _ = self.shutdown_tx.send(());

        if let Some(handle) = self.task_handle.take() {
            if handle.join().is_err() {
                tracing::warn!("Engine runtime executor panicked during shutdown");
            }
        }
        self.task_tx = None;

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

        if let Some(handle) = self.task_handle.take() {
            if handle.join().is_err() {
                tracing::warn!("Engine runtime executor panicked during drop");
            }
        }
        self.task_tx = None;

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

fn run_runtime_loop(
    task_rx: channel::Receiver<RuntimeTask>,
    shutdown_rx: channel::Receiver<()>,
    trace_runtime: bool,
) {
    loop {
        channel::select! {
            recv(shutdown_rx) -> _ => break,
            recv(task_rx) -> msg => match msg {
                Ok(task) => {
                    if trace_runtime {
                        tracing::trace!(
                            task = %task.description,
                            kind = ?task.kind,
                            "runtime executing task",
                        );
                    }
                    task.execute();
                }
                Err(_) => break,
            },
        }
    }
}

fn should_trace_runtime() -> bool {
    env::var("MIDGE_TRACE_RUNTIME")
        .map(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false)
}
