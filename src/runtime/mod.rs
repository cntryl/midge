//! Runtime - Actor-based background task execution
//!
//! Deterministic actor framework for compaction, flushing, WAL, cloud ops, GC, and manifest.
//! All engine state mutations flow through actors via message passing.
//!
//! # Architecture
//!
//! - **EventLoop**: Receives messages and dispatches to actors
//! - **State**: Centralized mutable state owned by runtime
//! - **Actors**: Stateless handlers that process messages and return state updates
//! - **Scheduler**: Prioritizes and batches work
//! - **Dispatcher**: Routes messages to appropriate actors

pub mod actors;
pub mod dispatch;
pub mod event_loop;
pub mod scheduler;
pub mod state;
pub mod task;

pub use actors::{CloudActor, CompactionActor, FlushActor, GcActor, ManifestActor, WalActor};
pub use dispatch::Dispatcher;
pub use event_loop::EventLoop;
pub use scheduler::Scheduler;
pub use state::RuntimeState;
pub use task::{Task, TaskId, TaskKind, TaskPriority};

use crate::common::{MidgeError, MidgeResult};
use crossbeam::channel::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};

/// Messages that can be sent to the runtime
#[derive(Debug)]
pub enum RuntimeMsg {
    // === Flush Actor ===
    /// Request memtable flush for a column family
    FlushMemtable { cf_id: u32 },
    /// Memtable flush completed
    FlushComplete {
        cf_id: u32,
        sst_name: String,
        sequence: u64,
    },

    // === Compaction Actor ===
    /// Trigger compaction check
    CheckCompaction,
    /// Execute a specific compaction plan
    RunCompaction { plan: CompactionPlan },
    /// Compaction completed
    CompactionComplete {
        input_ssts: Vec<String>,
        output_ssts: Vec<String>,
    },

    // === WAL Actor ===
    /// Append record to WAL
    WalAppend {
        cf_id: u32,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
        sequence: u64,
    },
    /// Sync WAL to disk
    WalSync,
    /// Rotate WAL segment
    WalRotate,
    /// WAL sync completed
    WalSyncComplete { segment_id: u64 },

    // === Cloud Actor ===
    /// Upload SST to cloud
    CloudUploadSst { sst_name: String },
    /// Upload WAL segment to cloud
    CloudUploadWal { segment_id: u64 },
    /// Cloud upload completed
    CloudUploadComplete { resource: String },

    // === GC Actor ===
    /// Check for garbage collection opportunities
    CheckGc,
    /// Delete obsolete SST files
    DeleteObsoleteSsts { sst_names: Vec<String> },

    // === Manifest Actor ===
    /// Update manifest with new SST
    ManifestAddSst { file_meta: FileMeta },
    /// Update manifest after compaction
    ManifestCompactionComplete {
        removed: Vec<String>,
        added: Vec<FileMeta>,
    },
    /// Persist manifest to disk
    ManifestPersist,

    // === Column Family Lifecycle ===
    /// Create a new column family
    ManifestCreateColumnFamily { name: String },
    /// Drop a column family (soft delete)
    ManifestDropColumnFamily { cf_id: u32 },

    // === Read Path ===
    /// Query a value from memtables and SST files
    Read {
        cf_id: u32,
        key: Vec<u8>,
        sequence: u64, // Read at this sequence number or earlier
    },

    // === Control ===
    /// Shutdown the runtime
    Shutdown,
    /// No-op for testing
    Noop,
}

/// Simplified compaction plan for message passing
#[derive(Debug, Clone)]
pub struct CompactionPlan {
    pub input_files: Vec<String>,
    pub source_level: u32,
    pub target_level: u32,
    pub cf_id: u32,
}

/// Simplified file metadata for message passing
#[derive(Debug, Clone)]
pub struct FileMeta {
    pub name: String,
    pub level: u32,
    pub size_bytes: u64,
    pub cf_id: u32,
    pub smallest_key: Option<Vec<u8>>,
    pub largest_key: Option<Vec<u8>>,
    pub smallest_seq: Option<u64>,
    pub largest_seq: Option<u64>,
}

/// Response from runtime operations
#[derive(Debug)]
pub enum RuntimeResponse {
    Ok,
    Error(String),
    ReadValue(Option<Vec<u8>>),
    FlushComplete { sst_name: String },
    CompactionComplete { output_ssts: Vec<String> },
    ColumnFamilyCreated { cf_id: u32 },
}

/// Handle for submitting work to the runtime
#[derive(Clone)]
pub struct RuntimeHandle {
    msg_tx: Sender<RuntimeMsg>,
    response_rx: Receiver<RuntimeResponse>,
}

impl RuntimeHandle {
    /// Submit a message to the runtime (non-blocking)
    pub fn send(&self, msg: RuntimeMsg) -> MidgeResult<()> {
        self.msg_tx
            .send(msg)
            .map_err(|_| MidgeError::Internal("Runtime channel closed".to_string()))
    }

    /// Submit a message and wait for response
    pub fn send_and_wait(&self, msg: RuntimeMsg) -> MidgeResult<RuntimeResponse> {
        self.send(msg)?;
        self.response_rx
            .recv()
            .map_err(|_| MidgeError::Internal("Runtime response channel closed".to_string()))
    }

    /// Submit a message and wait until a response matches the predicate
    pub fn send_and_wait_filtered<F>(
        &self,
        msg: RuntimeMsg,
        mut predicate: F,
    ) -> MidgeResult<RuntimeResponse>
    where
        F: FnMut(&RuntimeResponse) -> bool,
    {
        self.send(msg)?;
        loop {
            match self.response_rx.recv() {
                Ok(resp) => {
                    if predicate(&resp) {
                        return Ok(resp);
                    }
                }
                Err(_) => {
                    return Err(MidgeError::Internal(
                        "Runtime response channel closed".to_string(),
                    ));
                }
            }
        }
    }

    /// Request shutdown
    pub fn shutdown(&self) -> MidgeResult<()> {
        self.send(RuntimeMsg::Shutdown)
    }
}

/// Main runtime for background operations
///
/// Owns all mutable engine state and coordinates actors via message passing.
/// No direct thread spawning outside the runtime - all background work flows through here.
pub struct Runtime {
    /// Message channel sender (for handle)
    msg_tx: Sender<RuntimeMsg>,
    /// Message channel receiver (for event loop)
    msg_rx: Receiver<RuntimeMsg>,
    /// Response channel sender (for event loop)
    response_tx: Sender<RuntimeResponse>,
    /// Response channel receiver (for handle)
    response_rx: Receiver<RuntimeResponse>,
    /// Event loop thread handle
    event_loop_handle: Option<JoinHandle<()>>,
    /// Whether tracing is enabled
    trace_enabled: bool,
}

impl Runtime {
    /// Create a new runtime and return a handle for submitting work
    pub fn new() -> MidgeResult<(Self, RuntimeHandle)> {
        let (msg_tx, msg_rx) = channel::unbounded();
        let (response_tx, response_rx) = channel::unbounded();

        let trace_enabled = std::env::var("MIDGE_TRACE_RUNTIME")
            .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
            .unwrap_or(false);

        let handle = RuntimeHandle {
            msg_tx: msg_tx.clone(),
            response_rx: response_rx.clone(),
        };

        let runtime = Self {
            msg_tx,
            msg_rx,
            response_tx,
            response_rx,
            event_loop_handle: None,
            trace_enabled,
        };

        Ok((runtime, handle))
    }

    /// Start the runtime event loop in a background thread
    pub fn start(mut self, state: RuntimeState) -> MidgeResult<RuntimeHandle> {
        let msg_rx = self.msg_rx;
        let response_tx = self.response_tx;
        let trace_enabled = self.trace_enabled;

        // Create handle with the channels that are connected to the event loop
        let handle = RuntimeHandle {
            msg_tx: self.msg_tx.clone(),
            response_rx: self.response_rx.clone(),
        };

        let event_loop_handle = thread::Builder::new()
            .name("midge-runtime".to_string())
            .spawn(move || match EventLoop::new(state, trace_enabled) {
                Ok(mut event_loop) => {
                    event_loop.run(msg_rx, response_tx);
                }
                Err(e) => {
                    tracing::error!("Failed to create event loop: {}", e);
                }
            })
            .map_err(|e| MidgeError::Internal(format!("Failed to spawn runtime thread: {}", e)))?;

        self.event_loop_handle = Some(event_loop_handle);

        Ok(handle)
    }

    /// Shutdown the runtime and wait for completion
    pub fn shutdown(mut self) {
        if let Some(handle) = self.event_loop_handle.take() {
            // Event loop will exit when channel is dropped
            drop(self.msg_tx);
            drop(self.msg_rx);
            if handle.join().is_err() {
                tracing::warn!("Runtime thread panicked during shutdown");
            }
        }
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new().expect("Failed to create default runtime").0
    }
}
