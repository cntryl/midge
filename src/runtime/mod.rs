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
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

/// Global request ID counter for routing responses to correct requesters.
static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

/// Allocate a new, globally unique request ID.
///
/// Callers should always use this when constructing `RuntimeMsg` values
/// that expect a response.
pub(crate) fn next_request_id() -> u64 {
    NEXT_REQUEST_ID.fetch_add(1, Ordering::SeqCst)
}

/// Simplified compaction plan for message passing.
#[derive(Debug, Clone)]
pub struct CompactionPlan {
    pub input_files: Vec<String>,
    pub source_level: u32,
    pub target_level: u32,
    pub cf_id: u32,
}

/// Simplified file metadata for message passing.
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

/// Messages that can be sent to the runtime.
///
/// Copilot: each variant that expects a response MUST carry a `request_id: u64`.
#[derive(Debug)]
pub enum RuntimeMsg {
    // === Flush Actor ===
    /// Request memtable flush for a column family.
    FlushMemtable { request_id: u64, cf_id: u32 },
    /// Memtable flush completed.
    FlushComplete {
        request_id: u64,
        cf_id: u32,
        sst_name: String,
        sequence: u64,
    },

    // === Compaction Actor ===
    /// Trigger compaction check.
    CheckCompaction { request_id: u64 },
    /// Execute a specific compaction plan.
    RunCompaction {
        request_id: u64,
        plan: CompactionPlan,
    },
    /// Compaction completed.
    CompactionComplete {
        request_id: u64,
        input_ssts: Vec<String>,
        output_ssts: Vec<String>,
    },

    // === WAL Actor ===
    /// Append record to WAL.
    WalAppend {
        request_id: u64,
        cf_id: u32,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
        sequence: u64,
        ttl_seconds: Option<u64>, // TTL in seconds, None means no expiration
        insert_only: bool,        // When true, fail if key already exists
    },
    /// Sync WAL to disk.
    WalSync { request_id: u64 },
    /// Rotate WAL segment.
    WalRotate { request_id: u64 },
    /// WAL sync completed.
    WalSyncComplete { request_id: u64, segment_id: u64 },

    // === Cloud Actor ===
    /// Upload SST to cloud.
    CloudUploadSst { request_id: u64, sst_name: String },
    /// Upload WAL segment to cloud.
    CloudUploadWal { request_id: u64, segment_id: u64 },
    /// Cloud upload completed.
    CloudUploadComplete { request_id: u64, resource: String },

    // === GC Actor ===
    /// Check for garbage collection opportunities.
    CheckGc { request_id: u64 },
    /// Delete obsolete SST files.
    DeleteObsoleteSsts {
        request_id: u64,
        sst_names: Vec<String>,
    },

    // === Manifest Actor ===
    /// Update manifest with new SST.
    ManifestAddSst {
        request_id: u64,
        file_meta: FileMeta,
    },
    /// Update manifest after compaction.
    ManifestCompactionComplete {
        request_id: u64,
        removed: Vec<String>,
        added: Vec<FileMeta>,
    },
    /// Persist manifest to disk.
    ManifestPersist { request_id: u64 },

    // === Column Family Lifecycle ===
    /// Create a new column family.
    ManifestCreateColumnFamily { request_id: u64, name: String },
    /// Drop a column family (soft delete).
    ManifestDropColumnFamily { request_id: u64, cf_id: u32 },

    // === Read Path ===
    /// Query a value from memtables and SST files.
    Read {
        request_id: u64,
        cf_id: u32,
        key: Vec<u8>,
        sequence: u64, // Read at this sequence number or earlier.
    },

    // === Control ===
    /// Shutdown the runtime (no request_id; fire-and-forget).
    Shutdown,
    /// No-op for testing.
    Noop { request_id: u64 },
}

impl RuntimeMsg {
    /// Extract the request_id for messages that expect a response.
    ///
    /// Returns `None` for messages that do not participate in request/response
    /// routing (e.g., `Shutdown`).
    pub fn request_id(&self) -> Option<u64> {
        use RuntimeMsg::*;
        match self {
            FlushMemtable { request_id, .. }
            | FlushComplete { request_id, .. }
            | CheckCompaction { request_id }
            | RunCompaction { request_id, .. }
            | CompactionComplete { request_id, .. }
            | WalAppend { request_id, .. }
            | WalSync { request_id }
            | WalRotate { request_id }
            | WalSyncComplete { request_id, .. }
            | CloudUploadSst { request_id, .. }
            | CloudUploadWal { request_id, .. }
            | CloudUploadComplete { request_id, .. }
            | CheckGc { request_id }
            | DeleteObsoleteSsts { request_id, .. }
            | ManifestAddSst { request_id, .. }
            | ManifestCompactionComplete { request_id, .. }
            | ManifestPersist { request_id }
            | ManifestCreateColumnFamily { request_id, .. }
            | ManifestDropColumnFamily { request_id, .. }
            | Read { request_id, .. }
            | Noop { request_id } => Some(*request_id),

            Shutdown => None,
        }
    }
}

/// Response from runtime operations.
///
/// Copilot: every response variant MUST carry the originating request_id.
#[derive(Debug)]
pub enum RuntimeResponse {
    Ok {
        request_id: u64,
    },
    Error {
        request_id: u64,
        message: String,
    },
    ReadValue {
        request_id: u64,
        value: Option<Vec<u8>>,
    },
    FlushComplete {
        request_id: u64,
        sst_name: String,
    },
    CompactionComplete {
        request_id: u64,
        output_ssts: Vec<String>,
    },
    ColumnFamilyCreated {
        request_id: u64,
        cf_id: u32,
    },
}

impl RuntimeResponse {
    pub fn request_id(&self) -> u64 {
        match self {
            RuntimeResponse::Ok { request_id }
            | RuntimeResponse::Error { request_id, .. }
            | RuntimeResponse::ReadValue { request_id, .. }
            | RuntimeResponse::FlushComplete { request_id, .. }
            | RuntimeResponse::CompactionComplete { request_id, .. }
            | RuntimeResponse::ColumnFamilyCreated { request_id, .. } => *request_id,
        }
    }
}

/// ResponseRouter - per-request routing using oneshot-style channels.
///
/// Copilot: this is the ONLY place where responses are matched to request_ids.
/// Do not invent global response_rx or other routing mechanisms.
#[derive(Debug)]
pub(crate) struct ResponseRouter {
    pending: Mutex<HashMap<u64, Sender<RuntimeResponse>>>,
}

impl ResponseRouter {
    pub fn new() -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
        }
    }

    /// Register a new pending response for a given request_id.
    ///
    /// Returns a receiver that will yield exactly one `RuntimeResponse`.
    pub fn register(&self, request_id: u64) -> Receiver<RuntimeResponse> {
        let (tx, rx) = channel::bounded(1);
        let mut guard = self
            .pending
            .lock()
            .expect("ResponseRouter::pending poisoned");
        guard.insert(request_id, tx);
        rx
    }

    /// Complete a request by delivering its response to the waiting receiver.
    ///
    /// If no pending entry exists, logs a warning and drops the response.
    pub fn complete(&self, response: RuntimeResponse) {
        let request_id = response.request_id();
        let tx_opt = {
            let mut guard = self
                .pending
                .lock()
                .expect("ResponseRouter::pending poisoned");
            guard.remove(&request_id)
        };

        if let Some(tx) = tx_opt {
            let _ = tx.send(response);
        } else {
            tracing::warn!(
                request_id,
                "response received with no matching pending request"
            );
        }
    }
}

/// Handle for submitting work to the runtime.
///
/// Copilot:
/// - Route responses by request_id using ResponseRouter.
/// - Use per-request channels (bounded(1)) created via ResponseRouter::register.
/// - Never use a single shared response_rx.
/// - RuntimeHandle MUST be thread-safe and support concurrent callers.
#[derive(Clone)]
pub struct RuntimeHandle {
    msg_tx: Sender<RuntimeMsg>,
    router: Arc<ResponseRouter>,
}

impl RuntimeHandle {
    /// Submit a message to the runtime (fire-and-forget).
    ///
    /// For messages that expect a response, prefer `send_and_wait`.
    pub fn send(&self, msg: RuntimeMsg) -> MidgeResult<()> {
        self.msg_tx
            .send(msg)
            .map_err(|_| MidgeError::Internal("Runtime channel closed".to_string()))
    }

    /// Submit a message and wait synchronously for its response.
    ///
    /// The `RuntimeMsg` MUST carry a `request_id`. Use `next_request_id()` when
    /// constructing such messages.
    pub fn send_and_wait(&self, msg: RuntimeMsg) -> MidgeResult<RuntimeResponse> {
        let request_id = msg.request_id().ok_or_else(|| {
            MidgeError::Internal(
                "send_and_wait called with message that has no request_id (e.g. Shutdown)"
                    .to_string(),
            )
        })?;

        // Register for the response before sending the request.
        let rx = self.router.register(request_id);

        self.msg_tx
            .send(msg)
            .map_err(|_| MidgeError::Internal("Runtime channel closed".to_string()))?;

        // Block waiting for the single response.
        rx.recv()
            .map_err(|_| MidgeError::Internal("Response channel closed".to_string()))
    }

    /// Submit a message and wait for a response that matches a predicate.
    ///
    /// Since each request_id yields exactly one response, this is mainly
    /// useful for callers that want to validate the response shape.
    pub fn send_and_wait_filtered<F>(
        &self,
        msg: RuntimeMsg,
        mut predicate: F,
    ) -> MidgeResult<RuntimeResponse>
    where
        F: FnMut(&RuntimeResponse) -> bool,
    {
        let resp = self.send_and_wait(msg)?;
        if predicate(&resp) {
            Ok(resp)
        } else {
            Err(MidgeError::Internal(
                "Response did not satisfy predicate".to_string(),
            ))
        }
    }

    /// Request runtime shutdown (fire-and-forget).
    pub fn shutdown(&self) -> MidgeResult<()> {
        self.send(RuntimeMsg::Shutdown)
    }
}

/// Main runtime for background operations.
///
/// Owns all mutable engine state and coordinates actors via message passing.
/// All background work flows through this runtime and its event loop thread.
pub struct Runtime {
    /// Message channel sender (for handle).
    msg_tx: Sender<RuntimeMsg>,
    /// Message channel receiver (for event loop).
    msg_rx: Receiver<RuntimeMsg>,
    /// Event loop thread handle.
    event_loop_handle: Option<JoinHandle<()>>,
    /// Whether tracing is enabled.
    trace_enabled: bool,
    /// Response router shared between handle and event loop.
    router: Arc<ResponseRouter>,
}

impl Runtime {
    /// Create a new runtime and a corresponding handle for submitting work.
    pub fn new() -> MidgeResult<(Self, RuntimeHandle)> {
        let (msg_tx, msg_rx) = channel::unbounded();
        let router = Arc::new(ResponseRouter::new());

        let trace_enabled = std::env::var("MIDGE_TRACE_RUNTIME")
            .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
            .unwrap_or(false);

        let handle = RuntimeHandle {
            msg_tx: msg_tx.clone(),
            router: router.clone(),
        };

        let runtime = Self {
            msg_tx,
            msg_rx,
            event_loop_handle: None,
            trace_enabled,
            router,
        };

        Ok((runtime, handle))
    }

    /// Start the runtime event loop in a background thread.
    ///
    /// The returned handle can be used from any thread to submit work.
    pub fn start(mut self, state: RuntimeState) -> MidgeResult<RuntimeHandle> {
        let msg_rx = self.msg_rx;
        let trace_enabled = self.trace_enabled;
        let router = self.router.clone();

        // Handle for callers to use.
        let handle = RuntimeHandle {
            msg_tx: self.msg_tx.clone(),
            router: router.clone(),
        };

        let event_loop_handle = thread::Builder::new()
            .name("midge-runtime".to_string())
            .spawn(move || match EventLoop::new(state, trace_enabled, router) {
                Ok(mut event_loop) => {
                    event_loop.run(msg_rx);
                }
                Err(e) => {
                    tracing::error!("Failed to create event loop: {}", e);
                }
            })
            .map_err(|e| MidgeError::Internal(format!("Failed to spawn runtime thread: {}", e)))?;

        self.event_loop_handle = Some(event_loop_handle);

        Ok(handle)
    }

    /// Shutdown the runtime and wait for completion.
    pub fn shutdown(mut self) {
        if let Some(handle) = self.event_loop_handle.take() {
            // Event loop will exit when channel is dropped.
            drop(self.msg_tx);
            drop(self.msg_rx);
            if handle.join().is_err() {
                tracing::warn!("Runtime thread panicked during shutdown");
            }
        }
    }
}

// Note: Runtime does not implement Default because it returns (Runtime, RuntimeHandle).
// Use Runtime::new() directly.
