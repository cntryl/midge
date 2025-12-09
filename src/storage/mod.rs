pub mod cloud;
pub mod filesystem;
pub mod hybrid;
pub mod paths;
pub mod providers;

pub use cloud::CloudStorage;
pub use filesystem::FileSystem;
pub use hybrid::HybridStorage;
pub use paths::Paths;
pub use providers::{AzureProvider, GcsProvider, OciProvider, S3Provider};

use crate::common::MidgeResult;

/// Storage events sent back to the runtime after async I/O.
///
/// These events are sent via StorageCallback channels when operations complete.
/// This unified event type works for both filesystem and cloud backends.
#[derive(Debug, Clone)]
pub enum StorageEvent {
    /// Read operation completed
    ReadComplete {
        path: String,
        result: StorageOutcome<Vec<u8>>,
    },
    /// Write operation completed
    WriteComplete {
        path: String,
        result: StorageOutcome<()>,
    },
    /// Delete operation completed
    DeleteComplete {
        path: String,
        result: StorageOutcome<()>,
    },
    /// List operation completed
    ListComplete {
        prefix: String,
        result: StorageOutcome<Vec<String>>,
    },
}

/// Serializable result type for storage operations.
///
/// Can be converted to/from MidgeResult for compatibility.
#[derive(Debug, Clone)]
pub enum StorageOutcome<T: Clone> {
    Ok(T),
    Err(String),
}

impl<T: Clone> StorageOutcome<T> {
    /// Convert from MidgeResult to StorageOutcome
    pub fn from_result(r: MidgeResult<T>) -> Self {
        match r {
            Ok(v) => StorageOutcome::Ok(v),
            Err(e) => StorageOutcome::Err(format!("{:?}", e)),
        }
    }

    /// Convert StorageOutcome to MidgeResult
    pub fn to_result(self) -> MidgeResult<T> {
        match self {
            StorageOutcome::Ok(v) => Ok(v),
            StorageOutcome::Err(e) => Err(crate::common::MidgeError::Internal(e)),
        }
    }

    /// Check if this is an Ok outcome
    pub fn is_ok(&self) -> bool {
        matches!(self, StorageOutcome::Ok(_))
    }

    /// Check if this is an Err outcome
    pub fn is_err(&self) -> bool {
        matches!(self, StorageOutcome::Err(_))
    }
}

/// Callback type: a sync channel to send StorageEvent back to runtime
pub type StorageCallback = std::sync::mpsc::Sender<StorageEvent>;

/// NEW async-compatible storage backend trait.
///
/// CRITICAL DESIGN:
/// - All operations return immediately (non-blocking)
/// - Real I/O happens asynchronously (in thread pools or tokio tasks)
/// - Results are reported back via StorageCallback
/// - Same trait for both filesystem and cloud backends
///
/// This allows:
/// - Synchronous engine with async I/O workers
/// - Deterministic runtime (events consumed in event loop)
/// - Unified hybrid storage (same interface for local + cloud)
/// - No mutable references (works with Arc)
/// - Ready for batching and pipelining
pub trait StorageBackend: Send + Sync + 'static {
    /// Submit a read operation. Returns immediately.
    fn submit_read(&self, path: String, callback: StorageCallback);

    /// Submit a write operation. Returns immediately.
    fn submit_write(&self, path: String, data: Vec<u8>, callback: StorageCallback);

    /// Submit a delete operation. Returns immediately.
    fn submit_delete(&self, path: String, callback: StorageCallback);

    /// Submit a prefix list operation. Returns immediately.
    fn submit_list(&self, prefix: String, callback: StorageCallback);
}
