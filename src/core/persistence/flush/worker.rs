//! Background flush worker thread.
//!
//! Manages the lifecycle of the background flush worker that processes
//! FlushJob messages and writes memtable contents to SST files.

use crossbeam::channel;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crate::common::codec::CompressionType;
use crate::error::{MidgeError, MidgeResult};
use crate::metrics::Metrics;

use super::process::process_flush_job;

/// A batch of memtable entries to be flushed to an SST file.
///
/// Created during WAL rotation when the memtable is drained. Contains all the
/// key-value pairs and range tombstones that need to be persisted.
pub struct FlushJob {
    /// Column family ID that owns this flush job
    pub cf_id: crate::api::column_family::ColumnFamilyId,
    /// Sequence number of the rotated WAL segment
    pub seq: u64,
    /// Drained memtable entries: (key, value, sequence, is_tombstone)
    pub entries: Vec<crate::core::EntryMeta>,
    /// Range tombstones drained from the memtable
    pub range_tombstones: Vec<(Vec<u8>, Vec<u8>, u64)>,
}

/// Messages sent to the background flush worker thread.
pub enum FlushMsg {
    /// Request to flush a batch of entries
    Entries(FlushJob),
    /// Signal to gracefully shut down the worker
    Shutdown,
    /// Barrier: requester wants to be notified when all prior flush jobs are processed
    Barrier { reply: channel::Sender<()> },
}

/// Configuration for creating a flush worker thread.
pub struct FlushWorkerConfig {
    pub sst_factory: Arc<dyn crate::sst::SstFactory>,
    pub sst_dir: PathBuf,
    pub wal_dir: PathBuf,
    pub db_path: PathBuf,
    pub compression: CompressionType,
    pub block_size: usize,
    pub mem_mode: bool,
    /// Optional cloud SST manager for uploading SSTs to cloud storage
    pub cloud_sst_manager: Option<Arc<crate::sst::cloud::CloudSstManager>>,
    /// Metrics collector to record memtable flushes from background worker
    pub metrics: Arc<Metrics>,
    /// Optional test hooks for deterministic coordination
    pub test_hooks: Option<crate::common::test_hooks::TestHooks>,
    /// Callback to update the engine's manifest cache after flush completes
    /// This ensures reads can immediately see newly flushed SST files
    pub manifest_update_callback: Option<Arc<dyn Fn(crate::core::manifest::Manifest) + Send + Sync>>,
    /// Optional shared background error container. When worker encounters a
    /// background error, it should set this to Some(err).
    pub background_error: Option<Arc<parking_lot::RwLock<Option<crate::error::MidgeError>>>>,
}

/// Spawn a background thread that processes flush jobs.
///
/// The worker thread listens for `FlushMsg::Entries` messages and writes them
/// to SST files, updating the manifest and cleaning up WAL files.
///
/// # Returns
/// * `tx` - Sender channel for sending flush messages
/// * `handle` - Join handle for the background thread
pub fn spawn_flush_worker(
    config: FlushWorkerConfig,
) -> MidgeResult<(channel::Sender<FlushMsg>, JoinHandle<()>)> {
    let (tx, rx) = channel::unbounded::<FlushMsg>();

    let background_error = config.background_error.clone();
    let handle = thread::Builder::new()
        .name("midge-flush-worker".to_string())
        .spawn(move || {
            // Wrap the entire worker loop in a panic guard so that any panic
            // inside the background thread is captured and converted into a
            // test hook event instead of unwinding into the test runner.
            let panic_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                for msg in rx.iter() {
                    match msg {
                        FlushMsg::Entries(job) => {
                            // Process the flush job
                            let res = process_flush_job(&config, job);
                            if let Err(e) = res {
                                // Mark background error if we were able to capture a container
                                if let Some(bg) = &background_error {
                                    *bg.write() =
                                        Some(crate::error::MidgeError::internal(e.to_string()));
                                }
                            } else {
                                // If there was previously a background error, clear it upon
                                // successful flush so writes can resume.
                                if let Some(bg) = &background_error {
                                    *bg.write() = None;
                                }
                            }
                        }
                        FlushMsg::Shutdown => break,
                        FlushMsg::Barrier { reply } => {
                            // Since messages are processed in-order, receiving the Barrier
                            // means all prior Entries have been processed. Acknowledge.
                            let _ = reply.send(());
                        }
                    }
                }
            }));

            if let Err(panic_payload) = panic_result {
                // Convert to test hook event if provided
                let _ = panic_payload; // silence unused warning when test_hooks is None
                if let Some(ref hooks) = config.test_hooks {
                    hooks.record_worker_panic("flush");
                }
                // Record background error if available so callers observe a failure
                if let Some(bg) = &background_error {
                    *bg.write() = Some(crate::error::MidgeError::internal(
                        "Background flush worker panicked".to_string(),
                    ));
                }
                // Swallow the panic; do not rethrow so the test runner is not aborted.
            }
        })
        .map_err(|e| MidgeError::internal(format!("Failed to spawn flush worker thread: {}", e)))?;

    Ok((tx, handle))
}

/// Struct for managing background flush worker.
pub struct FlushWorker;
