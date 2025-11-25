use crate::error::{MidgeError, MidgeResult};
use crate::wal::WalRecord;
use parking_lot::{Condvar, Mutex};
use std::sync::Arc;

// Re-export the blob-level backend under a WAL-focused name so callers can import
// `crate::wal::cloud::CloudStorageBackend` while providers implement the blob trait.
pub use crate::cloud::StorageBackend as CloudStorageBackend;

/// State of a durability promise.
#[derive(Clone, Debug)]
enum PromiseState {
    Pending,
    Completed {
        success: bool,
        error: Option<String>,
    },
}

/// Durability promise used by WAL upload APIs.
///
/// Allows callers to wait for cloud uploads to complete, enabling
/// durability guarantees without blocking the write path.
///
/// Uses parking_lot Condvar for synchronization (no tokio dependency).
#[derive(Clone)]
pub struct DurabilityPromise {
    state: Arc<Mutex<PromiseState>>,
    condvar: Arc<Condvar>,
}

impl DurabilityPromise {
    /// Create a new promise.
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(PromiseState::Pending)),
            condvar: Arc::new(Condvar::new()),
        }
    }

    /// Complete the promise with a result.
    pub fn complete(&self, result: Result<(), MidgeError>) {
        let mut state = self.state.lock();
        *state = match result {
            Ok(()) => PromiseState::Completed {
                success: true,
                error: None,
            },
            Err(e) => PromiseState::Completed {
                success: false,
                error: Some(e.to_string()),
            },
        };
        drop(state);
        self.condvar.notify_all();
    }

    /// Wait for the upload to complete. Returns Ok(()) if upload succeeded,
    /// or the error if it failed.
    pub fn wait(&self) -> Result<(), MidgeError> {
        let mut state = self.state.lock();
        loop {
            match &*state {
                PromiseState::Pending => {
                    self.condvar.wait(&mut state);
                }
                PromiseState::Completed { success, error } => {
                    return if *success {
                        Ok(())
                    } else {
                        Err(MidgeError::internal(
                            error.as_ref().unwrap_or(&"Unknown error".to_string()),
                        ))
                    };
                }
            }
        }
    }

    /// Check if the upload is complete (non-blocking).
    pub fn is_complete(&self) -> bool {
        matches!(*self.state.lock(), PromiseState::Completed { .. })
    }
}

impl Default for DurabilityPromise {
    fn default() -> Self {
        Self::new()
    }
}

/// A WAL segment packaged for cloud upload.
///
/// Segments batch multiple WAL records together to reduce the number of
/// cloud uploads (avoiding "death by tiny files"). The segment size is
/// configurable but typically 16-64 MB per the configuration spec.
#[derive(Clone, Debug)]
pub struct WalSegment {
    /// Monotonically increasing sequence number
    pub sequence: u64,
    /// WAL records in this segment
    pub records: Vec<WalRecord>,
    /// Approximate size in bytes
    size_bytes: usize,
}

impl WalSegment {
    /// Create a new segment with the given sequence number.
    pub fn new(sequence: u64, records: Vec<WalRecord>) -> MidgeResult<Self> {
        // Approximate size calculation
        let size_bytes = records
            .iter()
            .map(|r| {
                r.key.len()
                    + r.value.as_ref().map(|v| v.len()).unwrap_or(0)
                    + std::mem::size_of::<WalRecord>()
            })
            .sum();

        Ok(Self {
            sequence,
            records,
            size_bytes,
        })
    }

    /// Create an empty segment (used for initialization).
    pub fn empty(sequence: u64) -> Self {
        Self {
            sequence,
            records: Vec::new(),
            size_bytes: 0,
        }
    }

    /// Get the cloud storage key for this segment.
    pub fn segment_id(&self) -> String {
        format!("wal_segment_{:06}", self.sequence)
    }

    /// Serialize the segment to bytes for upload.
    pub fn serialize(&self) -> MidgeResult<Vec<u8>> {
        bincode::serialize(&self.records)
            .map_err(|e| MidgeError::internal(format!("Serialize error: {}", e)))
    }

    /// Approximate size of this segment in bytes.
    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    /// Number of records in this segment.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Check if segment is empty.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Add a record to this segment and update size estimate.
    pub fn add_record(&mut self, record: WalRecord) {
        self.size_bytes += record.key.len()
            + record.value.as_ref().map(|v| v.len()).unwrap_or(0)
            + std::mem::size_of::<WalRecord>();
        self.records.push(record);
    }
}

/// Manages batching and uploading WAL segments to cloud.
///
/// The batch manager accumulates WAL records into segments, uploads them
/// asynchronously to cloud storage, and provides durability promises that
/// can be awaited for sync() semantics.
///
/// # Design Principles
///
/// 1. **Buffering**: Records are batched into segments to reduce upload count
/// 2. **Durability**: sync() waits for pending uploads to complete
/// 3. **Non-blocking writes**: Uploads happen asynchronously via tokio tasks
/// 4. **Configurability**: Segment size is tunable (16-64 MB recommended)
pub struct WalBatchManager {
    backend: Arc<dyn CloudStorageBackend>,
    max_batch_size: usize,
    current_segment: Arc<Mutex<WalSegment>>,
    sequence_counter: Arc<Mutex<u64>>,
    pending_uploads: Arc<Mutex<Vec<DurabilityPromise>>>,
    shutdown_signal: Arc<std::sync::atomic::AtomicBool>,
}

impl WalBatchManager {
    /// Create a new batch manager.
    ///
    /// # Arguments
    ///
    /// * `backend` - Cloud storage backend
    /// * `max_batch_size` - Maximum segment size in bytes (16-64 MB recommended)
    /// * `manifest` - Optional manifest for tracking (currently unused)
    /// * `db_path` - Optional database path (currently unused)
    pub fn new(
        backend: Arc<dyn CloudStorageBackend>,
        max_batch_size: usize,
        _manifest: Option<Arc<parking_lot::Mutex<crate::core::manifest::Manifest>>>,
        _db_path: Option<std::path::PathBuf>,
    ) -> Self {
        Self {
            backend,
            max_batch_size,
            current_segment: Arc::new(Mutex::new(WalSegment::empty(0))),
            sequence_counter: Arc::new(Mutex::new(0)),
            pending_uploads: Arc::new(Mutex::new(Vec::new())),
            shutdown_signal: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Add a record to the current segment. If the segment exceeds max_batch_size,
    /// it will be flushed automatically.
    pub fn add_record(&self, record: WalRecord) -> MidgeResult<DurabilityPromise> {
        let mut segment = self.current_segment.lock();
        segment.add_record(record);

        // Check if we need to rotate
        if segment.size_bytes() >= self.max_batch_size {
            drop(segment); // Release lock before flushing
            self.flush_current_segment()
        } else {
            // Return a placeholder promise that's already complete
            let promise = DurabilityPromise::new();
            promise.complete(Ok(())); // Immediately complete
            Ok(promise)
        }
    }

    /// Flush the current segment to cloud storage and return a durability promise.
    pub fn flush_current_segment(&self) -> MidgeResult<DurabilityPromise> {
        let mut current = self.current_segment.lock();
        if current.is_empty() {
            drop(current);
            let promise = DurabilityPromise::new();
            promise.complete(Ok(()));
            return Ok(promise);
        }

        // Swap out the current segment with a new empty one
        let mut seq_counter = self.sequence_counter.lock();
        *seq_counter += 1;
        let next_sequence = *seq_counter;
        drop(seq_counter);

        let mut segment_to_upload =
            std::mem::replace(&mut *current, WalSegment::empty(next_sequence));
        // Assign the sequence number we just reserved to the segment being uploaded.
        // Previously the uploaded segment kept the old (often zero) sequence which
        // caused cloud replay to ignore the segment (ids <= manifest.last_persisted_sequence).
        segment_to_upload.sequence = next_sequence;
        drop(current);

        // Upload asynchronously
        self.upload_segment_async(segment_to_upload)
    }

    /// Upload a segment to cloud storage asynchronously.
    fn upload_segment_async(&self, segment: WalSegment) -> MidgeResult<DurabilityPromise> {
        let promise = DurabilityPromise::new();
        let backend = self.backend.clone();
        let shutdown_signal = self.shutdown_signal.clone();

        // Spawn background thread for upload. Use the guarded spawn helper
        // so panics are captured and the promise can be completed on panic.
        let promise_for_worker = promise.clone();
        let promise_for_on_panic = promise.clone();
        let _handle = crate::common::worker::spawn_guarded(
            "wal-cloud-upload",
            None,
            move || {
                let key = segment.segment_id();
                let data_res = segment.serialize();
                let data = match data_res {
                    Ok(d) => d,
                    Err(e) => {
                        // Complete the promise with the serialization error and return.
                        promise_for_worker.complete(Err(e));
                        return;
                    }
                };

                // Throttle cloud upload bandwidth if a global limiter is configured.
                let limiter = crate::common::rate_limiter::global_rate_limiter();
                limiter.request(data.len() as u64);

                // Bounded retry with shutdown checking
                const MAX_RETRIES: usize = 3;
                let mut last_error = None;
                
                for attempt in 0..MAX_RETRIES {
                    // Check shutdown signal before each attempt
                    if shutdown_signal.load(std::sync::atomic::Ordering::Relaxed) {
                        promise_for_worker.complete(Err(MidgeError::internal(
                            "WAL upload cancelled due to shutdown".to_string(),
                        )));
                        return;
                    }

                    match backend.put_blob(&key, bytes::Bytes::from(data.clone())) {
                        Ok(_) => {
                            // Success! Complete promise and return.
                            promise_for_worker.complete(Ok(()));
                            return;
                        }
                        Err(e) => {
                            last_error = Some(e);
                            if attempt < MAX_RETRIES - 1 {
                                // Brief backoff before retry (exponential: 10ms, 20ms, 40ms)
                                let backoff_ms = 10 * (1 << attempt);
                                std::thread::sleep(std::time::Duration::from_millis(backoff_ms));
                            }
                        }
                    }
                }

                // All retries exhausted - complete promise with error
                promise_for_worker.complete(Err(last_error.unwrap_or_else(|| {
                    MidgeError::internal("WAL upload failed after retries".to_string())
                })));
            },
            Some(move |_panic_payload| {
                // Convert a panic into an internal error for the promise so
                // callers observe a failure instead of a test abort.
                promise_for_on_panic.complete(Err(MidgeError::internal(
                    "WAL cloud upload worker panicked".to_string(),
                )));
            }),
        );

        // Track this promise for sync() calls
        self.pending_uploads.lock().push(promise.clone());

        Ok(promise)
    }

    /// Wait for all pending uploads to complete (sync semantics).
    pub fn sync(&self) -> MidgeResult<()> {
        // First flush any buffered data
        let promise = self.flush_current_segment()?;

        // Collect all pending promises
        let promises = {
            let mut pending = self.pending_uploads.lock();
            std::mem::take(&mut *pending)
        };

        // Wait for all uploads to complete
        for p in promises {
            p.wait()?;
        }

        // Wait for the flush we just triggered
        promise.wait()?;

        Ok(())
    }

    /// Flush without waiting for completion (non-blocking).
    pub fn flush_async(&self) -> MidgeResult<DurabilityPromise> {
        self.flush_current_segment()
    }

    /// Get the current sequence number.
    pub fn current_sequence(&self) -> u64 {
        *self.sequence_counter.lock()
    }

    /// Signal shutdown to all background workers.
    ///
    /// This sets the shutdown flag, causing upload workers to exit their retry loops.
    /// Does NOT wait for workers to complete - caller should use sync() or wait for
    /// pending promises if coordinated shutdown is required.
    pub fn shutdown(&self) {
        self.shutdown_signal.store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cloud::mock::MockCloudBackend;

    fn make_record(len: usize) -> WalRecord {
        WalRecord {
            cf_id: 0,
            op: crate::wal::WalOpKind::Put,
            key: bytes::Bytes::from("k"),
            value: Some(bytes::Bytes::from(vec![0u8; len])),
            seq: 1,
            expiration: None,
            range_end: None,
            txn_id: None,
            compression: None,
        }
    }

    #[test]
    fn should_complete_promise_immediately_for_small_record() {
        // Arrange: batch size larger than single record so no flush/worker
        let backend = Arc::new(MockCloudBackend::new());
        let manager = WalBatchManager::new(backend, 1024 * 1024, None, None);

        // Act
        let promise = manager.add_record(make_record(16)).expect("add_record");

        // Assert: promise is already complete and wait() returns Ok
        assert!(promise.is_complete());
        assert!(promise.wait().is_ok());
    }

    #[test]
    fn should_eventually_fail_promise_when_upload_retries_exhausted() {
        // Arrange: small batch size to force immediate flush and background upload
        let backend = Arc::new(MockCloudBackend::new());
        backend.reset_counters();
        // Fail all uploads by setting threshold to 0 (first attempt fails)
        backend.set_fail_upload_after(0);

        let manager = WalBatchManager::new(backend.clone(), 1, None, None);

        // Act
        let promise = manager.add_record(make_record(16)).expect("add_record");

        // Assert: promise.wait() should surface an error once retries are exhausted
        let res = promise.wait();
        assert!(res.is_err(), "expected upload failure under fail-after=0");
    }

    #[test]
    fn should_allow_sync_to_wait_for_pending_uploads() {
        // Arrange: batch size 1 to trigger upload on each record
        let backend = Arc::new(MockCloudBackend::new());
        let manager = WalBatchManager::new(backend.clone(), 1, None, None);

        // Two records should trigger at least one upload worker
        manager.add_record(make_record(8)).expect("add_record 1");
        manager.add_record(make_record(8)).expect("add_record 2");

        // Act: sync should block until all uploads complete
        let res = manager.sync();

        // Assert
        assert!(res.is_ok());
    }

    #[test]
    fn should_not_start_new_uploads_after_shutdown_is_set() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let manager = WalBatchManager::new(backend.clone(), 1, None, None);

        manager.shutdown();

        // Act: flushing an empty segment after shutdown should be a no-op
        let promise = manager
            .flush_current_segment()
            .expect("flush_current_segment after shutdown");

        // Assert: promise completes successfully
        assert!(promise.wait().is_ok());
    }
}
