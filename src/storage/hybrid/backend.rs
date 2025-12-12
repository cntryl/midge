//! Hybrid storage backend - combines local and cloud storage
//!
//! CRITICAL ARCHITECTURE:
//!
//! HybridStorage has TWO SEPARATE ROLES:
//!
//! 1. OBJECT STORAGE (SSTs, metadata):
//!    - submit_read/write/delete/list for SST files
//!    - Local + cloud merging/fallback
//!    - Cloud writes only for sst/ prefix
//!
//! 2. WAL DURABILITY PIPELINE (CloudFirst mode):
//!    - enqueue_wal_segment() - queue WAL for cloud upload
//!    - process_uploads() - initiate cloud uploads
//!    - poll() - retrieve CloudAck/CloudFail events
//!    - NEVER uses submit_write() for WAL
//!
//! WAL Flow:
//!   WalActor → local write → enqueue_wal_segment() → process_uploads() →
//!   cloud backend → CloudAck event → WalActor applies to memtable
//!
//! SST Flow:
//!   Engine → submit_write() → local write + cloud write (if sst/) → done

use crate::storage::{StorageBackend, StorageCallback, StorageEvent, StorageOutcome};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use super::actor;
use super::policy;
use super::state;

/// Status of a cloud upload operation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UploadStatus {
    Pending,
    InProgress,
    Completed,
    Failed(String),
}

/// State of a pending WAL segment upload
#[derive(Debug, Clone)]
pub struct UploadState {
    pub segment_id: u64,
    pub local_path: PathBuf,
    pub retries: u32,
    pub status: UploadStatus,
    pub max_sequence: u64,
}

/// Hybrid storage combining local filesystem and cloud backends
///
/// Managed by a Storage Budget Actor to enforce disk constraints, watermarks,
/// and coordination between local caching and cloud durability.
///
/// CloudFirst Durability:
/// - Tracks pending WAL segment uploads
/// - Emits CloudAck when cloud confirms durability
/// - Handles retries and failure reporting
pub struct HybridStorage {
    /// Local storage backend (usually filesystem)
    local: Arc<dyn StorageBackend>,
    /// Cloud storage backend (S3, GCS, Azure, etc.)
    cloud: Arc<dyn StorageBackend>,
    /// Storage Budget Actor for disk management
    budget_actor: Arc<Mutex<actor::StorageBudgetActor>>,
    /// Pending WAL segment uploads (CloudFirst mode)
    upload_queue: Arc<Mutex<VecDeque<UploadState>>>,
    /// Completed events ready for polling
    event_queue: Arc<Mutex<VecDeque<StorageEvent>>>,
}

impl HybridStorage {
    /// Create a new hybrid storage with local and cloud backends and default policy
    pub fn new(local: Arc<dyn StorageBackend>, cloud: Arc<dyn StorageBackend>) -> Self {
        Self::with_policy(local, cloud, policy::StorageBudgetPolicy::default())
    }

    /// Create a new hybrid storage with a custom storage budget policy
    pub fn with_policy(
        local: Arc<dyn StorageBackend>,
        cloud: Arc<dyn StorageBackend>,
        policy: policy::StorageBudgetPolicy,
    ) -> Self {
        let budget_actor = actor::StorageBudgetActor::new(policy);
        Self {
            local,
            cloud,
            budget_actor: Arc::new(Mutex::new(budget_actor)),
            upload_queue: Arc::new(Mutex::new(VecDeque::new())),
            event_queue: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// Enqueue a WAL segment for cloud upload (CloudFirst mode)
    ///
    /// **WAL DURABILITY PIPELINE ONLY**
    ///
    /// This is the EXCLUSIVE entry point for WAL cloud durability.
    /// - WalActor writes segment locally first
    /// - WalActor calls this method to queue for cloud upload
    /// - process_uploads() handles actual upload to cloud
    /// - CloudAck event emitted when cloud confirms durability
    /// - WalActor applies writes to memtable ONLY after CloudAck
    ///
    /// NEVER use submit_write() for WAL segments!
    pub fn enqueue_wal_segment(&self, segment_id: u64, local_path: PathBuf, max_sequence: u64) {
        let upload_state = UploadState {
            segment_id,
            local_path: local_path.clone(),
            retries: 0,
            status: UploadStatus::Pending,
            max_sequence,
        };

        let mut queue = self
            .upload_queue
            .lock()
            .expect("upload_queue lock poisoned");
        queue.push_back(upload_state);

        tracing::debug!(
            segment_id,
            ?local_path,
            max_sequence,
            "WAL segment enqueued for cloud upload"
        );
    }

    /// Poll for completed storage events (CloudAck, CloudFail, Backpressure)
    ///
    /// Called by Runtime event loop to process asynchronous storage completions.
    /// Returns all pending events and clears the internal queue.
    pub fn poll(&self) -> Vec<StorageEvent> {
        let mut events = self.event_queue.lock().expect("event_queue lock poisoned");
        events.drain(..).collect()
    }

    /// Process pending uploads (should be called periodically by runtime)
    ///
    /// **WAL DURABILITY PIPELINE**
    ///
    /// Initiates cloud uploads for pending WAL segments.
    /// This is the ONLY place where WAL segments are uploaded to cloud.
    /// - Reads pending uploads from upload_queue
    /// - Initiates cloud upload via cloud backend (not submit_write)
    /// - Handles retries on failure (up to 3 attempts)
    /// - Emits CloudAck/CloudFail events to event_queue
    ///
    /// Non-blocking - actual uploads happen asynchronously in spawned threads.
    pub fn process_uploads(&self) {
        let mut queue = self
            .upload_queue
            .lock()
            .expect("upload_queue lock poisoned");

        // Process each pending upload
        let mut completed_indices = Vec::new();

        for (idx, upload) in queue.iter_mut().enumerate() {
            match upload.status {
                UploadStatus::Pending => {
                    // Start upload
                    upload.status = UploadStatus::InProgress;
                    self.initiate_cloud_upload(upload.clone());
                }
                UploadStatus::Completed => {
                    completed_indices.push(idx);
                }
                UploadStatus::Failed(_) => {
                    if upload.retries < 3 {
                        // Retry
                        upload.retries += 1;
                        upload.status = UploadStatus::Pending;
                        tracing::warn!(
                            segment_id = upload.segment_id,
                            retry = upload.retries,
                            "Retrying cloud upload"
                        );
                    } else {
                        // Give up after 3 retries
                        completed_indices.push(idx);
                    }
                }
                UploadStatus::InProgress => {
                    // Wait for completion
                }
            }
        }

        // Remove completed items
        for &idx in completed_indices.iter().rev() {
            queue.remove(idx);
        }
    }

    /// Initiate cloud upload for a WAL segment
    ///
    /// **WAL DURABILITY PIPELINE ONLY**
    ///
    /// This method directly calls cloud.submit_write() for WAL segments.
    /// It does NOT go through HybridStorage::submit_write().
    /// This ensures WAL durability is independent of object storage logic.
    fn initiate_cloud_upload(&self, upload: UploadState) {
        let cloud = Arc::clone(&self.cloud);
        let event_queue = Arc::clone(&self.event_queue);
        let upload_queue = Arc::clone(&self.upload_queue);

        // Read local file and upload to cloud
        // This happens asynchronously in the cloud backend
        std::thread::spawn(move || {
            match std::fs::read(&upload.local_path) {
                Ok(data) => {
                    let (tx, rx) = std::sync::mpsc::channel();
                    let cloud_key = format!("wal/{}.wal", upload.segment_id);

                    cloud.submit_write(cloud_key, data, tx);

                    match rx.recv() {
                        Ok(StorageEvent::WriteComplete { result, .. }) => {
                            if result.is_ok() {
                                // Upload successful - emit CloudAck
                                let mut events =
                                    event_queue.lock().expect("event_queue lock poisoned");
                                events.push_back(StorageEvent::CloudAck {
                                    segment_id: upload.segment_id,
                                    max_sequence: upload.max_sequence,
                                });

                                // Mark as completed in upload queue
                                let mut queue =
                                    upload_queue.lock().expect("upload_queue lock poisoned");
                                if let Some(item) =
                                    queue.iter_mut().find(|u| u.segment_id == upload.segment_id)
                                {
                                    item.status = UploadStatus::Completed;
                                }

                                tracing::info!(
                                    segment_id = upload.segment_id,
                                    max_sequence = upload.max_sequence,
                                    "Cloud upload successful"
                                );
                            } else {
                                // Upload failed - emit CloudFail
                                let error = match result {
                                    StorageOutcome::Err(e) => e,
                                    _ => "Unknown error".to_string(),
                                };

                                let mut events =
                                    event_queue.lock().expect("event_queue lock poisoned");
                                events.push_back(StorageEvent::CloudFail {
                                    segment_id: upload.segment_id,
                                    error: error.clone(),
                                });

                                // Mark as failed in upload queue
                                let mut queue =
                                    upload_queue.lock().expect("upload_queue lock poisoned");
                                if let Some(item) =
                                    queue.iter_mut().find(|u| u.segment_id == upload.segment_id)
                                {
                                    item.status = UploadStatus::Failed(error);
                                }
                            }
                        }
                        _ => {
                            // Channel error - mark as failed
                            let mut events = event_queue.lock().expect("event_queue lock poisoned");
                            events.push_back(StorageEvent::CloudFail {
                                segment_id: upload.segment_id,
                                error: "Channel error".to_string(),
                            });
                        }
                    }
                }
                Err(e) => {
                    // Failed to read local file
                    let mut events = event_queue.lock().expect("event_queue lock poisoned");
                    events.push_back(StorageEvent::CloudFail {
                        segment_id: upload.segment_id,
                        error: format!("Failed to read local file: {:?}", e),
                    });
                }
            }
        });
    }

    /// Try to reserve space for a flush; returns the reservation result
    pub fn reserve_for_flush(&self, est_size: u64) -> actor::ReservationResult {
        let mut actor = self
            .budget_actor
            .lock()
            .expect("budget_actor lock poisoned");
        actor
            .handle_event(actor::StorageBudgetEvent::ReserveForFlush { est_size })
            .unwrap_or(actor::ReservationResult::Ok)
    }

    /// Signal that a flush completed with actual size
    pub fn flush_completed(&self, actual_size: u64) {
        let mut actor = self
            .budget_actor
            .lock()
            .expect("budget_actor lock poisoned");
        let _ = actor.handle_event(actor::StorageBudgetEvent::FlushCompleted { actual_size });
    }

    /// Signal that a cloud upload completed
    pub fn cloud_upload_completed(&self, sst_id: u64, actual_size: u64) {
        let mut actor = self
            .budget_actor
            .lock()
            .expect("budget_actor lock poisoned");
        let _ = actor.handle_event(actor::StorageBudgetEvent::CloudUploadCompleted {
            sst_id,
            actual_size,
        });
    }

    /// Signal that compaction is starting
    pub fn compaction_planned(&self, input_sizes: Vec<u64>) {
        let mut actor = self
            .budget_actor
            .lock()
            .expect("budget_actor lock poisoned");
        let _ = actor.handle_event(actor::StorageBudgetEvent::CompactionPlanned { input_sizes });
    }

    /// Signal that compaction completed
    pub fn compaction_completed(&self, output_sizes: Vec<u64>) {
        let mut actor = self
            .budget_actor
            .lock()
            .expect("budget_actor lock poisoned");
        let _ = actor.handle_event(actor::StorageBudgetEvent::CompactionCompleted { output_sizes });
    }

    /// Get current disk state snapshot
    pub fn disk_state(&self) -> state::DiskState {
        let actor = self
            .budget_actor
            .lock()
            .expect("budget_actor lock poisoned");
        actor.disk_state()
    }

    /// Get mutable access to the budget actor for testing and monitoring
    pub fn budget_actor(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, actor::StorageBudgetActor>, String> {
        self.budget_actor
            .lock()
            .map_err(|e| format!("Failed to lock budget actor: {}", e))
    }

    /// Get count of pending uploads (for monitoring)
    pub fn pending_upload_count(&self) -> usize {
        self.upload_queue
            .lock()
            .expect("upload_queue lock poisoned")
            .len()
    }
}

impl StorageBackend for HybridStorage {
    fn submit_read(&self, path: String, callback: StorageCallback) {
        // OBJECT STORAGE ONLY - reads SSTs, metadata, etc.
        // Try local first, fall back to cloud

        let local_clone = Arc::clone(&self.local);
        let cloud_clone = Arc::clone(&self.cloud);
        let path_clone = path.clone();

        let (tx, rx) = std::sync::mpsc::channel();
        local_clone.submit_read(path_clone.clone(), tx);

        match rx.recv() {
            Ok(StorageEvent::ReadComplete {
                path: p,
                result: StorageOutcome::Ok(data),
            }) => {
                // Success from local, return immediately
                let _ = callback.send(StorageEvent::ReadComplete {
                    path: p,
                    result: StorageOutcome::Ok(data),
                });
            }
            Ok(StorageEvent::ReadComplete {
                path: p,
                result: StorageOutcome::Err(_),
            }) => {
                // Local miss, try cloud
                let (tx_cloud, rx_cloud) = std::sync::mpsc::channel();
                cloud_clone.submit_read(p, tx_cloud);
                if let Ok(event) = rx_cloud.recv() {
                    let _ = callback.send(event);
                }
            }
            _ => {
                let _ = callback.send(StorageEvent::ReadComplete {
                    path,
                    result: StorageOutcome::Err("Hybrid read failed".to_string()),
                });
            }
        }
    }

    fn submit_write(&self, path: String, data: Vec<u8>, callback: StorageCallback) {
        // OBJECT STORAGE ONLY - NOT for WAL durability
        // WAL durability uses enqueue_wal_segment() instead

        let local_clone = Arc::clone(&self.local);
        let cloud_clone = Arc::clone(&self.cloud);
        let path_clone = path.clone();
        let data_clone = data.clone();

        // Always write to local first
        let (tx, rx) = std::sync::mpsc::channel();
        local_clone.submit_write(path_clone, data_clone, tx);

        match rx.recv() {
            Ok(StorageEvent::WriteComplete { ref result, .. }) => {
                // Send result back to caller immediately (local write complete)
                let event = StorageEvent::WriteComplete {
                    path: path.clone(),
                    result: result.clone(),
                };
                let _ = callback.send(event);

                // Schedule cloud write ONLY for SST files (not WAL)
                // WAL cloud uploads happen via enqueue_wal_segment() + process_uploads()
                if path.starts_with("sst/") && result.is_ok() {
                    let (tx_cloud, _) = std::sync::mpsc::channel();
                    cloud_clone.submit_write(path, data, tx_cloud);
                }
            }
            _ => {
                let _ = callback.send(StorageEvent::WriteComplete {
                    path,
                    result: StorageOutcome::Err("Hybrid write failed".to_string()),
                });
            }
        }
    }

    fn submit_delete(&self, path: String, callback: StorageCallback) {
        // OBJECT STORAGE ONLY - deletes SSTs, metadata, etc.
        // Delete from both local and cloud

        let local_clone = Arc::clone(&self.local);
        let cloud_clone = Arc::clone(&self.cloud);
        let path_clone = path.clone();

        let (tx_local, rx_local) = std::sync::mpsc::channel();
        local_clone.submit_delete(path_clone.clone(), tx_local);

        let (tx_cloud, rx_cloud) = std::sync::mpsc::channel();
        cloud_clone.submit_delete(path_clone, tx_cloud);

        // Wait for both and report result
        let local_result = rx_local.recv().ok();
        let cloud_result = rx_cloud.recv().ok();

        let combined_result = match (local_result, cloud_result) {
            (
                Some(StorageEvent::DeleteComplete {
                    result: StorageOutcome::Ok(()),
                    ..
                }),
                Some(StorageEvent::DeleteComplete {
                    result: StorageOutcome::Ok(()),
                    ..
                }),
            ) => StorageOutcome::Ok(()),
            _ => StorageOutcome::Err("Hybrid delete failed".to_string()),
        };

        let _ = callback.send(StorageEvent::DeleteComplete {
            path,
            result: combined_result,
        });
    }

    fn submit_list(&self, prefix: String, callback: StorageCallback) {
        // OBJECT STORAGE ONLY - lists SSTs, metadata, etc.
        // Merge results from both local and cloud

        let local_clone = Arc::clone(&self.local);
        let cloud_clone = Arc::clone(&self.cloud);
        let prefix_clone = prefix.clone();

        let (tx_local, rx_local) = std::sync::mpsc::channel();
        local_clone.submit_list(prefix_clone.clone(), tx_local);

        let (tx_cloud, rx_cloud) = std::sync::mpsc::channel();
        cloud_clone.submit_list(prefix_clone, tx_cloud);

        let mut results = Vec::new();

        if let Ok(StorageEvent::ListComplete {
            result: StorageOutcome::Ok(local_items),
            ..
        }) = rx_local.recv()
        {
            results.extend(local_items);
        }

        if let Ok(StorageEvent::ListComplete {
            result: StorageOutcome::Ok(cloud_items),
            ..
        }) = rx_cloud.recv()
        {
            for item in cloud_items {
                if !results.contains(&item) {
                    results.push(item);
                }
            }
        }

        results.sort();
        results.dedup();

        let _ = callback.send(StorageEvent::ListComplete {
            prefix,
            result: StorageOutcome::Ok(results),
        });
    }
}
