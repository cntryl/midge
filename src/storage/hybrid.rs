//! Hybrid storage backend - combines local and cloud storage
//!
//! Architecture:
//! - Reads try local first, fall back to cloud
//! - Writes go to local, with background cloud upload scheduled
//! - Deletes remove from both
//! - Lists merge results from both
//! - Storage Budget Actor manages disk constraints, watermarks, and backpressure

pub mod actor;
pub mod policy;
pub mod state;

use crate::storage::{StorageBackend, StorageCallback, StorageEvent, StorageOutcome};
use std::sync::Arc;

/// Hybrid storage combining local filesystem and cloud backends
///
/// Managed by a Storage Budget Actor to enforce disk constraints, watermarks,
/// and coordination between local caching and cloud durability.
pub struct HybridStorage {
    /// Local storage backend (usually filesystem)
    local: Arc<dyn StorageBackend>,
    /// Cloud storage backend (S3, GCS, Azure, etc.)
    cloud: Arc<dyn StorageBackend>,
    /// Storage Budget Actor for disk management
    budget_actor: Arc<std::sync::Mutex<actor::StorageBudgetActor>>,
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
            budget_actor: Arc::new(std::sync::Mutex::new(budget_actor)),
        }
    }

    /// Try to reserve space for a flush; returns the reservation result
    pub fn reserve_for_flush(&self, est_size: u64) -> actor::ReservationResult {
        let mut actor = self.budget_actor.lock().expect("budget_actor lock poisoned");
        actor
            .handle_event(actor::StorageBudgetEvent::ReserveForFlush { est_size })
            .unwrap_or(actor::ReservationResult::Ok)
    }

    /// Signal that a flush completed with actual size
    pub fn flush_completed(&self, actual_size: u64) {
        let mut actor = self.budget_actor.lock().expect("budget_actor lock poisoned");
        let _ = actor.handle_event(actor::StorageBudgetEvent::FlushCompleted { actual_size });
    }

    /// Signal that a cloud upload completed
    pub fn cloud_upload_completed(&self, sst_id: u64, actual_size: u64) {
        let mut actor = self.budget_actor.lock().expect("budget_actor lock poisoned");
        let _ = actor.handle_event(actor::StorageBudgetEvent::CloudUploadCompleted {
            sst_id,
            actual_size,
        });
    }

    /// Signal that compaction is starting
    pub fn compaction_planned(&self, input_sizes: Vec<u64>) {
        let mut actor = self.budget_actor.lock().expect("budget_actor lock poisoned");
        let _ = actor.handle_event(actor::StorageBudgetEvent::CompactionPlanned { input_sizes });
    }

    /// Signal that compaction completed
    pub fn compaction_completed(&self, output_sizes: Vec<u64>) {
        let mut actor = self.budget_actor.lock().expect("budget_actor lock poisoned");
        let _ = actor.handle_event(actor::StorageBudgetEvent::CompactionCompleted { output_sizes });
    }

    /// Get current disk state snapshot
    pub fn disk_state(&self) -> state::DiskState {
        let actor = self.budget_actor.lock().expect("budget_actor lock poisoned");
        actor.disk_state()
    }

    /// Get mutable access to the budget actor for testing and monitoring
    pub fn budget_actor(&self) -> Result<std::sync::MutexGuard<'_, actor::StorageBudgetActor>, String> {
        self.budget_actor
            .lock()
            .map_err(|e| format!("Failed to lock budget actor: {}", e))
    }
}

impl StorageBackend for HybridStorage {
    fn submit_read(&self, path: String, callback: StorageCallback) {
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
        // Write to local immediately
        let local_clone = Arc::clone(&self.local);
        let cloud_clone = Arc::clone(&self.cloud);

        let path_clone = path.clone();
        let data_clone = data.clone();

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

                // Schedule cloud write in background (fire and forget)
                // In production, this would be queued in the runtime
                let (tx_cloud, _) = std::sync::mpsc::channel();
                cloud_clone.submit_write(path, data, tx_cloud);
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
