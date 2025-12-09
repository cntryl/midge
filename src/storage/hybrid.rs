//! Hybrid storage backend - combines local and cloud storage
//!
//! Architecture:
//! - Reads try local first, fall back to cloud
//! - Writes go to local, with background cloud upload scheduled
//! - Deletes remove from both
//! - Lists merge results from both

use crate::storage::{StorageBackend, StorageCallback, StorageEvent, StorageOutcome};
use std::sync::Arc;

/// Hybrid storage combining local filesystem and cloud backends
pub struct HybridStorage {
    /// Local storage backend (usually filesystem)
    local: Arc<dyn StorageBackend>,
    /// Cloud storage backend (S3, GCS, Azure, etc.)
    cloud: Arc<dyn StorageBackend>,
}

impl HybridStorage {
    /// Create a new hybrid storage with local and cloud backends
    pub fn new(local: Arc<dyn StorageBackend>, cloud: Arc<dyn StorageBackend>) -> Self {
        Self { local, cloud }
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
