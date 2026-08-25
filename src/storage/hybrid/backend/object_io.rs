//! Raw local/cloud object operations and callback trait bridging.

use super::{
    Arc, Duration, HybridStorage, StorageBackend, StorageCallback, StorageEvent,
    StorageObjectMetadata, StorageOutcome,
};

impl HybridStorage {
    pub(super) fn cloud_backend_for_key(&self, key: &str) -> &Arc<dyn StorageBackend> {
        if key.starts_with(crate::cloud_layout::CloudObjectLayout::WAL_PREFIX) {
            &self.wal_cloud
        } else if key.starts_with(crate::cloud_layout::CloudObjectLayout::SST_PREFIX) {
            &self.cloud
        } else {
            &self.control_cloud
        }
    }

    pub(super) fn read_cloud_object_from_backend_blocking(
        cloud: &Arc<dyn StorageBackend>,
        key: &str,
        callback_timeout: Duration,
    ) -> Result<Vec<u8>, String> {
        Self::read_object_from_backend_blocking(cloud, key, callback_timeout)
    }

    pub(super) fn read_object_from_backend_blocking(
        backend: &Arc<dyn StorageBackend>,
        key: &str,
        callback_timeout: Duration,
    ) -> Result<Vec<u8>, String> {
        let (tx, rx) = std::sync::mpsc::channel();
        backend.submit_read_with_timeout(key, callback_timeout, tx);
        match rx.recv_timeout(callback_timeout) {
            Ok(StorageEvent::ReadComplete {
                result: StorageOutcome::Ok(data),
                ..
            }) => Ok(data),
            Ok(StorageEvent::ReadComplete {
                result: StorageOutcome::Err(error),
                ..
            }) => Err(format!("cloud object '{key}' unreadable: {error}")),
            Ok(other) => Err(format!(
                "unexpected cloud read response for '{key}': {other:?}"
            )),
            Err(error) => Err(format!("cloud read timed out for '{key}': {error}")),
        }
    }

    pub(super) fn head_object_from_backend_blocking(
        backend: &Arc<dyn StorageBackend>,
        key: &str,
        callback_timeout: Duration,
    ) -> Result<StorageObjectMetadata, String> {
        let (tx, rx) = std::sync::mpsc::channel();
        backend.submit_head_with_timeout(key, callback_timeout, tx);
        match rx.recv_timeout(callback_timeout) {
            Ok(StorageEvent::HeadComplete {
                key: returned_key,
                result: StorageOutcome::Ok(metadata),
            }) => {
                let _ = returned_key;
                Ok(metadata)
            }
            Ok(StorageEvent::HeadComplete {
                key: returned_key,
                result: StorageOutcome::Err(error),
            }) => {
                let _ = returned_key;
                Err(format!(
                    "cloud object '{key}' unreadable during cached proof revalidation: {error}"
                ))
            }
            Ok(other) => Err(format!(
                "unexpected cloud HEAD response for '{key}': {other:?}"
            )),
            Err(error) => Err(format!("cloud HEAD timed out for '{key}': {error}")),
        }
    }

    pub(super) fn storage_error_indicates_missing(error: &str) -> bool {
        let error = error.trim().to_ascii_lowercase();
        error.starts_with("not found:")
    }

    pub(super) fn storage_error_indicates_precondition_failure(error: &str) -> bool {
        let error = error.trim().to_ascii_lowercase();
        error == "precondition failed" || error.starts_with("precondition failed:")
    }

    pub(super) fn storage_error_indicates_timeout(error: &str) -> bool {
        let error = error.trim().to_ascii_lowercase();
        error.contains("timed out") || error.contains("timeout")
    }

    pub(super) fn object_exists_in_backend_blocking(
        backend: &Arc<dyn StorageBackend>,
        key: &str,
        callback_timeout: Duration,
    ) -> Result<bool, String> {
        let (tx, rx) = std::sync::mpsc::channel();
        backend.submit_head(key, tx);
        match rx.recv_timeout(callback_timeout) {
            Ok(StorageEvent::HeadComplete {
                key: returned_key,
                result: StorageOutcome::Ok(_),
            }) => {
                let _ = returned_key;
                Ok(true)
            }
            Ok(StorageEvent::HeadComplete {
                key: returned_key,
                result: StorageOutcome::Err(error),
            }) if Self::storage_error_indicates_missing(&error) => {
                let _ = returned_key;
                Ok(false)
            }
            Ok(StorageEvent::HeadComplete {
                key: returned_key,
                result: StorageOutcome::Err(error),
            }) => {
                let _ = returned_key;
                Err(format!("object '{key}' HEAD failed: {error}"))
            }
            Ok(other) => Err(format!(
                "unexpected storage HEAD response for '{key}': {other:?}"
            )),
            Err(error) => Err(format!("storage HEAD timed out for '{key}': {error}")),
        }
    }

    pub(super) fn delete_object_from_backend_blocking(
        backend: &Arc<dyn StorageBackend>,
        key: &str,
        callback_timeout: Duration,
    ) -> Result<bool, String> {
        let (tx, rx) = std::sync::mpsc::channel();
        backend.submit_delete(key, tx);
        match rx.recv_timeout(callback_timeout) {
            Ok(StorageEvent::DeleteComplete {
                key: returned_key,
                result: StorageOutcome::Ok(()),
            }) => {
                let _ = returned_key;
                Ok(true)
            }
            Ok(StorageEvent::DeleteComplete {
                key: returned_key,
                result: StorageOutcome::Err(error),
            }) if Self::storage_error_indicates_missing(&error) => {
                let _ = returned_key;
                Ok(false)
            }
            Ok(StorageEvent::DeleteComplete {
                key: returned_key,
                result: StorageOutcome::Err(error),
            }) => {
                let _ = returned_key;
                Err(format!("object '{key}' delete failed: {error}"))
            }
            Ok(other) => Err(format!(
                "unexpected storage delete response for '{key}': {other:?}"
            )),
            Err(error) => Err(format!("storage delete timed out for '{key}': {error}")),
        }
    }
}

impl StorageBackend for HybridStorage {
    fn submit_read(&self, key: &str, callback: StorageCallback) {
        // OBJECT STORAGE ONLY - reads SSTs, metadata, etc.
        // Try local first, fall back to cloud

        let local_clone = Arc::clone(&self.local);
        let cloud_clone = Arc::clone(self.cloud_backend_for_key(key));
        let key = key.to_string();

        let (tx, rx) = std::sync::mpsc::channel();
        local_clone.submit_read(&key, tx);

        match rx.recv_timeout(self.callback_timeout) {
            Ok(StorageEvent::ReadComplete {
                key: k,
                result: StorageOutcome::Ok(data),
            }) => {
                // Success from local, return immediately
                let _ = callback.send(StorageEvent::ReadComplete {
                    key: k,
                    result: StorageOutcome::Ok(data),
                });
            }
            Ok(StorageEvent::ReadComplete {
                key: k,
                result: StorageOutcome::Err(_),
            }) => {
                // Local miss, try cloud
                let (tx_cloud, rx_cloud) = std::sync::mpsc::channel();
                cloud_clone.submit_read(&k, tx_cloud);
                match rx_cloud.recv_timeout(self.callback_timeout) {
                    Ok(event) => {
                        let _ = callback.send(event);
                    }
                    Err(error) => {
                        let _ = callback.send(StorageEvent::ReadComplete {
                            key: k,
                            result: StorageOutcome::Err(format!(
                                "cloud read callback failed or timed out: {error}"
                            )),
                        });
                    }
                }
            }
            _ => {
                let _ = callback.send(StorageEvent::ReadComplete {
                    key: key.clone(),
                    result: StorageOutcome::Err("Hybrid read failed".to_string()),
                });
            }
        }
    }

    fn submit_write(&self, key: &str, data: Vec<u8>, callback: StorageCallback) {
        // OBJECT STORAGE ONLY - NOT for WAL durability
        // WAL durability uses enqueue_wal_segment() instead

        let local_clone = Arc::clone(&self.local);
        let cloud_clone = Arc::clone(self.cloud_backend_for_key(key));
        let data_clone = data.clone();

        // Always write to local first
        let (tx, rx) = std::sync::mpsc::channel();
        local_clone.submit_write(key, data_clone, tx);

        match rx.recv_timeout(self.callback_timeout) {
            Ok(StorageEvent::WriteComplete { result, .. }) => {
                if !key.starts_with("sst/") || matches!(result, StorageOutcome::Err(_)) {
                    let _ = callback.send(StorageEvent::WriteComplete {
                        key: key.to_string(),
                        result,
                    });
                    return;
                }

                let (tx_cloud, rx_cloud) = std::sync::mpsc::channel();
                cloud_clone.submit_write_with_headers(
                    key,
                    data,
                    vec![("If-None-Match".into(), "*".into())],
                    tx_cloud,
                );

                let result = match rx_cloud.recv_timeout(self.callback_timeout) {
                    Ok(StorageEvent::WriteComplete {
                        result: StorageOutcome::Ok(()),
                        ..
                    }) => StorageOutcome::Ok(()),
                    Ok(StorageEvent::WriteComplete {
                        result: StorageOutcome::Err(error),
                        ..
                    }) => StorageOutcome::Err(format!(
                        "cloud write failed after local write succeeded: {error}"
                    )),
                    Ok(other) => StorageOutcome::Err(format!(
                        "unexpected cloud write completion event: {other:?}"
                    )),
                    Err(error) => StorageOutcome::Err(format!(
                        "cloud write callback failed or timed out after local write succeeded: {error}"
                    )),
                };

                let _ = callback.send(StorageEvent::WriteComplete {
                    key: key.to_string(),
                    result,
                });
            }
            _ => {
                let _ = callback.send(StorageEvent::WriteComplete {
                    key: key.to_string(),
                    result: StorageOutcome::Err("Hybrid write failed".to_string()),
                });
            }
        }
    }

    fn submit_delete(&self, key: &str, callback: StorageCallback) {
        // OBJECT STORAGE ONLY - deletes SSTs, metadata, etc.
        // Delete from both local and cloud

        let local_clone = Arc::clone(&self.local);
        let cloud_clone = Arc::clone(self.cloud_backend_for_key(key));
        let key = key.to_string();

        let (tx_local, rx_local) = std::sync::mpsc::channel();
        local_clone.submit_delete(&key, tx_local);

        let (tx_cloud, rx_cloud) = std::sync::mpsc::channel();
        cloud_clone.submit_delete(&key, tx_cloud);

        // Wait for both and report result
        let local_result = rx_local.recv_timeout(self.callback_timeout).ok();
        let cloud_result = rx_cloud.recv_timeout(self.callback_timeout).ok();

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
            key,
            result: combined_result,
        });
    }

    #[cfg(test)]
    fn submit_list(&self, prefix: &str, callback: StorageCallback) {
        // OBJECT STORAGE ONLY - lists SSTs, metadata, etc.
        // Merge results from both local and cloud

        let local_clone = Arc::clone(&self.local);
        let cloud_clone = Arc::clone(self.cloud_backend_for_key(prefix));
        let prefix = prefix.to_string();

        let (tx_local, rx_local) = std::sync::mpsc::channel();
        local_clone.submit_list(&prefix, tx_local);

        let (tx_cloud, rx_cloud) = std::sync::mpsc::channel();
        cloud_clone.submit_list(&prefix, tx_cloud);

        let mut results = Vec::new();

        if let Ok(StorageEvent::ListComplete {
            result: StorageOutcome::Ok(local_items),
            ..
        }) = rx_local.recv_timeout(self.callback_timeout)
        {
            results.extend(local_items);
        }

        if let Ok(StorageEvent::ListComplete {
            result: StorageOutcome::Ok(cloud_items),
            ..
        }) = rx_cloud.recv_timeout(self.callback_timeout)
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
            prefix: prefix.clone(),
            result: StorageOutcome::Ok(results),
        });
    }
}
