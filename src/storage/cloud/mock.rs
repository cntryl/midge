//! Deterministic, synchronous in-memory `CloudBackend` for tests.

#[allow(clippy::wildcard_imports)]
use super::*;
use parking_lot::Mutex;
use std::collections::HashMap;

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// Deterministic mock backend for testing (synchronous).
pub struct MockCloudBackend {
    storage: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    gens: Arc<Mutex<HashMap<String, u64>>>,
    /// Serializes conditional mutation checks with their corresponding write
    /// or delete. Separate storage and generation maps otherwise permit a
    /// stale request to pass a read-then-write race.
    mutation_lock: Arc<Mutex<()>>,
    uploads: Arc<Mutex<Vec<(String, u64)>>>,
    downloads: Arc<Mutex<Vec<String>>>,
    range_downloads: Arc<Mutex<Vec<String>>>,
}
impl MockCloudBackend {
    pub fn new() -> Self {
        Self {
            storage: Arc::new(Mutex::new(HashMap::new())),
            gens: Arc::new(Mutex::new(HashMap::new())),
            mutation_lock: Arc::new(Mutex::new(())),
            uploads: Arc::new(Mutex::new(Vec::new())),
            downloads: Arc::new(Mutex::new(Vec::new())),
            range_downloads: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn get_uploads(&self) -> Vec<(String, u64)> {
        self.uploads.lock().clone()
    }

    pub fn get_downloads(&self) -> Vec<String> {
        self.downloads.lock().clone()
    }

    pub fn get_range_downloads(&self) -> Vec<String> {
        self.range_downloads.lock().clone()
    }

    pub fn clear_history(&self) {
        self.uploads.lock().clear();
        self.downloads.lock().clear();
        self.range_downloads.lock().clear();
    }
}

impl Default for MockCloudBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl CloudBackend for MockCloudBackend {
    fn submit_get_range_with_identity(
        &self,
        key: &str,
        start: u64,
        end: u64,
        expected: StorageObjectMetadata,
        _timeout: std::time::Duration,
        callback: CloudCallback,
    ) {
        self.range_downloads.lock().push(key.to_string());
        let _guard = self.mutation_lock.lock();
        let storage = self.storage.lock();
        let result = (|| {
            let data = storage
                .get(key)
                .ok_or_else(|| CloudError::NotFound(key.into()))?;
            let generation = self.gens.lock().get(key).copied().unwrap_or_default();
            let actual = StorageObjectMetadata {
                size: data.len() as u64,
                etag: format!("mock-gen-{generation}"),
                generation: None,
            };
            if !actual.same_version(&expected) {
                return Err(CloudError::PreconditionFailed(
                    "remote SST version changed".into(),
                ));
            }
            let start =
                usize::try_from(start).map_err(|error| CloudError::Protocol(error.to_string()))?;
            let end =
                usize::try_from(end).map_err(|error| CloudError::Protocol(error.to_string()))?;
            data.get(start..end)
                .map(<[u8]>::to_vec)
                .ok_or_else(|| CloudError::Protocol("range exceeds object".into()))
        })();
        let _ = callback.send(CloudEvent::GetRange {
            key: key.into(),
            start,
            end: Some(end),
            result,
        });
    }

    fn submit_put(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        let key = key.to_string();
        let _mutation = self.mutation_lock.lock();
        // Honor `If-None-Match: *` (conditional create).
        let if_none_match = headers
            .iter()
            .any(|(k, v)| k.eq_ignore_ascii_case("if-none-match") && v == "*");
        if if_none_match && self.storage.lock().contains_key(&key) {
            // Simulate conditional failure (precondition failed)
            let event = CloudEvent::Put {
                key,
                result: CloudOutcome::Err(CloudError::PreconditionFailed(
                    "precondition failed".to_string(),
                )),
            };
            let _ = callback.send(event);
            return;
        }

        // Honor `If-Match: <etag>` (conditional update).
        // If object missing → precondition failed. If etag mismatches → precondition failed.
        if let Some((_, expected)) = headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("if-match"))
        {
            let exists = self.storage.lock().contains_key(&key);
            if !exists {
                let event = CloudEvent::Put {
                    key,
                    result: CloudOutcome::Err(CloudError::PreconditionFailed(
                        "precondition failed".to_string(),
                    )),
                };
                let _ = callback.send(event);
                return;
            }

            // compare expected value to stored generation-based etag
            let gens_lock = self.gens.lock();
            let current_gen = gens_lock.get(&key).copied().unwrap_or(0);
            let current_etag = format!("mock-gen-{current_gen}");
            if expected != &current_etag {
                let event = CloudEvent::Put {
                    key,
                    result: CloudOutcome::Err(CloudError::PreconditionFailed(
                        "precondition failed".to_string(),
                    )),
                };
                let _ = callback.send(event);
                return;
            }
        }

        // Perform put: store data and bump generation (etag)
        {
            let mut store = self.storage.lock();
            store.insert(key.clone(), data.clone());
        }
        let mut gens = self.gens.lock();
        let new_gen = gens.get(&key).copied().unwrap_or(0).saturating_add(1);
        gens.insert(key.clone(), new_gen);

        self.uploads.lock().push((key.clone(), data.len() as u64));
        let event = CloudEvent::Put {
            key,
            result: CloudOutcome::Ok(()),
        };
        let _ = callback.send(event);
    }

    fn submit_get(&self, key: &str, callback: CloudCallback) {
        let key = key.to_string();
        let result = self
            .storage
            .lock()
            .get(&key)
            .cloned()
            .ok_or(MidgeError::NotFound);
        self.downloads.lock().push(key.clone());
        let event = CloudEvent::Get {
            key,
            result: cloud_outcome_from_result(result),
        };
        let _ = callback.send(event);
    }

    fn submit_get_with_metadata(&self, key: &str, callback: CloudCallback) {
        self.downloads.lock().push(key.to_string());
        let _guard = self.mutation_lock.lock();
        let data = self.storage.lock().get(key).cloned();
        let generation = self.gens.lock().get(key).copied();
        let result = match (data, generation) {
            (Some(data), Some(generation)) => CloudOutcome::Ok((
                data.clone(),
                ObjectMetadata::new(data.len() as u64, format!("mock-gen-{generation}")),
            )),
            _ => CloudOutcome::Err(CloudError::NotFound(key.to_string())),
        };
        let _ = callback.send(CloudEvent::GetWithMetadata {
            key: key.to_string(),
            result,
        });
    }

    #[cfg(any(test, feature = "cloud-common"))]
    fn submit_get_range(&self, key: &str, start: u64, end: Option<u64>, callback: CloudCallback) {
        let key = key.to_string();
        let result = self
            .storage
            .lock()
            .get(&key)
            .map(|data| {
                let end_idx =
                    usize::try_from(end.unwrap_or(usize_to_u64(data.len()))).unwrap_or(usize::MAX);
                let start_idx = usize::try_from(start).unwrap_or(usize::MAX);
                data[start_idx..end_idx].to_vec()
            })
            .ok_or(MidgeError::NotFound);
        let event = CloudEvent::GetRange {
            key,
            start,
            end,
            result: cloud_outcome_from_result(result),
        };
        let _ = callback.send(event);
    }

    fn submit_delete(&self, key: &str, headers: Vec<(String, String)>, callback: CloudCallback) {
        let key = key.to_string();
        let _mutation = self.mutation_lock.lock();
        if let Some((_, expected)) = headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("if-match"))
        {
            // Deleting an absent object succeeds, conditional or not, as on
            // every real provider.
            let exists = self.storage.lock().contains_key(&key);
            if !exists {
                let event = CloudEvent::Delete {
                    key,
                    result: CloudOutcome::Ok(()),
                };
                let _ = callback.send(event);
                return;
            }

            let current_gen = self.gens.lock().get(&key).copied().unwrap_or(0);
            let current_etag = format!("mock-gen-{current_gen}");
            if expected.trim_matches('"') != current_etag {
                let event = CloudEvent::Delete {
                    key,
                    result: CloudOutcome::Err(CloudError::PreconditionFailed(
                        "precondition failed".to_string(),
                    )),
                };
                let _ = callback.send(event);
                return;
            }
        }

        self.storage.lock().remove(&key);
        self.gens.lock().remove(&key);
        let event = CloudEvent::Delete {
            key,
            result: CloudOutcome::Ok(()),
        };
        let _ = callback.send(event);
    }

    fn submit_list(&self, prefix: &str, callback: CloudCallback) {
        let prefix = prefix.to_string();
        let results: Vec<_> = self
            .storage
            .lock()
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .cloned()
            .collect();
        let event = CloudEvent::List {
            prefix,
            result: CloudOutcome::Ok(results),
        };
        let _ = callback.send(event);
    }

    fn submit_head(&self, key: &str, callback: CloudCallback) {
        let key = key.to_string();
        let result = self
            .storage
            .lock()
            .get(&key)
            .map(|data| {
                // ETag is generation based and independent from content length.
                let gen = self.gens.lock().get(&key).copied().unwrap_or(0);
                ObjectMetadata::new(data.len() as u64, format!("mock-gen-{gen}"))
            })
            .ok_or(MidgeError::NotFound);
        let event = CloudEvent::Head {
            key,
            result: cloud_outcome_from_result(result),
        };
        let _ = callback.send(event);
    }
}
