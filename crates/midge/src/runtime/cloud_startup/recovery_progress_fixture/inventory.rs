use super::{backend::FixtureObservations, RecoveryProgressFixtureMode, PROVIDER_TIMEOUT};
use cntryl_midge_core::common::MidgeResult;
use cntryl_midge_storage::{StorageBackend, StorageError, StorageRequest};
use parking_lot::Mutex;
use std::path::PathBuf;
use std::sync::{mpsc, Arc};
use std::time::Duration;

/// Adapt genuine filesystem HEAD metadata to the startup cloud API. This
/// fixture contributes no tracing events and delays only a completed response.
pub(super) struct InventoryCloudBackend {
    inner: cntryl_midge_storage::filesystem::FileSystem,
    mode: RecoveryProgressFixtureMode,
    expected_sizes: std::collections::HashMap<String, u64>,
    completed_keys: Mutex<std::collections::HashSet<String>>,
    observations: Arc<FixtureObservations>,
    held: Mutex<Vec<cntryl_midge_storage::cloud::CloudCallback>>,
    head_delay: Duration,
    persist_progress: bool,
}

impl InventoryCloudBackend {
    pub(super) fn new(
        remote_root: PathBuf,
        mode: RecoveryProgressFixtureMode,
        files: &[cntryl_midge_durability::metadata::FileMeta],
        observations: Arc<FixtureObservations>,
        head_delay: Duration,
        persist_progress: bool,
    ) -> MidgeResult<Self> {
        Ok(Self {
            inner: cntryl_midge_storage::filesystem::FileSystem::new(remote_root)?,
            mode,
            expected_sizes: files
                .iter()
                .map(|file| {
                    (
                        cntryl_midge_core::cloud_layout::object_key(&file.name),
                        file.size_bytes,
                    )
                })
                .collect(),
            completed_keys: Mutex::new(std::collections::HashSet::new()),
            observations,
            held: Mutex::new(Vec::new()),
            head_delay,
            persist_progress,
        })
    }

    fn head(&self, key: &str, callback: cntryl_midge_storage::cloud::CloudCallback) {
        use cntryl_midge_storage::cloud::{CloudError, CloudEvent, CloudOutcome};
        if self.mode == RecoveryProgressFixtureMode::HeldInventory {
            if let Err(error) = self.observations.hold_request() {
                let _ = callback.send(CloudEvent::Head {
                    key: key.to_string(),
                    result: Err(CloudError::Protocol(error.to_string())),
                });
            } else {
                self.held.lock().push(callback);
            }
            return;
        }
        let (sender, receiver) = mpsc::channel();
        self.inner.submit_head_request(
            StorageRequest::new(
                key,
                cntryl_midge_core::common::OperationDeadline::from_budget(PROVIDER_TIMEOUT),
                PROVIDER_TIMEOUT,
            ),
            sender,
        );
        let result: CloudOutcome<cntryl_midge_storage::cloud::ObjectMetadata> = match receiver
            .recv_timeout(PROVIDER_TIMEOUT)
        {
            Ok(cntryl_midge_storage::StorageEvent::HeadComplete {
                key: actual,
                result,
            }) if actual == key => match result {
                cntryl_midge_storage::StorageOutcome::Ok(metadata) => Ok(metadata),
                cntryl_midge_storage::StorageOutcome::Err(error) => Err(Self::head_error(&error)),
            },
            Err(mpsc::RecvTimeoutError::Timeout) => Err(CloudError::Timeout(
                "fixture HEAD callback timed out".into(),
            )),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(CloudError::Transport(
                "fixture HEAD callback disconnected".into(),
            )),
            other => Err(CloudError::Protocol(format!(
                "fixture HEAD callback: {other:?}"
            ))),
        };
        let expected_size = result.as_ref().ok().map(|metadata| {
            self.expected_sizes
                .get(key)
                .is_some_and(|size| *size == metadata.size)
        });
        if expected_size.is_some() {
            std::thread::sleep(self.head_delay);
        }
        let delivered = callback
            .send(CloudEvent::Head {
                key: key.to_string(),
                result,
            })
            .is_ok();
        if let Some(expected_size) = expected_size.filter(|_| delivered) {
            let unique_key = self.completed_keys.lock().insert(key.to_string());
            self.observations
                .completed_inventory_head(expected_size && unique_key, self.persist_progress);
        }
    }

    fn head_error(error: &StorageError) -> cntryl_midge_storage::cloud::CloudError {
        use cntryl_midge_storage::cloud::CloudError;
        use cntryl_midge_storage::StorageErrorKind;
        let message = format!("fixture HEAD: {error}");
        match error.kind() {
            StorageErrorKind::NotFound => CloudError::NotFound(message),
            StorageErrorKind::Timeout => CloudError::Timeout(message),
            StorageErrorKind::Unauthorized => CloudError::Unauthorized(message),
            StorageErrorKind::PreconditionFailed => CloudError::PreconditionFailed(message),
            StorageErrorKind::Transport | StorageErrorKind::Io => CloudError::Transport(message),
            StorageErrorKind::Protocol
            | StorageErrorKind::ResourceLimit
            | StorageErrorKind::Corruption => CloudError::Protocol(message),
        }
    }
}

impl cntryl_midge_storage::cloud::CloudReadCapability for InventoryCloudBackend {
    fn submit_get(&self, key: &str, callback: cntryl_midge_storage::cloud::CloudCallback) {
        let _ = callback.send(cntryl_midge_storage::cloud::CloudEvent::Get {
            key: key.to_string(),
            result: Err(cntryl_midge_storage::cloud::CloudError::Protocol(
                "inventory fixture only supports HEAD".into(),
            )),
        });
    }

    fn submit_get_with_metadata(
        &self,
        key: &str,
        callback: cntryl_midge_storage::cloud::CloudCallback,
    ) {
        let _ = callback.send(cntryl_midge_storage::cloud::CloudEvent::GetWithMetadata {
            key: key.to_string(),
            result: Err(cntryl_midge_storage::cloud::CloudError::Protocol(
                "inventory fixture only supports HEAD".into(),
            )),
        });
    }

    fn submit_get_range(
        &self,
        key: &str,
        start: u64,
        end: Option<u64>,
        callback: cntryl_midge_storage::cloud::CloudCallback,
    ) {
        let _ = callback.send(cntryl_midge_storage::cloud::CloudEvent::GetRange {
            key: key.to_string(),
            start,
            end,
            result: Err(cntryl_midge_storage::cloud::CloudError::Protocol(
                "inventory fixture only supports HEAD".into(),
            )),
        });
    }
}

impl cntryl_midge_storage::cloud::CloudWriteCapability for InventoryCloudBackend {
    fn submit_put(
        &self,
        key: &str,
        _data: Vec<u8>,
        _headers: Vec<(String, String)>,
        callback: cntryl_midge_storage::cloud::CloudCallback,
    ) {
        let _ = callback.send(cntryl_midge_storage::cloud::CloudEvent::Put {
            key: key.to_string(),
            result: Err(cntryl_midge_storage::cloud::CloudError::Protocol(
                "inventory fixture only supports HEAD".into(),
            )),
        });
    }
}

impl cntryl_midge_storage::cloud::CloudDeleteCapability for InventoryCloudBackend {
    fn submit_delete(
        &self,
        key: &str,
        _headers: Vec<(String, String)>,
        callback: cntryl_midge_storage::cloud::CloudCallback,
    ) {
        let _ = callback.send(cntryl_midge_storage::cloud::CloudEvent::Delete {
            key: key.to_string(),
            result: Err(cntryl_midge_storage::cloud::CloudError::Protocol(
                "inventory fixture only supports HEAD".into(),
            )),
        });
    }
}

impl cntryl_midge_storage::cloud::CloudListCapability for InventoryCloudBackend {
    fn submit_list(&self, prefix: &str, callback: cntryl_midge_storage::cloud::CloudCallback) {
        let _ = callback.send(cntryl_midge_storage::cloud::CloudEvent::List {
            prefix: prefix.to_string(),
            result: Err(cntryl_midge_storage::cloud::CloudError::Protocol(
                "inventory fixture only supports HEAD".into(),
            )),
        });
    }
}

impl cntryl_midge_storage::cloud::CloudHeadCapability for InventoryCloudBackend {
    fn submit_head(&self, key: &str, callback: cntryl_midge_storage::cloud::CloudCallback) {
        self.head(key, callback);
    }
}

impl cntryl_midge_storage::cloud::CloudBackend for InventoryCloudBackend {}
