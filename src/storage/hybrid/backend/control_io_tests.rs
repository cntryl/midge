//! Provider lifetime and identity changes at the admitted control read boundary.

use super::*;
use crate::storage::cloud::{CloudBackend, CloudCallback, CloudStorage, MockCloudBackend};

#[derive(Default)]
struct InterruptedRange {
    inner: MockCloudBackend,
    pending: parking_lot::Mutex<Option<CloudCallback>>,
    replace: bool,
}

impl CloudBackend for InterruptedRange {
    fn submit_put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        self.inner.submit_put(key, bytes, headers, callback);
    }
    fn submit_get_range(&self, key: &str, start: u64, end: Option<u64>, callback: CloudCallback) {
        self.inner.submit_get_range(key, start, end, callback);
    }
    fn submit_head(&self, key: &str, callback: CloudCallback) {
        self.inner.submit_head(key, callback);
    }
    fn submit_get_range_with_identity(
        &self,
        key: &str,
        start: u64,
        end: u64,
        expected: StorageObjectMetadata,
        timeout: Duration,
        callback: CloudCallback,
    ) {
        if self.replace {
            self.inner
                .submit_get_range_with_identity(key, start, end, expected, timeout, callback);
            let (tx, rx) = mpsc::channel();
            self.inner.submit_put(key, vec![9; 1024], Vec::new(), tx);
            rx.recv().unwrap();
        } else {
            *self.pending.lock() = Some(callback);
        }
    }
}

#[test]
fn should_retain_control_read_charge_until_timed_out_provider_releases_callback() {
    // Arrange
    let provider = Arc::new(InterruptedRange::default());
    let (tx, rx) = mpsc::channel();
    provider
        .inner
        .submit_put("metadata/catalog", vec![7; 1024], Vec::new(), tx);
    rx.recv().unwrap();
    let cloud: Arc<dyn StorageBackend> =
        Arc::new(CloudStorage::new(provider.clone(), String::new()));
    let budget = ResourceBudget::new(1024 * 1024);

    // Act
    let result = HybridStorage::read_control_from_backend(
        &cloud,
        "metadata/catalog",
        &budget,
        Duration::from_millis(200),
        &OperationDeadline::unbounded(),
    );
    let retained = budget.used();
    drop(
        provider
            .pending
            .lock()
            .take()
            .expect("provider owns callback"),
    );
    let until = std::time::Instant::now() + Duration::from_secs(2);
    while budget.used() != 0 && std::time::Instant::now() < until {
        std::thread::yield_now();
    }

    // Assert
    assert!(matches!(result, Err(MidgeError::Timeout(_))));
    assert!(retained >= 1024);
    assert_eq!(budget.used(), 0);
}

#[test]
fn should_reject_control_proof_when_provider_identity_changes_after_range_read() {
    // Arrange
    let provider = Arc::new(InterruptedRange {
        replace: true,
        ..InterruptedRange::default()
    });
    let (tx, rx) = mpsc::channel();
    provider
        .inner
        .submit_put("metadata/catalog", vec![7; 1024], Vec::new(), tx);
    rx.recv().unwrap();
    let cloud: Arc<dyn StorageBackend> = Arc::new(CloudStorage::new(provider, String::new()));
    let budget = ResourceBudget::new(1024 * 1024);

    // Act
    let result = HybridStorage::read_control_from_backend(
        &cloud,
        "metadata/catalog",
        &budget,
        Duration::from_secs(1),
        &OperationDeadline::unbounded(),
    );

    // Assert
    assert!(
        matches!(result, Err(MidgeError::Corruption(_))),
        "{result:?}"
    );
    let until = std::time::Instant::now() + Duration::from_secs(2);
    while budget.used() != 0 && std::time::Instant::now() < until {
        std::thread::yield_now();
    }
    assert_eq!(budget.used(), 0);
}

#[test]
fn should_condition_control_replacement_on_filesystem_range_identity() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let backend: Arc<dyn StorageBackend> =
        Arc::new(crate::storage::filesystem::FileSystem::new(directory.path()).unwrap());
    let storage = HybridStorage::with_policy(
        backend.clone(),
        backend,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    );
    storage
        .compare_exchange_remote_object("metadata/catalog", None, b"old".to_vec())
        .unwrap();
    let identity = storage
        .remote_range_metadata_within("metadata/catalog", &OperationDeadline::unbounded())
        .unwrap();
    let budget = ResourceBudget::new(1024 * 1024);

    // Act
    let updated = storage
        .write_control_object(
            "metadata/catalog",
            Some(&identity),
            b"new",
            &budget,
            &OperationDeadline::unbounded(),
        )
        .unwrap();
    let stale = storage.write_control_object(
        "metadata/catalog",
        Some(&identity),
        b"stale",
        &budget,
        &OperationDeadline::unbounded(),
    );

    // Assert
    assert_eq!(updated.bytes(), b"new");
    assert!(matches!(stale, Err(MidgeError::Busy(_))));
    assert_eq!(
        std::fs::read(directory.path().join("metadata/catalog")).unwrap(),
        b"new"
    );
}
