//! Provider lifetime and identity changes at the admitted control read boundary.

use super::*;
use crate::storage::cloud::CloudWriteCapability;
use crate::storage::cloud::{CloudBackend, CloudCallback, CloudStorage, MockCloudBackend};

#[derive(Default)]
struct InterruptedRange {
    inner: MockCloudBackend,
    pending: parking_lot::Mutex<Option<CloudCallback>>,
    replace: bool,
    replacement: parking_lot::Mutex<Option<Vec<u8>>>,
    range_reads: std::sync::atomic::AtomicUsize,
}

impl crate::storage::cloud::CloudReadCapability for InterruptedRange {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_get, submit_get_with_metadata);

    fn submit_get_range(&self, key: &str, start: u64, end: Option<u64>, callback: CloudCallback) {
        self.inner.submit_get_range(key, start, end, callback);
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
        self.range_reads
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.replace {
            let (tx, rx) = mpsc::channel();
            self.inner.submit_get_range_with_identity(
                key,
                start,
                end,
                expected.clone(),
                timeout,
                tx,
            );
            let captured = rx.recv().unwrap();
            if let Some(replacement) = self.replacement.lock().take() {
                assert!(matches!(
                    &captured,
                    crate::storage::cloud::CloudEvent::GetRange { result: Ok(_), .. }
                ));
                let headers = crate::storage::cloud::object_match_precondition_headers(
                    &expected.etag,
                    expected.generation.as_deref(),
                )
                .expect("range identity also conditions replacement");
                let (tx, rx) = mpsc::channel();
                self.inner.submit_put(key, replacement, headers, tx);
                assert!(matches!(
                    rx.recv().unwrap(),
                    crate::storage::cloud::CloudEvent::Put { result: Ok(()), .. }
                ));
            }
            callback.send(captured).unwrap();
        } else {
            *self.pending.lock() = Some(callback);
        }
    }
}

impl crate::storage::cloud::CloudWriteCapability for InterruptedRange {
    fn submit_put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        self.inner.submit_put(key, bytes, headers, callback);
    }
}

impl crate::storage::cloud::CloudDeleteCapability for InterruptedRange {
    crate::storage::cloud::unsupported_cloud_backend!(submit_delete);
}

impl crate::storage::cloud::CloudListCapability for InterruptedRange {
    crate::storage::cloud::unsupported_cloud_backend!(submit_list);
}

impl crate::storage::cloud::CloudHeadCapability for InterruptedRange {
    fn submit_head(&self, key: &str, callback: CloudCallback) {
        self.inner.submit_head(key, callback);
    }
}

impl CloudBackend for InterruptedRange {}

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
fn should_return_pinned_control_snapshot_when_object_changes_after_its_final_range() {
    // Arrange
    let provider = Arc::new(InterruptedRange {
        replace: true,
        replacement: parking_lot::Mutex::new(Some(vec![9; 1024])),
        ..InterruptedRange::default()
    });
    let storage = HybridStorage::with_policy(
        Arc::new(CloudStorage::new(provider.clone(), String::new())),
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    );
    storage
        .compare_exchange_remote_object("metadata/catalog", None, vec![7; 1024])
        .unwrap();
    let initial = storage
        .remote_range_metadata_optional_within("metadata/catalog", &OperationDeadline::unbounded())
        .unwrap()
        .expect("initial object identity");
    let budget = ResourceBudget::new(1024 * 1024);

    // Act
    let captured = storage
        .read_control_object("metadata/catalog", &budget, &OperationDeadline::unbounded())
        .expect("a completed pinned range remains a coherent snapshot")
        .expect("captured initial object");
    let stale = storage.write_control_object(
        "metadata/catalog",
        Some(captured.object_metadata()),
        b"stale",
        &budget,
        &OperationDeadline::unbounded(),
    );
    let current = storage
        .read_control_object("metadata/catalog", &budget, &OperationDeadline::unbounded())
        .unwrap()
        .expect("replacement survives stale CAS");

    // Assert
    assert_eq!(captured.bytes(), vec![7; 1024]);
    assert_eq!(captured.object_metadata(), &initial);
    assert!(matches!(stale, Err(MidgeError::Busy(_))), "{stale:?}");
    assert_eq!(current.bytes(), vec![9; 1024]);
    assert!(!current.object_metadata().same_version(&initial));
    assert!(provider.replacement.lock().is_none());
    drop(captured);
    drop(current);
    let until = std::time::Instant::now() + Duration::from_secs(2);
    while budget.used() != 0 && std::time::Instant::now() < until {
        std::thread::yield_now();
    }
    assert_eq!(budget.used(), 0);
}

#[test]
fn should_refuse_mixed_control_snapshot_when_object_changes_between_pinned_ranges() {
    // Arrange
    let length = RANGE_BYTES + 1024;
    let provider = Arc::new(InterruptedRange {
        replace: true,
        replacement: parking_lot::Mutex::new(Some(vec![9; length])),
        ..InterruptedRange::default()
    });
    let storage = HybridStorage::with_policy(
        Arc::new(CloudStorage::new(provider.clone(), String::new())),
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    );
    storage
        .compare_exchange_remote_object("metadata/catalog", None, vec![7; length])
        .unwrap();
    let budget = ResourceBudget::new(1024 * 1024);

    // Act
    let result =
        storage.read_control_object("metadata/catalog", &budget, &OperationDeadline::unbounded());

    // Assert: the first range completed, but the second cannot read its version.
    assert!(
        result.is_err(),
        "mixed control bytes must not escape: {result:?}"
    );
    assert!(
        provider.replacement.lock().is_none(),
        "the conditional replacement must have committed"
    );
    assert_eq!(
        provider
            .range_reads
            .load(std::sync::atomic::Ordering::SeqCst),
        2
    );
    let until = std::time::Instant::now() + Duration::from_secs(2);
    while budget.used() != 0 && std::time::Instant::now() < until {
        std::thread::yield_now();
    }
    assert_eq!(budget.used(), 0);
    let current = storage
        .read_control_object("metadata/catalog", &budget, &OperationDeadline::unbounded())
        .unwrap()
        .expect("replacement remains authoritative");
    assert_eq!(current.bytes(), vec![9; length]);
}

#[test]
fn should_condition_control_replacement_on_filesystem_range_identity() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let backend: Arc<dyn StorageBackend> =
        Arc::new(crate::storage::filesystem::FileSystem::new(directory.path()).unwrap());
    let storage = HybridStorage::with_policy(
        backend,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    );
    storage
        .compare_exchange_remote_object("metadata/catalog", None, b"old".to_vec())
        .unwrap();
    let identity = storage
        .remote_range_metadata_optional_within("metadata/catalog", &OperationDeadline::unbounded())
        .unwrap()
        .expect("observed object");
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
