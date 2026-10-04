use super::*;
use std::time::Duration;

struct DelayedReadBackend {
    inner: MockCloudBackend,
    reads: AtomicUsize,
}

impl CloudBackend for DelayedReadBackend {
    fn submit_get(&self, key: &str, callback: CloudCallback) {
        self.reads.fetch_add(1, Ordering::AcqRel);
        std::thread::sleep(Duration::from_millis(30));
        self.inner.submit_get(key, callback);
    }

    fn submit_get_with_metadata(&self, key: &str, callback: CloudCallback) {
        self.reads.fetch_add(1, Ordering::AcqRel);
        std::thread::sleep(Duration::from_millis(30));
        self.inner.submit_get_with_metadata(key, callback);
    }

    crate::storage::cloud::forward_cloud_backend!(
        inner;
        submit_put, submit_get_range, submit_delete, submit_list, submit_head
    );
}

#[test]
fn should_reject_metadata_result_when_submission_exhausts_operation_deadline() {
    // Arrange: submit returns a queued, valid proof after the caller's budget
    // has expired. This must not receive a fresh callback wait allowance.
    let backend = Arc::new(DelayedReadBackend {
        inner: MockCloudBackend::new(),
        reads: AtomicUsize::new(0),
    });
    let (seed_sender, seed_receiver) = mpsc::channel();
    backend
        .inner
        .submit_put("tenant/object", b"value".to_vec(), Vec::new(), seed_sender);
    seed_receiver.recv().expect("seed proof object");
    let storage = CloudStorage::new(backend.clone(), "tenant".into());
    let (sender, receiver) = mpsc::channel();

    // Act
    StorageBackend::submit_metadata_read_request(
        &storage,
        crate::storage::StorageRequest::new(
            "object",
            crate::common::OperationDeadline::from_budget(Duration::from_millis(10)),
            Duration::from_secs(1),
        ),
        sender,
    );

    // Assert
    let result = receiver.recv().expect("metadata read completion");
    assert!(
        matches!(&result, Err(error) if error.kind() == crate::storage::StorageErrorKind::Timeout),
        "late queued proof must not be accepted: {result:?}"
    );
    assert_eq!(backend.reads.load(Ordering::Acquire), 1);
}

#[test]
fn should_refuse_metadata_submission_when_operation_deadline_is_exhausted() {
    // Arrange
    let backend = Arc::new(DelayedReadBackend {
        inner: MockCloudBackend::new(),
        reads: AtomicUsize::new(0),
    });
    let storage = CloudStorage::new(backend.clone(), "tenant".into());
    let (sender, receiver) = mpsc::channel();

    // Act
    StorageBackend::submit_metadata_read_request(
        &storage,
        crate::storage::StorageRequest::new(
            "object",
            crate::common::OperationDeadline::from_budget(Duration::ZERO),
            Duration::from_secs(1),
        ),
        sender,
    );

    // Assert
    assert!(matches!(
        receiver.recv().expect("metadata read completion"),
        Err(error) if error.kind() == crate::storage::StorageErrorKind::Timeout
    ));
    assert_eq!(backend.reads.load(Ordering::Acquire), 0);
}

#[test]
fn should_reject_blocking_get_result_when_submission_exhausts_callback_budget() {
    // Arrange: the callback cap expires during a synchronous submission that
    // queues valid bytes. The longer operation deadline must not refresh it.
    let backend = Arc::new(DelayedReadBackend {
        inner: MockCloudBackend::new(),
        reads: AtomicUsize::new(0),
    });
    let (seed_sender, seed_receiver) = mpsc::channel();
    backend
        .inner
        .submit_put("tenant/object", b"value".to_vec(), Vec::new(), seed_sender);
    seed_receiver.recv().expect("seed GET object");
    let storage =
        CloudStorage::new_with_timeout(backend.clone(), "tenant".into(), Duration::from_millis(10));
    let deadline = crate::common::OperationDeadline::from_budget(Duration::from_secs(1));

    // Act
    let result =
        crate::storage::cloud::BlockingCloud::new(&storage, &deadline).get_optional("object");

    // Assert
    assert!(
        matches!(&result, Err(crate::common::MidgeError::Timeout(_))),
        "late queued GET must not be accepted: {result:?}"
    );
    assert_eq!(backend.reads.load(Ordering::Acquire), 1);
    assert!(!deadline.remaining().is_zero());
}

#[test]
fn should_refuse_blocking_get_submission_when_operation_deadline_is_exhausted() {
    // Arrange
    let backend = Arc::new(DelayedReadBackend {
        inner: MockCloudBackend::new(),
        reads: AtomicUsize::new(0),
    });
    let storage = CloudStorage::new(backend.clone(), "tenant".into());
    let deadline = crate::common::OperationDeadline::from_budget(Duration::ZERO);

    // Act
    let result =
        crate::storage::cloud::BlockingCloud::new(&storage, &deadline).get_optional("object");

    // Assert
    assert!(matches!(result, Err(crate::common::MidgeError::Timeout(_))));
    assert_eq!(backend.reads.load(Ordering::Acquire), 0);
}
