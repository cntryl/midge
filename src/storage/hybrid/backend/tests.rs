//! Format-neutral tests for the hybrid object backend.
//!
//! Only raw object I/O, budget admission, upload-queue and provider
//! error-shape behaviour belongs here. WAL catalog, manifest-coverage prune
//! and SST publication semantics are owned by the runtime's
//! `hybrid_persistence` module and tested beside it.

use super::*;

use crate::storage::cloud::{CloudStorage, MockCloudBackend};
use crate::storage::StorageCallback;

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use std::time::{Duration, Instant};

/// Arbitrary control-plane object key for provider error-shape fixtures. The
/// tests below classify HTTP responses, so the key only has to be stable.
#[cfg(feature = "cloud-all")]
const PROVIDER_FIXTURE_OBJECT_KEY: &str = "control/provider-error-shape.bin";

#[test]
fn should_explain_local_working_storage_when_cloud_uploads_resume() {
    // Arrange
    let (_cloud, storage) = hybrid_with_mock_cloud();
    storage.enable_ephemeral_sst_cache(1000);
    storage.reconcile_local_disk_usage(100, 200);
    storage.reconcile_startup_scratch_residue(20).unwrap();
    storage.admit_local_scratch_bytes(50).unwrap();
    storage.set_flush_headroom(200).unwrap();
    let flush = storage.reserve_for_flush_with_token(100).unwrap();
    let compaction = storage.reserve_compaction_staging_with_token(80).unwrap();

    // Act
    assert!(storage.admit_local_wal_bytes(500).is_err());
    let blocked = storage.budget_snapshot();
    storage.release_local_wal_bytes(200);
    storage.admit_local_wal_bytes(500).unwrap();
    storage.flush_completed_with_token(flush, 40);
    storage.release_local_sst_bytes(40);
    storage.compaction_aborted_with_token(compaction);
    storage.release_local_scratch_bytes(50);
    let recovered = storage.budget_snapshot();

    // Assert
    assert_eq!(blocked.usage.wal_bytes, 200);
    assert_eq!(blocked.usage.transaction_spill_bytes, 50);
    assert_eq!(blocked.usage.resident_sst_bytes, 100);
    assert_eq!(blocked.usage.startup_residue_bytes, 20);
    assert_eq!(blocked.usage.flush_staging_reserved_bytes, 100);
    assert_eq!(blocked.usage.flush_headroom_reserved_bytes, 100);
    assert_eq!(blocked.usage.compaction_staging_reserved_bytes, 80);
    assert_eq!(blocked.usage.reservations, 2);
    assert_eq!(
        blocked.blocked_admission.unwrap().operation,
        super::super::pressure::StorageAdmissionKind::Wal
    );
    assert_eq!(blocked.admission_rejections_total, 1);
    assert!(recovered.blocked_admission.is_none());
    assert_eq!(recovered.usage.flush_headroom_reserved_bytes, 200);
    assert_eq!(recovered.usage.reservations, 0);
    assert_eq!(recovered.total_committed_bytes, 820);
}

#[cfg(feature = "cloud-all")]
#[derive(Clone, Copy)]
enum ProviderErrorShape {
    S3,
    Azure,
    Gcs,
}

#[cfg(feature = "cloud-all")]
fn provider_config_for_error_shape(
    shape: ProviderErrorShape,
    endpoint: String,
) -> crate::config::CloudProviderConfig {
    match shape {
        ProviderErrorShape::S3 => crate::config::CloudProviderConfig::s3_compatible_static(
            "bucket", endpoint, "access", "secret",
        ),
        ProviderErrorShape::Azure => crate::config::CloudProviderConfig::azure_blob_shared_key(
            "account",
            "container",
            "YQ==",
        )
        .with_endpoint(endpoint)
        .expect("Azure supports endpoint overrides"),
        ProviderErrorShape::Gcs => {
            crate::config::CloudProviderConfig::gcs_bearer_token("bucket", "token")
                .with_endpoint(endpoint)
                .expect("GCS supports endpoint overrides")
        }
    }
}

#[cfg(feature = "cloud-all")]
fn provider_error_response(shape: ProviderErrorShape, status: u16) -> (u16, String, String) {
    match (shape, status) {
        (ProviderErrorShape::S3, 404) => (
            404,
            "application/xml".to_string(),
            "<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>"
                .to_string(),
        ),
        (ProviderErrorShape::S3, 412) => (
            412,
            "application/xml".to_string(),
            "<Error><Code>PreconditionFailed</Code><Message>At least one condition failed.</Message></Error>"
                .to_string(),
        ),
        (ProviderErrorShape::Azure, 404) => (
            404,
            "application/xml".to_string(),
            "<Error><Code>BlobNotFound</Code><Message>The specified blob does not exist.</Message></Error>"
                .to_string(),
        ),
        (ProviderErrorShape::Azure, 412) => (
            412,
            "application/xml".to_string(),
            "<Error><Code>ConditionNotMet</Code><Message>The condition specified was not met.</Message></Error>"
                .to_string(),
        ),
        (ProviderErrorShape::Gcs, 404) => (
            404,
            "application/json".to_string(),
            r#"{"error":{"code":404,"message":"No such object","errors":[{"reason":"notFound"}]}}"#
                .to_string(),
        ),
        (ProviderErrorShape::Gcs, 412) => (
            412,
            "application/json".to_string(),
            r#"{"error":{"code":412,"message":"At least one precondition failed","errors":[{"reason":"conditionNotMet"}]}}"#
                .to_string(),
        ),
        _ => unreachable!("provider error fixture supports only 404 and 412"),
    }
}

#[cfg(feature = "cloud-all")]
fn assert_real_provider_missing_and_cas_errors(shape: ProviderErrorShape) {
    use crate::storage::providers::test_support::spawn_scripted_http_response_server;

    let missing_server =
        spawn_scripted_http_response_server(vec![provider_error_response(shape, 404)]);
    let missing_backend = crate::storage::providers::build_cloud_backend(
        &provider_config_for_error_shape(shape, missing_server.endpoint.clone()),
    )
    .expect("build credential-free provider against local response fixture");
    let missing_cloud = Arc::new(CloudStorage::new(missing_backend, String::new()));
    let missing_dir = tempfile::tempdir().expect("create missing-response local directory");
    let missing_local = Arc::new(
        crate::storage::filesystem::FileSystem::new(missing_dir.path().join("local"))
            .expect("create missing-response local backend"),
    );
    let missing_storage = HybridStorage::with_policy(
        missing_local,
        missing_cloud,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    );

    let missing = missing_storage
        .remote_object_proof_optional(PROVIDER_FIXTURE_OBJECT_KEY)
        .expect("provider-shaped 404 must classify as confirmed missing");
    assert!(missing.is_none());
    assert_eq!(missing_server.finish(), 1);

    let cas_server = spawn_scripted_http_response_server(vec![provider_error_response(shape, 412)]);
    let cas_backend = crate::storage::providers::build_cloud_backend(
        &provider_config_for_error_shape(shape, cas_server.endpoint.clone()),
    )
    .expect("build credential-free provider against local CAS fixture");
    let cas_cloud = Arc::new(CloudStorage::new(cas_backend, String::new()));
    let cas_dir = tempfile::tempdir().expect("create CAS-response local directory");
    let cas_local = Arc::new(
        crate::storage::filesystem::FileSystem::new(cas_dir.path().join("local"))
            .expect("create CAS-response local backend"),
    );
    let cas_storage = HybridStorage::with_policy(
        cas_local,
        cas_cloud,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    );

    let cas_error = cas_storage
        .compare_exchange_remote_object(PROVIDER_FIXTURE_OBJECT_KEY, None, b"catalog".to_vec())
        .expect_err("provider-shaped 412 must classify as a CAS conflict");
    assert!(matches!(
        cas_error,
        crate::common::MidgeError::Busy(message) if {
            let message = message.to_ascii_lowercase();
            message.contains("status 412") && message.contains("precondition")
        }
    ));
    assert_eq!(cas_server.finish(), 1);
}

#[cfg(feature = "cloud-all")]
#[test]
fn should_classify_real_s3_error_shapes_given_control_object_operations() {
    assert_real_provider_missing_and_cas_errors(ProviderErrorShape::S3);
}

#[cfg(feature = "cloud-all")]
#[test]
fn should_classify_real_azure_error_shapes_given_control_object_operations() {
    assert_real_provider_missing_and_cas_errors(ProviderErrorShape::Azure);
}

#[cfg(feature = "cloud-all")]
#[test]
fn should_classify_real_gcs_error_shapes_given_control_object_operations() {
    assert_real_provider_missing_and_cas_errors(ProviderErrorShape::Gcs);
}

#[test]
fn should_classify_missing_object_errors_by_kind_not_message() {
    // Arrange: every provider's absent-object error, plus a transport failure
    // whose text mentions a missing file but which is not an absent object.
    let missing = [
        crate::storage::StorageError::not_found("S3 GET failed: HTTP 404 NoSuchKey"),
        crate::storage::StorageError::not_found("Azure Blob request failed: 404 BlobNotFound"),
        crate::storage::StorageError::not_found("read C:\\data\\missing.sst: os error 2"),
    ];
    let credential_failure = crate::storage::StorageError::new(
        crate::storage::StorageErrorKind::Transport,
        "failed to read workload token: No such file or directory",
    );

    // Act
    let all_missing = missing
        .iter()
        .all(HybridStorage::storage_error_indicates_missing);
    let credential_is_missing = HybridStorage::storage_error_indicates_missing(&credential_failure);

    // Assert
    assert!(all_missing);
    assert!(
        !credential_is_missing,
        "a transport failure is not an absent object, whatever its message says"
    );
}

#[test]
fn should_not_classify_unrelated_numeric_diagnostics_as_absent() {
    // Arrange: diagnostic text mentioning 404 is not an absent object.
    let errors = [
        crate::storage::StorageError::protocol("expected metadata length 404, got 17"),
        crate::storage::StorageError::protocol("status 503: upstream request id 404"),
    ];

    // Act
    let any_missing = errors
        .iter()
        .any(HybridStorage::storage_error_indicates_missing);

    // Assert
    assert!(!any_missing);
}

#[test]
fn should_classify_cas_conflicts_by_kind_not_message() {
    // Arrange
    let conflicts = [
        crate::storage::StorageError::precondition_failed("status 412: S3 PUT"),
        crate::storage::StorageError::precondition_failed(""),
    ];
    let unrelated = [
        crate::storage::StorageError::protocol(
            "GCS JSON PUT cannot enforce If-Match; use a generation precondition",
        ),
        crate::storage::StorageError::protocol("precondition check failed: unauthorized"),
        crate::storage::StorageError::protocol("object already exists in a retained snapshot"),
    ];

    // Act
    let all_conflicts = conflicts
        .iter()
        .all(HybridStorage::storage_error_indicates_precondition_failure);
    let any_unrelated = unrelated
        .iter()
        .any(HybridStorage::storage_error_indicates_precondition_failure);

    // Assert
    assert!(all_conflicts);
    assert!(!any_unrelated);
}

#[test]
fn should_classify_timeouts_by_kind_not_message() {
    // Arrange: provider detail text is untrusted diagnostic content. Merely
    // mentioning "timeout" must not turn an error into a Timeout.
    let timeout = crate::storage::StorageError::timeout("cloud HEAD callback expired");
    let unrelated = [
        crate::storage::StorageError::new(
            crate::storage::StorageErrorKind::Unauthorized,
            "workload token for timeout.example was rejected",
        ),
        crate::storage::StorageError::protocol("timeout must be a positive integer"),
        crate::storage::StorageError::protocol("response included x-timeout metadata"),
    ];

    // Act
    let classified_timeout = HybridStorage::storage_error_indicates_timeout(&timeout);
    let any_unrelated = unrelated
        .iter()
        .any(HybridStorage::storage_error_indicates_timeout);

    // Assert
    assert!(classified_timeout);
    assert!(!any_unrelated);
}

#[test]
fn should_retain_terminal_upload_completion_when_transient_queue_is_saturated() {
    // Arrange
    let mut queue = BoundedEventQueue::new(1, std::mem::size_of::<StorageEvent>() * 2);
    queue
        .try_push(StorageEvent::BackpressureOn, false)
        .expect("fill transient event capacity");

    // Act
    let result = queue.try_push(
        StorageEvent::CloudAck {
            segment_id: 17,
            max_sequence: 29,
        },
        false,
    );

    // Assert
    assert!(
        result.is_ok(),
        "terminal completion was dropped: {result:?}"
    );
    assert!(queue.drain().iter().any(|queued| {
        matches!(
            queued.event,
            StorageEvent::CloudAck {
                segment_id: 17,
                max_sequence: 29
            }
        )
    }));
}

#[test]
fn should_create_remote_object_when_cas_key_is_missing() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let key = "metadata/ddl-manifest.json";
    let data = br#"{"epoch":1}"#.to_vec();
    assert!(storage
        .remote_object_proof_optional(key)
        .expect("check missing CAS key")
        .is_none());

    // Act
    let proof = storage
        .compare_exchange_remote_object(key, None, data.clone())
        .expect("conditionally create remote object");

    // Assert
    assert_eq!(proof.bytes(), data);
    assert_eq!(
        storage
            .remote_object_proof_optional(key)
            .expect("read created CAS key")
            .expect("created CAS key must exist")
            .bytes(),
        proof.bytes()
    );
}

#[test]
fn should_report_remote_cas_as_not_committed_when_identity_is_stale() {
    // Arrange: a lost conditional write proves the mutation never applied,
    // and callers must learn that from the outcome, not the message text.
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let key = "metadata/ddl-not-committed.json";
    let original = storage
        .compare_exchange_remote_object(key, None, b"epoch-1".to_vec())
        .expect("create initial remote object");
    storage
        .compare_exchange_remote_object(key, Some(original.metadata()), b"epoch-2".to_vec())
        .expect("advance remote object");

    // Act
    let failure = storage
        .compare_exchange_remote_object_phased(
            key,
            Some(original.metadata()),
            b"stale".to_vec(),
            &crate::common::OperationDeadline::unbounded(),
        )
        .expect_err("stale identity must lose provider CAS");

    // Assert
    assert!(
        !failure.may_have_committed,
        "a rejected precondition proves the mutation did not commit"
    );
    assert!(matches!(failure.error, crate::common::MidgeError::Busy(_)));
}

#[test]
fn should_reject_remote_cas_when_identity_is_stale() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let key = "metadata/ddl-manifest.json";
    let original = storage
        .compare_exchange_remote_object(key, None, b"epoch-1".to_vec())
        .expect("create initial remote object");
    storage
        .compare_exchange_remote_object(key, Some(original.metadata()), b"epoch-2".to_vec())
        .expect("advance remote object");

    // Act
    let error = storage
        .compare_exchange_remote_object(key, Some(original.metadata()), b"stale".to_vec())
        .expect_err("stale identity must lose provider CAS");

    // Assert
    assert!(matches!(error, crate::common::MidgeError::Busy(_)));
    assert_eq!(
        storage
            .remote_object_proof(key)
            .expect("read winning CAS bytes")
            .bytes(),
        b"epoch-2"
    );
}

#[test]
fn should_reject_guarded_delete_when_worker_capacity_is_exhausted() {
    // Arrange
    let tmp = tempfile::tempdir().expect("create guarded-delete test dir");
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(tmp.path().join("local"))
            .expect("create local backend"),
    );
    let cloud = Arc::new(NeverCompletesBackend::default());
    let limits = HybridQueueLimits {
        prune_workers: 1,
        prune_requests: 1,
        callback_timeout: Duration::from_millis(100),
        ..HybridQueueLimits::default()
    };
    let storage = HybridStorage::with_policy_event_sender_and_limits(
        local,
        cloud,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        None,
        limits,
    );
    let target = RemoteObjectProof {
        range_identity: false,
        key: "objects/first".to_string(),
        bytes: vec![1],
        metadata: StorageObjectMetadata {
            size: 1,
            etag: "first-etag".to_string(),
            generation: None,
        },
    };
    storage
        .delete_remote_object_guarded(1, target.clone())
        .expect("first guarded delete should occupy the worker");

    // Act
    let started = Instant::now();
    let error = storage
        .delete_remote_object_guarded(2, target)
        .expect_err("second guarded delete must be rejected at worker capacity");

    // Assert
    assert!(error.to_string().contains("workers at capacity"), "{error}");
    assert!(started.elapsed() < Duration::from_millis(50));
}

#[test]
fn should_bound_guarded_delete_batch_by_one_callback_budget() {
    // Arrange
    let tmp = tempfile::tempdir().expect("create bounded guarded-delete batch directory");
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(tmp.path().join("local"))
            .expect("create bounded guarded-delete local backend"),
    );
    let cloud = Arc::new(NeverCompletesBackend::default());
    let callback_timeout = Duration::from_millis(100);
    let limits = HybridQueueLimits {
        prune_workers: 1,
        prune_requests: 8,
        callback_timeout,
        ..HybridQueueLimits::default()
    };
    let storage = HybridStorage::with_policy_event_sender_and_limits(
        local,
        cloud.clone(),
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        None,
        limits,
    );
    let targets = (1..=8)
        .map(|request_id| {
            (
                request_id,
                RemoteObjectProof {
                    range_identity: false,
                    key: format!("wal/bounded-batch-{request_id}"),
                    bytes: vec![u8::try_from(request_id).expect("request id fits in u8")],
                    metadata: StorageObjectMetadata {
                        size: 1,
                        etag: format!("bounded-etag-{request_id}"),
                        generation: None,
                    },
                },
            )
        })
        .collect();
    storage
        .delete_remote_objects_guarded(targets)
        .expect("admit bounded guarded-delete batch");

    // Act
    let started = Instant::now();
    storage.shutdown_background_workers();
    let elapsed = started.elapsed();

    // Assert
    assert_eq!(
        cloud.callbacks.lock().len(),
        1,
        "an exhausted batch budget must not submit another provider callback"
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "guarded-delete batch multiplied its callback budget: {elapsed:?}"
    );
}

fn hybrid_with_mock_cloud() -> (Arc<MockCloudBackend>, HybridStorage) {
    let tmp = tempfile::tempdir().expect("create hybrid storage test dir");
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(tmp.path().join("local"))
            .expect("create local backend"),
    );
    let mock_cloud = Arc::new(MockCloudBackend::new());
    let cloud = Arc::new(CloudStorage::new(
        mock_cloud.clone(),
        "hybrid-test".to_string(),
    ));
    let storage = HybridStorage::with_policy(
        local,
        cloud,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    );
    (mock_cloud, storage)
}

#[derive(Default)]
struct NeverCompletesBackend {
    callbacks: Mutex<Vec<StorageCallback>>,
}

struct BudgetConsumingProofBackend {
    first_head_delay: Duration,
    head_calls: AtomicUsize,
    retained_callbacks: Mutex<Vec<StorageCallback>>,
    retained_metadata_callbacks: Mutex<Vec<crate::storage::MetadataReadCallback>>,
}

impl BudgetConsumingProofBackend {
    fn new(first_head_delay: Duration) -> Self {
        Self {
            first_head_delay,
            head_calls: AtomicUsize::new(0),
            retained_callbacks: Mutex::new(Vec::new()),
            retained_metadata_callbacks: Mutex::new(Vec::new()),
        }
    }

    fn retain_callback(&self, callback: StorageCallback) {
        self.retained_callbacks.lock().push(callback);
    }
}

impl StorageBackend for BudgetConsumingProofBackend {
    fn submit_range_read_request(
        &self,
        request: crate::storage::StorageRequest,
        range: std::ops::Range<u64>,
        callback: crate::storage::RangeReadCallback,
    ) {
        crate::storage::test_support::forward_typed_range_read_to_legacy(
            self, request, range, callback,
        );
    }

    fn submit_range_head_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::test_support::forward_typed_range_head_to_legacy(self, request, callback);
    }

    fn submit_metadata_read_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::MetadataReadCallback,
    ) {
        crate::storage::dispatch_metadata_read_request(request, callback, |_, _, callback| {
            self.retained_metadata_callbacks.lock().push(callback);
        });
    }

    fn submit_head_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::test_support::forward_typed_head_to_legacy(self, request, callback);
    }

    fn submit_delete_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::test_support::forward_typed_delete_to_legacy(self, request, callback);
    }

    fn submit_write_request(
        &self,
        request: crate::storage::StorageRequest,
        data: Vec<u8>,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::test_support::forward_typed_write_to_legacy(self, request, data, callback);
    }

    fn submit_write(&self, key: &str, _data: Vec<u8>, callback: StorageCallback) {
        let _ = callback.send(StorageEvent::WriteComplete {
            key: key.to_string(),
            result: StorageOutcome::Err(
                "writes are not used by this proof fixture"
                    .to_string()
                    .into(),
            ),
        });
    }

    fn submit_delete(&self, key: &str, callback: StorageCallback) {
        let _ = callback.send(StorageEvent::DeleteComplete {
            key: key.to_string(),
            result: StorageOutcome::Err(
                "deletes are not used by this proof fixture"
                    .to_string()
                    .into(),
            ),
        });
    }

    fn submit_head(&self, key: &str, callback: StorageCallback) {
        if self.head_calls.fetch_add(1, Ordering::SeqCst) == 0 {
            let delay = self.first_head_delay;
            let key = key.to_string();
            std::thread::spawn(move || {
                std::thread::sleep(delay);
                let _ = callback.send(StorageEvent::HeadComplete {
                    key,
                    result: StorageOutcome::Ok(StorageObjectMetadata {
                        size: 7,
                        etag: "slow-first-head".to_string(),
                        generation: None,
                    }),
                });
            });
        } else {
            self.retain_callback(callback);
        }
    }
}

impl NeverCompletesBackend {
    fn retain_callback(&self, callback: StorageCallback) {
        self.callbacks.lock().push(callback);
    }
}

impl StorageBackend for NeverCompletesBackend {
    fn submit_range_read_request(
        &self,
        request: crate::storage::StorageRequest,
        range: std::ops::Range<u64>,
        callback: crate::storage::RangeReadCallback,
    ) {
        crate::storage::test_support::forward_typed_range_read_to_legacy(
            self, request, range, callback,
        );
    }

    fn submit_range_head_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::test_support::forward_typed_range_head_to_legacy(self, request, callback);
    }

    fn submit_metadata_read_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::MetadataReadCallback,
    ) {
        let _ = (request, callback);
        panic!("test backend received undeclared metadata-read capability");
    }

    fn submit_head_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::test_support::forward_typed_head_to_legacy(self, request, callback);
    }

    fn submit_delete_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::test_support::forward_typed_delete_to_legacy(self, request, callback);
    }

    fn submit_write_request(
        &self,
        request: crate::storage::StorageRequest,
        data: Vec<u8>,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::test_support::forward_typed_write_to_legacy(self, request, data, callback);
    }

    fn submit_write(&self, _key: &str, _data: Vec<u8>, callback: StorageCallback) {
        self.retain_callback(callback);
    }

    fn submit_write_with_headers(
        &self,
        _key: &str,
        _data: Vec<u8>,
        _headers: Vec<(String, String)>,
        callback: StorageCallback,
    ) {
        self.retain_callback(callback);
    }

    fn submit_delete(&self, _key: &str, callback: StorageCallback) {
        self.retain_callback(callback);
    }

    fn submit_delete_with_headers(
        &self,
        _key: &str,
        _headers: Vec<(String, String)>,
        callback: StorageCallback,
    ) {
        self.retain_callback(callback);
    }

    fn submit_head(&self, _key: &str, callback: StorageCallback) {
        self.retain_callback(callback);
    }
}

fn write_cloud_object(storage: &HybridStorage, key: &str, data: Vec<u8>) {
    let (tx, rx) = std::sync::mpsc::channel();
    storage.stores.sst.submit_write(key, data, tx);
    match rx.recv_timeout(Duration::from_secs(1)) {
        Ok(StorageEvent::WriteComplete {
            result: StorageOutcome::Ok(()),
            ..
        }) => {}
        other => panic!("cloud write for '{key}' failed: {other:?}"),
    }
}

#[test]
fn should_enforce_internal_storage_event_queue_limits() {
    // Arrange
    let ack = StorageEvent::CloudAck {
        segment_id: 1,
        max_sequence: 1,
    };
    let second_ack = StorageEvent::CloudAck {
        segment_id: 2,
        max_sequence: 2,
    };
    let ack_bytes = BoundedEventQueue::event_bytes(&ack);
    let mut entry_limited = BoundedEventQueue::new(1, ack_bytes * 2);
    let mut byte_limited = BoundedEventQueue::new(2, ack_bytes);

    // Act
    entry_limited
        .try_push(ack.clone(), false)
        .expect("first event fits entry bound");
    let entry_error = entry_limited
        .try_push(second_ack.clone(), false)
        .expect_err("second event exceeds entry bound");
    byte_limited
        .try_push(ack.clone(), false)
        .expect("first event fits byte bound");
    let byte_error = byte_limited
        .try_push(second_ack, false)
        .expect_err("second event exceeds byte bound");

    // Assert
    assert!(entry_error.to_string().contains("entries=1/1"));
    assert!(byte_error.to_string().contains("bytes="));
    // Drain through the same accessor callers use to confirm the rejected
    // second event never made it into the queue: only the first `ack`
    // survived for each queue.
    let entry_limited_drained = entry_limited.drain();
    assert_eq!(entry_limited_drained.len(), 1);
    assert!(matches!(
        entry_limited_drained[0].event,
        StorageEvent::CloudAck { segment_id: 1, .. }
    ));
    let byte_limited_drained = byte_limited.drain();
    assert_eq!(byte_limited_drained.len(), 1);
    assert!(matches!(
        byte_limited_drained[0].event,
        StorageEvent::CloudAck { segment_id: 1, .. }
    ));
}

#[test]
fn should_fail_with_timeout_given_expired_deadline_when_reading_object_proof() {
    // Arrange: a zero-budget caller and a backend that records every submitted
    // callback. Returning Timeout is insufficient if a provider call starts.
    let tmp = tempfile::tempdir().expect("create expired deadline directory");
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(tmp.path().join("local"))
            .expect("create expired deadline local backend"),
    );
    let cloud = Arc::new(NeverCompletesBackend::default());
    let storage = HybridStorage::with_policy(
        local,
        cloud.clone(),
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    );
    let expired = crate::common::OperationDeadline::from_budget(Duration::ZERO);

    // Act
    let result = storage.remote_object_proof_within("sst/expired.sst", &expired);

    // Assert
    let error = result.expect_err("an exhausted budget must not start another cloud round trip");
    assert!(matches!(
        error,
        crate::common::MidgeError::Timeout(message)
            if message.contains("deadline") && message.contains("sst/expired.sst")
    ));
    assert!(
        cloud.callbacks.lock().is_empty(),
        "an exhausted deadline must not submit a zero-timeout provider call"
    );
}

#[test]
fn should_not_submit_conditional_delete_given_expired_deadline() {
    // Arrange
    let tmp = tempfile::tempdir().expect("create expired delete deadline directory");
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(tmp.path().join("local"))
            .expect("create expired delete deadline local backend"),
    );
    let cloud = Arc::new(NeverCompletesBackend::default());
    let storage = HybridStorage::with_policy(
        local,
        cloud.clone(),
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    );
    let target = RemoteObjectProof {
        range_identity: false,
        key: "wal/expired-delete".to_string(),
        bytes: vec![1],
        metadata: StorageObjectMetadata {
            size: 1,
            etag: "expired-delete-etag".to_string(),
            generation: None,
        },
    };
    let expired = crate::common::OperationDeadline::from_budget(Duration::ZERO);

    // Act
    let result = storage.delete_remote_object_guarded_blocking_within(&target, &expired);

    // Assert
    let error = result.expect_err("an exhausted budget must not submit a conditional delete");
    assert!(matches!(
        error,
        crate::common::MidgeError::Timeout(message)
            if message.contains("deadline") && message.contains("wal/expired-delete")
    ));
    assert!(
        cloud.callbacks.lock().is_empty(),
        "an exhausted deadline must not mutate the provider"
    );
}

#[test]
fn should_not_submit_provider_call_given_configured_callback_timeout_is_zero() {
    // Arrange: the aggregate deadline is live, but configuration allows no
    // time for even the first provider round trip.
    let tmp = tempfile::tempdir().expect("create zero callback timeout directory");
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(tmp.path().join("local"))
            .expect("create zero callback timeout local backend"),
    );
    let cloud = Arc::new(NeverCompletesBackend::default());
    let limits = HybridQueueLimits {
        callback_timeout: Duration::ZERO,
        ..HybridQueueLimits::default()
    };
    let storage = HybridStorage::with_policy_event_sender_and_limits(
        local,
        cloud.clone(),
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        None,
        limits,
    );

    // Act
    let result = storage.remote_object_proof_within(
        "sst/zero-timeout.sst",
        &crate::common::OperationDeadline::unbounded(),
    );

    // Assert
    assert!(matches!(result, Err(crate::common::MidgeError::Timeout(_))));
    assert!(
        cloud.callbacks.lock().is_empty(),
        "a zero per-operation budget must not reach the provider"
    );
}

#[test]
fn should_read_object_proof_given_ample_deadline_when_budget_remains() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    write_cloud_object(&storage, "sst/ample.sst", b"payload".to_vec());
    let deadline = crate::common::OperationDeadline::from_budget(Duration::from_mins(1));

    // Act
    let proof = storage
        .remote_object_proof_within("sst/ample.sst", &deadline)
        .expect("ample budget reads the object");

    // Assert
    assert_eq!(proof.bytes(), b"payload");
}

#[test]
fn should_recompute_remaining_budget_before_each_round_trip_when_reading_object_proof() {
    // Arrange: HEAD consumes most of the shared budget, then GET never answers.
    // Reusing the allowance calculated before HEAD would refund that elapsed
    // time and make the proof run well beyond the advertised deadline. The
    // two-second callback budget leaves a full second of scheduler headroom.
    let tmp = tempfile::tempdir().expect("create deadline proof directory");
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(tmp.path().join("local"))
            .expect("create deadline proof local backend"),
    );
    let cloud = Arc::new(BudgetConsumingProofBackend::new(Duration::from_millis(300)));
    let limits = HybridQueueLimits {
        callback_timeout: Duration::from_secs(2),
        ..HybridQueueLimits::default()
    };
    let storage = HybridStorage::with_policy_event_sender_and_limits(
        local,
        cloud,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        None,
        limits,
    );
    let deadline = crate::common::OperationDeadline::from_budget(Duration::from_millis(500));

    // Act
    let started = Instant::now();
    let result = storage.remote_object_proof_within("sst/slow-proof.sst", &deadline);
    let elapsed = started.elapsed();

    // Assert
    assert!(result.is_err(), "the never-completing GET must time out");
    assert!(
        elapsed < Duration::from_secs(1),
        "proof reused the pre-HEAD allowance and exceeded its shared budget: {elapsed:?}"
    );
}

#[cfg(feature = "failpoints")]
#[test]
fn should_emit_one_upload_terminal_event_when_wal_ack_logging_panics() {
    // Arrange
    let _test_guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let tmp = tempfile::tempdir().expect("create WAL post-ack panic test dir");
    let segment_id = 10;
    let max_sequence = 14;
    let wal_path = tmp.path().join("upload-payload.bin");
    let wal_bytes = b"upload-payload".to_vec();
    std::fs::write(&wal_path, &wal_bytes).expect("write local WAL");
    let upload = UploadState {
        segment_id,
        object_key: "wal/epochs/upload-ack-panic.bin".to_string(),
        local_path: wal_path,
        status: UploadStatus::InFlight {
            started_at: Instant::now(),
        },
        max_sequence,
        retries: 0,
        size_bytes: wal_bytes.len() as u64,
    };
    let (external_event_tx, external_event_rx) = cb::bounded(4);
    fail::cfg("midge::cloud::in_wal_upload_ack_log", "panic")
        .expect("configure WAL ack logging panic");

    // Act
    HybridStorage::process_wal_upload_attempt(
        storage.counters(),
        &upload,
        &storage.stores.wal,
        &storage.event_queue,
        Some(&external_event_tx),
        storage.callback_timeout,
    );
    let terminal_events = external_event_rx.try_iter().collect::<Vec<_>>();
    fail::remove("midge::cloud::in_wal_upload_ack_log");
    scenario.teardown();

    // Assert
    assert_eq!(
        terminal_events.len(),
        1,
        "one upload attempt must publish exactly one terminal event: {terminal_events:?}"
    );
    assert!(matches!(
        terminal_events.as_slice(),
        [StorageEvent::CloudFail {
            segment_id: failed_segment,
            terminal: false,
            failure_kind: crate::storage::CloudUploadFailureKind::Other,
            ..
        }] if *failed_segment == segment_id
    ));
}
