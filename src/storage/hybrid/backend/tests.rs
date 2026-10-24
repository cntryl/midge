//! Format-neutral tests for the hybrid object backend.
//!
//! Only raw object I/O, budget admission, upload-queue and provider
//! error-shape behaviour belongs here. WAL catalog, manifest-coverage prune
//! and SST publication semantics are owned by the runtime's
//! `hybrid_persistence` module and tested beside it.

use super::*;
use crate::storage::test_support::StorageBackendTestExt;

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
    let missing_storage = HybridStorage::with_policy(
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
    let cas_storage = HybridStorage::with_policy(
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
fn should_reject_conditional_delete_when_target_capacity_is_exhausted() {
    // Arrange
    let cloud = Arc::new(NeverCompletesBackend::default());
    let limits = HybridQueueLimits {
        prune_workers: 1,
        prune_requests: 1,
        callback_timeout: Duration::from_millis(100),
        ..HybridQueueLimits::default()
    };
    let storage = HybridStorage::with_policy_event_sender_and_limits(
        cloud,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        None,
        limits,
    );
    let target = StorageObjectMetadata {
        size: 1,
        etag: "first-etag".to_string(),
        generation: None,
    };
    storage
        .delete_remote_objects_by_identity(vec![(1, "objects/first".into(), target.clone())])
        .expect("first guarded delete should occupy the worker");

    // Act
    let started = Instant::now();
    let error = storage
        .delete_remote_objects_by_identity(vec![(2, "objects/first".into(), target)])
        .expect_err("second guarded delete must be rejected at worker capacity");

    // Assert
    assert!(error.to_string().contains("queue is full"), "{error}");
    assert!(started.elapsed() < Duration::from_millis(50));
}

#[test]
fn should_bound_conditional_delete_batch_by_one_callback_budget() {
    // Arrange
    let cloud = Arc::new(NeverCompletesBackend::default());
    let callback_timeout = Duration::from_millis(100);
    let limits = HybridQueueLimits {
        prune_workers: 1,
        prune_requests: 8,
        callback_timeout,
        ..HybridQueueLimits::default()
    };
    let storage = HybridStorage::with_policy_event_sender_and_limits(
        cloud.clone(),
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        None,
        limits,
    );
    let targets = (1..=8)
        .map(|request_id| {
            (
                request_id,
                format!("wal/bounded-batch-{request_id}"),
                StorageObjectMetadata {
                    size: 1,
                    etag: format!("bounded-etag-{request_id}"),
                    generation: None,
                },
            )
        })
        .collect();
    storage
        .delete_remote_objects_by_identity(targets)
        .expect("admit bounded conditional-delete batch");

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
    let mock_cloud = Arc::new(MockCloudBackend::new());
    let cloud = Arc::new(CloudStorage::new(
        mock_cloud.clone(),
        "hybrid-test".to_string(),
    ));
    let storage = HybridStorage::with_policy(
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
        let _ = (request, range, callback);
        panic!("test backend received undeclared range-read capability");
    }

    fn submit_range_head_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        let _ = (request, callback);
        panic!("test backend received undeclared range HEAD capability");
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
        crate::storage::dispatch_head_request(request, callback, |key, _, callback| {
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
        });
    }

    fn submit_delete_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::test_support::dispatch_test_delete_request(
            request,
            callback,
            |key, _precondition, callback| {
                let _ = callback.send(StorageEvent::DeleteComplete {
                    key: key.to_string(),
                    result: StorageOutcome::Err(
                        "deletes are not used by this proof fixture"
                            .to_string()
                            .into(),
                    ),
                });
            },
        );
    }

    fn submit_write_request(
        &self,
        request: crate::storage::StorageRequest,
        data: Vec<u8>,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::test_support::dispatch_test_write_request(
            request,
            data,
            callback,
            |key, _precondition, _data, callback| {
                let _ = callback.send(StorageEvent::WriteComplete {
                    key: key.to_string(),
                    result: StorageOutcome::Err(
                        "writes are not used by this proof fixture"
                            .to_string()
                            .into(),
                    ),
                });
            },
        );
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
        let _ = (request, range, callback);
        panic!("test backend received undeclared range-read capability");
    }

    fn submit_range_head_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        let _ = request;
        self.retain_callback(callback);
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
        crate::storage::dispatch_head_request(request, callback, |_key, _, callback| {
            self.retain_callback(callback);
        });
    }

    fn submit_delete_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::test_support::dispatch_test_delete_request(
            request,
            callback,
            |_key, _precondition, callback| {
                self.retain_callback(callback);
            },
        );
    }

    fn submit_write_request(
        &self,
        request: crate::storage::StorageRequest,
        data: Vec<u8>,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::test_support::dispatch_test_write_request(
            request,
            data,
            callback,
            |_key, _precondition, _data, callback| {
                self.retain_callback(callback);
            },
        );
    }
}

fn write_cloud_object(storage: &HybridStorage, key: &str, data: Vec<u8>) {
    let (tx, rx) = std::sync::mpsc::channel();
    storage.stores.sst.write_for_test(key, data, tx);
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
    let cloud = Arc::new(NeverCompletesBackend::default());
    let storage = HybridStorage::with_policy(
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
    let cloud = Arc::new(NeverCompletesBackend::default());
    let storage = HybridStorage::with_policy(
        cloud.clone(),
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    );
    let key = "wal/expired-delete";
    let identity = StorageObjectMetadata {
        size: 1,
        etag: "expired-delete-etag".into(),
        generation: None,
    };
    let expired = crate::common::OperationDeadline::from_budget(Duration::ZERO);

    // Act
    let result = storage.delete_remote_object_by_identity_blocking_within(key, &identity, &expired);

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
    let cloud = Arc::new(NeverCompletesBackend::default());
    let limits = HybridQueueLimits {
        callback_timeout: Duration::ZERO,
        ..HybridQueueLimits::default()
    };
    let storage = HybridStorage::with_policy_event_sender_and_limits(
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
    let cloud = Arc::new(BudgetConsumingProofBackend::new(Duration::from_millis(300)));
    let limits = HybridQueueLimits {
        callback_timeout: Duration::from_secs(2),
        ..HybridQueueLimits::default()
    };
    let storage = HybridStorage::with_policy_event_sender_and_limits(
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

#[test]
fn should_confirm_filesystem_wal_upload_when_range_identity_is_unchanged() -> MidgeResult<()> {
    // Arrange: FileSystem has no provider receipt, and ordinary HEAD returns
    // a content checksum while range HEAD returns the pinned file identity.
    let local = tempfile::tempdir()?;
    let remote = tempfile::tempdir()?;
    let bytes = vec![7; 130 * 1024];
    let path = local.path().join("segment.wal");
    std::fs::write(&path, &bytes)?;
    let cloud: Arc<dyn StorageBackend> =
        Arc::new(crate::storage::filesystem::FileSystem::new(remote.path())?);
    let storage = HybridStorage::with_policy(cloud, policy::StorageBudgetPolicy::default());
    let upload = UploadState {
        segment_id: 10,
        object_key: "wal/epochs/7/segment.wal".into(),
        source: Arc::new(crate::io::FsLocation::host_file(&path)),
        status: UploadStatus::InFlight {
            started_at: Instant::now(),
        },
        max_sequence: 14,
        retries: 0,
        size_bytes: u64::try_from(bytes.len()).unwrap(),
    };
    let (terminal, events) = cb::bounded(4);
    let mut identities = Vec::new();

    // Act: the second attempt reconciles an existing immutable object.
    for _ in 0..2 {
        HybridStorage::process_wal_upload_attempt(
            storage.counters(),
            &upload,
            &storage.stores.wal,
            &storage.event_queue,
            Some(&terminal),
            &super::uploads::UploadAttemptAdmission {
                timeout: Duration::from_secs(2),
                memory: None,
            },
            &storage.verified_uploads,
        );
        identities.push(
            HybridStorage::head_range_object_from_backend(
                &storage.stores.wal,
                &upload.object_key,
                Duration::from_secs(2),
            )
            .expect("published WAL range identity"),
        );
    }
    let results = events.try_iter().collect::<Vec<_>>();

    // Assert: complete byte verification grants both ACKs without replacing
    // the immutable remote object or losing the recoverable local source.
    assert_eq!(results.len(), 2, "{results:?}");
    assert!(
        results.iter().all(|event| matches!(
            event,
            StorageEvent::CloudAck {
                segment_id: 10,
                max_sequence: 14,
            }
        )),
        "{results:?}"
    );
    assert_eq!(
        std::fs::read(remote.path().join(&upload.object_key))?,
        bytes
    );
    assert!(identities[0].same_version(&identities[1]));
    assert_eq!(std::fs::read(path)?, bytes);
    Ok(())
}

#[cfg(feature = "failpoints")]
#[test]
fn should_complete_wal_upload_when_spawner_owns_failpoint_gate() -> MidgeResult<()> {
    // Arrange: the real worker must finish while its test owner holds the
    // global write gate, exactly as an engine's failpoint fixture does.
    let local = tempfile::tempdir()?;
    let remote = tempfile::tempdir()?;
    let bytes = b"owned upload";
    let path = local.path().join("segment.wal");
    std::fs::write(&path, bytes)?;
    let cloud = Arc::new(crate::storage::filesystem::FileSystem::new(remote.path())?);
    let (terminal, events) = cb::bounded(4);
    let guard = crate::failpoints::test_failpoint_guard();
    let storage = HybridStorage::with_policy_event_sender_and_limits(
        cloud,
        policy::StorageBudgetPolicy::default(),
        Some(terminal),
        HybridQueueLimits {
            worker_concurrency: 1,
            callback_timeout: Duration::from_secs(2),
            ..HybridQueueLimits::default()
        },
    );

    // Act: release the guard only to clean up a red run's blocked worker.
    let queued = storage.enqueue_object_upload(
        42,
        "wal/owned".into(),
        crate::io::FsLocation::host_file(&path),
        43,
    );
    storage.process_uploads();
    let completed = events.recv_timeout(Duration::from_secs(2));
    if completed.is_ok() {
        drop(storage);
        drop(guard);
    } else {
        drop(guard);
        drop(storage);
    }

    // Assert: actual immutable upload and byte proof completed before release.
    assert!(queued.is_ok(), "{queued:?}");
    assert!(
        matches!(
            completed,
            Ok(StorageEvent::CloudAck {
                segment_id: 42,
                max_sequence: 43,
            })
        ),
        "{completed:?}"
    );
    assert_eq!(std::fs::read(remote.path().join("wal/owned"))?, bytes);
    Ok(())
}

#[cfg(feature = "failpoints")]
#[test]
fn should_complete_conditional_delete_when_spawner_owns_failpoint_gate() -> MidgeResult<()> {
    // Arrange: capture the real immutable file's exact range identity.
    let remote = tempfile::tempdir()?;
    let key = "retired.wal";
    let path = remote.path().join(key);
    std::fs::write(&path, b"retired")?;
    let cloud: Arc<dyn StorageBackend> =
        Arc::new(crate::storage::filesystem::FileSystem::new(remote.path())?);
    let identity =
        HybridStorage::head_range_object_from_backend(&cloud, key, Duration::from_secs(2))
            .expect("retired file range identity");
    let (terminal, events) = cb::bounded(4);
    let storage = HybridStorage::with_policy_event_sender_and_limits(
        cloud,
        policy::StorageBudgetPolicy::default(),
        Some(terminal),
        HybridQueueLimits::default(),
    );
    let guard = crate::failpoints::test_failpoint_guard();

    // Act: the physical deletion must settle under the current owner.
    let queued = storage.delete_remote_objects_by_identity(vec![(42, key.into(), identity)]);
    storage.process_uploads();
    let completed = events.recv_timeout(Duration::from_secs(2));
    if completed.is_ok() {
        drop(storage);
        drop(guard);
    } else {
        drop(guard);
        drop(storage);
    }

    // Assert: red cleanup releases the guard before join, but cannot turn a
    // late actual deletion into an on-time terminal completion.
    assert!(queued.is_ok(), "{queued:?}");
    assert!(
        matches!(
            completed,
            Ok(StorageEvent::CloudWalPruneComplete {
                segment_id: 42,
                result: StorageOutcome::Ok(()),
            })
        ),
        "{completed:?}"
    );
    assert!(!path.exists());
    Ok(())
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
        source: Arc::new(crate::io::FsLocation::host_file(&wal_path)),
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
        &super::uploads::UploadAttemptAdmission {
            timeout: storage.callback_timeout,
            memory: None,
        },
        &storage.verified_uploads,
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
            failures: 1,
            failure_kind: crate::storage::CloudUploadFailureKind::Other,
            ..
        }] if *failed_segment == segment_id
    ));
}

#[test]
fn should_double_wal_upload_retry_delay_until_its_cap_when_failures_repeat() {
    // Arrange
    let retry = WalUploadRetryPolicy::default();
    let failures = [1, 2, 3, 4, 5, 6, 40, u32::MAX];

    // Act
    let delays = failures.map(|failures| retry.delay_after(failures));

    // Assert
    let millis = |millis| Duration::from_millis(millis);
    assert_eq!(
        delays,
        [
            millis(50),
            millis(100),
            millis(200),
            millis(400),
            millis(800),
            millis(1000),
            millis(1000),
            millis(1000),
        ]
    );
}

/// A queued upload of segment `segment_id` in `status`, after `retries`
/// failed attempts.
fn queued_upload(
    source: &Arc<crate::io::FsLocation>,
    segment_id: u64,
    status: UploadStatus,
    retries: u32,
) -> UploadState {
    UploadState {
        segment_id,
        object_key: format!("wal/{segment_id}.wal"),
        source: Arc::clone(source),
        status,
        max_sequence: segment_id,
        retries,
        size_bytes: 1,
    }
}

#[test]
fn should_schedule_next_attempt_by_retry_policy_when_an_upload_fails() {
    // Arrange
    let tmp = tempfile::tempdir().expect("create backoff test dir");
    let source = Arc::new(crate::io::FsLocation::host_file(
        &tmp.path().join("backoff.wal"),
    ));
    let retry = WalUploadRetryPolicy::default();
    let now = Instant::now();
    let mut upload = queued_upload(&source, 1, UploadStatus::Pending, 2);

    // Act
    upload.record_failure("provider unavailable".to_string(), now, retry);

    // Assert
    assert_eq!(upload.retries, 3);
    assert_eq!(
        upload.status,
        UploadStatus::Failed {
            error: "provider unavailable".to_string(),
            retry_at: now + retry.delay_after(3),
        }
    );
    assert!(!upload.is_due(now));
    assert!(upload.is_due(now + retry.delay_after(3)));
    assert!(!upload.is_finished());
}

#[test]
fn should_wait_for_earliest_retry_when_only_failed_uploads_are_queued() {
    // Arrange
    let tmp = tempfile::tempdir().expect("create backoff test dir");
    let source = Arc::new(crate::io::FsLocation::host_file(
        &tmp.path().join("backoff.wal"),
    ));
    let now = Instant::now();
    let poll = Duration::from_millis(1);
    let failed = |segment_id, retry_in: Duration| {
        queued_upload(
            &source,
            segment_id,
            UploadStatus::Failed {
                error: "provider unavailable".to_string(),
                retry_at: now + retry_in,
            },
            4,
        )
    };
    let mut queue = UploadQueue::new(8, 1024, WalUploadRetryPolicy::default());
    queue
        .try_push(failed(1, Duration::from_millis(300)))
        .unwrap();
    queue
        .try_push(failed(2, Duration::from_millis(200)))
        .unwrap();
    let empty = UploadQueue::new(8, 1024, WalUploadRetryPolicy::default());

    // Act
    let backed_off = queue.next_turn(now, poll);
    queue.entries.front_mut().expect("queued upload").status = UploadStatus::Pending;
    let pending = queue.next_turn(now, poll);

    // Assert
    assert_eq!(backed_off, Some(Duration::from_millis(200)));
    assert_eq!(pending, Some(poll));
    assert_eq!(empty.next_turn(now, poll), None);
}

#[test]
fn should_wait_at_least_one_poll_when_failed_uploads_are_overdue() {
    // Arrange: retries already due, which a full worker channel can keep
    // from starting.
    let tmp = tempfile::tempdir().expect("create overdue retry test dir");
    let source = Arc::new(crate::io::FsLocation::host_file(
        &tmp.path().join("overdue.wal"),
    ));
    let now = Instant::now();
    let poll = Duration::from_millis(5);
    let overdue = |segment_id| {
        queued_upload(
            &source,
            segment_id,
            UploadStatus::Failed {
                error: "provider unavailable".to_string(),
                retry_at: now.checked_sub(Duration::from_secs(1)).unwrap_or(now),
            },
            2,
        )
    };
    let mut queue = UploadQueue::new(8, 1024, WalUploadRetryPolicy::default());
    queue.try_push(overdue(1)).unwrap();
    queue.try_push(overdue(2)).unwrap();

    // Act
    let turn = queue.next_turn(now, poll);

    // Assert
    assert_eq!(
        turn,
        Some(poll),
        "an overdue retry must not wake its caller at once"
    );
}

struct ConditionalDeleteHeadPause {
    started: mpsc::SyncSender<()>,
    release: mpsc::Receiver<()>,
}

struct FailFirstConditionalDelete {
    inner: Arc<CloudStorage>,
    attempts: AtomicUsize,
    fail_first: bool,
    head_pause: Mutex<Option<ConditionalDeleteHeadPause>>,
}

impl StorageBackend for FailFirstConditionalDelete {
    fn submit_delete_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: StorageCallback,
    ) {
        if self.attempts.fetch_add(1, Ordering::AcqRel) == 0 && self.fail_first {
            let _ = callback.send(StorageEvent::DeleteComplete {
                key: request.key,
                result: StorageOutcome::Err(crate::storage::StorageError::new(
                    crate::storage::StorageErrorKind::Transport,
                    "temporary delete outage",
                )),
            });
        } else {
            self.inner.submit_delete_request(request, callback);
        }
    }

    fn submit_range_head_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: StorageCallback,
    ) {
        if let Some(pause) = self.head_pause.lock().take() {
            let inner = self.inner.clone();
            std::thread::spawn(move || {
                pause.started.send(()).unwrap();
                pause.release.recv_timeout(Duration::from_secs(3)).unwrap();
                inner.submit_range_head_request(request, callback);
            });
        } else {
            self.inner.submit_range_head_request(request, callback);
        }
    }

    crate::storage::forward_storage_backend!(inner;
    submit_range_read_request, submit_metadata_read_request,

        submit_head_request, submit_write_request);
}

#[test]
fn should_retry_a_retired_object_with_its_original_identity_after_transport_failure() {
    // Arrange
    let mock = Arc::new(MockCloudBackend::new());
    let cloud = Arc::new(CloudStorage::new(mock, "retry-delete".into()));
    let backend = Arc::new(FailFirstConditionalDelete {
        inner: cloud,
        attempts: AtomicUsize::new(0),
        fail_first: true,
        head_pause: Mutex::new(None),
    });
    let storage =
        HybridStorage::with_policy(backend.clone(), policy::StorageBudgetPolicy::default());
    let key = "wal/retired-object";
    let original = storage
        .compare_exchange_remote_object(key, None, b"retired".to_vec())
        .unwrap();
    storage
        .delete_remote_objects_by_identity(vec![(1, key.into(), original.metadata().clone())])
        .unwrap();

    // Act: only the normal storage tick runs after the failed attempt.
    let deadline = Instant::now() + Duration::from_secs(3);
    while backend.attempts.load(Ordering::Acquire) < 2 && Instant::now() < deadline {
        storage.process_uploads();
        std::thread::sleep(Duration::from_millis(10));
    }
    storage.shutdown_background_workers();

    // Assert
    assert_eq!(backend.attempts.load(Ordering::Acquire), 2);
    assert!(storage.remote_object_proof_optional(key).unwrap().is_none());
}

#[test]
fn should_queue_a_retired_object_until_the_conditional_delete_worker_is_available() {
    // Arrange
    let mock = Arc::new(MockCloudBackend::new());
    let cloud = Arc::new(CloudStorage::new(mock, "queued-delete".into()));
    let (started_tx, started) = mpsc::sync_channel(1);
    let (release, release_rx) = mpsc::channel();
    let cloud = Arc::new(FailFirstConditionalDelete {
        inner: cloud,
        attempts: AtomicUsize::new(0),
        fail_first: false,
        head_pause: Mutex::new(None),
    });
    let base = HybridStorage::with_policy(cloud.clone(), policy::StorageBudgetPolicy::default());
    let first = base
        .compare_exchange_remote_object("wal/first", None, b"a".to_vec())
        .unwrap();
    let second = base
        .compare_exchange_remote_object("wal/second", None, b"b".to_vec())
        .unwrap();
    *cloud.head_pause.lock() = Some(ConditionalDeleteHeadPause {
        started: started_tx,
        release: release_rx,
    });
    let storage = HybridStorage::with_policy_event_sender_and_limits(
        cloud,
        policy::StorageBudgetPolicy::default(),
        None,
        HybridQueueLimits {
            prune_workers: 1,
            prune_requests: 2,
            ..HybridQueueLimits::default()
        },
    );
    storage
        .delete_remote_objects_by_identity(vec![(1, "wal/first".into(), first.metadata().clone())])
        .unwrap();

    started.recv_timeout(Duration::from_secs(1)).unwrap();

    // Act: this admission remains bounded but cannot rely on a free worker.
    storage
        .delete_remote_objects_by_identity(vec![(
            2,
            "wal/second".into(),
            second.metadata().clone(),
        )])
        .expect("a pending target must survive temporary worker exhaustion");
    release.send(()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    while storage
        .remote_object_proof_optional("wal/second")
        .unwrap()
        .is_some()
        && Instant::now() < deadline
    {
        storage.process_uploads();
        std::thread::sleep(Duration::from_millis(10));
    }
    storage.shutdown_background_workers();

    // Assert
    assert!(storage
        .remote_object_proof_optional("wal/second")
        .unwrap()
        .is_none());
}

#[test]
fn should_release_cleanup_capacity_without_deleting_when_authority_does_not_commit() {
    // Arrange
    let cloud = Arc::new(NeverCompletesBackend::default());
    let storage = HybridStorage::with_policy_event_sender_and_limits(
        cloud.clone(),
        policy::StorageBudgetPolicy::default(),
        None,
        HybridQueueLimits {
            prune_requests: 1,
            ..HybridQueueLimits::default()
        },
    );
    let target = || {
        vec![(
            1,
            "wal/reserved".into(),
            StorageObjectMetadata {
                size: 1,
                etag: "reserved".into(),
                generation: None,
            },
        )]
    };
    let admitted = storage.admit_remote_deletes_by_identity(target()).unwrap();

    // Act
    assert!(storage.admit_remote_deletes_by_identity(target()).is_err());
    drop(admitted);
    let replacement = storage.admit_remote_deletes_by_identity(target()).unwrap();
    drop(replacement);

    // Assert
    assert!(cloud.callbacks.lock().is_empty());
    assert_eq!(storage.next_upload_turn(Duration::from_millis(5)), None);
}

#[test]
fn should_retain_a_replacement_object_when_a_physical_cleanup_retry_observes_another_identity() {
    // Arrange
    let mock = Arc::new(MockCloudBackend::new());
    let cloud = Arc::new(CloudStorage::new(mock, "replacement-delete".into()));
    let backend = Arc::new(FailFirstConditionalDelete {
        inner: cloud,
        attempts: AtomicUsize::new(0),
        fail_first: true,
        head_pause: Mutex::new(None),
    });
    let (events, completions) = cb::bounded(4);
    let storage = HybridStorage::with_policy_and_event_sender(
        backend.clone(),
        policy::StorageBudgetPolicy::default(),
        Some(events),
    );
    let key = "wal/retired-replacement";
    let original = storage
        .compare_exchange_remote_object(key, None, b"retired".to_vec())
        .unwrap();
    storage
        .delete_remote_objects_by_identity(vec![(1, key.into(), original.metadata().clone())])
        .unwrap();
    assert!(matches!(
        completions.recv_timeout(Duration::from_secs(1)).unwrap(),
        StorageEvent::CloudWalPruneComplete {
            result: StorageOutcome::Err(_),
            ..
        }
    ));

    // Act: no storage tick runs between the failed attempt and replacement.
    storage
        .compare_exchange_remote_object(key, Some(original.metadata()), b"replacement".to_vec())
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        storage.process_uploads();
        if completions.try_recv().is_ok() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "retained identity retry did not run"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    storage.shutdown_background_workers();

    // Assert
    assert_eq!(backend.attempts.load(Ordering::Acquire), 1);
    assert_eq!(
        storage.remote_object_proof(key).unwrap().bytes(),
        b"replacement"
    );
}

struct HeldWalWrite {
    bytes: Vec<u8>,
    options: crate::storage::cloud::CloudRequestOptions,
    callback: crate::storage::cloud::CloudCallback,
}

struct ParallelWalUploads {
    inner: MockCloudBackend,
    pending: Mutex<HashMap<String, HeldWalWrite>>,
    entered: cb::Sender<String>,
}

impl ParallelWalUploads {
    fn finish(&self, key: &str, fail: bool) {
        use crate::storage::cloud::{
            CloudError, CloudEvent, CloudHeadCapability, CloudWriteCapability,
        };
        let pending = self.pending.lock().remove(key).expect("held upload");
        if fail {
            pending
                .callback
                .send(CloudEvent::PutReceipt {
                    key: key.into(),
                    result: Err(CloudError::Transport(
                        "injected transient upload failure".into(),
                    )),
                })
                .unwrap();
            return;
        }
        let checksum = crc32c::crc32c(&pending.bytes);
        let (tx, rx) = mpsc::channel();
        self.inner
            .submit_put(key, pending.bytes, pending.options.headers, tx);
        assert!(matches!(
            rx.recv().unwrap(),
            CloudEvent::Put { result: Ok(()), .. }
        ));
        let (tx, rx) = mpsc::channel();
        self.inner.submit_head(key, tx);
        let CloudEvent::Head {
            result: Ok(metadata),
            ..
        } = rx.recv().unwrap()
        else {
            panic!("uploaded identity");
        };
        pending
            .callback
            .send(CloudEvent::PutReceipt {
                key: key.into(),
                result: Ok(Some(crate::storage::StorageWriteReceipt {
                    metadata,
                    content_crc32c: checksum,
                })),
            })
            .unwrap();
    }
}

impl crate::storage::cloud::CloudReadCapability for ParallelWalUploads {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_get, submit_get_with_metadata,
        submit_get_range, submit_get_range_with_identity);
}
impl crate::storage::cloud::CloudWriteCapability for ParallelWalUploads {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_put);
    fn write_with_receipt(
        &self,
        key: &str,
        bytes: Vec<u8>,
        options: crate::storage::cloud::CloudRequestOptions,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        assert!(
            options.reservation.is_some(),
            "upload body retains its admission"
        );
        self.pending.lock().insert(
            key.into(),
            HeldWalWrite {
                bytes,
                options,
                callback,
            },
        );
        self.entered.send(key.into()).unwrap();
    }
}
impl crate::storage::cloud::CloudDeleteCapability for ParallelWalUploads {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_delete);
}
impl crate::storage::cloud::CloudListCapability for ParallelWalUploads {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_list);
}
impl crate::storage::cloud::CloudHeadCapability for ParallelWalUploads {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_head);
}
impl crate::storage::cloud::CloudBackend for ParallelWalUploads {}

struct ReleaseWalUploads(Arc<ParallelWalUploads>);
impl Drop for ReleaseWalUploads {
    fn drop(&mut self) {
        let keys = self.0.pending.lock().keys().cloned().collect::<Vec<_>>();
        for key in keys {
            self.0.finish(&key, false);
        }
    }
}

#[test]
fn should_preserve_bounded_upload_ownership_when_completions_reorder() -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let (entered, arrivals) = cb::unbounded();
    let backend = Arc::new(ParallelWalUploads {
        inner: MockCloudBackend::new(),
        pending: Mutex::new(HashMap::new()),
        entered,
    });
    let cloud = Arc::new(CloudStorage::new(backend.clone(), String::new()));
    let storage = HybridStorage::with_policy_event_sender_and_limits(
        cloud,
        policy::StorageBudgetPolicy::default(),
        None,
        HybridQueueLimits {
            worker_concurrency: 4,
            upload_memory_bytes: 4
                * 16
                * 1024
                * (3 + crate::storage::CONSERVATIVE_WRITE_PAYLOAD_COPIES),
            wal_upload_retry: WalUploadRetryPolicy {
                base_delay: Duration::ZERO,
                max_delay: Duration::ZERO,
            },
            ..HybridQueueLimits::default()
        },
    );
    let _release = ReleaseWalUploads(Arc::clone(&backend));
    for id in 1..=4 {
        let path = directory.path().join(format!("{id}.wal"));
        std::fs::write(&path, vec![u8::try_from(id).unwrap(); 16 * 1024])?;
        storage.enqueue_object_upload(
            id,
            format!("raw/{id}"),
            crate::io::FsLocation::host_file(&path),
            id,
        )?;
    }

    // Act: every worker is live before any completion is released.
    storage.process_uploads();
    let mut started = (0..4)
        .map(|_| {
            arrivals
                .recv_timeout(Duration::from_secs(2))
                .expect("parallel upload started")
        })
        .collect::<Vec<_>>();
    started.sort();
    let retained = backend
        .pending
        .lock()
        .values()
        .map(|write| {
            write
                .options
                .reservation
                .as_ref()
                .expect("retained upload admission")
                .bytes()
        })
        .sum::<usize>();
    assert_eq!(
        retained,
        4 * 16 * 1024 * (3 + crate::storage::CONSERVATIVE_WRITE_PAYLOAD_COPIES)
    );
    for key in ["raw/4", "raw/3", "raw/2"] {
        backend.finish(key, false);
    }
    backend.finish("raw/1", true);
    let deadline = Instant::now() + Duration::from_secs(3);
    let retried = loop {
        storage.process_uploads();
        if let Ok(key) = arrivals.try_recv() {
            break key;
        }
        assert!(Instant::now() < deadline, "failed upload was not retried");
        std::thread::yield_now();
    };
    backend.finish(&retried, false);
    storage.shutdown_wal_upload_worker();
    storage.process_uploads();

    // Assert
    assert_eq!(started, ["raw/1", "raw/2", "raw/3", "raw/4"]);
    assert_eq!(retried, "raw/1");
    assert_eq!(backend.inner.get_uploads().len(), 4);
    assert_eq!(storage.pending_upload_count(), 0);
    assert!(backend.pending.lock().is_empty());
    Ok(())
}
