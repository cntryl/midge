use super::*;
use crate::runtime::hybrid_persistence::{
    CloudMetadataPruneGuard, CloudMetadataPruneProof, CloudWalPruneGuard, HybridPersistence,
};
use crate::sst::SstFactory;
#[cfg(feature = "failpoints")]
use crate::storage::cloud::CloudBackend;
use crate::storage::cloud::{CloudStorage, MockCloudBackend};
use bytes::Bytes;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Barrier,
};
use std::time::{Duration, Instant};

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
        .remote_object_proof_optional(crate::wal::cloud_catalog::OBJECT_KEY)
        .expect("provider-shaped 404 must classify as confirmed missing");
    assert!(missing.is_none());
    assert_eq!(missing_server.finish(), 1);

    let cas_server = spawn_scripted_http_response_server(vec![
        provider_error_response(shape, 404),
        provider_error_response(shape, 412),
    ]);
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
        .compare_exchange_remote_object(
            crate::wal::cloud_catalog::OBJECT_KEY,
            None,
            b"catalog".to_vec(),
        )
        .expect_err("provider-shaped 412 must classify as a CAS conflict");
    assert!(matches!(
        cas_error,
        crate::common::MidgeError::Busy(message) if {
            let message = message.to_ascii_lowercase();
            message.contains("status 412") && message.contains("precondition")
        }
    ));
    assert_eq!(cas_server.finish(), 2);
}

#[cfg(feature = "cloud-all")]
#[test]
fn should_classify_real_s3_error_shapes_given_wal_catalog_operations() {
    assert_real_provider_missing_and_cas_errors(ProviderErrorShape::S3);
}

#[cfg(feature = "cloud-all")]
#[test]
fn should_classify_real_azure_error_shapes_given_wal_catalog_operations() {
    assert_real_provider_missing_and_cas_errors(ProviderErrorShape::Azure);
}

#[cfg(feature = "cloud-all")]
#[test]
fn should_classify_real_gcs_error_shapes_given_wal_catalog_operations() {
    assert_real_provider_missing_and_cas_errors(ProviderErrorShape::Gcs);
}

#[test]
fn should_classify_windows_missing_paths_as_absent() {
    // Arrange
    let errors = [
        "read C:\\data\\missing.sst: The system cannot find the file specified. (os error 2)",
        "read C:\\data\\sst: The system cannot find the path specified. (os error 3)",
    ];

    // Act
    let all_missing = errors
        .iter()
        .all(|error| HybridStorage::storage_error_indicates_missing(error));

    // Assert
    assert!(all_missing);
}

#[test]
fn should_classify_provider_missing_object_errors_as_absent() {
    // Arrange
    let errors = [
        "S3 GET failed: HTTP 404 NoSuchKey",
        "Azure Blob request failed: 404 BlobNotFound",
        "GCS object lookup returned NotFound: No such object",
    ];

    // Act
    let all_missing = errors
        .iter()
        .all(|error| HybridStorage::storage_error_indicates_missing(error));

    // Assert
    assert!(all_missing);
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
fn should_reject_wal_catalog_publication_from_stale_epoch_after_takeover() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let tmp = tempfile::tempdir().expect("create stale publisher WAL dir");
    let segment_id = 1;
    let max_sequence = 11;
    let bytes = valid_wal_bytes(max_sequence);
    let local_path = tmp.path().join(crate::wal::segment_file_name(segment_id));
    std::fs::write(&local_path, &bytes).expect("write stale publisher local WAL");
    let object_key = crate::wal::cloud_segment_object_key(segment_id, 1);
    write_cloud_object(&storage, &object_key, bytes);
    storage
        .fence_cloud_wal_catalog(3)
        .expect("new lease fences WAL catalog");

    // Act
    let result = storage.publish_remote_wal_segment(segment_id, max_sequence, &local_path, 2);

    // Assert
    let error = result.expect_err("stale lease epoch must not publish WAL authority");
    assert!(error.contains("requires fencing epoch 3"), "{error}");
    let catalog_proof = storage
        .remote_object_proof(crate::wal::cloud_catalog::OBJECT_KEY)
        .expect("read winning WAL catalog");
    let catalog = crate::wal::cloud_catalog::WalPublicationCatalog::decode(catalog_proof.bytes())
        .expect("decode winning WAL catalog");
    assert!(!catalog.segments.contains_key(&segment_id));
    assert_cloud_object_exists(&storage, &object_key);
}

#[test]
fn should_reject_stale_catalog_compare_exchange_after_takeover() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let stale_proof = storage
        .remote_object_proof(crate::wal::cloud_catalog::OBJECT_KEY)
        .expect("read pre-takeover WAL catalog");
    let mut takeover_catalog =
        crate::wal::cloud_catalog::WalPublicationCatalog::decode(stale_proof.bytes())
            .expect("decode pre-takeover WAL catalog");
    takeover_catalog
        .fence_to(3)
        .expect("advance winning takeover epoch");
    storage
        .compare_exchange_remote_object(
            crate::wal::cloud_catalog::OBJECT_KEY,
            Some(stale_proof.metadata()),
            takeover_catalog.encode().expect("encode winning catalog"),
        )
        .expect("publish winning takeover catalog");
    let mut losing_catalog =
        crate::wal::cloud_catalog::WalPublicationCatalog::decode(stale_proof.bytes())
            .expect("decode stale WAL catalog");
    losing_catalog
        .fence_to(4)
        .expect("prepare losing catalog mutation");

    // Act
    let result = storage.compare_exchange_remote_object(
        crate::wal::cloud_catalog::OBJECT_KEY,
        Some(stale_proof.metadata()),
        losing_catalog.encode().expect("encode losing catalog"),
    );

    // Assert
    assert!(matches!(result, Err(crate::common::MidgeError::Busy(_))));
    let winning_proof = storage
        .remote_object_proof(crate::wal::cloud_catalog::OBJECT_KEY)
        .expect("read winning WAL catalog");
    let winning_catalog =
        crate::wal::cloud_catalog::WalPublicationCatalog::decode(winning_proof.bytes())
            .expect("decode winning WAL catalog");
    assert_eq!(winning_catalog.fencing_epoch, 3);
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
        key: "objects/first".to_string(),
        bytes: vec![1],
        metadata: StorageObjectMetadata {
            size: 1,
            etag: "first-etag".to_string(),
            generation: None,
        },
    };
    storage
        .delete_remote_object_guarded(1, target.clone(), Vec::new())
        .expect("first guarded delete should occupy the worker");

    // Act
    let started = Instant::now();
    let error = storage
        .delete_remote_object_guarded(2, target, Vec::new())
        .expect_err("second guarded delete must be rejected at worker capacity");

    // Assert
    assert!(error.contains("workers at capacity"), "{error}");
    assert!(started.elapsed() < Duration::from_millis(50));
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
    storage
        .fence_cloud_wal_catalog(2)
        .expect("initialize test WAL publication catalog");
    (mock_cloud, storage)
}

#[test]
fn should_route_each_object_class_to_its_separate_cloud_store() {
    // Arrange
    let tmp = tempfile::tempdir().expect("create class-routing test dir");
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(tmp.path().join("local"))
            .expect("create local backend"),
    );
    let wal_mock = Arc::new(MockCloudBackend::new());
    let sst_mock = Arc::new(MockCloudBackend::new());
    let control_mock = Arc::new(MockCloudBackend::new());
    let wal_cloud = Arc::new(CloudStorage::new(wal_mock.clone(), String::new()));
    let sst_cloud = Arc::new(CloudStorage::new(sst_mock.clone(), String::new()));
    let control_cloud = Arc::new(CloudStorage::new(control_mock.clone(), String::new()));
    let storage = HybridStorage::with_class_stores_policy_event_sender_and_limits(
        local,
        wal_cloud,
        sst_cloud,
        control_cloud,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        None,
        HybridQueueLimits::default(),
    );
    let wal_path = tmp.path().join("segment.wal");
    std::fs::write(&wal_path, valid_wal_bytes(17)).expect("write WAL segment");

    // Act
    storage
        .enqueue_wal_segment(17, &wal_path, 17)
        .expect("enqueue WAL segment");
    storage.process_uploads();
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline && wal_mock.get_uploads().is_empty() {
        std::thread::yield_now();
        storage.process_uploads();
    }
    storage
        .write_sst_object("000017.sst", valid_sst_bytes(b"key", b"value", 17))
        .expect("publish SST");
    storage
        .compare_exchange_remote_object(
            crate::runtime::ddl::REMOTE_DDL_REGISTRY_KEY,
            None,
            br#"{"epoch":17}"#.to_vec(),
        )
        .expect("publish control object");

    // Assert
    let wal_size = std::fs::metadata(&wal_path)
        .expect("read WAL segment metadata")
        .len();
    assert_eq!(
        wal_mock.get_uploads(),
        vec![(crate::wal::cloud_segment::object_key(17, 1), wal_size)]
    );
    assert_eq!(
        sst_mock
            .get_uploads()
            .iter()
            .map(|(key, _)| key.as_str())
            .collect::<Vec<_>>(),
        vec![crate::sst::object_key("000017.sst")]
    );
    assert_eq!(
        control_mock.get_uploads(),
        vec![(
            crate::runtime::ddl::REMOTE_DDL_REGISTRY_KEY.to_string(),
            br#"{"epoch":17}"#.len() as u64,
        )]
    );
}

fn valid_sst_bytes(key: &[u8], value: &[u8], seq: u64) -> Vec<u8> {
    let factory = crate::sst::FsSstFactoryIo::new(Arc::new(crate::io::MockFs::new()), 4096);
    let mut writer = factory.create().expect("create SST writer");
    writer
        .add_with_meta(key, Some(value), seq, 0, None)
        .expect("add SST entry");
    writer.finish_bytes().expect("finish SST bytes")
}

fn valid_wal_bytes(seq: u64) -> Vec<u8> {
    let record = crate::wal::WalRecord::new(
        crate::wal::WalOpKind::Put,
        Bytes::from_static(b"k"),
        Some(Bytes::from_static(b"v")),
        seq,
        1,
    );
    let payload = crate::wal::encoding::encode(&record).expect("encode WAL record");
    let mut bytes = Vec::new();
    crate::wal::frame::append_frame(&mut bytes, &payload).expect("append WAL frame");
    bytes
}

fn write_authoritative_cloud_wal(
    storage: &HybridStorage,
    segment_id: u64,
    catalog_max_sequence: u64,
    bytes: Vec<u8>,
) -> String {
    let publication = crate::wal::cloud_catalog::PublishedWalSegment::from_validated_bytes(
        segment_id,
        catalog_max_sequence,
        1,
        &bytes,
    );
    write_cloud_object(storage, &publication.object_key, bytes);
    let catalog_proof = storage
        .remote_object_proof(crate::wal::cloud_catalog::OBJECT_KEY)
        .expect("read test WAL catalog");
    let mut catalog =
        crate::wal::cloud_catalog::WalPublicationCatalog::decode(catalog_proof.bytes())
            .expect("decode test WAL catalog");
    catalog
        .publish(2, publication.clone())
        .expect("publish test WAL authority");
    storage
        .compare_exchange_remote_object(
            crate::wal::cloud_catalog::OBJECT_KEY,
            Some(catalog_proof.metadata()),
            catalog.encode().expect("encode test WAL catalog"),
        )
        .expect("update test WAL catalog");
    publication.object_key
}

struct AlwaysFailingWriteBackend {
    write_attempts: Arc<AtomicUsize>,
}

impl AlwaysFailingWriteBackend {
    fn new(write_attempts: Arc<AtomicUsize>) -> Self {
        Self { write_attempts }
    }
}

impl StorageBackend for AlwaysFailingWriteBackend {
    fn submit_read(&self, key: &str, callback: StorageCallback) {
        let _ = callback.send(StorageEvent::ReadComplete {
            key: key.to_string(),
            result: StorageOutcome::Err("read unavailable".to_string()),
        });
    }

    fn submit_write(&self, key: &str, _data: Vec<u8>, callback: StorageCallback) {
        self.write_attempts.fetch_add(1, Ordering::SeqCst);
        let _ = callback.send(StorageEvent::WriteComplete {
            key: key.to_string(),
            result: StorageOutcome::Err("write unavailable".to_string()),
        });
    }

    fn submit_write_with_headers(
        &self,
        key: &str,
        data: Vec<u8>,
        _headers: Vec<(String, String)>,
        callback: StorageCallback,
    ) {
        self.submit_write(key, data, callback);
    }

    fn submit_delete(&self, key: &str, callback: StorageCallback) {
        let _ = callback.send(StorageEvent::DeleteComplete {
            key: key.to_string(),
            result: StorageOutcome::Ok(()),
        });
    }

    fn submit_list(&self, prefix: &str, callback: StorageCallback) {
        let _ = callback.send(StorageEvent::ListComplete {
            prefix: prefix.to_string(),
            result: StorageOutcome::Ok(Vec::new()),
        });
    }

    fn submit_head(&self, key: &str, callback: StorageCallback) {
        let _ = callback.send(StorageEvent::HeadComplete {
            key: key.to_string(),
            result: StorageOutcome::Err("head unavailable".to_string()),
        });
    }
}

#[derive(Default)]
struct NeverCompletesBackend {
    callbacks: Mutex<Vec<StorageCallback>>,
}

struct RacingReadDeleteBackend {
    object: Mutex<Option<Vec<u8>>>,
    read_started: Barrier,
    release_read: Barrier,
}

impl RacingReadDeleteBackend {
    fn new(bytes: Vec<u8>) -> Self {
        Self {
            object: Mutex::new(Some(bytes)),
            read_started: Barrier::new(2),
            release_read: Barrier::new(2),
        }
    }

    fn metadata(bytes: &[u8]) -> StorageObjectMetadata {
        StorageObjectMetadata {
            size: bytes.len() as u64,
            etag: "race-etag".to_string(),
            generation: None,
        }
    }
}

impl StorageBackend for RacingReadDeleteBackend {
    fn submit_read(&self, key: &str, callback: StorageCallback) {
        let key = key.to_string();
        let snapshot = self.object.lock().clone();
        self.read_started.wait();
        self.release_read.wait();
        let result = snapshot.map_or_else(
            || StorageOutcome::Err("object not found".to_string()),
            StorageOutcome::Ok,
        );
        let _ = callback.send(StorageEvent::ReadComplete { key, result });
    }

    fn submit_write(&self, key: &str, data: Vec<u8>, callback: StorageCallback) {
        *self.object.lock() = Some(data);
        let _ = callback.send(StorageEvent::WriteComplete {
            key: key.to_string(),
            result: StorageOutcome::Ok(()),
        });
    }

    fn submit_delete(&self, key: &str, callback: StorageCallback) {
        self.submit_delete_with_headers(key, Vec::new(), callback);
    }

    fn submit_delete_with_headers(
        &self,
        key: &str,
        headers: Vec<(String, String)>,
        callback: StorageCallback,
    ) {
        let expected = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("if-match"))
            .map(|(_, value)| value.trim_matches('"'));
        let result = if expected.is_none_or(|value| value == "race-etag") {
            self.object.lock().take();
            StorageOutcome::Ok(())
        } else {
            StorageOutcome::Err("precondition failed".to_string())
        };
        let _ = callback.send(StorageEvent::DeleteComplete {
            key: key.to_string(),
            result,
        });
    }

    #[cfg(test)]
    fn submit_list(&self, prefix: &str, callback: StorageCallback) {
        let objects = self
            .object
            .lock()
            .as_ref()
            .map_or_else(Vec::new, |_| vec![prefix.to_string()]);
        let _ = callback.send(StorageEvent::ListComplete {
            prefix: prefix.to_string(),
            result: StorageOutcome::Ok(objects),
        });
    }

    fn submit_head(&self, key: &str, callback: StorageCallback) {
        let result = self.object.lock().as_deref().map_or_else(
            || StorageOutcome::Err("object not found".to_string()),
            |bytes| StorageOutcome::Ok(Self::metadata(bytes)),
        );
        let _ = callback.send(StorageEvent::HeadComplete {
            key: key.to_string(),
            result,
        });
    }
}

impl NeverCompletesBackend {
    fn retain_callback(&self, callback: StorageCallback) {
        self.callbacks.lock().push(callback);
    }
}

impl StorageBackend for NeverCompletesBackend {
    fn submit_read(&self, _key: &str, callback: StorageCallback) {
        self.retain_callback(callback);
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

    fn submit_list(&self, _prefix: &str, callback: StorageCallback) {
        self.retain_callback(callback);
    }

    fn submit_head(&self, _key: &str, callback: StorageCallback) {
        self.retain_callback(callback);
    }
}

fn write_cloud_object(storage: &HybridStorage, key: &str, data: Vec<u8>) {
    let (tx, rx) = std::sync::mpsc::channel();
    storage.cloud.submit_write(key, data, tx);
    match rx.recv_timeout(Duration::from_secs(1)) {
        Ok(StorageEvent::WriteComplete {
            result: StorageOutcome::Ok(()),
            ..
        }) => {}
        other => panic!("cloud write for '{key}' failed: {other:?}"),
    }
}

fn write_local_object(storage: &HybridStorage, key: &str, data: Vec<u8>) {
    let (tx, rx) = std::sync::mpsc::channel();
    storage.local.submit_write(key, data, tx);
    match rx.recv_timeout(Duration::from_secs(1)) {
        Ok(StorageEvent::WriteComplete {
            result: StorageOutcome::Ok(()),
            ..
        }) => {}
        other => panic!("local write for '{key}' failed: {other:?}"),
    }
}

fn read_local_object(storage: &HybridStorage, key: &str) -> Vec<u8> {
    let (tx, rx) = std::sync::mpsc::channel();
    storage.local.submit_read(key, tx);
    match rx.recv_timeout(Duration::from_secs(1)) {
        Ok(StorageEvent::ReadComplete {
            result: StorageOutcome::Ok(data),
            ..
        }) => data,
        other => panic!("local read for '{key}' failed: {other:?}"),
    }
}

fn read_cloud_object(storage: &HybridStorage, key: &str) -> Vec<u8> {
    let (tx, rx) = std::sync::mpsc::channel();
    storage.cloud.submit_read(key, tx);
    match rx.recv_timeout(Duration::from_secs(1)) {
        Ok(StorageEvent::ReadComplete {
            result: StorageOutcome::Ok(data),
            ..
        }) => data,
        other => panic!("cloud read for '{key}' failed: {other:?}"),
    }
}

fn read_hybrid_object(storage: &HybridStorage, key: &str) -> Vec<u8> {
    let (tx, rx) = std::sync::mpsc::channel();
    storage.submit_read(key, tx);
    match rx.recv_timeout(Duration::from_secs(1)) {
        Ok(StorageEvent::ReadComplete {
            result: StorageOutcome::Ok(data),
            ..
        }) => data,
        other => panic!("hybrid read for '{key}' failed: {other:?}"),
    }
}

fn delete_cloud_object(storage: &HybridStorage, key: &str) {
    let (tx, rx) = std::sync::mpsc::channel();
    storage.cloud.submit_delete(key, tx);
    match rx.recv_timeout(Duration::from_secs(1)) {
        Ok(StorageEvent::DeleteComplete {
            result: StorageOutcome::Ok(()),
            ..
        }) => {}
        other => panic!("cloud delete for '{key}' failed: {other:?}"),
    }
}

fn write_cloud_metadata_object(cloud: &CloudStorage, key: &str, data: Vec<u8>) {
    let (tx, rx) = std::sync::mpsc::channel();
    cloud.submit_put(key, data, vec![], tx);
    match rx.recv_timeout(Duration::from_secs(1)) {
        Ok(crate::storage::cloud::CloudEvent::Put {
            result: crate::storage::cloud::CloudOutcome::Ok(()),
            ..
        }) => {}
        other => panic!("cloud metadata write for '{key}' failed: {other:?}"),
    }
}

fn head_cloud_metadata_object(cloud: &CloudStorage, key: &str) -> StorageObjectMetadata {
    let (tx, rx) = std::sync::mpsc::channel();
    cloud.submit_head(key, tx);
    match rx.recv_timeout(Duration::from_secs(1)) {
        Ok(crate::storage::cloud::CloudEvent::Head {
            result: crate::storage::cloud::CloudOutcome::Ok(metadata),
            ..
        }) => StorageObjectMetadata {
            size: metadata.size,
            etag: metadata.etag,
            generation: metadata.generation,
        },
        other => panic!("cloud metadata head for '{key}' failed: {other:?}"),
    }
}

fn assert_cloud_object_exists(storage: &HybridStorage, key: &str) {
    let (tx, rx) = std::sync::mpsc::channel();
    storage.cloud.submit_head(key, tx);
    match rx.recv_timeout(Duration::from_secs(1)) {
        Ok(StorageEvent::HeadComplete {
            result: StorageOutcome::Ok(_),
            ..
        }) => {}
        other => panic!("expected cloud object '{key}' to exist, got {other:?}"),
    }
}

fn assert_cloud_object_missing(storage: &HybridStorage, key: &str) {
    let (tx, rx) = std::sync::mpsc::channel();
    storage.cloud.submit_head(key, tx);
    match rx.recv_timeout(Duration::from_secs(1)) {
        Ok(StorageEvent::HeadComplete {
            result: StorageOutcome::Err(_),
            ..
        }) => {}
        other => panic!("expected cloud object '{key}' to be missing, got {other:?}"),
    }
}

fn wait_for_wal_prune_result(storage: &HybridStorage, segment_id: u64) -> StorageOutcome<()> {
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        for event in storage.process_uploads() {
            if let StorageEvent::CloudWalPruneComplete {
                segment_id: event_segment,
                result,
            } = event
            {
                if event_segment == segment_id {
                    return result;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("timed out waiting for WAL prune result for segment {segment_id}");
}

fn manifest_for_ssts(files: &[(&str, u64)]) -> crate::metadata::Manifest {
    let files_with_crc: Vec<_> = files
        .iter()
        .map(|(name, size_bytes)| (*name, *size_bytes, None))
        .collect();
    manifest_for_ssts_with_crc(&files_with_crc)
}

fn manifest_for_ssts_with_crc(files: &[(&str, u64, Option<u32>)]) -> crate::metadata::Manifest {
    crate::metadata::Manifest {
        files: files
            .iter()
            .map(
                |(name, size_bytes, content_crc32c)| crate::metadata::FileMeta {
                    name: (*name).to_string(),
                    level: 0,
                    size_bytes: *size_bytes,
                    content_crc32c: *content_crc32c,
                    cf_id: 0,
                    smallest_key: Some(b"a".to_vec()),
                    largest_key: Some(b"a".to_vec()),
                    smallest_seq: Some(1),
                    largest_seq: Some(1),
                    ..Default::default()
                },
            )
            .collect(),
        ..Default::default()
    }
}

fn manifest_covering_wal(
    sst_name: &str,
    sst_bytes: &[u8],
    sequence: u64,
    content_crc32c: Option<u32>,
) -> crate::metadata::Manifest {
    crate::metadata::Manifest {
        files: vec![crate::metadata::FileMeta {
            name: sst_name.to_string(),
            level: 0,
            size_bytes: sst_bytes.len() as u64,
            content_crc32c,
            cf_id: 0,
            smallest_key: Some(b"k".to_vec()),
            largest_key: Some(b"k".to_vec()),
            smallest_seq: Some(sequence),
            largest_seq: Some(sequence),
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[test]
fn should_revalidate_manifest_ssts_on_repeated_validation() {
    // Arrange
    let (mock_cloud, storage) = hybrid_with_mock_cloud();
    let sst_name = "cached.sst";
    let bytes = valid_sst_bytes(b"a", b"v1", 1);
    write_cloud_object(&storage, &crate::sst::object_key(sst_name), bytes.clone());
    let manifest = manifest_for_ssts(&[(sst_name, bytes.len() as u64)]);

    mock_cloud.clear_history();
    storage
        .verify_manifest_cloud_objects(&manifest)
        .expect("first manifest validation");
    let first_downloads = mock_cloud.get_downloads();
    // Act
    // Assert
    assert!(
        first_downloads
            .iter()
            .any(|key| key.ends_with("sst/cached.sst")),
        "first validation should read the cloud SST, got {first_downloads:?}"
    );

    storage
        .verify_manifest_cloud_objects(&manifest)
        .expect("second manifest validation");

    let downloads = mock_cloud.get_downloads();
    assert_eq!(
        downloads
            .iter()
            .filter(|key| key.ends_with("sst/cached.sst"))
            .count(),
        2,
        "authoritative validation must reread the immutable SST: {downloads:?}"
    );
}

#[test]
fn should_revalidate_full_manifest_after_extension() {
    // Arrange
    let (mock_cloud, storage) = hybrid_with_mock_cloud();
    let first_name = "first.sst";
    let second_name = "second.sst";
    let first_bytes = valid_sst_bytes(b"a", b"v1", 1);
    let second_bytes = valid_sst_bytes(b"b", b"v2", 2);
    write_cloud_object(
        &storage,
        &crate::sst::object_key(first_name),
        first_bytes.clone(),
    );
    write_cloud_object(
        &storage,
        &crate::sst::object_key(second_name),
        second_bytes.clone(),
    );

    let first_manifest = manifest_for_ssts(&[(first_name, first_bytes.len() as u64)]);
    storage
        .verify_manifest_cloud_objects(&first_manifest)
        .expect("first manifest validation");

    mock_cloud.clear_history();
    let extended_manifest = crate::metadata::Manifest {
        files: vec![
            crate::metadata::FileMeta {
                name: first_name.to_string(),
                level: 0,
                size_bytes: first_bytes.len() as u64,
                cf_id: 0,
                smallest_key: Some(b"a".to_vec()),
                largest_key: Some(b"a".to_vec()),
                smallest_seq: Some(1),
                largest_seq: Some(1),
                ..Default::default()
            },
            crate::metadata::FileMeta {
                name: second_name.to_string(),
                level: 0,
                size_bytes: second_bytes.len() as u64,
                cf_id: 0,
                smallest_key: Some(b"b".to_vec()),
                largest_key: Some(b"b".to_vec()),
                smallest_seq: Some(2),
                largest_seq: Some(2),
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    storage
        .verify_manifest_cloud_objects(&extended_manifest)
        .expect("extended manifest validation");
    let downloads = mock_cloud.get_downloads();

    // Act
    // Assert
    assert!(
        downloads.iter().any(|key| key.ends_with("sst/second.sst")),
        "extended validation should read the new SST, got {downloads:?}"
    );
    assert!(
        downloads.iter().any(|key| key.ends_with("sst/first.sst")),
        "authoritative validation should reread the existing SST, got {downloads:?}"
    );
}

#[test]
fn should_reject_cached_manifest_sst_proof_when_cloud_object_is_deleted() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let sst_name = "deleted-after-proof.sst";
    let bytes = valid_sst_bytes(b"a", b"v1", 1);
    let key = crate::sst::object_key(sst_name);
    write_cloud_object(&storage, &key, bytes.clone());
    let manifest = manifest_for_ssts(&[(sst_name, bytes.len() as u64)]);

    storage
        .verify_manifest_cloud_objects(&manifest)
        .expect("initial manifest validation");
    delete_cloud_object(&storage, &key);

    let error = storage
        .verify_manifest_cloud_objects(&manifest)
        .expect_err("deleted SST must invalidate cached proof");
    // Act
    // Assert
    assert!(
        error.contains("changed since validation") || error.contains("unreadable"),
        "unexpected stale SST proof error: {error}"
    );
}

#[test]
fn should_reject_cached_manifest_sst_proof_when_cloud_object_is_overwritten() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let sst_name = "overwritten-after-proof.sst";
    let bytes = valid_sst_bytes(b"a", b"v1", 1);
    let key = crate::sst::object_key(sst_name);
    write_cloud_object(&storage, &key, bytes.clone());
    let manifest = manifest_for_ssts(&[(sst_name, bytes.len() as u64)]);

    storage
        .verify_manifest_cloud_objects(&manifest)
        .expect("initial manifest validation");
    write_cloud_object(&storage, &key, b"not a valid sst".to_vec());

    let error = storage
        .verify_manifest_cloud_objects(&manifest)
        .expect_err("overwritten SST must invalidate cached proof");
    // Act
    // Assert
    assert!(
        error.contains("changed since validation") || error.contains("size mismatch"),
        "unexpected stale SST proof error: {error}"
    );
}

#[test]
fn should_reject_manifest_sst_when_content_crc_differs() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let sst_name = "wrong-crc.sst";
    let bytes = valid_sst_bytes(b"a", b"v1", 1);
    let key = crate::sst::object_key(sst_name);
    let wrong_crc = crc32c::crc32c(&bytes) ^ 0xffff_ffff;
    write_cloud_object(&storage, &key, bytes.clone());
    let manifest = manifest_for_ssts_with_crc(&[(sst_name, bytes.len() as u64, Some(wrong_crc))]);

    let error = storage
        .verify_manifest_cloud_objects(&manifest)
        .expect_err("manifest SST with mismatched content CRC must not validate");

    // Act
    // Assert
    assert!(
        error.contains("crc") || error.contains("content"),
        "unexpected manifest SST CRC validation error: {error}"
    );
}

#[test]
fn should_not_reuse_size_only_sst_proof_when_manifest_later_requires_crc() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let sst_name = "crc-after-size-proof.sst";
    let bytes = valid_sst_bytes(b"a", b"v1", 1);
    let key = crate::sst::object_key(sst_name);
    let wrong_crc = crc32c::crc32c(&bytes) ^ 0xffff_ffff;
    write_cloud_object(&storage, &key, bytes.clone());
    let size_only_manifest = manifest_for_ssts(&[(sst_name, bytes.len() as u64)]);
    storage
        .verify_manifest_cloud_objects(&size_only_manifest)
        .expect("initial size-only manifest validation");
    let crc_manifest =
        manifest_for_ssts_with_crc(&[(sst_name, bytes.len() as u64, Some(wrong_crc))]);

    let error = storage
        .verify_manifest_cloud_objects(&crc_manifest)
        .expect_err("cached size-only SST proof must not satisfy later CRC requirement");

    // Act
    // Assert
    assert!(
        error.contains("crc") || error.contains("content"),
        "unexpected cached SST CRC validation error: {error}"
    );
}

#[test]
fn should_not_overwrite_remote_object_given_different_content_when_authoritative_upload_runs() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let sst_name = "collision.sst";
    let key = crate::sst::object_key(sst_name);
    let existing_bytes = valid_sst_bytes(b"a", b"already-committed", 1);
    let upload_bytes = valid_sst_bytes(b"b", b"new-upload", 2);
    write_cloud_object(&storage, &key, existing_bytes.clone());

    let error = storage
        .write_sst_object(sst_name, upload_bytes)
        .expect_err("existing remote SST object must fail authoritative upload");

    // Act
    // Assert
    assert!(
        error.to_string().contains("cloud immutable upload failed")
            || error.to_string().contains("precondition failed"),
        "unexpected SST collision error: {error}"
    );
    assert_eq!(
        read_cloud_object(&storage, &key),
        existing_bytes,
        "authoritative SST upload must not overwrite an existing remote object"
    );
    assert_eq!(
        read_hybrid_object(&storage, &key),
        existing_bytes,
        "failed authoritative SST upload must not leave a conflicting local cache entry"
    );
}

#[test]
fn should_not_create_remote_sst_when_local_cache_key_already_exists() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let sst_name = "local-collision.sst";
    let key = crate::sst::object_key(sst_name);
    let existing_bytes = valid_sst_bytes(b"a", b"local-already-committed", 1);
    let upload_bytes = valid_sst_bytes(b"b", b"new-upload", 2);
    write_local_object(&storage, &key, existing_bytes.clone());

    let error = storage
        .write_sst_object(sst_name, upload_bytes)
        .expect_err("existing local SST cache object must fail authoritative upload");

    // Act
    // Assert
    assert!(
        error.to_string().contains("local cache already exists"),
        "unexpected local SST collision error: {error}"
    );
    assert_eq!(
        read_local_object(&storage, &key),
        existing_bytes,
        "authoritative SST upload must not overwrite an existing local cache object"
    );
    assert_cloud_object_missing(&storage, &key);
}

#[test]
fn should_resume_same_content_sst_publication_after_remote_only_success() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let sst_name = "remote-only-retry.sst";
    let key = crate::sst::object_key(sst_name);
    let bytes = valid_sst_bytes(b"retry", b"value", 7);
    write_cloud_object(&storage, &key, bytes.clone());
    assert_cloud_object_exists(&storage, &key);

    // Act
    storage
        .write_sst_object(sst_name, bytes.clone())
        .expect("same-content retry should finish local cache publication");

    // Assert
    assert_eq!(read_cloud_object(&storage, &key), bytes);
    assert_eq!(read_local_object(&storage, &key), bytes);
}

#[test]
fn should_retry_same_content_upload_idempotently_given_remote_object_already_exists() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let sst_name = "fully-published-retry.sst";
    let key = crate::sst::object_key(sst_name);
    let bytes = valid_sst_bytes(b"retry", b"value", 8);
    storage
        .write_sst_object(sst_name, bytes.clone())
        .expect("publish both copies");

    // Act
    storage
        .write_sst_object(sst_name, bytes.clone())
        .expect("same-content retry must be idempotent");

    // Assert
    assert_eq!(read_cloud_object(&storage, &key), bytes);
    assert_eq!(read_local_object(&storage, &key), bytes);
}

#[test]
fn should_reread_remote_wal_given_repeated_authoritative_validation() {
    // Arrange
    let (mock_cloud, storage) = hybrid_with_mock_cloud();
    let segment_id = 7;
    let max_sequence = 11;
    let _key = write_authoritative_cloud_wal(
        &storage,
        segment_id,
        max_sequence,
        valid_wal_bytes(max_sequence),
    );

    mock_cloud.clear_history();
    storage
        .verify_remote_wal_segment(segment_id, max_sequence)
        .expect("first remote WAL validation");
    let first_downloads = mock_cloud.get_downloads();
    // Act
    // Assert
    assert!(
        first_downloads
            .iter()
            .any(|key| key.ends_with("wal/epochs/00000000000000000001/00000000000000000007.wal")),
        "first validation should read the cloud WAL, got {first_downloads:?}"
    );

    storage
        .verify_remote_wal_segment(segment_id, max_sequence)
        .expect("second remote WAL validation");
    let downloads = mock_cloud.get_downloads();
    assert_eq!(
        downloads
            .iter()
            .filter(|key| key.ends_with("wal/epochs/00000000000000000001/00000000000000000007.wal"))
            .count(),
        2,
        "authoritative WAL validation must reread the segment: {downloads:?}"
    );
}

#[test]
fn should_reject_cached_remote_wal_proof_when_cloud_object_is_deleted() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let segment_id = 8;
    let max_sequence = 12;
    let key = write_authoritative_cloud_wal(
        &storage,
        segment_id,
        max_sequence,
        valid_wal_bytes(max_sequence),
    );

    storage
        .verify_remote_wal_segment(segment_id, max_sequence)
        .expect("initial remote WAL validation");
    delete_cloud_object(&storage, &key);

    let error = storage
        .verify_remote_wal_segment(segment_id, max_sequence)
        .expect_err("deleted WAL must invalidate cached proof");
    // Act
    // Assert
    assert!(
        error.contains("changed since validation") || error.contains("unreadable"),
        "unexpected stale WAL proof error: {error}"
    );
}

#[test]
fn should_reject_stale_remote_wal_target_identity_when_guarded_delete_runs() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let segment_id = 11;
    let max_sequence = 21;
    let key = crate::wal::cloud_segment::object_key(segment_id, 1);
    write_cloud_object(&storage, &key, valid_wal_bytes(max_sequence));
    let stale_proof = storage
        .remote_object_proof(&key)
        .expect("initial remote object proof");

    write_cloud_object(&storage, &key, valid_wal_bytes(max_sequence));
    storage
        .delete_remote_object_guarded(segment_id, stale_proof, Vec::new())
        .expect("schedule prune");

    let result = wait_for_wal_prune_result(&storage, segment_id);
    // Act
    // Assert
    assert!(
        result.is_err(),
        "stale WAL proof must make remote prune fail conservatively"
    );
    assert_cloud_object_exists(&storage, &key);
}

#[test]
fn should_reject_reader_proof_when_guarded_prune_deletes_wal_during_download() {
    // Arrange
    let tmp = tempfile::tempdir().expect("create guarded-prune race test dir");
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(tmp.path().join("local"))
            .expect("create local backend"),
    );
    let segment_id = 12;
    let key = crate::wal::cloud_segment::object_key(segment_id, 1);
    let bytes = valid_wal_bytes(22);
    let backend = Arc::new(RacingReadDeleteBackend::new(bytes.clone()));
    let cloud: Arc<dyn StorageBackend> = backend.clone();
    let storage = Arc::new(HybridStorage::with_policy(
        local,
        cloud,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    ));
    let target = RemoteObjectProof {
        key: key.clone(),
        bytes: bytes.clone(),
        metadata: RacingReadDeleteBackend::metadata(&bytes),
    };
    let reader_storage = Arc::clone(&storage);
    let reader_key = key.clone();
    let reader = std::thread::spawn(move || reader_storage.remote_object_proof(&reader_key));
    backend.read_started.wait();

    // Act
    storage
        .delete_remote_object_guarded(segment_id, target, Vec::new())
        .expect("schedule guarded WAL prune");
    let prune_result = wait_for_wal_prune_result(&storage, segment_id);
    backend.release_read.wait();
    let reader_result = reader.join().expect("join concurrent WAL reader");

    // Assert
    assert!(
        prune_result.is_ok(),
        "guarded prune failed: {prune_result:?}"
    );
    let reader_error = reader_result.expect_err("racing read must not return a stale proof");
    assert!(
        reader_error.contains("unreadable"),
        "unexpected racing reader error: {reader_error}"
    );
    assert_cloud_object_missing(&storage, &key);
}

#[test]
fn should_not_prune_remote_wal_when_manifest_sst_disappears_after_initial_validation() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let segment_id = 13;
    let max_sequence = 23;
    let wal_key = write_authoritative_cloud_wal(
        &storage,
        segment_id,
        max_sequence,
        valid_wal_bytes(max_sequence),
    );
    let sst_name = "missing-after-validation.sst";
    let sst_key = crate::sst::object_key(sst_name);
    let sst_bytes = valid_sst_bytes(b"k", b"v1", max_sequence);
    let manifest = manifest_covering_wal(sst_name, &sst_bytes, max_sequence, None);

    write_cloud_object(&storage, &sst_key, sst_bytes);
    storage
        .verify_remote_wal_segment(segment_id, max_sequence)
        .expect("initial remote WAL validation");
    storage
        .verify_manifest_cloud_objects(&manifest)
        .expect("initial manifest SST validation");

    delete_cloud_object(&storage, &sst_key);
    let error = storage
        .prune_cloud_wal_segment(
            segment_id,
            max_sequence,
            CloudWalPruneGuard::new(manifest.clone(), None),
            2,
        )
        .expect_err("missing manifest SST must reject prune");

    // Act
    // Assert
    assert!(
        error.contains("unreadable") || error.contains("not found"),
        "unexpected missing manifest SST error: {error}"
    );
    assert_cloud_object_exists(&storage, &wal_key);
}

#[test]
fn should_not_prune_remote_wal_given_manifest_validation_failure_when_gc_runs() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let segment_id = 16;
    let max_sequence = 26;
    let wal_key = write_authoritative_cloud_wal(
        &storage,
        segment_id,
        max_sequence,
        valid_wal_bytes(max_sequence),
    );
    let sst_name = "wrong-crc-prune-guard.sst";
    let sst_key = crate::sst::object_key(sst_name);
    let sst_bytes = valid_sst_bytes(b"k", b"v1", max_sequence);
    let wrong_crc = crc32c::crc32c(&sst_bytes) ^ 0xffff_ffff;
    let manifest = manifest_covering_wal(sst_name, &sst_bytes, max_sequence, Some(wrong_crc));

    write_cloud_object(&storage, &sst_key, sst_bytes);
    storage
        .verify_remote_wal_segment(segment_id, max_sequence)
        .expect("initial remote WAL validation");
    let error = storage
        .prune_cloud_wal_segment(
            segment_id,
            max_sequence,
            CloudWalPruneGuard::new(manifest, None),
            2,
        )
        .expect_err("incorrect manifest CRC must reject prune");

    // Act
    // Assert
    assert!(
        error.contains("crc32c"),
        "unexpected manifest CRC error: {error}"
    );
    assert_cloud_object_exists(&storage, &wal_key);
}

#[cfg(feature = "failpoints")]
#[test]
fn should_not_prune_remote_wal_given_remote_sst_identity_change_when_gc_runs() {
    // Arrange
    let _test_guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    let (mock_cloud, storage) = hybrid_with_mock_cloud();
    let segment_id = 17;
    let max_sequence = 27;
    let wal_key = write_authoritative_cloud_wal(
        &storage,
        segment_id,
        max_sequence,
        valid_wal_bytes(max_sequence),
    );
    let sst_name = "changed-after-validation.sst";
    let sst_key = crate::sst::object_key(sst_name);
    let original = valid_sst_bytes(b"k", b"v1", max_sequence);
    let replacement = valid_sst_bytes(b"k", b"v2", max_sequence);
    let manifest = manifest_covering_wal(
        sst_name,
        &original,
        max_sequence,
        Some(crc32c::crc32c(&original)),
    );
    write_cloud_object(&storage, &sst_key, original);
    storage
        .verify_remote_wal_segment(segment_id, max_sequence)
        .expect("initial remote WAL validation");
    let callback_backend = Arc::clone(&mock_cloud);
    let callback_key = format!("hybrid-test/{sst_key}");
    let callback_replacement = replacement.clone();
    fail::cfg_callback(
        "midge::cloud::after_wal_prune_dependency_validation",
        move || {
            let (tx, rx) = std::sync::mpsc::channel();
            CloudBackend::submit_put(
                callback_backend.as_ref(),
                &callback_key,
                callback_replacement.clone(),
                Vec::new(),
                tx,
            );
            assert!(matches!(
                rx.recv_timeout(Duration::from_secs(1)),
                Ok(crate::storage::cloud::CloudEvent::Put {
                    result: crate::storage::cloud::CloudOutcome::Ok(()),
                    ..
                })
            ));
        },
    )
    .expect("configure SST identity replacement boundary");

    // Act
    let error = storage
        .prune_cloud_wal_segment(
            segment_id,
            max_sequence,
            CloudWalPruneGuard::new(manifest, None),
            2,
        )
        .expect_err("changed SST identity must reject WAL authority retirement");
    fail::remove("midge::cloud::after_wal_prune_dependency_validation");
    scenario.teardown();

    // Assert
    assert!(
        error.contains("identity changed before conditional delete"),
        "unexpected changed SST proof error: {error}"
    );
    let catalog_proof = storage
        .remote_object_proof(crate::wal::cloud_catalog::OBJECT_KEY)
        .expect("read retained WAL catalog");
    let catalog = crate::wal::cloud_catalog::WalPublicationCatalog::decode(catalog_proof.bytes())
        .expect("decode retained WAL catalog");
    assert!(catalog.segments.contains_key(&segment_id));
    assert_cloud_object_exists(&storage, &wal_key);
    assert_eq!(read_cloud_object(&storage, &sst_key), replacement);
}

#[test]
fn should_not_prune_remote_wal_when_cloud_metadata_changes_after_initial_validation() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let segment_id = 14;
    let max_sequence = 24;
    let wal_key = write_authoritative_cloud_wal(
        &storage,
        segment_id,
        max_sequence,
        valid_wal_bytes(max_sequence),
    );
    let sst_name = "metadata-guard.sst";
    let sst_key = crate::sst::object_key(sst_name);
    let sst_bytes = valid_sst_bytes(b"k", b"v1", max_sequence);
    let manifest = manifest_covering_wal(
        sst_name,
        &sst_bytes,
        max_sequence,
        Some(crc32c::crc32c(&sst_bytes)),
    );
    let metadata_backend = Arc::new(MockCloudBackend::new());
    let metadata_cloud = Arc::new(CloudStorage::new(
        metadata_backend,
        "metadata-test".to_string(),
    ));
    let metadata_key = crate::storage::cloud::cloud_metadata_key("manifest.json");
    let metadata_bytes = br#"{"last_persisted_sequence":24}"#.to_vec();

    write_cloud_object(&storage, &sst_key, sst_bytes);
    storage
        .verify_remote_wal_segment(segment_id, max_sequence)
        .expect("initial remote WAL validation");
    write_cloud_metadata_object(&metadata_cloud, &metadata_key, metadata_bytes.clone());
    let metadata_guard = CloudMetadataPruneGuard::new(
        metadata_cloud.clone(),
        vec![CloudMetadataPruneProof {
            key: metadata_key.clone(),
            expected_bytes: metadata_bytes,
            remote: head_cloud_metadata_object(&metadata_cloud, &metadata_key),
        }],
    );

    write_cloud_metadata_object(&metadata_cloud, &metadata_key, b"changed".to_vec());
    let error = storage
        .prune_cloud_wal_segment(
            segment_id,
            max_sequence,
            CloudWalPruneGuard::new(manifest, Some(metadata_guard)),
            2,
        )
        .expect_err("changed metadata proof must reject authority retirement");

    let catalog_proof = storage
        .remote_object_proof(crate::wal::cloud_catalog::OBJECT_KEY)
        .expect("read catalog after prune scheduling");
    let catalog = crate::wal::cloud_catalog::WalPublicationCatalog::decode(catalog_proof.bytes())
        .expect("decode catalog after prune scheduling");
    assert!(
        catalog.segments.contains_key(&segment_id),
        "catalog authority must remain when a coverage dependency changed"
    );
    // Act
    // Assert
    assert!(
        error.contains("changed before conditional delete")
            || error.contains("identity changed before conditional delete"),
        "unexpected changed metadata proof error: {error}"
    );
    assert_cloud_object_exists(&storage, &wal_key);
}

#[test]
fn should_prune_remote_wal_when_worker_side_guard_remains_valid() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let segment_id = 15;
    let max_sequence = 25;
    let wal_key = write_authoritative_cloud_wal(
        &storage,
        segment_id,
        max_sequence,
        valid_wal_bytes(max_sequence),
    );
    let sst_name = "guard-valid.sst";
    let sst_key = crate::sst::object_key(sst_name);
    let sst_bytes = valid_sst_bytes(b"k", b"v1", max_sequence);
    let manifest = crate::metadata::Manifest {
        files: vec![crate::metadata::FileMeta {
            name: sst_name.to_string(),
            level: 0,
            size_bytes: sst_bytes.len() as u64,
            content_crc32c: Some(crc32c::crc32c(&sst_bytes)),
            cf_id: 0,
            smallest_key: Some(b"k".to_vec()),
            largest_key: Some(b"k".to_vec()),
            smallest_seq: Some(max_sequence),
            largest_seq: Some(max_sequence),
            ..Default::default()
        }],
        ..Default::default()
    };
    let metadata_backend = Arc::new(MockCloudBackend::new());
    let metadata_cloud = Arc::new(CloudStorage::new(
        metadata_backend,
        "metadata-test".to_string(),
    ));
    let metadata_key = crate::storage::cloud::cloud_metadata_key("manifest.json");
    let metadata_bytes = br#"{"last_persisted_sequence":25}"#.to_vec();

    write_cloud_object(&storage, &sst_key, sst_bytes.clone());
    storage
        .verify_remote_wal_segment(segment_id, max_sequence)
        .expect("initial remote WAL validation");
    storage
        .verify_manifest_cloud_objects(&manifest)
        .expect("initial manifest SST validation");
    write_cloud_metadata_object(&metadata_cloud, &metadata_key, metadata_bytes.clone());
    let metadata_guard = CloudMetadataPruneGuard::new(
        metadata_cloud.clone(),
        vec![CloudMetadataPruneProof {
            key: metadata_key.clone(),
            expected_bytes: metadata_bytes,
            remote: head_cloud_metadata_object(&metadata_cloud, &metadata_key),
        }],
    );

    storage
        .prune_cloud_wal_segment(
            segment_id,
            max_sequence,
            CloudWalPruneGuard::new(manifest, Some(metadata_guard)),
            2,
        )
        .expect("schedule prune");

    let catalog_proof = storage
        .remote_object_proof(crate::wal::cloud_catalog::OBJECT_KEY)
        .expect("read catalog after prune scheduling");
    let catalog = crate::wal::cloud_catalog::WalPublicationCatalog::decode(catalog_proof.bytes())
        .expect("decode catalog after prune scheduling");
    assert!(
        !catalog.segments.contains_key(&segment_id),
        "catalog authority must retire before physical delete completion"
    );

    let result = wait_for_wal_prune_result(&storage, segment_id);
    // Act
    // Assert
    assert!(
        result.is_ok(),
        "valid worker-side guard should allow conditional remote WAL deletion"
    );
    assert_cloud_object_missing(&storage, &wal_key);
}

#[test]
fn should_reject_remote_wal_segment_with_sequence_beyond_expected_max() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let segment_id = 10;
    let expected_max_sequence = 20;
    write_authoritative_cloud_wal(
        &storage,
        segment_id,
        expected_max_sequence,
        valid_wal_bytes(expected_max_sequence + 1),
    );

    let error = storage
        .verify_remote_wal_segment(segment_id, expected_max_sequence)
        .expect_err("WAL segment with records beyond expected max must be rejected");
    // Act
    // Assert
    assert!(
        error.contains("exceeds expected"),
        "unexpected WAL max-sequence error: {error}"
    );
}

#[test]
fn should_not_overwrite_existing_remote_wal_during_upload() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let tmp = tempfile::tempdir().expect("create WAL dir");
    let segment_id = 12;
    let upload_max_sequence = 22;
    let key = crate::wal::cloud_segment::object_key(segment_id, 1);
    let existing_bytes = valid_wal_bytes(upload_max_sequence + 100);
    write_cloud_object(&storage, &key, existing_bytes.clone());

    let wal_path = tmp.path().join(crate::wal::segment_file_name(segment_id));
    std::fs::write(&wal_path, valid_wal_bytes(upload_max_sequence)).expect("write local WAL");
    storage
        .enqueue_wal_segment(segment_id, &wal_path, upload_max_sequence)
        .expect("enqueue WAL upload");
    storage.process_uploads();

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut saw_fail = false;
    let mut saw_ack = false;
    while Instant::now() < deadline {
        for event in storage.process_uploads() {
            match event {
                StorageEvent::CloudFail {
                    segment_id: failed_segment,
                    ..
                } if failed_segment == segment_id => saw_fail = true,
                StorageEvent::CloudAck {
                    segment_id: acked_segment,
                    ..
                } if acked_segment == segment_id => saw_ack = true,
                _ => {}
            }
        }
        if saw_fail || saw_ack {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    // Act
    // Assert
    assert!(saw_fail, "existing remote WAL object must fail upload");
    assert!(
        !saw_ack,
        "existing remote WAL object must not produce a CloudAck"
    );
    assert_eq!(
        read_cloud_object(&storage, &key),
        existing_bytes,
        "upload must not overwrite an existing remote WAL object"
    );
}

#[test]
fn should_readback_remote_wal_before_upload_worker_emits_ack() {
    // Arrange
    let (mock_cloud, storage) = hybrid_with_mock_cloud();
    let tmp = tempfile::tempdir().expect("create WAL dir");
    let segment_id = 9;
    let max_sequence = 13;
    let wal_path = tmp.path().join(crate::wal::segment_file_name(segment_id));
    std::fs::write(&wal_path, valid_wal_bytes(max_sequence)).expect("write local WAL");

    mock_cloud.clear_history();
    storage
        .enqueue_wal_segment(segment_id, &wal_path, max_sequence)
        .expect("enqueue WAL upload");
    storage.process_uploads();

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut events = Vec::new();
    while Instant::now() < deadline {
        events.extend(storage.process_uploads());
        if events
            .iter()
            .any(|event| matches!(event, StorageEvent::CloudAck { .. }))
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    // Act
    // Assert
    assert!(
        events.iter().any(|event| matches!(
            event,
            StorageEvent::CloudAck {
                segment_id: 9,
                max_sequence: 13
            }
        )),
        "worker should emit CloudAck after upload and readback, got {events:?}"
    );
    assert!(
        mock_cloud
            .get_downloads()
            .iter()
            .any(|key| key.ends_with("wal/epochs/00000000000000000001/00000000000000000009.wal")),
        "upload worker must read back the remote WAL before ack"
    );
}

#[test]
fn should_stop_retrying_failed_wal_upload_after_retry_budget_exhausted() {
    // Arrange
    let tmp = tempfile::tempdir().expect("create WAL retry test dir");
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(tmp.path().join("local"))
            .expect("create local backend"),
    );
    let write_attempts = Arc::new(AtomicUsize::new(0));
    let cloud = Arc::new(AlwaysFailingWriteBackend::new(Arc::clone(&write_attempts)));
    let storage = HybridStorage::with_policy(
        local,
        cloud,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    );
    let segment_id = 11;
    let max_sequence = 21;
    let wal_path = tmp.path().join(crate::wal::segment_file_name(segment_id));
    std::fs::write(&wal_path, valid_wal_bytes(max_sequence)).expect("write local WAL");

    storage
        .enqueue_wal_segment(segment_id, &wal_path, max_sequence)
        .expect("enqueue WAL upload");

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut observed_failures = 0usize;
    while Instant::now() < deadline {
        let events = storage.process_uploads();
        observed_failures += events
            .iter()
            .filter(|event| matches!(event, StorageEvent::CloudFail { .. }))
            .count();
        if storage.pending_upload_count() == 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    // Act
    // Assert
    assert_eq!(
        write_attempts.load(Ordering::SeqCst),
        3,
        "permanently failing WAL uploads should stop after the retry budget"
    );
    assert_eq!(
        observed_failures, 3,
        "each failed WAL upload attempt should surface exactly one CloudFail"
    );
    assert_eq!(
        storage.pending_upload_count(),
        0,
        "exhausted WAL uploads must leave the queue so cleanup and shutdown are bounded"
    );
}

#[test]
fn should_reject_wal_upload_when_entry_or_byte_capacity_is_exhausted() {
    // Arrange
    let tmp = tempfile::tempdir().expect("create bounded upload test dir");
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(tmp.path().join("local"))
            .expect("create local backend"),
    );
    let cloud = Arc::new(NeverCompletesBackend::default());
    let first_path = tmp.path().join(crate::wal::segment_file_name(1));
    let second_path = tmp.path().join(crate::wal::segment_file_name(2));
    let wal_bytes = valid_wal_bytes(1);
    std::fs::write(&first_path, &wal_bytes).expect("write first WAL");
    std::fs::write(&second_path, &wal_bytes).expect("write second WAL");
    let limits = HybridQueueLimits {
        upload_entries: 1,
        upload_bytes: wal_bytes.len() as u64,
        callback_timeout: Duration::from_millis(20),
        ..HybridQueueLimits::default()
    };
    let storage = HybridStorage::with_policy_event_sender_and_limits(
        local,
        cloud,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        None,
        limits,
    );
    storage
        .enqueue_wal_segment(1, &first_path, 1)
        .expect("first upload must fit");
    assert!(matches!(
        storage.ensure_wal_write_admission(),
        Err(crate::common::MidgeError::WriteStall(_))
    ));

    // Act
    let error = storage
        .enqueue_wal_segment(2, &second_path, 1)
        .expect_err("second upload must be rejected at capacity");

    // Assert
    assert!(matches!(error, crate::common::MidgeError::WriteStall(_)));
    assert!(matches!(
        storage.ensure_wal_write_admission(),
        Err(crate::common::MidgeError::WriteStall(_))
    ));
    assert_eq!(storage.pending_upload_count(), 1);
    assert_eq!(storage.pending_upload_bytes(), wal_bytes.len() as u64);
}

#[test]
fn should_restore_wal_admission_after_upload_capacity_drains() {
    // Arrange
    let tmp = tempfile::tempdir().expect("create admission release test dir");
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(tmp.path().join("local"))
            .expect("create local backend"),
    );
    let mock_cloud = Arc::new(MockCloudBackend::new());
    let cloud = Arc::new(CloudStorage::new(
        mock_cloud,
        "bounded-admission".to_string(),
    ));
    let limits = HybridQueueLimits {
        upload_entries: 1,
        ..HybridQueueLimits::default()
    };
    let storage = HybridStorage::with_policy_event_sender_and_limits(
        local,
        cloud,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        None,
        limits,
    );
    let first_path = tmp.path().join(crate::wal::segment_file_name(21));
    let second_path = tmp.path().join(crate::wal::segment_file_name(22));
    std::fs::write(&first_path, valid_wal_bytes(21)).expect("write first WAL");
    std::fs::write(&second_path, valid_wal_bytes(22)).expect("write second WAL");
    storage
        .enqueue_wal_segment(21, &first_path, 21)
        .expect("enqueue first upload");
    let _ = storage
        .enqueue_wal_segment(22, &second_path, 22)
        .expect_err("capacity must reject second upload");
    assert!(storage.ensure_wal_write_admission().is_err());

    // Act
    storage.process_uploads();
    let deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < deadline && storage.ensure_wal_write_admission().is_err() {
        storage.process_uploads();
        std::thread::sleep(Duration::from_millis(5));
    }

    // Assert
    storage
        .ensure_wal_write_admission()
        .expect("completed upload must reopen admission");
    assert_eq!(storage.pending_upload_count(), 0);
}

#[test]
fn should_release_storage_reservation_given_upload_callback_is_lost_when_worker_times_out() {
    // Arrange
    let tmp = tempfile::tempdir().expect("create callback timeout test dir");
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(tmp.path().join("local"))
            .expect("create local backend"),
    );
    let cloud = Arc::new(NeverCompletesBackend::default());
    let limits = HybridQueueLimits {
        callback_timeout: Duration::from_millis(20),
        upload_entries: 1,
        ..HybridQueueLimits::default()
    };
    let storage = HybridStorage::with_policy_event_sender_and_limits(
        local,
        cloud,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        None,
        limits,
    );
    let wal_path = tmp.path().join(crate::wal::segment_file_name(3));
    std::fs::write(&wal_path, valid_wal_bytes(3)).expect("write WAL");
    storage
        .enqueue_wal_segment(3, &wal_path, 3)
        .expect("enqueue WAL upload");
    assert!(
        storage.ensure_wal_write_admission().is_err(),
        "the only queue reservation must close write admission while upload is stuck"
    );
    storage.process_uploads();

    // Act
    let deadline = Instant::now() + Duration::from_secs(1);
    let mut timeout_failures = 0;
    while Instant::now() < deadline && storage.pending_upload_count() != 0 {
        timeout_failures += storage
            .process_uploads()
            .into_iter()
            .filter(|event| {
                matches!(
                    event,
                    StorageEvent::CloudFail { error, .. } if error.contains("timed out")
                )
            })
            .count();
        if storage.pending_upload_count() == 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }

    // Assert
    assert_eq!(timeout_failures, 3, "every bounded retry must time out");
    assert_eq!(storage.pending_upload_count(), 0);
    storage
        .ensure_wal_write_admission()
        .expect("terminal callback timeouts must release the queue reservation");
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
    assert!(entry_error.contains("entries=1/1"));
    assert!(byte_error.contains("bytes="));
    assert_eq!(entry_limited.terminal_entries.len(), 1);
    assert_eq!(byte_limited.terminal_pending_bytes, ack_bytes);
}
