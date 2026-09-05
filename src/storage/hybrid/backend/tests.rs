use super::*;
use crate::runtime::hybrid_persistence::{
    CloudMetadataPruneGuard, CloudMetadataPruneProof, CloudWalPruneGuard, HybridPersistence,
};
use crate::sst::SstFactory;
use crate::storage::cloud::{CloudBackend, CloudStorage, MockCloudBackend};
use bytes::Bytes;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Barrier,
};
use std::time::{Duration, Instant};

#[test]
fn should_prune_wal_without_fetching_unrelated_ssts_when_local_cache_is_ephemeral() {
    // Arrange
    let (cloud, storage) = hybrid_with_mock_cloud();
    storage.enable_ephemeral_sst_cache(1024 * 1024);
    let sequence = 31;
    write_authoritative_cloud_wal(&storage, 1, sequence, valid_wal_bytes(sequence));
    let name = "relevant.sst";
    let bytes = valid_sst_bytes(b"k", b"v", sequence);
    let mut manifest = manifest_covering_wal(name, &bytes, sequence, Some(crc32c::crc32c(&bytes)));
    write_cloud_object(&storage, &crate::sst::object_key(name), bytes);
    manifest.files.push(crate::metadata::FileMeta {
        name: "unrelated.sst".into(),
        cf_id: 0,
        smallest_key: Some(b"z".to_vec()),
        largest_key: Some(b"zz".to_vec()),
        key_bounds_complete: true,
        ..Default::default()
    });
    cloud.clear_history();

    // Act
    storage
        .prune_cloud_wal_segment(1, sequence, CloudWalPruneGuard::new(manifest, None), 2)
        .expect("exactly covered WAL retirement");

    // Assert
    assert!(wait_for_wal_prune_result(&storage, 1).is_ok());
    assert!(
        !cloud.get_downloads().iter().any(|key| key.contains("sst/")),
        "WAL cleanup must use bounded ranges, not whole SST downloads"
    );
}

#[test]
fn should_publish_sst_without_duplicate_local_copy_when_ephemeral_cache_is_enabled() {
    // Arrange
    let (_cloud, storage) = hybrid_with_mock_cloud();
    storage.enable_ephemeral_sst_cache(20 * 1024 * 1024 * 1024);
    let name = "ephemeral.sst";
    let key = crate::sst::object_key(name);
    let bytes = valid_sst_bytes(b"key", b"value", 1);

    // Act
    storage
        .write_sst_object(name, bytes.clone())
        .expect("remote publication");

    // Assert
    assert_eq!(read_cloud_object(&storage, &key), bytes);
    let exists = HybridStorage::object_exists_in_backend_within(
        &storage.local,
        &key,
        storage.callback_timeout,
        &crate::common::OperationDeadline::unbounded(),
    )
    .expect("local cache existence check");
    assert!(
        !exists,
        "remote publication must not duplicate the runtime SST staging file"
    );
}

#[test]
fn should_preserve_remote_sst_when_evicting_legacy_local_cache() {
    // Arrange
    let (_cloud, storage) = hybrid_with_mock_cloud();
    let name = "legacy-local.sst";
    let key = crate::sst::object_key(name);
    let bytes = valid_sst_bytes(b"key", b"value", 1);
    storage
        .write_sst_object(name, bytes.clone())
        .expect("publish both copies");
    storage.enable_ephemeral_sst_cache(20 * 1024 * 1024 * 1024);

    // Act
    storage
        .evict_local_object_cache(&key)
        .expect("local cache eviction");

    // Assert
    assert_eq!(read_cloud_object(&storage, &key), bytes);
    assert_eq!(read_hybrid_object(&storage, &key), bytes);
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
        .remote_object_proof_optional(crate::wal::cloud_catalog::OBJECT_KEY)
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
    assert_eq!(cas_server.finish(), 1);
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
fn should_classify_only_normalized_missing_object_errors_as_absent() {
    // Arrange
    let errors = [
        "not found: S3 GET failed: HTTP 404 NoSuchKey",
        "not found: Azure Blob request failed: 404 BlobNotFound",
        "not found: read C:\\data\\missing.sst: os error 2",
    ];

    // Act
    let all_missing = errors
        .iter()
        .all(|error| HybridStorage::storage_error_indicates_missing(error));

    // Assert
    assert!(all_missing);
}

#[test]
fn should_not_classify_credential_file_failure_as_missing_object() {
    // Arrange
    let error =
        "transport error: Internal(\"failed to read workload token: No such file or directory\")";

    // Act
    let missing = HybridStorage::storage_error_indicates_missing(error);

    // Assert
    assert!(!missing);
}

#[test]
fn should_not_classify_unrelated_numeric_diagnostics_as_absent() {
    // Arrange
    let errors = [
        "protocol error: expected metadata length 404, got 17",
        "server error: status 503: upstream request id 404",
    ];

    // Act
    let any_missing = errors
        .iter()
        .any(|error| HybridStorage::storage_error_indicates_missing(error));

    // Assert
    assert!(!any_missing);
}

#[test]
fn should_only_classify_structured_precondition_failure_as_cas_conflict() {
    // Arrange
    let conflicts = [
        "precondition failed: status 412: S3 PUT",
        "precondition failed",
    ];
    let unrelated = [
        "protocol error: GCS JSON PUT cannot enforce If-Match; use a generation precondition",
        "protocol error: precondition check failed: unauthorized",
        "invalid request: object already exists in a retained snapshot",
    ];

    // Act
    let all_conflicts = conflicts
        .iter()
        .all(|error| HybridStorage::storage_error_indicates_precondition_failure(error));
    let any_unrelated = unrelated
        .iter()
        .any(|error| HybridStorage::storage_error_indicates_precondition_failure(error));

    // Assert
    assert!(all_conflicts);
    assert!(!any_unrelated);
}

#[test]
fn should_classify_only_canonical_storage_timeout_errors_as_timeouts() {
    // Arrange: provider detail text is untrusted diagnostic content. Merely
    // mentioning "timeout" must not change a public error into Timeout.
    let timeout = crate::storage::storage_timeout_error("cloud HEAD callback expired");
    let unrelated = [
        "unauthorized: workload token for timeout.example was rejected",
        "invalid request: timeout must be a positive integer",
        "protocol error: response included x-timeout metadata",
    ];

    // Act
    let classified_timeout = HybridStorage::storage_error_indicates_timeout(&timeout);
    let any_unrelated = unrelated
        .iter()
        .any(|error| HybridStorage::storage_error_indicates_timeout(error));

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
    let result = storage.publish_remote_wal_segment(
        segment_id,
        max_sequence,
        &local_path,
        2,
        &crate::common::OperationDeadline::unbounded(),
    );

    // Assert
    let error = result.expect_err("stale lease epoch must not publish WAL authority");
    assert!(matches!(
        error,
        crate::common::MidgeError::Fenced(message)
            if message.contains("requires fencing epoch 3")
    ));
    let catalog_proof = storage
        .remote_object_proof(crate::wal::cloud_catalog::OBJECT_KEY)
        .expect("read winning WAL catalog");
    let catalog = crate::wal::cloud_catalog::WalPublicationCatalog::decode(catalog_proof.bytes())
        .expect("decode winning WAL catalog");
    assert!(!catalog.segments.contains_key(&segment_id));
    assert_cloud_object_exists(&storage, &object_key);
}

#[test]
fn should_converge_catalog_copies_after_publication() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let tmp = tempfile::tempdir().expect("create mirrored catalog WAL dir");
    let segment_id = 1;
    let max_sequence = 11;
    let bytes = valid_wal_bytes(max_sequence);
    let local_path = tmp.path().join(crate::wal::segment_file_name(segment_id));
    std::fs::write(&local_path, &bytes).expect("write mirrored catalog local WAL");
    let object_key = crate::wal::cloud_segment_object_key(segment_id, 1);
    write_cloud_object(&storage, &object_key, bytes);

    // Act
    storage
        .publish_remote_wal_segment(
            segment_id,
            max_sequence,
            &local_path,
            2,
            &crate::common::OperationDeadline::unbounded(),
        )
        .expect("publish WAL through mirrored catalog");

    // Assert
    let catalog = assert_wal_catalog_copies_match(&storage);
    assert_eq!(catalog.fencing_epoch, 2);
    assert!(catalog.segments.contains_key(&segment_id));
}

#[test]
fn should_converge_catalog_copies_after_takeover() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();

    // Act
    storage
        .fence_cloud_wal_catalog(3)
        .expect("advance mirrored catalog authority");

    // Assert
    let catalog = assert_wal_catalog_copies_match(&storage);
    assert_eq!(catalog.fencing_epoch, 3);
}

#[test]
fn should_not_publish_losing_mirror_when_initial_catalog_cas_loses() {
    // Arrange
    let tmp = tempfile::tempdir().expect("create initial catalog race test dir");
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(tmp.path().join("local"))
            .expect("create local backend"),
    );
    let mock_cloud = Arc::new(MockCloudBackend::new());
    let winning_catalog = crate::wal::cloud_catalog::WalPublicationCatalog::empty(7)
        .expect("create winning catalog")
        .encode()
        .expect("encode winning catalog");
    let racing_cloud = Arc::new(WinningInitialCatalogCasBackend {
        inner: Arc::clone(&mock_cloud),
        winning_catalog: winning_catalog.clone(),
        inject_winner: AtomicBool::new(true),
    });
    let cloud = Arc::new(CloudStorage::new(racing_cloud, String::new()));
    let storage = HybridStorage::with_policy(
        local,
        cloud,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    );

    // Act
    let error = storage
        .fence_cloud_wal_catalog(2)
        .expect_err("losing initial catalog CAS must fail");

    // Assert
    assert!(matches!(error, crate::common::MidgeError::Busy(_)));
    assert_eq!(
        storage
            .remote_object_proof(crate::wal::cloud_catalog::OBJECT_KEY)
            .expect("read winning primary catalog")
            .bytes(),
        winning_catalog
    );
    assert!(
        storage
            .remote_object_proof_optional(crate::wal::cloud_catalog::MIRROR_OBJECT_KEY)
            .expect("check losing mirror")
            .is_none(),
        "a draft that lost primary authority must never be published to the mirror"
    );
}

#[test]
fn should_report_fenced_when_catalog_authority_changes_before_publication() {
    // Arrange
    let tmp = tempfile::tempdir().expect("create takeover race test dir");
    let (pausing_cloud, storage) = hybrid_with_pausing_catalog_cloud(tmp.path());

    let segment_id = 1;
    let max_sequence = 11;
    let bytes = valid_wal_bytes(max_sequence);
    let local_path = tmp.path().join(crate::wal::segment_file_name(segment_id));
    std::fs::write(&local_path, &bytes).expect("write publisher local WAL");
    let object_key = crate::wal::cloud_segment::object_key(segment_id, 1);
    write_cloud_object(&storage, &object_key, bytes);

    pausing_cloud.pause_next_catalog_compare_exchange();
    let publisher_storage = Arc::clone(&storage);
    let publisher = std::thread::spawn(move || {
        publisher_storage.publish_remote_wal_segment(
            segment_id,
            max_sequence,
            &local_path,
            2,
            &crate::common::OperationDeadline::unbounded(),
        )
    });
    pausing_cloud.wait_for_catalog_compare_exchange();
    storage
        .fence_cloud_wal_catalog(3)
        .expect("takeover must advance catalog authority");

    // Act
    pausing_cloud.release_catalog_compare_exchange();
    let error = publisher
        .join()
        .expect("publisher thread must not panic")
        .expect_err("stale publisher must lose catalog authority");

    // Assert
    assert!(matches!(error, crate::common::MidgeError::Fenced(_)));
    let catalog_proof = storage
        .remote_object_proof(crate::wal::cloud_catalog::OBJECT_KEY)
        .expect("read takeover catalog");
    let catalog = crate::wal::cloud_catalog::WalPublicationCatalog::decode(catalog_proof.bytes())
        .expect("decode takeover catalog");
    assert_eq!(catalog.fencing_epoch, 3);
    assert!(!catalog.segments.contains_key(&segment_id));
    assert_cloud_object_exists(&storage, &object_key);
}

#[test]
fn should_preserve_same_epoch_catalog_updates_when_publication_races_retirement() {
    // Arrange
    let tmp = tempfile::tempdir().expect("create catalog mutation race test dir");
    let (pausing_cloud, storage) = hybrid_with_pausing_catalog_cloud(tmp.path());

    let retired_segment_id = 1;
    let retired_max_sequence = 11;
    write_authoritative_cloud_wal(
        &storage,
        retired_segment_id,
        retired_max_sequence,
        valid_wal_bytes(retired_max_sequence),
    );
    let sst_name = "catalog-mutation-race.sst";
    let sst_bytes = valid_sst_bytes(b"k", b"v", retired_max_sequence);
    write_cloud_object(
        &storage,
        &crate::sst::object_key(sst_name),
        sst_bytes.clone(),
    );
    let manifest = manifest_covering_wal(
        sst_name,
        &sst_bytes,
        retired_max_sequence,
        Some(crc32c::crc32c(&sst_bytes)),
    );

    let published_segment_id = 2;
    let published_max_sequence = 12;
    let published_bytes = valid_wal_bytes(published_max_sequence);
    let published_local_path = tmp
        .path()
        .join(crate::wal::segment_file_name(published_segment_id));
    std::fs::write(&published_local_path, &published_bytes)
        .expect("write concurrently published local WAL");
    let published_object_key = crate::wal::cloud_segment::object_key(published_segment_id, 1);
    write_cloud_object(&storage, &published_object_key, published_bytes);

    pausing_cloud.pause_next_catalog_compare_exchange();
    let publisher_storage = Arc::clone(&storage);
    let publisher = std::thread::spawn(move || {
        publisher_storage.publish_remote_wal_segment(
            published_segment_id,
            published_max_sequence,
            &published_local_path,
            2,
            &crate::common::OperationDeadline::unbounded(),
        )
    });
    pausing_cloud.wait_for_catalog_compare_exchange();

    let (prune_tx, prune_rx) = std::sync::mpsc::channel();
    let pruner_storage = Arc::clone(&storage);
    let pruner = std::thread::spawn(move || {
        let result = pruner_storage.prune_cloud_wal_segment(
            retired_segment_id,
            retired_max_sequence,
            CloudWalPruneGuard::new(manifest, None),
            2,
        );
        prune_tx
            .send(result)
            .expect("send concurrent catalog retirement result");
    });
    let early_prune_result = match prune_rx.recv_timeout(Duration::from_millis(100)) {
        Ok(result) => Some(result),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => None,
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            panic!("catalog retirement thread disconnected before publication release")
        }
    };

    // Act
    pausing_cloud.release_catalog_compare_exchange();
    let publication_result = publisher.join().expect("publisher thread must not panic");
    let retirement_result = early_prune_result.unwrap_or_else(|| {
        prune_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("catalog retirement must complete after publication release")
    });
    pruner
        .join()
        .expect("catalog retirement thread must not panic");

    // Assert
    publication_result.expect("same-epoch WAL publication must succeed");
    retirement_result.expect("same-epoch WAL catalog retirement must succeed");
    let delete_result = wait_for_wal_prune_result(&storage, retired_segment_id);
    assert!(
        delete_result.is_ok(),
        "retired WAL object deletion must succeed: {delete_result:?}"
    );

    let catalog_proof = storage
        .remote_object_proof(crate::wal::cloud_catalog::OBJECT_KEY)
        .expect("read catalog after serialized mutations");
    let catalog = crate::wal::cloud_catalog::WalPublicationCatalog::decode(catalog_proof.bytes())
        .expect("decode catalog after serialized mutations");
    assert!(!catalog.segments.contains_key(&retired_segment_id));
    assert!(catalog.segments.contains_key(&published_segment_id));
    storage
        .verify_remote_wal_segment(published_segment_id, published_max_sequence)
        .expect("newly published segment must remain authoritative");
}

#[test]
fn should_bound_wal_catalog_lock_wait_by_publication_deadline() {
    // Arrange
    let tmp = tempfile::tempdir().expect("create bounded catalog lock test dir");
    let (pausing_cloud, storage) = hybrid_with_pausing_catalog_cloud(tmp.path());

    let holding_segment_id = 1;
    let holding_max_sequence = 11;
    let holding_bytes = valid_wal_bytes(holding_max_sequence);
    let holding_local_path = tmp
        .path()
        .join(crate::wal::segment_file_name(holding_segment_id));
    std::fs::write(&holding_local_path, &holding_bytes).expect("write lock-holding local WAL");
    write_cloud_object(
        &storage,
        &crate::wal::cloud_segment::object_key(holding_segment_id, 1),
        holding_bytes,
    );

    let waiting_segment_id = 2;
    let waiting_max_sequence = 12;
    let waiting_bytes = valid_wal_bytes(waiting_max_sequence);
    let waiting_local_path = tmp
        .path()
        .join(crate::wal::segment_file_name(waiting_segment_id));
    std::fs::write(&waiting_local_path, &waiting_bytes).expect("write lock-waiting local WAL");
    write_cloud_object(
        &storage,
        &crate::wal::cloud_segment::object_key(waiting_segment_id, 1),
        waiting_bytes,
    );

    pausing_cloud.pause_next_catalog_compare_exchange();
    let publisher_storage = Arc::clone(&storage);
    let holder = std::thread::spawn(move || {
        publisher_storage.publish_remote_wal_segment(
            holding_segment_id,
            holding_max_sequence,
            &holding_local_path,
            2,
            &crate::common::OperationDeadline::unbounded(),
        )
    });
    pausing_cloud.wait_for_catalog_compare_exchange();

    // Act
    let started = Instant::now();
    let waiting_result = storage.publish_remote_wal_segment(
        waiting_segment_id,
        waiting_max_sequence,
        &waiting_local_path,
        2,
        &crate::common::OperationDeadline::from_budget(Duration::from_millis(100)),
    );
    let elapsed = started.elapsed();
    pausing_cloud.release_catalog_compare_exchange();
    let holding_result = holder.join().expect("lock holder thread must not panic");

    // Assert
    holding_result.expect("lock-holding publication must finish after release");
    assert!(
        matches!(
            &waiting_result,
            Err(crate::common::MidgeError::Timeout(message))
                if message.contains("waiting to mutate the cloud WAL catalog")
        ),
        "bounded publication must time out waiting for the catalog lock: {waiting_result:?}"
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "catalog lock wait exceeded the shared publication deadline: {elapsed:?}"
    );
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
        .delete_remote_object_guarded(1, target.clone())
        .expect("first guarded delete should occupy the worker");

    // Act
    let started = Instant::now();
    let error = storage
        .delete_remote_object_guarded(2, target)
        .expect_err("second guarded delete must be rejected at worker capacity");

    // Assert
    assert!(error.contains("workers at capacity"), "{error}");
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
    storage
        .fence_cloud_wal_catalog(2)
        .expect("initialize test WAL publication catalog");
    (mock_cloud, storage)
}

fn assert_wal_catalog_copies_match(
    storage: &HybridStorage,
) -> crate::wal::cloud_catalog::WalPublicationCatalog {
    let primary = storage
        .remote_object_proof(crate::wal::cloud_catalog::OBJECT_KEY)
        .expect("read primary WAL catalog");
    let mirror = storage
        .remote_object_proof(crate::wal::cloud_catalog::MIRROR_OBJECT_KEY)
        .expect("read WAL catalog mirror");
    assert_eq!(
        primary.bytes(),
        mirror.bytes(),
        "successful catalog mutations must converge both copies"
    );
    crate::wal::cloud_catalog::WalPublicationCatalog::decode(primary.bytes())
        .expect("decode converged WAL catalog")
}

fn hybrid_with_pausing_catalog_cloud(
    root: &std::path::Path,
) -> (Arc<PausingCatalogCasBackend>, Arc<HybridStorage>) {
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(root.join("local"))
            .expect("create pausing catalog local backend"),
    );
    let mock_cloud = Arc::new(MockCloudBackend::new());
    let pausing_cloud = Arc::new(PausingCatalogCasBackend::new(Arc::clone(&mock_cloud)));
    let cloud = Arc::new(CloudStorage::new(pausing_cloud.clone(), String::new()));
    let storage = Arc::new(HybridStorage::with_policy(
        local,
        cloud,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    ));
    storage
        .fence_cloud_wal_catalog(2)
        .expect("initialize pausing WAL publication catalog");
    (pausing_cloud, storage)
}

struct PausingCatalogCasBackend {
    inner: Arc<MockCloudBackend>,
    pause_next_catalog_compare_exchange: AtomicBool,
    catalog_compare_exchange_started: Barrier,
    release_catalog_compare_exchange: Barrier,
}

struct WinningInitialCatalogCasBackend {
    inner: Arc<MockCloudBackend>,
    winning_catalog: Vec<u8>,
    inject_winner: AtomicBool,
}

impl CloudBackend for WinningInitialCatalogCasBackend {
    fn submit_put(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        let is_initial_catalog_compare_exchange = key == crate::wal::cloud_catalog::OBJECT_KEY
            && headers
                .iter()
                .any(|(name, value)| name.eq_ignore_ascii_case("if-none-match") && value == "*");
        if is_initial_catalog_compare_exchange && self.inject_winner.swap(false, Ordering::SeqCst) {
            let (winner_tx, winner_rx) = std::sync::mpsc::channel();
            self.inner
                .submit_put(key, self.winning_catalog.clone(), Vec::new(), winner_tx);
            winner_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("competing initial catalog write must complete");
        }
        self.inner.submit_put(key, data, headers, callback);
    }

    crate::storage::cloud::forward_cloud_backend!(inner; submit_get, submit_get_with_metadata, submit_get_range, submit_get_range_with_identity, submit_delete, submit_list, submit_head);
}

impl PausingCatalogCasBackend {
    fn new(inner: Arc<MockCloudBackend>) -> Self {
        Self {
            inner,
            pause_next_catalog_compare_exchange: AtomicBool::new(false),
            catalog_compare_exchange_started: Barrier::new(2),
            release_catalog_compare_exchange: Barrier::new(2),
        }
    }

    fn pause_next_catalog_compare_exchange(&self) {
        self.pause_next_catalog_compare_exchange
            .store(true, Ordering::SeqCst);
    }

    fn wait_for_catalog_compare_exchange(&self) {
        self.catalog_compare_exchange_started.wait();
    }

    fn release_catalog_compare_exchange(&self) {
        self.release_catalog_compare_exchange.wait();
    }
}

impl CloudBackend for PausingCatalogCasBackend {
    fn submit_put(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        let is_catalog_compare_exchange = key == crate::wal::cloud_catalog::OBJECT_KEY
            && headers
                .iter()
                .any(|(name, _)| name.eq_ignore_ascii_case("if-match"));
        if is_catalog_compare_exchange
            && self
                .pause_next_catalog_compare_exchange
                .swap(false, Ordering::SeqCst)
        {
            self.catalog_compare_exchange_started.wait();
            self.release_catalog_compare_exchange.wait();
        }
        self.inner.submit_put(key, data, headers, callback);
    }

    crate::storage::cloud::forward_cloud_backend!(inner; submit_get, submit_get_with_metadata, submit_get_range, submit_get_range_with_identity, submit_delete, submit_list, submit_head);
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

struct PanickingWriteBackend {
    write_attempts: Arc<AtomicUsize>,
}

impl AlwaysFailingWriteBackend {
    fn new(write_attempts: Arc<AtomicUsize>) -> Self {
        Self { write_attempts }
    }
}

impl PanickingWriteBackend {
    fn new(write_attempts: Arc<AtomicUsize>) -> Self {
        Self { write_attempts }
    }
}

impl StorageBackend for PanickingWriteBackend {
    fn submit_read(&self, key: &str, callback: StorageCallback) {
        let _ = callback.send(StorageEvent::ReadComplete {
            key: key.to_string(),
            result: StorageOutcome::Err("read unavailable".to_string()),
        });
    }

    fn submit_write(&self, _key: &str, _data: Vec<u8>, _callback: StorageCallback) {
        self.write_attempts.fetch_add(1, Ordering::SeqCst);
        panic!("injected cloud upload worker panic");
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

struct BudgetConsumingProofBackend {
    first_head_delay: Duration,
    head_calls: AtomicUsize,
    retained_callbacks: Mutex<Vec<StorageCallback>>,
    retained_metadata_callbacks: Mutex<Vec<crate::storage::MetadataReadCallback>>,
}

struct BudgetConsumingSstPublicationBackend {
    first_head_delay: Duration,
    head_calls: AtomicUsize,
    retained_callbacks: Mutex<Vec<StorageCallback>>,
}

impl BudgetConsumingSstPublicationBackend {
    fn new(first_head_delay: Duration) -> Self {
        Self {
            first_head_delay,
            head_calls: AtomicUsize::new(0),
            retained_callbacks: Mutex::new(Vec::new()),
        }
    }

    fn retain_callback(&self, callback: StorageCallback) {
        self.retained_callbacks.lock().push(callback);
    }
}

impl StorageBackend for BudgetConsumingSstPublicationBackend {
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

    fn submit_list(&self, prefix: &str, callback: StorageCallback) {
        let _ = callback.send(StorageEvent::ListComplete {
            prefix: prefix.to_string(),
            result: StorageOutcome::Ok(Vec::new()),
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
                    result: StorageOutcome::Err("not found: delayed miss".to_string()),
                });
            });
        } else {
            self.retain_callback(callback);
        }
    }
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
    fn submit_read_with_metadata(
        &self,
        _key: &str,
        _timeout: Duration,
        callback: crate::storage::MetadataReadCallback,
    ) {
        self.retained_metadata_callbacks.lock().push(callback);
    }

    fn submit_read(&self, _key: &str, callback: StorageCallback) {
        self.retain_callback(callback);
    }

    fn submit_write(&self, key: &str, _data: Vec<u8>, callback: StorageCallback) {
        let _ = callback.send(StorageEvent::WriteComplete {
            key: key.to_string(),
            result: StorageOutcome::Err("writes are not used by this proof fixture".to_string()),
        });
    }

    fn submit_delete(&self, key: &str, callback: StorageCallback) {
        let _ = callback.send(StorageEvent::DeleteComplete {
            key: key.to_string(),
            result: StorageOutcome::Err("deletes are not used by this proof fixture".to_string()),
        });
    }

    fn submit_list(&self, prefix: &str, callback: StorageCallback) {
        let _ = callback.send(StorageEvent::ListComplete {
            prefix: prefix.to_string(),
            result: StorageOutcome::Ok(Vec::new()),
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
    fn submit_read_with_metadata(
        &self,
        _key: &str,
        _timeout: Duration,
        callback: crate::storage::MetadataReadCallback,
    ) {
        let snapshot = self.object.lock().clone();
        self.read_started.wait();
        self.release_read.wait();
        let result = snapshot
            .map(|bytes| {
                let metadata = Self::metadata(&bytes);
                (bytes, metadata)
            })
            .ok_or_else(|| "object not found".to_string());
        let _ = callback.send(result);
    }

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

    fn submit_delete_with_headers(
        &self,
        _key: &str,
        _headers: Vec<(String, String)>,
        callback: StorageCallback,
    ) {
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
        .delete_remote_object_guarded(segment_id, stale_proof)
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
        .delete_remote_object_guarded(segment_id, target)
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
    let sst_bytes = valid_sst_bytes(b"k", b"v", max_sequence);
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
    assert_wal_prune_rejects_manifest_crc_mismatch(false);
}

#[test]
fn should_keep_crc_proof_before_wal_retirement_when_reading_remote_sst_ranges() {
    assert_wal_prune_rejects_manifest_crc_mismatch(true);
}

fn assert_wal_prune_rejects_manifest_crc_mismatch(ephemeral: bool) {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    if ephemeral {
        storage.enable_ephemeral_sst_cache(1024 * 1024);
    }
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
    let sst_bytes = valid_sst_bytes(b"k", b"v", max_sequence);
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
    let original = valid_sst_bytes(b"k", b"v", max_sequence);
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
    let sst_bytes = valid_sst_bytes(b"k", b"v", max_sequence);
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
    let sst_bytes = valid_sst_bytes(b"k", b"v", max_sequence);
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
    assert_wal_catalog_copies_match(&storage);

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
    let wal_path = tmp.path().join(crate::wal::segment_file_name(segment_id));
    let wal_bytes = valid_wal_bytes(max_sequence);
    std::fs::write(&wal_path, &wal_bytes).expect("write local WAL");
    let upload = UploadState {
        segment_id,
        object_key: crate::wal::cloud_segment::object_key(segment_id, 1),
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
        &upload,
        &storage.wal_cloud,
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
    let mut observed_terminal_failures = Vec::new();
    while Instant::now() < deadline {
        let events = storage.process_uploads();
        observed_terminal_failures.extend(events.into_iter().filter_map(|event| match event {
            StorageEvent::CloudFail {
                terminal,
                failure_kind,
                ..
            } => Some((terminal, failure_kind)),
            _ => None,
        }));
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
        observed_terminal_failures,
        vec![
            (false, crate::storage::CloudUploadFailureKind::Other),
            (false, crate::storage::CloudUploadFailureKind::Other),
            (true, crate::storage::CloudUploadFailureKind::Other),
        ],
        "only the failure that exhausts storage retry ownership may be terminal"
    );
    assert_eq!(
        storage.pending_upload_count(),
        0,
        "exhausted WAL uploads must leave the queue so cleanup and shutdown are bounded"
    );
}

#[test]
fn should_transfer_wal_retry_ownership_when_upload_worker_panics() {
    // Arrange
    let tmp = tempfile::tempdir().expect("create WAL worker panic test dir");
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(tmp.path().join("local"))
            .expect("create local backend"),
    );
    let write_attempts = Arc::new(AtomicUsize::new(0));
    let cloud = Arc::new(PanickingWriteBackend::new(Arc::clone(&write_attempts)));
    let storage = HybridStorage::with_policy(
        local,
        cloud,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    );
    let segment_id = 12;
    let max_sequence = 22;
    let wal_path = tmp.path().join(crate::wal::segment_file_name(segment_id));
    std::fs::write(&wal_path, valid_wal_bytes(max_sequence)).expect("write local WAL");
    storage
        .enqueue_wal_segment(segment_id, &wal_path, max_sequence)
        .expect("enqueue WAL upload");

    // Act
    let deadline = Instant::now() + Duration::from_millis(500);
    let mut terminal_failure = false;
    while Instant::now() < deadline {
        terminal_failure |= storage.process_uploads().into_iter().any(|event| {
            matches!(
                event,
                StorageEvent::CloudFail {
                    segment_id: failed_segment,
                    terminal: true,
                    ..
                } if failed_segment == segment_id
            )
        });
        if storage.pending_upload_count() == 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }

    // Assert
    assert_eq!(write_attempts.load(Ordering::SeqCst), 3);
    assert!(
        terminal_failure,
        "worker panic must transfer retry ownership"
    );
    assert_eq!(storage.pending_upload_count(), 0);
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
                    StorageEvent::CloudFail {
                        error,
                        failure_kind: crate::storage::CloudUploadFailureKind::Timeout,
                        ..
                    } if error.contains("timed out")
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

#[test]
fn should_bound_sst_publication_with_shared_deadline_when_remote_preflight_consumes_budget() {
    // Arrange: remote HEAD consumes most of the shared budget, then the
    // conditional PUT never answers. A fresh per-call timeout would let this
    // attempt outlive its advertised deadline by roughly two seconds, leaving
    // a full second of scheduler headroom in the bounded assertion.
    let tmp = tempfile::tempdir().expect("create deadline publication directory");
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(tmp.path().join("local"))
            .expect("create deadline publication local backend"),
    );
    let cloud = Arc::new(BudgetConsumingSstPublicationBackend::new(
        Duration::from_millis(300),
    ));
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
    let result = storage.write_sst_object_within(
        "deadline-publication.sst",
        valid_sst_bytes(b"deadline", b"value", 1),
        &deadline,
    );
    let elapsed = started.elapsed();

    // Assert
    assert!(
        matches!(result, Err(crate::common::MidgeError::Timeout(_))),
        "deadline expiry must remain typed as Timeout: {result:?}"
    );
    assert!(
        elapsed < Duration::from_secs(1),
        "SST publication reused a fresh callback timeout: {elapsed:?}"
    );
}
