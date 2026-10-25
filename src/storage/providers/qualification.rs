//! Sqrzl qualification tests for real cloud provider front doors.
//!
//! These tests intentionally assume Sqrzl is already running. Start it with:
//! `docker compose up -d sqrzl`
//! then run with the feature enabled, for example:
//! `cargo test --lib --features sqrzl-tests storage::providers::qualification -- --test-threads=1`

use super::build_cloud_storage;
use crate::config::{CloudPreflightOptions, CloudStorageLocation};
use crate::config::{CloudProviderConfig, OciCredentialSource};
use crate::storage::cloud::{CloudError, CloudEvent, CloudOutcome, CloudStorage, ObjectMetadata};
use std::time::Duration;

#[test]
fn should_run_s3_compatible_contract_against_sqrzl() {
    let provider = CloudProviderConfig::sqrzl_s3("midge-sqrzl-s3");
    run_provider_contract("s3", &provider);
}

#[test]
fn should_run_azure_blob_contract_against_sqrzl() {
    let provider = CloudProviderConfig::sqrzl_azure("midge-sqrzl-azure");
    run_provider_contract("azure", &provider);
}

#[test]
fn should_reject_incorrect_azure_shared_key_for_conditional_write_against_sqrzl() {
    // Arrange: create the namespace with the emulator's actual credential.
    require_sqrzl("azure-wrong-key");
    ensure_sqrzl_azure_container("midge-sqrzl-azure").unwrap();
    let provider = CloudProviderConfig::azure_blob_shared_key(
        "admin",
        "midge-sqrzl-azure",
        "bm90LXRoZS1lbXVsYXRvci1rZXk=",
    )
    .with_endpoint("http://127.0.0.1:9000")
    .unwrap();
    let backend = build_cloud_storage(&provider, "").unwrap();
    let key = format!(
        "qualification/azure-wrong-key/{}/object",
        uuid::Uuid::new_v4()
    );

    // Act
    let result = put(
        &backend,
        &key,
        b"must not be written".to_vec(),
        vec![("If-None-Match".into(), "*".into())],
    );

    // Assert: authentication failure must not masquerade as a CAS conflict.
    assert!(
        matches!(result, Err(CloudError::Unauthorized(_))),
        "{result:?}"
    );
    let valid =
        build_cloud_storage(&CloudProviderConfig::sqrzl_azure("midge-sqrzl-azure"), "").unwrap();
    assert!(matches!(get(&valid, &key), Err(CloudError::NotFound(_))));
}

#[test]
fn should_validate_streamed_multipart_receipt_from_sqrzl_azure() {
    // Arrange
    let provider = CloudProviderConfig::sqrzl_azure("midge-sqrzl-receipt-azure");
    // Act
    assert_sqrzl_streamed_receipt("sqrzl-azure-receipt", &provider);
    // Assert: the helper verifies exact receipt identity and size.
}

fn assert_sqrzl_streamed_receipt(label: &str, provider: &CloudProviderConfig) {
    use crate::io::{File, FsPath, OpenOptions as FsOpenOptions, RealFs};
    use crate::storage::{
        StorageBackend, StorageFileUpload, StoragePrecondition, StorageRequest,
        FILE_UPLOAD_PART_BYTES,
    };
    use std::sync::Arc;

    // Arrange: prepare an authenticated namespace and a payload spanning two parts.
    require_sqrzl(label);
    ensure_sqrzl_namespace(provider).unwrap_or_else(|error| {
        panic!("{label}: failed to prepare Sqrzl namespace: {error}");
    });
    let backend = build_cloud_storage(provider, "").expect("build Sqrzl storage backend");
    let prefix = format!("qualification/{label}/{}/", uuid::Uuid::new_v4());
    let size = FILE_UPLOAD_PART_BYTES + 1;
    let bytes: Vec<u8> = (0..size)
        .map(|index| (index.wrapping_mul(131).wrapping_add(17) & 0xff) as u8)
        .collect();
    let directory = tempfile::tempdir().expect("create streamed receipt source");
    std::fs::write(directory.path().join("source"), &bytes).expect("write streamed receipt source");
    let fs = RealFs::new(directory.path()).expect("open streamed receipt source directory");
    let file =
        crate::io::open_persistent_handle(&fs, &FsPath::new("source"), FsOpenOptions::READ_ONLY)
            .expect("open streamed receipt source");
    let file: Arc<dyn File> = Arc::from(file);
    let deadline = crate::common::OperationDeadline::from_budget(Duration::from_secs(60));
    let (tx, rx) = std::sync::mpsc::channel();
    // Act: stream the file through the provider receipt API.
    backend.submit_file_write_receipt_request(
        StorageRequest::new(
            format!("{prefix}object.sst"),
            deadline,
            Duration::from_secs(60),
        )
        .with_precondition(StoragePrecondition::IfAbsent),
        StorageFileUpload {
            file,
            size: size as u64,
            content_crc32c: crc32c::crc32c(&bytes),
            part_size: FILE_UPLOAD_PART_BYTES,
        },
        tx,
    );
    let receipt = rx
        .recv_timeout(Duration::from_secs(60))
        .expect("wait for Sqrzl streamed upload receipt")
        .unwrap_or_else(|error| panic!("{label}: streamed upload failed: {error}"))
        .unwrap_or_else(|| panic!("{label}: provider omitted its checksum receipt"));
    // Assert: the receipt covers the exact multipart payload and byte length.
    assert_eq!(receipt.metadata.size, size as u64, "{label}: receipt size");
    assert!(
        receipt.matches(&bytes),
        "{label}: provider receipt must match the exact uploaded bytes and identity"
    );
}

#[test]
fn should_run_gcs_xml_contract_against_sqrzl() {
    let provider = CloudProviderConfig::sqrzl_gcs("midge-sqrzl-gcs");
    run_provider_contract("gcs", &provider);
}

#[test]
fn should_run_gcs_json_contract_against_sqrzl() {
    let provider = CloudProviderConfig::sqrzl_gcs_json("midge-sqrzl-gcs-json");
    run_provider_contract("gcs-json", &provider);
}

#[test]
fn should_run_oci_s3_compatibility_contract_against_sqrzl() {
    // Arrange
    let provider = CloudProviderConfig::oci_object_storage(
        "sqrzl",
        "midge-sqrzl-oci",
        "us-east-1",
        OciCredentialSource::access_key("admin", "easy-peasy"),
    )
    .with_endpoint("http://127.0.0.1:9000")
    .expect("OCI supports endpoint overrides");

    // Act
    run_provider_contract("oci", &provider);

    // Assert
    // The shared contract asserts provider operations and conditional identities.
}

#[test]
fn should_run_s3_compatible_contract_against_real_provider_if_configured() {
    // Arrange
    let Some(provider) = configured_real_s3_provider() else {
        return;
    };

    run_provider_contract_without_namespace_setup("real-s3", &provider);
    // Act
    // Assert
}

fn run_provider_contract(label: &str, provider: &CloudProviderConfig) {
    require_sqrzl(label);

    ensure_sqrzl_namespace(provider).unwrap_or_else(|error| {
        panic!("{label}: failed to prepare Sqrzl namespace: {error}");
    });
    run_provider_contract_body(label, provider);
}

fn run_provider_contract_without_namespace_setup(label: &str, provider: &CloudProviderConfig) {
    run_provider_contract_body(label, provider);
}

fn run_provider_contract_body(label: &str, provider: &CloudProviderConfig) {
    assert!(
        provider.validate().is_valid,
        "{label}: structural validation"
    );
    let backend = build_cloud_storage(provider, "").unwrap_or_else(|error| {
        panic!("{label}: failed to build provider backend: {error}");
    });
    let prefix = format!("qualification/{label}/{}/", uuid::Uuid::new_v4());
    let key = format!("{prefix}object.bin");
    let overwrite_key = format!("{prefix}overwrite.bin");
    let conditional_key = format!("{prefix}conditional.bin");
    let missing_key = format!("{prefix}missing.bin");
    let empty_key = format!("{prefix}empty.bin");

    put(&backend, &key, b"hello-sqrzl".to_vec(), vec![]).expect("PUT");
    assert_eq!(get(&backend, &key).expect("GET"), b"hello-sqrzl");

    let preflight = CloudStorageLocation::new(provider.clone(), prefix.trim_end_matches('/'))
        .preflight(CloudPreflightOptions::default());
    assert!(
        preflight.is_ready,
        "{label}: read-only preflight: {preflight:?}"
    );
    assert!(
        preflight.is_fully_verified,
        "{label}: full read verification"
    );

    let metadata = head(&backend, &key).expect("HEAD");
    assert_eq!(metadata.size, b"hello-sqrzl".len() as u64);

    let listed = list(&backend, &prefix).expect("LIST");
    assert!(
        listed.iter().any(|item| item == &key),
        "LIST did not include {key}; got {listed:?}"
    );

    assert_eq!(
        range(&backend, &key, 0, Some(5)).expect("range read"),
        b"hello"
    );

    put(&backend, &empty_key, Vec::new(), vec![]).expect("empty PUT");
    assert_eq!(
        get(&backend, &empty_key).expect("empty GET"),
        Vec::<u8>::new()
    );
    assert_eq!(head(&backend, &empty_key).expect("empty HEAD").size, 0);
    delete(&backend, &empty_key).expect("empty DELETE");
    assert!(
        matches!(get(&backend, &empty_key), Err(CloudError::NotFound(_))),
        "{label}: deleted empty object should be NotFound"
    );

    put(&backend, &overwrite_key, b"first".to_vec(), vec![]).expect("initial overwrite PUT");
    put(&backend, &overwrite_key, b"second".to_vec(), vec![]).expect("overwrite PUT");
    assert_eq!(
        get(&backend, &overwrite_key).expect("overwrite GET"),
        b"second"
    );

    put(&backend, &conditional_key, b"created".to_vec(), vec![]).expect("conditional seed");
    assert!(
        matches!(
            put(
                &backend,
                &conditional_key,
                b"duplicate".to_vec(),
                vec![("If-None-Match".to_string(), "*".to_string())],
            ),
            Err(CloudError::PreconditionFailed(_))
        ),
        "{label}: conditional create on an existing object should be PreconditionFailed"
    );
    let conditional_head = head(&backend, &conditional_key).expect("conditional HEAD");
    assert!(
        !conditional_head.etag.is_empty(),
        "HEAD should return an ETag for conditional update"
    );
    let conditional_headers = crate::storage::cloud::object_match_precondition_headers(
        &conditional_head.etag,
        conditional_head.generation.as_deref(),
    )
    .expect("provider HEAD should provide a conditional identity token");
    put(
        &backend,
        &conditional_key,
        b"updated".to_vec(),
        conditional_headers,
    )
    .expect("conditional update with matching ETag");
    assert_eq!(
        get(&backend, &conditional_key).expect("conditional GET"),
        b"updated"
    );

    verify_metadata_proof_conditions(&backend, &conditional_key, &missing_key);

    verify_missing_object_contract(label, &backend, &conditional_key, &missing_key);

    delete(&backend, &key).expect("DELETE");
    assert!(
        matches!(get(&backend, &key), Err(CloudError::NotFound(_))),
        "{label}: deleted object should be NotFound"
    );
}

/// A missing object reads as `NotFound`, a conditional update of it loses
/// the precondition, and deleting it succeeds, on every provider (#373).
fn verify_missing_object_contract(
    label: &str,
    backend: &CloudStorage,
    conditional_key: &str,
    missing_key: &str,
) {
    assert!(
        matches!(get(backend, missing_key), Err(CloudError::NotFound(_))),
        "{label}: missing object GET should be NotFound"
    );
    assert!(
        matches!(head(backend, missing_key), Err(CloudError::NotFound(_))),
        "{label}: missing object HEAD should be NotFound"
    );
    assert!(
        matches!(
            put(
                backend,
                missing_key,
                b"never".to_vec(),
                conditional_head_headers(backend, conditional_key),
            ),
            Err(CloudError::PreconditionFailed(_))
        ),
        "{label}: conditional update of a missing object should be PreconditionFailed"
    );
    delete(backend, missing_key).expect("DELETE of a missing object succeeds");
}

fn verify_metadata_proof_conditions(
    backend: &CloudStorage,
    conditional_key: &str,
    missing_key: &str,
) {
    let original_proof =
        crate::storage::cloud::blocking_cloud_object_proof(backend, conditional_key)
            .expect("metadata-bearing proof")
            .expect("conditional object exists");
    assert_eq!(original_proof.bytes, b"updated");
    let stale_headers = crate::storage::cloud::object_match_precondition_headers(
        &original_proof.metadata.etag,
        original_proof.metadata.generation.as_deref(),
    )
    .expect("GET identity supports conditions");
    put(backend, conditional_key, b"changed".to_vec(), vec![]).expect("same-length replacement");
    // A same-length replacement must invalidate both mutation forms, while
    // the proof continues to describe precisely the bytes originally read.
    assert!(matches!(
        put(
            backend,
            conditional_key,
            b"invalid".to_vec(),
            stale_headers.clone()
        ),
        Err(CloudError::PreconditionFailed(_))
    ));
    let (tx, rx) = std::sync::mpsc::channel();
    backend.submit_delete_with_headers(conditional_key, stale_headers, tx);
    assert!(matches!(
        rx.recv_timeout(Duration::from_secs(10))
            .expect("stale delete callback"),
        CloudEvent::Delete {
            result: Err(crate::storage::cloud::CloudError::PreconditionFailed(_)),
            ..
        }
    ));
    assert_eq!(
        get(backend, conditional_key).expect("replacement survives"),
        b"changed"
    );
    assert!(
        crate::storage::cloud::blocking_cloud_object_proof(backend, missing_key)
            .expect("missing proof is not a transport failure")
            .is_none()
    );
}

fn configured_real_s3_provider() -> Option<CloudProviderConfig> {
    let settings = configured_real_s3_settings()?;
    let provider = CloudProviderConfig::s3_compatible(
        settings.bucket,
        settings.region,
        settings.endpoint,
        settings.access_key,
        settings.secret_key,
    );
    Some(
        provider
            .with_path_style(settings.path_style)
            .expect("real S3 path-style override"),
    )
}

#[path = "../../../tests/support/sqrzl.rs"]
mod sqrzl;
use sqrzl::*;

fn ensure_sqrzl_namespace(provider: &CloudProviderConfig) -> Result<(), String> {
    match provider {
        CloudProviderConfig::AwsS3(_) => Ok(()),
        CloudProviderConfig::S3Compatible(config) => ensure_sqrzl_s3_bucket(config.bucket()),
        CloudProviderConfig::Gcs(config) => ensure_sqrzl_gcs_bucket(config.bucket()),
        CloudProviderConfig::AzureBlob(config) => ensure_sqrzl_azure_container(config.container()),
        CloudProviderConfig::OciObjectStorage(config) => ensure_sqrzl_s3_bucket(config.bucket()),
    }
}

#[test]
fn should_join_azure_canonical_headers_directly_to_resource() {
    // Arrange
    let headers = vec![
        (
            "x-ms-date".to_string(),
            "Tue, 11 Aug 2026 12:00:00 GMT".to_string(),
        ),
        ("x-ms-version".to_string(), "2026-10-06".to_string()),
    ];

    // Act
    let string_to_sign = azure_string_to_sign("GET", &headers, "/admin/container/blob", "", b"");

    // Assert
    assert!(string_to_sign.contains("x-ms-version:2026-10-06\n/admin/admin/container/blob"));
    assert!(!string_to_sign.contains("x-ms-version:2026-10-06\n\n/"));
}

/// Precondition headers naming the current version of `key`.
fn conditional_head_headers(backend: &CloudStorage, key: &str) -> Vec<(String, String)> {
    let metadata = head(backend, key).expect("HEAD for a conditional identity");
    crate::storage::cloud::object_match_precondition_headers(
        &metadata.etag,
        metadata.generation.as_deref(),
    )
    .expect("provider HEAD should provide a conditional identity token")
}

fn put(
    backend: &CloudStorage,
    key: &str,
    data: Vec<u8>,
    headers: Vec<(String, String)>,
) -> Result<(), CloudError> {
    let (tx, rx) = std::sync::mpsc::channel();
    backend.submit_put(key, data, headers, tx);
    match rx.recv_timeout(Duration::from_secs(30)) {
        Ok(CloudEvent::Put { result, .. }) => match result {
            CloudOutcome::Ok(()) => Ok(()),
            CloudOutcome::Err(error) => Err(error),
        },
        other => Err(CloudError::Protocol(format!(
            "unexpected PUT event: {other:?}"
        ))),
    }
}

fn get(backend: &CloudStorage, key: &str) -> Result<Vec<u8>, CloudError> {
    let (tx, rx) = std::sync::mpsc::channel();
    backend.submit_get(key, tx);
    match rx.recv_timeout(Duration::from_secs(30)) {
        Ok(CloudEvent::Get { result, .. }) => match result {
            CloudOutcome::Ok(data) => Ok(data),
            CloudOutcome::Err(error) => Err(error),
        },
        other => Err(CloudError::Protocol(format!(
            "unexpected GET event: {other:?}"
        ))),
    }
}

fn range(
    backend: &CloudStorage,
    key: &str,
    start: u64,
    end: Option<u64>,
) -> Result<Vec<u8>, CloudError> {
    let (tx, rx) = std::sync::mpsc::channel();
    backend.submit_get_range(key, start, end, tx);
    match rx.recv_timeout(Duration::from_secs(30)) {
        Ok(CloudEvent::GetRange { result, .. }) => match result {
            CloudOutcome::Ok(data) => Ok(data),
            CloudOutcome::Err(error) => Err(error),
        },
        other => Err(CloudError::Protocol(format!(
            "unexpected range event: {other:?}"
        ))),
    }
}

fn head(backend: &CloudStorage, key: &str) -> Result<ObjectMetadata, CloudError> {
    let (tx, rx) = std::sync::mpsc::channel();
    backend.submit_head(key, tx);
    match rx.recv_timeout(Duration::from_secs(30)) {
        Ok(CloudEvent::Head { result, .. }) => match result {
            CloudOutcome::Ok(metadata) => Ok(metadata),
            CloudOutcome::Err(error) => Err(error),
        },
        other => Err(CloudError::Protocol(format!(
            "unexpected HEAD event: {other:?}"
        ))),
    }
}

fn list(backend: &CloudStorage, prefix: &str) -> Result<Vec<String>, CloudError> {
    let (tx, rx) = std::sync::mpsc::channel();
    backend.submit_list(prefix, tx);
    match rx.recv_timeout(Duration::from_secs(30)) {
        Ok(CloudEvent::List { result, .. }) => match result {
            CloudOutcome::Ok(keys) => Ok(keys),
            CloudOutcome::Err(error) => Err(error),
        },
        other => Err(CloudError::Protocol(format!(
            "unexpected LIST event: {other:?}"
        ))),
    }
}

fn delete(backend: &CloudStorage, key: &str) -> Result<(), CloudError> {
    let (tx, rx) = std::sync::mpsc::channel();
    backend.submit_delete(key, tx);
    match rx.recv_timeout(Duration::from_secs(30)) {
        Ok(CloudEvent::Delete { result, .. }) => match result {
            CloudOutcome::Ok(()) => Ok(()),
            CloudOutcome::Err(error) => Err(error),
        },
        other => Err(CloudError::Protocol(format!(
            "unexpected DELETE event: {other:?}"
        ))),
    }
}
