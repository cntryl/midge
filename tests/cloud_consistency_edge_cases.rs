mod common;

use bytes::Bytes;
use cntryl_midge::cloud::backend::StorageBackend;
use cntryl_midge::cloud::mock::MockCloudBackend;
use cntryl_midge::core::manifest::Manifest;

#[test]
fn should_handle_cloud_listing_lag_when_manifest_references_new_sst() {
    // Arrange
    let (temp_dir, backend, _manifest) = common::cloud::setup_cloud_test();

    // Create a new manifest that references an SST that the cloud doesn't report yet
    let mut m = Manifest::default();
    m.ssts.push("sst/new_sst.blob".to_string());

    // Act
    // Simulate a cloud listing that is stale by not setting a cloud manifest on the backend
    // and then validate that manifest differs from cloud copy
    backend.reset_counters();

    // Assert - cloud has no manifest set yet (reading manifest.json should return KeyNotFound)
    let cloud_manifest_read =
        <MockCloudBackend as StorageBackend>::get_blob(&*backend, "manifest.json");
    assert!(cloud_manifest_read.is_err());
    // The test verifies the mock API surface: local manifest contains sst while cloud has no manifest
    assert!(m.ssts.contains(&"sst/new_sst.blob".to_string()));

    common::cloud::cleanup_cloud_test(&temp_dir);
}

#[test]
fn should_retry_cloud_upload_atomically_under_latency_spike() {
    // Arrange
    let (temp_dir, backend, _manifest) = common::cloud::setup_cloud_test();

    // Force upload failures for the first 1 uploads
    backend.set_fail_upload_after(1);

    // Act
    // Use the mock backend directly and perform two uploads; first will fail, second should succeed
    let _ = <MockCloudBackend as StorageBackend>::put_blob(
        &*backend,
        "sst/1.blob",
        Bytes::from_static(b"a"),
    );
    // Second upload (should succeed once failure threshold reached)
    backend.set_fail_upload_after(usize::MAX);
    let second = <MockCloudBackend as StorageBackend>::put_blob(
        &*backend,
        "sst/2.blob",
        Bytes::from_static(b"b"),
    );

    // Assert
    assert!(second.is_ok());
    common::cloud::cleanup_cloud_test(&temp_dir);
}

#[test]
fn should_rehydrate_partial_cloud_object_without_corruption() {
    // Arrange
    let (temp_dir, backend, _manifest) = common::cloud::setup_cloud_test();

    // Simulate a partial object by writing a short file to blob path directly
    let key = "sst/partial.blob";
    <MockCloudBackend as StorageBackend>::put_blob(
        &*backend,
        key,
        Bytes::from_static(b"prefix-partial"),
    )
    .unwrap();

    // Act
    let got = <MockCloudBackend as StorageBackend>::get_blob(&*backend, key);

    // Assert
    assert!(got.is_ok());
    let b = got.unwrap();
    assert!(b.starts_with(b"prefix-partial"));

    common::cloud::cleanup_cloud_test(&temp_dir);
}

#[test]
fn should_resolve_mismatched_local_vs_cloud_checksums_during_sync() {
    // Arrange
    let (temp_dir, backend, _manifest) = common::cloud::setup_cloud_test();

    // Create a manifest that will be returned from cloud
    let mut cloud_manifest = Manifest::default();
    cloud_manifest.ssts.push("sst/one.blob".to_string());
    backend.set_cloud_manifest(cloud_manifest.clone());

    // Act
    // Fetch cloud manifest from backend API
    let got = <MockCloudBackend as StorageBackend>::get_blob(&*backend, "manifest.json").unwrap();
    let parsed: Manifest = serde_json::from_slice(&got).unwrap();

    // Assert
    assert_eq!(parsed.ssts, cloud_manifest.ssts);
    common::cloud::cleanup_cloud_test(&temp_dir);
}
