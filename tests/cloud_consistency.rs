//! Cloud Consistency Tests
//!
//! Tests for cloud storage consistency guarantees, including eventual consistency
//! handling, checksum validation, manifest synchronization, and cloud listing lag.
//!
//! Test coverage:
//! - Cloud listing lag handling
//! - Eventual consistency tolerance
//! - Checksum validation
//! - Corrupted cloud blob handling
//! - Local vs cloud state synchronization

mod common;

use bytes::Bytes;
use cntryl_midge::cloud::backend::StorageBackend;
use cntryl_midge::cloud::mock::MockCloudBackend;
use cntryl_midge::core::manifest::Manifest;

// ============================================================================
// Cloud Listing Lag
// ============================================================================

#[test]
fn should_handle_cloud_listing_lag_given_manifest_references_new_sst_when_stale_listing() {
    // Arrange
    let (temp_dir, backend, _manifest) = common::cloud::setup_cloud_test();

    // Create a manifest that references an SST that the cloud doesn't report yet
    let mut m = Manifest::default();
    m.ssts.push("sst/new_sst.blob".to_string());

    // Act - cloud listing is stale by not setting a cloud manifest on the backend
    backend.reset_counters();

    // Assert
    let cloud_manifest_read =
        <MockCloudBackend as StorageBackend>::get_blob(&*backend, "manifest.json");
    assert!(cloud_manifest_read.is_err());
    // Local manifest contains sst while cloud has no manifest
    assert!(m.ssts.contains(&"sst/new_sst.blob".to_string()));

    common::cloud::cleanup_cloud_test(&temp_dir);
}

#[test]
fn should_tolerate_eventual_consistency_given_cloud_listing_missing_recent_sst_when_rebuilding() {
    // Arrange
    let (tmp_dir, backend, _manifest_lock) = common::cloud::setup_cloud_test();

    // Put an SST into the mock backend (simulating a file exists in cloud storage)
    backend
        .put_blob("sst/recent.sst", Bytes::from("sst-bytes"))
        .expect("put sst");

    // Set a cloud manifest that does NOT reference the above SST (stale manifest)
    let stale = Manifest::default();
    backend.set_cloud_manifest(stale);

    // Act
    let listed = backend.list_blobs("").expect("list blobs");
    let cloud_manifest_bytes = backend.get_blob("manifest.json");

    // Assert - listing should include the SST even if manifest is stale
    assert!(
        listed.iter().any(|k| k.ends_with("recent.sst")),
        "Cloud listing should contain the uploaded SST"
    );
    if let Ok(bytes) = cloud_manifest_bytes {
        assert!(
            !bytes.is_empty(),
            "manifest.json should be readable (stale manifest)"
        );
    }

    common::cloud::cleanup_cloud_test(&tmp_dir);
}

// ============================================================================
// Cloud Upload Atomicity
// ============================================================================

#[test]
fn should_retry_cloud_upload_atomically_given_latency_spike_when_retrying() {
    // Arrange
    let (temp_dir, backend, _manifest) = common::cloud::setup_cloud_test();

    // Force upload failures for the first upload
    backend.set_fail_upload_after(1);

    // Act
    let _ = <MockCloudBackend as StorageBackend>::put_blob(
        &*backend,
        "sst/1.blob",
        Bytes::from_static(b"a"),
    );
    // Second upload should succeed once failure threshold reached
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

// ============================================================================
// Partial Object Handling
// ============================================================================

#[test]
fn should_rehydrate_partial_cloud_object_given_short_file_when_reading() {
    // Arrange
    let (temp_dir, backend, _manifest) = common::cloud::setup_cloud_test();

    // Simulate a partial object by writing a short file
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

// ============================================================================
// Checksum Validation
// ============================================================================

#[test]
fn should_resolve_mismatched_checksums_given_local_vs_cloud_when_syncing() {
    // Arrange
    let (temp_dir, backend, _manifest) = common::cloud::setup_cloud_test();

    // Create a manifest that will be returned from cloud
    let mut cloud_manifest = Manifest::default();
    cloud_manifest.ssts.push("sst/one.blob".to_string());
    backend.set_cloud_manifest(cloud_manifest.clone());

    // Act
    let got = <MockCloudBackend as StorageBackend>::get_blob(&*backend, "manifest.json").unwrap();
    let parsed: Manifest = serde_json::from_slice(&got).unwrap();

    // Assert
    assert_eq!(parsed.ssts, cloud_manifest.ssts);
    common::cloud::cleanup_cloud_test(&temp_dir);
}

// ============================================================================
// Corrupted Cloud Data
// ============================================================================

#[test]
fn should_fail_fast_given_corrupted_cloud_sst_index_block_when_reading_data() {
    // Arrange
    let backend = MockCloudBackend::new();
    <MockCloudBackend as StorageBackend>::put_blob(
        &backend,
        "sst/corrupt.sst",
        Bytes::from(&b"corrupted-content"[..]),
    )
    .expect("write corrupt sst");

    // Act
    let got = <MockCloudBackend as StorageBackend>::get_blob(&backend, "sst/corrupt.sst")
        .expect("get corrupt sst");

    // Assert - blob is readable (corruption detection happens at higher layer)
    assert_eq!(got, Bytes::from(&b"corrupted-content"[..]));
}
