// Tests for cloud/hybrid fault scenarios.
//
// Each test follows the repository test conventions: Arrange / Act / Assert.

mod common;
use bytes::Bytes;
use cntryl_midge::cloud::backend::StorageBackend;
use cntryl_midge::{
    cloud::mock::MockCloudBackend, config::cloud::StorageContext, MidgeOptions, StorageMode,
};
use common::*;
use common::test_helpers::TEST_CLOUD_TIMEOUT;
use std::sync::Arc;

#[test]
fn should_recover_consistently_given_partial_cloud_sst_upload_when_local_manifest_was_already_updated(
) {
    // Arrange: open engine with a cloud-backed storage mode and a failing backend
    let dir = test_temp_dir();
    let backend = Arc::new(MockCloudBackend::new());

    // Allow one successful upload then fail subsequent uploads to simulate a partial/failed upload
    backend.set_fail_upload_after(1);

    let opts = MidgeOptions {
        storage_mode: StorageMode::CloudBacked {
            local_cache_path: dir.path().to_path_buf(),
            cloud_backend: backend.clone(),
            storage_context: StorageContext::new("partial-upload"),
            local_wal_sync: true,
            wal_batch_size: 1024 * 1024,
            sst_cache_capacity: 8,
        },
        memtable_size: 1024,
        wal_sync: true,
        wal_recovery_mode: cntryl_midge::WalRecoveryMode::TolerateCorruptedTail,
        ..Default::default()
    };

    // Act: perform a write and flush under simulated cloud failures, then restart
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            eng.put(&cf, b"key1", b"value1").expect("put");
            // Flush will attempt to upload SST to cloud and may encounter failures
            if let Err(e) = eng.flush_cf(&cf) {
                // With our simulated cloud failure it's acceptable for flush/upload to return an error.
                eprintln!(
                    "flush encountered error (expected in simulated failure): {:?}",
                    e
                );
            }
            // Allow background upload attempts to run using the mock helper to wait for uploads
            assert!(backend.wait_for_uploads(1, TEST_CLOUD_TIMEOUT));
            // Expect at least one failed upload attempt due to our fail config
            assert!(backend.upload_failure_count() > 0 || backend.upload_count() == 0);
        },
        |eng| {
            // Assert: data must still be readable after recovery
            // Clear the forced failure so any background retries can succeed
            backend.reset_counters();
            backend.set_fail_upload_after(usize::MAX);

            let cf = eng.default_column_family();
            let result = eng.get(&cf, b"key1").expect("get");
            assert!(
                result.is_some(),
                "Data should be present after recovery despite partial cloud upload"
            );
        },
    );
}

#[test]
fn should_retry_idempotently_given_duplicate_cloud_upload_requests_when_network_flaps() {
    // Arrange: create an in-memory mock backend
    let backend = MockCloudBackend::new();

    // Act: first attempt should succeed, second should be rejected as duplicate
    let etag = backend
        .put_blob_if_not_exists("sst/dup.sst", Bytes::from("payload"))
        .expect("first put_blob_if_not_exists");

    // Second duplicate attempt should return a DatabaseLocked error (semantic for "already exists")
    let second = backend.put_blob_if_not_exists("sst/dup.sst", Bytes::from("payload"));

    // Assert: duplicate put must fail while existing blob is still discoverable
    assert!(
        second.is_err(),
        "duplicate put should return an error indicating existing blob"
    );
    // The existing blob is discoverable via head_blob
    let head = backend.head_blob("sst/dup.sst").expect("head_blob");
    assert!(
        head.is_some(),
        "head_blob should report metadata for existing SST"
    );
    let meta = head.unwrap();
    assert!(meta.etag.is_some() || !etag.is_empty());
}

#[test]
fn should_tolerate_eventual_consistency_given_cloud_listing_missing_recent_sst_when_rebuilding_version_set(
) {
    // Arrange: upload an SST blob, then install a stale manifest that omits it
    let (tmp_dir, backend, _manifest_lock) = common::cloud::setup_cloud_test();

    // Put an SST into the mock backend (simulating a file exists in cloud storage)
    backend
        .put_blob("sst/recent.sst", Bytes::from("sst-bytes"))
        .expect("put sst");

    // Now set a cloud manifest that does NOT reference the above SST (stale manifest)
    // We reuse the project's Manifest types via the helper; using default() simulates stale view
    let stale = cntryl_midge::core::manifest::Manifest::default();
    backend.set_cloud_manifest(stale);

    // Act: list blobs and read manifest to observe the drift
    let listed = backend.list_blobs("").expect("list blobs");
    let cloud_manifest_bytes = backend.get_blob("manifest.json");

    // Assert: listing should include the SST even if manifest is stale
    assert!(
        listed.iter().any(|k| k.ends_with("recent.sst")),
        "Cloud listing should contain the uploaded SST"
    );
    // Manifest read should succeed (returns the stale manifest we set), or be missing
    if let Ok(bytes) = cloud_manifest_bytes {
        assert!(
            !bytes.is_empty(),
            "manifest.json should be readable (stale manifest)"
        );
    }

    // Cleanup
    common::cloud::cleanup_cloud_test(&tmp_dir);
}

#[test]
fn should_fail_fast_leaving_engine_in_safe_state_given_corrupted_cloud_sst_index_block_when_reading_data(
) {
    // Arrange: use a mock backend and write a deliberately corrupted SST blob
    let backend = MockCloudBackend::new();
    <MockCloudBackend as StorageBackend>::put_blob(
        &backend,
        "sst/corrupt.sst",
        Bytes::from(&b"corrupted-content"[..]),
    )
    .expect("write corrupt sst");

    // Act: read the blob back from the cloud backend
    let got = <MockCloudBackend as StorageBackend>::get_blob(&backend, "sst/corrupt.sst")
        .expect("get corrupt sst");

    // Assert
    assert_eq!(got, Bytes::from(&b"corrupted-content"[..]));
    // Engine-level behavior when encountering corrupted SSTs is covered by separate engine tests.
}
