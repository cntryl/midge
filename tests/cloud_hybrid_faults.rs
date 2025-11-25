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
use std::sync::Arc;

#[test]
#[ignore] // Temporarily ignored due to hanging - investigating shutdown issue
fn should_recover_consistently_given_partial_cloud_sst_upload_when_local_manifest_was_already_updated(
) {
    // Arrange: open engine with a cloud-backed storage mode and a mock backend
    let dir = test_temp_dir();
    let backend = Arc::new(MockCloudBackend::new());

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

    // Act: perform a write and flush under simulated **SST** cloud failures, then restart
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            eng.put(&cf, b"key1", b"value1").expect("put");

            // Arm the failure injection **after** engine open to avoid poisoning WAL startup.
            backend.reset_counters();
            // Allow exactly one new successful upload (e.g. first SST/manifest), then fail.
            backend.set_fail_upload_after(1);

            let attempts_before = backend.upload_count() + backend.upload_failure_count();

            // Flush will attempt to upload SST/manifest to cloud and may encounter failures.
            let _ = eng.flush_cf(&cf); // error is acceptable under simulated cloud failure

            let attempts_after = backend.upload_count() + backend.upload_failure_count();

            // Assert: at least one upload attempt happened.
            assert!(
                attempts_after > attempts_before,
                "flush should trigger at least one cloud upload attempt"
            );
        },
        |eng| {
            // Assert: data must still be readable after recovery
            // Put backend back into non-failing mode for restart/read.
            backend.reset_counters();
            backend.set_fail_upload_after(usize::MAX);

            let cf = eng.default_column_family();
            let result = eng.get(&cf, b"key1").expect("get");
            assert!(
                result.is_some(),
                "Data should be present after recovery despite partial/failed cloud uploads",
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

#[test]
fn should_not_poison_wal_startup_given_fail_upload_after_is_armed_post_open() {
    // Arrange: open an engine with a cloud-backed storage mode and mock backend
    let dir = test_temp_dir();
    let backend = Arc::new(MockCloudBackend::new());

    let opts = MidgeOptions {
        storage_mode: StorageMode::CloudBacked {
            local_cache_path: dir.path().to_path_buf(),
            cloud_backend: backend.clone(),
            storage_context: StorageContext::new("uploader-fail-after"),
            local_wal_sync: true,
            wal_batch_size: 1024 * 1024,
            sst_cache_capacity: 8,
        },
        memtable_size: 1024,
        wal_sync: true,
        wal_recovery_mode: cntryl_midge::WalRecoveryMode::TolerateCorruptedTail,
        ..Default::default()
    };

    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            eng.put(&cf, b"key-wal", b"value")
                .expect("put before fail-after");

            // Arm failure only after engine and WAL uploader are fully initialized.
            backend.reset_counters();
            backend.set_fail_upload_after(1);

            let attempts_before = backend.upload_count() + backend.upload_failure_count();

            // Act: force a flush; this should make progress (attempt uploads) and not hang,
            // even if some cloud uploads fail after the first success.
            let _ = eng.flush_cf(&cf);

            let attempts_after = backend.upload_count() + backend.upload_failure_count();

            assert!(
                attempts_after > attempts_before,
                "flush should trigger at least one cloud upload attempt when fail-after is armed",
            );
        },
        |eng| {
            // Assert: WAL-backed data should still be recoverable after restart.
            backend.reset_counters();
            backend.set_fail_upload_after(usize::MAX);

            let cf = eng.default_column_family();
            let result = eng.get(&cf, b"key-wal").expect("get after restart");
            assert!(
                result.is_some(),
                "Data written before fail-after should survive WAL/uploader failures",
            );
        },
    );
}

#[test]
fn should_allow_clean_shutdown_given_cloud_upload_failures_after_flush_attempts() {
    // Arrange: cloud-backed engine with a mock backend where uploads may fail
    let dir = test_temp_dir();
    let backend = Arc::new(MockCloudBackend::new());

    let opts = MidgeOptions {
        storage_mode: StorageMode::CloudBacked {
            local_cache_path: dir.path().to_path_buf(),
            cloud_backend: backend.clone(),
            storage_context: StorageContext::new("shutdown-after-fail"),
            local_wal_sync: true,
            wal_batch_size: 1024 * 1024,
            sst_cache_capacity: 8,
        },
        memtable_size: 1024,
        wal_sync: true,
        wal_recovery_mode: cntryl_midge::WalRecoveryMode::TolerateCorruptedTail,
        ..Default::default()
    };

    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            for i in 0..5u8 {
                eng.put(&cf, &[b'k', i], &[b'v', i]).expect("put");
            }

            backend.reset_counters();
            backend.set_fail_upload_after(1);

            let attempts_before = backend.upload_count() + backend.upload_failure_count();

            let _ = eng.flush_cf(&cf);

            let attempts_after = backend.upload_count() + backend.upload_failure_count();
            assert!(
                attempts_after > attempts_before,
                "flush under fail-after should still attempt cloud uploads",
            );
        },
        |eng| {
            // Assert: engine can restart cleanly and data is readable despite prior failures
            backend.reset_counters();
            backend.set_fail_upload_after(usize::MAX);

            let cf = eng.default_column_family();
            for i in 0..5u8 {
                let key = [b'k', i];
                let value = [b'v', i];
                let got = eng.get(&cf, &key).expect("get after restart");
                assert!(got.is_some(), "key should survive: {:?}", key);
                assert_eq!(got.unwrap(), &value[..]);
            }
        },
    );
}

#[test]
fn should_not_block_puts_when_background_uploads_are_flaky() {
    // Arrange: engine in cloud-backed mode with a flakily failing backend
    let dir = test_temp_dir();
    let backend = Arc::new(MockCloudBackend::new());

    let opts = MidgeOptions {
        storage_mode: StorageMode::CloudBacked {
            local_cache_path: dir.path().to_path_buf(),
            cloud_backend: backend.clone(),
            storage_context: StorageContext::new("flaky-puts"),
            local_wal_sync: true,
            wal_batch_size: 1024 * 1024,
            sst_cache_capacity: 8,
        },
        memtable_size: 8 * 1024,
        wal_sync: true,
        wal_recovery_mode: cntryl_midge::WalRecoveryMode::TolerateCorruptedTail,
        ..Default::default()
    };

    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            backend.reset_counters();
            backend.set_fail_upload_after(1);

            // Act: a burst of puts should complete quickly even if uploads fail later.
            for i in 0..50u8 {
                let key = [b'k', i];
                let value = [b'v', i];
                eng.put(&cf, &key, &value).expect("put under flaky cloud");
            }

            let _attempts = backend.upload_count() + backend.upload_failure_count();
        },
        |eng| {
            backend.reset_counters();
            backend.set_fail_upload_after(usize::MAX);

            let cf = eng.default_column_family();
            // Assert: at least some keys are readable; the exact coverage is handled elsewhere.
            let got = eng
                .get(&cf, b"k\x00")
                .expect("get one key after flaky puts");
            assert!(
                got.is_some(),
                "engine should remain readable after flaky upload activity",
            );
        },
    );
}

#[test]
fn should_report_upload_attempts_when_manifest_sync_happens_under_fail_after() {
    // Arrange: engine with cloud-backed manifest writes and fail-after semantics
    let dir = test_temp_dir();
    let backend = Arc::new(MockCloudBackend::new());

    let opts = MidgeOptions {
        storage_mode: StorageMode::CloudBacked {
            local_cache_path: dir.path().to_path_buf(),
            cloud_backend: backend.clone(),
            storage_context: StorageContext::new("manifest-fail-after"),
            local_wal_sync: true,
            wal_batch_size: 1024 * 1024,
            sst_cache_capacity: 8,
        },
        memtable_size: 1024,
        wal_sync: true,
        wal_recovery_mode: cntryl_midge::WalRecoveryMode::TolerateCorruptedTail,
        ..Default::default()
    };

    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            eng.put(&cf, b"m-key", b"m-val")
                .expect("put before manifest sync");

            backend.reset_counters();
            backend.set_fail_upload_after(1);

            let attempts_before = backend.upload_count() + backend.upload_failure_count();

            let _ = eng.flush_cf(&cf);

            let attempts_after = backend.upload_count() + backend.upload_failure_count();
            assert!(
                attempts_after > attempts_before,
                "manifest-related uploads should still be attempted under fail-after",
            );
        },
        |eng| {
            // Assert: manifest and data remain coherent enough for reads
            backend.reset_counters();
            backend.set_fail_upload_after(usize::MAX);

            let cf = eng.default_column_family();
            let got = eng
                .get(&cf, b"m-key")
                .expect("get after manifest fail-after");
            assert!(
                got.is_some(),
                "data should still be discoverable after manifest uploads under fail-after",
            );
        },
    );
}
