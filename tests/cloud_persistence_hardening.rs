use bytes::Bytes;
mod common;
use cntryl_midge::{
    Engine, EngineHealth, MidgeError, OpenOptions, RecoveryPolicy, TransactionMode, WriteOptions,
};
use common::{opts_for_mode, MidgeOptions, StorageMode};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

static FAILPOINT_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

#[test]
fn should_recover_cloud_strict_write_from_authoritative_remote_wal_after_local_cache_loss() {
    // Arrange
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
    let opts = opts_for_mode("cloud");
    let db_path = cloud_db_path(&opts);
    let engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");

    put_default(
        &engine,
        b"strict-remote-key",
        b"strict-remote-value",
        WriteOptions::cloud_strict(),
    );
    drop(engine);
    reset_dir(&db_path.join("wal"));

    // Act
    let reopened = Engine::open(opts.to_open_options()).expect("reopen cloud engine");

    // Assert
    assert_eq!(
        get_default(&reopened, b"strict-remote-key"),
        Some(Bytes::from_static(b"strict-remote-value"))
    );
}

#[test]
fn should_remove_local_wal_segment_after_cloud_durable_upload() {
    // Arrange
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
    let opts = opts_for_mode("cloud");
    let db_path = cloud_db_path(&opts);
    let engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");

    // Act
    put_default(
        &engine,
        b"cloud-pruned-local-wal",
        b"remote-wal-value",
        WriteOptions::cloud_strict(),
    );
    let metrics = engine.get_runtime_metrics().expect("runtime metrics");
    let local_segments = list_files_with_extension(&db_path.join("wal"), "wal");
    let remote_segments =
        list_files_with_extension(&db_path.join("cloud_store").join("wal"), "wal");
    drop(engine);
    reset_dir(&db_path.join("wal"));
    let reopened = Engine::open(opts.to_open_options()).expect("reopen cloud engine");

    // Assert
    assert!(
        metrics.wal_cloud_durable_seq >= metrics.current_sequence,
        "cloud-strict write should advance the cloud durability frontier"
    );
    assert!(
        local_segments.is_empty(),
        "cloud-durable local WAL segments should be removed, found: {local_segments:?}"
    );
    assert!(
        !remote_segments.is_empty(),
        "authoritative remote WAL segment should remain available"
    );
    assert_eq!(
        get_default(&reopened, b"cloud-pruned-local-wal"),
        Some(Bytes::from_static(b"remote-wal-value"))
    );
}

#[test]
fn should_prune_remote_wal_segment_after_cloud_sst_covers_it() {
    // Arrange
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
    let opts = opts_for_mode("cloud");
    let db_path = cloud_db_path(&opts);
    let remote_wal_dir = db_path.join("cloud_store").join("wal");
    let engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");

    put_default(
        &engine,
        b"remote-pruned-after-flush",
        b"covered-by-sst",
        WriteOptions::cloud_strict(),
    );
    assert!(
        !list_files_with_extension(&remote_wal_dir, "wal").is_empty(),
        "cloud-strict write should create an authoritative remote WAL segment"
    );
    let default_cf = default_cf(&engine);

    // Act
    engine.flush_cf(&default_cf).expect("flush default cf");
    let remote_segments = wait_for_remote_wal_count(&remote_wal_dir, 0);
    drop(engine);
    reset_dir(&db_path.join("wal"));
    reset_dir(&db_path.join("sst"));
    let reopened = Engine::open(opts.to_open_options()).expect("reopen cloud engine");

    // Assert
    assert!(
        remote_segments.is_empty(),
        "remote WAL should be pruned after cloud SST coverage"
    );
    assert_eq!(
        get_default(&reopened, b"remote-pruned-after-flush"),
        Some(Bytes::from_static(b"covered-by-sst"))
    );
    assert!(
        !list_files_with_extension(&db_path.join("sst"), "sst").is_empty(),
        "reopen should restore the covered value from cloud SST state"
    );
}

#[test]
fn should_recover_delete_range_after_remote_wal_pruned_and_local_cache_lost() {
    // Arrange
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
    let opts = opts_for_mode("cloud");
    let db_path = cloud_db_path(&opts);
    let remote_wal_dir = db_path.join("cloud_store").join("wal");
    let engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");
    let default_cf = default_cf(&engine);

    put_default(
        &engine,
        b"range-10",
        b"covered-before-delete",
        WriteOptions::cloud_strict(),
    );
    put_default(
        &engine,
        b"range-15",
        b"covered-before-delete",
        WriteOptions::cloud_strict(),
    );
    put_default(
        &engine,
        b"range-25",
        b"outside-delete-range",
        WriteOptions::cloud_strict(),
    );
    let mut tx = engine
        .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
        .expect("begin delete-range tx");
    tx.delete_range(b"range-10".to_vec(), b"range-20".to_vec())
        .expect("delete range");
    tx.commit(WriteOptions::cloud_strict())
        .expect("commit delete range");
    assert!(
        !wait_for_remote_wal_count_at_least(&remote_wal_dir, 1).is_empty(),
        "cloud-strict writes should create remote WAL before flush"
    );

    // Act
    engine.flush_cf(&default_cf).expect("flush range tombstone");
    let remote_segments = wait_for_remote_wal_count(&remote_wal_dir, 0);
    drop(engine);
    reset_dir(&db_path.join("wal"));
    reset_dir(&db_path.join("sst"));
    let reopened = Engine::open(opts.to_open_options()).expect("reopen cloud engine");

    // Assert
    assert!(
        remote_segments.is_empty(),
        "remote WAL should be pruned after the range tombstone is covered by cloud SST"
    );
    assert_eq!(get_default(&reopened, b"range-10"), None);
    assert_eq!(get_default(&reopened, b"range-15"), None);
    assert_eq!(
        get_default(&reopened, b"range-25"),
        Some(Bytes::from_static(b"outside-delete-range"))
    );
}

#[test]
fn should_keep_remote_wal_segment_when_unflushed_column_family_still_needs_it() {
    // Arrange
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
    let opts = opts_for_mode("cloud");
    let db_path = cloud_db_path(&opts);
    let remote_wal_dir = db_path.join("cloud_store").join("wal");
    let engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");
    let default_cf = default_cf(&engine);
    let other_cf = engine
        .create_column_family("other")
        .expect("create other cf");

    put_cf(
        &engine,
        &default_cf,
        b"default-buffered",
        b"default-value",
        WriteOptions::buffered(),
    );
    put_cf(
        &engine,
        &other_cf,
        b"other-buffered",
        b"other-value",
        WriteOptions::buffered(),
    );
    put_cf(
        &engine,
        &default_cf,
        b"default-strict",
        b"default-strict-value",
        WriteOptions::cloud_strict(),
    );
    assert!(
        !wait_for_remote_wal_count_at_least(&remote_wal_dir, 1).is_empty(),
        "shared remote WAL segment should exist before partial flush"
    );

    // Act
    engine.flush_cf(&default_cf).expect("flush default cf");
    wait_for_no_remote_wal_prune(&remote_wal_dir);
    drop(engine);
    reset_dir(&db_path.join("wal"));
    let reopened = Engine::open(opts.to_open_options()).expect("reopen cloud engine");

    // Assert
    assert_eq!(
        get_cf(&reopened, "other", b"other-buffered"),
        Some(Bytes::from_static(b"other-value")),
        "unflushed column family data should still recover from retained remote WAL"
    );
}

#[test]
fn should_recover_after_partial_remote_wal_cleanup_and_local_cache_loss() {
    // Arrange
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
    let opts = opts_for_mode("cloud");
    let db_path = cloud_db_path(&opts);
    let remote_wal_dir = db_path.join("cloud_store").join("wal");
    let engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");
    let default_cf = default_cf(&engine);

    put_default(
        &engine,
        b"covered-before-partial-cleanup",
        b"covered-value",
        WriteOptions::cloud_strict(),
    );
    assert!(
        !wait_for_remote_wal_count_at_least(&remote_wal_dir, 1).is_empty(),
        "first strict write should create remote WAL before flush"
    );
    engine.flush_cf(&default_cf).expect("flush covered value");
    wait_for_remote_wal_count(&remote_wal_dir, 0);

    // Act: a later strict write remains WAL-backed after the earlier segment was pruned.
    put_default(
        &engine,
        b"retained-after-partial-cleanup",
        b"retained-value",
        WriteOptions::cloud_strict(),
    );
    let retained_segments = wait_for_remote_wal_count_at_least(&remote_wal_dir, 1);
    drop(engine);
    reset_dir(&db_path.join("wal"));
    reset_dir(&db_path.join("sst"));
    let reopened = Engine::open(opts.to_open_options()).expect("reopen cloud engine");

    // Assert
    assert_eq!(
        get_default(&reopened, b"covered-before-partial-cleanup"),
        Some(Bytes::from_static(b"covered-value")),
        "covered data should recover from cloud SST after its remote WAL was pruned"
    );
    assert_eq!(
        get_default(&reopened, b"retained-after-partial-cleanup"),
        Some(Bytes::from_static(b"retained-value")),
        "later unflushed data should recover from the retained remote WAL"
    );
    assert!(
        !retained_segments.is_empty(),
        "test must prove at least one later remote WAL segment survived partial cleanup"
    );
}

#[test]
fn should_keep_sync_write_local_only_when_cloud_wal_upload_fails() {
    // Arrange
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
    let opts = opts_for_mode("cloud");
    let db_path = cloud_db_path(&opts);
    let scenario = fail::FailScenario::setup();
    fail::cfg("midge::cloud::inject_fail_wal_upload", "return")
        .expect("configure wal upload failure failpoint");

    let engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");

    // Act
    put_default(
        &engine,
        b"sync-local-only",
        b"sync-value",
        WriteOptions::sync(),
    );
    let metrics = wait_for_cloud_gap(&engine, 1);

    // Assert
    assert!(metrics.current_sequence >= 1);
    assert!(
        metrics.wal_cloud_durable_seq < metrics.current_sequence,
        "cloud durability frontier must not advance on failed upload"
    );
    assert_eq!(
        get_default(&engine, b"sync-local-only"),
        Some(Bytes::from_static(b"sync-value"))
    );
    drop(engine);

    fail::remove("midge::cloud::inject_fail_wal_upload");
    scenario.teardown();

    reset_dir(&db_path.join("wal"));
    let reopened = Engine::open(opts.to_open_options()).expect("reopen cloud engine");
    assert_eq!(get_default(&reopened, b"sync-local-only"), None);
}

#[test]
fn should_keep_buffered_write_local_only_when_cloud_wal_upload_fails() {
    // Arrange
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
    let opts = opts_for_mode("cloud");
    let db_path = cloud_db_path(&opts);
    let scenario = fail::FailScenario::setup();
    fail::cfg("midge::cloud::inject_fail_wal_upload", "return")
        .expect("configure wal upload failure failpoint");

    let engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");
    put_default(
        &engine,
        b"buffered-local-only",
        b"buffered-value",
        WriteOptions::buffered(),
    );
    thread::sleep(Duration::from_millis(600));

    // Act
    let metrics = wait_for_cloud_gap(&engine, 1);

    // Assert
    assert!(metrics.current_sequence >= 1);
    assert!(
        metrics.wal_cloud_durable_seq < metrics.current_sequence,
        "buffered cloud writes must stay below the cloud durability frontier after upload failure"
    );
    assert_eq!(
        get_default(&engine, b"buffered-local-only"),
        Some(Bytes::from_static(b"buffered-value"))
    );
    drop(engine);

    fail::remove("midge::cloud::inject_fail_wal_upload");
    scenario.teardown();

    reset_dir(&db_path.join("wal"));
    let reopened = Engine::open(opts.to_open_options()).expect("reopen cloud engine");
    assert_eq!(get_default(&reopened, b"buffered-local-only"), None);
}

#[test]
fn should_fail_cloud_strict_commit_when_wal_upload_fails() {
    // Arrange
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
    let opts = opts_for_mode("cloud");
    let db_path = cloud_db_path(&opts);
    let scenario = fail::FailScenario::setup();
    fail::cfg("midge::cloud::inject_fail_wal_upload", "return")
        .expect("configure wal upload failure failpoint");

    let engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");
    let default_cf = default_cf(&engine);
    let mut tx = engine
        .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
        .expect("begin write tx");
    tx.put(
        b"cloud-strict-fail".to_vec(),
        b"strict-fail-value".to_vec(),
        None,
    )
    .expect("put strict-fail value");

    // Act
    let error = tx
        .commit(WriteOptions::cloud_strict())
        .expect_err("cloud_strict should fail when the authoritative upload fails");
    drop(engine);

    // Assert
    match error {
        MidgeError::Internal(message) => {
            assert!(
                message.contains("Cloud durability failed"),
                "expected cloud durability failure, got: {message}"
            );
        }
        other => panic!("expected cloud durability failure, got: {other:?}"),
    }

    fail::remove("midge::cloud::inject_fail_wal_upload");
    scenario.teardown();

    reset_dir(&db_path.join("wal"));
    let reopened = Engine::open(opts.to_open_options()).expect("reopen cloud engine");
    assert_eq!(get_default(&reopened, b"cloud-strict-fail"), None);
}

#[test]
fn should_salvage_valid_prefix_when_remote_wal_segment_is_corrupt() {
    // Arrange
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
    let opts = opts_for_mode("cloud");
    let db_path = cloud_db_path(&opts);
    let engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");

    put_default(
        &engine,
        b"prefix-key",
        b"prefix-value",
        WriteOptions::cloud_strict(),
    );
    put_default(
        &engine,
        b"truncated-key",
        b"truncated-value",
        WriteOptions::cloud_strict(),
    );
    drop(engine);

    let remote_wal_dir = db_path.join("cloud_store").join("wal");
    corrupt_last_file(&remote_wal_dir);
    reset_dir(&db_path.join("wal"));

    // Act
    let strict_error = match Engine::open(opts.clone().to_open_options()) {
        Ok(_) => panic!("strict cloud reopen should fail on corrupt authoritative WAL"),
        Err(error) => error,
    };
    let salvaged = Engine::open(cloud_open_options(&db_path, RecoveryPolicy::Salvage))
        .expect("salvage cloud reopen");
    let metrics = salvaged.get_runtime_metrics().expect("runtime metrics");

    // Assert
    match strict_error {
        MidgeError::RecoveryFailed(_) => {}
        other => panic!("expected strict recovery failure, got: {other:?}"),
    }
    assert_eq!(metrics.health, EngineHealth::SalvageMode);
    assert_eq!(
        get_default(&salvaged, b"prefix-key"),
        Some(Bytes::from_static(b"prefix-value"))
    );
    assert_eq!(get_default(&salvaged, b"truncated-key"), None);
}

#[test]
fn should_restore_local_sst_cache_from_authoritative_cloud_object() {
    // Arrange
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
    let opts = opts_for_mode("cloud");
    let db_path = cloud_db_path(&opts);
    let engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");
    let default_cf = default_cf(&engine);

    put_default(
        &engine,
        b"sst-restore-key",
        b"sst-restore-value",
        WriteOptions::best_effort(),
    );
    engine.flush_cf(&default_cf).expect("flush default cf");
    drop(engine);

    reset_dir(&db_path.join("sst"));

    // Act
    let reopened = Engine::open(opts.to_open_options()).expect("reopen cloud engine");

    // Assert
    assert_eq!(
        get_default(&reopened, b"sst-restore-key"),
        Some(Bytes::from_static(b"sst-restore-value"))
    );
    assert!(
        !list_files_with_extension(&db_path.join("sst"), "sst").is_empty(),
        "reopen should restore local SST cache from the authoritative cloud object"
    );
}

#[test]
fn should_fail_strict_reopen_when_authoritative_remote_sst_is_missing() {
    // Arrange
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
    let opts = opts_for_mode("cloud");
    let db_path = cloud_db_path(&opts);
    let engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");
    let default_cf = default_cf(&engine);

    put_default(
        &engine,
        b"missing-remote-sst-key",
        b"sst-value",
        WriteOptions::best_effort(),
    );
    engine.flush_cf(&default_cf).expect("flush default cf");
    drop(engine);

    for remote_sst in list_files_with_extension(&db_path.join("cloud_store").join("sst"), "sst") {
        fs::remove_file(&remote_sst).expect("delete authoritative remote sst");
    }

    // Act
    let error = match Engine::open(opts.to_open_options()) {
        Ok(_) => panic!("strict reopen should reject missing remote sst"),
        Err(error) => error,
    };

    // Assert
    match error {
        MidgeError::RecoveryFailed(message) => {
            assert!(
                message.contains("authoritative cloud SST"),
                "expected remote SST recovery failure, got: {message}"
            );
        }
        other => panic!("expected recovery failure, got: {other:?}"),
    }
}

fn cloud_db_path(opts: &MidgeOptions) -> PathBuf {
    match &opts.storage_mode {
        StorageMode::CloudBacked { local_cache_path } => local_cache_path.clone(),
        _ => panic!("expected cloud-backed storage mode"),
    }
}

fn cloud_open_options(db_path: &Path, recovery_policy: RecoveryPolicy) -> OpenOptions {
    OpenOptions::cloud_simulated(
        db_path.to_path_buf(),
        "test-bucket".to_string(),
        "test-prefix/".to_string(),
    )
    .recovery_policy(recovery_policy)
    .build()
}

fn default_cf(engine: &Engine) -> cntryl_midge::ColumnFamilyHandle {
    engine
        .get_column_family("default")
        .expect("default column family")
}

fn put_default(engine: &Engine, key: &[u8], value: &[u8], opts: WriteOptions) {
    let default_cf = default_cf(engine);
    put_cf(engine, &default_cf, key, value, opts);
}

fn put_cf(
    engine: &Engine,
    cf: &cntryl_midge::ColumnFamilyHandle,
    key: &[u8],
    value: &[u8],
    opts: WriteOptions,
) {
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin write tx");
    tx.put(key.to_vec(), value.to_vec(), None)
        .expect("put value");
    tx.commit(opts).expect("commit value");
}

fn get_default(engine: &Engine, key: &[u8]) -> Option<Bytes> {
    let default_cf = default_cf(engine);
    get_cf_by_handle(engine, &default_cf, key)
}

fn get_cf(engine: &Engine, name: &str, key: &[u8]) -> Option<Bytes> {
    let cf = engine
        .get_column_family(name)
        .unwrap_or_else(|| panic!("missing column family: {name}"));
    get_cf_by_handle(engine, &cf, key)
}

fn get_cf_by_handle(
    engine: &Engine,
    cf: &cntryl_midge::ColumnFamilyHandle,
    key: &[u8],
) -> Option<Bytes> {
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin read tx");
    tx.get(key).expect("get value")
}

fn wait_for_cloud_gap(engine: &Engine, min_sequence: u64) -> cntryl_midge::RuntimeMetricsSnapshot {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let metrics = engine.get_runtime_metrics().expect("runtime metrics");
        if metrics.current_sequence >= min_sequence
            && metrics.wal_cloud_durable_seq < metrics.current_sequence
        {
            return metrics;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for cloud durability gap; last metrics: seq={} cloud_seq={} health={:?}",
            metrics.current_sequence,
            metrics.wal_cloud_durable_seq,
            metrics.health
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn corrupt_last_file(dir: &Path) {
    let mut files = list_files_with_extension(dir, "wal");
    files.sort();
    let target = files.pop().expect("expected at least one file to corrupt");
    fs::write(&target, b"\x01\x02\x03").expect("corrupt remote wal segment");
}

fn list_files_with_extension(dir: &Path, extension: &str) -> Vec<PathBuf> {
    list_files(dir)
        .into_iter()
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some(extension))
        .collect()
}

fn list_files(dir: &Path) -> Vec<PathBuf> {
    fs::read_dir(dir)
        .unwrap_or_else(|error| panic!("read_dir({}): {error}", dir.display()))
        .map(|entry| entry.expect("dir entry").path())
        .collect()
}

fn wait_for_remote_wal_count(dir: &Path, expected: usize) -> Vec<PathBuf> {
    wait_for_remote_wal_condition(dir, |files| files.len() == expected)
}

fn wait_for_remote_wal_count_at_least(dir: &Path, expected: usize) -> Vec<PathBuf> {
    wait_for_remote_wal_condition(dir, |files| files.len() >= expected)
}

fn wait_for_no_remote_wal_prune(dir: &Path) {
    let deadline = Instant::now() + Duration::from_millis(300);
    loop {
        let files = list_files_with_extension(dir, "wal");
        assert!(
            !files.is_empty(),
            "remote WAL segment was pruned while another column family still needed it"
        );
        if Instant::now() >= deadline {
            return;
        }
        thread::sleep(Duration::from_millis(25));
    }
}

fn wait_for_remote_wal_condition<F>(dir: &Path, predicate: F) -> Vec<PathBuf>
where
    F: Fn(&[PathBuf]) -> bool,
{
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let files = list_files_with_extension(dir, "wal");
        if predicate(&files) {
            return files;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for remote WAL condition; found: {files:?}"
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn reset_dir(dir: &Path) {
    let _ = fs::remove_dir_all(dir);
    fs::create_dir_all(dir).expect("recreate directory");
}

fn failpoint_test_lock() -> &'static Mutex<()> {
    FAILPOINT_TEST_LOCK.get_or_init(|| Mutex::new(()))
}
