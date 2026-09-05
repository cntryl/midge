use bytes::Bytes;
mod common;
use cntryl_midge::{
    Engine, EngineHealth, MidgeError, OpenOptions, RecoveryPolicy, TransactionMode, WriteOptions,
};
use common::{crash, opts_for_mode, MidgeOptions, StorageMode};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

static FAILPOINT_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
const CORRUPT_WAL_CHILD_TEST_NAME: &str =
    "should_abort_in_child_process_when_remote_wal_corruption_scenario_requested";
const CORRUPT_WAL_ENV_DB_PATH: &str = "MIDGE_CORRUPT_REMOTE_WAL_DB_PATH";
const CORRUPT_WAL_SCENARIO: &str = "remote_wal_corruption_after_strict_ack";
const CORRUPT_WAL_TRIGGER: &str = "manual::remote_wal_corruption_after_strict_ack";
const TRUNCATED_PRIMARY_CATALOG: &[u8] = b"{\"format_version\":1";
const TRUNCATED_MIRROR_CATALOG: &[u8] = b"{\"format_version\":1,\"fencing_epoch\":";

#[test]
fn should_abort_in_child_process_when_remote_wal_corruption_scenario_requested() {
    // Arrange
    let Some(db_path) = std::env::var_os(CORRUPT_WAL_ENV_DB_PATH) else {
        return;
    };
    let engine = Engine::open(cloud_open_options(
        Path::new(&db_path),
        RecoveryPolicy::Strict,
    ))
    .expect("open cloud engine in crash child");
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

    // Act
    // Assert
    crash::abort_at_trigger(CORRUPT_WAL_SCENARIO, CORRUPT_WAL_TRIGGER);
}

#[test]
fn should_recover_cloud_strict_write_from_authoritative_remote_wal_after_local_cache_loss() {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let opts = opts_for_mode("cloud");
    let db_path = cloud_db_path(&opts);
    let mut engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");

    put_default(
        &engine,
        b"strict-remote-key",
        b"strict-remote-value",
        WriteOptions::cloud_strict(),
    );
    engine
        .shutdown(std::time::Duration::from_secs(5))
        .expect("shutdown before reopen");
    reset_dir(&db_path.join("wal"));

    // Act
    let reopened = Engine::open(opts.to_open_options()).expect("reopen cloud engine");

    // Assert
    assert_eq!(
        get_default(&reopened, b"strict-remote-key"),
        Some(Bytes::from_static(b"strict-remote-value"))
    );
    shutdown_test_engine(reopened);
}

#[test]
fn should_recover_from_valid_catalog_mirror_when_primary_catalog_has_torn_tail() {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let opts = opts_for_mode("cloud");
    let db_path = cloud_db_path(&opts);
    let mut engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");
    put_default(
        &engine,
        b"catalog-mirror-key",
        b"catalog-mirror-value",
        WriteOptions::cloud_strict(),
    );
    engine
        .shutdown(Duration::from_secs(5))
        .expect("shutdown before damaging primary catalog");

    let remote_wal_dir = db_path.join("cloud_store").join("wal");
    let primary_catalog = remote_wal_dir.join("publication-catalog.v1.json");
    let mirror_catalog = remote_wal_dir.join("publication-catalog.v1.mirror.json");
    let valid_catalog = fs::read(&primary_catalog).expect("read valid primary WAL catalog");
    assert_eq!(
        fs::read(&mirror_catalog).expect("read valid WAL catalog mirror"),
        valid_catalog,
        "successful strict publication must converge the catalog mirror"
    );
    fs::write(
        &primary_catalog,
        &valid_catalog[..valid_catalog.len().saturating_sub(7)],
    )
    .expect("truncate primary WAL catalog tail");
    reset_dir(&db_path.join("wal"));

    // Act
    let reopened = Engine::open(opts.to_open_options()).expect("recover through catalog mirror");

    // Assert
    assert_eq!(
        get_default(&reopened, b"catalog-mirror-key"),
        Some(Bytes::from_static(b"catalog-mirror-value"))
    );
    assert_eq!(
        fs::read(&primary_catalog).expect("read repaired primary WAL catalog"),
        fs::read(&mirror_catalog).expect("read converged WAL catalog mirror"),
        "startup must repair and fence both catalog copies"
    );
    shutdown_test_engine(reopened);
}

#[test]
fn should_fail_closed_when_both_cloud_wal_catalog_copies_are_invalid() {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let opts = opts_for_mode("cloud");
    let db_path = cloud_db_path(&opts);
    let mut engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");
    put_default(
        &engine,
        b"doubly-corrupt-catalog-key",
        b"must-not-be-ambiguously-recovered",
        WriteOptions::cloud_strict(),
    );
    engine
        .shutdown(Duration::from_secs(5))
        .expect("shutdown before damaging catalog copies");

    let remote_wal_dir = db_path.join("cloud_store").join("wal");
    fs::write(
        remote_wal_dir.join("publication-catalog.v1.json"),
        TRUNCATED_PRIMARY_CATALOG,
    )
    .expect("damage primary WAL catalog");
    fs::write(
        remote_wal_dir.join("publication-catalog.v1.mirror.json"),
        TRUNCATED_MIRROR_CATALOG,
    )
    .expect("damage WAL catalog mirror");
    reset_dir(&db_path.join("wal"));

    // Act
    let error = expect_engine_open_error(opts.to_open_options());

    // Assert
    assert!(
        matches!(&error, MidgeError::Corruption(message) if message.contains("both cloud WAL publication catalogs are invalid")),
        "unexpected catalog corruption error: {error:?}"
    );
}

#[test]
fn should_remove_local_wal_segment_after_cloud_durable_upload() {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let opts = opts_for_mode("cloud");
    let db_path = cloud_db_path(&opts);
    let mut engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");

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
    engine
        .shutdown(std::time::Duration::from_secs(5))
        .expect("shutdown before reopen");
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
    shutdown_test_engine(reopened);
}

#[test]
fn should_prune_remote_wal_segment_after_cloud_sst_covers_it() {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let opts = opts_for_mode("cloud");
    let db_path = cloud_db_path(&opts);
    let remote_wal_dir = db_path.join("cloud_store").join("wal");
    let mut engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");

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
    engine
        .shutdown(std::time::Duration::from_secs(5))
        .expect("shutdown before reopen");
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
        list_files_with_extension(&db_path.join("sst"), "sst").is_empty(),
        "reopen should read covered values without restoring full SST files"
    );
    shutdown_test_engine(reopened);
}

#[test]
fn should_ignore_reintroduced_manifest_covered_remote_wal_after_restart() {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let opts = opts_for_mode("cloud");
    let db_path = cloud_db_path(&opts);
    let remote_wal_dir = db_path.join("cloud_store").join("wal");
    let mut engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");
    put_default(
        &engine,
        b"restart-prune-key",
        b"restart-prune-value",
        WriteOptions::cloud_strict(),
    );
    let retained_segments = wait_for_remote_wal_count_at_least(&remote_wal_dir, 1)
        .into_iter()
        .map(|path| {
            let bytes = fs::read(&path).expect("read remote WAL before simulated interruption");
            (
                path.strip_prefix(&remote_wal_dir)
                    .expect("remote WAL path below WAL root")
                    .to_owned(),
                bytes,
            )
        })
        .collect::<Vec<_>>();
    let default_cf = default_cf(&engine);
    engine.flush_cf(&default_cf).expect("flush covered value");
    assert!(wait_for_remote_wal_count(&remote_wal_dir, 0).is_empty());
    engine
        .shutdown(Duration::from_secs(5))
        .expect("shutdown before restoring interrupted prune residue");

    fs::create_dir_all(&remote_wal_dir).expect("recreate remote WAL directory");
    for (relative_path, bytes) in retained_segments {
        let restored_path = remote_wal_dir.join(relative_path);
        fs::create_dir_all(restored_path.parent().expect("remote WAL parent"))
            .expect("restore remote WAL epoch directory");
        fs::write(restored_path, bytes).expect("restore covered remote WAL residue");
    }
    reset_dir(&db_path.join("wal"));
    reset_dir(&db_path.join("sst"));

    // Act
    let reopened = Engine::open(opts.to_open_options()).expect("reopen cloud engine");
    let remaining = wait_for_remote_wal_count_at_least(&remote_wal_dir, 1);

    // Assert
    assert!(
        !remaining.is_empty(),
        "a WAL object reintroduced after catalog retirement should remain a harmless orphan"
    );
    assert_eq!(
        get_default(&reopened, b"restart-prune-key"),
        Some(Bytes::from_static(b"restart-prune-value"))
    );
    shutdown_test_engine(reopened);
}

#[test]
fn should_recover_delete_range_given_remote_wal_only_when_local_cache_is_lost() {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let opts = opts_for_mode("cloud");
    let db_path = cloud_db_path(&opts);
    let remote_wal_dir = db_path.join("cloud_store").join("wal");
    let mut engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");
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
    let remote_segments = wait_for_remote_wal_count_at_least(&remote_wal_dir, 1);
    engine
        .shutdown(std::time::Duration::from_secs(5))
        .expect("shutdown before reopen");
    reset_dir(&db_path.join("wal"));
    reset_dir(&db_path.join("sst"));
    let reopened = Engine::open(opts.to_open_options()).expect("reopen cloud engine");

    // Assert
    assert!(
        !remote_segments.is_empty(),
        "range tombstone WAL must be retained without an exact per-record SST coverage proof"
    );
    assert_eq!(get_default(&reopened, b"range-10"), None);
    assert_eq!(get_default(&reopened, b"range-15"), None);
    assert_eq!(
        get_default(&reopened, b"range-25"),
        Some(Bytes::from_static(b"outside-delete-range"))
    );
    shutdown_test_engine(reopened);
}

#[test]
fn should_preserve_remote_wal_when_unflushed_column_family_still_depends_on_it_given_partial_gc() {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let opts = opts_for_mode("cloud");
    let db_path = cloud_db_path(&opts);
    let remote_wal_dir = db_path.join("cloud_store").join("wal");
    let mut engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");
    let default_cf = default_cf(&engine);
    let other_cf = engine
        .create_column_family("other")
        .expect("create other cf");

    put_cf(
        &engine,
        &default_cf,
        b"default-buffered",
        b"default-value",
        WriteOptions::cloud_async(),
    );
    put_cf(
        &engine,
        &other_cf,
        b"other-buffered",
        b"other-value",
        WriteOptions::cloud_async(),
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
    engine
        .shutdown(std::time::Duration::from_secs(5))
        .expect("shutdown before reopen");
    reset_dir(&db_path.join("wal"));
    let reopened = Engine::open(opts.to_open_options()).expect("reopen cloud engine");

    // Assert
    assert_eq!(
        get_cf(&reopened, "other", b"other-buffered"),
        Some(Bytes::from_static(b"other-value")),
        "unflushed column family data should still recover from retained remote WAL"
    );
    shutdown_test_engine(reopened);
}

#[test]
fn should_recover_partial_remote_wal_cleanup_given_mixed_flush_state_when_reopening() {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let opts = opts_for_mode("cloud");
    let db_path = cloud_db_path(&opts);
    let remote_wal_dir = db_path.join("cloud_store").join("wal");
    let mut engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");
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
    engine
        .shutdown(std::time::Duration::from_secs(5))
        .expect("shutdown before reopen");
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
    shutdown_test_engine(reopened);
}

#[test]
fn should_reject_sync_buffered_options_given_cloud_storage_when_committing() {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let opts = opts_for_mode("cloud");
    let engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");

    // Act
    let default_cf = default_cf(&engine);
    let mut tx = engine
        .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
        .expect("begin write tx");
    tx.put(b"sync-local-only".to_vec(), b"sync-value".to_vec(), None)
        .expect("put sync-local-only value");
    let error = tx
        .commit(WriteOptions::sync())
        .expect_err("sync() should be rejected for cloud-backed storage");

    // Assert
    assert!(
        matches!(error, MidgeError::InvalidArgument(message) if message.contains("local-only"))
    );
    shutdown_test_engine(engine);
}

#[cfg(feature = "failpoints")]
#[test]
fn should_keep_cloud_async_commit_visible_given_cloud_upload_failure_when_committing() {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let recovery_opts = opts_for_mode("cloud");
    let failure_opts = recovery_opts
        .clone()
        .with_shutdown_cloud_drain_timeout(Duration::from_millis(100));
    let db_path = cloud_db_path(&recovery_opts);
    let scenario = fail::FailScenario::setup();
    fail::cfg("midge::cloud::inject_fail_wal_upload", "return")
        .expect("configure wal upload failure failpoint");

    let mut engine = Engine::open(failure_opts.to_open_options()).expect("open cloud engine");
    put_default(
        &engine,
        b"buffered-local-only",
        b"buffered-value",
        WriteOptions::cloud_async(),
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
    assert_shutdown_fails_with_pending_cloud_uploads(&mut engine);

    fail::remove("midge::cloud::inject_fail_wal_upload");
    scenario.teardown();

    reset_dir(&db_path.join("wal"));
    let reopened = Engine::open(recovery_opts.to_open_options()).expect("reopen cloud engine");
    assert_eq!(get_default(&reopened, b"buffered-local-only"), None);
    shutdown_test_engine(reopened);
}

#[cfg(feature = "failpoints")]
#[test]
fn should_recover_cloud_async_commit_given_intact_local_wal_when_upload_fails() {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let recovery_opts = opts_for_mode("cloud");
    let failure_opts = recovery_opts
        .clone()
        .with_shutdown_cloud_drain_timeout(Duration::from_millis(100));
    let db_path = cloud_db_path(&recovery_opts);
    let remote_wal_dir = db_path.join("cloud_store").join("wal");
    let scenario = fail::FailScenario::setup();
    fail::cfg("midge::cloud::inject_fail_wal_upload", "return")
        .expect("configure wal upload failure failpoint");

    let mut engine = Engine::open(failure_opts.to_open_options()).expect("open cloud engine");
    put_default(
        &engine,
        b"intact-local-cloud-async",
        b"survives-same-node-restart",
        WriteOptions::cloud_async(),
    );
    let _metrics = wait_for_cloud_gap(&engine, 1);
    assert_shutdown_fails_with_pending_cloud_uploads(&mut engine);

    fail::remove("midge::cloud::inject_fail_wal_upload");
    scenario.teardown();

    // Act
    let reopened = Engine::open(recovery_opts.to_open_options()).expect("reopen cloud engine");

    // Assert
    assert_eq!(
        get_default(&reopened, b"intact-local-cloud-async"),
        Some(Bytes::from_static(b"survives-same-node-restart"))
    );
    let durable = wait_for_cloud_catch_up(&reopened, 1);
    assert!(
        durable.wal_cloud_durable_seq >= durable.current_sequence,
        "resumed upload must advance the cloud frontier only after acknowledgment"
    );
    assert!(
        !wait_for_remote_wal_count_at_least(&remote_wal_dir, 1).is_empty(),
        "same-node recovery must resume publication of the local-only WAL"
    );
    shutdown_test_engine(reopened);
}

#[cfg(feature = "failpoints")]
#[test]
fn should_fail_cloud_strict_commit_given_cloud_upload_failure_when_waiting_for_ack() {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let recovery_opts = opts_for_mode("cloud");
    let failure_opts = recovery_opts
        .clone()
        .with_shutdown_cloud_drain_timeout(Duration::from_millis(100));
    let db_path = cloud_db_path(&recovery_opts);
    let scenario = fail::FailScenario::setup();
    fail::cfg("midge::cloud::inject_fail_wal_upload", "return")
        .expect("configure wal upload failure failpoint");

    let mut engine = Engine::open(failure_opts.to_open_options()).expect("open cloud engine");
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
    assert_shutdown_fails_with_pending_cloud_uploads(&mut engine);

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
    let reopened = Engine::open(recovery_opts.to_open_options()).expect("reopen cloud engine");
    assert_eq!(get_default(&reopened, b"cloud-strict-fail"), None);
    shutdown_test_engine(reopened);
}

#[test]
fn should_salvage_valid_prefix_when_remote_wal_segment_is_corrupt() {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let opts = opts_for_mode("cloud");
    let db_path = cloud_db_path(&opts);
    run_remote_wal_corruption_child(&db_path);
    expire_crashed_process_lease(&db_path);

    let remote_wal_dir = db_path.join("cloud_store").join("wal");
    let corrupt_remote_wal = list_files_with_extension(&remote_wal_dir, "wal")
        .into_iter()
        .max()
        .expect("remote WAL object to corrupt");
    corrupt_last_file(&remote_wal_dir);
    let corrupt_authoritative_bytes =
        fs::read(&corrupt_remote_wal).expect("read corrupt authoritative WAL bytes");
    reset_dir(&db_path.join("wal"));

    // Act
    let Err(strict_error) = Engine::open(opts.clone().to_open_options()) else {
        panic!("strict cloud reopen should fail on corrupt authoritative WAL");
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
    assert!(
        corrupt_remote_wal.exists(),
        "corrupt recovered WAL must be retained when it cannot be proven safe to delete"
    );
    assert_eq!(
        fs::read(&corrupt_remote_wal).expect("read retained corrupt authoritative WAL"),
        corrupt_authoritative_bytes,
        "salvage recovery must retain corrupt authoritative WAL byte-for-byte"
    );
    shutdown_test_engine(salvaged);
}

fn run_remote_wal_corruption_child(db_path: &Path) {
    let current_exe = std::env::current_exe().expect("current test executable");
    let mut command = Command::new(current_exe);
    command
        .arg("--exact")
        .arg(CORRUPT_WAL_CHILD_TEST_NAME)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(CORRUPT_WAL_ENV_DB_PATH, db_path);
    crash::run_child_expect_abort(
        &mut command,
        CORRUPT_WAL_SCENARIO,
        CORRUPT_WAL_TRIGGER,
        db_path,
    );
}

fn expire_crashed_process_lease(db_path: &Path) {
    let lease_path = db_path.join("midge_primary_lease.json");
    if lease_path.exists() {
        let mut content = fs::read_to_string(&lease_path).expect("read crashed lease record");
        if content.contains("acquired_at: ") || content.contains("expires_at: ") {
            content = content
                .lines()
                .map(|line| {
                    if line.starts_with("acquired_at: ") {
                        "acquired_at: 1970-01-01T00:00:00Z".to_string()
                    } else if line.starts_with("expires_at: ") {
                        "expires_at: 1970-01-01T00:00:00Z".to_string()
                    } else {
                        line.to_string()
                    }
                })
                .collect::<Vec<_>>()
                .join("\n");
            content.push('\n');
            fs::write(&lease_path, content).expect("expire crashed lease record");
        }
    }
    crash::clear_crashed_process_acquisition_lock(db_path);
}

#[test]
fn should_read_authoritative_remote_sst_when_reopening_after_cache_loss() {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let opts = opts_for_mode("cloud");
    let db_path = cloud_db_path(&opts);
    let mut engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");
    let default_cf = default_cf(&engine);

    put_default(
        &engine,
        b"sst-restore-key",
        b"sst-restore-value",
        WriteOptions::best_effort(),
    );
    engine.flush_cf(&default_cf).expect("flush default cf");
    engine
        .shutdown(std::time::Duration::from_secs(5))
        .expect("shutdown before reopen");

    reset_dir(&db_path.join("sst"));

    // Act
    let reopened = Engine::open(opts.to_open_options()).expect("reopen cloud engine");

    // Assert
    assert_eq!(
        get_default(&reopened, b"sst-restore-key"),
        Some(Bytes::from_static(b"sst-restore-value"))
    );
    assert!(
        list_files_with_extension(&db_path.join("sst"), "sst").is_empty(),
        "reads should leave the full SST in authoritative cloud storage"
    );
    shutdown_test_engine(reopened);
}

#[test]
fn should_fail_strict_recovery_given_authoritative_remote_sst_missing_when_reopening() {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let opts = opts_for_mode("cloud");
    let db_path = cloud_db_path(&opts);
    let mut engine = Engine::open(opts.clone().to_open_options()).expect("open cloud engine");
    let default_cf = default_cf(&engine);

    put_default(
        &engine,
        b"missing-remote-sst-key",
        b"sst-value",
        WriteOptions::best_effort(),
    );
    engine.flush_cf(&default_cf).expect("flush default cf");
    engine
        .shutdown(std::time::Duration::from_secs(5))
        .expect("shutdown before reopen");

    for remote_sst in list_files_with_extension(&db_path.join("cloud_store").join("sst"), "sst") {
        fs::remove_file(&remote_sst).expect("delete authoritative remote sst");
    }

    // Act
    let Err(error) = Engine::open(opts.to_open_options()) else {
        panic!("strict reopen should reject missing remote sst");
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

fn shutdown_test_engine(mut engine: Engine) {
    engine
        .shutdown(Duration::from_secs(5))
        .expect("shut down cloud test engine");
}

#[cfg(feature = "failpoints")]
fn assert_shutdown_fails_with_pending_cloud_uploads(engine: &mut Engine) {
    let started = Instant::now();
    let error = engine
        .shutdown(Duration::from_secs(5))
        .expect_err("permanent upload failure must prevent a clean shutdown");
    let elapsed = started.elapsed();

    match error {
        MidgeError::Internal(message) => assert!(
            message.contains("cloud uploads")
                && (message.contains("storage-owned") || message.contains("runtime-owned")),
            "expected pending cloud-upload shutdown error, got: {message}"
        ),
        other => panic!("expected pending cloud-upload shutdown error, got: {other:?}"),
    }
    assert!(
        elapsed < Duration::from_secs(2),
        "terminal cloud-upload shutdown exceeded its injected drain budget: {elapsed:?}"
    );
}

fn cloud_open_options(db_path: &Path, recovery_policy: RecoveryPolicy) -> OpenOptions {
    OpenOptions::cloud_simulated(
        db_path.to_path_buf(),
        "test-bucket".to_string(),
        "test-prefix/".to_string(),
    )
    .recovery_policy(recovery_policy)
    .build()
    .expect("build cloud options")
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

#[cfg(feature = "failpoints")]
fn wait_for_cloud_gap(engine: &Engine, min_sequence: u64) -> cntryl_midge::RuntimeMetricsSnapshot {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let metrics = engine.get_runtime_metrics().expect("runtime metrics");
        if metrics.current_sequence >= min_sequence
            && metrics.wal_cloud_durable_seq < metrics.current_sequence
            && metrics.health == EngineHealth::Degraded
        {
            return metrics;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for failed cloud durability; last metrics: seq={} cloud_seq={} health={:?}",
            metrics.current_sequence,
            metrics.wal_cloud_durable_seq,
            metrics.health
        );
        thread::sleep(Duration::from_millis(25));
    }
}

#[cfg(feature = "failpoints")]
fn wait_for_cloud_catch_up(
    engine: &Engine,
    min_sequence: u64,
) -> cntryl_midge::RuntimeMetricsSnapshot {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let metrics = engine.get_runtime_metrics().expect("runtime metrics");
        if metrics.current_sequence >= min_sequence
            && metrics.wal_cloud_durable_seq >= metrics.current_sequence
        {
            return metrics;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for recovered WAL cloud acknowledgment; last metrics: seq={} cloud_seq={} health={:?}",
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
    let mut files = Vec::new();
    collect_files(dir, &mut files);
    files
}

fn collect_files(dir: &Path, files: &mut Vec<PathBuf>) {
    for entry in
        fs::read_dir(dir).unwrap_or_else(|error| panic!("read_dir({}): {error}", dir.display()))
    {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            collect_files(&path, files);
        } else {
            files.push(path);
        }
    }
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

fn expect_engine_open_error(options: OpenOptions) -> MidgeError {
    match Engine::open(options) {
        Ok(_) => panic!("engine open unexpectedly succeeded"),
        Err(error) => error,
    }
}
