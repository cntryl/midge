//! Public durability-boundary and physical fsync-count coverage.
//!
//! These tests do not directly establish `KeyedGroupCommit` waiter merge/fanout behavior. That
//! primitive has dedicated unit coverage, while its real cloud fanout path is exercised by the
//! runtime `CloudAck` tests.

use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
use std::sync::{Arc, Barrier, Mutex, OnceLock};
use std::time::Duration;

static SYNC_COUNT_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
static TELEMETRY_INIT: OnceLock<()> = OnceLock::new();

fn sync_count_test_guard() -> std::sync::MutexGuard<'static, ()> {
    SYNC_COUNT_TEST_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn open_engine() -> (tempfile::TempDir, Engine, cntryl_midge::ColumnFamilyHandle) {
    init_test_telemetry();
    let temp_dir = tempfile::tempdir().expect("create database directory");
    let engine = Engine::open(
        OpenOptions::local(temp_dir.path())
            .build()
            .expect("build options"),
    )
    .expect("open engine");
    let cf = engine
        .get_column_family("default")
        .expect("default column family");
    (temp_dir, engine, cf)
}

fn init_test_telemetry() {
    TELEMETRY_INIT.get_or_init(|| {
        cntryl_midge::init_benchmark_telemetry().expect("enable test-visible WAL metrics");
    });
}

fn wal_fsync_count(engine: &Engine) -> u64 {
    engine
        .get_runtime_metrics()
        .expect("read runtime metrics")
        .wal_fsync_count
}

fn wal_append_count(engine: &Engine) -> u64 {
    engine
        .get_runtime_metrics()
        .expect("read runtime metrics")
        .wal_append_count
}

#[test]
fn should_issue_one_physical_wal_sync_when_non_empty_sync_transaction_commits() {
    // Arrange
    let _guard = sync_count_test_guard();
    let (_temp_dir, mut engine, cf) = open_engine();
    let before = wal_fsync_count(&engine);
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin write transaction");
    tx.put(b"key".to_vec(), b"value".to_vec(), None)
        .expect("stage value");

    // Act
    tx.commit(WriteOptions::sync())
        .expect("commit synchronously");
    let after = wal_fsync_count(&engine);

    // Assert
    assert_eq!(
        after.saturating_sub(before),
        1,
        "strict WAL append must own the sole physical sync boundary"
    );
    engine
        .shutdown(Duration::from_secs(2))
        .expect("shutdown sync-count engine");
}

#[test]
fn should_issue_one_physical_wal_sync_when_spilled_transaction_commits() {
    // Arrange
    let _guard = sync_count_test_guard();
    init_test_telemetry();
    let temp_dir = tempfile::tempdir().expect("create database directory");
    let mut engine = Engine::open(
        OpenOptions::local(temp_dir.path())
            .transaction_memory_pool_size(8 * 1024)
            .build()
            .expect("build options"),
    )
    .expect("open engine");
    let cf = engine
        .get_column_family("default")
        .expect("default column family");
    let before = wal_fsync_count(&engine);
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin spilled transaction");
    for index in 0..4 {
        tx.put(
            format!("spill-{index}").into_bytes(),
            vec![b'x'; 8 * 1024],
            None,
        )
        .expect("stage spilled value");
    }

    // Act
    tx.commit(WriteOptions::sync())
        .expect("commit spilled transaction synchronously");
    let after = wal_fsync_count(&engine);

    // Assert
    assert_eq!(
        after.saturating_sub(before),
        1,
        "split WAL frames must share one physical sync boundary"
    );
    engine
        .shutdown(Duration::from_secs(2))
        .expect("shutdown spilled sync-count engine");
}

#[test]
fn should_issue_one_physical_wal_sync_when_empty_sync_transaction_commits() {
    // Arrange
    let _guard = sync_count_test_guard();
    let (_temp_dir, mut engine, cf) = open_engine();
    let before = wal_fsync_count(&engine);
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin empty write transaction");

    // Act
    tx.commit(WriteOptions::sync())
        .expect("commit empty transaction synchronously");
    let after = wal_fsync_count(&engine);

    // Assert
    assert_eq!(
        after.saturating_sub(before),
        1,
        "an explicit empty synchronous commit must perform exactly one barrier"
    );
    engine
        .shutdown(Duration::from_secs(2))
        .expect("shutdown empty-sync engine");
}

#[test]
fn should_issue_one_physical_wal_sync_when_assertion_only_sync_transaction_commits() {
    // Arrange
    let _guard = sync_count_test_guard();
    let (_temp_dir, mut engine, cf) = open_engine();
    let mut seed = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin seed transaction");
    seed.put(b"guard".to_vec(), b"value".to_vec(), None)
        .expect("stage guard value");
    seed.commit(WriteOptions::buffered())
        .expect("commit guard value to the WAL buffer");
    let before = wal_fsync_count(&engine);
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin assertion-only transaction");
    tx.assert_value(b"guard".to_vec(), Some(b"value".to_vec()))
        .expect("register guard assertion");

    // Act
    tx.commit(WriteOptions::sync())
        .expect("commit assertion-only transaction synchronously");
    let after = wal_fsync_count(&engine);

    // Assert
    assert_eq!(
        after.saturating_sub(before),
        1,
        "an assertion-only synchronous commit must establish one physical WAL barrier"
    );
    engine
        .shutdown(Duration::from_secs(2))
        .expect("shutdown assertion-only sync engine");
}

#[test]
fn should_not_issue_physical_wal_sync_before_buffered_commit_returns() {
    // Arrange
    let _guard = sync_count_test_guard();
    let (_temp_dir, mut engine, cf) = open_engine();
    let before = wal_fsync_count(&engine);
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin buffered transaction");
    tx.put(b"buffered-key".to_vec(), b"buffered-value".to_vec(), None)
        .expect("stage buffered value");

    // Act
    tx.commit(WriteOptions::buffered())
        .expect("commit to the WAL buffer");
    let after = wal_fsync_count(&engine);

    // Assert
    assert_eq!(
        after.saturating_sub(before),
        0,
        "buffered commit must not introduce a synchronous physical barrier"
    );
    engine
        .shutdown(Duration::from_secs(2))
        .expect("shutdown buffered engine");
}

#[test]
fn should_share_physical_wal_sync_across_concurrent_strict_transactions() {
    // Arrange
    const WRITERS: usize = 16;
    let _guard = sync_count_test_guard();
    let (temp_dir, engine, cf) = open_engine();
    let engine = Arc::new(engine);
    let before_fsyncs = wal_fsync_count(&engine);
    let before_appends = wal_append_count(&engine);
    let barrier = Arc::new(Barrier::new(WRITERS + 1));
    let mut handles = Vec::with_capacity(WRITERS);
    for writer in 0..WRITERS {
        let engine = Arc::clone(&engine);
        let barrier = Arc::clone(&barrier);
        let cf_id = cf.id();
        handles.push(std::thread::spawn(move || {
            let mut tx = engine
                .begin_tx(cf_id, TransactionMode::ReadWrite)
                .expect("begin concurrent strict transaction");
            tx.put(
                format!("strict-group-{writer:02}").into_bytes(),
                format!("value-{writer:02}").into_bytes(),
                None,
            )
            .expect("stage concurrent strict value");
            barrier.wait();
            tx.commit(WriteOptions::sync())
                .expect("commit concurrent strict transaction");
        }));
    }

    // Act
    barrier.wait();
    for handle in handles {
        handle.join().expect("join concurrent strict writer");
    }
    let after_fsyncs = wal_fsync_count(&engine);
    let after_appends = wal_append_count(&engine);

    // Assert
    let physical_fsyncs = after_fsyncs.saturating_sub(before_fsyncs);
    let wal_appends = after_appends.saturating_sub(before_appends);
    assert!(
        physical_fsyncs < u64::try_from(WRITERS).expect("writer count fits in u64"),
        "concurrent strict commits must share at least one physical fsync"
    );
    assert_eq!(
        wal_appends, physical_fsyncs,
        "each strict group should use one WAL append and one physical fsync"
    );
    for writer in 0..WRITERS {
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin verification transaction");
        assert_eq!(
            tx.get(format!("strict-group-{writer:02}").as_bytes())
                .expect("read concurrent strict value")
                .as_deref(),
            Some(format!("value-{writer:02}").as_bytes())
        );
    }

    let mut engine =
        Arc::try_unwrap(engine).unwrap_or_else(|_| panic!("all writer handles must be released"));
    engine
        .shutdown(Duration::from_secs(2))
        .expect("shutdown grouped strict engine");
    let reopened = Engine::open(
        OpenOptions::local(temp_dir.path())
            .build()
            .expect("build reopen options"),
    )
    .expect("reopen grouped strict engine");
    let reopened_cf = reopened
        .get_column_family("default")
        .expect("reopened default column family");
    for writer in 0..WRITERS {
        let tx = reopened
            .begin_tx(reopened_cf.id(), TransactionMode::ReadOnly)
            .expect("begin reopened verification transaction");
        assert_eq!(
            tx.get(format!("strict-group-{writer:02}").as_bytes())
                .expect("read recovered strict value")
                .as_deref(),
            Some(format!("value-{writer:02}").as_bytes())
        );
    }
}
