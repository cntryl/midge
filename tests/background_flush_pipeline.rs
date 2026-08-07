use bytes::Bytes;
use cntryl_midge::{
    ColumnFamilyId, Engine, MidgeError, OpenOptions, TransactionMode, WriteOptions,
};
use std::sync::{mpsc, Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tempfile::TempDir;

static FAILPOINT_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn failpoint_test_lock() -> &'static Mutex<()> {
    FAILPOINT_TEST_LOCK.get_or_init(|| Mutex::new(()))
}

fn open_small_memtable_engine(temp_dir: &TempDir) -> Arc<Engine> {
    Arc::new(
        Engine::open(
            OpenOptions::local(temp_dir.path())
                .with_memtable_size_limit(512 * 1024)
                .with_memtable_flush_threshold(128 * 1024)
                .background_compaction(false)
                .build()
                .expect("build options"),
        )
        .expect("open engine"),
    )
}

fn commit_sync_put(engine: &Engine, cf_id: ColumnFamilyId, key: &[u8], value: Vec<u8>) {
    let mut tx = engine
        .begin_tx(cf_id, TransactionMode::ReadWrite)
        .expect("begin write transaction");
    tx.put(key.to_vec(), value, None).expect("stage put");
    tx.commit(WriteOptions::sync()).expect("commit sync put");
}

fn seed_visible_values(engine: &Engine, cf_id: ColumnFamilyId) {
    let mut tx = engine
        .begin_tx(cf_id, TransactionMode::ReadWrite)
        .expect("begin seed transaction");
    tx.put(b"overwrite".to_vec(), b"old".to_vec(), None)
        .expect("seed overwrite key");
    tx.put(b"point-delete".to_vec(), b"old".to_vec(), None)
        .expect("seed point delete key");
    tx.put(b"range-b".to_vec(), b"old".to_vec(), None)
        .expect("seed range key");
    tx.commit(WriteOptions::sync()).expect("commit seed values");
}

fn rotate_with_mixed_operations(engine: &Engine, cf_id: ColumnFamilyId) {
    let mut tx = engine
        .begin_tx(cf_id, TransactionMode::ReadWrite)
        .expect("begin rotation transaction");
    tx.delete_range(b"range-a".to_vec(), b"range-z".to_vec())
        .expect("stage range delete");
    tx.put(b"overwrite".to_vec(), b"new".to_vec(), None)
        .expect("stage overwrite");
    tx.delete(b"point-delete".to_vec())
        .expect("stage point delete");
    tx.put(b"rotation-payload".to_vec(), vec![0xA5; 160 * 1024], None)
        .expect("stage rotation payload");
    tx.commit(WriteOptions::sync())
        .expect("commit rotation transaction");
}

fn wait_for_metrics(
    engine: &Engine,
    timeout: Duration,
    predicate: impl Fn(&cntryl_midge::RuntimeMetricsSnapshot) -> bool,
) -> cntryl_midge::RuntimeMetricsSnapshot {
    let deadline = Instant::now() + timeout;
    loop {
        let metrics = engine.get_runtime_metrics().expect("runtime metrics");
        if predicate(&metrics) {
            return metrics;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for flush state: {metrics:?}"
        );
        std::thread::yield_now();
    }
}

fn assert_visibility_while_flush_is_blocked(
    engine: &Engine,
    cf_id: ColumnFamilyId,
    old_snapshot: &cntryl_midge::Transaction,
) {
    let current = engine
        .begin_tx(cf_id, TransactionMode::ReadOnly)
        .expect("begin current read");
    assert_eq!(
        current.get(b"overwrite").expect("read overwrite"),
        Some(Bytes::from_static(b"new"))
    );
    assert_eq!(
        current.get(b"point-delete").expect("read point delete"),
        None
    );
    assert_eq!(current.get(b"range-b").expect("read range delete"), None);

    assert_eq!(
        old_snapshot.get(b"overwrite").expect("read old overwrite"),
        Some(Bytes::from_static(b"old"))
    );
    assert_eq!(
        old_snapshot
            .get(b"point-delete")
            .expect("read old point delete"),
        Some(Bytes::from_static(b"old"))
    );
    assert_eq!(
        old_snapshot.get(b"range-b").expect("read old range key"),
        Some(Bytes::from_static(b"old"))
    );
}

fn assert_followup_sync_commit_completes(engine: &Arc<Engine>, cf_id: ColumnFamilyId) {
    let (result_tx, result_rx) = mpsc::sync_channel(1);
    let worker_engine = Arc::clone(engine);
    let worker = std::thread::spawn(move || {
        let result = (|| {
            let mut tx = worker_engine.begin_tx(cf_id, TransactionMode::ReadWrite)?;
            tx.put(b"foreground-followup".to_vec(), b"committed".to_vec(), None)?;
            tx.commit(WriteOptions::sync())
        })();
        result_tx.send(result).expect("send commit result");
    });

    let result = result_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("sync commit must complete while flush I/O is blocked");
    result.expect("follow-up sync commit");
    worker.join().expect("join commit worker");
}

#[test]
fn should_keep_foreground_responsive_while_sst_build_is_blocked() {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let scenario = fail::FailScenario::setup();
    fail::cfg("midge::flush_worker::before_build", "pause").expect("configure build pause");
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = open_small_memtable_engine(&temp_dir);
    let cf = engine
        .create_column_family("background-build")
        .expect("create column family");
    seed_visible_values(&engine, cf.id());
    let old_snapshot = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin pre-rotation snapshot");

    // Act
    rotate_with_mixed_operations(&engine, cf.id());
    let blocked = wait_for_metrics(&engine, Duration::from_secs(2), |metrics| {
        metrics.flush_inflight == 1 && metrics.immutable_memtables >= 1
    });
    assert_visibility_while_flush_is_blocked(&engine, cf.id(), &old_snapshot);
    assert_followup_sync_commit_completes(&engine, cf.id());

    fail::remove("midge::flush_worker::before_build");
    drop(old_snapshot);
    engine.flush_cf(&cf).expect("flush after releasing build");
    scenario.teardown();

    // Assert
    let finished = engine.get_runtime_metrics().expect("finished metrics");
    assert_eq!(blocked.flush_build_count, 0);
    assert!(finished.flush_build_count >= 1);
    assert!(finished.flush_publish_count >= 1);
    assert_eq!(finished.flush_inflight, 0);
}

#[test]
fn should_keep_foreground_responsive_while_publication_is_blocked() {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let scenario = fail::FailScenario::setup();
    fail::cfg("midge::flush_worker::before_publication", "pause")
        .expect("configure publication pause");
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = open_small_memtable_engine(&temp_dir);
    let cf = engine
        .create_column_family("background-publish")
        .expect("create column family");
    seed_visible_values(&engine, cf.id());
    let old_snapshot = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin pre-rotation snapshot");

    // Act
    rotate_with_mixed_operations(&engine, cf.id());
    let blocked = wait_for_metrics(&engine, Duration::from_secs(2), |metrics| {
        metrics.flush_build_count >= 1
            && metrics.flush_publish_count == 0
            && metrics.flush_inflight == 1
    });
    assert_visibility_while_flush_is_blocked(&engine, cf.id(), &old_snapshot);
    assert_followup_sync_commit_completes(&engine, cf.id());

    fail::remove("midge::flush_worker::before_publication");
    drop(old_snapshot);
    engine
        .flush_cf(&cf)
        .expect("flush after releasing publication");
    scenario.teardown();

    // Assert
    let finished = engine.get_runtime_metrics().expect("finished metrics");
    assert_eq!(blocked.flush_publish_count, 0);
    assert!(finished.flush_publish_count >= 1);
    assert_eq!(finished.flush_inflight, 0);
}

#[test]
fn should_retry_retained_immutable_after_flush_worker_panics() {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let scenario = fail::FailScenario::setup();
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = open_small_memtable_engine(&temp_dir);
    let cf = engine
        .create_column_family("worker-panic")
        .expect("create column family");
    fail::cfg("midge::flush_worker::before_build", "panic").expect("configure build panic");

    // Act
    commit_sync_put(&engine, cf.id(), b"panic-value", vec![0x5A; 160 * 1024]);
    let failed = wait_for_metrics(&engine, Duration::from_secs(2), |metrics| {
        metrics.flush_failures_total >= 1
    });
    fail::remove("midge::flush_worker::before_build");
    engine
        .flush_cf(&cf)
        .expect("retry retained immutable after panic");
    scenario.teardown();

    // Assert
    assert!(failed.immutable_memtables >= 1);
    let metrics = engine.get_runtime_metrics().expect("runtime metrics");
    assert!(metrics.flush_failures_total >= 1);
    assert!(metrics.flush_retries_total >= 1);
    assert_eq!(metrics.immutable_memtables, 0);
    assert_eq!(metrics.flush_inflight, 0);
}

#[test]
fn should_retain_writer_lease_until_blocked_flush_worker_exits() {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let scenario = fail::FailScenario::setup();
    fail::cfg("midge::flush_worker::before_publication", "pause")
        .expect("configure publication pause");
    let temp_dir = TempDir::new().expect("temp dir");
    let options = OpenOptions::local(temp_dir.path())
        .with_memtable_size_limit(512 * 1024)
        .with_memtable_flush_threshold(128 * 1024)
        .background_compaction(false)
        .build()
        .expect("build options");
    let mut engine = Engine::open(options.clone()).expect("open engine");
    let cf = engine
        .create_column_family("shutdown-fence")
        .expect("create column family");
    commit_sync_put(&engine, cf.id(), b"blocked", vec![0x7A; 160 * 1024]);
    wait_for_metrics(&engine, Duration::from_secs(2), |metrics| {
        metrics.flush_build_count >= 1 && metrics.flush_inflight == 1
    });

    // Act
    let first_shutdown = engine.shutdown(Duration::from_millis(25));
    let contending_open = Engine::open(options.clone());
    fail::remove("midge::flush_worker::before_publication");
    let second_shutdown = engine.shutdown(Duration::from_secs(2));
    scenario.teardown();

    // Assert
    assert!(matches!(first_shutdown, Err(MidgeError::Timeout(_))));
    assert!(matches!(contending_open, Err(MidgeError::LeaseHeld(_))));
    second_shutdown.expect("cleanup reaper should finish after publication resumes");
    let mut reopened = Engine::open(options).expect("lease should be acquirable after worker exit");
    reopened
        .shutdown(Duration::from_secs(2))
        .expect("shutdown reopened engine");
}

#[test]
fn should_serialize_column_family_drop_after_complete_flush_lifecycle() {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let scenario = fail::FailScenario::setup();
    fail::cfg("midge::flush_worker::before_build", "pause").expect("configure build pause");
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = open_small_memtable_engine(&temp_dir);
    let cf = engine
        .create_column_family("drop-after-flush")
        .expect("create column family");
    commit_sync_put(&engine, cf.id(), b"drop-key", b"drop-value".to_vec());

    let (flush_tx, flush_rx) = mpsc::sync_channel(1);
    let flush_engine = Arc::clone(&engine);
    let flush_cf = cf.clone();
    let flush_thread = std::thread::spawn(move || {
        flush_tx
            .send(flush_engine.flush_cf(&flush_cf))
            .expect("send flush result");
    });
    wait_for_metrics(&engine, Duration::from_secs(2), |metrics| {
        metrics.flush_inflight == 1 && metrics.immutable_memtables == 1
    });

    // Act
    let (drop_tx, drop_rx) = mpsc::sync_channel(1);
    let drop_engine = Arc::clone(&engine);
    let cf_id = cf.id();
    let drop_thread = std::thread::spawn(move || {
        drop_tx
            .send(drop_engine.drop_column_family(cf_id))
            .expect("send drop result");
    });
    assert!(
        drop_rx.recv_timeout(Duration::from_millis(50)).is_err(),
        "target column-family deletion must wait for its active flush"
    );
    fail::remove("midge::flush_worker::before_build");
    flush_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("flush barrier should resolve")
        .expect("flush should succeed");
    drop_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("drop should resolve after flush")
        .expect("drop should succeed");
    flush_thread.join().expect("join flush thread");
    drop_thread.join().expect("join drop thread");
    scenario.teardown();

    // Assert
    let metrics = engine.get_runtime_metrics().expect("runtime metrics");
    assert_eq!(metrics.flush_inflight, 0);
    assert_eq!(metrics.immutable_memtables, 0);
    let staging_dir = temp_dir.path().join("sst/.flush-staging");
    assert!(
        !staging_dir.exists()
            || std::fs::read_dir(staging_dir)
                .expect("read staging directory")
                .next()
                .is_none(),
        "completed flush/drop must not leak staging files"
    );
}
