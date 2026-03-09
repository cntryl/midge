use bytes::Bytes;
use cntryl_midge::{Engine, MidgeError, OpenOptions, TransactionMode, WriteOptions};
use std::path::Path;
use std::sync::{Mutex, OnceLock};
use tempfile::TempDir;

static FAILPOINT_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

#[test]
fn should_reject_transaction_when_no_space_hits_before_batch_append_and_remain_usable() {
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();
    let engine = open_local_engine(db_path);
    let cf = default_cf(&engine);

    let scenario = fail::FailScenario::setup();
    fail::cfg("midge::wal::inject_no_space_on_txn_append_batch", "return")
        .expect("configure txn batch no-space failpoint");

    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin write tx");
    tx.put(b"batch-fail-a".to_vec(), b"value-a".to_vec(), None)
        .expect("put a");
    tx.put(b"batch-fail-b".to_vec(), b"value-b".to_vec(), None)
        .expect("put b");

    let error = engine
        .commit(tx, WriteOptions::sync())
        .expect_err("txn append batch should fail with no space");
    assert_no_space_like(&error);
    assert_absent(&engine, &cf, b"batch-fail-a");
    assert_absent(&engine, &cf, b"batch-fail-b");
    fail::remove("midge::wal::inject_no_space_on_txn_append_batch");
    scenario.teardown();

    let mut recovery_tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin recovery tx");
    recovery_tx
        .put(b"batch-recovery".to_vec(), b"value".to_vec(), None)
        .expect("put recovery value");
    engine
        .commit(recovery_tx, WriteOptions::sync())
        .expect("commit recovery txn");

    drop(engine);

    let reopened = open_local_engine(db_path);
    let reopened_cf = default_cf(&reopened);
    assert_absent(&reopened, &reopened_cf, b"batch-fail-a");
    assert_absent(&reopened, &reopened_cf, b"batch-fail-b");
    assert_visible(&reopened, &reopened_cf, b"batch-recovery", b"value");
}

#[test]
fn should_not_leak_partial_transaction_when_no_space_hits_before_commit_marker_append() {
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();
    let engine = open_local_engine(db_path);
    let cf = default_cf(&engine);

    let scenario = fail::FailScenario::setup();
    fail::cfg("midge::wal::inject_no_space_on_txn_commit_append", "return")
        .expect("configure txn commit no-space failpoint");

    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin write tx");
    tx.put(b"commit-fail-a".to_vec(), b"value-a".to_vec(), None)
        .expect("put a");
    tx.put(b"commit-fail-b".to_vec(), b"value-b".to_vec(), None)
        .expect("put b");
    tx.put(b"commit-fail-c".to_vec(), b"value-c".to_vec(), None)
        .expect("put c");

    let error = engine
        .commit(tx, WriteOptions::sync())
        .expect_err("txn commit append should fail with no space");
    assert_no_space_like(&error);
    assert_absent(&engine, &cf, b"commit-fail-a");
    assert_absent(&engine, &cf, b"commit-fail-b");
    assert_absent(&engine, &cf, b"commit-fail-c");
    fail::remove("midge::wal::inject_no_space_on_txn_commit_append");
    scenario.teardown();

    drop(engine);

    let reopened = open_local_engine(db_path);
    let reopened_cf = default_cf(&reopened);
    assert_absent(&reopened, &reopened_cf, b"commit-fail-a");
    assert_absent(&reopened, &reopened_cf, b"commit-fail-b");
    assert_absent(&reopened, &reopened_cf, b"commit-fail-c");
}

#[test]
fn should_preserve_existing_keys_when_delete_range_wal_append_hits_no_space() {
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();
    let engine = open_local_engine(db_path);
    let cf = default_cf(&engine);

    seed_range(&engine, &cf, 0..10, "range");

    let scenario = fail::FailScenario::setup();
    fail::cfg(
        "midge::wal::inject_no_space_on_delete_range_append",
        "return",
    )
    .expect("configure delete_range no-space failpoint");
    let error = engine
        .delete_range(
            &cf,
            b"range-03".to_vec(),
            b"range-07".to_vec(),
            WriteOptions::sync(),
        )
        .expect_err("delete_range should fail with no space");
    assert_no_space_like(&error);

    for index in 0..10 {
        let key = format!("range-{index:02}");
        let value = format!("value-{index:02}");
        assert_visible(&engine, &cf, key.as_bytes(), value.as_bytes());
    }
    fail::remove("midge::wal::inject_no_space_on_delete_range_append");
    scenario.teardown();

    drop(engine);

    let reopened = open_local_engine(db_path);
    let reopened_cf = default_cf(&reopened);
    for index in 0..10 {
        let key = format!("range-{index:02}");
        let value = format!("value-{index:02}");
        assert_visible(&reopened, &reopened_cf, key.as_bytes(), value.as_bytes());
    }
}

#[test]
fn should_recover_wal_state_when_flush_sst_finalize_hits_no_space() {
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();
    let engine = open_local_engine(db_path);
    let cf = default_cf(&engine);

    seed_range(&engine, &cf, 0..12, "flush");

    let scenario = fail::FailScenario::setup();
    fail::cfg("midge::sst::inject_no_space_on_finish_to_path", "return")
        .expect("configure sst finalize no-space failpoint");
    let error = engine
        .flush_cf(&cf)
        .expect_err("flush should fail with no space");
    assert_no_space_like(&error);
    assert_eq!(
        count_sst_files(db_path),
        0,
        "failed flush must not publish any SST files"
    );
    fail::remove("midge::sst::inject_no_space_on_finish_to_path");
    scenario.teardown();

    drop(engine);

    let reopened = open_local_engine(db_path);
    let reopened_cf = default_cf(&reopened);
    for index in 0..12 {
        let key = format!("flush-{index:02}");
        let value = format!("value-{index:02}");
        assert_visible(&reopened, &reopened_cf, key.as_bytes(), value.as_bytes());
    }
    reopened
        .flush_cf(&reopened_cf)
        .expect("flush after fault clears");
}

#[test]
fn should_preserve_compacted_input_state_when_compaction_output_hits_no_space() {
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();
    let engine = open_local_engine(db_path);
    let cf = default_cf(&engine);

    for batch in 0..6 {
        for index in 0..25 {
            let key = format!("cmp-b{batch}-k{index:02}");
            let value = format!("value-b{batch}-k{index:02}");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin batch tx");
            tx.put(key.into_bytes(), value.into_bytes(), None)
                .expect("put compaction seed");
            engine
                .commit(tx, WriteOptions::sync())
                .expect("commit compaction seed");
        }
        engine.flush_cf(&cf).expect("flush compaction seed");
    }

    let initial_sst_count = engine
        .get_runtime_metrics()
        .expect("runtime metrics")
        .sst_count;
    assert!(
        initial_sst_count >= 6,
        "expected multiple L0 files before forced compaction"
    );

    let scenario = fail::FailScenario::setup();
    fail::cfg("midge::sst::inject_no_space_on_finish_to_path", "return")
        .expect("configure compaction output no-space failpoint");
    engine
        .compact_all()
        .expect("compact_all returns after failure");
    assert_eq!(
        engine
            .get_runtime_metrics()
            .expect("runtime metrics")
            .sst_count,
        initial_sst_count,
        "failed compaction must not publish partial output SSTs"
    );
    fail::remove("midge::sst::inject_no_space_on_finish_to_path");
    scenario.teardown();

    drop(engine);

    let reopened = open_local_engine(db_path);
    let reopened_cf = default_cf(&reopened);
    for batch in 0..6 {
        for index in 0..25 {
            let key = format!("cmp-b{batch}-k{index:02}");
            let value = format!("value-b{batch}-k{index:02}");
            assert_visible(&reopened, &reopened_cf, key.as_bytes(), value.as_bytes());
        }
    }
}

fn failpoint_test_lock() -> &'static Mutex<()> {
    FAILPOINT_TEST_LOCK.get_or_init(|| Mutex::new(()))
}

fn open_local_engine(db_path: &Path) -> Engine {
    Engine::open(OpenOptions::local(db_path).build()).expect("open engine")
}

fn default_cf(engine: &Engine) -> cntryl_midge::ColumnFamilyHandle {
    engine
        .get_column_family("default")
        .expect("default column family")
}

fn seed_range(
    engine: &Engine,
    cf: &cntryl_midge::ColumnFamilyHandle,
    range: std::ops::Range<u32>,
    prefix: &str,
) {
    for index in range {
        let key = format!("{prefix}-{index:02}");
        let value = format!("value-{index:02}");
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin seed tx");
        tx.put(key.into_bytes(), value.into_bytes(), None)
            .expect("put seed value");
        engine
            .commit(tx, WriteOptions::sync())
            .expect("commit seed value");
    }
}

fn assert_visible(
    engine: &Engine,
    cf: &cntryl_midge::ColumnFamilyHandle,
    key: &[u8],
    expected: &[u8],
) {
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin read tx");
    assert_eq!(
        tx.get(key).expect("get visible key"),
        Some(Bytes::copy_from_slice(expected)),
        "key {:?} must remain visible",
        String::from_utf8_lossy(key)
    );
}

fn assert_absent(engine: &Engine, cf: &cntryl_midge::ColumnFamilyHandle, key: &[u8]) {
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin read tx");
    assert_eq!(
        tx.get(key).expect("get absent key"),
        None,
        "key {:?} must not become visible",
        String::from_utf8_lossy(key)
    );
}

fn assert_no_space_like(error: &MidgeError) {
    assert!(
        matches!(error, MidgeError::NoSpace(_)) || error.to_string().contains("No space"),
        "expected a no-space error, got: {error}"
    );
}

fn count_sst_files(db_path: &Path) -> usize {
    let sst_dir = db_path.join("sst");
    let Ok(entries) = std::fs::read_dir(&sst_dir) else {
        return 0;
    };
    entries
        .flatten()
        .filter(|entry| {
            entry
                .path()
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext.eq_ignore_ascii_case("sst"))
                .unwrap_or(false)
        })
        .count()
}
