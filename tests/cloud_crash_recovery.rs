use bytes::Bytes;
use cntryl_midge::testkit::{CloudRuntimePolicyOverrides, MidgeOptions, StorageMode};
use cntryl_midge::{Engine, RuntimeMetricsSnapshot, TransactionMode, WriteOptions};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

const CHILD_TEST_NAME: &str = "should_abort_in_child_process_when_cloud_crash_scenario_requested";
const ENV_SCENARIO: &str = "MIDGE_CLOUD_CRASH_SCENARIO";
const ENV_DB_PATH: &str = "MIDGE_CLOUD_CRASH_DB_PATH";
const LARGE_MEMTABLE_BYTES: usize = 512 * 1024 * 1024;
const EVENTUAL_FLUSH_GAP: u64 = 4;

#[test]
fn should_abort_in_child_process_when_cloud_crash_scenario_requested() {
    // Arrange
    let Some(scenario) = std::env::var_os(ENV_SCENARIO) else {
        return;
    };

    let db_path = PathBuf::from(std::env::var_os(ENV_DB_PATH).expect("db path env"));
    match scenario.to_string_lossy().as_ref() {
        "cloud_strict_after_ack" => child_cloud_strict_after_ack(&db_path),
        "buffered_eventual_flush_after_publish" => {
            child_buffered_eventual_flush_after_publish(&db_path);
        }
        other => panic!("unknown cloud crash scenario: {other}"),
    }

    // Act
    // Assert
    panic!("child scenario returned without abort");
}

#[test]
fn should_recover_cloud_strict_write_when_cache_lost_after_child_abort() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    run_child_expect_abort("cloud_strict_after_ack", db_path);
    expire_crashed_process_lease(db_path);
    reset_dir(&db_path.join("wal"));
    reset_dir(&db_path.join("sst"));

    let reopened = open_cloud_engine(db_path, None);
    // Act
    // Assert
    assert_value_visible(
        &reopened,
        b"cloud-strict-crash-key",
        b"cloud-strict-crash-value",
    );
}

#[test]
fn should_restore_published_cloud_sst_when_cache_lost_after_child_abort() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    run_child_expect_abort("buffered_eventual_flush_after_publish", db_path);
    expire_crashed_process_lease(db_path);
    reset_dir(&db_path.join("wal"));
    reset_dir(&db_path.join("sst"));

    let reopened = open_cloud_engine(db_path, Some(buffered_cloud_policy()));
    let metrics = reopened.get_runtime_metrics().expect("runtime metrics");
    // Act
    // Assert
    assert!(
        metrics.sst_count >= 1,
        "reopen should restore at least one SST from the authoritative cloud object"
    );
    assert!(
        metrics.manifest_last_persisted_sequence > 0,
        "reopen should preserve manifest persistence progress after crash"
    );

    let layout = reopened.get_storage_layout().expect("storage layout");
    assert!(
        layout
            .levels
            .iter()
            .map(|level| level.file_count)
            .sum::<usize>()
            >= 1,
        "reopen should restore the published SST cache from cloud"
    );

    for index in 0..16 {
        let key = format!("cloud-buffered-crash-key-{index:04}");
        assert_value_visible(&reopened, key.as_bytes(), b"cloud-buffered-crash-value");
    }
}

fn child_cloud_strict_after_ack(db_path: &Path) {
    let engine = open_cloud_engine(db_path, None);
    commit_value(
        &engine,
        b"cloud-strict-crash-key",
        b"cloud-strict-crash-value",
        WriteOptions::cloud_strict(),
    );

    std::process::abort();
}

fn child_buffered_eventual_flush_after_publish(db_path: &Path) {
    let engine = open_cloud_engine(db_path, Some(buffered_cloud_policy()));
    let cf = default_cf(&engine);

    for index in 0..16 {
        let key = format!("cloud-buffered-crash-key-{index:04}");
        commit_value(
            &engine,
            key.as_bytes(),
            b"cloud-buffered-crash-value",
            WriteOptions::buffered(),
        );
    }

    let pre_publish_metrics = engine
        .get_runtime_metrics()
        .expect("runtime metrics before publish");
    assert!(
        pre_publish_metrics.max_memtable_wal_segment_gap > 0,
        "buffered cloud writes should build WAL-segment pressure before the first SST publish"
    );

    let _published = wait_for_metrics(&engine, Duration::from_secs(5), |metrics| {
        metrics.sst_count >= 1 && metrics.manifest_last_persisted_sequence > 0
    });

    // Ensure the recovered SST is the authoritative copy, not a local-cache artifact.
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin read tx");
    assert_eq!(
        tx.get(b"cloud-buffered-crash-key-0000")
            .expect("read back buffered key before crash"),
        Some(Bytes::from_static(b"cloud-buffered-crash-value"))
    );

    std::process::abort();
}

fn buffered_cloud_policy() -> CloudRuntimePolicyOverrides {
    CloudRuntimePolicyOverrides {
        eventual_flush_segment_gap: Some(EVENTUAL_FLUSH_GAP),
        wal_seal_min_segment_bytes: Some(usize::MAX),
        wal_seal_max_flush_delay: Some(Duration::from_hours(1)),
        wal_seal_max_pending_writes: Some(1),
    }
}

fn open_cloud_engine(db_path: &Path, overrides: Option<CloudRuntimePolicyOverrides>) -> Engine {
    let opts = MidgeOptions {
        storage_mode: StorageMode::CloudBacked {
            local_cache_path: db_path.to_path_buf(),
        },
        wal_sync: true,
        wal_batch_config: None,
        memtable_size: LARGE_MEMTABLE_BYTES,
        compression: false,
        enable_compaction: false,
        memory_budget: None,
        cloud_runtime_policy_overrides: overrides,
        simulated_cloud_overrides: None,
    };

    Engine::open(
        opts.to_open_options()
            .with_memtable_size_limit(LARGE_MEMTABLE_BYTES)
            .with_memtable_flush_threshold(LARGE_MEMTABLE_BYTES)
            .build(),
    )
    .expect("open cloud engine")
}

fn default_cf(engine: &Engine) -> cntryl_midge::ColumnFamilyHandle {
    engine
        .get_column_family("default")
        .expect("default column family")
}

fn commit_value(engine: &Engine, key: &[u8], value: &[u8], opts: WriteOptions) {
    let cf = default_cf(engine);
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin write tx");
    tx.put(key.to_vec(), value.to_vec(), None)
        .expect("put value");
    tx.commit(opts).expect("commit value");
}

fn assert_value_visible(engine: &Engine, key: &[u8], expected: &[u8]) {
    let cf = default_cf(engine);
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin read tx");
    assert_eq!(
        tx.get(key).expect("read value"),
        Some(Bytes::copy_from_slice(expected))
    );
}

fn wait_for_metrics<F>(engine: &Engine, timeout: Duration, predicate: F) -> RuntimeMetricsSnapshot
where
    F: Fn(&RuntimeMetricsSnapshot) -> bool,
{
    let deadline = Instant::now() + timeout;
    loop {
        let metrics = engine.get_runtime_metrics().expect("runtime metrics");
        if predicate(&metrics) {
            return metrics;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for cloud recovery condition; last metrics: sst_count={} persisted_seq={} wal_segment={} current_seq={} cloud_seq={} gap={}",
            metrics.sst_count,
            metrics.manifest_last_persisted_sequence,
            metrics.wal_current_segment_id,
            metrics.current_sequence,
            metrics.wal_cloud_durable_seq,
            metrics.max_memtable_wal_segment_gap
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn run_child_expect_abort(scenario: &str, db_path: &Path) {
    let current_exe = std::env::current_exe().expect("current exe");
    let output = Command::new(current_exe)
        .arg("--exact")
        .arg(CHILD_TEST_NAME)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(ENV_SCENARIO, scenario)
        .env(ENV_DB_PATH, db_path)
        .output()
        .expect("run child test binary");

    assert!(
        !output.status.success(),
        "child scenario {scenario} unexpectedly exited successfully"
    );
}

fn expire_crashed_process_lease(db_path: &Path) {
    let lease_path = db_path.join("midge_primary_lease.json");
    if !lease_path.exists() {
        return;
    }

    let mut content = std::fs::read_to_string(&lease_path).expect("read lease record");
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
        std::fs::write(&lease_path, content).expect("rewrite lease record as stale");
    }
}

fn reset_dir(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).expect("recreate directory");
}
