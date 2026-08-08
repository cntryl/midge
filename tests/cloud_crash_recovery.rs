use bytes::Bytes;
mod common;

use cntryl_midge::{
    CloudWritePolicy, Engine, RuntimeMetricsSnapshot, TransactionMode, WriteOptions,
};
use common::{crash, MidgeOptions, StorageMode};
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
        "cloud_async_active_after_ack" => child_cloud_async_active_after_ack(&db_path),
        #[cfg(feature = "failpoints")]
        "cloud_async_local_wal_after_ack" => child_cloud_async_local_wal_after_ack(&db_path),
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
fn should_clear_acquisition_lock_when_simulating_crash_timeout() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();
    std::fs::write(
        db_path.join("midge_primary_lease.json"),
        "holder_id: crashed@host\nowner_token: lease-token\nacquired_at: 2026-07-26T11:00:00Z\nexpires_at: 2026-07-26T11:00:30Z\n",
    )
    .expect("write lease record");
    std::fs::write(
        db_path.join(".midge_leader.lock"),
        "holder_id=crashed@host\nowner_token=lock-token\ncreated_at=2026-07-26T11:00:10Z\n",
    )
    .expect("write acquisition lock");

    // Act
    expire_crashed_process_lease(db_path);

    // Assert
    assert!(!db_path.join(".midge_leader.lock").exists());
    let _reopened = open_cloud_engine(db_path, None);
}

#[test]
fn should_clear_incomplete_acquisition_lock_when_crashed_child_has_exited() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();
    std::fs::write(
        db_path.join("midge_primary_lease.json"),
        "holder_id: crashed@host\nowner_token: lease-token\nacquired_at: 2026-07-26T11:00:00Z\nexpires_at: 2026-07-26T11:00:30Z\n",
    )
    .expect("write lease record");
    std::fs::write(db_path.join(".midge_leader.lock"), []).expect("write incomplete lock");

    // Act
    expire_crashed_process_lease(db_path);

    // Assert
    assert!(!db_path.join(".midge_leader.lock").exists());
    let _reopened = open_cloud_engine(db_path, None);
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
fn should_resume_cloud_upload_from_recovered_active_wal_after_child_abort() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    run_child_expect_abort("cloud_async_active_after_ack", db_path);
    expire_crashed_process_lease(db_path);
    let active_wal = db_path.join("wal").join("wal.log");
    assert!(
        std::fs::metadata(&active_wal).is_ok_and(|metadata| metadata.len() > 0),
        "crashed child must leave the acknowledged write in the active WAL"
    );

    // Act
    let reopened = open_cloud_engine(db_path, None);
    let metrics = wait_for_metrics(&reopened, Duration::from_secs(10), |metrics| {
        metrics.current_sequence >= 1 && metrics.wal_cloud_durable_seq >= metrics.current_sequence
    });

    // Assert
    assert_value_visible(
        &reopened,
        b"cloud-async-active-crash-key",
        b"cloud-async-active-crash-value",
    );
    assert!(metrics.wal_cloud_durable_seq >= metrics.current_sequence);
    let remote_wal_dir = db_path.join("cloud_store").join("wal");
    assert!(
        contains_file_with_extension(&remote_wal_dir, "wal"),
        "recovered active WAL must be sealed and uploaded"
    );
}

#[cfg(feature = "failpoints")]
#[test]
fn should_recover_local_cloud_async_wal_when_child_aborts_before_upload() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    run_child_expect_abort("cloud_async_local_wal_after_ack", db_path);
    expire_crashed_process_lease(db_path);

    // Act
    let reopened = open_cloud_engine(db_path, None);

    // Assert
    assert_value_visible(
        &reopened,
        b"cloud-async-local-crash-key",
        b"cloud-async-local-crash-value",
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

    for index in 0..17 {
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

    abort_after_marking_ready(db_path, "cloud_strict_after_ack");
}

fn child_cloud_async_active_after_ack(db_path: &Path) {
    let engine = open_cloud_engine(db_path, Some(unsealed_cloud_policy()));
    commit_value(
        &engine,
        b"cloud-async-active-crash-key",
        b"cloud-async-active-crash-value",
        WriteOptions::cloud_async(),
    );

    abort_after_marking_ready(db_path, "cloud_async_active_after_ack");
}

#[cfg(feature = "failpoints")]
fn child_cloud_async_local_wal_after_ack(db_path: &Path) {
    let _scenario = fail::FailScenario::setup();
    fail::cfg("midge::cloud::inject_fail_wal_upload", "return")
        .expect("configure WAL upload failure");
    let engine = open_cloud_engine(db_path, None);
    commit_value(
        &engine,
        b"cloud-async-local-crash-key",
        b"cloud-async-local-crash-value",
        WriteOptions::cloud_async(),
    );
    wait_for_metrics(&engine, Duration::from_secs(10), |metrics| {
        metrics.current_sequence >= 1 && metrics.wal_cloud_durable_seq < metrics.current_sequence
    });

    abort_after_marking_ready(db_path, "cloud_async_local_wal_after_ack");
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
            WriteOptions::cloud_async(),
        );
    }

    let pre_publish_metrics = engine
        .get_runtime_metrics()
        .expect("runtime metrics before publish");
    assert!(
        pre_publish_metrics.flush_enqueued_total > 0,
        "buffered cloud writes should trigger at least one eventual flush"
    );

    let _published = wait_for_metrics(&engine, Duration::from_secs(10), |metrics| {
        metrics.sst_count >= 1 && metrics.manifest_last_persisted_sequence > 0
    });
    // Add one acknowledged write after publication, below the four-segment
    // eventual-flush trigger. Recovery must reconcile an authoritative SST
    // with this newer WAL-only frontier.
    commit_value(
        &engine,
        b"cloud-buffered-crash-key-0016",
        b"cloud-buffered-crash-value",
        WriteOptions::cloud_async(),
    );
    wait_for_metrics(&engine, Duration::from_secs(10), |metrics| {
        metrics.current_sequence >= 17 && metrics.wal_cloud_durable_seq >= metrics.current_sequence
    });
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin read tx");
    assert_eq!(
        tx.get(b"cloud-buffered-crash-key-0000")
            .expect("read back buffered key before crash"),
        Some(Bytes::from_static(b"cloud-buffered-crash-value"))
    );

    abort_after_marking_ready(db_path, "buffered_eventual_flush_after_publish");
}

fn abort_after_marking_ready(_db_path: &Path, scenario: &str) -> ! {
    crash::abort_at_trigger(scenario, cloud_crash_trigger(scenario));
}

fn buffered_cloud_policy() -> CloudWritePolicy {
    CloudWritePolicy {
        eventual_flush_segment_gap: EVENTUAL_FLUSH_GAP,
        wal_seal_min_segment_bytes: usize::MAX,
        wal_seal_max_flush_delay: Duration::from_hours(1),
        wal_seal_max_pending_writes: 1,
    }
}

fn unsealed_cloud_policy() -> CloudWritePolicy {
    CloudWritePolicy {
        eventual_flush_segment_gap: u64::MAX,
        wal_seal_min_segment_bytes: usize::MAX,
        wal_seal_max_flush_delay: Duration::from_hours(1),
        wal_seal_max_pending_writes: usize::MAX,
    }
}

fn open_cloud_engine(db_path: &Path, policy: Option<CloudWritePolicy>) -> Engine {
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
        cloud_write_policy: policy,
        simulated_cloud_local_storage_budget_bytes: None,
    };

    Engine::open(opts.to_open_options()).expect("open cloud engine")
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
    let mut command = Command::new(current_exe);
    command
        .arg("--exact")
        .arg(CHILD_TEST_NAME)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(ENV_SCENARIO, scenario)
        .env(ENV_DB_PATH, db_path);
    crash::run_child_expect_abort(
        &mut command,
        scenario,
        cloud_crash_trigger(scenario),
        db_path,
    );
}

fn cloud_crash_trigger(scenario: &str) -> &'static str {
    match scenario {
        "cloud_strict_after_ack" => "manual::cloud_strict_after_ack",
        "cloud_async_active_after_ack" => "manual::cloud_async_active_after_ack",
        "cloud_async_local_wal_after_ack" => "manual::cloud_async_local_wal_after_ack",
        "buffered_eventual_flush_after_publish" => "manual::buffered_eventual_flush_after_publish",
        other => panic!("unknown cloud crash scenario: {other}"),
    }
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

    crash::clear_crashed_process_acquisition_lock(db_path);
}

fn reset_dir(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).expect("recreate directory");
}

fn contains_file_with_extension(dir: &Path, extension: &str) -> bool {
    std::fs::read_dir(dir)
        .unwrap_or_else(|error| panic!("list {}: {error}", dir.display()))
        .flatten()
        .any(|entry| {
            let path = entry.path();
            if path.is_dir() {
                contains_file_with_extension(&path, extension)
            } else {
                path.extension().and_then(|value| value.to_str()) == Some(extension)
            }
        })
}
