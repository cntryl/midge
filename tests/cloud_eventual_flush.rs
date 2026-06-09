use cntryl_midge::testkit::{opts_for_mode, CloudRuntimePolicyOverrides, MidgeOptions};
use cntryl_midge::{Engine, RuntimeMetricsSnapshot, TransactionMode, WriteOptions};
use std::thread;
use std::time::{Duration, Instant};

const LARGE_MEMTABLE_BYTES: usize = 512 * 1024 * 1024;
const BUFFERED_TEST_GAP: u64 = 4;

fn buffered_cloud_policy() -> CloudRuntimePolicyOverrides {
    CloudRuntimePolicyOverrides {
        eventual_flush_segment_gap: Some(BUFFERED_TEST_GAP),
        wal_seal_min_segment_bytes: Some(usize::MAX),
        wal_seal_max_flush_delay: Some(Duration::from_secs(3600)),
        wal_seal_max_pending_writes: Some(1),
    }
}

fn open_large_cloud_engine(opts: MidgeOptions) -> Engine {
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

fn commit_small_write(
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
        .expect("put small value");
    tx.commit(opts).expect("commit small value");
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
            "timed out waiting for runtime condition; last metrics: sst_count={} persisted_seq={} wal_segment={} current_seq={} cloud_seq={} gap={}",
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

#[test]
fn should_eventually_publish_sst_given_many_cloud_strict_writes_when_memtable_never_reaches_size_threshold(
) {
    let engine = open_large_cloud_engine(opts_for_mode("cloud"));
    let cf = default_cf(&engine);

    for i in 0..160 {
        let key = format!("strict-gap-key-{i:04}");
        commit_small_write(
            &engine,
            &cf,
            key.as_bytes(),
            b"strict-gap-value",
            WriteOptions::cloud_strict(),
        );
    }

    let pre_flush_metrics = engine.get_runtime_metrics().expect("runtime metrics");
    assert!(
        pre_flush_metrics.max_memtable_wal_segment_gap > 0,
        "cloud segment gap metric should grow while WAL segments rotate ahead of SST publication"
    );

    let metrics = wait_for_metrics(&engine, Duration::from_secs(3), |metrics| {
        metrics.sst_count >= 1 && metrics.manifest_last_persisted_sequence > 0
    });

    assert!(
        metrics.wal_current_segment_id > 100,
        "expected many sealed WAL segments before the first SST publish; saw segment {}",
        metrics.wal_current_segment_id
    );
    assert!(
        metrics.max_memtable_wal_segment_gap < 128,
        "automatic flush should reset the active memtable segment gap below the production threshold; saw {}",
        metrics.max_memtable_wal_segment_gap
    );

    let layout = engine.get_storage_layout().expect("storage layout");
    assert!(
        layout
            .levels
            .iter()
            .map(|level| level.file_count)
            .sum::<usize>()
            >= 1,
        "storage layout should report the eventually published SST"
    );
}

#[test]
fn should_eventually_publish_sst_given_many_cloud_buffered_writes_when_memtable_never_reaches_size_threshold(
) {
    let opts = opts_for_mode("cloud").with_cloud_runtime_policy_overrides(buffered_cloud_policy());
    let engine = open_large_cloud_engine(opts);
    let cf = default_cf(&engine);

    for i in 0..(BUFFERED_TEST_GAP - 1) {
        let key = format!("buffered-gap-preflush-key-{i:04}");
        commit_small_write(
            &engine,
            &cf,
            key.as_bytes(),
            b"buffered-gap-value",
            WriteOptions::buffered(),
        );
    }

    let pre_flush_metrics = engine.get_runtime_metrics().expect("runtime metrics");
    assert_eq!(
        pre_flush_metrics.max_memtable_wal_segment_gap,
        BUFFERED_TEST_GAP - 1,
        "buffered cloud writes should grow the memtable segment gap before the first automatic flush"
    );

    for i in (BUFFERED_TEST_GAP - 1)..16 {
        let key = format!("buffered-gap-key-{i:04}");
        commit_small_write(
            &engine,
            &cf,
            key.as_bytes(),
            b"buffered-gap-value",
            WriteOptions::buffered(),
        );
    }

    let metrics = wait_for_metrics(&engine, Duration::from_secs(3), |metrics| {
        metrics.sst_count >= 1 && metrics.manifest_last_persisted_sequence > 0
    });

    assert!(
        metrics.wal_current_segment_id > BUFFERED_TEST_GAP,
        "buffered cloud writes should seal multiple WAL segments in the background; saw segment {}",
        metrics.wal_current_segment_id
    );
    assert!(
        metrics.max_memtable_wal_segment_gap < BUFFERED_TEST_GAP,
        "automatic flush should reset the cloud segment gap after buffered publication; saw {}",
        metrics.max_memtable_wal_segment_gap
    );
}

#[test]
fn should_publish_lightly_written_column_family_given_busy_neighbor_when_cloud_segment_gap_flush_runs(
) {
    let opts = opts_for_mode("cloud").with_cloud_runtime_policy_overrides(buffered_cloud_policy());
    let engine = open_large_cloud_engine(opts);
    let light_cf = engine
        .create_column_family("light")
        .expect("create light cf");
    let busy_cf = engine.create_column_family("busy").expect("create busy cf");

    for i in 0..(BUFFERED_TEST_GAP - 1) {
        let key = format!("light-gap-key-{i:04}");
        commit_small_write(
            &engine,
            &light_cf,
            key.as_bytes(),
            b"light-gap-value",
            WriteOptions::buffered(),
        );
    }

    for i in 0..24 {
        let key = format!("busy-gap-key-{i:04}");
        commit_small_write(
            &engine,
            &busy_cf,
            key.as_bytes(),
            b"busy-gap-value",
            WriteOptions::buffered(),
        );
    }

    let deadline = Instant::now() + Duration::from_secs(3);
    let layout = loop {
        let layout = engine.get_storage_layout().expect("storage layout");
        let light_has_sst = layout
            .levels
            .iter()
            .flat_map(|level| level.files.iter())
            .any(|file| file.cf_id == light_cf.id());
        if light_has_sst {
            break layout;
        }

        let metrics = engine.get_runtime_metrics().expect("runtime metrics");
        assert!(
            Instant::now() < deadline,
            "timed out waiting for a lightly written CF SST; last metrics: sst_count={} gap={} segment={}",
            metrics.sst_count,
            metrics.max_memtable_wal_segment_gap,
            metrics.wal_current_segment_id
        );
        thread::sleep(Duration::from_millis(25));
    };

    assert!(
        layout
            .levels
            .iter()
            .flat_map(|level| level.files.iter())
            .any(|file| file.cf_id == light_cf.id()),
        "lightly written column family should receive its own SST publication"
    );
}

#[test]
fn should_reset_memtable_wal_gap_after_reopen_before_new_segment_churn() {
    let opts =
        opts_for_mode("cloud").with_cloud_runtime_policy_overrides(CloudRuntimePolicyOverrides {
            eventual_flush_segment_gap: None,
            wal_seal_min_segment_bytes: Some(usize::MAX),
            wal_seal_max_flush_delay: Some(Duration::from_secs(3600)),
            wal_seal_max_pending_writes: Some(1),
        });

    {
        let engine = open_large_cloud_engine(opts.clone());
        let cf = default_cf(&engine);
        for i in 0..16 {
            let key = format!("reopen-gap-key-{i:04}");
            commit_small_write(
                &engine,
                &cf,
                key.as_bytes(),
                b"reopen-gap-value",
                WriteOptions::buffered(),
            );
        }

        let metrics = engine
            .get_runtime_metrics()
            .expect("runtime metrics before reopen");
        assert_eq!(
            metrics.sst_count, 0,
            "pre-restart workload should remain WAL-backed"
        );
        assert!(
            metrics.wal_current_segment_id > 10,
            "pre-restart workload should build historical WAL segment churn; saw {}",
            metrics.wal_current_segment_id
        );
    }

    let reopened = open_large_cloud_engine(opts);
    let reopened_metrics = reopened
        .get_runtime_metrics()
        .expect("runtime metrics after reopen");
    assert_eq!(
        reopened_metrics.max_memtable_wal_segment_gap, 0,
        "reopened non-empty memtables should start counting segment churn from the new runtime"
    );
    assert_eq!(
        reopened_metrics.sst_count, 0,
        "reopen should not immediately auto-flush based on historical WAL segment churn"
    );
}
