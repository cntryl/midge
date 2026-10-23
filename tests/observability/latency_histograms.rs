//! Per-engine latency histograms exported through the runtime metrics
//! snapshot (#930). Counts are exact except for point reads, which are
//! sampled; percentiles are bucket upper bounds.
use cntryl_midge::{Engine, LatencySummary, OpenOptions, Query, TransactionMode, WriteOptions};

fn assert_populated(name: &str, summary: &LatencySummary, expected_count: u64) {
    assert_eq!(summary.count, expected_count, "{name} count");
    assert_ordered(name, summary);
}

fn assert_ordered(name: &str, summary: &LatencySummary) {
    assert!(
        summary.p50_ns > 0,
        "{name} p50 must be a positive bucket bound"
    );
    assert!(
        summary.p50_ns <= summary.p95_ns
            && summary.p95_ns <= summary.p99_ns
            && summary.p99_ns <= summary.p999_ns,
        "{name} percentiles must be monotonic: {summary:?}"
    );
}

#[test]
fn should_populate_every_exercised_histogram_given_local_workload_when_metrics_read() {
    // Arrange
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(
        OpenOptions::local(dir.path())
            .background_compaction(false)
            .build()
            .unwrap(),
    )
    .unwrap();
    let cf = engine.create_column_family("latency").unwrap();
    let fresh = engine.metrics().get_runtime_metrics().unwrap().latency;
    assert_eq!(fresh.commit_sync.count, 0);
    assert_eq!(fresh.point_read.count, 0);
    assert_eq!(fresh.flush.count, 0);

    // Act: 20 strict commits, 20 buffered commits, one flush, 40 point reads,
    // one full scan.
    for i in 0_u32..20 {
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(i.to_be_bytes().to_vec(), b"sync".to_vec(), None)
            .unwrap();
        tx.commit(WriteOptions::sync()).unwrap();
    }
    for i in 20_u32..40 {
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(i.to_be_bytes().to_vec(), b"buffered".to_vec(), None)
            .unwrap();
        tx.commit(WriteOptions::buffered()).unwrap();
    }
    engine.flush_cf(&cf).unwrap();
    let read = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
    for i in 0_u32..40 {
        assert!(read.get(&i.to_be_bytes()).unwrap().is_some());
    }
    let rows = read.scan(&Query::new()).unwrap().count();
    assert_eq!(rows, 40);
    drop(read);
    let after = engine.metrics().get_runtime_metrics().unwrap().latency;

    // Assert: exact histograms count exactly what ran, the sampled point-read
    // histogram saw a bounded sample of the 40 reads, the histograms not
    // exercised stay empty, and percentiles are ordered.
    assert_populated("commit_sync", &after.commit_sync, 20);
    assert_populated("commit_buffered", &after.commit_buffered, 20);
    assert_eq!(after.commit_best_effort.count, 0);
    assert_eq!(after.commit_cloud_async.count, 0);
    assert_eq!(after.commit_cloud_strict.count, 0);
    assert!(
        (1..=40).contains(&after.point_read.count),
        "40 reads sampled one in sixteen: {:?}",
        after.point_read
    );
    assert_ordered("point_read", &after.point_read);
    assert_populated("scan", &after.scan, 1);
    assert_populated("flush", &after.flush, 1);
    assert!(
        after.wal_fsync.count >= 20,
        "each strict commit waits on at least one fsync: {:?}",
        after.wal_fsync
    );
    assert_eq!(after.compaction.count, 0);
    assert_eq!(after.cloud_wal_upload.count, 0);
    assert_eq!(after.cloud_metadata_mirror.count, 0);
}

#[test]
fn should_record_one_compaction_duration_per_compaction_given_overlapping_ssts_when_compact_all_runs(
) {
    // Arrange
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(
        OpenOptions::local(dir.path())
            .background_compaction(false)
            .build()
            .unwrap(),
    )
    .unwrap();
    let cf = engine.create_column_family("compaction-latency").unwrap();
    for round in 0_u32..3 {
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        for i in 0_u32..32 {
            tx.put(
                i.to_be_bytes().to_vec(),
                format!("round-{round}").into_bytes(),
                None,
            )
            .unwrap();
        }
        tx.commit(WriteOptions::sync()).unwrap();
        engine.flush_cf(&cf).unwrap();
    }
    let before = engine.metrics().get_runtime_metrics().unwrap();
    assert_eq!(before.latency.compaction.count, 0);

    // Act
    engine.compact_all().unwrap();

    // Assert
    let after = engine.metrics().get_runtime_metrics().unwrap();
    assert_eq!(
        after.latency.compaction.count, after.compactions_run,
        "one compaction duration per completed compaction: {after:?}"
    );
    assert!(after.latency.compaction.count >= 1);
    assert_ordered("compaction", &after.latency.compaction);
}
