mod common;

use cntryl_midge::{MidgeError, MidgeResult, Query, TransactionMode, WriteOptions};
use common::*;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn wait_for_active_snapshots(
    engine: &cntryl_midge::Engine,
    expected: usize,
    timeout: Duration,
) -> MidgeResult<()> {
    let deadline = Instant::now() + timeout;

    loop {
        let metrics = engine.get_runtime_metrics()?;
        if metrics.active_snapshots == expected {
            return Ok(());
        }

        if Instant::now() >= deadline {
            return Err(cntryl_midge::MidgeError::Internal(format!(
                "timed out waiting for active_snapshots={}, got {}",
                expected, metrics.active_snapshots
            )));
        }

        std::thread::sleep(Duration::from_millis(10));
    }
}

fn seed_scan_pin_generations(
    engine: &cntryl_midge::Engine,
    cf: &cntryl_midge::ColumnFamilyHandle,
) -> Vec<String> {
    for generation in 0..4 {
        let mut write = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin seed generation");
        if generation == 0 {
            for index in 0..24 {
                write
                    .put(
                        format!("row-{index:03}").into_bytes(),
                        b"baseline".to_vec(),
                        None,
                    )
                    .expect("put baseline row");
            }
        } else {
            write
                .put(
                    format!("zz-filler-{generation}").into_bytes(),
                    b"baseline".to_vec(),
                    None,
                )
                .expect("put filler row");
        }
        write
            .commit(WriteOptions::sync())
            .expect("commit seed generation");
        engine.flush_cf(cf).expect("flush seed generation");
    }
    let layout = engine.get_storage_layout().expect("layout before scan");
    let input_names: Vec<_> = layout
        .levels
        .iter()
        .flat_map(|level| level.files.iter())
        .filter(|file| file.cf_id == cf.id())
        .map(|file| file.name.clone())
        .collect();
    assert_eq!(input_names.len(), 4, "fixture must expose four L0 inputs");
    input_names
}

fn wait_for_sst_files_removed(db_path: &Path, names: &[String], timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while names
        .iter()
        .any(|name| db_path.join("sst").join(name).exists())
        && Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        names
            .iter()
            .all(|name| !db_path.join("sst").join(name).exists()),
        "retired inputs should become reclaimable after the snapshot closes"
    );
}

#[test]
fn should_register_snapshot_when_begin_tx_starts_transaction() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(&opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");
        wait_for_active_snapshots(&engine, 0, Duration::from_secs(1))
            .expect("wait for zero active snapshots");

        // Act
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin read-only tx");

        // Assert
        wait_for_active_snapshots(&engine, 1, Duration::from_secs(1))
            .expect("wait for one active snapshot");

        drop(tx);
        wait_for_active_snapshots(&engine, 0, Duration::from_secs(1))
            .expect("wait for zero active snapshots after drop");
    });
}

#[test]
fn should_report_active_snapshot_immediately_when_begin_tx_returns() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(&opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin read-only tx");

        // Assert
        let metrics = engine
            .get_runtime_metrics()
            .expect("get runtime metrics immediately after begin_tx");
        assert_eq!(metrics.active_snapshots, 1, "mode: {mode}");

        drop(tx);
        wait_for_active_snapshots(&engine, 0, Duration::from_secs(1))
            .expect("wait for zero active snapshots after drop");
    });
}

#[test]
fn should_report_snapshot_retention_pressure_metrics_when_snapshot_pins_ssts() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(&opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        let mut seed = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin seed tx");
        for i in 0..32 {
            let key = format!("metric_key_{i:03}");
            seed.put(key.into_bytes(), b"v".to_vec(), None)
                .expect("seed put");
        }
        seed.commit(buffered_write_options(mode))
            .expect("seed commit");
        engine.flush_cf(&cf).expect("seed flush");

        // Act
        let snapshot = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin snapshot tx");

        // Assert
        let metrics = engine
            .get_runtime_metrics()
            .expect("get runtime metrics with active snapshot");
        assert_eq!(metrics.active_snapshots, 1, "mode: {mode}");
        assert!(metrics.pinned_ssts > 0, "mode: {mode}");
        assert!(metrics.oldest_snapshot_age_seconds <= 1, "mode: {mode}");

        drop(snapshot);
        wait_for_active_snapshots(&engine, 0, Duration::from_secs(1))
            .expect("wait for zero active snapshots after drop");
    });
}

#[test]
fn should_not_register_snapshot_given_dropped_cf_when_begin_tx_fails() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(&opts, mode);
        let cf = engine.create_column_family("dropped").expect("create cf");
        let cf_id = cf.id();

        let mut seed = engine
            .begin_tx(cf_id, TransactionMode::ReadWrite)
            .expect("begin seed tx");
        seed.put(b"cached_key".to_vec(), b"cached_value".to_vec(), None)
            .expect("seed put");
        seed.commit(buffered_write_options(mode))
            .expect("seed commit");
        engine.flush_cf(&cf).expect("flush seed data");

        wait_for_active_snapshots(&engine, 0, Duration::from_secs(1))
            .expect("wait for zero active snapshots before drop");
        engine.drop_column_family(cf_id).expect("drop cf");

        // Act and assert
        for tx_mode in [TransactionMode::ReadOnly, TransactionMode::ReadWrite] {
            let result = engine.begin_tx(cf_id, tx_mode);
            match result {
                Err(MidgeError::InvalidArgument(message)) => {
    // Assert
                    assert_eq!(message, format!("column family {cf_id} does not exist"));
                }
                Err(error) => panic!(
                    "expected InvalidArgument for dropped CF in {mode} with {tx_mode:?}, got {error}"
                ),
                Ok(_) => panic!("expected dropped CF begin_tx to fail in {mode} with {tx_mode:?}"),
            }

            let metrics = engine
                .get_runtime_metrics()
                .expect("get metrics after failed begin_tx");
            assert_eq!(
                metrics.active_snapshots, 0,
                "mode: {mode}, tx_mode: {tx_mode:?}"
            );
            assert_eq!(metrics.pinned_ssts, 0, "mode: {mode}, tx_mode: {tx_mode:?}");
        }
    });
}

#[test]
fn should_unregister_snapshot_when_commit_finishes_transaction() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(&opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin read-write tx");
        tx.put(b"k".to_vec(), b"v".to_vec(), None)
            .expect("put value");
        wait_for_active_snapshots(&engine, 1, Duration::from_secs(1))
            .expect("wait for one active snapshot before commit");
        tx.commit(buffered_write_options(mode)).expect("commit tx");

        // Assert
        wait_for_active_snapshots(&engine, 0, Duration::from_secs(1))
            .expect("wait for zero active snapshots after commit");
    });
}

#[test]
fn should_unregister_snapshot_when_rollback_ends_transaction() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(&opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let tx1 = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin read-only tx");
        wait_for_active_snapshots(&engine, 1, Duration::from_secs(1))
            .expect("wait for one active snapshot before rollback");
        tx1.rollback().expect("rollback tx");

        // Assert
        wait_for_active_snapshots(&engine, 0, Duration::from_secs(1))
            .expect("wait for zero active snapshots after rollback");
    });
}

#[test]
fn should_unregister_snapshot_when_drop_ends_transaction() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(&opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let tx2 = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin read-only tx");
        wait_for_active_snapshots(&engine, 1, Duration::from_secs(1))
            .expect("wait for one active snapshot before drop");
        drop(tx2);

        // Assert
        wait_for_active_snapshots(&engine, 0, Duration::from_secs(1))
            .expect("wait for zero active snapshots after drop");
    });
}

#[test]
fn should_preserve_snapshot_value_when_delete_is_compacted_with_snapshot_active() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(&opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        let mut seed = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin seed tx");
        seed.put(b"k".to_vec(), b"v1".to_vec(), None)
            .expect("seed put");
        seed.commit(buffered_write_options(mode))
            .expect("seed commit");
        engine.flush_cf(&cf).expect("seed flush");

        let snapshot = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin snapshot tx");
        wait_for_active_snapshots(&engine, 1, Duration::from_secs(1))
            .expect("wait for active snapshot");

        let mut deleter = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin delete tx");
        deleter.delete(b"k".to_vec()).expect("delete key");
        deleter
            .commit(buffered_write_options(mode))
            .expect("delete commit");
        engine.flush_cf(&cf).expect("delete flush");
        for index in 0..2 {
            let mut filler = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin filler tx");
            filler
                .put(
                    format!("filler-{index}").into_bytes(),
                    b"value".to_vec(),
                    None,
                )
                .expect("put filler");
            filler
                .commit(buffered_write_options(mode))
                .expect("commit filler");
            engine.flush_cf(&cf).expect("flush filler");
        }
        let layout_before = engine
            .get_storage_layout()
            .expect("layout before compaction");
        let l0_before = layout_before
            .levels
            .iter()
            .find(|level| level.level == 0)
            .map_or(0, |level| level.file_count);
        assert_eq!(l0_before, 4, "fixture must create four L0 files");

        // Act
        engine.compact_all().expect("compact all");

        // Assert
        let layout_after = engine
            .get_storage_layout()
            .expect("layout after compaction");
        let l0_after = layout_after
            .levels
            .iter()
            .find(|level| level.level == 0)
            .map_or(0, |level| level.file_count);
        let l1_after = layout_after
            .levels
            .iter()
            .find(|level| level.level == 1)
            .map_or(0, |level| level.file_count);
        assert!(
            l0_after < l0_before,
            "fixture must compact L0 in mode {mode}"
        );
        assert!(
            l1_after > 0,
            "fixture must publish L1 output in mode {mode}"
        );
        let snapshot_value = snapshot.get(b"k").expect("snapshot get after compaction");
        assert_eq!(
            snapshot_value,
            Some(bytes::Bytes::from_static(b"v1")),
            "mode: {mode}"
        );

        drop(snapshot);
        wait_for_active_snapshots(&engine, 0, Duration::from_secs(1))
            .expect("wait for no active snapshots");

        let current = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin current read tx");
        assert_eq!(
            current.get(b"k").expect("current get"),
            None,
            "mode: {mode}"
        );
    });
}

#[test]
fn should_preserve_snapshot_range_scan_when_compaction_gc_runs_with_snapshot_active() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(&opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        let mut seed = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin seed tx");
        for i in 0..64 {
            let key = format!("gc_key_{i:03}");
            seed.put(key.into_bytes(), b"old".to_vec(), None)
                .expect("seed put");
        }
        seed.commit(buffered_write_options(mode))
            .expect("seed commit");
        engine.flush_cf(&cf).expect("seed flush");

        let snapshot = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin snapshot tx");
        wait_for_active_snapshots(&engine, 1, Duration::from_secs(1))
            .expect("wait for active snapshot");

        for generation in ["new1", "new2"] {
            let mut overwrite = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin overwrite tx");
            for i in 0..64 {
                let key = format!("gc_key_{i:03}");
                overwrite
                    .put(key.into_bytes(), generation.as_bytes().to_vec(), None)
                    .expect("overwrite put");
            }
            overwrite
                .commit(buffered_write_options(mode))
                .expect("overwrite commit");
            engine.flush_cf(&cf).expect("overwrite flush");
            engine.compact_all().expect("compact all");
        }

        // Act
        let rows = snapshot
            .scan(&Query::new())
            .expect("snapshot scan")
            .try_collect()
            .expect("collect snapshot scan");

        // Assert
        assert_eq!(rows.len(), 64, "mode: {mode}");
        for (_key, value) in rows {
            assert_eq!(value, bytes::Bytes::from_static(b"old"), "mode: {mode}");
        }

        let current = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin current read tx");
        assert_eq!(
            current.get(b"gc_key_000").expect("current get"),
            Some(bytes::Bytes::from_static(b"new2")),
            "mode: {mode}"
        );
        drop(current);

        drop(snapshot);
        wait_for_active_snapshots(&engine, 0, Duration::from_secs(1))
            .expect("wait for no active snapshots");
    });
}

#[test]
fn should_keep_snapshot_range_scan_stable_when_compaction_runs_concurrently() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(&opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        let mut seed = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin seed tx");
        for i in 0..32 {
            let key = format!("concurrent_key_{i:03}");
            seed.put(key.into_bytes(), b"baseline".to_vec(), None)
                .expect("seed put");
        }
        seed.commit(buffered_write_options(mode))
            .expect("seed commit");
        engine.flush_cf(&cf).expect("seed flush");

        let snapshot = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin snapshot tx");
        wait_for_active_snapshots(&engine, 1, Duration::from_secs(1))
            .expect("wait for active snapshot");

        // Act
        for round in 0..5 {
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin overwrite tx");
            for i in 0..32 {
                let key = format!("concurrent_key_{i:03}");
                let value = format!("round_{round}").into_bytes();
                tx.put(key.into_bytes(), value, None)
                    .expect("overwrite put");
            }
            tx.commit(buffered_write_options(mode))
                .expect("overwrite commit");
            engine.flush_cf(&cf).expect("overwrite flush");
            engine.compact_all().expect("compact all");

            let rows = snapshot
                .scan(&Query::new())
                .expect("snapshot scan after compaction round")
                .try_collect()
                .expect("collect snapshot scan after compaction round");
            assert_eq!(rows.len(), 32, "mode: {mode}");
            for (_key, value) in rows {
                assert_eq!(
                    value,
                    bytes::Bytes::from_static(b"baseline"),
                    "mode: {mode}"
                );
            }
        }

        // Assert
        let rows = snapshot
            .scan(&Query::new())
            .expect("snapshot scan after compaction")
            .try_collect()
            .expect("collect snapshot scan after compaction");
        assert_eq!(rows.len(), 32, "mode: {mode}");
        for (_key, value) in rows {
            assert_eq!(
                value,
                bytes::Bytes::from_static(b"baseline"),
                "mode: {mode}"
            );
        }

        drop(snapshot);
        wait_for_active_snapshots(&engine, 0, Duration::from_secs(1))
            .expect("wait for no active snapshots");
    });
}

#[test]
fn should_preserve_iterator_view_when_flush_and_compaction_run_during_active_scan() {
    // Arrange
    let opts = opts_for_mode("local");
    let db_path = match &opts.storage_mode {
        StorageMode::LocalDisk { db_path } => db_path.clone(),
        _ => unreachable!("local options must expose a local path"),
    };
    let engine = Arc::new(open_with_mode(&opts, "local"));
    let cf = engine.create_column_family("scan-pins").expect("create cf");
    let input_names = seed_scan_pin_generations(&engine, &cf);
    let read = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin pinned snapshot");
    let mut scan = read.scan(&Query::new()).expect("open snapshot scan");
    let first = scan
        .next()
        .transpose()
        .expect("read first row")
        .expect("first row");
    assert_eq!(first.0.as_ref(), b"row-000");

    // Act
    let writer_engine = Arc::clone(&engine);
    let writer_cf = cf.clone();
    let worker = std::thread::spawn(move || {
        let mut overwrite = writer_engine
            .begin_tx(writer_cf.id(), TransactionMode::ReadWrite)
            .expect("begin concurrent overwrite");
        for index in 0..24 {
            overwrite
                .put(
                    format!("row-{index:03}").into_bytes(),
                    b"updated".to_vec(),
                    None,
                )
                .expect("overwrite row");
        }
        overwrite
            .commit(WriteOptions::sync())
            .expect("commit overwrite");
        writer_engine
            .flush_cf(&writer_cf)
            .expect("flush concurrent overwrite");
        writer_engine
            .compact_all()
            .expect("compact while scan is open");
    });
    worker.join().expect("join concurrent compaction");
    let remaining = scan.try_collect().expect("finish pinned scan");

    // Assert
    assert_eq!(remaining.len(), 26);
    assert!(
        remaining
            .iter()
            .all(|(_key, value)| value.as_ref() == b"baseline"),
        "the already-open iterator must retain its frozen values"
    );
    let after = engine
        .get_storage_layout()
        .expect("layout after compaction");
    assert!(
        after
            .levels
            .iter()
            .any(|level| level.level == 1 && level.file_count > 0),
        "manual compaction must publish a deeper-level output"
    );
    let live_names: std::collections::HashSet<_> = after
        .levels
        .iter()
        .flat_map(|level| level.files.iter())
        .map(|file| file.name.as_str())
        .collect();
    let retired_input_names: Vec<_> = input_names
        .iter()
        .filter(|name| !live_names.contains(name.as_str()))
        .cloned()
        .collect();
    assert!(
        !retired_input_names.is_empty(),
        "real compaction must retire at least one pinned input"
    );
    assert!(
        retired_input_names
            .iter()
            .all(|name| db_path.join("sst").join(name.as_str()).exists()),
        "snapshot-pinned compaction inputs must remain on disk until the scan closes"
    );
    drop(read);
    wait_for_sst_files_removed(&db_path, &retired_input_names, Duration::from_secs(2));
}

#[test]
fn should_shadow_neither_write_when_delete_range_tombstone_precedes_newer_put_across_sst_generations(
) {
    // Arrange
    let opts = opts_for_mode("local");
    let engine = open_with_mode(&opts, "local");
    let cf = engine
        .create_column_family("range-generations")
        .expect("create cf");
    let mut initial = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin initial write");
    initial
        .put(b"middle".to_vec(), b"old".to_vec(), None)
        .expect("put old value");
    initial
        .commit(WriteOptions::sync())
        .expect("commit old value");
    engine.flush_cf(&cf).expect("flush old value");

    let mut delete = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin range delete");
    delete
        .delete_range(b"a".to_vec(), b"z".to_vec())
        .expect("delete range");
    delete
        .commit(WriteOptions::sync())
        .expect("commit range delete");
    engine.flush_cf(&cf).expect("flush range tombstone");

    let mut newer = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin newer write");
    newer
        .put(b"middle".to_vec(), b"new".to_vec(), None)
        .expect("put newer value");
    newer
        .commit(WriteOptions::sync())
        .expect("commit newer value");
    engine.flush_cf(&cf).expect("flush newer value");

    let mut filler = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin filler write");
    filler
        .put(b"zz".to_vec(), b"filler".to_vec(), None)
        .expect("put filler");
    filler.commit(WriteOptions::sync()).expect("commit filler");
    engine.flush_cf(&cf).expect("flush filler");
    let before = engine
        .get_storage_layout()
        .expect("layout before generation compaction");
    assert_eq!(
        before
            .levels
            .iter()
            .find(|level| level.level == 0)
            .map_or(0, |level| level.file_count),
        4,
        "fixture must publish four L0 generations"
    );

    // Act
    engine.compact_all().expect("compact generations");
    let read = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin current read");
    let value = read.get(b"middle").expect("read middle");
    let rows = read
        .scan(&Query::new())
        .expect("scan current view")
        .try_collect()
        .expect("collect current view");
    let reverse_rows = read
        .scan(&Query::new().reverse())
        .expect("reverse-scan current view")
        .try_collect()
        .expect("collect reverse current view");

    // Assert
    let after = engine
        .get_storage_layout()
        .expect("layout after generation compaction");
    assert!(
        after
            .levels
            .iter()
            .any(|level| level.level == 1 && level.file_count > 0),
        "the range tombstone and newer point must pass through real compaction"
    );
    assert_eq!(value.as_deref(), Some(&b"new"[..]));
    assert!(rows
        .iter()
        .any(|(key, value)| { key.as_ref() == b"middle" && value.as_ref() == b"new" }));
    assert!(reverse_rows
        .iter()
        .any(|(key, value)| { key.as_ref() == b"middle" && value.as_ref() == b"new" }));
}

#[test]
fn should_return_stable_results_when_scanning_after_column_family_dropped_mid_transaction() {
    // Arrange
    let opts = opts_for_mode("local");
    let engine = Arc::new(open_with_mode(&opts, "local"));
    let cf = engine.create_column_family("drop-scan").expect("create cf");
    let mut write = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin seed write");
    for key in [b"a", b"b", b"c"] {
        write
            .put(key.to_vec(), b"value".to_vec(), None)
            .expect("put row");
    }
    write.commit(WriteOptions::sync()).expect("commit rows");
    engine.flush_cf(&cf).expect("flush rows");
    let read = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin snapshot before drop");
    let mut scan = read.scan(&Query::new()).expect("open scan before drop");
    let first = scan
        .next()
        .transpose()
        .expect("read first row")
        .expect("first row");

    // Act
    let drop_engine = Arc::clone(&engine);
    let cf_id = cf.id();
    std::thread::spawn(move || drop_engine.drop_column_family(cf_id))
        .join()
        .expect("join concurrent drop")
        .expect("drop column family with pinned snapshot");
    let remaining = scan.try_collect().expect("finish scan after drop");

    // Assert
    assert_eq!(first.0.as_ref(), b"a");
    assert_eq!(remaining.len(), 2);
    assert_eq!(remaining[0].0.as_ref(), b"b");
    assert_eq!(remaining[1].0.as_ref(), b"c");
    assert!(matches!(
        engine.begin_tx(cf.id(), TransactionMode::ReadOnly),
        Err(MidgeError::InvalidArgument(_))
    ));
    drop(read);
}

#[test]
fn should_return_busy_when_scan_iterator_is_still_open_across_shutdown_call() {
    // Arrange
    let opts = opts_for_mode("local");
    let mut engine = open_with_mode(&opts, "local");
    let cf = engine
        .create_column_family("shutdown-scan")
        .expect("create cf");
    let read = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin snapshot");
    let scan = read.scan(&Query::new()).expect("open scan");

    // Act
    let result = engine.shutdown(Duration::from_millis(100));

    // Assert
    assert!(matches!(result, Err(MidgeError::Busy(_))));
    drop(scan);
    drop(read);
    engine
        .shutdown(Duration::from_secs(2))
        .expect("shutdown after scan closes");
}
