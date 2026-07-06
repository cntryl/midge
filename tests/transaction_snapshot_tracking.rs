mod common;

use cntryl_midge::{MidgeError, MidgeResult, Query, TransactionMode, WriteOptions};
use common::*;
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
        seed.commit(WriteOptions::buffered()).expect("seed commit");
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
        seed.commit(WriteOptions::buffered()).expect("seed commit");
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
        tx.commit(WriteOptions::buffered()).expect("commit tx");

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
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(&opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        let mut seed = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin seed tx");
        seed.put(b"k".to_vec(), b"v1".to_vec(), None)
            .expect("seed put");
        seed.commit(WriteOptions::buffered()).expect("seed commit");
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
            .commit(WriteOptions::buffered())
            .expect("delete commit");
        engine.flush_cf(&cf).expect("delete flush");

        // Act
        engine.compact_all().expect("compact all");

        // Assert
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
        seed.commit(WriteOptions::buffered()).expect("seed commit");
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
                .commit(WriteOptions::buffered())
                .expect("overwrite commit");
            engine.flush_cf(&cf).expect("overwrite flush");
            engine.compact_all().expect("compact all");
        }

        // Act
        let mut iter = snapshot.scan(&Query::new()).expect("snapshot scan");
        let rows: Vec<_> = std::iter::from_fn(|| iter.next()).collect();

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
        seed.commit(WriteOptions::buffered()).expect("seed commit");
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
            tx.commit(WriteOptions::buffered())
                .expect("overwrite commit");
            engine.flush_cf(&cf).expect("overwrite flush");
            engine.compact_all().expect("compact all");

            let mut iter = snapshot
                .scan(&Query::new())
                .expect("snapshot scan after compaction round");
            let rows: Vec<_> = std::iter::from_fn(|| iter.next()).collect();
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
        let mut iter = snapshot
            .scan(&Query::new())
            .expect("snapshot scan after compaction");
        let rows: Vec<_> = std::iter::from_fn(|| iter.next()).collect();
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
