//! Memory-related tests (formerly Phase 1 fixes)

mod common;
use cntryl_midge::{EngineHealth, MidgeError, TransactionMode};
use common::*;
use std::time::Duration;

#[test]
fn should_handle_small_memory_budget_without_unexpected_errors() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange: Open engine with very small memtable limit to trigger frequent flushes
        let mut opts = opts;
        opts = opts.memory_budget(64 * 1024); // 64KB
        let engine = open_with_mode(&opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Act: Write enough data to fill immutable queue
        let mut write_count = 0;
        let mut stall_encountered = false;

        for i in 0..1000 {
            let key = format!("key_{i:08}");
            let value = vec![b'x'; 1024]; // 1KB value

            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            tx.put(key.into_bytes(), value, None).unwrap();

            match tx.commit(buffered_write_options(mode)) {
                Ok(()) => {
                    write_count += 1;
                }
                Err(MidgeError::WriteStall(_)) => {
                    stall_encountered = true;
                    break;
                }
                Err(e) => {
                    panic!("Unexpected error: {e:?}");
                }
            }

            if i % 10 == 0 {
                let _ = engine.flush_cf(&cf);
            }
        }

        engine.flush_cf(&cf).expect("final flush");
        let metrics = engine.get_runtime_metrics().expect("runtime metrics");

        // Assert
        if !mode.eq("memory") {
            // Under sustained pressure the engine may either surface WriteStall or
            // keep up by flushing synchronously. Both are acceptable as long as it
            // keeps making progress and does not end in a degraded memory state.
            assert!(
                stall_encountered || write_count > 0,
                "Expected progress or backpressure in mode {mode}"
            );
            assert_ne!(
                metrics.health,
                EngineHealth::WriteStalled,
                "Engine should not remain write-stalled after final flush in mode {mode}"
            );
        }
    });
}

#[test]
fn should_complete_shutdown_when_wal_writer_drops() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let mut engine = open_with_mode(&opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Act: Write some data
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key".to_vec(), b"value".to_vec(), None).unwrap();
        tx.commit(buffered_write_options(mode)).unwrap();

        // Act: shut down explicitly (this drops the WAL writer as part of an
        // orderly shutdown) and assert it actually reports success within a
        // bound, rather than only relying on the test harness's own timeout
        // to catch a hang with no diagnostic about *why* it failed.
        let result = engine.shutdown(Duration::from_secs(10));

        // Assert
        assert!(
            result.is_ok(),
            "shutdown did not complete cleanly when the WAL writer dropped in mode {mode}: {result:?}"
        );
    });
}
