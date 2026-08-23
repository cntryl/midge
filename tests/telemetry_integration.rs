//! Operation-integrity checks for code paths that are expected to emit
//! telemetry when instrumentation is available, plus direct assertions
//! against the real runtime-metrics/telemetry API (`Engine::get_runtime_metrics`,
//! `Engine::flush_cf`'s flush counters, `Engine::compact_all`'s compaction
//! counters) where such an API exists.

use bytes::Bytes;
mod common;
use cntryl_midge::TransactionMode;
use common::*;

#[test]
fn should_initialize_global_telemetry_once_given_repeated_init_calls_when_starting() {
    // Arrange
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
    let callers = (0..2)
        .map(|_| {
            let barrier = std::sync::Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                cntryl_midge::init_benchmark_telemetry()
            })
        })
        .collect::<Vec<_>>();

    // Act
    barrier.wait();
    let concurrent_results = callers
        .into_iter()
        .map(|caller| caller.join().expect("telemetry caller must not panic"))
        .collect::<Vec<_>>();
    let later = cntryl_midge::init_benchmark_telemetry();

    // Assert
    for result in concurrent_results {
        result.expect("concurrent public telemetry initialization should be idempotent");
    }
    later.expect("later public telemetry initialization should replay success");
}

#[test]
fn should_preserve_all_values_given_repeated_reads_when_values_accessed_repeatedly() {
    cntryl_midge::init_benchmark_telemetry().expect("enable test-visible cache metrics");

    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange
        let engine = open_with_mode(&opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin write transaction");
        for i in 0..50 {
            let key = format!("metrics_read_key_{i:04}");
            tx.put(key.as_bytes().to_vec(), b"metric_value".to_vec(), None)
                .expect("put read-path value");
        }
        tx.commit(buffered_write_options(mode)).expect("commit");
        engine.flush_cf(&cf).expect("flush");

        // Act: first read pass is expected to miss the block cache (data was
        // just flushed to disk), the repeated second pass should hit it.
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin first-pass read transaction");
        for i in 0..50 {
            let key = format!("metrics_read_key_{i:04}");
            assert_eq!(
                tx.get(key.as_bytes()).expect("read repeated-read key"),
                Some(Bytes::from_static(b"metric_value")),
                "mode: {mode} key: {key}"
            );
        }
        drop(tx);

        let before = engine.get_runtime_metrics().expect("runtime metrics");

        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin repeated-read transaction");
        for i in 0..50 {
            let key = format!("metrics_read_key_{i:04}");
            assert_eq!(
                tx.get(key.as_bytes()).expect("read repeated-read key"),
                Some(Bytes::from_static(b"metric_value")),
                "mode: {mode} key: {key}"
            );
        }

        // Assert: repeatedly accessing the same values must register block
        // cache hits in the telemetry runtime metrics.
        let after = engine.get_runtime_metrics().expect("runtime metrics");
        assert!(
            after.cache_hits > before.cache_hits,
            "mode: {mode} repeated reads should register block cache hits (before: {}, after: {})",
            before.cache_hits,
            after.cache_hits
        );
    });
}

#[test]
fn should_preserve_all_values_given_compaction_when_requested() {
    cntryl_midge::init_benchmark_telemetry().expect("enable test-visible compaction metrics");

    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange
        let engine = open_with_mode(&opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Flush enough separate L0 files (default l0_file_count_threshold is
        // 4) that compact_all() actually has work to schedule instead of
        // observing only two files and deciding no compaction is needed.
        let batches = 5;
        let keys_per_batch = 50;
        for batch in 0..batches {
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin batch transaction");
            for i in 0..keys_per_batch {
                let key = format!("compact_metric_key_{:04}", batch * keys_per_batch + i);
                let value = format!("gen{batch}").into_bytes();
                tx.put(key.as_bytes().to_vec(), value, None)
                    .expect("put compaction batch value");
            }
            tx.commit(buffered_write_options(mode)).expect("commit");
            engine.flush_cf(&cf).expect("flush batch");
        }

        let before = engine.get_runtime_metrics().expect("runtime metrics");

        // Act
        engine.compact_all().ok();

        // Assert: compaction must be recorded in the telemetry runtime
        // metrics, not just leave the data intact.
        let after = engine.get_runtime_metrics().expect("runtime metrics");
        assert!(
            after.compactions_run > before.compactions_run,
            "mode: {mode} compact_all should increment compactions_run (before: {}, after: {})",
            before.compactions_run,
            after.compactions_run
        );

        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin verification transaction");
        for batch in 0..batches {
            for i in 0..keys_per_batch {
                let key = format!("compact_metric_key_{:04}", batch * keys_per_batch + i);
                let expected = Bytes::from(format!("gen{batch}").into_bytes());
                assert_eq!(
                    tx.get(key.as_bytes()).expect("read compacted key"),
                    Some(expected),
                    "mode: {mode} key: {key}"
                );
            }
        }
    });
}

#[test]
fn should_preserve_large_values_given_wal_backed_write_batch_when_flushed() {
    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange
        let engine = open_with_mode(&opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        let value = vec![b'W'; 1024];

        let before = engine.get_runtime_metrics().expect("runtime metrics");

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin wal-sized write transaction");
        for i in 0..100 {
            let key = format!("wal_metric_key_{i:04}");
            tx.put(key.as_bytes().to_vec(), value.clone(), None)
                .expect("put wal-sized value");
        }
        tx.commit(buffered_write_options(mode)).expect("commit");
        engine.flush_cf(&cf).expect("flush");

        // Assert: flushing the WAL-backed write batch to an SST must be
        // recorded by the flush build/publish runtime metrics.
        let after = engine.get_runtime_metrics().expect("runtime metrics");
        assert!(
            after.flush_build_count > before.flush_build_count,
            "mode: {mode} flush_cf should increment flush_build_count (before: {}, after: {})",
            before.flush_build_count,
            after.flush_build_count
        );
        assert!(
            after.flush_publish_count > before.flush_publish_count,
            "mode: {mode} flush_cf should increment flush_publish_count (before: {}, after: {})",
            before.flush_publish_count,
            after.flush_publish_count
        );

        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin verification transaction");
        assert_eq!(
            tx.get(b"wal_metric_key_0000").expect("read first wal key"),
            Some(Bytes::from(value.clone()))
        );
        assert_eq!(
            tx.get(b"wal_metric_key_0099").expect("read last wal key"),
            Some(Bytes::from(value))
        );
    });
}
