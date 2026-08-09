//! Operation-integrity checks for code paths that are expected to emit
//! telemetry when instrumentation is available.
//!
//! These tests do not claim to verify metric counters because the integration
//! suite does not currently consume a real telemetry API here.

use bytes::Bytes;
mod common;
use cntryl_midge::TransactionMode;
use common::*;
use std::thread;
use std::time::Duration;

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

        // Act
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin read transaction");

        // Assert
        for i in 0..50 {
            let key = format!("metrics_read_key_{i:04}");
            assert_eq!(
                tx.get(key.as_bytes()).expect("read repeated-read key"),
                Some(Bytes::from_static(b"metric_value")),
                "mode: {mode} key: {key}"
            );
        }
    });
}

#[test]
fn should_preserve_all_written_values_given_large_write_batch_when_written() {
    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange
        let engine = open_with_mode(&opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        let value = b"metric_write_value";

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin write batch transaction");
        for i in 0..100 {
            let key = format!("metrics_write_key_{i:04}");
            tx.put(key.as_bytes().to_vec(), value.to_vec(), None)
                .expect("put write-batch value");
        }
        tx.commit(buffered_write_options(mode)).expect("commit");

        // Assert
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin verification transaction");
        for i in 0..100 {
            let key = format!("metrics_write_key_{i:04}");
            assert_eq!(
                tx.get(key.as_bytes()).expect("read write-batch key"),
                Some(Bytes::from_static(value)),
                "mode: {mode} key: {key}"
            );
        }
    });
}

#[test]
fn should_preserve_all_values_given_compaction_when_requested() {
    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange
        let engine = open_with_mode(&opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin first batch transaction");
        for i in 0..100 {
            let key = format!("compact_metric_key_{i:04}");
            tx.put(key.as_bytes().to_vec(), b"gen1".to_vec(), None)
                .expect("put first compaction batch value");
        }
        tx.commit(buffered_write_options(mode)).expect("commit");
        engine.flush_cf(&cf).expect("flush first batch");

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin second batch transaction");
        for i in 100..200 {
            let key = format!("compact_metric_key_{i:04}");
            tx.put(key.as_bytes().to_vec(), b"gen2".to_vec(), None)
                .expect("put second compaction batch value");
        }
        tx.commit(buffered_write_options(mode)).expect("commit");
        engine.flush_cf(&cf).expect("flush second batch");

        // Act
        engine.compact_all().ok();

        // Assert
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin verification transaction");
        for i in 0..200 {
            let key = format!("compact_metric_key_{i:04}");
            let expected = if i < 100 {
                Bytes::from_static(b"gen1")
            } else {
                Bytes::from_static(b"gen2")
            };
            assert_eq!(
                tx.get(key.as_bytes()).expect("read compacted key"),
                Some(expected),
                "mode: {mode} key: {key}"
            );
        }
    });
}

#[test]
fn should_preserve_repeated_reads_given_short_cache_warmup_window_when_reads_repeated() {
    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange
        let engine = open_with_mode(&opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin cache seed transaction");
        for i in 0..100 {
            let key = format!("cache_metric_key_{i:04}");
            tx.put(key.as_bytes().to_vec(), b"cached_value".to_vec(), None)
                .expect("put cache seed value");
        }
        tx.commit(buffered_write_options(mode)).expect("commit");
        engine.flush_cf(&cf).expect("flush");

        // Act
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin first read transaction");
        for i in 0..50 {
            let key = format!("cache_metric_key_{i:04}");
            assert_eq!(
                tx.get(key.as_bytes()).expect("read first-pass cache key"),
                Some(Bytes::from_static(b"cached_value"))
            );
        }

        drop(tx);
        thread::sleep(Duration::from_millis(50));

        // Assert
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin second read transaction");
        for i in 0..50 {
            let key = format!("cache_metric_key_{i:04}");
            assert_eq!(
                tx.get(key.as_bytes()).expect("read second-pass cache key"),
                Some(Bytes::from_static(b"cached_value")),
                "mode: {mode} key: {key}"
            );
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

        // Assert
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

#[test]
fn should_preserve_existing_data_given_placeholder_reset_when_new_write_added() {
    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange
        let engine = open_with_mode(&opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin initial seed transaction");
        for i in 0..50 {
            let key = format!("reset_metric_key_{i:04}");
            tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                .expect("put initial reset key");
        }
        tx.commit(buffered_write_options(mode)).expect("commit");

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin follow-up transaction");
        tx.put(b"single_key".to_vec(), b"single_value".to_vec(), None)
            .expect("put follow-up key");
        tx.commit(buffered_write_options(mode)).expect("commit");

        // Assert
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin verification transaction");
        for i in 0..50 {
            let key = format!("reset_metric_key_{i:04}");
            assert_eq!(
                tx.get(key.as_bytes()).expect("read preserved reset key"),
                Some(Bytes::from_static(b"value")),
                "mode: {mode} key: {key}"
            );
        }
        assert_eq!(
            tx.get(b"single_key").expect("read follow-up key"),
            Some(Bytes::from_static(b"single_value"))
        );
    });
}
