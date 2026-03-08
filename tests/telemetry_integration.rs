//! Operation-integrity checks for code paths that are expected to emit
//! telemetry when instrumentation is available.
//!
//! These tests do not claim to verify metric counters because the integration
//! suite does not currently consume a real telemetry API here.

use bytes::Bytes;
use cntryl_midge::testkit::*;
use cntryl_midge::{TransactionMode, WriteOptions};
use std::thread;
use std::time::Duration;

#[test]
fn should_preserve_all_values_after_exercising_repeated_read_path() {
    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts.clone(), mode);
        let cf = engine.create_column_family("test").expect("create cf");

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin write transaction");
        for i in 0..50 {
            let key = format!("metrics_read_key_{:04}", i);
            tx.put(key.as_bytes().to_vec(), b"metric_value".to_vec(), None)
                .expect("put read-path value");
        }
        engine.commit(tx, WriteOptions::buffered()).expect("commit");
        engine.flush_cf(&cf).expect("flush");

        // Act
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin read transaction");

        // Assert
        for i in 0..50 {
            let key = format!("metrics_read_key_{:04}", i);
            assert_eq!(
                tx.get(key.as_bytes()).expect("read repeated-read key"),
                Some(Bytes::from_static(b"metric_value")),
                "mode: {} key: {}",
                mode,
                key
            );
        }
    });
}

#[test]
fn should_preserve_all_written_values_after_large_write_batch() {
    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts.clone(), mode);
        let cf = engine.create_column_family("test").expect("create cf");

        let value = b"metric_write_value";

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin write batch transaction");
        for i in 0..100 {
            let key = format!("metrics_write_key_{:04}", i);
            tx.put(key.as_bytes().to_vec(), value.to_vec(), None)
                .expect("put write-batch value");
        }
        engine.commit(tx, WriteOptions::buffered()).expect("commit");

        // Assert
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin verification transaction");
        for i in 0..100 {
            let key = format!("metrics_write_key_{:04}", i);
            assert_eq!(
                tx.get(key.as_bytes()).expect("read write-batch key"),
                Some(Bytes::from_static(value)),
                "mode: {} key: {}",
                mode,
                key
            );
        }
    });
}

#[test]
fn should_preserve_all_values_after_compaction_request() {
    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts.clone(), mode);
        let cf = engine.create_column_family("test").expect("create cf");

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin first batch transaction");
        for i in 0..100 {
            let key = format!("compact_metric_key_{:04}", i);
            tx.put(key.as_bytes().to_vec(), b"gen1".to_vec(), None)
                .expect("put first compaction batch value");
        }
        engine.commit(tx, WriteOptions::buffered()).expect("commit");
        engine.flush_cf(&cf).expect("flush first batch");

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin second batch transaction");
        for i in 100..200 {
            let key = format!("compact_metric_key_{:04}", i);
            tx.put(key.as_bytes().to_vec(), b"gen2".to_vec(), None)
                .expect("put second compaction batch value");
        }
        engine.commit(tx, WriteOptions::buffered()).expect("commit");
        engine.flush_cf(&cf).expect("flush second batch");

        // Act
        engine.compact_all().ok();

        // Assert
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin verification transaction");
        for i in 0..200 {
            let key = format!("compact_metric_key_{:04}", i);
            let expected = if i < 100 {
                Bytes::from_static(b"gen1")
            } else {
                Bytes::from_static(b"gen2")
            };
            assert_eq!(
                tx.get(key.as_bytes()).expect("read compacted key"),
                Some(expected),
                "mode: {} key: {}",
                mode,
                key
            );
        }
    });
}

#[test]
fn should_preserve_repeated_reads_across_short_cache_warmup_window() {
    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts.clone(), mode);
        let cf = engine.create_column_family("test").expect("create cf");

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin cache seed transaction");
        for i in 0..100 {
            let key = format!("cache_metric_key_{:04}", i);
            tx.put(key.as_bytes().to_vec(), b"cached_value".to_vec(), None)
                .expect("put cache seed value");
        }
        engine.commit(tx, WriteOptions::buffered()).expect("commit");
        engine.flush_cf(&cf).expect("flush");

        // Act
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin first read transaction");
        for i in 0..50 {
            let key = format!("cache_metric_key_{:04}", i);
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
            let key = format!("cache_metric_key_{:04}", i);
            assert_eq!(
                tx.get(key.as_bytes()).expect("read second-pass cache key"),
                Some(Bytes::from_static(b"cached_value")),
                "mode: {} key: {}",
                mode,
                key
            );
        }
    });
}

#[test]
fn should_preserve_large_values_after_flushing_wal_backed_write_batch() {
    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts.clone(), mode);
        let cf = engine.create_column_family("test").expect("create cf");

        let value = vec![b'W'; 1024];

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin wal-sized write transaction");
        for i in 0..100 {
            let key = format!("wal_metric_key_{:04}", i);
            tx.put(key.as_bytes().to_vec(), value.clone(), None)
                .expect("put wal-sized value");
        }
        engine.commit(tx, WriteOptions::buffered()).expect("commit");
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
fn should_preserve_existing_data_while_adding_new_write_after_placeholder_reset_step() {
    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts.clone(), mode);
        let cf = engine.create_column_family("test").expect("create cf");

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin initial seed transaction");
        for i in 0..50 {
            let key = format!("reset_metric_key_{:04}", i);
            tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                .expect("put initial reset key");
        }
        engine.commit(tx, WriteOptions::buffered()).expect("commit");

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin follow-up transaction");
        tx.put(b"single_key".to_vec(), b"single_value".to_vec(), None)
            .expect("put follow-up key");
        engine.commit(tx, WriteOptions::buffered()).expect("commit");

        // Assert
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin verification transaction");
        for i in 0..50 {
            let key = format!("reset_metric_key_{:04}", i);
            assert_eq!(
                tx.get(key.as_bytes()).expect("read preserved reset key"),
                Some(Bytes::from_static(b"value")),
                "mode: {} key: {}",
                mode,
                key
            );
        }
        assert_eq!(
            tx.get(b"single_key").expect("read follow-up key"),
            Some(Bytes::from_static(b"single_value"))
        );
    });
}
