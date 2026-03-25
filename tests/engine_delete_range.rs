//! Delete Range Integration Tests
//!
//! Tests range deletion operations end-to-end using the public MidgeEngine API.

use bytes::Bytes;
mod common;
use cntryl_midge::{TransactionMode, WriteOptions};
use common::*;
use std::sync::Arc;

#[test]
fn should_delete_keys_in_range_given_delete_range_when_querying() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        for (key, value) in [
            ("key1", "val1"),
            ("key2", "val2"),
            ("key3", "val3"),
            ("key4", "val4"),
        ] {
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                .expect("seed put");
            tx.commit(WriteOptions::buffered()).unwrap();
        }

        // Act
        let mut delete_tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        delete_tx
            .delete_range(b"key2".to_vec(), b"key4".to_vec())
            .expect("delete_range");
        delete_tx
            .commit(WriteOptions::buffered())
            .expect("commit delete_range");

        // Assert
        let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        assert_eq!(
            tx.get(b"key1").expect("get1"),
            Some(Bytes::from_static(b"val1")),
            "key1 should exist (outside range) in mode: {}",
            mode
        );
        assert_eq!(
            tx.get(b"key2").expect("get2"),
            None,
            "key2 should be deleted (in range) in mode: {}",
            mode
        );
        assert_eq!(
            tx.get(b"key3").expect("get3"),
            None,
            "key3 should be deleted (in range) in mode: {}",
            mode
        );
        assert_eq!(
            tx.get(b"key4").expect("get4"),
            Some(Bytes::from_static(b"val4")),
            "key4 should exist (outside range) in mode: {}",
            mode
        );
    });
}

#[test]
fn should_handle_empty_range_given_start_equals_end_when_delete_range() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key".to_vec(), b"val".to_vec(), None).expect("put");
        tx.commit(WriteOptions::buffered()).unwrap();

        // Act
        let mut delete_tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        delete_tx
            .delete_range(b"key".to_vec(), b"key".to_vec())
            .expect("delete_range");
        delete_tx
            .commit(WriteOptions::buffered())
            .expect("commit delete_range");

        // Assert
        let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        assert_eq!(
            tx.get(b"key").expect("get"),
            Some(Bytes::from_static(b"val")),
            "key should exist (empty range) in mode: {}",
            mode
        );
    });
}

#[test]
fn should_reject_delete_range_given_reversed_bounds_when_called() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut delete_tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        let result = delete_tx.delete_range(b"key9".to_vec(), b"key1".to_vec());

        // Assert
        assert!(
            matches!(result, Err(cntryl_midge::MidgeError::InvalidArgument(_))),
            "mode: {}",
            mode
        );
    });
}

#[test]
fn should_delete_key_given_delete_range_with_single_key_when_matching() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"target".to_vec(), b"value".to_vec(), None)
            .expect("put");
        tx.commit(WriteOptions::buffered()).unwrap();

        // Act
        let mut delete_tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        delete_tx
            .delete_range(b"target".to_vec(), b"targetZ".to_vec())
            .expect("delete_range");
        delete_tx
            .commit(WriteOptions::buffered())
            .expect("commit delete_range");

        // Assert
        let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        assert_eq!(
            tx.get(b"target").expect("get"),
            None,
            "target should be deleted in mode: {}",
            mode
        );
    });
}

#[test]
fn should_allow_multiple_delete_ranges_when_called_sequentially() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        for i in 0..20 {
            let key = format!("k{:02}", i);
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                .expect("put");
            tx.commit(WriteOptions::buffered()).unwrap();
        }

        // Act
        for (start, end, label) in [
            (b"k03", b"k10", "delete_range1"),
            (b"k15", b"k18", "delete_range2"),
        ] {
            let mut delete_tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            delete_tx
                .delete_range(start.to_vec(), end.to_vec())
                .expect(label);
            delete_tx.commit(WriteOptions::buffered()).expect(label);
        }

        // Assert
        let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        for i in 0..20 {
            let key = format!("k{:02}", i);
            let should_exist = !((3..10).contains(&i) || (15..18).contains(&i));
            let result = tx.get(key.as_bytes()).expect("get");
            assert_eq!(
                result.is_some(),
                should_exist,
                "key {} should {} in mode: {}",
                i,
                if should_exist { "exist" } else { "be deleted" },
                mode
            );
        }
    });
}

#[test]
fn should_persist_keys_across_delete_range_with_restart_when_durable() {
    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            for (key, value) in [("key1", "val1"), ("key2", "val2"), ("key3", "val3")] {
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .unwrap();
                tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                    .expect("put seed");
                tx.commit(WriteOptions::buffered()).unwrap();
            }

            let mut delete_tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            delete_tx
                .delete_range(b"key1".to_vec(), b"key3".to_vec())
                .expect("delete_range");
            delete_tx
                .commit(WriteOptions::buffered())
                .expect("commit delete_range");
        }

        // Act
        // Assert
        let engine = open_with_mode(opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");
        let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        assert!(tx.get(b"key1").expect("get1").is_none());
        assert!(tx.get(b"key2").expect("get2").is_none());
        assert_eq!(
            tx.get(b"key3").expect("get3"),
            Some(Bytes::from_static(b"val3")),
            "key3 should persist after restart"
        );
    });
}

#[test]
fn should_handle_concurrent_delete_ranges_when_multiple_threads() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");

        for i in 0..100 {
            let key = format!("key{:03}", i);
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                .expect("put");
            tx.commit(WriteOptions::buffered()).unwrap();
        }

        // Act
        let mut handles = vec![];
        for thread_id in 0..5 {
            let engine_clone = Arc::clone(&engine);
            let cf_clone = cf.clone();
            handles.push(std::thread::spawn(move || {
                let start = format!("key{:03}", thread_id * 10);
                let end = format!("key{:03}", (thread_id + 1) * 10);
                let mut delete_tx = engine_clone
                    .begin_tx(cf_clone.id(), TransactionMode::ReadWrite)
                    .unwrap();
                delete_tx
                    .delete_range(start.as_bytes().to_vec(), end.as_bytes().to_vec())
                    .expect("delete_range");
                delete_tx
                    .commit(WriteOptions::buffered())
                    .expect("commit delete_range");
            }));
        }

        for handle in handles {
            handle.join().expect("thread");
        }

        // Assert
        let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        for i in 0..100 {
            let key = format!("key{:03}", i);
            let should_exist = !(0..50).contains(&i);
            let got = tx.get(key.as_bytes()).expect("get");
            assert_eq!(
                got.is_some(),
                should_exist,
                "key {} should {} in mode: {}",
                i,
                if should_exist { "exist" } else { "be deleted" },
                mode
            );
        }
    });
}
