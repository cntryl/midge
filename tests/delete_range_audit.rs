//! Delete-range correctness checks.
//!
//! These tests verify actual delete-range and scan behavior rather than
//! printing manual audit output.

use bytes::Bytes;
mod common;
use cntryl_midge::Query;
use common::*;

#[test]
fn should_delete_only_keys_within_requested_range_when_delete_range_commits() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(&opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin seed transaction");
        for i in 1..=10 {
            let key = format!("key{i:02}");
            let value = format!("val{i:02}");
            tx.put(key.into_bytes(), value.into_bytes(), None)
                .expect("seed key");
        }
        tx.commit(buffered_write_options(mode))
            .expect("commit seed transaction");

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin delete_range transaction");
        tx.delete_range(b"key02".to_vec(), b"key08".to_vec())
            .expect("delete range");
        tx.commit(buffered_write_options(mode))
            .expect("commit delete range transaction");

        // Assert
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read transaction");
        for i in 1..=10 {
            let key = format!("key{i:02}");
            let value = tx.get(key.as_bytes()).expect("read key after delete_range");
            if (2..8).contains(&i) {
                assert_eq!(value, None, "mode: {mode} key: {key}");
            } else {
                let expected = format!("val{i:02}");
                assert_eq!(
                    value,
                    Some(Bytes::copy_from_slice(expected.as_bytes())),
                    "mode: {mode} key: {key}"
                );
            }
        }
    });
}

#[test]
fn should_return_all_inserted_keys_when_scanning_unbounded_query() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(&opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin seed transaction");
        for key in [b"a", b"b", b"c", b"d"] {
            tx.put(
                key.to_vec(),
                [b'v', b'a', b'l', b'_', key[0]].to_vec(),
                None,
            )
            .expect("seed scan key");
        }
        tx.commit(buffered_write_options(mode))
            .expect("commit seed transaction");

        // Act
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin scan transaction");
        let results = tx
            .scan(&Query::new())
            .expect("scan all keys")
            .try_collect()
            .expect("collect all keys");

        // Assert
        assert_eq!(results.len(), 4, "mode: {mode}");
        assert_eq!(results[0].0, Bytes::from_static(b"a"));
        assert_eq!(results[1].0, Bytes::from_static(b"b"));
        assert_eq!(results[2].0, Bytes::from_static(b"c"));
        assert_eq!(results[3].0, Bytes::from_static(b"d"));
    });
}
