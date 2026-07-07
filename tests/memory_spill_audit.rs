//! Large-transaction behavior under low configured memory budgets.
//!
//! These tests verify externally visible behavior: large transactions commit
//! successfully and their data remains readable. They do not claim direct
//! visibility into whether spill-to-disk occurred internally.

use bytes::Bytes;
mod common;
use cntryl_midge::TransactionMode;
use common::*;

#[test]
fn should_commit_large_transaction_given_memory_budget_exceeded_when_committed() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(&opts.memory_budget(128 * 1024), mode);
        let cf = engine.create_column_family("test").expect("create cf");

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin large transaction");
        for i in 0..500 {
            let key = format!("large_key_{i:05}");
            tx.put(key.as_bytes().to_vec(), vec![65u8; 1024], None)
                .expect("put large value");
        }

        // Act
        tx.commit(buffered_write_options(mode))
            .expect("commit large transaction");

        // Assert
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin verification transaction");
        assert_eq!(
            tx.get(b"large_key_00000").expect("read first key"),
            Some(Bytes::from(vec![65u8; 1024]))
        );
        assert_eq!(
            tx.get(b"large_key_00499").expect("read last key"),
            Some(Bytes::from(vec![65u8; 1024]))
        );
        assert_eq!(
            tx.get(b"large_key_00250").expect("read middle key"),
            Some(Bytes::from(vec![65u8; 1024]))
        );
    });
}

#[test]
fn should_preserve_values_given_two_large_transactions_within_budget_when_read() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(&opts.memory_budget(256 * 1024), mode);
        let cf = engine.create_column_family("test").expect("create cf");

        let mut tx1 = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin first transaction");
        for i in 0..128 {
            let key = format!("batch1_key_{i:03}");
            tx1.put(key.as_bytes().to_vec(), vec![65u8; 1024], None)
                .expect("put batch1 value");
        }
        tx1.commit(buffered_write_options(mode))
            .expect("commit first transaction");

        let mut tx2 = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin second transaction");
        for i in 0..128 {
            let key = format!("batch2_key_{i:03}");
            tx2.put(key.as_bytes().to_vec(), vec![66u8; 1024], None)
                .expect("put batch2 value");
        }

        // Act
        tx2.commit(buffered_write_options(mode))
            .expect("commit second transaction");

        // Assert
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin verification transaction");
        assert_eq!(
            tx.get(b"batch1_key_000").expect("read first batch key"),
            Some(Bytes::from(vec![65u8; 1024]))
        );
        assert_eq!(
            tx.get(b"batch2_key_000").expect("read second batch key"),
            Some(Bytes::from(vec![66u8; 1024]))
        );
        assert_eq!(
            tx.get(b"batch1_key_127").expect("read last batch1 key"),
            Some(Bytes::from(vec![65u8; 1024]))
        );
        assert_eq!(
            tx.get(b"batch2_key_127").expect("read last batch2 key"),
            Some(Bytes::from(vec![66u8; 1024]))
        );
    });
}

#[test]
fn should_preserve_sample_keys_given_large_transaction_low_budget_when_committed() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(&opts.memory_budget(64 * 1024), mode);
        let cf = engine.create_column_family("test").expect("create cf");

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin low-budget transaction");
        for i in 0..200 {
            let key = format!("spilltest_key_{i:04}");
            tx.put(key.as_bytes().to_vec(), vec![88u8; 512], None)
                .expect("put low-budget value");
        }

        // Act
        tx.commit(buffered_write_options(mode))
            .expect("commit low-budget transaction");

        // Assert
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin verification transaction");
        for key in [
            b"spilltest_key_0000".as_slice(),
            b"spilltest_key_0100".as_slice(),
            b"spilltest_key_0199".as_slice(),
        ] {
            assert_eq!(
                tx.get(key).expect("read sample low-budget key"),
                Some(Bytes::from(vec![88u8; 512])),
                "mode: {} key: {}",
                mode,
                String::from_utf8_lossy(key)
            );
        }
    });
}
