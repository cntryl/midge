//! Read-path behavior across multiple flushed SST generations.
//!
//! The engine does not currently expose hot-SST counters through a public test
//! API, so these tests verify only the externally observable read behavior that
//! exercises those code paths.

use bytes::Bytes;
use cntryl_midge::testkit::{open_with_mode, opts_for_mode};
use cntryl_midge::MidgeResult;
use cntryl_midge::{TransactionMode, WriteOptions};

#[test]
fn should_preserve_reads_from_both_flushed_batches_after_repeated_access() -> MidgeResult<()> {
    // Arrange
    let engine = open_with_mode(opts_for_mode("memory"), "memory");
    let cf = engine.create_column_family("test").expect("create cf");

    for i in 0..10 {
        let key = format!("batch1_key{:03}", i);
        let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
        tx.put(key.as_bytes().to_vec(), b"value1".to_vec(), None)?;
        engine.commit(tx, WriteOptions::best_effort())?;
    }
    engine.flush_cf(&cf)?;

    for i in 0..10 {
        let key = format!("batch2_key{:03}", i);
        let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
        tx.put(key.as_bytes().to_vec(), b"value2".to_vec(), None)?;
        engine.commit(tx, WriteOptions::best_effort())?;
    }
    engine.flush_cf(&cf)?;

    // Act
    for _ in 0..5 {
        let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
        assert_eq!(
            read_tx.get(b"batch1_key000")?,
            Some(Bytes::from_static(b"value1"))
        );
    }
    let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;

    // Assert
    assert_eq!(
        read_tx.get(b"batch2_key000")?,
        Some(Bytes::from_static(b"value2"))
    );
    Ok(())
}

#[test]
fn should_return_latest_value_for_overlapping_l0_keys_after_flushes() -> MidgeResult<()> {
    // Arrange
    let engine = open_with_mode(opts_for_mode("memory"), "memory");
    let cf = engine.create_column_family("test").expect("create cf");

    for batch in 0..3 {
        for i in 0..5 {
            let key = format!("key{:03}", i);
            let value = format!("value_batch{}", batch);
            let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
            tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)?;
            engine.commit(tx, WriteOptions::best_effort())?;
        }
        engine.flush_cf(&cf)?;
    }

    // Act
    let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;

    // Assert
    assert_eq!(
        read_tx.get(b"key000")?,
        Some(Bytes::from_static(b"value_batch2"))
    );
    assert_eq!(
        read_tx.get(b"key004")?,
        Some(Bytes::from_static(b"value_batch2"))
    );
    Ok(())
}

#[test]
fn should_find_keys_in_middle_range_after_flushing_disjoint_key_ranges() -> MidgeResult<()> {
    // Arrange
    let engine = open_with_mode(opts_for_mode("memory"), "memory");
    let cf = engine.create_column_family("test").expect("create cf");

    for i in 0..10 {
        let key = format!("a{:03}", i);
        let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
        tx.put(key.as_bytes().to_vec(), b"value_a".to_vec(), None)?;
        engine.commit(tx, WriteOptions::best_effort())?;
    }
    engine.flush_cf(&cf)?;

    for i in 0..10 {
        let key = format!("b{:03}", i);
        let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
        tx.put(key.as_bytes().to_vec(), b"value_b".to_vec(), None)?;
        engine.commit(tx, WriteOptions::best_effort())?;
    }
    engine.flush_cf(&cf)?;

    for i in 0..10 {
        let key = format!("c{:03}", i);
        let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
        tx.put(key.as_bytes().to_vec(), b"value_c".to_vec(), None)?;
        engine.commit(tx, WriteOptions::best_effort())?;
    }
    engine.flush_cf(&cf)?;

    // Act
    let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;

    // Assert
    assert_eq!(read_tx.get(b"b005")?, Some(Bytes::from_static(b"value_b")));
    assert_eq!(read_tx.get(b"a005")?, Some(Bytes::from_static(b"value_a")));
    assert_eq!(read_tx.get(b"c005")?, Some(Bytes::from_static(b"value_c")));
    Ok(())
}

#[test]
fn should_preserve_readability_after_mixed_access_pattern() -> MidgeResult<()> {
    // Arrange
    let engine = open_with_mode(opts_for_mode("memory"), "memory");
    let cf = engine.create_column_family("test").expect("create cf");

    for i in 0..20 {
        let key = format!("key{:03}", i);
        let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
        tx.put(key.as_bytes().to_vec(), b"test_value".to_vec(), None)?;
        engine.commit(tx, WriteOptions::best_effort())?;
    }
    engine.flush_cf(&cf)?;

    // Act
    for _ in 0..10 {
        let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
        assert_eq!(
            read_tx.get(b"key000")?,
            Some(Bytes::from_static(b"test_value"))
        );
    }
    for _ in 0..3 {
        let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
        assert_eq!(
            read_tx.get(b"key010")?,
            Some(Bytes::from_static(b"test_value"))
        );
    }
    let read_tx1 = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
    let read_tx2 = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;

    // Assert
    assert_eq!(
        read_tx1.get(b"key019")?,
        Some(Bytes::from_static(b"test_value"))
    );
    assert_eq!(read_tx2.get(b"missing_key")?, None);
    Ok(())
}
