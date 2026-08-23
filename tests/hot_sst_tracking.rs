//! Read-path behavior across multiple flushed SST generations.
//!
//! The engine does not currently expose hot-SST counters through a public test
//! API, so these tests verify only the externally observable read behavior that
//! exercises those code paths.

use bytes::Bytes;
mod common;
use cntryl_midge::MidgeResult;
use cntryl_midge::{TransactionMode, WriteOptions};
use common::{open_with_mode, opts_for_mode};

#[test]
fn should_return_latest_value_given_overlapping_l0_keys_when_multiple_batches_flushed(
) -> MidgeResult<()> {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("memory"), "memory");
    let cf = engine.create_column_family("test").expect("create cf");

    for batch in 0..3 {
        for i in 0..5 {
            let key = format!("key{i:03}");
            let value = format!("value_batch{batch}");
            let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
            tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)?;
            tx.commit(WriteOptions::best_effort())?;
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
fn should_find_keys_given_disjoint_key_ranges_when_flushed() -> MidgeResult<()> {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("memory"), "memory");
    let cf = engine.create_column_family("test").expect("create cf");

    for i in 0..10 {
        let key = format!("a{i:03}");
        let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
        tx.put(key.as_bytes().to_vec(), b"value_a".to_vec(), None)?;
        tx.commit(WriteOptions::best_effort())?;
    }
    engine.flush_cf(&cf)?;

    for i in 0..10 {
        let key = format!("b{i:03}");
        let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
        tx.put(key.as_bytes().to_vec(), b"value_b".to_vec(), None)?;
        tx.commit(WriteOptions::best_effort())?;
    }
    engine.flush_cf(&cf)?;

    for i in 0..10 {
        let key = format!("c{i:03}");
        let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
        tx.put(key.as_bytes().to_vec(), b"value_c".to_vec(), None)?;
        tx.commit(WriteOptions::best_effort())?;
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
