//! Test `BestEffort` durability mode - verifies WAL is skipped but data is in memtable
//!
//! `BestEffort` mode should:
//! 1. Skip WAL writes entirely (no I/O overhead)
//! 2. Update memtable immediately (data visible for reads)
//! 3. Allow flush to SST (data becomes durably restart-safe after flush)
//! 4. Lose data on crash before flush (documented trade-off)

mod common;
use cntryl_midge::{MidgeEngine, TransactionMode, WriteOptions};
use common::*;
use std::time::Duration;

#[test]
fn should_skip_wal_when_using_best_effort() -> cntryl_midge::MidgeResult<()> {
    // Arrange
    let opts = opts_for_mode("local");
    let engine = MidgeEngine::open(opts.to_open_options())?;
    let cf = engine.create_column_family("test")?;

    // Act - Write with BestEffort (should skip WAL)
    let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    tx.put(b"key1".to_vec(), b"value1".to_vec(), None)?;
    tx.put(b"key2".to_vec(), b"value2".to_vec(), None)?;
    tx.commit(WriteOptions::best_effort())?;

    // Assert - Data is visible in memtable
    let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
    let val1 = tx.get(b"key1")?;
    let val2 = tx.get(b"key2")?;
    assert_eq!(val1.as_deref(), Some(&b"value1"[..]));
    assert_eq!(val2.as_deref(), Some(&b"value2"[..]));

    Ok(())
}

#[test]
fn should_persist_best_effort_data_when_flushed() -> cntryl_midge::MidgeResult<()> {
    // Arrange
    let opts = opts_for_mode("local");
    let mut engine = MidgeEngine::open(opts.clone().to_open_options())?;
    let cf = engine.create_column_family("test")?;
    let cf_id = cf.id();

    // Act - Write with BestEffort, then flush
    let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite)?;
    for i in 0..100 {
        tx.put(
            format!("key{i}").into_bytes(),
            format!("value{i}").into_bytes(),
            None,
        )?;
    }
    tx.commit(WriteOptions::best_effort())?;

    // Flush to SST - but note: without WAL, BestEffort data relies ONLY on successful flush
    engine.flush_cf(&cf)?;

    // Write a durable marker after flush so restart exercises both WAL replay and
    // manifest-backed SST visibility on reopen.
    let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite)?;
    tx.put(b"durable_marker".to_vec(), b"marker".to_vec(), None)?;
    tx.commit(WriteOptions::buffered())?;

    // Reopen engine (simulates restart)
    engine.shutdown(Duration::from_secs(2))?;
    let engine = MidgeEngine::open(opts.to_open_options())?;

    // Assert - Once flush_cf() succeeds, flushed data must be durable across restart
    let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly)?;
    let val_0 = tx.get(b"key0")?;
    let marker = tx.get(b"durable_marker")?;

    assert!(
        val_0.is_some(),
        "BestEffort data should survive restart once flush_cf() succeeds"
    );
    assert_eq!(marker.as_deref(), Some(&b"marker"[..]));

    Ok(())
}

#[test]
fn should_lose_best_effort_data_when_not_flushed() -> cntryl_midge::MidgeResult<()> {
    // Arrange
    let opts = opts_for_mode("local");
    let mut engine = MidgeEngine::open(opts.clone().to_open_options())?;
    let cf = engine.create_column_family("test")?;
    let cf_id = cf.id();

    // Act - Write with BestEffort but DON'T flush
    let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite)?;
    tx.put(b"ephemeral_key".to_vec(), b"ephemeral_value".to_vec(), None)?;
    tx.commit(WriteOptions::best_effort())?;

    // Verify data is in memtable before restart
    let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly)?;
    let val_before = tx.get(b"ephemeral_key")?;
    assert_eq!(val_before.as_deref(), Some(&b"ephemeral_value"[..]));
    drop(tx);

    // Simulate crash: drop engine WITHOUT flush
    engine.shutdown(Duration::from_secs(2))?;

    // Reopen engine
    let engine = MidgeEngine::open(opts.to_open_options())?;

    // Assert - Data is lost (not in WAL, not in SST)
    let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly)?;
    let val_after = tx.get(b"ephemeral_key")?;
    assert_eq!(
        val_after, None,
        "BestEffort data should be lost on crash without flush"
    );

    Ok(())
}

#[test]
fn should_handle_large_batches_with_best_effort() -> cntryl_midge::MidgeResult<()> {
    // Arrange - This tests the original YCSB issue: large batches shouldn't overflow WAL queue
    let opts = opts_for_mode("local");
    let engine = MidgeEngine::open(opts.to_open_options())?;
    let cf = engine.create_column_family("test")?;

    // Act - Write 50,000 ops (same size as YCSB batch that triggered the bug)
    // This would previously fail with "WAL queue full (1000 items)"
    let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    for i in 0..50_000 {
        tx.put(
            format!("key{i:08}").into_bytes(),
            format!("value{i:08}").into_bytes(),
            None,
        )?;
    }

    // This should NOT panic with "WAL queue full" anymore
    tx.commit(WriteOptions::best_effort())?;

    // Assert - All data is visible
    let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
    let val_first = tx.get(b"key00000000")?;
    let val_mid = tx.get(b"key00025000")?;
    let val_last = tx.get(b"key00049999")?;

    assert!(val_first.is_some(), "First key should exist");
    assert!(val_mid.is_some(), "Middle key should exist");
    assert!(val_last.is_some(), "Last key should exist");

    Ok(())
}
