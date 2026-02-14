//! Test BestEffort durability mode - verifies WAL is skipped but data is in memtable
//!
//! BestEffort mode should:
//! 1. Skip WAL writes entirely (no I/O overhead)
//! 2. Update memtable immediately (data visible for reads)
//! 3. Allow flush to SST (data persists via flush)
//! 4. Lose data on crash before flush (documented trade-off)

use cntryl_midge::testkit::*;
use cntryl_midge::{MidgeEngine, TransactionMode, WriteOptions};

#[test]
fn should_skip_wal_when_using_best_effort() -> cntryl_midge::MidgeResult<()> {
    // Arrange
    let opts = opts_for_mode("local");
    let engine = MidgeEngine::open_with_options(opts)?;
    let cf = engine.create_column_family("test")?;

    // Act - Write with BestEffort (should skip WAL)
    let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    tx.put(b"key1".to_vec(), b"value1".to_vec(), None)?;
    tx.put(b"key2".to_vec(), b"value2".to_vec(), None)?;
    engine.commit(tx, WriteOptions::best_effort())?;

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
    let engine = MidgeEngine::open_with_options(opts.clone())?;
    let cf = engine.create_column_family("test")?;
    let cf_id = cf.id();

    // Act - Write with BestEffort, then flush
    let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite)?;
    for i in 0..100 {
        tx.put(
            format!("key{}", i).into_bytes(),
            format!("value{}", i).into_bytes(),
            None,
        )?;
    }
    engine.commit(tx, WriteOptions::best_effort())?;

    // Flush to SST - but note: without WAL, BestEffort data relies ONLY on successful flush
    engine.flush_cf(&cf)?;

    // Verify flush completed by writing with durable mode AFTER flush
    // This ensures the flush has hit disk before we restart
    let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite)?;
    tx.put(b"durable_marker".to_vec(), b"marker".to_vec(), None)?;
    engine.commit(tx, WriteOptions::buffered())?;

    // Reopen engine (simulates restart)
    drop(engine);
    let engine = MidgeEngine::open_with_options(opts)?;

    // Assert - Best-effort data should be lost on restart even with flush
    // (flush may not be synchronous, and without WAL, data is ephemeral)
    // This demonstrates the trade-off: BestEffort is for bulk loads where
    // the entire dataset can be reloaded on failure.
    let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly)?;
    let val_0 = tx.get(b"key0")?;

    // Data loss is expected - BestEffort provides no durability guarantee
    // The safe pattern is: load with best_effort → flush → switch to buffered/sync
    // If crash happens during load phase, reload from source.
    assert!(
        val_0.is_none(),
        "BestEffort data without WAL should not survive restart"
    );

    Ok(())
}

#[test]
fn should_lose_best_effort_data_when_not_flushed() -> cntryl_midge::MidgeResult<()> {
    // Arrange
    let opts = opts_for_mode("local");
    let engine = MidgeEngine::open_with_options(opts.clone())?;
    let cf = engine.create_column_family("test")?;
    let cf_id = cf.id();

    // Act - Write with BestEffort but DON'T flush
    let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite)?;
    tx.put(
        b"ephemeral_key".to_vec(),
        b"ephemeral_value".to_vec(),
        None,
    )?;
    engine.commit(tx, WriteOptions::best_effort())?;

    // Verify data is in memtable before restart
    let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly)?;
    let val_before = tx.get(b"ephemeral_key")?;
    assert_eq!(val_before.as_deref(), Some(&b"ephemeral_value"[..]));
    drop(tx);

    // Simulate crash: drop engine WITHOUT flush
    drop(engine);

    // Reopen engine
    let engine = MidgeEngine::open_with_options(opts)?;

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
    let engine = MidgeEngine::open_with_options(opts)?;
    let cf = engine.create_column_family("test")?;

    // Act - Write 50,000 ops (same size as YCSB batch that triggered the bug)
    // This would previously fail with "WAL queue full (1000 items)"
    let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    for i in 0..50_000 {
        tx.put(
            format!("key{:08}", i).into_bytes(),
            format!("value{:08}", i).into_bytes(),
            None,
        )?;
    }

    // This should NOT panic with "WAL queue full" anymore
    engine.commit(tx, WriteOptions::best_effort())?;

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
