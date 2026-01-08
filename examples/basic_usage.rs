//! Basic usage example for Midge
//!
//! This demonstrates the core API that library consumers would use.

use cntryl_midge::{MidgeEngine, MidgeResult, Query, TransactionMode, WriteOptions};
use std::path::PathBuf;

fn main() -> MidgeResult<()> {
    // Open a database
    let db = MidgeEngine::open(PathBuf::from("./example_db"))?;
    let cf = db.default_column_family();

    // Basic put/get operations using transactions
    let mut tx = db.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    tx.put(cf.id(), b"key1".to_vec(), b"value1".to_vec(), None)?;
    tx.put(cf.id(), b"key2".to_vec(), b"value2".to_vec(), None)?;
    db.commit(tx, WriteOptions::sync())?;

    // Read using transaction
    let tx = db.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
    let value = db.tx_get(&tx, b"key1")?;
    println!("key1 = {:?}", value);
    // ReadOnly transactions don't need commit

    // Delete operation
    let mut tx = db.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    tx.delete(cf.id(), b"key1".to_vec())?;
    db.commit(tx, WriteOptions::buffered())?;

    // Multiple operations in one transaction (replaces WriteBatch)
    let mut tx = db.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    tx.put(cf.id(), b"batch_key1".to_vec(), b"batch_value1".to_vec(), None)?;
    tx.put(cf.id(), b"batch_key2".to_vec(), b"batch_value2".to_vec(), None)?;
    tx.delete(cf.id(), b"key2".to_vec())?;
    db.commit(tx, WriteOptions::buffered())?;

    // Range scan within transaction
    let tx = db.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
    let results = db.tx_scan(&tx, b"batch_", b"batch_z")?;
    println!("Found {} keys in range", results.len());

    // Range scan with Query parameters
    let query = Query::new()
        .start_key(b"batch_".to_vec().into())
        .end_key(b"batch_z".to_vec().into())
        .limit(10);
    let results = db.tx_scan_range(&tx, &query)?;
    println!("Query returned {} keys", results.len());

    // Flush and sync
    db.sync()?;
    db.flush()?;

    // Shutdown gracefully
    db.shutdown()?;

    Ok(())
}
