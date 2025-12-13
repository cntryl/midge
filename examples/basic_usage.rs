//! Basic usage example for Midge
//!
//! This demonstrates the core API that library consumers would use.

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeResult, Query, WriteBatch};
use std::path::PathBuf;

fn main() -> MidgeResult<()> {
    // Open a database
    let db = MidgeEngine::open(PathBuf::from("./example_db"))?;
    let cf = db.default_column_family();

    // Basic put/get operations
    db.put(cf, b"key1", b"value1")?;
    db.put(cf, b"key2", b"value2")?;

    let value = db.get(cf, b"key1")?;
    println!("key1 = {:?}", value);

    // Delete operation
    db.delete(cf, b"key1")?;

    // Write batch (multiple operations)
    let mut batch = WriteBatch::new();
    batch.put(
        b"batch_key1".to_vec().into(),
        b"batch_value1".to_vec().into(),
    );
    batch.put(
        b"batch_key2".to_vec().into(),
        b"batch_value2".to_vec().into(),
    );
    batch.delete(b"key2".to_vec().into());
    db.write_batch(&batch)?;

    // Range scan
    let query = Query::new()
        .start_key(Bytes::from(&b"batch_"[..]))
        .end_key(Bytes::from(&b"batch_z"[..]))
        .limit(10);
    let results = db.scan(cf, &query)?;
    println!("Found {} keys in range", results.len());

    // Transactions
    let mut txn = db.transaction();
    txn.put(cf.id(), b"txn_key".to_vec(), b"txn_value".to_vec())?;
    txn.put(cf.id(), b"txn_key2".to_vec(), b"txn_value2".to_vec())?;
    db.commit_transaction(txn)?;

    // Snapshots (for consistent reads)
    let snapshot = db.snapshot();
    println!("Created snapshot at sequence {}", snapshot.sequence());

    // Compare-and-swap
    let result = db.compare_and_swap(cf, b"cas_key", None, b"new_value")?;
    println!("CAS result: {:?}", result);

    // Flush and sync
    db.sync()?;
    db.flush()?;

    // Shutdown gracefully
    db.shutdown()?;

    Ok(())
}
