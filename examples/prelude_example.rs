//! Example demonstrating the canonical Midge API using only the prelude.
//!
//! This example shows that `use cntryl_midge::prelude::*;` provides everything
//! needed for the standard transaction-based workflow.

use cntryl_midge::prelude::*;

fn main() -> Result<(), MidgeError> {
    // Open engine (using test utilities for this example)
    let opts = cntryl_midge::testkit::MidgeOptions {
        storage_mode: cntryl_midge::testkit::StorageMode::Memory,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts)?;
    let cf = engine.default_column_family();

    // Write: explicit transaction, explicit commit, explicit durability
    let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    tx.put(b"hello".to_vec(), b"world".to_vec(), None)?;
    tx.put(b"foo".to_vec(), b"bar".to_vec(), None)?;
    engine.commit(tx, WriteOptions::recommended())?;

    // Read: explicit transaction
    let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
    let value = tx.get(b"hello")?;
    println!("Value: {:?}", value);

    // Scan: using scan method
    let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
    let results = tx.scan(b"", b"\xFF")?;
    println!("Scan found {} keys", results.len());

    Ok(())
}
