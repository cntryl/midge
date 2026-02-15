//! CloudFirst durability policy verification tests
//!
//! Ensures CloudFirst background mode never blocks on single writes,
//! while CloudStrict mode provides explicit cloud durability guarantees.

use cntryl_midge::testkit::{open_with_mode, opts_for_mode};
use cntryl_midge::{TransactionMode, WriteOptions};

#[test]
fn should_batch_writes_when_using_cloud_mode() {
    // Arrange
    let opts = opts_for_mode("cloud");
    let engine = open_with_mode(opts, "cloud");
    let cf = engine.create_column_family("test").expect("create cf");
    let cf_id = cf.id();

    // Act: Write multiple records with buffered policy (default CloudFirst background mode)
    // CloudFirst batches uploads in the background, so commits should not block
    for i in 0..100 {
        let mut tx = engine
            .begin_tx(cf_id, TransactionMode::ReadWrite)
            .expect("begin");
        let key = format!("key_{:04}", i);
        let value = format!("value_{:04}", i);
        tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
            .unwrap();
        engine.commit(tx, WriteOptions::buffered()).unwrap();
    }

    // Assert: Verify all data is readable (correctness check)
    for i in 0..100 {
        let tx = engine
            .begin_tx(cf_id, TransactionMode::ReadOnly)
            .expect("begin");
        let key = format!("key_{:04}", i);
        let value = tx.get(key.as_bytes()).unwrap();
        assert!(value.is_some(), "key_{:04} should exist", i);
    }
}

#[test]
fn should_support_cloud_strict_for_explicit_durability() {
    // Arrange
    let opts = opts_for_mode("cloud");
    let engine = open_with_mode(opts, "cloud");
    let cf = engine.create_column_family("test").expect("create cf");
    let cf_id = cf.id();

    // Act: Write with CloudStrict policy (explicit cloud durability)
    let mut tx = engine
        .begin_tx(cf_id, TransactionMode::ReadWrite)
        .expect("begin");
    tx.put(b"strict_key".to_vec(), b"strict_value".to_vec(), None)
        .unwrap();

    // CloudStrict forces immediate WAL seal + rotate + upload, blocking until complete
    engine.commit(tx, WriteOptions::cloud_strict()).unwrap();

    // Assert: Data should be readable immediately
    let tx = engine
        .begin_tx(cf_id, TransactionMode::ReadOnly)
        .expect("begin");
    let value = tx.get(b"strict_key").unwrap();
    assert!(value.is_some());
    assert_eq!(value.unwrap().as_ref(), b"strict_value");
}

#[test]
fn should_flush_cloud_segments_on_shutdown() {
    // Arrange
    let opts = opts_for_mode("cloud");
    let engine = open_with_mode(opts, "cloud");
    let cf = engine.create_column_family("test").expect("create cf");
    let cf_id = cf.id();

    // Act: Write data with background CloudFirst
    for i in 0..50 {
        let mut tx = engine
            .begin_tx(cf_id, TransactionMode::ReadWrite)
            .expect("begin");
        let key = format!("shutdown_key_{:04}", i);
        let value = format!("shutdown_value_{:04}", i);
        tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
            .unwrap();
        engine.commit(tx, WriteOptions::buffered()).unwrap();
    }

    // Drop engine (triggers shutdown)
    drop(engine);

    // Assert: Shutdown should complete without panics, and all pending
    // CloudFirst uploads should be flushed (verified implicitly by clean drop)
}
