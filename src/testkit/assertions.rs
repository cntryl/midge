//! Small assertion helpers shared across integration tests.

use crate::engine::{api::TransactionMode, ColumnFamilyHandle};

/// Assert that a key has the expected value.
pub fn assert_get_equals(
    engine: &crate::MidgeEngine,
    cf: &ColumnFamilyHandle,
    key: &[u8],
    expected: &[u8],
) {
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin_tx failed");
    let result = tx.get(key).expect("get failed");
    assert_eq!(result.as_ref().map(|b| b.as_ref()), Some(expected));
}

/// Assert that a key is absent (returns None).
pub fn assert_key_absent(engine: &crate::MidgeEngine, cf: &ColumnFamilyHandle, key: &[u8]) {
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin_tx failed");
    let result = tx.get(key).expect("get failed");
    assert!(
        result.is_none(),
        "Expected key to be absent, but found value"
    );
}

/// Bulk insert keys for testing.
pub fn bulk_put(
    engine: &crate::MidgeEngine,
    cf: &ColumnFamilyHandle,
    kvs: &[(&[u8], &[u8])],
) -> crate::MidgeResult<()> {
    for (key, value) in kvs {
        let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
        tx.put(key.to_vec(), value.to_vec(), None)?;
        engine.commit(tx, crate::engine::api::WriteOptions::buffered())?;
    }
    Ok(())
}
