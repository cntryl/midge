//! Small assertion helpers shared across integration tests.

use crate::engine::ColumnFamilyHandle;

/// Assert that a key has the expected value.
pub fn assert_get_equals(
    engine: &crate::MidgeEngine,
    cf: &ColumnFamilyHandle,
    key: &[u8],
    expected: &[u8],
) {
    let result = engine.get_cf(cf, key).expect("get failed");
    assert_eq!(result.as_ref().map(|b| b.as_ref()), Some(expected));
}

/// Assert that a key is absent (returns None).
pub fn assert_key_absent(engine: &crate::MidgeEngine, cf: &ColumnFamilyHandle, key: &[u8]) {
    let result = engine.get_cf(cf, key).expect("get failed");
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
        engine.put_cf(cf, key, value)?;
    }
    Ok(())
}
