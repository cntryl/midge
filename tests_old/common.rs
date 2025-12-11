// Test utilities module
// This is a stub to satisfy test imports

pub fn assert_get_equals(engine: &cntryl_midge::MidgeEngine, cf: &cntryl_midge::ColumnFamilyHandle, key: &[u8], expected: &[u8]) {
    let result = engine.get_cf(cf, key).expect("get failed");
    assert_eq!(result.as_ref().map(|b| b.as_ref()), Some(expected));
}
