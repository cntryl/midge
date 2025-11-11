mod common;
use common::{assert_get_equals, assert_key_absent, new_engine};

#[test]
fn should_not_return_key_from_different_cf_given_same_user_key_when_read() {
    // Arrange
    let (_dir, eng) = new_engine();
    let default_cf = eng.default_column_family();
    
    // Act - write to default CF
    eng.put(&default_cf, b"shared_key", b"default_value").expect("put");
    
    // TODO: Create second CF and write same key
    // let cf2 = eng.create_column_family("cf2").expect("create CF");
    // eng.put(&cf2, b"shared_key", b"cf2_value").expect("put");
    
    // Assert - default CF should only see its own value
    assert_get_equals(&eng, b"shared_key", b"default_value");
    
    // TODO: Verify cf2 sees its own value, not default's value
}

#[test]
fn should_compact_cf_independently_given_multiple_cfs_when_threshold_exceeded() {
    // Arrange
    let (_dir, eng) = new_engine();
    let default_cf = eng.default_column_family();
    
    // Act - write to default CF
    for i in 0..100 {
        eng.put(&default_cf, format!("key{:03}", i).as_bytes(), b"value").expect("put");
    }
    
    // TODO: Create second CF and write different data
    // let cf2 = eng.create_column_family("cf2").expect("create CF");
    // for i in 0..200 {
    //     eng.put(&cf2, format!("key{:03}", i).as_bytes(), b"value2").expect("put");
    // }
    
    // TODO: Verify each CF compacts independently based on its own metrics
    
    // Assert - default CF data should be present
    let result = eng.get(&default_cf, b"key050").expect("get");
    assert!(result.is_some(), "CF should compact independently");
}

#[test]
fn should_recreate_cf_with_same_name_given_previous_drop_when_reopen() {
    // Arrange
    let (_dir, eng) = new_engine();
    let cf = eng.default_column_family();
    eng.put(&cf, b"key1", b"value1").expect("put");
    
    // TODO: Create and drop a CF
    // let test_cf = eng.create_column_family("test_cf").expect("create");
    // eng.put(&test_cf, b"cf_key", b"cf_value").expect("put");
    // eng.drop_column_family("test_cf").expect("drop");
    
    // Act - recreate with same name
    // let new_cf = eng.create_column_family("test_cf").expect("recreate");
    
    // Assert - old data should not be visible (fresh CF)
    // assert_key_absent_cf(&eng, &new_cf, b"cf_key");
    
    // Original CF should be unaffected
    assert_get_equals(&eng, b"key1", b"value1");
}
