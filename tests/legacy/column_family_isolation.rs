mod common;
use cntryl_midge::ColumnFamilyConfig;
use common::{assert_get_equals_cf, assert_key_absent_cf, new_engine};

#[test]
fn should_not_return_key_from_different_cf_given_same_user_key_when_read() {
    // Arrange
    let (_dir, eng) = new_engine();
    let default_cf = eng.default_column_family();
    let cf2 = eng
        .create_column_family("cf2", ColumnFamilyConfig::default())
        .expect("create CF");

    // Act - write same key to both CFs
    eng.put(&default_cf, b"shared_key", b"default_value")
        .expect("put default");
    eng.put(&cf2, b"shared_key", b"cf2_value").expect("put cf2");

    // Assert - each CF should only see its own value
    assert_get_equals_cf(&eng, &default_cf, b"shared_key", b"default_value");
    assert_get_equals_cf(&eng, &cf2, b"shared_key", b"cf2_value");
}

#[test]
fn should_compact_cf_independently_given_multiple_cfs_when_threshold_exceeded() {
    // Arrange
    let (_dir, eng) = new_engine();
    let default_cf = eng.default_column_family();
    let cf2 = eng
        .create_column_family("cf2", ColumnFamilyConfig::default())
        .expect("create CF");

    // Act - write different amounts to each CF
    for i in 0..100 {
        eng.put(
            &default_cf,
            format!("key{:03}", i).as_bytes(),
            b"value_default",
        )
        .expect("put default");
    }

    for i in 0..200 {
        eng.put(&cf2, format!("key{:03}", i).as_bytes(), b"value_cf2")
            .expect("put cf2");
    }

    // Assert - both CFs should maintain their data independently
    assert_get_equals_cf(&eng, &default_cf, b"key050", b"value_default");
    assert_get_equals_cf(&eng, &cf2, b"key050", b"value_cf2");
    assert_get_equals_cf(&eng, &cf2, b"key150", b"value_cf2");

    // Verify cf2 has more data than default
    let cf2_key_not_in_default = eng.get(&default_cf, b"key150").expect("get");
    assert!(
        cf2_key_not_in_default.is_none(),
        "Key should only exist in cf2"
    );
}

#[test]
fn should_recreate_cf_with_same_name_given_previous_drop_when_reopen() {
    // Arrange
    let (_dir, eng) = new_engine();
    let default_cf = eng.default_column_family();
    eng.put(&default_cf, b"key1", b"value1").expect("put");

    let test_cf = eng
        .create_column_family("test_cf", ColumnFamilyConfig::default())
        .expect("create");
    eng.put(&test_cf, b"cf_key", b"cf_value").expect("put");

    // Act - flush and drop CF, then recreate with same name
    eng.flush_cf(&test_cf).expect("flush before drop");
    eng.drop_column_family(&test_cf).expect("drop");
    let new_cf = eng
        .create_column_family("test_cf", ColumnFamilyConfig::default())
        .expect("recreate");

    // Assert - old data should not be visible in recreated CF (fresh CF)
    assert_key_absent_cf(&eng, &new_cf, b"cf_key");

    // Original default CF should be unaffected
    assert_get_equals_cf(&eng, &default_cf, b"key1", b"value1");
}
