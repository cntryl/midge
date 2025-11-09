// Atomic Operations (Insert, CAS)
// Extracted from engine.rs

#![allow(clippy::field_reassign_with_default)]
// Engine integration tests consolidated per repo preference
// Structure: Arrange // Act // Assert, one behavior per test, behavior-first names
use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, Query, StorageMode};

mod common;
use common::test_temp_dir;
#[test]
fn should_insert_key_given_nonexistent_key() {
    // Arrange
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();
    let key = Bytes::from("key1");
    let value = Bytes::from("value1");

    // Act
    let inserted = engine.insert(&cf, key.as_ref(), value.as_ref()).unwrap();
    let result = engine.get(&cf, key.as_ref()).unwrap();

    // Assert
    assert!(inserted, "First insert should return true");
    assert_eq!(result, Some(value));
}

#[test]
fn should_not_insert_given_existing_key() {
    // Arrange
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();
    let key = Bytes::from("key1");
    let value1 = Bytes::from("value1");
    let value2 = Bytes::from("value2");
    engine.put(&cf, key.as_ref(), value1.as_ref()).unwrap();

    // Act
    let inserted = engine.insert(&cf, key.as_ref(), value2.as_ref()).unwrap();
    let result = engine.get(&cf, key.as_ref()).unwrap();

    // Assert
    assert!(!inserted, "Insert should return false for existing key");
    assert_eq!(result, Some(value1));
}

#[test]
fn should_return_existing_value_given_insert_with_value() {
    // Arrange
    use cntryl_midge::InsertResult;
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();
    let key = Bytes::from("key1");
    let value1 = Bytes::from("value1");
    let value2 = Bytes::from("value2");

    // Act
    let result1 = engine
        .insert_with_value(&cf, key.as_ref(), value1.as_ref())
        .unwrap();
    let result2 = engine.insert_with_value(&cf, key.as_ref(), value2.as_ref()).unwrap();
    let stored = engine.get(&cf, key.as_ref()).unwrap();

    // Assert
    assert_eq!(result1, InsertResult::Inserted);
    assert_eq!(result2, InsertResult::AlreadyExists(value1.clone()));
    assert_eq!(stored, Some(value1));
}

#[test]
fn should_swap_value_given_matching_expected() {
    // Arrange
    use cntryl_midge::CasResult;
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();
    let key = Bytes::from("counter");

    // Act
    let result1 = engine
        .compare_and_swap(&cf, key.as_ref(), None, b"0")
        .unwrap();
    let result2 = engine
        .compare_and_swap(&cf, key.as_ref(), Some(Bytes::from("0")), b"1")
        .unwrap();
    let value = engine.get(&cf, key.as_ref()).unwrap();

    // Assert
    assert_eq!(result1, CasResult::Swapped);
    assert_eq!(result2, CasResult::Swapped);
    assert_eq!(value, Some(Bytes::from("1")));
}

#[test]
fn should_handle_concurrent_inserts_given_race_simulation() {
    // Arrange
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();
    let key = Bytes::from("shared_key");

    // Act
    let result1 = engine.insert(&cf, key.as_ref(), b"value1").unwrap();
    let result2 = engine.insert(&cf, key.as_ref(), b"value2").unwrap();
    let result3 = engine.insert(&cf, key.as_ref(), b"value3").unwrap();
    let value = engine.get(&cf, key.as_ref()).unwrap();

    // Assert
    assert!(result1, "First insert should succeed");
    assert!(!result2, "Second insert should fail");
    assert!(!result3, "Third insert should fail");
    assert_eq!(value, Some(Bytes::from("value1")));
}

#[test]
fn should_handle_concurrent_cas_given_race_simulation() {
    // Arrange
    use cntryl_midge::CasResult;
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();
    let key = Bytes::from("counter");
    engine.put(&cf, key.as_ref(), b"0").unwrap();

    // Act
    let result1 = engine
        .compare_and_swap(&cf, key.as_ref(), Some(Bytes::from("0")), b"1")
        .unwrap();
    let result2 = engine
        .compare_and_swap(&cf, key.as_ref(), Some(Bytes::from("0")), b"2")
        .unwrap();
    let result3 = engine
        .compare_and_swap(&cf, key.as_ref(), Some(Bytes::from("1")), b"3")
        .unwrap();
    let value = engine.get(&cf, key.as_ref()).unwrap();

    // Assert
    assert_eq!(result1, CasResult::Swapped);
    assert_eq!(result2, CasResult::Mismatch(Some(Bytes::from("1"))));
    assert_eq!(result3, CasResult::Swapped);
    assert_eq!(value, Some(Bytes::from("3")));
}

#[test]
fn should_respect_snapshot_isolation_given_insert() {
    // Arrange
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();
    let key = Bytes::from("key1");
    let value1 = Bytes::from("value1");
    let value2 = Bytes::from("value2");
    let snap1 = engine.snapshot();

    // Act
    let inserted1 = engine.insert(&cf, key.as_ref(), value1.as_ref()).unwrap();
    let snap2 = engine.snapshot();
    let inserted2 = engine.insert(&cf, key.as_ref(), value2.as_ref()).unwrap();

    // Assert
    assert!(inserted1);
    assert!(!inserted2);
    assert_eq!(engine.get_at(key.as_ref(), &snap1).unwrap(), None);
    assert_eq!(engine.get_at(key.as_ref(), &snap2).unwrap(), Some(value1.clone()));
    assert_eq!(engine.get(&cf, key.as_ref()).unwrap(), Some(value1));
}

#[test]
fn should_handle_insert_after_delete() {
    // Arrange
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();
    let key = Bytes::from("key1");
    let value1 = Bytes::from("value1");
    let value2 = Bytes::from("value2");
    engine.put(&cf, key.as_ref(), value1.as_ref()).unwrap();
    engine.delete(&cf, key.as_ref()).unwrap();

    // Act
    let inserted = engine.insert(&cf, key.as_ref(), value2.as_ref()).unwrap();
    let result = engine.get(&cf, key.as_ref()).unwrap();

    // Assert
    assert!(inserted, "Insert should succeed after delete");
    assert_eq!(result, Some(value2));
}

#[test]
fn should_use_latest_value_given_cas_after_concurrent_put() {
    // Arrange
    use cntryl_midge::CasResult;
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();
    let key = Bytes::from("key1");
    engine.put(&cf, key.as_ref(), b"A").unwrap();
    let snap = engine.snapshot();

    // Act
    engine.put(&cf, key.as_ref(), b"B").unwrap();
    let result = engine
        .compare_and_swap(&cf, key.as_ref(), Some(Bytes::from("A")), b"C")
        .unwrap();

    // Assert
    assert_eq!(
        result,
        CasResult::Mismatch(Some(Bytes::from("B"))),
        "CAS should see the updated value"
    );
    assert_eq!(engine.get_at(&key, &snap).unwrap(), Some(Bytes::from("A")));
}

// ============================================================================
// Read-only mode tests (consolidated from tests/read_only.rs)
// ============================================================================
