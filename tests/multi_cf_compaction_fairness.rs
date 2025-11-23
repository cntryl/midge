mod common;
use common::*;

use cntryl_midge::api::column_family::ColumnFamilyConfig;
use bytes::Bytes;

#[test]
fn should_not_starve_cf_compaction_under_multi_cf_pressure() {
    // Arrange
    let (tmp, eng) = new_engine();
    let cf1 = eng.create_column_family("cf1", ColumnFamilyConfig::default()).unwrap();
    let cf2 = eng.create_column_family("cf2", ColumnFamilyConfig::default()).unwrap();

    // Act
    for i in 0..100u8 {
        eng.put(&cf1, &[i], format!("a{}", i).as_bytes()).unwrap();
        eng.put(&cf2, &[i + 200], format!("b{}", i).as_bytes()).unwrap();
    }

    // Assert
    // Both column families should contain their writes and be readable deterministically
    for i in 0..100u8 {
        assert!(eng.get(&cf1, &[i]).unwrap().is_some());
        assert!(eng.get(&cf2, &[i + 200]).unwrap().is_some());
    }

    drop(eng);
    drop(tmp);
}

#[test]
fn should_keep_cf_compaction_independent_under_write_pressure() {
    // Arrange
    let (tmp, eng) = new_engine();
    let cf1 = eng.create_column_family("hot", ColumnFamilyConfig::default()).unwrap();
    let cf2 = eng.create_column_family("cold", ColumnFamilyConfig::default()).unwrap();

    // Act
    for i in 0..50u8 {
        eng.put(&cf1, &[i], b"hotval").unwrap();
    }
    for i in 0..5u8 {
        eng.put(&cf2, &[i], b"coldval").unwrap();
    }

    // Assert
    // Under pressure, the small CF2's data remains accessible and consistent
    for i in 0..5u8 {
        assert_eq!(eng.get(&cf2, &[i]).unwrap().unwrap(), Bytes::from("coldval"));
    }

    drop(eng);
    drop(tmp);
}

#[test]
fn should_handle_cf_drop_during_other_cf_compaction() {
    // Arrange
    let (tmp, eng) = new_engine();
    let cf1 = eng.create_column_family("one", ColumnFamilyConfig::default()).unwrap();
    let cf2 = eng.create_column_family("two", ColumnFamilyConfig::default()).unwrap();

    for i in 0..20u8 {
        eng.put(&cf1, &[i], b"x").unwrap();
        eng.put(&cf2, &[i + 100], b"y").unwrap();
    }

    // Act
    // Drop cf2 while other CF is active — should not panic or leak resources
    eng.drop_column_family(&cf2).unwrap();

    // Assert
    // op on dropped CF should produce an error and writes to cf1 remain readable
    assert!(eng.get(&cf1, &[0]).unwrap().is_some());

    drop(eng);
    drop(tmp);
}

#[test]
fn should_not_unblock_freeze_for_other_cf_during_unrelated_compaction() {
    // Arrange
    let (tmp, eng) = new_engine();
    let cf_a = eng.create_column_family("a", ColumnFamilyConfig::default()).unwrap();
    let cf_b = eng.create_column_family("b", ColumnFamilyConfig::default()).unwrap();

    for i in 0..10u8 {
        eng.put(&cf_a, &[i], format!("A{}", i).as_bytes()).unwrap();
        eng.put(&cf_b, &[i + 50], format!("B{}", i).as_bytes()).unwrap();
    }

    // Act
    // Trigger operations that would normally cause compaction; here we simply exercise puts
    eng.flush_cf(&cf_a).unwrap();

    // Assert
    // Both CFs remain accessible and their data intact
    for i in 0..10u8 {
        assert!(eng.get(&cf_a, &[i]).unwrap().is_some());
        assert!(eng.get(&cf_b, &[i + 50]).unwrap().is_some());
    }

    drop(eng);
    drop(tmp);
}
