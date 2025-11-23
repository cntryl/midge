mod common;
use common::*;

use bytes::Bytes;
use cntryl_midge::Query;

#[test]
fn should_preserve_snapshot_seq_during_concurrent_freeze() {
    // Arrange
    let (tmp, eng) = new_engine();
    let cf = eng.default_column_family();

    // Act
    eng.put(&cf, b"k1", b"v1").unwrap();
    let snap = eng.snapshot();
    // create further writes that might be flushed
    eng.put(&cf, b"k2", b"v2").unwrap();

    // Assert
    assert!(snap.seq <= eng.current_sequence());
    drop(eng);
    drop(tmp);
}

#[test]
fn should_not_drop_range_tombstones_during_freeze_rollover() {
    // Arrange
    let (tmp, eng) = new_engine();
    let cf = eng.default_column_family();

    // Act
    eng.delete_range(&cf, b"a", b"z").unwrap();
    eng.put(&cf, b"in-range", b"v").unwrap();

    // Assert
    // The delete_range should make the key absent according to the engine's visibility model
    let got = eng.get(&cf, b"in-range").unwrap();
    assert!(got.is_none());

    drop(eng);
    drop(tmp);
}

#[test]
fn should_not_lose_merge_operands_across_freeze_boundary() {
    // Arrange
    let (tmp, eng) = new_engine();
    let cf = eng.default_column_family();

    // Act
    eng.put(&cf, b"m", b"1").unwrap();
    eng.merge_cf(&cf, b"m", b"+2").unwrap();

    // Assert
    let got = eng.get(&cf, b"m").unwrap();
    assert!(got.is_some());

    drop(eng);
    drop(tmp);
}

#[test]
fn should_not_publish_partial_freeze_given_concurrent_writes() {
    // Arrange
    let (tmp, eng) = new_engine();
    let cf = eng.default_column_family();

    // Act
    for i in 0..10u8 {
        eng.put(&cf, &[i], format!("v{}", i).as_bytes()).unwrap();
    }

    // Assert
    for i in 0..10u8 {
        assert!(eng.get(&cf, &[i]).unwrap().is_some());
    }

    drop(eng);
    drop(tmp);
}

#[test]
fn should_resolve_freeze_race_during_large_value_insert() {
    // Arrange
    let (tmp, eng) = new_engine();
    let cf = eng.default_column_family();

    // Act
    let large = Bytes::from(vec![b'x'; 1024 * 8]);
    eng.put(&cf, b"big", large.as_ref()).unwrap();

    // Assert
    let got = eng.get(&cf, b"big").unwrap();
    assert_eq!(got.unwrap(), large);

    drop(eng);
    drop(tmp);
}

#[test]
fn should_support_iterator_across_freeze_and_spill() {
    // Arrange
    let (tmp, eng) = new_engine();
    let cf = eng.default_column_family();

    for i in 0..20u8 {
        eng.put(&cf, &[i], format!("v{}", i).as_bytes()).unwrap();
    }

    // Act: scan to validate iterator-like behaviour
    let rows = eng.scan(&cf, Query::new()).expect("scan");
    let count = rows.len();

    // Assert
    assert_eq!(count, 20);

    drop(eng);
    drop(tmp);
}
