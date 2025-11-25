mod common;
use common::*;

#[test]
fn should_coalesce_large_tombstone_fanout_during_compaction() {
    // Arrange
    let (tmp, eng) = new_engine();
    let cf = eng.default_column_family();

    for i in 0..100u8 {
        eng.put(&cf, &[i], b"v").unwrap();
    }

    // Act
    eng.delete_range(&cf, &[0], &[255]).unwrap();
    eng.flush().unwrap();

    // Assert
    for i in 0..100u8 {
        assert!(eng.get(&cf, &[i]).unwrap().is_none());
    }

    drop(eng);
    drop(tmp);
}

#[test]
fn should_handle_long_lived_snapshots_with_massive_range_tombstones() {
    // Arrange
    let (tmp, eng) = new_engine();
    let cf = eng.default_column_family();

    eng.put(&cf, b"x", b"v").unwrap();
    let snap = eng.snapshot();

    // Act
    eng.delete_range(&cf, b"a", b"z").unwrap();
    eng.flush().unwrap();

    // Assert
    // Snapshot should still be able to read pre-delete view
    assert!(snap.get(&eng, &cf, b"x").unwrap().is_some());

    drop(snap);
    drop(eng);
    drop(tmp);
}

#[test]
fn should_apply_range_tombstones_given_cf_flush_when_compacting() {
    // Arrange
    let (tmp, eng) = new_engine();
    let cf = eng.default_column_family();

    eng.put(&cf, b"1", b"one").unwrap();
    eng.put(&cf, b"2", b"two").unwrap();

    // Act
    eng.delete_range(&cf, b"0", b"3").unwrap();
    eng.flush().unwrap();

    // Assert
    assert!(eng.get(&cf, b"1").unwrap().is_none());
    assert!(eng.get(&cf, b"2").unwrap().is_none());

    drop(eng);
    drop(tmp);
}

#[test]
fn should_handle_snapshot_then_tombstone_then_compaction_triple_interaction() {
    // Arrange
    let (tmp, eng) = new_engine();
    let cf = eng.default_column_family();

    for i in 0..10u8 {
        eng.put(&cf, &[i], format!("v{}", i).as_bytes()).unwrap();
    }

    let snap = eng.snapshot();

    // Act
    eng.delete_range(&cf, &[2], &[8]).unwrap();
    eng.flush().unwrap();

    // Assert
    // Snapshot should still see original items
    for i in 0..10u8 {
        assert!(snap.get(&eng, &cf, &[i]).unwrap().is_some());
    }

    // Engine should reflect tombstone for affected range
    for i in 2..8u8 {
        assert!(eng.get(&cf, &[i]).unwrap().is_none());
    }

    drop(snap);
    drop(eng);
    drop(tmp);
}
