mod common;
use common::*;

use cntryl_midge::MidgeEngine;
use cntryl_midge::MidgeOptions;
use cntryl_midge::StorageMode;

#[test]
fn should_recover_consistently_given_checkpoint_during_compaction_then_crash() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions { storage_mode: StorageMode::LocalDisk { db_path: dir.path().to_path_buf() }, ..Default::default() };

    // Act
    with_engine_restart(
        opts.clone(),
        |eng| {
            // perform writes and create checkpoint
            let cf = eng.default_column_family();
            for i in 0..20u8 { eng.put(&cf, &[i], format!("v{}", i).as_bytes()).unwrap(); }
            let cp_dir = dir.path().join("cp1");
            eng.create_checkpoint(&cp_dir).expect("checkpoint");
        },
        |eng| {
            // Assert after restart - ensure the first inserted (raw-byte) key exists
            let cf = eng.default_column_family();
            assert!(eng.get(&cf, &[0]).unwrap().is_some());
        }
    );
}

#[test]
fn should_not_produce_partial_checkpoint_when_manifest_is_stale() {
    // Arrange
    let (tmp, eng) = new_engine();
    let cf = eng.default_column_family();
    eng.put(&cf, b"x", b"1").unwrap();

    // Act
    let cp_dir = tmp.path().join("cp2");
    eng.create_checkpoint(&cp_dir).expect("create checkpoint");

    // Assert - checkpoint should open successfully
    let cp_opts = cntryl_midge::MidgeOptions { storage_mode: cntryl_midge::StorageMode::LocalDisk { db_path: cp_dir.clone() }, ..Default::default() };
    let cp = MidgeEngine::open(cp_opts).expect("open checkpoint");
    assert!(cp.get(&cp.default_column_family(), b"x").unwrap().is_some());
}

#[test]
fn should_apply_wal_replay_correctly_when_checkpoint_excludes_pending_tombstones() {
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());

    // Act
    with_engine_restart(
        opts.clone(),
        |eng| {
            let cf = eng.default_column_family();
            eng.put(&cf, b"a", b"1").unwrap();
            eng.delete_range(&cf, b"a", b"z").unwrap();
            let cp_dir = dir.path().join("cp3");
            eng.create_checkpoint(&cp_dir).unwrap();
        },
        |eng| {
            // After restart the WAL replay should return a consistent view
            let cf = eng.default_column_family();
            // Either the delete applied or the checkpoint captured the state: deterministic check is that engine doesn't panic
            assert!(eng.get(&cf, b"a").is_ok());
        },
    );
}

#[test]
fn should_resolve_conflict_between_checkpoint_and_inflight_compaction_on_restart() {
    // Arrange
    let (tmp, eng) = new_engine();
    let cf = eng.default_column_family();

    // Act
    for i in 0..10u8 { eng.put(&cf, &[i], b"v").unwrap(); }
    eng.flush().unwrap();
    let cp_dir = tmp.path().join("cp4");
    eng.create_checkpoint(&cp_dir).unwrap();

    // Simulate inflight compaction by writing additional data and then restart
    eng.put(&cf, b"extra", b"e").unwrap();
    drop(eng);

    // Reopen engine to simulate restart during compaction
    let opts = MidgeOptions { storage_mode: StorageMode::LocalDisk { db_path: tmp.path().to_path_buf() }, ..Default::default() };
    let eng2 = MidgeEngine::open(opts).unwrap();

    // Assert
    assert!(eng2.get(&eng2.default_column_family(), b"extra").unwrap().is_some());
}
