// Checkpoint Operations (Phase 3 - P2)
// Tests checkpoint creation, consistency, and recovery

#![allow(clippy::field_reassign_with_default)]
mod common;
use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use common::test_temp_dir;

#[test]
fn should_maintain_consistency_given_checkpoint_during_writes_when_created() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).unwrap();
    let cf = eng.default_column_family();

    eng.put(&cf, b"key1", b"val1").unwrap();
    eng.put(&cf, b"key2", b"val2").unwrap();
    eng.flush().unwrap();

    // Act - checkpoint
    let checkpoint_path = dir.path().join("checkpoint1");
    eng.create_checkpoint(&checkpoint_path).unwrap();

    // Write more data after checkpoint
    eng.put(&cf, b"key3", b"val3").unwrap();

    // Assert - checkpoint should have consistent state (key1, key2 only)
    let ckpt_opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: checkpoint_path,
        },
        ..Default::default()
    };
    let ckpt_eng = MidgeEngine::open(ckpt_opts).unwrap();
    let ckpt_cf = ckpt_eng.default_column_family();

    assert!(ckpt_eng.get(&ckpt_cf, b"key1").unwrap().is_some());
    assert!(ckpt_eng.get(&ckpt_cf, b"key2").unwrap().is_some());
    assert!(ckpt_eng.get(&ckpt_cf, b"key3").unwrap().is_none());
}

#[test]
fn should_recover_given_crash_mid_checkpoint_when_incomplete() {
    // Would test that incomplete checkpoints don't corrupt DB
}

#[test]
fn should_verify_integrity_given_checkpoint_restored_when_validating() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).unwrap();
    let cf = eng.default_column_family();

    // Write deterministic data
    for i in 0..100 {
        let key = format!("key{:03}", i);
        let val = format!("val{:03}", i);
        eng.put(&cf, key.as_bytes(), val.as_bytes()).unwrap();
    }
    eng.flush().unwrap();

    // Act - checkpoint and restore
    let checkpoint_path = dir.path().join("checkpoint_verify");
    eng.create_checkpoint(&checkpoint_path).unwrap();

    let ckpt_opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: checkpoint_path,
        },
        ..Default::default()
    };
    let ckpt_eng = MidgeEngine::open(ckpt_opts).unwrap();
    let ckpt_cf = ckpt_eng.default_column_family();

    // Assert - all data intact
    for i in 0..100 {
        let key = format!("key{:03}", i);
        let expected = format!("val{:03}", i);
        assert_eq!(
            ckpt_eng.get(&ckpt_cf, key.as_bytes()).unwrap().unwrap(),
            Bytes::from(expected)
        );
    }
}

#[test]
fn should_create_incremental_checkpoint_given_previous_checkpoint_when_supported() {
    // Would test incremental checkpoint functionality if implemented
}

#[test]
fn should_isolate_checkpoint_given_original_modified_when_independent() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).unwrap();
    let cf = eng.default_column_family();

    eng.put(&cf, b"key1", b"val1").unwrap();
    eng.flush().unwrap();

    let checkpoint_path = dir.path().join("checkpoint_isolated");
    eng.create_checkpoint(&checkpoint_path).unwrap();

    // Act - modify original
    eng.put(&cf, b"key1", b"modified").unwrap();
    eng.delete(&cf, b"key1").unwrap();
    eng.flush().unwrap();

    // Assert - checkpoint unchanged
    let ckpt_opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: checkpoint_path,
        },
        ..Default::default()
    };
    let ckpt_eng = MidgeEngine::open(ckpt_opts).unwrap();
    let ckpt_cf = ckpt_eng.default_column_family();

    assert_eq!(
        ckpt_eng.get(&ckpt_cf, b"key1").unwrap().unwrap(),
        Bytes::from("val1")
    );
}

#[test]
fn should_create_multiple_checkpoints_given_sequential_creation_when_requested() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).unwrap();
    let cf = eng.default_column_family();

    eng.put(&cf, b"key", b"v1").unwrap();
    eng.flush().unwrap();
    let ckpt1 = dir.path().join("ckpt1");
    eng.create_checkpoint(&ckpt1).unwrap();

    eng.put(&cf, b"key", b"v2").unwrap();
    eng.flush().unwrap();
    let ckpt2 = dir.path().join("ckpt2");
    eng.create_checkpoint(&ckpt2).unwrap();

    eng.put(&cf, b"key", b"v3").unwrap();
    eng.flush().unwrap();
    let ckpt3 = dir.path().join("ckpt3");
    eng.create_checkpoint(&ckpt3).unwrap();

    // Act - open all checkpoints
    let eng1 = MidgeEngine::open(MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: ckpt1 },
        ..Default::default()
    })
    .unwrap();
    let eng2 = MidgeEngine::open(MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: ckpt2 },
        ..Default::default()
    })
    .unwrap();
    let eng3 = MidgeEngine::open(MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: ckpt3 },
        ..Default::default()
    })
    .unwrap();

    let cf1 = eng1.default_column_family();
    let cf2 = eng2.default_column_family();
    let cf3 = eng3.default_column_family();

    // Assert - each checkpoint has correct version
    assert_eq!(eng1.get(&cf1, b"key").unwrap().unwrap(), Bytes::from("v1"));
    assert_eq!(eng2.get(&cf2, b"key").unwrap().unwrap(), Bytes::from("v2"));
    assert_eq!(eng3.get(&cf3, b"key").unwrap().unwrap(), Bytes::from("v3"));
}

#[test]
fn should_checkpoint_empty_db_given_no_data_when_created() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).unwrap();

    // Act - checkpoint empty DB
    let checkpoint_path = dir.path().join("empty_checkpoint");
    eng.create_checkpoint(&checkpoint_path).unwrap();

    // Assert - can open empty checkpoint
    let ckpt_opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: checkpoint_path,
        },
        ..Default::default()
    };
    let ckpt_eng = MidgeEngine::open(ckpt_opts).unwrap();
    let ckpt_cf = ckpt_eng.default_column_family();

    assert!(ckpt_eng.get(&ckpt_cf, b"any_key").unwrap().is_none());
}

#[test]
fn should_include_all_column_families_given_checkpoint_when_multiple_cfs() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).unwrap();

    use cntryl_midge::ColumnFamilyConfig;
    let cf1 = eng.default_column_family();
    let cf2 = eng
        .create_column_family("cf2", ColumnFamilyConfig::default())
        .unwrap();

    eng.put(&cf1, b"key_default", b"val_default").unwrap();
    eng.put(&cf2, b"key_cf2", b"val_cf2").unwrap();
    eng.flush().unwrap();

    // Act - checkpoint
    let checkpoint_path = dir.path().join("multi_cf_checkpoint");
    eng.create_checkpoint(&checkpoint_path).unwrap();

    // Assert - both CFs in checkpoint
    let ckpt_opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: checkpoint_path,
        },
        ..Default::default()
    };
    let ckpt_eng = MidgeEngine::open(ckpt_opts).unwrap();

    let all_cfs = ckpt_eng.list_column_families();
    assert!(all_cfs.iter().any(|cf| cf.name() == "default"));
    assert!(all_cfs.iter().any(|cf| cf.name() == "cf2"));
}
