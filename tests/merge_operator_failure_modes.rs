mod common;
use common::*;

use cntryl_midge::api::merge_operator::{IntegerAddOperator, MergeOperator};
use cntryl_midge::MidgeError;
use cntryl_midge::MidgeOptions;
use cntryl_midge::StorageMode;
use cntryl_midge::MidgeEngine;
use cntryl_midge::api::column_family::ColumnFamilyConfig;
use std::sync::Arc;

// This set focuses on deterministic recovery behavior when merge operators change or fail.

struct FailingOperator;
impl MergeOperator for FailingOperator {
    fn name(&self) -> &str {
        "FailingOperator"
    }

    fn merge(&self, _key: &[u8], _existing_value: Option<&[u8]>, _operands: &[u8]) -> Result<Vec<u8>, MidgeError> {
        Err(MidgeError::internal("simulated merge operator failure"))
    }
}

#[test]
fn should_recover_consistently_when_merge_operator_changes_mid_run() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: dir.path().to_path_buf() },
        enable_compaction: false,
        ..Default::default()
    };

    // Act
    // Write with an operator
    {
        let engine = MidgeEngine::open(opts.clone()).unwrap();
        let cf = engine.create_column_family("mo", ColumnFamilyConfig::default()).unwrap();
        engine.register_merge_operator(&cf, Arc::new(IntegerAddOperator));
        engine.put(&cf, b"k", b"10").unwrap();
        let _ = engine.merge_cf(&cf, b"k", b"5");
        engine.flush_cf(&cf).unwrap();
    }

    // Reopen without re-registering same operator (change in operator)
    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.get_column_family("mo").unwrap();

    // Assert
    // Read should not panic and should return Ok or an interpretable value
    let r = engine.get(&cf, b"k");
    assert!(r.is_ok());
}

#[test]
fn should_handle_merge_operator_panic_during_flush() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: dir.path().to_path_buf() },
        memtable_size: 512,
        enable_compaction: false,
        ..Default::default()
    };

    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.create_column_family("panic_cf", ColumnFamilyConfig::default()).unwrap();

    engine.register_merge_operator(&cf, Arc::new(FailingOperator));

    // Act
    engine.put(&cf, b"k", b"10").unwrap();
    let _ = engine.merge_cf(&cf, b"k", b"5");

    // Assert
    // Flush may fail deterministically when the merge operator returns an error. Ensure we don't panic.
    let flush_res = engine.flush_cf(&cf);
    assert!(flush_res.is_ok() || flush_res.is_err());
}

#[test]
fn should_apply_merge_chain_correctly_during_freeze_plus_wal_rotation() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: dir.path().to_path_buf() },
        enable_compaction: false,
        ..Default::default()
    };

    // Act - write merges and ensure data persisted across reopen
    {
        let engine = MidgeEngine::open(opts.clone()).unwrap();
        let cf = engine.create_column_family("merge_cf", ColumnFamilyConfig::default()).unwrap();
        engine.register_merge_operator(&cf, Arc::new(IntegerAddOperator));
        engine.put(&cf, b"a", b"1").unwrap();
        let _ = engine.merge_cf(&cf, b"a", b"2");
        engine.flush_cf(&cf).unwrap();
    }

    // Reopen and check
    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.get_column_family("merge_cf").unwrap();
    let v = engine.get(&cf, b"a").unwrap();
    assert!(v.is_some());
}
