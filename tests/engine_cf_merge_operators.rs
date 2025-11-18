// Column Family Merge Operator Tests
// Tests per-CF merge operator registration and resolution

use bytes::Bytes;
use cntryl_midge::{
    api::{
        column_family::ColumnFamilyConfig,
        merge_operator::{IntegerAddOperator, StringAppendOperator},
    },
    MidgeEngine, MidgeOptions, StorageMode,
};
use std::sync::Arc;

mod common;
use common::test_temp_dir;

#[test]
fn should_resolve_merge_using_cf_specific_operator_when_different_operators_registered() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    // Create two CFs with different merge operators
    let cf_numbers = engine
        .create_column_family("numbers", ColumnFamilyConfig::default())
        .expect("create cf_numbers");
    let cf_strings = engine
        .create_column_family("strings", ColumnFamilyConfig::default())
        .expect("create cf_strings");

    // Register IntegerAddOperator for numbers CF
    engine.register_merge_operator(&cf_numbers, Arc::new(IntegerAddOperator));

    // Register StringAppendOperator for strings CF
    engine.register_merge_operator(&cf_strings, Arc::new(StringAppendOperator::new(b",")));

    // Act - Write merges to both CFs
    engine
        .put(&cf_numbers, b"counter", b"10")
        .expect("put number");
    engine
        .merge_cf(&cf_numbers, b"counter", b"5")
        .expect("merge number 1");
    engine
        .merge_cf(&cf_numbers, b"counter", b"3")
        .expect("merge number 2");

    engine
        .put(&cf_strings, b"log", b"first")
        .expect("put string");
    engine
        .merge_cf(&cf_strings, b"log", b"second")
        .expect("merge string 1");
    engine
        .merge_cf(&cf_strings, b"log", b"third")
        .expect("merge string 2");

    // Flush to trigger merge resolution
    engine.flush_cf(&cf_numbers).expect("flush numbers");
    engine.flush_cf(&cf_strings).expect("flush strings");

    // Assert - Each CF uses its own operator
    let number_result = engine.get(&cf_numbers, b"counter").expect("get number");
    assert_eq!(number_result, Some(Bytes::from_static(b"18"))); // 10 + 5 + 3

    let string_result = engine.get(&cf_strings, b"log").expect("get string");
    assert_eq!(
        string_result,
        Some(Bytes::from_static(b"first,second,third"))
    );
}

#[test]
fn should_resolve_merge_correctly_after_flush_when_per_cf_operator_registered() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    let cf = engine
        .create_column_family("test_cf", ColumnFamilyConfig::default())
        .expect("create cf");

    engine.register_merge_operator(&cf, Arc::new(IntegerAddOperator));

    // Act - Write base value and multiple merges
    engine.put(&cf, b"key1", b"100").expect("put");
    engine.merge_cf(&cf, b"key1", b"20").expect("merge 1");
    engine.merge_cf(&cf, b"key1", b"30").expect("merge 2");
    engine.merge_cf(&cf, b"key1", b"50").expect("merge 3");

    // Flush to resolve merges
    engine.flush_cf(&cf).expect("flush");

    // Assert - Value is correctly resolved
    let result = engine.get(&cf, b"key1").expect("get");
    assert_eq!(result, Some(Bytes::from_static(b"200"))); // 100 + 20 + 30 + 50
}

#[test]
fn should_resolve_merge_without_base_value_when_per_cf_operator_registered() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    let cf = engine
        .create_column_family("test_cf", ColumnFamilyConfig::default())
        .expect("create cf");

    engine.register_merge_operator(&cf, Arc::new(IntegerAddOperator));

    // Act - Write only merges (no base value)
    engine.merge_cf(&cf, b"key2", b"10").expect("merge 1");
    engine.merge_cf(&cf, b"key2", b"20").expect("merge 2");
    engine.merge_cf(&cf, b"key2", b"30").expect("merge 3");

    // Flush to resolve merges
    engine.flush_cf(&cf).expect("flush");

    // Assert - Merges are resolved starting from None
    let result = engine.get(&cf, b"key2").expect("get");
    assert_eq!(result, Some(Bytes::from_static(b"60"))); // 0 + 10 + 20 + 30
}

#[test]
fn should_handle_merge_after_delete_when_per_cf_operator_registered() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    let cf = engine
        .create_column_family("test_cf", ColumnFamilyConfig::default())
        .expect("create cf");

    engine.register_merge_operator(&cf, Arc::new(StringAppendOperator::new(b"-")));

    // Act - Write, delete, then merge
    engine.put(&cf, b"key3", b"old").expect("put");
    engine.delete(&cf, b"key3").expect("delete");
    engine.merge_cf(&cf, b"key3", b"new1").expect("merge 1");
    engine.merge_cf(&cf, b"key3", b"new2").expect("merge 2");

    // Flush to resolve merges
    engine.flush_cf(&cf).expect("flush");

    // Assert - Delete terminates merge chain, merges start fresh
    let result = engine.get(&cf, b"key3").expect("get");
    assert_eq!(result, Some(Bytes::from_static(b"new1-new2")));
}

#[test]
fn should_isolate_merge_operators_across_cfs_when_concurrent_flushes() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    let cf_alpha = engine
        .create_column_family("alpha", ColumnFamilyConfig::default())
        .expect("create cf_alpha");
    let cf_beta = engine
        .create_column_family("beta", ColumnFamilyConfig::default())
        .expect("create cf_beta");

    // Register different operators
    engine.register_merge_operator(&cf_alpha, Arc::new(IntegerAddOperator));
    engine.register_merge_operator(&cf_beta, Arc::new(StringAppendOperator::new(b"|")));

    // Act - Write to both CFs
    for i in 0..10 {
        engine
            .merge_cf(&cf_alpha, b"sum", format!("{}", i).as_bytes())
            .expect("merge alpha");
        engine
            .merge_cf(&cf_beta, b"list", format!("item{}", i).as_bytes())
            .expect("merge beta");
    }

    // Flush both CFs
    engine.flush_cf(&cf_alpha).expect("flush alpha");
    engine.flush_cf(&cf_beta).expect("flush beta");

    // Assert - Each CF resolved with correct operator
    let alpha_result = engine.get(&cf_alpha, b"sum").expect("get alpha");
    assert_eq!(alpha_result, Some(Bytes::from_static(b"45"))); // 0+1+2+...+9 = 45

    let beta_result = engine.get(&cf_beta, b"list").expect("get beta");
    assert_eq!(
        beta_result,
        Some(Bytes::from_static(
            b"item0|item1|item2|item3|item4|item5|item6|item7|item8|item9"
        ))
    );
}

#[test]
fn should_handle_default_cf_merge_independently_from_other_cfs() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    let default_cf = engine.default_column_family();
    let custom_cf = engine
        .create_column_family("custom", ColumnFamilyConfig::default())
        .expect("create custom cf");

    // Register operators on both CFs
    engine.register_merge_operator(&default_cf, Arc::new(IntegerAddOperator));
    engine.register_merge_operator(&custom_cf, Arc::new(StringAppendOperator::new(b":")));

    // Act - Write merges to both CFs
    engine
        .put(&default_cf, b"count", b"0")
        .expect("put default");
    engine
        .merge_cf(&default_cf, b"count", b"1")
        .expect("merge default 1");
    engine
        .merge_cf(&default_cf, b"count", b"2")
        .expect("merge default 2");

    engine
        .put(&custom_cf, b"path", b"root")
        .expect("put custom");
    engine
        .merge_cf(&custom_cf, b"path", b"dir")
        .expect("merge custom 1");
    engine
        .merge_cf(&custom_cf, b"path", b"file")
        .expect("merge custom 2");

    // Flush both
    engine.flush_cf(&default_cf).expect("flush default");
    engine.flush_cf(&custom_cf).expect("flush custom");

    // Assert - Each CF uses its own operator
    let default_result = engine.get(&default_cf, b"count").expect("get default");
    assert_eq!(default_result, Some(Bytes::from_static(b"3"))); // 0 + 1 + 2

    let custom_result = engine.get(&custom_cf, b"path").expect("get custom");
    assert_eq!(custom_result, Some(Bytes::from_static(b"root:dir:file")));
}

#[test]
fn should_persist_and_recover_merge_resolutions_across_restart() {
    // Arrange
    let dir = test_temp_dir();
    let db_path = dir.path().to_path_buf();

    {
        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk {
                db_path: db_path.clone(),
            },
            enable_compaction: false,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");

        let cf = engine
            .create_column_family("persist_cf", ColumnFamilyConfig::default())
            .expect("create cf");

        engine.register_merge_operator(&cf, Arc::new(IntegerAddOperator));

        // Act - Write merges and flush
        engine.put(&cf, b"total", b"100").expect("put");
        engine.merge_cf(&cf, b"total", b"25").expect("merge 1");
        engine.merge_cf(&cf, b"total", b"75").expect("merge 2");
        engine.flush_cf(&cf).expect("flush");
    }

    // Reopen database
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path },
        enable_compaction: false,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("reopen");
    // Column families should persist across restarts - if not found, create it
    let cf = engine.get_column_family("persist_cf").or_else(|_| {
        engine.create_column_family("persist_cf", ColumnFamilyConfig::default())
    }).expect("get or create cf");

    // Re-register merge operator (operators are not persisted, must be registered on startup)
    engine.register_merge_operator(&cf, Arc::new(IntegerAddOperator));

    // Assert - Resolved value persists
    let result = engine.get(&cf, b"total").expect("get");
    assert_eq!(result, Some(Bytes::from_static(b"200"))); // 100 + 25 + 75
}
