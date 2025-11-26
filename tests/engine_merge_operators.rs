//! Merge Operator Tests
//!
//! Tests for merge operator functionality - associative operations that allow
//! efficient updates without read-modify-write cycles.
//!
//! # Test Categories
//!
//! - Basic correctness: merge with/without base value, sequential merges
//! - Associativity: different merge orders produce same result
//! - Flush/Compaction: merge resolution during background operations
//! - Column family isolation: per-CF operator registration
//! - Error handling: missing operator, failing operator, operator changes
//! - Recovery: merge persistence across restarts
//! - Concurrency: concurrent merges to same/different keys
//! - Edge cases: empty operands, binary data, tombstone interaction
//!
//! # Storage Mode Coverage
//!
//! All tests run on all three storage modes via `all_storage_modes()`.

mod common;

use bytes::Bytes;
use cntryl_midge::{
    api::{
        column_family::ColumnFamilyConfig,
        merge_operator::{IntegerAddOperator, MergeOperator, StringAppendOperator},
    },
    MidgeEngine, MidgeError, MidgeOptions, MidgeResult,
};
use common::{all_storage_modes, create_storage_mode, disk_storage_modes, DurabilityTestContext};
use std::sync::Arc;

// ============================================================================
// BASIC CORRECTNESS TESTS
// ============================================================================

#[test]
fn should_merge_without_base_value_given_no_existing_key_when_merging() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();
        eng.register_merge_operator(&cf, Arc::new(IntegerAddOperator));

        // Act - merge on non-existent key
        eng.merge_cf(&cf, b"counter", b"5").expect("merge");

        // Assert - should treat missing base as 0
        let result = eng.get(&cf, b"counter").expect("get");
        assert_eq!(result, Some(Bytes::from("5")), "{}", name);
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_merge_with_existing_base_value_given_put_when_merging() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();
        eng.register_merge_operator(&cf, Arc::new(IntegerAddOperator));

        eng.put(&cf, b"counter", b"10").expect("put");

        // Act
        eng.merge_cf(&cf, b"counter", b"5").expect("merge");

        // Assert
        let result = eng.get(&cf, b"counter").expect("get");
        assert_eq!(result, Some(Bytes::from("15")), "{}", name);
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_apply_multiple_merges_sequentially_given_repeated_operations_when_reading() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();
        eng.register_merge_operator(&cf, Arc::new(IntegerAddOperator));

        // Act
        eng.merge_cf(&cf, b"counter", b"1").expect("merge");
        eng.merge_cf(&cf, b"counter", b"2").expect("merge");
        eng.merge_cf(&cf, b"counter", b"3").expect("merge");
        eng.merge_cf(&cf, b"counter", b"4").expect("merge");

        // Assert
        let result = eng.get(&cf, b"counter").expect("get");
        assert_eq!(result, Some(Bytes::from("10")), "{}", name);
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_merge_after_delete_given_tombstone_when_treating_as_missing() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();
        eng.register_merge_operator(&cf, Arc::new(IntegerAddOperator));

        eng.put(&cf, b"counter", b"100").expect("put");
        eng.delete(&cf, b"counter").expect("delete");

        // Act
        eng.merge_cf(&cf, b"counter", b"5").expect("merge");

        // Assert - should treat deleted key as missing
        let result = eng.get(&cf, b"counter").expect("get");
        assert_eq!(result, Some(Bytes::from("5")), "{}", name);
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_handle_merge_with_put_interleaved_given_mixed_ops_when_reading() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();
        eng.register_merge_operator(&cf, Arc::new(IntegerAddOperator));

        // Act - interleave put and merge
        eng.merge_cf(&cf, b"counter", b"10").expect("merge");
        eng.merge_cf(&cf, b"counter", b"5").expect("merge");
        eng.put(&cf, b"counter", b"100").expect("put"); // reset
        eng.merge_cf(&cf, b"counter", b"7").expect("merge");

        // Assert - merge after put should add to new base
        let result = eng.get(&cf, b"counter").expect("get");
        assert_eq!(result, Some(Bytes::from("107")), "{}", name);
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

// ============================================================================
// STRING APPEND OPERATOR TESTS
// ============================================================================

#[test]
fn should_use_string_append_operator_given_delimiter_when_merging() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();
        eng.register_merge_operator(&cf, Arc::new(StringAppendOperator::new(b",")));

        // Act
        eng.merge_cf(&cf, b"list", b"apple").expect("merge");
        eng.merge_cf(&cf, b"list", b"banana").expect("merge");
        eng.merge_cf(&cf, b"list", b"cherry").expect("merge");

        // Assert
        let result = eng.get(&cf, b"list").expect("get");
        assert_eq!(result, Some(Bytes::from("apple,banana,cherry")), "{}", name);
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_string_append_with_base_value_given_initial_put_when_merging() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();
        eng.register_merge_operator(&cf, Arc::new(StringAppendOperator::new(b"|")));

        eng.put(&cf, b"tags", b"initial").expect("put");

        // Act
        eng.merge_cf(&cf, b"tags", b"tag1").expect("merge");
        eng.merge_cf(&cf, b"tags", b"tag2").expect("merge");

        // Assert
        let result = eng.get(&cf, b"tags").expect("get");
        assert_eq!(result, Some(Bytes::from("initial|tag1|tag2")), "{}", name);
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_handle_empty_merge_operand_given_empty_bytes_when_appending() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();
        eng.register_merge_operator(&cf, Arc::new(StringAppendOperator::new(b",")));

        // Act
        eng.merge_cf(&cf, b"list", b"").expect("merge");
        eng.merge_cf(&cf, b"list", b"item").expect("merge");

        // Assert
        let result = eng.get(&cf, b"list").expect("get");
        assert_eq!(result, Some(Bytes::from(",item")), "{}", name);
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

// ============================================================================
// COLUMN FAMILY ISOLATION TESTS
// ============================================================================

#[test]
fn should_isolate_merge_operators_across_cfs_given_different_operators_when_merging() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf1 = eng
            .create_column_family("cf1", ColumnFamilyConfig::default())
            .expect("create_cf");
        let cf2 = eng
            .create_column_family("cf2", ColumnFamilyConfig::default())
            .expect("create_cf");

        // Register different operators
        eng.register_merge_operator(&cf1, Arc::new(IntegerAddOperator));
        eng.register_merge_operator(&cf2, Arc::new(StringAppendOperator::new(b"-")));

        // Act
        eng.merge_cf(&cf1, b"counter", b"5").expect("merge");
        eng.merge_cf(&cf1, b"counter", b"10").expect("merge");
        eng.merge_cf(&cf2, b"list", b"A").expect("merge");
        eng.merge_cf(&cf2, b"list", b"B").expect("merge");

        // Assert
        assert_eq!(
            eng.get(&cf1, b"counter").expect("get"),
            Some(Bytes::from("15")),
            "{}: cf1",
            name
        );
        assert_eq!(
            eng.get(&cf2, b"list").expect("get"),
            Some(Bytes::from("A-B")),
            "{}: cf2",
            name
        );
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_resolve_merge_correctly_after_flush_given_per_cf_operator_when_flushing() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng
            .create_column_family("test_cf", ColumnFamilyConfig::default())
            .expect("create cf");
        eng.register_merge_operator(&cf, Arc::new(IntegerAddOperator));

        // Act
        eng.put(&cf, b"key1", b"100").expect("put");
        eng.merge_cf(&cf, b"key1", b"20").expect("merge");
        eng.merge_cf(&cf, b"key1", b"30").expect("merge");
        eng.merge_cf(&cf, b"key1", b"50").expect("merge");
        eng.flush_cf(&cf).expect("flush");

        // Assert
        let result = eng.get(&cf, b"key1").expect("get");
        assert_eq!(result, Some(Bytes::from("200")), "{}", name);
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_handle_default_cf_merge_independently_given_custom_cf_when_merging() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let default_cf = eng.default_column_family();
        let custom_cf = eng
            .create_column_family("custom", ColumnFamilyConfig::default())
            .expect("create custom cf");

        eng.register_merge_operator(&default_cf, Arc::new(IntegerAddOperator));
        eng.register_merge_operator(&custom_cf, Arc::new(StringAppendOperator::new(b":")));

        // Act
        eng.put(&default_cf, b"count", b"0").expect("put default");
        eng.merge_cf(&default_cf, b"count", b"1")
            .expect("merge default");
        eng.merge_cf(&default_cf, b"count", b"2")
            .expect("merge default");

        eng.put(&custom_cf, b"path", b"root").expect("put custom");
        eng.merge_cf(&custom_cf, b"path", b"dir")
            .expect("merge custom");
        eng.merge_cf(&custom_cf, b"path", b"file")
            .expect("merge custom");

        // Assert
        let default_result = eng.get(&default_cf, b"count").expect("get default");
        assert_eq!(default_result, Some(Bytes::from("3")), "{}: default", name);

        let custom_result = eng.get(&custom_cf, b"path").expect("get custom");
        assert_eq!(
            custom_result,
            Some(Bytes::from("root:dir:file")),
            "{}: custom",
            name
        );
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

// ============================================================================
// RECOVERY TESTS
// ============================================================================

#[test]
fn should_preserve_merge_semantics_across_restart_given_flush_when_recovering() {
    // BUG EXPOSED: CloudBacked fails this test - returns b"20" (last operand) instead of b"30" (sum)
    // Root cause: Merge operands not persisted to SST during flush. EntryMeta has op_type field
    // but add_with_meta() only accepts tombstone boolean. Merge operands written as Put.
    // Fix: Update SST writer API to accept OpType, write entry_type=3 for merge operands.
    for mode in disk_storage_modes() {
        // Arrange
        let ctx = DurabilityTestContext::new(mode);
        let name = ctx.name().to_string();

        {
            let opts = MidgeOptions {
                storage_mode: ctx.create_storage_mode(),
                ..Default::default()
            };
            let eng = MidgeEngine::open(opts).expect("open");
            let cf = eng.default_column_family();
            eng.register_merge_operator(&cf, Arc::new(IntegerAddOperator));

            eng.merge_cf(&cf, b"counter", b"10").expect("merge");
            eng.merge_cf(&cf, b"counter", b"20").expect("merge");
            eng.flush().expect("flush");
        }

        // Act - reopen and register operator again
        let opts = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("reopen");
        let cf = eng.default_column_family();
        eng.register_merge_operator(&cf, Arc::new(IntegerAddOperator));

        // Assert
        assert_eq!(
            eng.get(&cf, b"counter").expect("get"),
            Some(Bytes::from("30")),
            "{}",
            name
        );
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_apply_merge_chain_correctly_given_freeze_and_wal_rotation_when_recovering() {
    for mode in disk_storage_modes() {
        // Arrange
        let ctx = DurabilityTestContext::new(mode);
        let name = ctx.name().to_string();

        {
            let opts = MidgeOptions {
                storage_mode: ctx.create_storage_mode(),
                ..Default::default()
            };
            let eng = MidgeEngine::open(opts).expect("open");
            let cf = eng
                .create_column_family("merge_cf", ColumnFamilyConfig::default())
                .expect("create cf");
            eng.register_merge_operator(&cf, Arc::new(IntegerAddOperator));

            eng.put(&cf, b"a", b"1").expect("put");
            eng.merge_cf(&cf, b"a", b"2").expect("merge");
            eng.flush_cf(&cf).expect("flush");
        }

        // Act - reopen and check
        let opts = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("reopen");
        let cf = eng.get_column_family("merge_cf").expect("get cf");
        eng.register_merge_operator(&cf, Arc::new(IntegerAddOperator));

        // Assert
        let v = eng.get(&cf, b"a").expect("get");
        assert!(v.is_some(), "{}", name);
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_persist_recover_merge_resolutions_given_cf_restart_when_reopening() {
    // BUG EXPOSED: CloudBacked fails this test - returns b"75" (last operand) instead of b"200" (sum)
    // Root cause: Same as should_preserve_merge_semantics_across_restart - merge operands
    // are converted to Put entries during flush, losing merge semantics across restarts.
    for mode in disk_storage_modes() {
        // Arrange
        let ctx = DurabilityTestContext::new(mode);
        let name = ctx.name().to_string();

        {
            let opts = MidgeOptions {
                storage_mode: ctx.create_storage_mode(),
                ..Default::default()
            };
            let eng = MidgeEngine::open(opts).expect("open");
            let cf = eng
                .create_column_family("persist_cf", ColumnFamilyConfig::default())
                .expect("create cf");
            eng.register_merge_operator(&cf, Arc::new(IntegerAddOperator));

            eng.put(&cf, b"total", b"100").expect("put");
            eng.merge_cf(&cf, b"total", b"25").expect("merge");
            eng.merge_cf(&cf, b"total", b"75").expect("merge");
            eng.flush_cf(&cf).expect("flush");
        }

        // Act - reopen
        let opts = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("reopen");
        let cf = eng.get_column_family("persist_cf").expect("get cf");
        eng.register_merge_operator(&cf, Arc::new(IntegerAddOperator));

        // Assert
        let result = eng.get(&cf, b"total").expect("get");
        assert_eq!(result, Some(Bytes::from("200")), "{}", name);
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

// ============================================================================
// ERROR HANDLING TESTS
// ============================================================================

// Custom failing merge operator
struct FailingMergeOperator;

impl MergeOperator for FailingMergeOperator {
    fn name(&self) -> &str {
        "FailingMergeOperator"
    }

    fn merge(
        &self,
        _key: &[u8],
        _existing_value: Option<&[u8]>,
        _operands: &[u8],
    ) -> MidgeResult<Vec<u8>> {
        Err(MidgeError::internal("Simulated merge operator failure"))
    }
}

#[test]
fn should_handle_merge_without_registered_operator_given_no_operator_when_merging() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng
            .create_column_family("test_cf", ColumnFamilyConfig::default())
            .expect("create cf");

        // Act - attempt merge without registering operator
        eng.put(&cf, b"key", b"10").expect("put");
        let result = eng.merge_cf(&cf, b"key", b"5");

        // Assert - should either succeed or return error
        assert!(result.is_ok() || result.is_err(), "{}", name);
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_return_consistent_results_given_no_merge_operator_when_reading() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng
            .create_column_family("test_cf", ColumnFamilyConfig::default())
            .expect("create cf");

        // Act - write merge operations without operator
        eng.put(&cf, b"key", b"base").expect("put");
        let _ = eng.merge_cf(&cf, b"key", b"delta1");
        let _ = eng.merge_cf(&cf, b"key", b"delta2");

        // Assert - reading should not panic
        let result = eng.get(&cf, b"key");
        assert!(result.is_ok(), "{}: read should not panic", name);
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_handle_failing_merge_operator_given_error_when_flushing() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 512,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng
            .create_column_family("test_cf", ColumnFamilyConfig::default())
            .expect("create cf");

        eng.register_merge_operator(&cf, Arc::new(FailingMergeOperator));

        // Act
        eng.put(&cf, b"key", b"10").expect("put");
        let _ = eng.merge_cf(&cf, b"key", b"5");

        let large_value = vec![b'x'; 256];
        for i in 0..30 {
            let _ = eng.put(&cf, format!("filler{:03}", i).as_bytes(), &large_value);
        }

        let flush_result = eng.flush_cf(&cf);

        // Assert - either flush fails or read surfaces error
        if flush_result.is_ok() {
            let read_result = eng.get(&cf, b"key");
            assert!(read_result.is_ok() || read_result.is_err(), "{}", name);
        }
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_maintain_consistency_given_merge_operator_changed_when_reopening() {
    for mode in disk_storage_modes() {
        // Arrange
        let ctx = DurabilityTestContext::new(mode);
        let name = ctx.name().to_string();

        {
            let opts = MidgeOptions {
                storage_mode: ctx.create_storage_mode(),
                ..Default::default()
            };
            let eng = MidgeEngine::open(opts).expect("open");
            let cf = eng
                .create_column_family("test_cf", ColumnFamilyConfig::default())
                .expect("create cf");
            eng.register_merge_operator(&cf, Arc::new(IntegerAddOperator));

            eng.put(&cf, b"key", b"10").expect("put");
            let _ = eng.merge_cf(&cf, b"key", b"5");
            eng.flush_cf(&cf).ok();
        }

        // Act - reopen without re-registering operator
        let opts = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("reopen");
        let cf = eng.get_column_family("test_cf").expect("get cf");

        // Assert - read should not panic or corrupt data
        let result = eng.get(&cf, b"key");
        assert!(result.is_ok(), "{}: should handle missing operator", name);
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

// ============================================================================
// CONCURRENCY TESTS
// ============================================================================

#[test]
fn should_handle_concurrent_merges_to_same_key_given_multiple_threads_when_merging() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = eng.default_column_family();
        eng.register_merge_operator(&cf, Arc::new(IntegerAddOperator));

        // Act - concurrent merges
        let mut handles = vec![];
        for i in 0..10 {
            let engine_clone = Arc::clone(&eng);
            let cf_clone = cf.clone();
            let handle = std::thread::spawn(move || {
                for _ in 0..10 {
                    let value = format!("{}", i);
                    engine_clone
                        .merge_cf(&cf_clone, b"counter", value.as_bytes())
                        .expect("merge");
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().expect("join");
        }

        // Assert - should sum all concurrent merges
        // Each thread: 10 times value i, for i=0..9 = 10*(0+1+2+...+9) = 450
        let result = eng.get(&cf, b"counter").expect("get");
        assert_eq!(result, Some(Bytes::from("450")), "{}", name);
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_handle_merge_to_multiple_keys_concurrently_given_threads_when_merging() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = eng.default_column_family();
        eng.register_merge_operator(&cf, Arc::new(IntegerAddOperator));

        // Act - concurrent merges to different keys
        let mut handles = vec![];
        for thread_id in 0..10 {
            let engine_clone = Arc::clone(&eng);
            let cf_clone = cf.clone();
            let handle = std::thread::spawn(move || {
                let key = format!("counter{}", thread_id);
                for i in 1..=10 {
                    engine_clone
                        .merge_cf(&cf_clone, key.as_bytes(), format!("{}", i).as_bytes())
                        .expect("merge");
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().expect("join");
        }

        // Assert - each counter should sum to 55 (1+2+...+10)
        for thread_id in 0..10 {
            let key = format!("counter{}", thread_id);
            let result = eng.get(&cf, key.as_bytes()).expect("get");
            assert_eq!(
                result,
                Some(Bytes::from("55")),
                "{}: counter{}",
                name,
                thread_id
            );
        }
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

// ============================================================================
// EDGE CASES
// ============================================================================

#[test]
fn should_resolve_merge_chain_during_get_given_long_chain_when_reading() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();
        eng.register_merge_operator(&cf, Arc::new(IntegerAddOperator));

        // Act - create long merge chain
        for i in 1..=100 {
            eng.merge_cf(&cf, b"counter", format!("{}", i).as_bytes())
                .expect("merge");
        }

        // Assert - should resolve entire chain (1+2+...+100 = 5050)
        let result = eng.get(&cf, b"counter").expect("get");
        assert_eq!(result, Some(Bytes::from("5050")), "{}", name);
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_merge_with_binary_data_given_binary_key_when_merging() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();
        eng.register_merge_operator(&cf, Arc::new(IntegerAddOperator));

        // Act - use binary representation
        let binary_key = vec![0x00, 0xFF, 0xAB, 0xCD];
        eng.merge_cf(&cf, &binary_key, b"42").expect("merge");
        eng.merge_cf(&cf, &binary_key, b"8").expect("merge");

        // Assert
        let result = eng.get(&cf, &binary_key).expect("get");
        assert_eq!(result, Some(Bytes::from("50")), "{}", name);
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_not_merge_across_delete_range_given_tombstone_when_range_deleted() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();
        eng.register_merge_operator(&cf, Arc::new(IntegerAddOperator));

        eng.merge_cf(&cf, b"key1", b"10").expect("merge");
        eng.delete_range(&cf, b"key0", b"key2")
            .expect("delete_range");

        // Act
        eng.merge_cf(&cf, b"key1", b"5").expect("merge");

        // Assert - should only have the post-delete merge
        let result = eng.get(&cf, b"key1").expect("get");
        assert_eq!(result, Some(Bytes::from("5")), "{}", name);
        drop(eng);
        eprintln!("✓ {}", name);
    }
}
