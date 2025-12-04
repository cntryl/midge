//! Merge Operator Tests
//!
//! Tests for merge operator functionality - associative operations that allow
//! efficient updates without read-modify-write cycles.
//!
//! # Test Categories
//!
//! - Basic correctness: merge with/without base value, sequential merges
//! - Associativity: different merge orders produce same result
//! - Flush/Compaction: merge resolution during writes to SST
//! - Column family isolation: per-CF operator registration
//! - Error handling: missing operator, failing operator, operator changes
//! - Recovery: merge persistence across restarts
//! - Concurrency: concurrent merges to same/different keys
//! - Edge cases: empty operands, binary data, tombstone interaction
//!
//! # Storage Mode Coverage
//!
//! All tests run on all storage modes via `all_storage_modes()` /
//! `disk_storage_modes()` from `common`.

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
// TEST-ONLY MERGE OPERATORS
// ============================================================================

/// Test merge operator that counts how many merge operations have been
/// applied for a key. The value is stored as UTF-8 "count=N".
struct CollectOperandsOperator;

impl MergeOperator for CollectOperandsOperator {
    fn name(&self) -> &str {
        "CollectOperandsOperator"
    }

    fn merge(
        &self,
        _key: &[u8],
        existing_value: Option<&[u8]>,
        _operands: &[u8],
    ) -> MidgeResult<Vec<u8>> {
        let base = existing_value
            .map(|v| std::str::from_utf8(v).unwrap())
            .map(|s| s.strip_prefix("count=").unwrap().parse::<usize>().unwrap())
            .unwrap_or(0);

        // In the current engine, each merge_cf call is a single logical operand.
        // We just bump the count by 1 per merge invocation.
        let next = base + 1;
        Ok(format!("count={next}").into_bytes())
    }
}

/// Merge operator that always fails. Used to verify error propagation.
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
        Err(MidgeError::internal("simulated merge operator failure"))
    }
}

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

        // Act
        eng.merge_cf(&cf, b"counter", b"5").expect("merge");

        // Assert
        let result = eng.get(&cf, b"counter").expect("get");
        assert_eq!(result, Some(Bytes::from("5")), "{}", name);
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

        // Assert
        let result = eng.get(&cf, b"counter").expect("get");
        assert_eq!(result, Some(Bytes::from("5")), "{}", name);
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

        // Act
        eng.merge_cf(&cf, b"counter", b"10").expect("merge");
        eng.merge_cf(&cf, b"counter", b"5").expect("merge");
        eng.put(&cf, b"counter", b"100").expect("put");
        eng.merge_cf(&cf, b"counter", b"7").expect("merge");

        // Assert
        let result = eng.get(&cf, b"counter").expect("get");
        assert_eq!(result, Some(Bytes::from("107")), "{}", name);
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
            .expect("create cf1");
        let cf2 = eng
            .create_column_family("cf2", ColumnFamilyConfig::default())
            .expect("create cf2");

        eng.register_merge_operator(&cf1, Arc::new(IntegerAddOperator));
        eng.register_merge_operator(&cf2, Arc::new(StringAppendOperator::new(b"-")));

        // Act
        eng.merge_cf(&cf1, b"counter", b"5").expect("merge");
        eng.merge_cf(&cf1, b"counter", b"10").expect("merge");
        eng.merge_cf(&cf2, b"list", b"A").expect("merge");
        eng.merge_cf(&cf2, b"list", b"B").expect("merge");

        // Assert
        let cf1_result = eng.get(&cf1, b"counter").expect("get cf1");
        let cf2_result = eng.get(&cf2, b"list").expect("get cf2");

        assert_eq!(cf1_result, Some(Bytes::from("15")), "{}: cf1", name);
        assert_eq!(cf2_result, Some(Bytes::from("A-B")), "{}: cf2", name);
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
        let custom_result = eng.get(&custom_cf, b"path").expect("get custom");

        assert_eq!(default_result, Some(Bytes::from("3")), "{}: default", name);
        assert_eq!(
            custom_result,
            Some(Bytes::from("root:dir:file")),
            "{}: custom",
            name
        );
    }
}

// ============================================================================
// RECOVERY TESTS
// ============================================================================

#[test]
fn should_preserve_merge_semantics_across_restart_given_flush_when_recovering() {
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

        // Act
        let opts = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("reopen");
        let cf = eng.default_column_family();
        eng.register_merge_operator(&cf, Arc::new(IntegerAddOperator));

        // Assert
        let result = eng.get(&cf, b"counter").expect("get");
        assert_eq!(result, Some(Bytes::from("30")), "{}", name);
    }
}

#[test]
fn should_persist_merge_resolutions_given_cf_restart_when_reopening() {
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

        // Act
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
    }
}

// ============================================================================
// ERROR HANDLING TESTS
// ============================================================================

#[test]
fn should_error_when_merging_without_registered_operator_given_no_operator_when_merging() {
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

        // Act
        eng.put(&cf, b"key", b"10").expect("put");
        let result = eng.merge_cf(&cf, b"key", b"5");

        // Assert
        assert!(
            result.is_err(),
            "{}: merge without operator should error",
            name
        );
    }
}

#[test]
fn should_surface_error_given_failing_merge_operator_when_getting() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng
            .create_column_family("fail_cf", ColumnFamilyConfig::default())
            .expect("create cf");

        eng.register_merge_operator(&cf, Arc::new(FailingMergeOperator));

        eng.put(&cf, b"key", b"10").expect("put");
        eng.merge_cf(&cf, b"key", b"5").expect("merge");

        // Act
        let result = eng.get(&cf, b"key");

        // Assert
        assert!(
            result.is_err(),
            "{}: failing operator should surface error on get",
            name
        );
    }
}

#[test]
fn should_keep_data_readable_given_merge_operator_changed_across_restart_when_reopening() {
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
            eng.merge_cf(&cf, b"key", b"5").expect("merge");
            eng.flush_cf(&cf).ok();
        }

        // Act
        let opts = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("reopen");
        let cf = eng.get_column_family("test_cf").expect("get cf");
        // NOTE: deliberately do not re-register the merge operator.

        let result = eng.get(&cf, b"key");

        // Assert
        assert!(
            result.is_ok(),
            "{}: should not panic if operator changed/missing after restart",
            name
        );
    }
}

// ============================================================================
// CONCURRENCY TESTS (Option 1: memtable-only, no flush, no compaction dependency)
// ============================================================================

#[test]
fn should_not_lose_merge_operands_under_concurrency_given_same_key_when_merging() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        // Big memtable to avoid flush; rely on in-memory state only.
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 32 * 1024 * 1024,
            ..Default::default()
        };
        let eng = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = eng.default_column_family();
        eng.register_merge_operator(&cf, Arc::new(CollectOperandsOperator));

        let threads = 10;
        let merges_per_thread = 10;

        // Act
        let mut handles = Vec::new();
        for _ in 0..threads {
            let eng_cloned = Arc::clone(&eng);
            let cf_cloned = cf.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..merges_per_thread {
                    eng_cloned
                        .merge_cf(&cf_cloned, b"counter", b"x")
                        .expect("merge");
                }
            }));
        }

        for h in handles {
            h.join().expect("join");
        }

        // Assert
        let result = eng.get(&cf, b"counter").expect("get");
        let value = result.expect("value");
        let text = std::str::from_utf8(&value).expect("utf8");
        let count: usize = text.strip_prefix("count=").unwrap().parse().unwrap();

        assert_eq!(
            count,
            threads * merges_per_thread,
            "{}: lost merge operands under concurrency",
            name
        );
    }
}

#[test]
fn should_handle_concurrent_merges_to_same_key_given_integer_add_operator_when_merging() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        // Big memtable to keep everything in-memory.
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 32 * 1024 * 1024,
            ..Default::default()
        };
        let eng = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = eng.default_column_family();
        eng.register_merge_operator(&cf, Arc::new(IntegerAddOperator));

        let threads = 10;
        let merges_per_thread = 10;

        // Act
        let mut handles = Vec::new();
        for i in 0..threads {
            let eng_cloned = Arc::clone(&eng);
            let cf_cloned = cf.clone();
            handles.push(std::thread::spawn(move || {
                let value = i.to_string();
                for _ in 0..merges_per_thread {
                    eng_cloned
                        .merge_cf(&cf_cloned, b"counter", value.as_bytes())
                        .expect("merge");
                }
            }));
        }

        for h in handles {
            h.join().expect("join");
        }

        // Assert
        // Sum = merges_per_thread * (0 + 1 + ... + (threads - 1))
        let expected_sum = merges_per_thread * (threads * (threads - 1) / 2);
        let result = eng.get(&cf, b"counter").expect("get");
        assert_eq!(
            result,
            Some(Bytes::from(expected_sum.to_string())),
            "{}: concurrent merges lost updates",
            name
        );
    }
}

#[test]
fn should_handle_merge_to_multiple_keys_concurrently_given_distinct_keys_when_merging() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        // Big memtable to avoid flush.
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 32 * 1024 * 1024,
            ..Default::default()
        };
        let eng = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = eng.default_column_family();
        eng.register_merge_operator(&cf, Arc::new(IntegerAddOperator));

        let threads = 10;
        let merges_per_thread = 10;

        // Act
        let mut handles = Vec::new();
        for thread_id in 0..threads {
            let eng_cloned = Arc::clone(&eng);
            let cf_cloned = cf.clone();
            handles.push(std::thread::spawn(move || {
                let key = format!("counter{}", thread_id);
                for i in 1..=merges_per_thread {
                    eng_cloned
                        .merge_cf(&cf_cloned, key.as_bytes(), i.to_string().as_bytes())
                        .expect("merge");
                }
            }));
        }

        for h in handles {
            h.join().expect("join");
        }

        // Assert
        let per_key_expected = merges_per_thread * (merges_per_thread + 1) / 2;
        for thread_id in 0..threads {
            let key = format!("counter{}", thread_id);
            let result = eng.get(&cf, key.as_bytes()).expect("get");
            assert_eq!(
                result,
                Some(Bytes::from(per_key_expected.to_string())),
                "{}: {}",
                name,
                key
            );
        }
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

        // Act
        for i in 1..=100 {
            eng.merge_cf(&cf, b"counter", i.to_string().as_bytes())
                .expect("merge");
        }

        // Assert (1 + 2 + ... + 100 = 5050)
        let result = eng.get(&cf, b"counter").expect("get");
        assert_eq!(result, Some(Bytes::from("5050")), "{}", name);
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

        let binary_key = vec![0x00, 0xFF, 0xAB, 0xCD];

        // Act
        eng.merge_cf(&cf, &binary_key, b"42").expect("merge");
        eng.merge_cf(&cf, &binary_key, b"8").expect("merge");

        // Assert
        let result = eng.get(&cf, &binary_key).expect("get");
        assert_eq!(result, Some(Bytes::from("50")), "{}", name);
    }
}

#[test]
fn should_not_merge_across_delete_range_given_range_tombstone_when_merging() {
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

        // Assert - only the post-delete merge should be visible
        let result = eng.get(&cf, b"key1").expect("get");
        assert_eq!(result, Some(Bytes::from("5")), "{}", name);
    }
}
