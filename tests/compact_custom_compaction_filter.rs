// Custom Compaction Filter
// Extracted from compaction_concurrent.rs

mod common;

use cntryl_midge::compaction::{CompactionFilter, CompactionVersion, FilterDecision};
use common::{bulk_put, create_storage_mode};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

// ============================================================================

// Custom filter that counts how many times it's invoked
struct CountingFilter {
    count: Arc<AtomicUsize>,
}

impl CountingFilter {
    fn new(count: Arc<AtomicUsize>) -> Self {
        Self { count }
    }
}

impl CompactionFilter for CountingFilter {
    fn filter(&self, _level: u32, _version: &CompactionVersion) -> FilterDecision {
        self.count.fetch_add(1, Ordering::Relaxed);
        FilterDecision::Keep
    }
}

#[test]
fn should_invoke_filter_for_each_key_given_compaction_with_custom_filter() {
    for mode in common::disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = cntryl_midge::MidgeOptions {
            storage_mode,
            memtable_size: 512,
            enable_compaction: true,
            ..Default::default()
        };
        let eng = cntryl_midge::MidgeEngine::open(opts).expect("open engine");
        let cf = eng.default_column_family();

        let invocation_count = Arc::new(AtomicUsize::new(0));
        let filter = CountingFilter::new(invocation_count.clone());
        eng.set_compaction_filter(&cf, Arc::new(filter))
            .expect("set filter");

        // Write keys to trigger compaction
        bulk_put(&eng, &cf, "key_", 50, b"value");

        // Act
        eng.flush_cf(&cf).expect("flush");
        eng.compact_all().expect("compact");

        // Assert
        // The filter should have been invoked for each key during compaction
        let invocations = invocation_count.load(Ordering::Relaxed);
        assert!(
            invocations >= 50,
            "Filter should have been invoked for each key (50), got {}",
            invocations
        );

        // Verify data is still accessible after filtered compaction
        let result = eng.get(&cf, b"key_000").expect("get failed");
        assert!(
            result.is_some(),
            "Data should be present after filtered compaction"
        );
    }
}

// ============================================================================

// Custom filter that removes keys with a specific prefix
struct PrefixRemovalFilter {
    prefix: Vec<u8>,
}

impl PrefixRemovalFilter {
    fn new(prefix: &[u8]) -> Self {
        Self {
            prefix: prefix.to_vec(),
        }
    }
}

impl CompactionFilter for PrefixRemovalFilter {
    fn filter(&self, _level: u32, version: &CompactionVersion) -> FilterDecision {
        if version.user_key.starts_with(&self.prefix) {
            FilterDecision::Remove
        } else {
            FilterDecision::Keep
        }
    }
}

#[test]
fn should_drop_key_given_filter_returns_remove_decision() {
    for mode in common::disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = cntryl_midge::MidgeOptions {
            storage_mode,
            memtable_size: 512,
            enable_compaction: true,
            ..Default::default()
        };
        let eng = cntryl_midge::MidgeEngine::open(opts).expect("open engine");
        let cf = eng.default_column_family();

        let filter = PrefixRemovalFilter::new(b"remove_");
        eng.set_compaction_filter(&cf, Arc::new(filter))
            .expect("set filter");

        // Write keys with different prefixes (bulk_put generates keys with {:03} format)
        bulk_put(&eng, &cf, "keep_", 10, b"value");
        bulk_put(&eng, &cf, "remove_", 10, b"value");

        // Act
        eng.flush_cf(&cf).expect("flush");
        eng.compact_all().expect("compact");

        // Assert
        // Kept keys should still exist
        let result = eng.get(&cf, b"keep_000").expect("get failed");
        assert!(result.is_some(), "Kept keys should survive compaction");

        // Removed keys should be gone after compaction
        let result = eng.get(&cf, b"remove_000").expect("get failed");
        assert!(
            result.is_none(),
            "Keys with 'remove_' prefix should be filtered out after compaction"
        );

        let result = eng.get(&cf, b"remove_005").expect("get failed");
        assert!(
            result.is_none(),
            "All keys with 'remove_' prefix should be filtered out after compaction"
        );
    }
}

// ============================================================================

// Custom filter that explicitly keeps all keys (useful for testing)
struct KeepAllFilter;

impl KeepAllFilter {
    fn new() -> Self {
        Self
    }
}

impl CompactionFilter for KeepAllFilter {
    fn filter(&self, _level: u32, _version: &CompactionVersion) -> FilterDecision {
        FilterDecision::Keep
    }
}

#[test]
fn should_keep_key_given_filter_returns_keep_decision() {
    for mode in common::disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = cntryl_midge::MidgeOptions {
            storage_mode,
            memtable_size: 512,
            enable_compaction: true,
            ..Default::default()
        };
        let eng = cntryl_midge::MidgeEngine::open(opts).expect("open engine");
        let cf = eng.default_column_family();

        let filter = KeepAllFilter::new();
        eng.set_compaction_filter(&cf, Arc::new(filter))
            .expect("set filter");

        // Write data
        bulk_put(&eng, &cf, "key_", 30, b"important_data");

        // Act
        eng.flush_cf(&cf).expect("flush");
        eng.compact_all().expect("compact");

        // Assert
        // All keys should still exist after compaction (bulk_put uses {:03} format)
        for i in 0..30 {
            let key = format!("key_{:03}", i);
            let result = eng.get(&cf, key.as_bytes()).expect("get failed");
            assert!(
                result.is_some(),
                "All keys should be kept by filter, missing: {}",
                key
            );
        }
    }
}

// NOTE: Value modification during compaction is intentionally not supported.
//
// Reasons:
// 1. Breaks LSM semantics - compaction should preserve logical state
// 2. Complexity - would require FilterDecision::Change(Bytes) variant
// 3. Performance - modifying values during compaction adds overhead
// 4. Alternative - use merge operators for value transformations
//
// If you need to transform values, use one of these approaches:
// - Merge operators (designed for value aggregation/transformation)
// - Application-level rewriting (read-modify-write cycle)
// - Background job that rewrites keys outside compaction
//
// Compaction filters are for *removing* data (GC, TTL, privacy), not transforming it.
