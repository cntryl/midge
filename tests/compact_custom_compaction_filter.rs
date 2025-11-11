// Custom Compaction Filter
// Extracted from compaction_concurrent.rs

mod common;

use cntryl_midge::compaction::{CompactionFilter, CompactionVersion, FilterDecision};
use common::{bulk_put, new_engine_with_opts};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

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
    // Arrange
    let (_dir, eng) = new_engine_with_opts(512, true);
    let cf = eng.default_column_family();

    let invocation_count = Arc::new(AtomicUsize::new(0));
    let filter = CountingFilter::new(invocation_count.clone());
    eng.set_compaction_filter(&cf, Arc::new(filter))
        .expect("set filter");

    // Write keys to trigger compaction
    bulk_put(&eng, &cf, "key_", 50, b"value");

    // Act - flush and compact (both are synchronous/blocking operations - no sleeps needed!)
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
    assert!(result.is_some(), "Data should be present after filtered compaction");
}

#[test]
#[ignore = "TODO: Implement PrefixRemovalFilter example"]
fn should_drop_key_given_filter_returns_remove_decision() {
    // Arrange
    let (_dir, eng) = new_engine_with_opts(512, true);
    let cf = eng.default_column_family();

    // TODO: Create a CompactionFilter that removes keys with specific prefix
    // let filter = PrefixRemovalFilter::new(b"remove_");
    // eng.set_compaction_filter(&cf, filter);

    // Write keys with different prefixes
    bulk_put(&eng, &cf, "keep_", 10, b"value");
    bulk_put(&eng, &cf, "remove_", 10, b"value");

    // Act
    // TODO: Add explicit compaction trigger API
    thread::sleep(Duration::from_millis(500)); // Allow compaction

    // Assert
    // Kept keys should still exist
    let result = eng.get(&cf, b"keep_00").expect("get failed");
    assert!(result.is_some(), "Kept keys should survive compaction");

    // TODO: Assert removed keys are gone (requires filter implementation)
    // assert_key_absent(&eng, &cf, b"remove_00");
    let result = eng.get(&cf, b"remove_00").expect("get failed");
    assert!(result.is_some(), "Keys will be present until filter is implemented");
}

#[test]
#[ignore = "TODO: Implement KeepAllFilter example"]
fn should_keep_key_given_filter_returns_keep_decision() {
    // Arrange
    let (_dir, eng) = new_engine_with_opts(512, true);
    let cf = eng.default_column_family();

    // TODO: Create a CompactionFilter that keeps all keys
    // let filter = KeepAllFilter::new();
    // eng.set_compaction_filter(&cf, filter);

    // Write data
    bulk_put(&eng, &cf, "key_", 30, b"important_data");

    // Act
    // TODO: Add explicit compaction trigger API
    thread::sleep(Duration::from_millis(500)); // Allow compaction

    // Assert
    // All keys should still exist after compaction
    for i in 0..30 {
        let key = format!("key_{:02}", i);
        let result = eng.get(&cf, key.as_bytes()).expect("get failed");
        assert!(result.is_some(), "All keys should be kept by filter");
    }
}

#[test]
#[ignore = "TODO: Implement ValueModifyFilter example (requires FilterDecision::Change support)"]
fn should_modify_value_given_filter_returns_change_decision() {
    // Arrange
    let (_dir, eng) = new_engine_with_opts(512, true);
    let cf = eng.default_column_family();

    // TODO: Create a CompactionFilter that modifies values
    // let filter = ValueModifyFilter::new(|value| {
    //     format!("{}_modified", String::from_utf8_lossy(value))
    // });
    // eng.set_compaction_filter(&cf, filter);

    // Write original values
    bulk_put(&eng, &cf, "key_", 20, b"original");

    // Act
    // TODO: Add explicit compaction trigger API
    thread::sleep(Duration::from_millis(500)); // Allow compaction

    // Assert
    // TODO: Verify values are modified after compaction
    // let result = eng.get(&cf, b"key_00").expect("get failed");
    // assert_eq!(result.unwrap().as_ref(), b"original_modified");
    
    // For now, just verify data integrity
    let result = eng.get(&cf, b"key_00").expect("get failed");
    assert!(result.is_some(), "Data should be present after compaction");
}
