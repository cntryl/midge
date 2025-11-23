// tests/compaction_filters.rs

mod common;

use cntryl_midge::compaction::{CompactionFilter, CompactionVersion, FilterDecision};
use common::{bulk_put, create_storage_mode};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

// =============================================================================
// CountingFilter — counts invocations
// =============================================================================

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

        // deterministic: no background compaction
        let opts = cntryl_midge::MidgeOptions {
            storage_mode,
            memtable_size: 32,
            enable_compaction: false,
            ..Default::default()
        };

        let eng = cntryl_midge::MidgeEngine::open(opts).expect("open engine");
        let cf = eng.default_column_family();

        let invocation_count = Arc::new(AtomicUsize::new(0));
        eng.set_compaction_filter(&cf, Arc::new(CountingFilter::new(invocation_count.clone())))
            .expect("set filter");

        // produce >1 SST → compaction always happens
        bulk_put(&eng, &cf, "key_", 50, b"value");
        eng.flush_cf(&cf).expect("flush");

        // deterministic rewriter
        eng.compact_cf_full_rewrite(&cf)
            .expect("forced full rewrite compaction");

        // assert filter actually executed
        let count = invocation_count.load(Ordering::SeqCst);
        assert!(count > 0, "Expected filter invocations > 0, got {}", count);

        // sanity check key
        assert!(eng.get(&cf, b"key_000").unwrap().is_some());
    }
}

// =============================================================================
// PrefixRemovalFilter — drops keys starting with prefix
// =============================================================================

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

        let opts = cntryl_midge::MidgeOptions {
            storage_mode,
            memtable_size: 32,
            enable_compaction: false,
            ..Default::default()
        };

        let eng = cntryl_midge::MidgeEngine::open(opts).expect("open engine");
        let cf = eng.default_column_family();

        eng.set_compaction_filter(&cf, Arc::new(PrefixRemovalFilter::new(b"remove_")))
            .expect("set filter");

        bulk_put(&eng, &cf, "keep_", 10, b"value");
        bulk_put(&eng, &cf, "remove_", 10, b"value");

        eng.flush_cf(&cf).expect("flush");
        eng.compact_cf_full_rewrite(&cf)
            .expect("forced full rewrite compaction");

        // kept keys
        assert!(eng.get(&cf, b"keep_000").unwrap().is_some());

        // removed keys
        assert!(eng.get(&cf, b"remove_000").unwrap().is_none());
        assert!(eng.get(&cf, b"remove_005").unwrap().is_none());
    }
}

// =============================================================================
// KeepAllFilter — ensures compaction is non-destructive
// =============================================================================

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

        let opts = cntryl_midge::MidgeOptions {
            storage_mode,
            memtable_size: 32,
            enable_compaction: false,
            ..Default::default()
        };

        let eng = cntryl_midge::MidgeEngine::open(opts).expect("open engine");
        let cf = eng.default_column_family();

        eng.set_compaction_filter(&cf, Arc::new(KeepAllFilter::new()))
            .expect("set filter");

        bulk_put(&eng, &cf, "key_", 30, b"data");
        eng.flush_cf(&cf).expect("flush");

        eng.compact_cf_full_rewrite(&cf)
            .expect("forced full rewrite compaction");

        for i in 0..30 {
            let key = format!("key_{:03}", i);
            let res = eng.get(&cf, key.as_bytes()).expect("get");
            assert!(res.is_some(), "Expected to keep {}", key);
        }
    }
}
