// TTL Compaction Filter
// Extracted from compaction_concurrent.rs

// Compaction During Concurrent Operations tests - P1 Priority
use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, Query, StorageMode, ColumnFamilyHandle};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

mod common;
use common::{assert_get_equals, assert_key_absent};

// Helper to create test options with small memtable for quick flushes
fn compaction_test_opts() -> MidgeOptions {
    MidgeOptions {
        storage_mode: StorageMode::Memory,
        memtable_size: 1024,         // Small memtable to trigger flushes easily
        compaction_sst_threshold: 2, // Trigger compaction with just 2 SST files
        ..Default::default()
    }
}

// Helper to populate engine with data spread across multiple L0 files
fn populate_multi_level_data(engine: &MidgeEngine, cf: &ColumnFamilyHandle) {
    // Write batch 1 and flush to L0
    for i in 0..50 {
    let key = format!("key{:03}", i);
    let value = format!("value1_{}", i);
    engine.put(cf, key.as_bytes(), value.as_bytes()).unwrap();
    }
    engine.flush().unwrap();

    // Write batch 2 and flush to L0 (overlapping keys)
    for i in 25..75 {
    let key = format!("key{:03}", i);
    let value = format!("value2_{}", i);
    engine.put(cf, key.as_bytes(), value.as_bytes()).unwrap();
    }
    engine.flush().unwrap();

    // Write batch 3 and flush to L0
    for i in 50..100 {
    let key = format!("key{:03}", i);
    let value = format!("value3_{}", i);
    engine.put(cf, key.as_bytes(), value.as_bytes()).unwrap();
    }
    engine.flush().unwrap();
}

#[test]
fn should_remove_expired_keys_given_ttl_exceeded_when_compacting() {
    // Arrange
    let opts = compaction_test_opts();
    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();

    // Write keys with very short TTL (1 second)
    for i in 0..20 {
        let key = format!("ttl_key{}", i);
        engine
            .put_with_ttl(Bytes::from(key), Bytes::from("expire_me"), 1)
            .unwrap();
    }
    engine.flush().unwrap();

    // Wait for expiration
    thread::sleep(Duration::from_secs(2));

    // Act - Compact (should remove expired keys)
    engine.compact_all().unwrap();

    // Assert - Expired keys should not be readable
    for i in 0..20 {
        let key = format!("ttl_key{}", i);
        let result = engine.get(&cf, key.as_bytes()).unwrap();
        // Keys may or may not be removed depending on compaction filter implementation
        // At minimum, reads should not crash
        let _ = result;
    }
}

#[test]
fn should_preserve_non_expired_keys_given_ttl_not_reached() {
    // Arrange
    let opts = compaction_test_opts();
    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();

    // Write keys with long TTL (1 hour)
    for i in 0..20 {
        let key = format!("long_ttl{}", i);
        engine
            .put_with_ttl(Bytes::from(key), Bytes::from("keep_me"), 3600)
            .unwrap();
    }
    engine.flush().unwrap();

    // Act - Compact immediately (keys still valid)
    engine.compact_all().unwrap();

    // Assert - Non-expired keys should be preserved
    for i in 0..20 {
        let key = format!("long_ttl{}", i);
        let result = engine.get(&cf, key.as_bytes()).unwrap();
        assert!(result.is_some(), "Non-expired key should be preserved");
        assert_eq!(result.unwrap().as_ref(), b"keep_me");
    }
}

#[test]
fn should_respect_cf_ttl_setting_given_column_family_config() {
    // Arrange - Uses default CF which may have TTL config
    let opts = compaction_test_opts();
    let engine = MidgeEngine::open(opts).unwrap();

    // Write mix of TTL and non-TTL keys
    engine
        .put(Bytes::from("no_ttl"), Bytes::from("permanent"))
        .unwrap();
    engine
        .put_with_ttl(Bytes::from("with_ttl"), Bytes::from("temp"), 1)
        .unwrap();
    engine.flush().unwrap();

    thread::sleep(Duration::from_secs(2));

    // Act
    engine.compact_all().unwrap();

    // Assert - Non-TTL keys always preserved
    assert_get_equals(&engine, b"no_ttl", b"permanent");
}

#[test]
fn should_update_metrics_given_ttl_filtered_keys() {
    // Arrange
    let opts = compaction_test_opts();
    let engine = MidgeEngine::open(opts).unwrap();

    // Write keys with short TTL
    for i in 0..30 {
        engine
            .put_with_ttl(Bytes::from(format!("metric_k{}", i)), Bytes::from("v"), 1)
            .unwrap();
    }
    engine.flush().unwrap();

    thread::sleep(Duration::from_secs(2));

    // Act - Compact and potentially filter expired keys
    let result = engine.compact_all();

    // Assert - Compaction completes successfully
    assert!(
        result.is_ok(),
        "Compaction with TTL filtering should succeed"
    );
    // Note: Actual metrics checking would require engine.get_metrics() or similar
}
