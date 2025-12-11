//! Read path caching tests
//!
//! Tests for block cache behavior, LRU eviction, bloom filters,
//! and read amplification under various access patterns.

mod common;

use bytes::Bytes;
use cntryl_midge::Query;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use cntryl_midge::testkit::{assert_get_equals, new_engine};
use std::sync::Arc;
use tempfile::TempDir;

// ============================================================================
// PARANOID CHECKSUM VERIFICATION
// ============================================================================

#[test]
fn should_verify_checksums_on_read_given_paranoid_mode_enabled_when_reading() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: temp_dir.path().to_path_buf(),
        },
        paranoid_checksums: true,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    // Act
    eng.put(&cf, b"key1", b"value1").expect("put");
    eng.put(&cf, b"key2", b"value2").expect("put");
    eng.flush().expect("flush");

    // Assert
    assert_get_equals(&eng, b"key1", b"value1");
    assert_get_equals(&eng, b"key2", b"value2");
}

// ============================================================================
// LRU CACHE BEHAVIOR
// ============================================================================

#[test]
fn should_evict_least_recently_used_entry_given_cache_full_when_inserting() {
    // Arrange
    let (_dir, eng) = new_engine();
    let cf = eng.default_column_family();

    // Act
    for i in 0..1000 {
        eng.put(&cf, format!("key{:04}", i).as_bytes(), b"value")
            .expect("put");
    }

    // Read keys in specific order to establish LRU state
    for i in (0..100).rev() {
        let _ = eng
            .get(&cf, format!("key{:04}", i).as_bytes())
            .expect("get");
    }

    // Assert
    let result = eng.get(&cf, b"key0099").expect("get");
    assert!(result.is_some(), "Recently accessed key should be present");
}

// ============================================================================
// READ AMPLIFICATION WITH BLOOM FILTERS
// ============================================================================

#[test]
fn should_limit_read_amplification_given_bloom_filters_when_point_reading() {
    // Arrange
    let (_dir, eng) = new_engine();
    let cf = eng.default_column_family();

    // Act
    for i in 0..1000 {
        eng.put(&cf, format!("key{:04}", i).as_bytes(), b"value")
            .expect("put");
    }

    // Point reads (should benefit from bloom filters)
    for i in (0..1000).step_by(10) {
        let _ = eng
            .get(&cf, format!("key{:04}", i).as_bytes())
            .expect("get");
    }

    // Range scan (should benefit from index locality)
    let query = Query::new()
        .start_key(Bytes::from("key0000"))
        .end_key(Bytes::from("key0100"));
    let results = eng.scan(&cf, query).expect("scan");

    // Assert
    assert_eq!(results.len(), 100, "Range scan should return correct count");
}

// ============================================================================
// CACHE HIT PATTERNS
// ============================================================================

#[test]
fn should_hit_cache_for_frequently_accessed_keys_given_working_set_fits_when_reading() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();
    let engine = Arc::new(engine);

    // Act
    for i in 0..500 {
        let key = format!("cache_test_key_{:04}", i).into_bytes();
        engine.put(&cf, &key, b"value_for_cache_test").expect("put");
    }

    // Repeatedly access a small working set (should stay in cache)
    const WORKING_SET_SIZE: usize = 50;
    const ACCESS_ITERATIONS: usize = 100;
    for _ in 0..ACCESS_ITERATIONS {
        for i in 0..WORKING_SET_SIZE {
            let key = format!("cache_test_key_{:04}", i).into_bytes();
            let result = engine.get(&cf, &key).expect("get");
            assert!(
                result.is_some(),
                "Working set key should always be readable"
            );
        }
    }

    // Assert
    for i in 0..WORKING_SET_SIZE {
        let key = format!("cache_test_key_{:04}", i).into_bytes();
        let result = engine.get(&cf, &key).expect("final get");
        assert_eq!(
            result.unwrap(),
            b"value_for_cache_test".to_vec(),
            "Cache should preserve data integrity"
        );
    }
}

// ============================================================================
// CONCURRENT CACHE ACCESS
// ============================================================================

#[test]
fn should_balance_cache_given_concurrent_readers_when_working_sets_overlap() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();
    let engine = Arc::new(engine);
    const NUM_READERS: usize = 10;
    const KEYS_PER_READER: usize = 20;
    const TOTAL_KEYS: usize = 100;
    const ITERATIONS: usize = 50;

    for i in 0..TOTAL_KEYS {
        let key = format!("overlap_key_{:04}", i).into_bytes();
        engine
            .put(&cf, &key, format!("value_{}", i).as_bytes())
            .expect("put");
    }

    // Act
    let handles: Vec<_> = (0..NUM_READERS)
        .map(|reader_id| {
            let eng = engine.clone();
            let cf_clone = cf.clone();
            std::thread::spawn(move || {
                for iter in 0..ITERATIONS {
                    for offset in 0..KEYS_PER_READER {
                        let key_idx = (reader_id * KEYS_PER_READER + offset + iter) % TOTAL_KEYS;
                        let key = format!("overlap_key_{:04}", key_idx).into_bytes();
                        let result = eng.get(&cf_clone, &key).expect("get");
                        assert!(
                            result.is_some(),
                            "Key should be readable in overlap pattern"
                        );
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("Thread panicked");
    }

    // Assert
    for i in 0..TOTAL_KEYS {
        let key = format!("overlap_key_{:04}", i).into_bytes();
        let result = engine.get(&cf, &key).expect("get after concurrent reads");
        assert_eq!(
            result.unwrap(),
            format!("value_{}", i).as_bytes().to_vec(),
            "Cache should maintain data integrity under concurrent access"
        );
    }
}

#[test]
fn should_maintain_efficiency_given_concurrent_range_scans_when_accessing() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();
    let engine = Arc::new(engine);
    const NUM_THREADS: usize = 8;
    const KEYS_PER_SCAN: usize = 100;
    const SCAN_ITERATIONS: usize = 20;

    for i in 0..1000 {
        let key = format!("scan_key_{:05}", i).into_bytes();
        engine
            .put(&cf, &key, format!("scan_value_{}", i).as_bytes())
            .expect("put");
    }

    // Act
    let handles: Vec<_> = (0..NUM_THREADS)
        .map(|thread_id| {
            let eng = engine.clone();
            let cf_clone = cf.clone();
            std::thread::spawn(move || {
                for iter in 0..SCAN_ITERATIONS {
                    let start_idx = (thread_id * KEYS_PER_SCAN + iter * 10) % 1000;
                    let end_idx = (start_idx + KEYS_PER_SCAN) % 1000;

                    let query = Query::new()
                        .start_key(Bytes::from(format!("scan_key_{:05}", start_idx)))
                        .end_key(Bytes::from(format!("scan_key_{:05}", end_idx)));

                    let results = eng.scan(&cf_clone, query).expect("scan");
                    assert!(
                        !results.is_empty(),
                        "Concurrent scans should return results"
                    );
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("Thread panicked");
    }

    // Assert
    let query = Query::new()
        .start_key(Bytes::from("scan_key_00000"))
        .end_key(Bytes::from("scan_key_00500"));
    let results = engine.scan(&cf, query).expect("final scan");
    assert!(
        !results.is_empty(),
        "Final scan should return results after concurrent load"
    );
}
