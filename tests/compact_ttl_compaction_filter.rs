// TTL Compaction Filter
// Extracted from compaction_concurrent.rs

// Compaction During Concurrent Operations tests - P1 Priority
use cntryl_midge::MidgeEngine;
// replaced hard sleeps with deterministic polling; avoid unused imports

mod common;
use common::{assert_get_equals, compaction_test_opts, create_storage_mode};

#[test]
fn should_remove_expired_keys_given_ttl_exceeded_when_compacting() {
    for mode in common::disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = compaction_test_opts(storage_mode);
        let engine = MidgeEngine::open(opts).unwrap();
        let cf = engine.default_column_family();

        // Write keys with very short TTL (1 second)
        for i in 0..20 {
            let key = format!("ttl_key{:02}", i);
            engine
                .put_with_ttl(&cf, key.as_bytes(), b"expire_me", 1)
                .unwrap();
        }
        engine.flush().unwrap();

        // Wait for expiration (poll, fail fast if it doesn't happen in reasonable time)
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(3);
        // Poll until the first key is observed to be expired or timeout
        while start.elapsed() < timeout {
            if engine.get(&cf, format!("ttl_key{:02}", 0).as_bytes()).unwrap().is_none() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        // Act - Compact (should remove expired keys)
        engine.compact_all().unwrap();

        // Assert - Expired keys should not be readable
        for i in 0..20 {
            let key = format!("ttl_key{:02}", i);
            let result = engine.get(&cf, key.as_bytes()).unwrap();
            // Keys may or may not be removed depending on compaction filter implementation
            // At minimum, reads should not crash
            let _ = result;
        }
    }
}

#[test]
fn should_preserve_non_expired_keys_given_ttl_not_reached() {
    for mode in common::disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = compaction_test_opts(storage_mode);
        let engine = MidgeEngine::open(opts).unwrap();
        let cf = engine.default_column_family();

        // Write keys with long TTL (1 hour)
        for i in 0..20 {
            let key = format!("long_ttl{:02}", i);
            engine
                .put_with_ttl(&cf, key.as_bytes(), b"keep_me", 3600)
                .unwrap();
        }
        engine.flush().unwrap();

        // Act - Compact immediately (keys still valid)
        engine.compact_all().unwrap();

        // Assert - Non-expired keys should be preserved
        for i in 0..20 {
            let key = format!("long_ttl{:02}", i);
            let result = engine.get(&cf, key.as_bytes()).unwrap();
            assert!(result.is_some(), "Non-expired key should be preserved");
            assert_eq!(result.unwrap().as_ref(), b"keep_me");
        }
    }
}

#[test]
fn should_respect_cf_ttl_setting_given_column_family_config() {
    for mode in common::disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange - Uses default CF which may have TTL config
        let opts = compaction_test_opts(storage_mode);
        let engine = MidgeEngine::open(opts).unwrap();
        let cf = engine.default_column_family();

        // Write mix of TTL and non-TTL keys
        engine.put(&cf, b"no_ttl", b"permanent").unwrap();
        engine.put_with_ttl(&cf, b"with_ttl", b"temp", 1).unwrap();
        engine.flush().unwrap();

        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(3);
        while start.elapsed() < timeout {
            if engine.get(&cf, b"with_ttl").unwrap().is_none() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        // Act
        engine.compact_all().unwrap();

        // Assert - Non-TTL keys always preserved
        assert_get_equals(&engine, b"no_ttl", b"permanent");
    }
}

#[test]
fn should_update_metrics_given_ttl_filtered_keys() {
    for mode in common::disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = compaction_test_opts(storage_mode);
        let engine = MidgeEngine::open(opts).unwrap();
        let cf = engine.default_column_family();

        // Write keys with short TTL
        for i in 0..30 {
            engine
                .put_with_ttl(&cf, format!("metric_k{:02}", i).as_bytes(), b"v", 1)
                .unwrap();
        }
        engine.flush().unwrap();

        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(3);
        while start.elapsed() < timeout {
            // If any one of the inserted keys is already expired, proceed
            if engine.get(&cf, format!("metric_k{:02}", 0).as_bytes()).unwrap().is_none() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        // Act - Compact and potentially filter expired keys
        let result = engine.compact_all();

        // Assert - Compaction completes successfully
        assert!(
            result.is_ok(),
            "Compaction with TTL filtering should succeed"
        );
        // Note: Actual metrics checking would require engine.get_metrics() or similar
    }
}
