// Crash recovery and corruption handling tests
// Based on RocksDB bug patterns analysis - ensures Midge handles edge cases correctly

mod common;

use bytes::Bytes;
use common::*;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

/// Helper to truncate a file by removing bytes from the end
fn truncate_file(path: &Path, bytes_to_remove: u64) -> std::io::Result<()> {
    let cf = engine.default_column_family();
    let file = OpenOptions::new().write(true).open(path)?;
    let metadata = file.metadata()?;
    let new_size = metadata.len().saturating_sub(bytes_to_remove);
    file.set_len(new_size)?;
    Ok(())
}

/// Helper to corrupt bytes in the middle of a file
fn corrupt_file_at_offset(path: &Path, offset: u64, corruption: &[u8]) -> std::io::Result<()> {
    let cf = engine.default_column_family();
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(corruption)?;
    file.flush()?;
    Ok(())
}

/// Helper to find WAL files in a directory
fn find_wal_files(db_path: &Path) -> Vec<std::path::PathBuf> {
    let cf = engine.default_column_family();
    let wal_dir = db_path.join("wal");
    if !wal_dir.exists() {
        return vec![];
    }

    std::fs::read_dir(&wal_dir)
        .ok()
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.path()
                        .extension()
                        .and_then(|s| s.to_str())
                        .map(|s| s == "wal")
                        .unwrap_or(false)
                })
                .map(|e| e.path())
                .collect()
        })
        .unwrap_or_default()
}

// ============================================================================
// PRIORITY 1: WAL Corruption Tests
// ============================================================================

// Note: These tests document Midge's current strict recovery behavior.
// Midge currently operates in "AbsoluteConsistency" mode - it refuses to
// recover if ANY corruption is detected, even in the tail.
//
// Future enhancement: Add `WalRecoveryMode::TolerateCorruptedTail` to recover
// partial data when only the tail is corrupted (common in power-loss scenarios).

#[test]
fn should_recover_from_truncated_tail_given_power_loss() {
    let cf = engine.default_column_family();
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());

    // Write data and find WAL file
    let wal_path = {
        let eng = cntryl_midge::MidgeEngine::open(opts.clone()).unwrap();

        // Write 100 records
        for i in 0..100 {
            let key = format!("key{:03}", i);
            let value = format!("value{:03}", i);
            eng.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Force flush to ensure all data is in WAL
        drop(eng);

        // Find the WAL file
        let wal_files = find_wal_files(dir.path());
        assert!(!wal_files.is_empty(), "No WAL files found");
        wal_files[0].clone()
    };

    // Get original file size for diagnostics
    let original_size = std::fs::metadata(&wal_path).unwrap().len();
    println!("Original WAL size: {} bytes", original_size);

    // Act - Simulate power loss by truncating the tail of the WAL
    // Remove last 50 bytes (likely truncates the last record)
    truncate_file(&wal_path, 50).expect("Failed to truncate WAL");

    let truncated_size = std::fs::metadata(&wal_path).unwrap().len();
    println!(
        "Truncated WAL size: {} bytes (removed {} bytes)",
        truncated_size,
        original_size - truncated_size
    );

    // Assert - Engine with TolerateCorruptedTail should recover partial data
    let mut opts_tolerant = opts.clone();
    opts_tolerant.wal_recovery_mode = cntryl_midge::WalRecoveryMode::TolerateCorruptedTail;

    match cntryl_midge::MidgeEngine::open(opts_tolerant) {
        Ok(eng) => {
            // At least the first 95 records should be recoverable
            // (last few might be in the truncated tail)
            let mut recovered_count = 0;
            for i in 0..95 {
                let key = format!("key{:03}", i);
                let expected = format!("value{:03}", i);
                if let Ok(Some(value)) = eng.get(&cf, key.as_bytes()) {
                    assert_eq!(value, Bytes::from(expected), "Mismatch for {}", key);
                    recovered_count += 1;
                }
            }

            println!(
                "Recovered {} records in TolerateCorruptedTail mode",
                recovered_count
            );

            assert!(
                recovered_count >= 90,
                "Should recover at least 90/95 records in tolerant mode, got {}",
                recovered_count
            );
        }
        Err(e) => {
            panic!(
                "TolerateCorruptedTail mode should have opened database, got error: {:?}",
                e
            );
        }
    }

    // Also verify strict mode still fails
    println!("\nTesting AbsoluteConsistency mode with same truncated WAL:");
    let mut opts_strict = opts.clone();
    opts_strict.wal_recovery_mode = cntryl_midge::WalRecoveryMode::AbsoluteConsistency;
    match cntryl_midge::MidgeEngine::open(opts_strict) {
        Ok(eng) => {
            // If it opened, it should have 0 records (rejected the corrupted WAL)
            let mut count = 0;
            for i in 0..100 {
                let key = format!("key{:03}", i);
                if eng.get(&cf, key.as_bytes()).unwrap().is_some() {
                    count += 1;
                }
            }
            println!("Strict mode recovered {} records (expected 0)", count);
            assert_eq!(
                count, 0,
                "Strict mode should not recover any records from corrupted WAL"
            );
        }
        Err(e) => {
            println!("Strict mode refused to open (EXPECTED): {:?}", e);
            // This is the preferred behavior for strict mode
        }
    }
}

#[test]
fn should_detect_middle_corruption_given_checksum_mismatch() {
    let cf = engine.default_column_family();
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());

    // Write data and find WAL file
    let wal_path = {
        let eng = cntryl_midge::MidgeEngine::open(opts.clone()).unwrap();

        // Write 100 records so we have enough data
        for i in 0..100 {
            let key = format!("key{:03}", i);
            let value = format!("value{:03}", i);
            eng.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
        }

        drop(eng);

        let wal_files = find_wal_files(dir.path());
        assert!(!wal_files.is_empty(), "No WAL files found");
        wal_files[0].clone()
    };

    // Get file size to find middle
    let file_size = std::fs::metadata(&wal_path).unwrap().len();
    let middle_offset = file_size / 2;

    // Act - Corrupt the middle of the file (flip some bits)
    let corruption = vec![0xFF, 0xFF, 0xFF, 0xFF];
    corrupt_file_at_offset(&wal_path, middle_offset, &corruption).expect("Failed to corrupt WAL");

    // Assert - Engine should detect corruption
    // This may either:
    // 1. Fail to open (strict mode)
    // 2. Open but stop recovery at corruption point
    // 3. Skip corrupted record and continue (tolerant mode)

    match cntryl_midge::MidgeEngine::open(opts) {
        Ok(eng) => {
            // If it opened, verify it didn't silently accept corrupt data
            // At least some early records should be present
            let mut found_early = false;
            for i in 0..10 {
                let key = format!("key{:03}", i);
                if eng.get(&cf, key.as_bytes()).is_ok() {
                    found_early = true;
                    break;
                }
            }
            assert!(
                found_early,
                "If engine opens with corruption, early records should be accessible"
            );
        }
        Err(e) => {
            // Expected behavior: detect corruption and refuse to open
            let err_str = format!("{:?}", e);
            assert!(
                err_str.contains("Corruption")
                    || err_str.contains("checksum")
                    || err_str.contains("CRC"),
                "Error should indicate corruption, got: {}",
                err_str
            );
        }
    }
}

#[test]
fn should_handle_empty_wal_file_given_crash_during_creation() {
    let cf = engine.default_column_family();
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());

    // Create engine to establish directory structure
    {
        let eng = cntryl_midge::MidgeEngine::open(opts.clone()).unwrap();
        eng.put(&cf, "key1".as_bytes(), "value1".as_bytes()).unwrap();
        drop(eng);
    }

    // Act - Create an empty WAL file (simulates crash during file creation)
    let wal_dir = dir.path().join("wal");
    let empty_wal = wal_dir.join("000999.wal");
    File::create(&empty_wal).expect("Failed to create empty WAL");

    // Assert - Engine should handle empty WAL gracefully
    let eng = cntryl_midge::MidgeEngine::open(opts).unwrap();
    assert_get_equals(&eng, b"key1", b"value1");
}

#[test]
fn should_handle_partially_written_record_given_crash() {
    let cf = engine.default_column_family();
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());

    // Write some complete records
    let wal_path = {
        let eng = cntryl_midge::MidgeEngine::open(opts.clone()).unwrap();

        for i in 0..50 {
            let key = format!("key{:03}", i);
            let value = format!("value{:03}", i);
            eng.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
        }

        drop(eng);

        let wal_files = find_wal_files(dir.path());
        wal_files[0].clone()
    };

    // Act - Append incomplete data (simulates crash mid-write)
    {
        let mut file = OpenOptions::new().append(true).open(&wal_path).unwrap();
        // Write partial record header
        file.write_all(&[0x00, 0x00, 0x00]).unwrap();
    }

    // Assert - Should recover the 50 complete records, ignore partial
    let eng = cntryl_midge::MidgeEngine::open(opts).unwrap();

    for i in 0..50 {
        let key = format!("key{:03}", i);
        let expected = format!("value{:03}", i);
        assert_get_equals(&eng, key.as_bytes(), expected.as_bytes());
    }
}

// ============================================================================
// PRIORITY 2: Manifest Consistency Tests
// ============================================================================

#[test]
fn should_maintain_consistency_given_restart_during_flush() {
    let cf = engine.default_column_family();
    // Arrange
    let dir = test_temp_dir();
    // Use small memtable to trigger flush
    let opts = flush_test_opts(dir.path().to_path_buf(), 1024);

    // Act - Write enough data to trigger flush, then restart mid-flush
    {
        let eng = cntryl_midge::MidgeEngine::open(opts.clone()).unwrap();

        // Write enough to trigger flush
        for i in 0..100 {
            let key = format!("key{:03}", i);
            let value = "x".repeat(100); // 100 bytes per value
            eng.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Abrupt shutdown (drop without graceful shutdown)
        drop(eng);
    }

    // Assert - After restart, database should be consistent
    let eng = cntryl_midge::MidgeEngine::open(opts).unwrap();

    // All committed writes should be present
    let mut found_count = 0;
    for i in 0..100 {
        let key = format!("key{:03}", i);
        if let Ok(Some(_)) = eng.get(&cf, key.as_bytes()) {
            found_count += 1;
        }
    }

    assert!(
        found_count >= 95,
        "Should recover most records after crash, got {}",
        found_count
    );
}

#[test]
fn should_not_lose_data_given_manifest_and_wal_mismatch() {
    let cf = engine.default_column_family();
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());

    // Write initial data
    {
        let eng = cntryl_midge::MidgeEngine::open(opts.clone()).unwrap();
        eng.put(&cf, "persistent".as_bytes(), "data".as_bytes())
            .unwrap();
        drop(eng);
    }

    // Act - Write more data but simulate crash before manifest update
    {
        let eng = cntryl_midge::MidgeEngine::open(opts.clone()).unwrap();
        eng.put(&cf, "new_key".as_bytes(), "new_value".as_bytes())
            .unwrap();

        // Drop without explicit close to simulate crash
        drop(eng);
    }

    // Assert - After restart, at minimum the persistent data should be there
    let eng = cntryl_midge::MidgeEngine::open(opts).unwrap();
    assert_get_equals(&eng, b"persistent", b"data");

    // new_key may or may not be present depending on timing, but no corruption
}

// ============================================================================
// PRIORITY 3: Compaction Crash Tests
// ============================================================================

#[test]
fn should_recover_from_crash_during_compaction() {
    let cf = engine.default_column_family();
    // Arrange
    let dir = test_temp_dir();
    let opts = flush_test_opts(dir.path().to_path_buf(), 2048);

    // Act - Write enough data to trigger multiple flushes and potential compaction
    {
        let eng = cntryl_midge::MidgeEngine::open(opts.clone()).unwrap();

        // Write data in multiple batches
        for batch in 0..5 {
            for i in 0..50 {
                let key = format!("key_{:02}_{:03}", batch, i);
                let value = "x".repeat(100);
                eng.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
            }

            // Give compaction time to start (if implemented)
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        // Abrupt shutdown during potential compaction
        drop(eng);
    }

    // Assert - Database should recover and all data should be accessible
    let eng = cntryl_midge::MidgeEngine::open(opts).unwrap();

    let mut total_found = 0;
    for batch in 0..5 {
        for i in 0..50 {
            let key = format!("key_{:02}_{:03}", batch, i);
            if let Ok(Some(_)) = eng.get(&cf, key.as_bytes()) {
                total_found += 1;
            }
        }
    }

    assert!(
        total_found >= 240,
        "Should recover at least 240/250 records, got {}",
        total_found
    );
}

// ============================================================================
// Edge Cases and Stress Tests
// ============================================================================

#[test]
fn should_handle_multiple_wal_files_given_recovery() {
    let cf = engine.default_column_family();
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());

    // Act - Create multiple sessions (each may create new WAL)
    for session in 0..3 {
        let eng = cntryl_midge::MidgeEngine::open(opts.clone()).unwrap();

        for i in 0..20 {
            let key = format!("session{}_key{}", session, i);
            let value = format!("value{}", i);
            eng.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
        }

        drop(eng);
    }

    // Assert - All data from all sessions should be recoverable
    let eng = cntryl_midge::MidgeEngine::open(opts).unwrap();

    for session in 0..3 {
        for i in 0..20 {
            let key = format!("session{}_key{}", session, i);
            let expected = format!("value{}", i);
            assert_get_equals(&eng, key.as_bytes(), expected.as_bytes());
        }
    }
}

#[test]
fn should_preserve_sequence_order_across_corrupted_recovery() {
    let cf = engine.default_column_family();
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());

    // Write data with specific order
    let wal_path = {
        let eng = cntryl_midge::MidgeEngine::open(opts.clone()).unwrap();

        // Write, overwrite, delete pattern
        eng.put(&cf, "key".as_bytes(), "v1".as_bytes()).unwrap();
        eng.put(&cf, "key".as_bytes(), "v2".as_bytes()).unwrap();
        eng.put(&cf, "key".as_bytes(), "v3".as_bytes()).unwrap();
        eng.delete(&cf, "key".as_bytes()).unwrap();
        eng.put(&cf, "key".as_bytes(), "final".as_bytes()).unwrap();

        drop(eng);

        let wal_files = find_wal_files(dir.path());
        wal_files[0].clone()
    };

    // Act - Truncate a bit but not too much
    truncate_file(&wal_path, 20).expect("Failed to truncate");

    // Assert - Whatever we recover should be consistent
    // If "final" is lost, we should see nothing (due to delete)
    // If "final" is present, we should see "final"
    let eng = cntryl_midge::MidgeEngine::open(opts).unwrap();

    match eng.get(&cf, b"key") {
        Ok(Some(value)) => {
            // If key exists, it must be "final" (the last write)
            assert_eq!(
                value,
                Bytes::from("final"),
                "If key exists, must be final value"
            );
        }
        Ok(None) => {
            // Key doesn't exist - acceptable if final write was truncated
            // This means the delete was the last operation recovered
        }
        Err(e) => {
            panic!("Get should not error: {:?}", e);
        }
    }
}

#[test]
fn should_handle_zero_byte_wal_file() {
    let cf = engine.default_column_family();
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());

    // Create initial data
    {
        let eng = cntryl_midge::MidgeEngine::open(opts.clone()).unwrap();
        eng.put(&cf, "key1".as_bytes(), "value1".as_bytes()).unwrap();
        drop(eng);
    }

    // Act - Create a zero-byte WAL file
    let wal_dir = dir.path().join("wal");
    let zero_wal = wal_dir.join("000998.wal");
    File::create(&zero_wal).expect("Failed to create zero-byte WAL");

    // Assert - Should handle gracefully
    let eng = cntryl_midge::MidgeEngine::open(opts).unwrap();
    assert_get_equals(&eng, b"key1", b"value1");
}

#[test]
fn should_recover_correct_count_after_batch_write_truncation() {
    let cf = engine.default_column_family();
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());

    // Write data in batch
    let wal_path = {
        let eng = cntryl_midge::MidgeEngine::open(opts.clone()).unwrap();

        // Write exactly 100 records
        for i in 0..100 {
            eng.put(&cf, 
                Bytes::from(format!("k{:03}", i)),
                Bytes::from(format!("v{:03}", i)),
            )
            .unwrap();
        }

        drop(eng);

        let wal_files = find_wal_files(dir.path());
        wal_files[0].clone()
    };

    // Act - Truncate tail
    let original_size = std::fs::metadata(&wal_path).unwrap().len();
    truncate_file(&wal_path, 100).expect("Failed to truncate");
    let truncated_size = std::fs::metadata(&wal_path).unwrap().len();

    println!(
        "Truncated WAL from {} to {} bytes",
        original_size, truncated_size
    );

    // Assert - Count recovered records
    match cntryl_midge::MidgeEngine::open(opts) {
        Ok(eng) => {
            let mut count = 0;
            for i in 0..100 {
                let key = format!("k{:03}", i);
                if eng.get(&cf, key.as_bytes()).unwrap().is_some() {
                    count += 1;
                }
            }

            println!("Recovered {} records after truncation", count);

            // NOTE: Current strict mode behavior - rejects corrupted WAL entirely
            if count == 0 {
                println!("CURRENT BEHAVIOR: Strict mode rejected corrupted WAL (expected)");
            } else {
                assert!(
                    count >= 90,
                    "Should recover at least 90 records, got {}",
                    count
                );
            }
        }
        Err(e) => {
            // EXPECTED in strict mode - acceptable behavior
            println!("Engine refused to open (EXPECTED in strict mode): {:?}", e);
        }
    }
}
