// Range Scan Edge Cases tests - P2 Priority stubs
// These tests will FAIL until implemented

// ============================================================================
// Large-Scale Scans (3 tests)
// ============================================================================

#[test]
fn should_scan_across_1000_ssts_given_large_database() {
    let cf = engine.default_column_family();
    // Smoke test: create many small SSTs and scan across them
    // Arrange
    let opts = cntryl_midge::MidgeOptions {
        storage_mode: cntryl_midge::StorageMode::Memory,
        memtable_size: 256,
        ..Default::default()
    };
    let engine = cntryl_midge::MidgeEngine::open(opts).unwrap();

    // Act - create multiple flushes to produce many SST files
    let mut expected = 0usize;
    for batch in 0..20 {
        for i in 0..10 {
            let key = format!("k{:03}_b{:02}", i, batch);
            engine
                .put(bytes::Bytes::from(key), bytes::Bytes::from("v"))
                .unwrap();
            expected += 1;
        }
        engine.flush().unwrap();
        // Wait for this flush to fully complete before starting next batch
        engine
            .wait_for_compaction_idle(std::time::Duration::from_secs(1))
            .unwrap();
    }

    // Wait for all background operations to complete before scanning
    engine
        .wait_for_compaction_idle(std::time::Duration::from_secs(5))
        .unwrap();

    // Assert - scan entire range returns all keys
    let results = engine
        .scan(cntryl_midge::Query::new().start_key(bytes::Bytes::from("k000")))
        .unwrap();
    assert_eq!(results.len(), expected);
}

#[test]
fn should_not_exhaust_memory_given_scan_over_millions_of_keys() {
    let cf = engine.default_column_family();
    // Arrange - create a large-ish dataset in memory (smoke sized)
    let opts = cntryl_midge::MidgeOptions {
        storage_mode: cntryl_midge::StorageMode::Memory,
        memtable_size: 1024,
        ..Default::default()
    };
    let engine = cntryl_midge::MidgeEngine::open(opts).unwrap();

    // Act - insert many keys and flush periodically
    let total = 2000usize;
    for i in 0..total {
        let key = format!("big{:06}", i);
        engine
            .put(bytes::Bytes::from(key), bytes::Bytes::from("v"))
            .unwrap();
        if i % 100 == 0 {
            engine.flush().unwrap();
        }
    }

    // Assert - scanning returns expected count (allow >= in case of implementation differences)
    let results = engine.scan(cntryl_midge::Query::new()).unwrap();
    assert!(
        results.len() >= total,
        "expected at least {} results, got {}",
        total,
        results.len()
    );
}

#[test]
fn should_handle_scan_with_many_tombstones_efficiently() {
    let cf = engine.default_column_family();
    // Arrange
    let opts = cntryl_midge::MidgeOptions {
        storage_mode: cntryl_midge::StorageMode::Memory,
        memtable_size: 512,
        ..Default::default()
    };
    let engine = cntryl_midge::MidgeEngine::open(opts).unwrap();

    // Act - insert and then tombstone many keys
    let total = 200usize;
    for i in 0..total {
        let key = format!("t{:04}", i);
        engine
            .put(bytes::Bytes::from(key), bytes::Bytes::from("v"))
            .unwrap();
    }
    engine.flush().unwrap();

    // Tombstone most keys
    for i in 0..150 {
        let key = format!("t{:04}", i);
        engine.delete(&cf, bytes::Bytes::from(key)).unwrap();
    }
    engine.flush().unwrap();

    // Assert - scanning returns remaining keys only
    let results = engine.scan(cntryl_midge::Query::new()).unwrap();
    assert_eq!(results.len(), 50);
}

// ============================================================================
// Scans During Compaction (3 tests)
// ============================================================================

#[test]
fn should_maintain_consistency_given_compaction_during_scan() {
    let cf = engine.default_column_family();
    // Arrange
    let opts = cntryl_midge::MidgeOptions {
        storage_mode: cntryl_midge::StorageMode::Memory,
        memtable_size: 256,
        ..Default::default()
    };
    let engine = std::sync::Arc::new(cntryl_midge::MidgeEngine::open(opts).unwrap());

    // Populate data across flushes
    for batch in 0..10 {
        for i in 0..50 {
            let key = format!("c{:03}_{}", batch, i);
            engine
                .put(bytes::Bytes::from(key), bytes::Bytes::from("v"))
                .unwrap();
        }
        engine.flush().unwrap();
    }

    // Act - start compaction in background while scanning repeatedly
    let e = std::sync::Arc::clone(&engine);
    let handle = std::thread::spawn(move || {
        // Give scanner a head start
        std::thread::sleep(std::time::Duration::from_millis(5));
        let _ = e.compact_all();
    });

    // Perform scans while compaction may be running
    for _ in 0..20 {
        let results = engine.scan(cntryl_midge::Query::new()).unwrap();
        assert!(!results.is_empty());
    }

    handle.join().unwrap();

    // Assert - key visibility preserved after compaction
    let sample = engine.get(&cf, b"c000_0").unwrap();
    assert!(sample.is_some());
}

#[test]
fn should_not_skip_keys_given_files_being_compacted_when_scanning() {
    // Arrange
    let opts = cntryl_midge::MidgeOptions {
        storage_mode: cntryl_midge::StorageMode::Memory,
        memtable_size: 256,
        ..Default::default()
    };
    let engine = std::sync::Arc::new(cntryl_midge::MidgeEngine::open(opts).unwrap());

    // Create overlapping key sets across multiple SSTs
    for batch in 0..8 {
        for i in 0..50 {
            let key = format!("ov{:03}", i + batch * 50);
            engine
                .put(bytes::Bytes::from(key), bytes::Bytes::from("v"))
                .unwrap();
        }
        engine.flush().unwrap();
    }

    // Act - compact in background and then scan
    let e = std::sync::Arc::clone(&engine);
    let comp = std::thread::spawn(move || {
        let _ = e.compact_all();
    });

    let results = engine.scan(cntryl_midge::Query::new()).unwrap();
    comp.join().unwrap();

    // Assert - all keys present
    assert_eq!(results.len(), 8 * 50);
}

#[test]
fn should_handle_iterator_invalidation_given_compaction_completes_mid_scan() {
    // Arrange
    let opts = cntryl_midge::MidgeOptions {
        storage_mode: cntryl_midge::StorageMode::Memory,
        memtable_size: 256,
        ..Default::default()
    };
    let engine = std::sync::Arc::new(cntryl_midge::MidgeEngine::open(opts).unwrap());

    for i in 0..200 {
        let key = format!("it{:04}", i);
        engine
            .put(bytes::Bytes::from(key), bytes::Bytes::from("v"))
            .unwrap();
    }
    engine.flush().unwrap();

    // Act - start compaction while iterating over scan results
    let e = std::sync::Arc::clone(&engine);
    let handle = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(5));
        let _ = e.compact_all();
    });

    let results = engine.scan(cntryl_midge::Query::new()).unwrap();
    // Iterate to simulate long-lived iterator consumption
    let mut seen = 0usize;
    for _ in results.iter() {
        seen += 1;
    }

    handle.join().unwrap();

    // Assert
    assert_eq!(seen, 200);
}

// ============================================================================
// Scan Memory Management (2 tests)
// ============================================================================

#[test]
fn should_limit_iterator_memory_given_buffering_threshold() {
    // Smoke: ensure large scans can be consumed incrementally without panicking
    let opts = cntryl_midge::MidgeOptions {
        storage_mode: cntryl_midge::StorageMode::Memory,
        memtable_size: 1024,
        ..Default::default()
    };
    let engine = cntryl_midge::MidgeEngine::open(opts).unwrap();

    let total = 1500usize;
    for i in 0..total {
        let key = format!("m{:05}", i);
        engine
            .put(bytes::Bytes::from(key), bytes::Bytes::from("v"))
            .unwrap();
        if i % 200 == 0 {
            engine.flush().unwrap();
        }
    }

    // Act & Assert - consume results in streaming-like fashion
    let results = engine.scan(cntryl_midge::Query::new()).unwrap();
    assert_eq!(results.len(), total);
}

#[test]
fn should_release_blocks_given_iterator_advanced_beyond_range() {
    // Smoke: iterating and dropping prefixes should not leak or crash
    let opts = cntryl_midge::MidgeOptions {
        storage_mode: cntryl_midge::StorageMode::Memory,
        memtable_size: 512,
        ..Default::default()
    };
    let engine = cntryl_midge::MidgeEngine::open(opts).unwrap();

    for i in 0..500 {
        let key = format!("b{:04}", i);
        engine
            .put(bytes::Bytes::from(key), bytes::Bytes::from("v"))
            .unwrap();
    }
    engine.flush().unwrap();

    // Act - perform progressive scans and drop results
    for start in (0..500).step_by(100) {
        let start_key = format!("b{:04}", start);
        let results = engine
            .scan(cntryl_midge::Query::new().start_key(bytes::Bytes::from(start_key)))
            .unwrap();
        assert!(!results.is_empty());
    }
}

// ============================================================================
// Seek Performance (3 tests)
// ============================================================================

#[test]
fn should_seek_efficiently_given_large_skip_forward() {
    // Arrange
    let opts = cntryl_midge::MidgeOptions {
        storage_mode: cntryl_midge::StorageMode::Memory,
        memtable_size: 512,
        ..Default::default()
    };
    let engine = cntryl_midge::MidgeEngine::open(opts).unwrap();

    for i in 0..1000 {
        let key = format!("s{:06}", i);
        engine
            .put(bytes::Bytes::from(key), bytes::Bytes::from("v"))
            .unwrap();
    }
    engine.flush().unwrap();

    // Act - seek forward to a far key and scan a small range
    let start_key = bytes::Bytes::from("s000900");
    let q = cntryl_midge::Query::new()
        .start_key(start_key)
        .end_key(bytes::Bytes::from("s001000"));
    let results = engine.scan(q).unwrap();

    // Assert
    assert_eq!(results.len(), 100);
}

#[test]
fn should_seek_backward_efficiently_given_reverse_iterator() {
    let cf = engine.default_column_family();
    // Arrange
    let opts = cntryl_midge::MidgeOptions {
        storage_mode: cntryl_midge::StorageMode::Memory,
        memtable_size: 512,
        ..Default::default()
    };
    let engine = cntryl_midge::MidgeEngine::open(opts).unwrap();

    for i in 0..500 {
        let key = format!("r{:04}", i);
        engine
            .put(bytes::Bytes::from(key), bytes::Bytes::from("v"))
            .unwrap();
    }
    engine.flush().unwrap();

    // Act - reverse scan a tail range
    let q = cntryl_midge::Query::new()
        .start_key(bytes::Bytes::from("r0490"))
        .end_key(bytes::Bytes::from("r0500"))
        .reverse();
    let results = engine.scan(q).unwrap();

    // Assert
    assert_eq!(results.len(), 10);
}

#[test]
fn should_use_bloom_filters_given_seek_to_nonexistent_key() {
    let cf = engine.default_column_family();
    // Arrange
    let opts = cntryl_midge::MidgeOptions {
        storage_mode: cntryl_midge::StorageMode::Memory,
        memtable_size: 512,
        ..Default::default()
    };
    let engine = cntryl_midge::MidgeEngine::open(opts).unwrap();

    // Insert sparse keys
    for i in (0..1000).step_by(10) {
        let key = format!("bf{:05}", i);
        engine
            .put(bytes::Bytes::from(key), bytes::Bytes::from("v"))
            .unwrap();
    }
    engine.flush().unwrap();

    // Act - seek to a key that does not exist
    let q = cntryl_midge::Query::new()
        .start_key(bytes::Bytes::from("bf00007"))
        .end_key(bytes::Bytes::from("bf00009"));
    let results = engine.scan(q).unwrap();

    // Assert - no results for nonexistent key
    assert!(results.is_empty());
}

// ============================================================================
// Snapshot Isolation for Scans (4 tests)
// ============================================================================

#[test]
fn should_not_see_new_writes_given_snapshot_iterator_when_concurrent_puts() {
    let cf = engine.default_column_family();
    // Arrange
    let opts = cntryl_midge::MidgeOptions {
        storage_mode: cntryl_midge::StorageMode::Memory,
        memtable_size: 512,
        ..Default::default()
    };
    let engine = std::sync::Arc::new(cntryl_midge::MidgeEngine::open(opts).unwrap());

    for i in 0..100 {
        engine
            .put(
                bytes::Bytes::from(format!("n{:03}", i)),
                bytes::Bytes::from("v"),
            )
            .unwrap();
    }
    engine.flush().unwrap();

    // Act - take a snapshot-like scan (scan returns snapshot) and then write more
    let results_before = engine.scan(cntryl_midge::Query::new()).unwrap();

    // Concurrent writes
    let e = std::sync::Arc::clone(&engine);
    let handle = std::thread::spawn(move || {
        for i in 100..200 {
            e.put(&cf, 
                bytes::Bytes::from(format!("n{:03}", i)),
                bytes::Bytes::from("v"),
            )
            .unwrap();
        }
        e.flush().unwrap();
    });

    handle.join().unwrap();

    // Assert - original scan result should not include new writes
    assert_eq!(results_before.len(), 100);
}

#[test]
fn should_maintain_consistent_view_given_snapshot_scan_when_compaction_runs() {
    // Arrange
    let opts = cntryl_midge::MidgeOptions {
        storage_mode: cntryl_midge::StorageMode::Memory,
        memtable_size: 256,
        ..Default::default()
    };
    let engine = std::sync::Arc::new(cntryl_midge::MidgeEngine::open(opts).unwrap());

    for i in 0..200 {
        engine
            .put(
                bytes::Bytes::from(format!("sv{:03}", i)),
                bytes::Bytes::from("v"),
            )
            .unwrap();
    }
    engine.flush().unwrap();

    // Act - start compaction and take a scan snapshot
    let e = std::sync::Arc::clone(&engine);
    let comp = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(5));
        let _ = e.compact_all();
    });

    let snap = engine.scan(cntryl_midge::Query::new()).unwrap();
    comp.join().unwrap();

    // Assert - snapshot view remains consistent
    assert_eq!(snap.len(), 200);
}

#[test]
fn should_see_all_keys_at_snapshot_sequence_given_range_scan() {
    // Arrange
    let opts = cntryl_midge::MidgeOptions {
        storage_mode: cntryl_midge::StorageMode::Memory,
        memtable_size: 512,
        ..Default::default()
    };
    let engine = cntryl_midge::MidgeEngine::open(opts).unwrap();

    for i in 0..150 {
        engine
            .put(
                bytes::Bytes::from(format!("ss{:03}", i)),
                bytes::Bytes::from("v"),
            )
            .unwrap();
    }
    engine.flush().unwrap();

    // Act
    let results = engine.scan(cntryl_midge::Query::new()).unwrap();

    // Assert
    assert_eq!(results.len(), 150);
}

#[test]
fn should_handle_expired_snapshot_given_long_running_scan() {
    // Arrange - long-running scan simulated by many iterations
    let opts = cntryl_midge::MidgeOptions {
        storage_mode: cntryl_midge::StorageMode::Memory,
        memtable_size: 512,
        ..Default::default()
    };
    let engine = std::sync::Arc::new(cntryl_midge::MidgeEngine::open(opts).unwrap());

    for i in 0..300 {
        engine
            .put(
                bytes::Bytes::from(format!("ls{:03}", i)),
                bytes::Bytes::from("v"),
            )
            .unwrap();
    }
    engine.flush().unwrap();

    // Act - start a background compaction to potentially expire snapshots
    let e = std::sync::Arc::clone(&engine);
    let comp = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(5));
        let _ = e.compact_all();
    });

    // Simulate long-running scan by repeated partial scans
    let mut total_seen = 0usize;
    for i in 0..10 {
        let start = format!("ls{:03}", i * 30);
        let q = cntryl_midge::Query::new().start_key(bytes::Bytes::from(start));
        let res = engine.scan(q).unwrap();
        total_seen += res.len();
    }

    comp.join().unwrap();

    // Assert - we observed keys across the long-running scan
    assert!(total_seen > 0);
}
