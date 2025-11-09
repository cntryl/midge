// Amplification Measurement
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

/// Helper: create a new engine in a fresh temp dir and return both.
fn new_engine() -> (tempfile::TempDir, cntryl_midge::MidgeEngine) {
    let dir = test_temp_dir();
    let opts = cntryl_midge::MidgeOptions {
        storage_mode: cntryl_midge::StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = cntryl_midge::MidgeEngine::open(opts).expect("open");
    (dir, engine)
}

// ============================================================================

#[test]
#[ignore = "Amplification metrics API not yet exposed"]
fn should_measure_read_amplification_given_multilevel_scan() {
    // TODO: Implement when engine exposes read amplification metrics
    panic!("NOT IMPLEMENTED: Read amplification measurement test needed");
}

#[test]
#[ignore = "Amplification metrics API not yet exposed"]
fn should_measure_write_amplification_given_compaction_cascade() {
    // TODO: Implement when engine exposes write amplification metrics
    panic!("NOT IMPLEMENTED: Write amplification measurement test needed");
}

#[test]
#[ignore = "Amplification metrics API not yet exposed"]
fn should_measure_space_amplification_given_live_vs_total_data() {
    // TODO: Implement when engine exposes space amplification metrics
    panic!("NOT IMPLEMENTED: Space amplification measurement test needed");
}

#[test]
#[ignore = "Amplification metrics API not yet exposed"]
fn should_track_amplification_over_time_given_workload() {
    // TODO: Implement when engine exposes amplification trend tracking
    panic!("NOT IMPLEMENTED: Amplification trend tracking test needed");
}
