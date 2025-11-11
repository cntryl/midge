// Amplification Measurement
// Extracted from compaction_concurrent.rs

mod common;

use common::new_engine_with_opts;
use cntryl_midge::Query;
use std::thread;
use std::time::Duration;

// ============================================================================

#[test]
fn should_measure_read_amplification_given_multilevel_scan() {
    // Arrange
    let (_dir, eng) = new_engine_with_opts(1024, true);
    let cf = eng.default_column_family();

    // Write data across multiple levels
    for level in 0..3 {
        for i in 0..10 {
            let key = format!("key_l{}_i{}", level, i);
            let value = format!("value_{}", level);
            eng.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
        }
        eng.flush_cf(&cf).expect("flush");
        thread::sleep(Duration::from_millis(100)); // Allow compaction
    }

    // Act
    // TODO: Get baseline read_io_count from metrics (sst_reads, bloom_checks, etc.)
    let query = Query::new()
        .start_key(bytes::Bytes::from("key_l0"))
        .end_key(bytes::Bytes::from("key_l3"));
    let results = eng.scan(&cf, query).expect("scan failed");
    // TODO: Get final read_io_count from metrics

    // Assert
    assert!(results.len() >= 30, "Should read keys across all levels");
    // TODO: Assert read_amplification = read_io_count / logical_reads
    // Expected: read_amp > 1.0 for multi-level scan (ideally 2-3x)
}

#[test]
fn should_measure_write_amplification_given_compaction_cascade() {
    // Arrange
    let (_dir, eng) = new_engine_with_opts(512, true);
    let cf = eng.default_column_family();

    // TODO: Get baseline bytes_written from metrics (total_bytes_written, compaction_bytes_written)
    let _initial_written = 0u64; // Placeholder

    // Act
    // Write data that will trigger cascading compactions
    for i in 0..100 {
        let key = format!("key_{:04}", i);
        let value = vec![b'x'; 256]; // 256-byte values
        eng.put(&cf, key.as_bytes(), &value).unwrap();
    }
    eng.flush_cf(&cf).expect("flush");
    thread::sleep(Duration::from_millis(500)); // Allow compaction cascade

    // TODO: Get final bytes_written from metrics
    // let final_written = eng.get_stats(&cf).total_bytes_written;

    // Assert
    let _logical_bytes = 100 * (8 + 256); // keys + values (approx)
    // TODO: Assert write_amp = (final_written - initial_written) / logical_bytes
    // Expected: write_amp > 2.0 for cascading compactions (ideally 3-5x)
    let result = eng.get(&cf, b"key_0000").expect("get failed");
    assert!(result.is_some(), "Data should be present after compaction");
}

#[test]
fn should_measure_space_amplification_given_live_vs_total_data() {
    // Arrange
    let (_dir, eng) = new_engine_with_opts(1024, false);
    let cf = eng.default_column_family();

    // Write initial data
    for i in 0..50 {
        let key = format!("key_{:02}", i);
        eng.put(&cf, key.as_bytes(), b"version1").unwrap();
    }
    eng.flush_cf(&cf).expect("flush");

    // Act
    // Overwrite half the keys (creates obsolete data)
    for i in 0..25 {
        let key = format!("key_{:02}", i);
        eng.put(&cf, key.as_bytes(), b"version2").unwrap();
    }
    eng.flush_cf(&cf).expect("flush");

    // TODO: Get total_disk_bytes and live_data_bytes from metrics
    // let stats = eng.get_stats(&cf);
    // let space_amp = stats.total_disk_bytes as f64 / stats.live_data_bytes as f64;

    // Assert
    // TODO: Assert space_amp > 1.3 (50 live keys + ~25 obsolete keys before compaction)
    // Expected: space_amp = (50 + 25) / 50 = 1.5
    let result = eng.get(&cf, b"key_00").expect("get failed");
    assert_eq!(result.unwrap().as_ref(), b"version2", "Overwritten key should have new value");
    
    let result = eng.get(&cf, b"key_30").expect("get failed");
    assert_eq!(result.unwrap().as_ref(), b"version1", "Non-overwritten key should have original value");
}

#[test]
fn should_track_amplification_over_time_given_workload() {
    // Arrange
    let (_dir, eng) = new_engine_with_opts(512, true);
    let cf = eng.default_column_family();

    // TODO: Create metrics snapshot API or export amplification history
    // let mut read_amp_samples = Vec::new();
    // let mut write_amp_samples = Vec::new();
    // let mut space_amp_samples = Vec::new();

    // Act
    // Simulate workload over time
    for phase in 0..5 {
        for i in 0..20 {
            let key = format!("key_p{}_i{}", phase, i);
            eng.put(&cf, key.as_bytes(), b"data").unwrap();
        }
        eng.flush_cf(&cf).expect("flush");
        thread::sleep(Duration::from_millis(100));

        // TODO: Sample amplification metrics at each phase (requires metrics API)
        // let snapshot = eng.get_amplification_metrics(&cf);
        // read_amp_samples.push(snapshot.read_amplification);
        // write_amp_samples.push(snapshot.write_amplification);
        // space_amp_samples.push(snapshot.space_amplification);
    }

    // Assert
    // TODO: Assert read_amp trend (should stabilize after initial compactions)
    // TODO: Assert write_amp trend (should be roughly constant ~3-5x)
    // TODO: Assert space_amp trend (should decrease as compaction catches up)
    let result = eng.get(&cf, b"key_p0_i0").expect("get failed");
    assert_eq!(result.unwrap().as_ref(), b"data", "First phase data should be present");
    
    let result = eng.get(&cf, b"key_p4_i19").expect("get failed");
    assert_eq!(result.unwrap().as_ref(), b"data", "Last phase data should be present");
}
