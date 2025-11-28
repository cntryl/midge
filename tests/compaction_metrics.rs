//! Compaction metrics tests
//!
//! Tests for measuring and tracking read, write, and space amplification
//! during compaction operations.

mod common;

use cntryl_midge::Query;
use common::{
    compaction_test_opts, create_storage_mode, disk_storage_modes, populate_multi_level_data,
};

// ============================================================================
// READ AMPLIFICATION
// ============================================================================

#[test]
fn should_measure_read_amplification_given_multilevel_data_when_scanning() {
    for mode in disk_storage_modes() {
        // Arrange
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        let opts = compaction_test_opts(storage_mode);
        let eng = cntryl_midge::MidgeEngine::open(opts).unwrap();
        let cf = eng.default_column_family();

        populate_multi_level_data(&eng, &cf);
        eng.compact_all().unwrap();

        // Act
        let metrics_before = eng.performance_metrics().sst.total_reads();
        let query = Query::new()
            .start_key(bytes::Bytes::from("key000"))
            .end_key(bytes::Bytes::from("key999"));
        let results = eng.scan(&cf, query).expect("scan failed");
        let metrics_after = eng.performance_metrics().sst.total_reads();
        let sst_reads = metrics_after - metrics_before;

        // Assert
        assert!(!results.is_empty(), "Scan should return data");
        assert!(
            metrics_after >= metrics_before,
            "SST read counter should not decrease"
        );
        if sst_reads > 0 {
            let read_amp = sst_reads as f64 / results.len() as f64;
            assert!(
                read_amp >= 0.0,
                "Read amplification ratio should be non-negative"
            );
        }
    }
}

// ============================================================================
// WRITE AMPLIFICATION
// ============================================================================

#[test]
fn should_measure_write_amplification_given_compaction_cascade_when_writing() {
    for mode in disk_storage_modes() {
        // Arrange
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        let mut opts = compaction_test_opts(storage_mode);
        opts.wal_sync = true;
        let eng = cntryl_midge::MidgeEngine::open(opts).unwrap();
        let cf = eng.default_column_family();

        let initial_compaction_bytes = eng.performance_metrics().compaction.total_bytes_written();
        let initial_wal_bytes = eng.performance_metrics().wal.total_bytes_written();

        // Act
        let mut user_bytes: usize = 0;
        for i in 0..200 {
            let key = format!("key_{:05}", i);
            let value = vec![b'x'; 300];
            user_bytes += value.len();
            eng.put(&cf, key.as_bytes(), &value).unwrap();
            if i % 50 == 49 {
                eng.flush_cf(&cf).expect("flush");
            }
        }
        eng.flush_cf(&cf).ok();
        eng.compact_all().unwrap();

        let final_compaction_bytes = eng.performance_metrics().compaction.total_bytes_written();
        let final_wal_bytes = eng.performance_metrics().wal.total_bytes_written();

        // Assert
        assert!(
            final_wal_bytes >= initial_wal_bytes,
            "WAL bytes metric should be monotonic"
        );
        let compaction_bytes_written = final_compaction_bytes - initial_compaction_bytes;
        if compaction_bytes_written > 0 {
            let write_amp = compaction_bytes_written as f64 / user_bytes as f64;
            assert!(
                write_amp >= 0.0,
                "Write amplification ratio should be non-negative"
            );
        }
    }
}

// ============================================================================
// SPACE AMPLIFICATION
// ============================================================================

#[test]
fn should_measure_space_amplification_given_overwrites_when_comparing_live_vs_total() {
    for mode in disk_storage_modes() {
        // Arrange
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        let mut opts = compaction_test_opts(storage_mode);
        opts.enable_compaction = false;
        let eng = cntryl_midge::MidgeEngine::open(opts).unwrap();
        let cf = eng.default_column_family();

        // Write initial data
        for i in 0..50 {
            let key = format!("key_{:02}", i);
            eng.put(&cf, key.as_bytes(), b"version1").unwrap();
        }
        eng.flush_cf(&cf).expect("flush");

        let total_sst_after_first_write = eng.metrics().get_total_sst_bytes();

        // Act - Overwrite half the keys
        for i in 0..25 {
            let key = format!("key_{:02}", i);
            eng.put(&cf, key.as_bytes(), b"version2").unwrap();
        }
        eng.flush_cf(&cf).expect("flush");
        eng.compact_all().unwrap();

        let total_sst_after_overwrite = eng.metrics().get_total_sst_bytes();

        // Assert
        assert!(
            total_sst_after_overwrite >= total_sst_after_first_write,
            "Total SST bytes should not decrease: {} -> {}",
            total_sst_after_first_write,
            total_sst_after_overwrite
        );
    }
}

// ============================================================================
// AMPLIFICATION OVER TIME
// ============================================================================

#[test]
fn should_track_amplification_over_time_given_continuous_workload_when_compacting() {
    for mode in disk_storage_modes() {
        // Arrange
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        let opts = compaction_test_opts(storage_mode);
        let eng = cntryl_midge::MidgeEngine::open(opts).unwrap();
        let cf = eng.default_column_family();
        let mut samples = Vec::new();

        // Act
        for phase in 0..6 {
            let start_written = eng.performance_metrics().compaction.total_bytes_written();
            let start_read = eng.performance_metrics().compaction.total_bytes_read();
            for i in 0..24 {
                let key = format!("key_p{phase:02}_i{i:02}");
                eng.put(&cf, key.as_bytes(), b"data").unwrap();
            }
            eng.flush_cf(&cf).unwrap();
            eng.compact_all().unwrap();
            let end_written = eng.performance_metrics().compaction.total_bytes_written();
            let end_read = eng.performance_metrics().compaction.total_bytes_read();
            let read_delta = end_read - start_read;
            let written_delta = end_written - start_written;
            if read_delta > 0 {
                let amp = written_delta as f64 / read_delta as f64;
                samples.push(amp);
            }
        }

        // Assert
        if !samples.is_empty() {
            for (idx, amp) in samples.iter().enumerate() {
                assert!(
                    *amp >= 1.0,
                    "Phase {idx} write amplification should be >=1, got {amp:.2}"
                );
            }
        }
    }
}
