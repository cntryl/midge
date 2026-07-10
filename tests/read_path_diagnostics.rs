//! Regression coverage for benchmark read-path diagnostics.

mod common;

use cntryl_midge::{MidgeResult, Query, TransactionMode, WriteOptions};
use common::{open_with_mode, opts_for_mode};

#[test]
fn should_report_flushed_sst_read_deltas_without_setup_leakage() -> MidgeResult<()> {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("local"), "local");
    let cf = engine.create_column_family("diagnostics")?;
    let mut write = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    write.put(b"key".to_vec(), b"value".to_vec(), None)?;
    write.commit(WriteOptions::best_effort())?;
    engine.flush_cf(&cf)?;
    let start = engine.read_path_diagnostics_snapshot_for_benchmarks();

    // Act - the first read opens/populates caches; the second verifies hits.
    for _ in 0..2 {
        let read = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
        assert_eq!(read.get(b"key")?.as_deref(), Some(b"value".as_slice()));
    }
    let end = engine.read_path_diagnostics_snapshot_for_benchmarks();

    // Assert - only the measured window contributes to each delta.
    assert!(end.read_only_begin_tx_count > start.read_only_begin_tx_count);
    assert!(end.read_only_snapshot_cache_hits > start.read_only_snapshot_cache_hits);
    assert!(end.sst_reader_cache_hits > start.sst_reader_cache_hits);
    assert!(end.sst_block_cache_hits > start.sst_block_cache_hits);
    assert!(end.candidate_blocks_checked > start.candidate_blocks_checked);
    assert!(end.data_blocks_read > start.data_blocks_read);
    Ok(())
}

#[test]
fn should_isolate_read_path_diagnostics_between_engines() -> MidgeResult<()> {
    // Arrange
    let idle_engine = open_with_mode(&opts_for_mode("local"), "local");
    let busy_engine = open_with_mode(&opts_for_mode("local"), "local");
    let cf = busy_engine.create_column_family("diagnostics")?;
    let mut write = busy_engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    write.put(b"key".to_vec(), b"value".to_vec(), None)?;
    write.commit(WriteOptions::best_effort())?;
    busy_engine.flush_cf(&cf)?;

    let idle_start = idle_engine.read_path_diagnostics_snapshot_for_benchmarks();
    let busy_start = busy_engine.read_path_diagnostics_snapshot_for_benchmarks();

    // Act
    for _ in 0..2 {
        let read = busy_engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
        assert_eq!(read.get(b"key")?.as_deref(), Some(b"value".as_slice()));
    }

    let idle_end = idle_engine.read_path_diagnostics_snapshot_for_benchmarks();
    let busy_end = busy_engine.read_path_diagnostics_snapshot_for_benchmarks();

    // Assert
    assert_eq!(
        idle_end, idle_start,
        "another engine leaked into idle metrics"
    );
    assert!(
        busy_end.read_only_begin_tx_count > busy_start.read_only_begin_tx_count,
        "the owning engine did not observe its read-only transactions"
    );
    assert!(
        busy_end.sst_block_cache_hits > busy_start.sst_block_cache_hits,
        "the owning engine did not observe its block-cache activity"
    );
    Ok(())
}

#[test]
fn should_report_zero_delta_for_an_idle_engine_window() {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("local"), "local");

    // Act
    let start = engine.read_path_diagnostics_snapshot_for_benchmarks();
    let end = engine.read_path_diagnostics_snapshot_for_benchmarks();

    // Assert
    assert_eq!(end, start);
}

#[test]
fn should_count_flushed_range_scan_work_in_its_own_window() -> MidgeResult<()> {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("local"), "local");
    let cf = engine.create_column_family("diagnostics")?;
    let mut write = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    write.put(b"a".to_vec(), b"one".to_vec(), None)?;
    write.put(b"b".to_vec(), b"two".to_vec(), None)?;
    write.put(b"c".to_vec(), b"three".to_vec(), None)?;
    write.commit(WriteOptions::best_effort())?;

    let mut delete = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    delete.delete_range(b"b".to_vec(), b"c".to_vec())?;
    delete.commit(WriteOptions::best_effort())?;
    engine.flush_cf(&cf)?;
    let start = engine.read_path_diagnostics_snapshot_for_benchmarks();

    // Act
    let read = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
    let rows: Vec<_> = read.scan(&Query::new())?.collect();
    let end = engine.read_path_diagnostics_snapshot_for_benchmarks();

    // Assert
    assert_eq!(rows.len(), 2);
    assert!(end.candidate_sst_files_checked > start.candidate_sst_files_checked);
    assert!(end.candidate_blocks_checked > start.candidate_blocks_checked);
    assert!(end.data_blocks_read > start.data_blocks_read);
    assert!(end.range_tombstone_scans > start.range_tombstone_scans);
    Ok(())
}
