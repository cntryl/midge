//! Regression coverage for benchmark read-path diagnostics.

mod common;

use cntryl_midge::diagnostics::read_path_diagnostics_snapshot_for_benchmarks;
use cntryl_midge::{MidgeResult, TransactionMode, WriteOptions};
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
    let start = read_path_diagnostics_snapshot_for_benchmarks();

    // Act - the first read opens/populates caches; the second verifies hits.
    for _ in 0..2 {
        let read = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
        assert_eq!(read.get(b"key")?.as_deref(), Some(b"value".as_slice()));
    }
    let end = read_path_diagnostics_snapshot_for_benchmarks();

    // Assert - only the measured window contributes to each delta.
    assert!(end.read_only_begin_tx_count > start.read_only_begin_tx_count);
    assert!(end.read_only_snapshot_cache_hits > start.read_only_snapshot_cache_hits);
    assert!(end.sst_reader_cache_hits > start.sst_reader_cache_hits);
    assert!(end.sst_block_cache_hits > start.sst_block_cache_hits);
    assert!(end.candidate_blocks_checked > start.candidate_blocks_checked);
    assert!(end.data_blocks_read > start.data_blocks_read);
    Ok(())
}
