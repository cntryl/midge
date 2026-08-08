use cntryl_midge::{
    ColumnFamilyHandle, Engine, MidgeResult, OpenOptions, TransactionMode, WriteOptions,
};
use tempfile::TempDir;

fn open_local_without_compaction(temp_dir: &TempDir) -> Engine {
    Engine::open(
        OpenOptions::local(temp_dir.path())
            .background_compaction(false)
            .build()
            .expect("build local options"),
    )
    .expect("open local engine")
}

fn flush_hot_key_generation(
    engine: &Engine,
    cf: &ColumnFamilyHandle,
    generation: usize,
) -> MidgeResult<()> {
    let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    tx.put(
        b"hot-key".to_vec(),
        format!("value-{generation:02}").into_bytes(),
        None,
    )?;
    tx.commit(WriteOptions::buffered())?;
    engine.flush_cf(cf)
}

#[test]
fn should_expose_exact_read_amp_metrics_for_local_sst_reads() -> MidgeResult<()> {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = open_local_without_compaction(&temp_dir);
    let cf = engine.create_column_family("read-amp")?;
    flush_hot_key_generation(&engine, &cf, 0)?;
    let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;

    // Act
    for _ in 0..5 {
        assert_eq!(tx.get(b"hot-key")?.as_deref(), Some(b"value-00".as_slice()));
    }
    let metrics = engine.get_read_amp_metrics()?;

    // Assert
    assert_eq!(metrics.reads_total, 5);
    assert_eq!(metrics.ssts_touched_total, 5);
    assert_eq!(metrics.l0_ssts_touched_total, 5);
    assert_eq!(metrics.blocks_read_total, 10);
    assert!((metrics.avg_ssts_per_read - 1.0).abs() <= f64::EPSILON);
    assert!((metrics.avg_l0_ssts_per_read - 1.0).abs() <= f64::EPSILON);
    assert!((metrics.avg_blocks_per_read - 2.0).abs() <= f64::EPSILON);
    assert!((metrics.l0_overlap_rate - 1.0).abs() <= f64::EPSILON);
    Ok(())
}

#[test]
fn should_track_exact_l0_overlap_across_overlapping_ssts() -> MidgeResult<()> {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = open_local_without_compaction(&temp_dir);
    let cf = engine.create_column_family("l0-overlap")?;
    for generation in 0..3 {
        flush_hot_key_generation(&engine, &cf, generation)?;
    }
    let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;

    // Act
    let value = tx.get(b"hot-key")?;
    let metrics = engine.get_read_amp_metrics()?;

    // Assert
    assert_eq!(value.as_deref(), Some(b"value-02".as_slice()));
    assert_eq!(metrics.reads_total, 1);
    assert_eq!(metrics.ssts_touched_total, 3);
    assert_eq!(metrics.l0_ssts_touched_total, 3);
    assert!((metrics.avg_ssts_per_read - 3.0).abs() <= f64::EPSILON);
    assert!((metrics.avg_l0_ssts_per_read - 3.0).abs() <= f64::EPSILON);
    assert!((metrics.l0_overlap_rate - 1.0).abs() <= f64::EPSILON);
    Ok(())
}

#[test]
fn should_show_zero_read_amp_metrics_for_fresh_engine() -> MidgeResult<()> {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = open_local_without_compaction(&temp_dir);

    // Act
    let metrics = engine.get_read_amp_metrics()?;

    // Assert
    assert_eq!(metrics.reads_total, 0);
    assert_eq!(metrics.ssts_touched_total, 0);
    assert_eq!(metrics.l0_ssts_touched_total, 0);
    assert_eq!(metrics.blocks_read_total, 0);
    assert!(metrics.avg_ssts_per_read.abs() <= f64::EPSILON);
    assert!(metrics.avg_l0_ssts_per_read.abs() <= f64::EPSILON);
    assert!(metrics.avg_blocks_per_read.abs() <= f64::EPSILON);
    assert!(metrics.l0_overlap_rate.abs() <= f64::EPSILON);
    assert!(metrics.sst_budget_violation_rate.abs() <= f64::EPSILON);
    assert!(metrics.block_budget_violation_rate.abs() <= f64::EPSILON);
    Ok(())
}

#[test]
fn should_accumulate_exact_average_across_multiple_local_sst_reads() -> MidgeResult<()> {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = open_local_without_compaction(&temp_dir);
    let cf = engine.create_column_family("read-average")?;
    for generation in 0..2 {
        flush_hot_key_generation(&engine, &cf, generation)?;
    }
    let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;

    // Act
    for _ in 0..10 {
        let _ = tx.get(b"hot-key")?;
    }
    let metrics = engine.get_read_amp_metrics()?;

    // Assert
    assert_eq!(metrics.reads_total, 10);
    assert_eq!(metrics.ssts_touched_total, 20);
    assert_eq!(metrics.l0_ssts_touched_total, 20);
    assert!((metrics.avg_ssts_per_read - 2.0).abs() <= f64::EPSILON);
    assert!((metrics.avg_l0_ssts_per_read - 2.0).abs() <= f64::EPSILON);
    Ok(())
}

#[test]
fn should_report_budget_violations_for_one_high_amplification_read() -> MidgeResult<()> {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = open_local_without_compaction(&temp_dir);
    let cf = engine.create_column_family("budget-violation")?;
    for generation in 0..11 {
        flush_hot_key_generation(&engine, &cf, generation)?;
    }
    let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;

    // Act
    let value = tx.get(b"hot-key")?;
    let metrics = engine.get_read_amp_metrics()?;

    // Assert
    assert_eq!(value.as_deref(), Some(b"value-10".as_slice()));
    assert_eq!(metrics.reads_total, 1);
    assert_eq!(metrics.ssts_touched_total, 11);
    assert_eq!(metrics.l0_ssts_touched_total, 11);
    assert_eq!(metrics.blocks_read_total, 22);
    assert!((metrics.sst_budget_violation_rate - 1.0).abs() <= f64::EPSILON);
    assert!((metrics.block_budget_violation_rate - 1.0).abs() <= f64::EPSILON);
    Ok(())
}
