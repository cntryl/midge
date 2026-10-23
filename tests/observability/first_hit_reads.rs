//! Point reads stop at the newest source that decides the key (#903). Level
//! order (SST-10) guarantees every older source holds only older versions, so
//! a hit in the memtable touches no SST and a hit in L0 touches one.

use cntryl_midge::{
    ColumnFamilyHandle, Engine, MidgeResult, OpenOptions, ReadAmpMetricsSnapshot, TransactionMode,
    WriteOptions,
};
use tempfile::TempDir;

const KEY: &[u8] = b"key:0001";

fn open_local_without_compaction(temp_dir: &TempDir) -> Engine {
    Engine::open(
        OpenOptions::local(temp_dir.path())
            .background_compaction(false)
            .build()
            .expect("build local options"),
    )
    .expect("open local engine")
}

fn put(engine: &Engine, cf: &ColumnFamilyHandle, key: &[u8], value: &[u8]) -> MidgeResult<()> {
    let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    tx.put(key.to_vec(), value.to_vec(), None)?;
    tx.commit(WriteOptions::buffered())?;
    Ok(())
}

fn delete(engine: &Engine, cf: &ColumnFamilyHandle, key: &[u8]) -> MidgeResult<()> {
    let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    tx.delete(key.to_vec())?;
    tx.commit(WriteOptions::buffered())?;
    Ok(())
}

fn delete_range(
    engine: &Engine,
    cf: &ColumnFamilyHandle,
    start: &[u8],
    end: &[u8],
) -> MidgeResult<()> {
    let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    tx.delete_range(start.to_vec(), end.to_vec())?;
    tx.commit(WriteOptions::buffered())?;
    Ok(())
}

/// Write `key` and move it to L1: flush produces the L0 file, `compact_all`
/// moves it down.
fn seed_l1_value(engine: &Engine, cf: &ColumnFamilyHandle, value: &[u8]) -> MidgeResult<()> {
    put(engine, cf, KEY, value)?;
    engine.flush_cf(cf)?;
    engine.compact_all()
}

fn read_amp_after_one_read(
    engine: &Engine,
    cf: &ColumnFamilyHandle,
    expected: Option<&[u8]>,
) -> MidgeResult<ReadAmpMetricsSnapshot> {
    let before = engine.metrics().get_read_amp_metrics()?;
    let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
    assert_eq!(tx.get(KEY)?.as_deref(), expected);
    let after = engine.metrics().get_read_amp_metrics()?;
    assert_eq!(after.reads_total, before.reads_total + 1);
    Ok(ReadAmpMetricsSnapshot {
        reads_total: 1,
        ssts_touched_total: after.ssts_touched_total - before.ssts_touched_total,
        l0_ssts_touched_total: after.l0_ssts_touched_total - before.l0_ssts_touched_total,
        blocks_read_total: after.blocks_read_total - before.blocks_read_total,
        ..after
    })
}

#[test]
fn should_touch_zero_ssts_given_memtable_holds_newer_version_when_point_read() -> MidgeResult<()> {
    // Arrange: v1 lives in L1; v2 is in the active memtable.
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = open_local_without_compaction(&temp_dir);
    let cf = engine.create_column_family("first-hit")?;
    seed_l1_value(&engine, &cf, b"v1")?;
    put(&engine, &cf, KEY, b"v2")?;

    // Act
    let delta = read_amp_after_one_read(&engine, &cf, Some(b"v2"))?;

    // Assert
    assert_eq!(delta.ssts_touched_total, 0);
    assert_eq!(delta.blocks_read_total, 0);
    Ok(())
}

#[test]
fn should_touch_zero_ssts_given_memtable_range_tombstone_covers_key_when_point_read(
) -> MidgeResult<()> {
    // Arrange: the value lives in L1; a range tombstone in the memtable covers
    // its key.
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = open_local_without_compaction(&temp_dir);
    let cf = engine.create_column_family("first-hit")?;
    seed_l1_value(&engine, &cf, b"v1")?;
    delete_range(&engine, &cf, b"key:0000", b"key:0009")?;

    // Act
    let delta = read_amp_after_one_read(&engine, &cf, None)?;

    // Assert
    assert_eq!(delta.ssts_touched_total, 0);
    Ok(())
}

#[test]
fn should_touch_one_sst_given_l0_tombstone_over_l1_value_when_point_read() -> MidgeResult<()> {
    // Arrange: the value lives in L1; a point tombstone was flushed to L0.
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = open_local_without_compaction(&temp_dir);
    let cf = engine.create_column_family("first-hit")?;
    seed_l1_value(&engine, &cf, b"v1")?;
    delete(&engine, &cf, KEY)?;
    engine.flush_cf(&cf)?;

    // Act
    let delta = read_amp_after_one_read(&engine, &cf, None)?;

    // Assert
    assert_eq!(delta.ssts_touched_total, 1);
    assert_eq!(delta.l0_ssts_touched_total, 1);
    Ok(())
}

#[test]
fn should_touch_every_candidate_sst_given_absent_key_when_point_read() -> MidgeResult<()> {
    // Arrange (control): two L0 files cover the key's range but neither holds
    // it, so no source decides the read and every candidate is visited.
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = open_local_without_compaction(&temp_dir);
    let cf = engine.create_column_family("first-hit")?;
    for generation in 0..2_u8 {
        let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
        tx.put(b"key:0000".to_vec(), vec![generation], None)?;
        tx.put(b"key:0002".to_vec(), vec![generation], None)?;
        tx.commit(WriteOptions::buffered())?;
        engine.flush_cf(&cf)?;
    }

    // Act
    let delta = read_amp_after_one_read(&engine, &cf, None)?;

    // Assert
    assert_eq!(delta.ssts_touched_total, 2);
    Ok(())
}
