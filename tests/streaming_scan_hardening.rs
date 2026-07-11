//! Hardening contract for lazy, fallible transaction scans.

use bytes::Bytes;
use cntryl_midge::{Engine, Goal, MidgeResult, OpenOptions, Query, TransactionMode, WriteOptions};
use std::path::Path;
use std::time::Duration;
use tempfile::TempDir;

#[test]
fn should_stop_prefix_scan_before_neighboring_prefix() -> MidgeResult<()> {
    // Arrange
    let mut engine = open_memory()?;
    let cf = default_cf(&engine);
    seed_rows(
        &engine,
        cf.id(),
        &[(b"aa:1", b"one"), (b"aa:2", b"two"), (b"ab:0", b"other")],
    )?;
    let read = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;

    // Act
    let rows = read
        .scan(&Query::new().prefix(Bytes::from_static(b"aa:")))?
        .try_collect()?;

    // Assert
    assert_eq!(keys(&rows), vec![b"aa:1".to_vec(), b"aa:2".to_vec()]);
    drop(read);
    engine.shutdown(Duration::from_secs(2))?;
    Ok(())
}

#[test]
fn should_intersect_prefix_with_explicit_end_bound() -> MidgeResult<()> {
    // Arrange
    let mut engine = open_memory()?;
    let cf = default_cf(&engine);
    seed_rows(
        &engine,
        cf.id(),
        &[
            (b"scan:1", b"one"),
            (b"scan:2", b"two"),
            (b"scan:3", b"three"),
            (b"scao:0", b"neighbor"),
        ],
    )?;
    let read = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
    let query = Query::new()
        .prefix(Bytes::from_static(b"scan:"))
        .end_key(Bytes::from_static(b"scan:2"));

    // Act
    let rows = read.scan(&query)?.try_collect()?;

    // Assert
    assert_eq!(keys(&rows), vec![b"scan:1".to_vec()]);
    drop(read);
    engine.shutdown(Duration::from_secs(2))?;
    Ok(())
}

#[test]
fn should_scan_binary_prefix_without_finite_successor() -> MidgeResult<()> {
    // Arrange
    let mut engine = open_memory()?;
    let cf = default_cf(&engine);
    let mut write = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    for key in [
        vec![0xFE, 0xFF],
        vec![0xFF],
        vec![0xFF, 0x00],
        vec![0xFF, 0xFF, 0x01],
    ] {
        write.put(key, b"value".to_vec(), None)?;
    }
    write.commit(WriteOptions::sync())?;
    let read = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;

    // Act
    let rows = read
        .scan(&Query::new().prefix(Bytes::from_static(&[0xFF])))?
        .try_collect()?;

    // Assert
    assert_eq!(
        keys(&rows),
        vec![vec![0xFF], vec![0xFF, 0x00], vec![0xFF, 0xFF, 0x01]]
    );
    drop(read);
    engine.shutdown(Duration::from_secs(2))?;
    Ok(())
}

#[test]
fn should_read_only_one_candidate_block_for_forward_limit() -> MidgeResult<()> {
    // Arrange
    let temp = TempDir::new()?;
    let mut engine = open_local(temp.path())?;
    let cf = default_cf(&engine);
    seed_flushed_blocks(&engine, cf.id(), 24)?;
    let before_construction = engine.read_path_diagnostics_snapshot_for_benchmarks();
    let read = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;

    // Act
    let mut scan = read.scan(&Query::new().limit(1))?;
    let after_construction = engine.read_path_diagnostics_snapshot_for_benchmarks();
    let first = scan.next().transpose()?.expect("first row");
    let after_first = engine.read_path_diagnostics_snapshot_for_benchmarks();

    // Assert
    assert_eq!(first.0, Bytes::from_static(b"block-000"));
    assert_eq!(
        after_construction.data_blocks_read, before_construction.data_blocks_read,
        "iterator construction must not read an SST data block"
    );
    assert!(
        after_first
            .data_blocks_read
            .saturating_sub(after_construction.data_blocks_read)
            <= 2,
        "limit(1) read too many data blocks: before={after_construction:?} after={after_first:?}"
    );
    drop(scan);
    drop(read);
    engine.shutdown(Duration::from_secs(2))?;
    Ok(())
}

#[test]
fn should_read_only_one_candidate_block_for_reverse_limit() -> MidgeResult<()> {
    // Arrange
    let temp = TempDir::new()?;
    let mut engine = open_local(temp.path())?;
    let cf = default_cf(&engine);
    seed_flushed_blocks(&engine, cf.id(), 24)?;
    let read = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
    let before = engine.read_path_diagnostics_snapshot_for_benchmarks();

    // Act
    let mut scan = read.scan(&Query::new().reverse().limit(1))?;
    let first = scan.next().transpose()?.expect("last row");
    let after = engine.read_path_diagnostics_snapshot_for_benchmarks();

    // Assert
    assert_eq!(first.0, Bytes::from_static(b"block-023"));
    assert!(
        after
            .data_blocks_read
            .saturating_sub(before.data_blocks_read)
            <= 2,
        "reverse limit read too many data blocks: before={before:?} after={after:?}"
    );
    drop(scan);
    drop(read);
    engine.shutdown(Duration::from_secs(2))?;
    Ok(())
}

#[test]
fn should_surface_corrupt_later_sst_block_from_iterator_item() -> MidgeResult<()> {
    // Arrange
    let temp = TempDir::new()?;
    let mut engine = open_local(temp.path())?;
    let cf = default_cf(&engine);
    seed_flushed_blocks(&engine, cf.id(), 8)?;
    let read = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
    let mut scan = read.scan(&Query::new())?;
    corrupt_second_sst_data_block(temp.path())?;

    // Act
    let first = scan.next().transpose()?.expect("row before corruption");
    let late_error = scan
        .find_map(Result::err)
        .expect("corrupt later block must surface from an iterator item");

    // Assert
    assert_eq!(first.0, Bytes::from_static(b"block-000"));
    assert!(
        late_error
            .to_string()
            .to_ascii_lowercase()
            .contains("corrupt")
            || late_error.to_string().contains("CRC32C"),
        "expected corruption error, got {late_error}"
    );
    drop(scan);
    drop(read);
    engine.shutdown(Duration::from_secs(2))?;
    Ok(())
}

#[test]
fn should_preserve_snapshot_visibility_during_streaming_scan() -> MidgeResult<()> {
    // Arrange
    let mut engine = open_memory()?;
    let cf = default_cf(&engine);
    seed_rows(&engine, cf.id(), &[(b"stable", b"old")])?;
    let old_snapshot = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
    seed_rows(
        &engine,
        cf.id(),
        &[(b"stable", b"new"), (b"new-key", b"new")],
    )?;

    // Act
    let rows = old_snapshot.scan(&Query::new())?.try_collect()?;

    // Assert
    assert_eq!(
        rows,
        vec![(Bytes::from_static(b"stable"), Bytes::from_static(b"old"))]
    );
    drop(old_snapshot);
    engine.shutdown(Duration::from_secs(2))?;
    Ok(())
}

#[test]
fn should_apply_range_tombstones_before_streaming_rows() -> MidgeResult<()> {
    // Arrange
    let mut engine = open_memory()?;
    let cf = default_cf(&engine);
    seed_rows(
        &engine,
        cf.id(),
        &[(b"range:a", b"a"), (b"range:b", b"b"), (b"range:c", b"c")],
    )?;
    let mut delete = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    delete.delete_range(b"range:b".to_vec(), b"range:d".to_vec())?;
    delete.commit(WriteOptions::sync())?;
    let read = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;

    // Act
    let rows = read.scan(&Query::new())?.try_collect()?;

    // Assert
    assert_eq!(keys(&rows), vec![b"range:a".to_vec()]);
    drop(read);
    engine.shutdown(Duration::from_secs(2))?;
    Ok(())
}

fn open_memory() -> MidgeResult<Engine> {
    Engine::open(OpenOptions::in_memory().build()?)
}

fn open_local(path: &Path) -> MidgeResult<Engine> {
    Engine::open(
        OpenOptions::local(path)
            .goal(Goal::Latency)
            .background_compaction(false)
            .build()?,
    )
}

fn default_cf(engine: &Engine) -> cntryl_midge::ColumnFamilyHandle {
    engine
        .get_column_family("default")
        .expect("default column family")
}

fn seed_rows(engine: &Engine, cf_id: u32, rows: &[(&[u8], &[u8])]) -> MidgeResult<()> {
    let mut write = engine.begin_tx(cf_id, TransactionMode::ReadWrite)?;
    for (key, value) in rows {
        write.put(key.to_vec(), value.to_vec(), None)?;
    }
    write.commit(WriteOptions::sync())
}

fn seed_flushed_blocks(engine: &Engine, cf_id: u32, count: usize) -> MidgeResult<()> {
    let mut write = engine.begin_tx(cf_id, TransactionMode::ReadWrite)?;
    for index in 0..count {
        write.put(
            format!("block-{index:03}").into_bytes(),
            vec![b'x'; 8 * 1024],
            None,
        )?;
    }
    write.commit(WriteOptions::sync())?;
    let cf = engine
        .get_column_family("default")
        .expect("default column family");
    engine.flush_cf(&cf)
}

fn corrupt_second_sst_data_block(db_path: &Path) -> MidgeResult<()> {
    use std::io::{Read, Seek, Write};

    let sst_path = std::fs::read_dir(db_path.join("sst"))?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| path.extension().is_some_and(|extension| extension == "sst"))
        .expect("flushed SST file");
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(sst_path)?;
    let mut length = [0_u8; 4];
    file.read_exact(&mut length)?;
    let second_block = 4_u64.saturating_add(u64::from(u32::from_le_bytes(length)));
    file.seek(std::io::SeekFrom::Start(second_block + 4))?;
    let mut byte = [0_u8; 1];
    file.read_exact(&mut byte)?;
    file.seek(std::io::SeekFrom::Start(second_block + 4))?;
    file.write_all(&[byte[0] ^ 0x01])?;
    file.sync_all()?;
    Ok(())
}

fn keys(rows: &[(Bytes, Bytes)]) -> Vec<Vec<u8>> {
    rows.iter().map(|(key, _)| key.to_vec()).collect()
}
