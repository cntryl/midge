//! Scans that bypass the block cache read ahead through the reader's file
//! handle (#945). These scans cross several readahead windows in both
//! directions and compare against the keys that were written, so a window that
//! was mis-sliced, mis-clipped or replaced at the wrong offset would show up as
//! a wrong, missing or duplicated row.

use cntryl_midge::{Engine, OpenOptions, Query, TransactionMode, WriteOptions};
use tempfile::TempDir;

/// Two SSTs of about 450 KiB each, so a full scan replaces its 256 KiB
/// readahead window several times per file.
const KEYS_PER_SST: usize = 6_000;
const VALUE_BYTES: usize = 64;

fn key(index: usize) -> Vec<u8> {
    format!("key:{index:08}").into_bytes()
}

fn value(index: usize) -> Vec<u8> {
    let mut value = index.to_le_bytes().to_vec();
    value.resize(VALUE_BYTES, b'v');
    value
}

fn open_two_sst_engine(temp_dir: &TempDir) -> (Engine, cntryl_midge::ColumnFamilyHandle) {
    let engine = Engine::open(
        OpenOptions::local(temp_dir.path())
            .background_compaction(false)
            .build()
            .expect("build local options"),
    )
    .expect("open local engine");
    let cf = engine
        .create_column_family("readahead")
        .expect("create column family");
    for sst in 0..2 {
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin write");
        for index in 0..KEYS_PER_SST {
            let global = index * 2 + sst;
            tx.put(key(global), value(global), None).expect("put");
        }
        tx.commit(WriteOptions::buffered()).expect("commit");
        engine.flush_cf(&cf).expect("flush");
    }
    (engine, cf)
}

fn collect(engine: &Engine, cf: &cntryl_midge::ColumnFamilyHandle, query: &Query) -> Vec<usize> {
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin read");
    tx.scan(query)
        .expect("scan")
        .try_collect()
        .expect("rows")
        .into_iter()
        .map(|(key, value)| {
            let text = std::str::from_utf8(&key).expect("utf8 key");
            let index: usize = text["key:".len()..].parse().expect("key index");
            assert_eq!(value, self::value(index), "value of {text}");
            index
        })
        .collect()
}

#[test]
fn should_return_every_row_in_order_given_multi_window_ssts_when_scanning_forward() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let (engine, cf) = open_two_sst_engine(&temp_dir);

    // Act
    let rows = collect(&engine, &cf, &Query::new());

    // Assert
    let expected: Vec<usize> = (0..KEYS_PER_SST * 2).collect();
    assert_eq!(rows, expected);
}

#[test]
fn should_return_every_row_in_order_given_multi_window_ssts_when_scanning_in_reverse() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let (engine, cf) = open_two_sst_engine(&temp_dir);

    // Act
    let rows = collect(&engine, &cf, &Query::new().reverse());

    // Assert
    let expected: Vec<usize> = (0..KEYS_PER_SST * 2).rev().collect();
    assert_eq!(rows, expected);
}

#[test]
fn should_return_exactly_the_bounded_range_given_no_limit_when_scanning_both_directions() {
    // Arrange: a bounded range without a limit bypasses the cache and reads
    // ahead, but only within its own blocks.
    let temp_dir = TempDir::new().expect("temp dir");
    let (engine, cf) = open_two_sst_engine(&temp_dir);
    let bounded = Query::new()
        .start_key(key(5_000).into())
        .end_key(key(5_100).into());

    // Act
    let forward = collect(&engine, &cf, &bounded);
    let reverse = collect(&engine, &cf, &bounded.clone().reverse());

    // Assert
    let expected: Vec<usize> = (5_000..5_100).collect();
    assert_eq!(forward, expected);
    assert_eq!(reverse, expected.iter().rev().copied().collect::<Vec<_>>());
}
