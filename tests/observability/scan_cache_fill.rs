//! Range scans do not evict the point-read working set from the block cache
//! (#944): by default a scan's block reads bypass cache insertion, and a scan
//! can opt back in.

use cntryl_midge::{
    ColumnFamilyHandle, Engine, MemoryBudget, OpenOptions, Query, TransactionMode, WriteOptions,
};
use tempfile::TempDir;

const VALUE_BYTES: usize = 4096;
const KEYS_PER_SST: usize = 128;
const SST_COUNT: usize = 8;

fn key(index: usize) -> Vec<u8> {
    format!("key:{index:06}").into_bytes()
}

/// A small block cache under eight SSTs whose data blocks exceed it several
/// times over.
fn open_small_cache_engine(temp_dir: &TempDir) -> (Engine, ColumnFamilyHandle) {
    let engine = Engine::open(
        OpenOptions::local(temp_dir.path())
            .memory_budget(MemoryBudget::Bytes(8 * 1024 * 1024))
            .background_compaction(false)
            .build()
            .expect("build local options"),
    )
    .expect("open local engine");
    let cf = engine
        .create_column_family("scan-cache")
        .expect("create column family");
    for sst in 0..SST_COUNT {
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin write");
        for index in 0..KEYS_PER_SST {
            tx.put(
                key(sst * KEYS_PER_SST + index),
                vec![u8::try_from(sst).expect("sst index"); VALUE_BYTES],
                None,
            )
            .expect("put");
        }
        tx.commit(WriteOptions::buffered()).expect("commit");
        engine.flush_cf(&cf).expect("flush");
    }
    (engine, cf)
}

fn block_cache_hits(engine: &Engine) -> u64 {
    engine
        .read_path_diagnostics_snapshot_for_benchmarks()
        .sst_block_cache_hits
}

fn point_read(engine: &Engine, cf: &ColumnFamilyHandle, index: usize) {
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin read");
    let value = tx.get(&key(index)).expect("get").expect("key present");
    assert_eq!(value.len(), VALUE_BYTES);
}

#[test]
fn should_keep_hot_block_cached_given_full_scan_when_scan_completes() {
    // Arrange: the hot key's block is in the cache; the scan covers every
    // block of every SST, many times the cache's capacity.
    let temp_dir = TempDir::new().expect("temp dir");
    let (engine, cf) = open_small_cache_engine(&temp_dir);
    point_read(&engine, &cf, 0);
    point_read(&engine, &cf, 0);

    // Act
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin scan");
    let rows = tx
        .scan(&Query::new())
        .expect("scan")
        .try_collect()
        .expect("scan rows");
    let hits_before = block_cache_hits(&engine);
    point_read(&engine, &cf, 0);
    let hits_after = block_cache_hits(&engine);

    // Assert: the scan did not evict the hot block.
    assert_eq!(rows.len(), SST_COUNT * KEYS_PER_SST);
    assert_eq!(
        hits_after - hits_before,
        1,
        "the hot key's block was evicted by the scan"
    );
}
