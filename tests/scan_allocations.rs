//! A long range scan allocates per block, not per row (#945): decoded keys are
//! sliced from a block-level arena and the merge compares source heads by
//! reference, so emitting a row costs no heap allocation of its own.

use cntryl_midge::{Engine, OpenOptions, Query, TransactionMode, WriteOptions};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};
use tempfile::TempDir;

/// Counts allocation events; growth through `realloc` counts as one.
struct CountingAllocator;

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);

// SAFETY: every operation delegates to `System` unchanged; the counter is an
// atomic bumped before the result is returned.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if new_size > layout.size() {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

const SST_COUNT: usize = 4;
const KEYS_PER_SST: usize = 2_000;
const MEMTABLE_KEYS: usize = 1_000;

fn key(index: usize) -> Vec<u8> {
    format!("key:{index:08}").into_bytes()
}

/// Four SSTs with interleaved keys, plus a memtable tail, so the merge has
/// several live sources at every step.
fn open_scan_fixture(temp_dir: &TempDir) -> (Engine, cntryl_midge::ColumnFamilyHandle) {
    let engine = Engine::open(
        OpenOptions::local(temp_dir.path())
            .background_compaction(false)
            .build()
            .expect("build local options"),
    )
    .expect("open local engine");
    let cf = engine
        .create_column_family("scan-allocations")
        .expect("create column family");
    for sst in 0..SST_COUNT {
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin write");
        for index in 0..KEYS_PER_SST {
            tx.put(key(index * SST_COUNT + sst), vec![b'v'; 32], None)
                .expect("put");
        }
        tx.commit(WriteOptions::buffered()).expect("commit");
        engine.flush_cf(&cf).expect("flush");
    }
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin write");
    for index in 0..MEMTABLE_KEYS {
        tx.put(key(SST_COUNT * KEYS_PER_SST + index), vec![b'm'; 32], None)
            .expect("put");
    }
    tx.commit(WriteOptions::buffered()).expect("commit");
    (engine, cf)
}

#[test]
fn should_allocate_less_than_once_per_row_given_many_sources_when_scanning_everything() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let (engine, cf) = open_scan_fixture(&temp_dir);
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin scan");
    let mut rows = tx.scan(&Query::new()).expect("scan");
    let expected_rows = SST_COUNT * KEYS_PER_SST + MEMTABLE_KEYS;

    // Act: count allocations while rows are pulled, without collecting them.
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    let mut count = 0_usize;
    while let Some(row) = rows.next() {
        let (key, value) = row.expect("row");
        std::hint::black_box((key.len(), value.len()));
        count += 1;
    }
    let allocations = ALLOCATIONS.load(Ordering::Relaxed) - before;

    // Assert: the per-row cost is amortized block work, not a key copy or a
    // merge clone per row.
    assert_eq!(count, expected_rows);
    let per_row = allocations as f64 / count as f64;
    assert!(
        per_row < 1.0,
        "{allocations} allocations for {count} rows ({per_row:.2} per row)"
    );
}
