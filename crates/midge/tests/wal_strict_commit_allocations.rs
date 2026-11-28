//! Allocation observations around public local Strict commits of raw 4 KiB
//! WAL values (#947).

use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

struct CountingAllocator;

static TRACK_ALLOCATIONS: AtomicBool = AtomicBool::new(false);
static ALLOCATION_COUNT: AtomicU64 = AtomicU64::new(0);

// SAFETY: allocation operations delegate to System unchanged; the counter is
// enabled only around the measured Strict transaction loop.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if TRACK_ALLOCATIONS.load(Ordering::Relaxed) {
            ALLOCATION_COUNT.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if TRACK_ALLOCATIONS.load(Ordering::Relaxed) {
            ALLOCATION_COUNT.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if TRACK_ALLOCATIONS.load(Ordering::Relaxed) && new_size > layout.size() {
            ALLOCATION_COUNT.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

const VALUE_LEN: usize = 4096;
const MEASURED_COMMITS: usize = 16;

fn raw_value(seed: u64) -> Vec<u8> {
    let mut state = seed;
    let mut value = Vec::with_capacity(VALUE_LEN);
    while value.len() < VALUE_LEN {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        value.extend_from_slice(&state.to_le_bytes());
    }
    value.truncate(VALUE_LEN);
    value
}

fn assert_raw_fixture(value: &[u8]) {
    let mut seen = [false; 256];
    for byte in value.iter().take(256) {
        seen[usize::from(*byte)] = true;
    }
    let distinct = seen.iter().filter(|present| **present).count();
    assert!(
        distinct > 128,
        "fixture must bypass the WAL compression probe"
    );
}

fn count_allocations(run: impl FnOnce()) -> u64 {
    ALLOCATION_COUNT.store(0, Ordering::Relaxed);
    TRACK_ALLOCATIONS.store(true, Ordering::SeqCst);
    run();
    TRACK_ALLOCATIONS.store(false, Ordering::SeqCst);
    ALLOCATION_COUNT.load(Ordering::Relaxed)
}

#[test]
fn should_measure_allocations_per_public_strict_commit_for_raw_four_kib_values() {
    // Arrange: open and warm the engine before enabling the process-wide
    // allocator counter. Input buffers are allocated before the measured loop.
    let directory = tempfile::tempdir().expect("create database directory");
    let mut engine = Engine::open(
        OpenOptions::local(directory.path())
            .build()
            .expect("build local options"),
    )
    .expect("open engine");
    let cf = engine
        .create_column_family("wal-allocation")
        .expect("create CF");
    let warmup_key = b"warmup".to_vec();
    let warmup_value = raw_value(1);
    assert_raw_fixture(&warmup_value);
    let mut warmup = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin warmup transaction");
    warmup
        .put(warmup_key, warmup_value, None)
        .expect("stage warmup value");
    warmup
        .commit(WriteOptions::sync())
        .expect("commit warmup value");

    let entries: Vec<_> = (0..MEASURED_COMMITS)
        .map(|ordinal| {
            let value = raw_value(u64::try_from(ordinal).expect("ordinal fits in u64") + 2);
            assert_raw_fixture(&value);
            (format!("measured-{ordinal:04}").into_bytes(), value)
        })
        .collect();

    // Act: count allocations through transaction creation, put admission, WAL
    // encoding, writer completion, and Strict acknowledgement.
    let allocation_count = count_allocations(|| {
        for (key, value) in &entries {
            let mut transaction = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin measured transaction");
            transaction
                .put(key.clone(), value.clone(), None)
                .expect("stage measured value");
            transaction
                .commit(WriteOptions::sync())
                .expect("commit measured value");
        }
    });

    // Assert: the public Strict path committed every exact raw input value.
    for (key, value) in &entries {
        let transaction = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin verification transaction");
        assert_eq!(
            transaction.get(key).expect("read committed value"),
            Some(bytes::Bytes::copy_from_slice(value))
        );
    }
    println!(
        "public_strict_commit_total_allocations={allocation_count} commits={MEASURED_COMMITS}"
    );
    engine
        .shutdown(Duration::from_secs(30))
        .expect("shutdown engine");
}
