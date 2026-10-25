//! The borrowed raw-value WAL encoder avoids the owned intermediate value
//! allocation used by the previous construction path (#947).

use cntryl_midge::__internal::codec::compress_wal_value;
use cntryl_midge::__internal::wal::encoding::encode_into;
use cntryl_midge::__internal::wal::{WalOpKind, WalRecord};
use cntryl_midge::Bytes;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

struct CountingAllocator;

static TRACK_ALLOCATIONS: AtomicBool = AtomicBool::new(false);
static ALLOCATION_COUNT: AtomicU64 = AtomicU64::new(0);

// SAFETY: every allocator operation delegates to System unchanged; the test
// counter observes allocation calls only while the encoder is measured.
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
const SAMPLES: usize = 16;

fn measured_allocations(run: impl FnOnce()) -> u64 {
    ALLOCATION_COUNT.store(0, Ordering::Relaxed);
    TRACK_ALLOCATIONS.store(true, Ordering::SeqCst);
    run();
    TRACK_ALLOCATIONS.store(false, Ordering::SeqCst);
    ALLOCATION_COUNT.load(Ordering::Relaxed)
}

#[test]
fn should_avoid_owned_intermediate_allocation_for_incompressible_raw_wal_values() {
    // Arrange: this deterministic high-entropy value stays raw in the WAL.
    let value = Bytes::from(
        (0..VALUE_LEN)
            .map(|index| u8::try_from(index % 251).expect("byte alphabet"))
            .collect::<Vec<_>>(),
    );
    let record = WalRecord::new_cf(
        7,
        WalOpKind::Put,
        Bytes::from_static(b"key"),
        Some(value),
        19,
    );
    let mut borrowed_payload = Vec::with_capacity(VALUE_LEN + 128);
    let mut owned_payload = Vec::with_capacity(VALUE_LEN + 128);

    // Act: count allocations only around the production encoder with reusable
    // output capacity. The owned reference reproduces the former intermediate
    // `Bytes::copy_from_slice` before the same production encoder call.
    let borrowed_allocations = measured_allocations(|| {
        for _ in 0..SAMPLES {
            borrowed_payload.clear();
            encode_into(&record, &mut borrowed_payload).expect("encode borrowed raw value");
            std::hint::black_box(&borrowed_payload);
        }
    });
    let mut owned_record = record.clone();
    let owned_allocations = measured_allocations(|| {
        for _ in 0..SAMPLES {
            let (owned_value, marker) = compress_wal_value(record.value.as_ref().unwrap());
            assert_eq!(marker, None, "fixture must remain uncompressed");
            owned_record.value = Some(owned_value);
            owned_payload.clear();
            encode_into(&owned_record, &mut owned_payload).expect("encode owned raw value");
            std::hint::black_box(&owned_payload);
        }
    });

    // Assert: both paths emit identical bytes, while only the former owned
    // intermediate allocates once per 4 KiB raw value.
    assert_eq!(borrowed_payload, owned_payload);
    assert_eq!(borrowed_allocations, 0);
    assert_eq!(owned_allocations, SAMPLES as u64);
}
