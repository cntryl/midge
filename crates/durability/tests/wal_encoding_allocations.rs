//! Allocation contracts for WAL payload encoding and decoding.

use bytes::{Buf, Bytes};
use cntryl_midge_core::codec::compress_wal_value;
use cntryl_midge_durability::wal::encoding::{decode, decode_view, encode, encode_into};
use cntryl_midge_durability::wal::sequenced::SequencedWalUnit;
use cntryl_midge_durability::wal::{WalOpKind, WalRecord};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, Ordering};

struct CountingAllocator;

thread_local! {
    static ALLOCATION_COUNT: Cell<Option<u64>> = const { Cell::new(None) };
}

fn record_allocation() {
    // Const, non-dropping TLS needs no allocation. Other threads and thread
    // teardown cannot contribute to the synchronous codec's observation.
    let _ = ALLOCATION_COUNT.try_with(|counter| {
        if let Some(count) = counter.get() {
            counter.set(Some(count + 1));
        }
    });
}

// SAFETY: every allocator operation delegates to System unchanged; the test
// counter observes allocation calls only on the measured codec thread.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if new_size > layout.size() {
            record_allocation();
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

const VALUE_LEN: usize = 4096;
const SAMPLES: usize = 16;

fn measured_allocations(run: impl FnOnce()) -> u64 {
    ALLOCATION_COUNT.with(|counter| counter.set(Some(0)));
    run();
    ALLOCATION_COUNT.with(|counter| counter.replace(None).expect("active codec measurement"))
}

fn raw_value() -> Bytes {
    Bytes::from(
        (0..VALUE_LEN)
            .map(|index| u8::try_from(index % 251).expect("byte alphabet"))
            .collect::<Vec<_>>(),
    )
}

#[test]
fn should_avoid_owned_intermediate_allocation_for_incompressible_raw_wal_values() {
    // Arrange: this deterministic high-entropy value stays raw in the WAL.
    let record = WalRecord::new_cf(
        7,
        WalOpKind::Put,
        Bytes::from_static(b"key"),
        Some(raw_value()),
        19,
    );
    let mut borrowed_payload = Vec::with_capacity(VALUE_LEN + 128);
    let mut owned_payload = Vec::with_capacity(VALUE_LEN + 128);
    let allocate_elsewhere = std::sync::Arc::new(AtomicBool::new(false));
    let allocation_finished = std::sync::Arc::new(AtomicBool::new(false));
    let unrelated_thread = {
        let allocate = allocate_elsewhere.clone();
        let finished = allocation_finished.clone();
        std::thread::spawn(move || {
            while !allocate.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            std::hint::black_box(vec![7_u8; VALUE_LEN]);
            finished.store(true, Ordering::Release);
        })
    };

    // Act: count allocations only around the production encoder with reusable
    // output capacity. The owned reference reproduces the former intermediate
    // `Bytes::copy_from_slice` before the same production encoder call.
    let borrowed_allocations = measured_allocations(|| {
        // Test-harness threads may allocate concurrently with this encoder.
        // Force that interleaving while preserving the exact encoder contract.
        allocate_elsewhere.store(true, Ordering::Release);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !allocation_finished.load(Ordering::Acquire) {
            assert!(
                std::time::Instant::now() < deadline,
                "foreign allocation stalled"
            );
            std::thread::yield_now();
        }
        for _ in 0..SAMPLES {
            borrowed_payload.clear();
            encode_into(&record, &mut borrowed_payload).expect("encode borrowed raw value");
            std::hint::black_box(&borrowed_payload);
        }
    });
    unrelated_thread
        .join()
        .expect("unrelated allocator completed");
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

#[test]
fn should_allocate_only_owned_fields_when_decoding_raw_wal_payload() {
    // Arrange
    let record = WalRecord::new_cf(
        7,
        WalOpKind::Put,
        Bytes::from_static(b"key"),
        Some(raw_value()),
        19,
    );
    let payload = encode(&record).expect("encode raw record");
    assert_eq!(
        decode_view(&payload).expect("read record view").compression,
        None
    );
    let mut input = payload.as_ref();
    let mut decoded = None;

    // Act
    let allocations = measured_allocations(|| {
        decoded = Some(decode(&mut input).expect("decode raw record"));
    });

    // Assert
    assert_eq!(decoded.expect("decoded record"), record);
    assert_eq!(input, b"");
    assert_eq!(
        allocations, 2,
        "only the owned key and value should allocate"
    );
}

#[test]
fn should_consume_only_contiguous_input_when_wal_decode_rejects_payload() {
    // Arrange
    let mut truncated = b"MW".as_slice();
    let mut split = b"M".as_slice().chain(b"W".as_slice());

    // Act
    let truncated_result = decode(&mut truncated);
    let split_result = decode(&mut split);

    // Assert
    assert!(truncated_result.is_err());
    assert_eq!(truncated, b"");
    assert!(split_result.is_err());
    assert_eq!(split.remaining(), 2);
}

#[test]
fn should_allocate_only_the_payload_when_sizing_and_encoding_a_sequenced_batch() {
    // Arrange
    let mut record = WalRecord::new_cf(
        7,
        WalOpKind::Put,
        Bytes::from_static(b"key"),
        Some(Bytes::from_static(b"value")),
        20,
    );
    record.txn_id = Some(4);
    let batch = SequencedWalUnit {
        txn_id: 4,
        begin_sequence: 19,
        commit_sequence: 21,
        cf_id: 7,
        operations: vec![record],
    };
    let mut len = 0;
    let mut encoded = None;

    // Act
    let sizing_allocations = measured_allocations(|| {
        len = batch.nested_payload_len().expect("size sequenced batch");
    });
    let encoding_allocations = measured_allocations(|| {
        encoded = Some(batch.encode_record().expect("encode sequenced batch"));
    });

    // Assert
    let payload = encoded
        .expect("encoded batch")
        .value
        .expect("batch payload");
    let expected = b"TB\x01\
        \x04\0\0\0\0\0\0\0\x13\0\0\0\0\0\0\0\x15\0\0\0\0\0\0\0\x01\0\0\0\
        \0\x07\0\0\0\x14\0\0\0\0\0\0\0\0\x03\0\0\0key\x01\x05\0\0\0value\0";
    assert_eq!(payload.as_ref(), expected);
    assert_eq!(payload.len(), len);
    assert_eq!(sizing_allocations, 0);
    assert_eq!(encoding_allocations, 1);
}
