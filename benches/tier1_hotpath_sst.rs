//! Tier 1 — SST Encoding Hot Path Benchmarks
//!
//! Target: < 500ms total runtime
//! Frequency: Every PR (CI gate)
//!
//! Hotpath = operations that occur on most Get/Put cycles:
//! - encode single SST entry (TLV format)
//! - decode single SST entry
//! - roundtrip encode→decode

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_stress::{black_box, stress_main, stress_test, StressContext};

use cntryl_midge::sst::encoding::{decode, encode, EntryType};

cntryl_stress::stress_allocator!();

const DECODE_BATCH_SIZE: usize = 256;

// ---------------------------------------------------------------------------
// Shared prefix helper (allocation-free)
// ---------------------------------------------------------------------------
fn shared_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

// ---------------------------------------------------------------------------
// HOTPATH 1: Encode single entry
// ---------------------------------------------------------------------------
#[stress_test(
    tier = 1,
    metadata(component = "sst_encoding", scenario = "encode_small")
)]
fn encode_small(ctx: &mut StressContext) {
    let prev = b"user:1000:settings";
    let key = b"user:1000:profile";
    let shared_len = shared_prefix_len(prev, key);
    let shared = u16::try_from(shared_len).expect("shared prefix length fits in u16");
    let delta = &key[shared_len..];

    let value = b"value_data";

    ctx.measure_micro(|| {
        black_box(encode(delta, shared, Some(value), 1, EntryType::Put));
    });
}

// ---------------------------------------------------------------------------
// HOTPATH 2: Decode single entry
// ---------------------------------------------------------------------------
#[stress_test(
    tier = 1,
    metadata(
        component = "sst_encoding",
        scenario = "decode_small",
        validated_micro = "true"
    )
)]
fn decode_small(ctx: &mut StressContext) {
    let prev = b"user:1000:settings";
    let key = b"user:1000:profile";
    let shared_len = shared_prefix_len(prev, key);
    let shared = u16::try_from(shared_len).expect("shared prefix length fits in u16");
    let delta = &key[shared_len..];

    let encoded_entries: Vec<Vec<u8>> = (0..DECODE_BATCH_SIZE)
        .map(|i| encode(delta, shared, Some(b"value"), i as u64 + 1, EntryType::Put))
        .collect();
    let (view, consumed) = decode(&encoded_entries[0], 0).unwrap();
    assert_eq!(view.shared_len, shared);
    assert_eq!(view.key_delta, delta);
    assert_eq!(view.value, Some(b"value".as_slice()));
    assert_eq!(consumed, encoded_entries[0].len());
    ctx.parameter("decode_batch_size", DECODE_BATCH_SIZE);

    stress_config::measure_micro_batch(ctx, DECODE_BATCH_SIZE as u64, || {
        let mut consumed_total = 0usize;
        for encoded in &encoded_entries {
            let (_view, consumed) = decode(black_box(encoded.as_slice()), 0).unwrap();
            consumed_total = consumed_total.wrapping_add(consumed);
        }
        black_box(consumed_total);
    });
}

// ---------------------------------------------------------------------------
// HOTPATH 4: Roundtrip (encode → decode 1 entry)
// ---------------------------------------------------------------------------
#[stress_test(
    tier = 1,
    metadata(component = "sst_encoding", scenario = "roundtrip_small")
)]
fn roundtrip_small(ctx: &mut StressContext) {
    let prev = b"user:1000:settings";
    let key = b"user:1000:profile";
    let shared_len = shared_prefix_len(prev, key);
    let shared = u16::try_from(shared_len).expect("shared prefix length fits in u16");
    let delta = &key[shared_len..];

    let value = b"value_data";

    ctx.measure_micro(|| {
        let encoded = encode(delta, shared, Some(value), 1, EntryType::Put);
        let _ = decode(&encoded, 0).unwrap();
        black_box(encoded);
    });
}

stress_main!();
