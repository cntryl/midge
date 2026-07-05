//! Tier 1 — SST Encoding Hot Path Benchmarks
//!
//! Target: < 500ms total runtime
//! Frequency: Every PR (CI gate)
//!
//! Hotpath = operations that occur on most Get/Put cycles:
//! - encode single SST entry (TLV format)
//! - decode single SST entry
//! - roundtrip encode→decode

use cntryl_stress::{black_box, stress_main, stress_test, StressContext};

use cntryl_midge::sst::encoding::{decode, encode, EntryType};

cntryl_stress::stress_allocator!();

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
    metadata(component = "sst_encoding", scenario = "decode_small")
)]
fn decode_small(ctx: &mut StressContext) {
    let prev = b"user:1000:settings";
    let key = b"user:1000:profile";
    let shared_len = shared_prefix_len(prev, key);
    let shared = u16::try_from(shared_len).expect("shared prefix length fits in u16");
    let delta = &key[shared_len..];

    let encoded = encode(delta, shared, Some(b"value"), 1, EntryType::Put);

    ctx.measure_micro(|| {
        black_box(decode(&encoded, 0).unwrap());
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
