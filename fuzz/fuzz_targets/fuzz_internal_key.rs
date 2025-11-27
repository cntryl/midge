//! Fuzz target for internal key encoding/decoding.
//!
//! Internal keys embed sequence numbers and entry types into user keys.
//! This fuzzer ensures the encoding is robust against malformed inputs.

#![no_main]

use libfuzzer_sys::fuzz_target;

use cntryl_midge::common::internal_key::{
    decode_internal_key, decode_internal_key_typed, encode_internal_key, encode_internal_key_typed,
    EntryType,
};

fuzz_target!(|data: &[u8]| {
    // Fuzz decoding - should handle any input gracefully
    let decoded = decode_internal_key(data);
    let decoded_typed = decode_internal_key_typed(data);

    // If decode succeeds, verify roundtrip doesn't panic
    if let Some((user_key, seq, _is_tombstone)) = decoded {
        let _ = encode_internal_key(&user_key, seq, false);
        let _ = encode_internal_key(&user_key, seq, true);
    }

    if let Some((user_key, seq, entry_type)) = decoded_typed {
        let re_encoded = encode_internal_key_typed(&user_key, seq, entry_type);
        // Verify we can decode what we encoded
        let _ = decode_internal_key_typed(&re_encoded);
    }

    // Test encoding with fuzzer-provided data as user key
    // Use first 8 bytes as sequence number if available
    let seq = if data.len() >= 8 {
        u64::from_le_bytes(data[..8].try_into().unwrap_or([0; 8]))
    } else {
        0
    };
    let user_key = if data.len() > 8 { &data[8..] } else { data };

    let _ = encode_internal_key(user_key, seq, false);
    let _ = encode_internal_key(user_key, seq, true);
    let _ = encode_internal_key_typed(user_key, seq, EntryType::Value);
    let _ = encode_internal_key_typed(user_key, seq, EntryType::Tombstone);
    let _ = encode_internal_key_typed(user_key, seq, EntryType::RangeTombstone);
});
