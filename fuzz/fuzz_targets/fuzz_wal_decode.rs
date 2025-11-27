//! Fuzz target for WAL record decoder.
//!
//! WAL records are decoded during recovery after crashes. This fuzzer ensures
//! that corrupted or malicious WAL data cannot cause panics during recovery.

#![no_main]

use libfuzzer_sys::fuzz_target;

// decode is re-exported, decode_borrowed needs full path
use cntryl_midge::wal::{decode, encoding::decode_borrowed};

fuzz_target!(|data: &[u8]| {
    // Fuzz the primary decode function
    // This should return Err for invalid data, never panic
    let _ = decode(data);

    // Fuzz zero-copy decode variant
    let _ = decode_borrowed(data);

    // Test with various slice windows to catch off-by-one errors
    if data.len() > 1 {
        let _ = decode(&data[1..]);
        let _ = decode_borrowed(&data[1..]);
    }
    if data.len() > 2 {
        let _ = decode(&data[..data.len() - 1]);
        let _ = decode_borrowed(&data[..data.len() - 1]);
    }
});
