//! Fuzz target for TLV (Type-Length-Value) parser.
//!
//! The TLV format is the foundation of all Midge serialization (WAL records,
//! SST entries, index blocks). This fuzzer ensures the parser handles all
//! malformed inputs gracefully without panicking.

#![no_main]

use libfuzzer_sys::fuzz_target;

use cntryl_midge::common::tlv::{decode_varint32, decode_varint64, TlvReader};

fuzz_target!(|data: &[u8]| {
    // Fuzz the TLV reader iterator - should never panic
    let reader = TlvReader::new(data);
    for (tag, _value) in reader {
        // Just consume the iterator - we're testing that parsing doesn't panic
        let _ = tag;
    }

    // Fuzz try_next explicitly for error handling paths
    let mut reader = TlvReader::new(data);
    while let Ok(Some((_tag, _value))) = reader.try_next() {
        // Continue until exhausted or error
    }

    // Fuzz varint decoders - critical for length prefixes
    let _ = decode_varint32(data);
    let _ = decode_varint64(data);

    // Fuzz at various offsets to test boundary conditions
    for offset in [0, 1, 2, 4, 8, 16].iter().filter(|&&o| o < data.len()) {
        let _ = decode_varint32(&data[*offset..]);
        let _ = decode_varint64(&data[*offset..]);
    }
});
