//! Fuzz target for bloom filter decoder.
//!
//! Bloom filters are decoded from SST files and used for fast negative lookups.
//! This fuzzer ensures corrupted bloom filter data doesn't cause panics.

#![no_main]

use libfuzzer_sys::fuzz_target;

use cntryl_midge::sst::BloomFilter;

fuzz_target!(|data: &[u8]| {
    // Fuzz bloom filter decoding
    if let Ok(filter) = BloomFilter::decode_block(data) {
        // If decoding succeeds, test the query path too
        // Use parts of the input as test keys
        for chunk in data.chunks(8) {
            let _ = filter.may_contain(chunk);
        }

        // Test with empty key
        let _ = filter.may_contain(&[]);

        // Test with large key
        let _ = filter.may_contain(&[0u8; 1024]);
    }

    // Test at various sizes
    for size in [0, 1, 2, 4, 8, 16, 32, 64, 128] {
        if data.len() >= size {
            let _ = BloomFilter::decode_block(&data[..size]);
        }
    }
});
