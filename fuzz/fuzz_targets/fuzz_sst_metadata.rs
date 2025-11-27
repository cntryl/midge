//! Fuzz target for SST metadata parser.
//!
//! SST files are read from disk and potentially from untrusted cloud storage.
//! This fuzzer ensures that corrupted SST data cannot cause panics when
//! parsing the footer, index, and metadata structures.

#![no_main]

use libfuzzer_sys::fuzz_target;

use cntryl_midge::sst::reader_common::{read_data_block_from_bytes, read_data_block_from_bytes_paranoid};
use cntryl_midge::sst::SparseIndex;

fuzz_target!(|data: &[u8]| {
    // Fuzz sparse index decoder
    let _ = SparseIndex::decode(data);

    // Fuzz data block decoder
    let _ = read_data_block_from_bytes(data);

    // Test paranoid mode (with checksum verification)
    let _ = read_data_block_from_bytes_paranoid(data, true);

    // Test at various sizes to catch boundary issues
    for size in [0, 1, 8, 16, 32, 47, 48, 49, 64, 128, 256] {
        if data.len() >= size {
            let _ = SparseIndex::decode(&data[..size]);
            let _ = read_data_block_from_bytes(&data[..size]);
        }
    }
});
