//! Fuzz target for block decoder.
//!
//! Blocks are the fundamental storage unit in SST files. This fuzzer tests
//! all block types (Data, Index, MetaIndex, Filter) to ensure corrupted
//! blocks don't cause panics.

#![no_main]

use libfuzzer_sys::fuzz_target;

use cntryl_midge::sst::{Block, BlockType};

fuzz_target!(|data: &[u8]| {
    // Test all block types - each has different internal structure
    for block_type in [
        BlockType::Data,
        BlockType::Index,
        BlockType::MetaIndex,
        BlockType::Filter,
    ] {
        // Standard decode
        let _ = Block::decode(data, block_type);

        // Paranoid decode (verifies checksum)
        let _ = Block::decode_with_options(data, block_type, true);
    }

    // Test various slice sizes to catch boundary conditions
    for end in [0, 1, 4, 5, 8, 12, 16, 32, 64].iter().filter(|&&e| e <= data.len()) {
        let _ = Block::decode(&data[..*end], BlockType::Data);
    }

    // Test with offset to catch alignment issues
    if data.len() > 4 {
        let _ = Block::decode(&data[4..], BlockType::Data);
    }
});
