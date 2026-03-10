#![no_main]

use cntryl_midge::sst::types::{decode_range_tombstones, Footer};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = Footer::decode(data);
    let _ = decode_range_tombstones(data);
});
