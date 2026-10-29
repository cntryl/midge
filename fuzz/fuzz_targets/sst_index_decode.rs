#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = cntryl_midge::__internal::sst::decoder::validate_index_block(data);
});
