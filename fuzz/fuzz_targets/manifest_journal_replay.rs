#![no_main]

use libfuzzer_sys::fuzz_target;

mod common;

fuzz_target!(|data: &[u8]| {
    let dir = tempfile::tempdir().expect("tempdir");
    common::seed_db(dir.path());
    common::write_relative(dir.path(), "manifest.journal", data);
    common::exercise_open_and_verify(dir.path());
});
