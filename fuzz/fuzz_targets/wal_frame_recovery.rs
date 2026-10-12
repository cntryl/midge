#![no_main]

use libfuzzer_sys::fuzz_target;

mod common;

fuzz_target!(|data: &[u8]| {
    let dir = tempfile::tempdir().expect("tempdir");
    common::seed_db(dir.path());
    // A freshly seeded database appends to WAL segment 1, the open segment
    // its segment ledger starts at; its header stays.
    common::write_open_segment_body(dir.path(), data);
    common::exercise_open_and_verify(dir.path());
});
