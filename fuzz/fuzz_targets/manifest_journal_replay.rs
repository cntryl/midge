#![no_main]

use libfuzzer_sys::fuzz_target;

mod common;

fuzz_target!(|data: &[u8]| {
    let dir = tempfile::tempdir().expect("tempdir");
    common::seed_db(dir.path());
    let (selector, body) = data
        .split_first()
        .map_or((0, data), |(&first, rest)| (first, rest));
    let path = if selector & 1 == 0 {
        "manifest.journal"
    } else {
        "manifest.snapshot.json"
    };
    common::write_relative(dir.path(), path, body);
    common::exercise_open_and_verify(dir.path());
});
