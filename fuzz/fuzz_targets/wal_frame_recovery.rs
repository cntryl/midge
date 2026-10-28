#![no_main]

use libfuzzer_sys::fuzz_target;

mod common;

fuzz_target!(|data: &[u8]| {
    let dir = tempfile::tempdir().expect("tempdir");
    common::seed_db(dir.path());
    // A freshly seeded database appends to WAL segment 1, the open segment
    // its segment ledger starts at; its header stays.
    write_open_segment_body(dir.path(), data);
    common::exercise_open_and_verify(dir.path());
});

/// Fuzz frames in segment 1 while retaining its seeded header (#843).
fn write_open_segment_body(path: &std::path::Path, data: &[u8]) {
    let relative = "wal/00000000000000000001.wal";
    let mut bytes = std::fs::read(path.join(relative)).expect("read seeded open WAL segment");
    assert!(bytes.len() >= cntryl_midge::__internal::wal::segment::HEADER_LEN);
    bytes.truncate(cntryl_midge::__internal::wal::segment::HEADER_LEN);
    bytes.extend_from_slice(data);
    common::write_relative(path, relative, &bytes);
}
