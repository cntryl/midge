#![no_main]

//! Differential fuzzing of the public engine API (#951): the input decodes
//! into a history of transactions, reads, snapshots, clock advances, flushes,
//! compactions, reopens and power-loss crashes, and every read is checked
//! against the `BTreeMap` model in `tests/support/differential.rs`.

use libfuzzer_sys::fuzz_target;

mod differential;

fuzz_target!(|data: &[u8]| {
    differential::run_history(data);
});
