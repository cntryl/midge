#![allow(rustdoc::broken_intra_doc_links)]
#![allow(dead_code)]

use super::config::MidgeOptions;
use cntryl_midge::Engine;

pub const KEY_SIZE: usize = 16;

#[must_use]
/// # Panics
///
/// Panics if the engine cannot be opened with compaction disabled.
pub fn open_engine_no_compaction(mut opts: MidgeOptions) -> Engine {
    opts.enable_compaction = false;
    Engine::open(opts.to_open_options()).expect("open_engine_no_compaction: open engine")
}

#[must_use]
pub fn key16_u64_be(i: u64) -> [u8; KEY_SIZE] {
    let mut k = [0u8; KEY_SIZE];
    k[..8].copy_from_slice(&i.to_be_bytes());
    k
}

#[must_use]
pub fn key16_prefix_u64_be(prefix: u8, i: u64) -> [u8; KEY_SIZE] {
    let mut k = [0u8; KEY_SIZE];
    k[0] = prefix;
    k[1..9].copy_from_slice(&i.to_be_bytes());
    k
}

#[must_use]
pub fn precompute_keys16_u64_be(num: usize) -> Vec<[u8; KEY_SIZE]> {
    (0..num).map(|i| key16_u64_be(i as u64)).collect()
}

#[must_use]
pub fn precompute_kv16_u64_be(
    num_keys: usize,
    value_size: usize,
    value_mod: u8,
) -> (Vec<[u8; KEY_SIZE]>, Vec<Vec<u8>>) {
    let mut keys = Vec::with_capacity(num_keys);
    let mut values = Vec::with_capacity(num_keys);

    for i in 0..num_keys {
        keys.push(key16_u64_be(i as u64));
        values.push(vec![
            u8::try_from(i).unwrap_or(u8::MAX) % value_mod;
            value_size
        ]);
    }

    (keys, values)
}
