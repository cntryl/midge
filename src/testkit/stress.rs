#![allow(rustdoc::broken_intra_doc_links)]

use crate::testkit::config::MidgeOptions;
use crate::Engine;

pub const KEY_SIZE: usize = 16;

pub fn open_engine_no_compaction(mut opts: MidgeOptions) -> Engine {
    opts.enable_compaction = false;
    Engine::open_with_options(opts).expect("open_engine_no_compaction: open engine")
}

pub fn key16_u64_be(i: u64) -> [u8; KEY_SIZE] {
    let mut k = [0u8; KEY_SIZE];
    k[..8].copy_from_slice(&i.to_be_bytes());
    k
}

pub fn key16_prefix_u64_be(prefix: u8, i: u64) -> [u8; KEY_SIZE] {
    let mut k = [0u8; KEY_SIZE];
    k[0] = prefix;
    k[1..9].copy_from_slice(&i.to_be_bytes());
    k
}

pub fn precompute_keys16_u64_be(num: usize) -> Vec<[u8; KEY_SIZE]> {
    (0..num).map(|i| key16_u64_be(i as u64)).collect()
}

pub fn precompute_kv16_u64_be(
    num_keys: usize,
    value_size: usize,
    value_mod: u8,
) -> (Vec<[u8; KEY_SIZE]>, Vec<Vec<u8>>) {
    let mut keys = Vec::with_capacity(num_keys);
    let mut values = Vec::with_capacity(num_keys);

    for i in 0..num_keys {
        keys.push(key16_u64_be(i as u64));
        values.push(vec![(i as u8) % value_mod; value_size]);
    }

    (keys, values)
}
