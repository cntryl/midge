//! Small, dependency-light helpers for generating deterministic keys/values
//! used across benches, stress workloads, and tests.

use bytes::Bytes;

/// Default key size for bench/test helpers: 14 bytes.
///
/// Format: `key_` + 10 ASCII digits.
pub const KEY_SIZE: usize = 14;

/// Generate a fixed-size key without `format!` allocations.
///
/// Format: `key_` + 10-digit zero-padded decimal number.
#[inline]
#[must_use]
pub fn make_key(i: usize) -> Bytes {
    let mut key = vec![0u8; KEY_SIZE];
    key[..4].copy_from_slice(b"key_");

    let mut n = i;
    for j in (4..KEY_SIZE).rev() {
        key[j] = b'0' + (n % 10) as u8;
        n /= 10;
    }

    Bytes::from(key)
}

/// Generate a fixed-size value filled with `b'x'`.
#[inline]
#[must_use]
pub fn make_value_fixed(size: usize) -> Bytes {
    Bytes::from(vec![b'x'; size])
}

/// Precompute keys and values outside hot loops.
#[must_use]
pub fn precompute_kv(n: usize, value_size: usize) -> (Vec<Bytes>, Vec<Bytes>) {
    let mut keys = Vec::with_capacity(n);
    let mut vals = Vec::with_capacity(n);

    for i in 0..n {
        keys.push(make_key(i));
        vals.push(make_value_fixed(value_size));
    }

    (keys, vals)
}

/// Precompute deterministic pseudo-random indices.
///
/// Uses a simple LCG for reproducible patterns.
#[must_use]
pub fn precompute_read_indices(n: usize, count: usize, seed: u64) -> Vec<usize> {
    let mut indices = Vec::with_capacity(count);
    let mut state = seed;

    for _ in 0..count {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        indices.push((state as usize) % n);
    }

    indices
}
