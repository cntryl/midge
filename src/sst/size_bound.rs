//! Conservative encoded bounds shared by memtable admission and SST writers.

pub(crate) const FIXED_SST_BYTES: usize = 16 * 1024;

pub(crate) fn point_bytes(key_len: usize, value_len: usize) -> usize {
    256usize
        .saturating_add(key_len.saturating_mul(8))
        .saturating_add(value_len.saturating_mul(2))
}

pub(crate) fn range_bytes(start_len: usize, end_len: usize) -> usize {
    128usize.saturating_add(start_len.saturating_add(end_len).saturating_mul(6))
}

/// Streaming scratch can coexist with a completed SST; legacy publishers
/// may instead retain a verification copy alongside that output.
pub(crate) fn flush_staging_bytes(encoded_bytes: usize) -> u64 {
    u64::try_from(encoded_bytes)
        .unwrap_or(u64::MAX)
        .saturating_mul(2)
}
