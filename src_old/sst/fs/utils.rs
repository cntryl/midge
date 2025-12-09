//! FS-backed SST reader/writer at crate root (`crate::fs`)
//!
//! This file is a flattening of `src/internal/storage/fs/sst/*` to provide a
//! simpler import path while maintaining the same implementations.

use crate::error::MidgeResult;

/// Get current time in milliseconds since UNIX epoch for TTL checks
#[allow(dead_code)]
#[inline]
pub(super) fn now_millis() -> u64 {
    crate::common::timestamp::now_millis()
}

/// Calculate the boundary where entries end in a TLV-formatted block.
/// Accounts for restart array and version marker.
#[inline]
pub(super) fn calculate_entries_end(data: &[u8]) -> Option<usize> {
    if data.len() < 8 {
        return None;
    }
    let data_len = data.len();
    let num_restarts = u32::from_le_bytes([
        data[data_len - 4],
        data[data_len - 3],
        data[data_len - 2],
        data[data_len - 1],
    ]) as usize;
    let restarts_start = data_len.checked_sub(4 + num_restarts * 4)?;
    let version_offset = restarts_start.checked_sub(1)?;
    Some(version_offset)
}

/// Decode an internal key with fallback to the raw key if decoding fails.
#[inline]
pub(super) fn decode_internal_key_or_raw(key: &[u8]) -> (Vec<u8>, u64, bool) {
    if let Some((user_key, seq, is_tomb)) = crate::common::internal_key::decode_internal_key(key) {
        (user_key, seq, is_tomb)
    } else {
        (key.to_vec(), 0, false)
    }
}

/// Helper to perform binary search and find the appropriate restart point for a key.
pub(super) fn binary_search_restart_points<F>(
    data: &[u8],
    num_restarts: usize,
    restarts_start: usize,
    entries_end: usize,
    target_key: &[u8],
    parse_key_fn: F,
) -> usize
where
    F: Fn(&[u8], usize, usize) -> MidgeResult<Vec<u8>>,
{
    let mut left = 0;
    let mut right = num_restarts;
    while left < right {
        let mid = (left + right) / 2;
        let restart_offset = u32::from_le_bytes([
            data[restarts_start + mid * 4],
            data[restarts_start + mid * 4 + 1],
            data[restarts_start + mid * 4 + 2],
            data[restarts_start + mid * 4 + 3],
        ]) as usize;
        if let Ok(key) = parse_key_fn(data, restart_offset, entries_end) {
            if key.as_slice() <= target_key {
                left = mid + 1;
            } else {
                right = mid;
            }
        } else {
            break;
        }
    }
    let restart_idx = if left > 0 { left - 1 } else { 0 };
    u32::from_le_bytes([
        data[restarts_start + restart_idx * 4],
        data[restarts_start + restart_idx * 4 + 1],
        data[restarts_start + restart_idx * 4 + 2],
        data[restarts_start + restart_idx * 4 + 3],
    ]) as usize
}

/// Check if a data block payload is valid TLV format
pub(super) fn is_valid_data_block_payload(data: &[u8]) -> bool {
    use crate::sst::format::BlockType;

    let entries_end = match calculate_entries_end(data) {
        Some(e) => e,
        None => return false,
    };
    if entries_end == 0 {
        return true; // empty block is valid
    }
    if data.len() < 8 {
        return false;
    }

    // For old format, check restart offsets
    let data_len = data.len();
    let num_restarts = u32::from_le_bytes([
        data[data_len - 4],
        data[data_len - 3],
        data[data_len - 2],
        data[data_len - 1],
    ]) as usize;
    if num_restarts > 0 {
        let restarts_start = match data_len.checked_sub(4 + num_restarts * 4) {
            Some(s) => s,
            None => return false,
        };
        // Check version marker if present (backwards compatibility - allow blocks without version marker)
        if restarts_start > 0 {
            let version = data[restarts_start - 1];
            // Allow versions 0, 1, 2, or block type marker, but also allow no version marker
            if version > 2 && version != BlockType::Data as u8 {
                // Could be no version marker - this is OK for backwards compatibility
            }
        }
        // Check restart offsets are in range
        // Use version marker position if present, otherwise use restarts_start
        let max_offset = if restarts_start > 0 {
            restarts_start - 1
        } else {
            restarts_start
        };
        let mut prev = 0usize;
        for i in 0..num_restarts {
            let off = u32::from_le_bytes([
                data[restarts_start + i * 4],
                data[restarts_start + i * 4 + 1],
                data[restarts_start + i * 4 + 2],
                data[restarts_start + i * 4 + 3],
            ]) as usize;
            if off > max_offset || off < prev {
                return false;
            }
            prev = off;
        }
    }

    // For TLV format, just check we can read the first tag
    // Don't try to parse varints since TLV format is different
    if entries_end == 0 {
        return false;
    }

    // First byte should be a TLV tag (SHARED_LEN_PACKED = 0x41)
    let first_tag = data[0];
    // Valid tags have wire type in upper 4 bits (0-5) and field tag in lower 4 bits (1-15)
    let wire_type = first_tag >> 4;
    let field_tag = first_tag & 0x0F;
    if wire_type > 5 || field_tag == 0 || field_tag > 15 {
        return false;
    }

    true
}

/// Decode a data block, validating it's a proper data block
pub(super) fn decode_data_block(
    raw: &[u8],
) -> crate::error::MidgeResult<crate::sst::format::Block> {
    decode_data_block_paranoid(raw, false)
}

/// Decode a data block with optional paranoid checksum verification
pub(super) fn decode_data_block_paranoid(
    raw: &[u8],
    paranoid: bool,
) -> crate::error::MidgeResult<crate::sst::format::Block> {
    use crate::error::MidgeError;
    use crate::sst::format::{Block, BlockType};

    match Block::decode_with_options(raw, BlockType::Data, paranoid) {
        Ok(b) if b.block_type == BlockType::Data && is_valid_data_block_payload(&b.data) => Ok(b),
        _ => Err(MidgeError::InvalidData(
            "Unable to decode data block".into(),
        )),
    }
}
