//! Allocation-free extent validation before the borrowed `MessagePack` decoder.

use crate::common::{MidgeError, MidgeResult};

pub(super) fn validate(payload: &[u8]) -> MidgeResult<()> {
    let mut offset = 0;
    value(payload, &mut offset, 0)?;
    if offset != payload.len() {
        return Err(corruption("metadata payload has trailing bytes"));
    }
    Ok(())
}

fn corruption(message: &str) -> MidgeError {
    MidgeError::Corruption(message.into())
}

fn take<'a>(bytes: &'a [u8], offset: &mut usize, length: usize) -> MidgeResult<&'a [u8]> {
    let end = offset
        .checked_add(length)
        .ok_or_else(|| corruption("metadata extent overflow"))?;
    let extent = bytes
        .get(*offset..end)
        .ok_or_else(|| corruption("truncated metadata value"))?;
    *offset = end;
    Ok(extent)
}

fn length(bytes: &[u8], offset: &mut usize, width: usize) -> MidgeResult<usize> {
    let mut value = 0_u32;
    for &byte in take(bytes, offset, width)? {
        value = (value << 8) | u32::from(byte);
    }
    usize::try_from(value).map_err(|_| corruption("metadata length overflow"))
}

fn children(bytes: &[u8], offset: &mut usize, count: usize, depth: usize) -> MidgeResult<()> {
    // Every child consumes at least one marker. Reject announced huge arrays
    // before a deserializer can use their size hint to reserve storage.
    if count > bytes.len().saturating_sub(*offset) {
        return Err(corruption("metadata child count exceeds remaining frame"));
    }
    for _ in 0..count {
        value(bytes, offset, depth + 1)?;
    }
    Ok(())
}

fn value(bytes: &[u8], offset: &mut usize, depth: usize) -> MidgeResult<()> {
    if depth >= 64 {
        return Err(corruption("metadata nesting exceeds decoder limit"));
    }
    let marker = take(bytes, offset, 1)?[0];
    let fixed = match marker {
        0x00..=0x7f | 0xc0 | 0xc2 | 0xc3 | 0xe0..=0xff => Some(0),
        0xa0..=0xbf => Some(usize::from(marker & 0x1f)),
        0xcc | 0xd0 => Some(1),
        0xcd | 0xd1 | 0xd4 => Some(2),
        0xd5 => Some(3),
        0xca | 0xce | 0xd2 => Some(4),
        0xd6 => Some(5),
        0xcb | 0xcf | 0xd3 => Some(8),
        0xd7 => Some(9),
        0xd8 => Some(17),
        _ => None,
    };
    if let Some(length) = fixed {
        take(bytes, offset, length)?;
        return Ok(());
    }
    match marker {
        0x80..=0x8f => children(bytes, offset, usize::from(marker & 0x0f) * 2, depth),
        0x90..=0x9f => children(bytes, offset, usize::from(marker & 0x0f), depth),
        0xc4..=0xc9 | 0xd9..=0xdb => {
            let width = match marker {
                0xc4 | 0xc7 | 0xd9 => 1,
                0xc5 | 0xc8 | 0xda => 2,
                _ => 4,
            };
            let size = length(bytes, offset, width)?;
            let size = size
                .checked_add(usize::from(matches!(marker, 0xc7..=0xc9)))
                .ok_or_else(|| corruption("metadata extension length overflow"))?;
            take(bytes, offset, size)?;
            Ok(())
        }
        0xdc..=0xdf => {
            let width = if matches!(marker, 0xdc | 0xde) { 2 } else { 4 };
            let count = length(bytes, offset, width)?;
            let count = count
                .checked_mul(if marker >= 0xde { 2 } else { 1 })
                .ok_or_else(|| corruption("metadata container count overflow"))?;
            children(bytes, offset, count, depth)
        }
        _ => Err(corruption("reserved metadata marker")),
    }
}
