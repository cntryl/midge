//! Bounded WAL frame reads, including strict truncated-tail classification.

use super::super::{map_fs_error, read_wal_bytes, NextWalFrame, ReplayFailure, ReplayedWalFrame};
use super::StreamingReplayLimits;
use crate::common::{MidgeError, MidgeResult};
use crate::io::{File, FsPath};
use crate::wal::{encoding, frame};

pub(super) fn next_frame(
    file: &dyn File,
    path: &FsPath,
    pos: u64,
    limits: StreamingReplayLimits,
    read_ns: &mut u128,
) -> Result<NextWalFrame, ReplayFailure> {
    let file_len = file.len().map_err(map_fs_error)?;
    if pos == file_len {
        return Ok(NextWalFrame::Eof);
    }
    let header_len = frame::WAL_FRAME_HEADER_LEN as u64;
    if pos > file_len {
        return Err(MidgeError::Corruption(format!("WAL read past EOF in {path}")).into());
    }
    if file_len - pos < header_len {
        return Err(incomplete(format!(
            "Incomplete WAL frame header at {pos} in {path}"
        )));
    }
    let header = read_wal_bytes(file, path, pos, header_len, read_ns)?;
    let payload_start = pos + header_len;
    if header.iter().all(|byte| *byte == 0)
        && zero_tail(
            file,
            path,
            payload_start,
            file_len,
            limits.max_frame_bytes,
            read_ns,
        )?
    {
        return Err(incomplete(format!(
            "Zero-filled WAL tail at {pos} in {path}"
        )));
    }
    let (len, crc) = frame::decode_frame_header(&header)?;
    let end = payload_start
        .checked_add(len as u64)
        .ok_or_else(|| MidgeError::Corruption(format!("WAL frame offset overflow in {path}")))?;
    if end > file_len {
        if verified_suffix(file, path, payload_start, file_len, limits, read_ns)? {
            return Err(MidgeError::Corruption(format!(
                "WAL frame length at {pos} in {path} hides a verified later frame"
            ))
            .into());
        }
        return Err(incomplete(format!(
            "Incomplete WAL record at {pos} in {path}"
        )));
    }
    if len > limits.max_frame_bytes {
        return Err(MidgeError::ResourceLimit(format!(
            "WAL frame needs {len} bytes, exceeding {}-byte replay frame limit",
            limits.max_frame_bytes
        ))
        .into());
    }
    let payload = read_wal_bytes(file, path, payload_start, len as u64, read_ns)?;
    frame::verify_frame_crc(&payload, crc)?;
    preflight_decoded_record(&payload, limits.max_pending_txn_bytes)?;
    Ok(NextWalFrame::Frame(ReplayedWalFrame {
        record: encoding::decode(payload.as_ref())?,
        next_pos: end,
    }))
}

fn incomplete(message: String) -> ReplayFailure {
    ReplayFailure::IncompleteTail(MidgeError::Corruption(message))
}

fn zero_tail(
    file: &dyn File,
    path: &FsPath,
    mut pos: u64,
    end: u64,
    chunk_bytes: usize,
    read_ns: &mut u128,
) -> MidgeResult<bool> {
    while pos < end {
        let len = (end - pos).min(chunk_bytes as u64);
        let bytes = read_wal_bytes(file, path, pos, len, read_ns)?;
        if bytes.iter().any(|byte| *byte != 0) {
            return Ok(false);
        }
        pos += len;
    }
    Ok(true)
}

fn verified_suffix(
    file: &dyn File,
    path: &FsPath,
    mut pos: u64,
    end: u64,
    limits: StreamingReplayLimits,
    read_ns: &mut u128,
) -> MidgeResult<bool> {
    let header_len = frame::WAL_FRAME_HEADER_LEN;
    let overlap = header_len + 2;
    while end.saturating_sub(pos) > overlap as u64 {
        let len = (end - pos).min(limits.max_frame_bytes as u64);
        let bytes = read_wal_bytes(file, path, pos, len, read_ns)?;
        for payload_start in header_len..=bytes.len().saturating_sub(3) {
            if !encoding::has_current_record_prefix(&bytes[payload_start..]) {
                continue;
            }
            let Ok((payload_len, crc)) =
                frame::decode_frame_header(&bytes[payload_start - header_len..payload_start])
            else {
                continue;
            };
            let absolute_start = pos + payload_start as u64;
            if payload_len as u64 > end.saturating_sub(absolute_start) {
                continue;
            }
            if payload_len > limits.max_frame_bytes {
                // A plausible later frame exceeds the configured verification
                // budget. Fail closed instead of calling the prefix a torn tail.
                return Err(MidgeError::ResourceLimit(
                    "WAL suffix candidate exceeds replay frame limit".into(),
                ));
            }
            let payload = read_wal_bytes(file, path, absolute_start, payload_len as u64, read_ns)?;
            if frame::verify_frame_crc(&payload, crc).is_ok()
                && encoding::decode_view(&payload).is_ok()
            {
                return Ok(true);
            }
        }
        pos += len - overlap as u64;
    }
    Ok(false)
}

fn preflight_decoded_record(payload: &[u8], limit: usize) -> MidgeResult<()> {
    let view = encoding::decode_view(payload)?;
    let value_bytes = if let Some(value) = view.value {
        match view.compression {
            None | Some(0) => value.len(),
            Some(1) => {
                let header: [u8; 4] = value
                    .get(..4)
                    .ok_or_else(|| MidgeError::Corruption("LZ4 WAL size prefix missing".into()))?
                    .try_into()
                    .map_err(|_| MidgeError::Corruption("LZ4 WAL size prefix invalid".into()))?;
                u32::from_le_bytes(header) as usize
            }
            Some(2 | 3) => match zstd::zstd_safe::get_frame_content_size(value) {
                Ok(Some(size)) => usize::try_from(size).unwrap_or(usize::MAX),
                Ok(None) => crate::sst::compression::MAX_BLOCK_SIZE,
                Err(error) => {
                    return Err(MidgeError::Corruption(format!(
                        "Zstd WAL size invalid: {error}"
                    )))
                }
            },
            Some(code) => {
                return Err(MidgeError::Corruption(format!(
                    "Unknown WAL compression code {code}"
                )))
            }
        }
    } else {
        0
    };
    let decoded = size_of::<crate::wal::WalRecord>()
        .saturating_add(view.key.len())
        .saturating_add(view.range_end.map_or(0, <[u8]>::len))
        .saturating_add(value_bytes);
    if decoded > limit {
        return Err(MidgeError::ResourceLimit(format!(
            "Decoded WAL record needs {decoded} bytes, exceeding {limit}-byte replay transaction limit"
        )));
    }
    Ok(())
}
