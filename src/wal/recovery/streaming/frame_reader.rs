//! Bounded WAL frame reads, including strict truncated-tail classification.

use super::super::{NextWalFrame, ReplayFailure, ReplayedWalFrame};
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
    let source = frame::FileFrames::new(file, path);
    let step = frame::next_frame(
        &source,
        &path,
        pos,
        frame::FrameLimits {
            max_frame_bytes: Some(limits.max_frame_bytes),
            zero_tail_scan_bytes: Some(limits.max_frame_bytes),
        },
    );
    *read_ns = read_ns.saturating_add(source.read_ns());
    match step? {
        frame::FrameStep::Eof => Ok(NextWalFrame::Eof),
        frame::FrameStep::Frame { payload, next_pos } => {
            // Streaming replay buffers records, so reject one whose decoded
            // size would exceed the pending-transaction budget before
            // decoding it.
            preflight_decoded_record(&payload, limits.max_pending_txn_bytes)?;
            Ok(NextWalFrame::Frame(ReplayedWalFrame {
                record: encoding::decode(payload.as_ref())?,
                next_pos,
            }))
        }
    }
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
