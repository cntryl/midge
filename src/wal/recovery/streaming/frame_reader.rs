//! Bounded WAL frame reads, including strict truncated-tail classification.

use super::super::{NextWalFrame, ReplayFailure, ReplayedWalFrame};
use super::StreamingReplayLimits;
use crate::codec::{decompressed_len_hint, CompressionAlgo};
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
            // Scan a zero-filled tail in bounded chunks; a whole-frame chunk
            // would allocate up to the WAL record limit per scan.
            zero_tail_scan_bytes: Some(limits.max_frame_bytes.min(1024 * 1024)),
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
    let value_bytes = view.value.map_or(Ok(0), |value| {
        decoded_value_len_for_preflight(value, view.compression)
    })?;
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

fn decoded_value_len_for_preflight(value: &[u8], compression: Option<u8>) -> MidgeResult<usize> {
    match compression {
        None => Ok(value.len()),
        Some(code) => {
            let algo = CompressionAlgo::from_u8(code).ok_or_else(|| {
                MidgeError::Corruption(format!("Unknown WAL compression code {code}"))
            })?;
            Ok(decompressed_len_hint(algo, value)?.decode_limit)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::decoded_value_len_for_preflight;
    use crate::codec::{
        compress_block, compress_wal_value, decompress_wal_value, CompressionAlgo,
        CompressionPolicy,
    };

    #[test]
    fn should_agree_with_shared_wal_codec_on_known_compressed_value_lengths() {
        // Arrange
        let values = [vec![b'r'; 512], b"structured wal value ".repeat(256)];

        for value in values {
            let (lz4, lz4_tag) = compress_wal_value(&value);
            let (zstd, zstd_algo) =
                compress_block(&value, &CompressionPolicy::Fixed(CompressionAlgo::Zstd3))
                    .expect("compress zstd test value");

            // Act
            let lz4_preflight =
                decoded_value_len_for_preflight(&lz4, lz4_tag).expect("preflight lz4 test value");
            let zstd_preflight = decoded_value_len_for_preflight(&zstd, Some(zstd_algo.to_u8()))
                .expect("preflight zstd test value");
            let lz4_decoded = decompress_wal_value(&lz4, lz4_tag).expect("decode lz4 test value");
            let zstd_decoded = decompress_wal_value(&zstd, Some(zstd_algo.to_u8()))
                .expect("decode zstd test value");

            // Assert
            assert_eq!(lz4_preflight, lz4_decoded.len());
            assert_eq!(zstd_preflight, zstd_decoded.len());
            assert_eq!(lz4_decoded.as_ref(), value.as_slice());
            assert_eq!(zstd_decoded.as_ref(), value.as_slice());
        }
    }
}
