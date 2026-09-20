use crate::common::{MidgeError, MidgeResult};

pub const WAL_FRAME_HEADER_LEN: usize = 8;
pub const WAL_MAX_RECORD_LEN: usize = 64 * 1024 * 1024;
/// Largest uncompressed VALUE a WAL record may carry. Replay decompresses
/// VALUE with this ceiling, so the writer must never accept more even when
/// compression shrinks the frame below [`WAL_MAX_RECORD_LEN`].
pub const WAL_MAX_VALUE_LEN: usize = crate::sst::compression::MAX_DECOMPRESSED_BLOCK_SIZE;

/// Compute the encoded WAL frame length for a payload.
///
/// # Errors
///
/// Returns `MidgeError::InvalidArgument` when `payload_len` exceeds the WAL limits.
pub fn encoded_frame_len(payload_len: usize) -> MidgeResult<usize> {
    if payload_len > WAL_MAX_RECORD_LEN {
        return Err(MidgeError::InvalidArgument(format!(
            "WAL record length exceeds max frame size ({WAL_MAX_RECORD_LEN} bytes)"
        )));
    }
    if payload_len > u32::MAX as usize {
        return Err(MidgeError::InvalidArgument(
            "WAL record length exceeds u32::MAX".into(),
        ));
    }
    Ok(WAL_FRAME_HEADER_LEN + payload_len)
}

/// Append a length-prefixed WAL frame to `dst`.
///
/// # Errors
///
/// Returns `MidgeError::InvalidArgument` when the payload exceeds the WAL limits.
pub fn append_frame(dst: &mut Vec<u8>, payload: &[u8]) -> MidgeResult<()> {
    let frame_len = encoded_frame_len(payload.len())?;
    let crc = crc32c::crc32c(payload);

    dst.reserve(frame_len);
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| MidgeError::InvalidArgument("WAL record length exceeds u32::MAX".into()))?;
    dst.extend_from_slice(&payload_len.to_le_bytes());
    dst.extend_from_slice(&crc.to_le_bytes());
    dst.extend_from_slice(payload);
    Ok(())
}

/// Append a WAL frame whose payload is produced directly into `dst`.
///
/// This preserves the same frame layout as [`append_frame`] while avoiding an
/// intermediate payload allocation on write hot paths.
///
/// # Errors
///
/// Returns `MidgeError::InvalidArgument` when the encoded payload exceeds WAL
/// limits, or returns any error produced by `encode_payload`.
pub fn append_frame_encoded(
    dst: &mut Vec<u8>,
    encode_payload: impl FnOnce(&mut Vec<u8>) -> MidgeResult<()>,
) -> MidgeResult<()> {
    let frame_start = dst.len();
    dst.extend_from_slice(&[0; WAL_FRAME_HEADER_LEN]);
    let payload_start = dst.len();

    if let Err(error) = encode_payload(dst) {
        dst.truncate(frame_start);
        return Err(error);
    }

    let payload_len = dst.len().saturating_sub(payload_start);
    if let Err(error) = encoded_frame_len(payload_len) {
        dst.truncate(frame_start);
        return Err(error);
    }

    let Ok(payload_len_u32) = u32::try_from(payload_len) else {
        dst.truncate(frame_start);
        return Err(MidgeError::InvalidArgument(
            "WAL record length exceeds u32::MAX".into(),
        ));
    };
    let crc = crc32c::crc32c(&dst[payload_start..]);

    dst[frame_start..frame_start + 4].copy_from_slice(&payload_len_u32.to_le_bytes());
    dst[frame_start + 4..frame_start + WAL_FRAME_HEADER_LEN].copy_from_slice(&crc.to_le_bytes());
    Ok(())
}

/// Decode the fixed-size WAL frame header.
///
/// # Errors
///
/// Returns `MidgeError::Corruption` when the header is malformed or declares
/// an oversized payload.
pub fn decode_frame_header(header: &[u8]) -> MidgeResult<(usize, u32)> {
    if header.len() != WAL_FRAME_HEADER_LEN {
        return Err(MidgeError::Corruption(format!(
            "bad WAL frame header length: expected {}, got {}",
            WAL_FRAME_HEADER_LEN,
            header.len()
        )));
    }

    let mut len_buf = [0u8; 4];
    len_buf.copy_from_slice(&header[..4]);
    let payload_len = u32::from_le_bytes(len_buf) as usize;
    if payload_len > WAL_MAX_RECORD_LEN {
        return Err(MidgeError::Corruption(format!(
            "WAL record too large (len={payload_len}, max={WAL_MAX_RECORD_LEN})"
        )));
    }

    let mut crc_buf = [0u8; 4];
    crc_buf.copy_from_slice(&header[4..8]);
    let expected_crc = u32::from_le_bytes(crc_buf);

    Ok((payload_len, expected_crc))
}

/// Verify the CRC32C of a WAL frame payload.
///
/// # Errors
///
/// Returns `MidgeError::Corruption` when the CRC does not match.
pub fn verify_frame_crc(payload: &[u8], expected_crc: u32) -> MidgeResult<()> {
    let actual_crc = crc32c::crc32c(payload);
    if actual_crc != expected_crc {
        return Err(MidgeError::Corruption(format!(
            "WAL frame CRC mismatch: expected {expected_crc:#010x}, got {actual_crc:#010x}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn should_reject_frame_header_given_declared_length_one_byte_over_wal_max_record_len() {
        // Arrange
        let declared_len = u32::try_from(WAL_MAX_RECORD_LEN + 1)
            .expect("WAL maximum plus one fits the encoded length field");
        let mut header = [0_u8; WAL_FRAME_HEADER_LEN];
        header[..4].copy_from_slice(&declared_len.to_le_bytes());

        // Act
        let result = decode_frame_header(&header);

        // Assert
        assert!(matches!(result, Err(MidgeError::Corruption(_))));
    }

    proptest! {
        #[test]
        fn should_roundtrip_arbitrary_payloads(payload in proptest::collection::vec(any::<u8>(), 0..8192)) {
            // Arrange
            let mut buf = Vec::new();

            // Act
            append_frame(&mut buf, &payload)?;

            // Assert
            prop_assert_eq!(buf.len(), WAL_FRAME_HEADER_LEN + payload.len());

            let (payload_len, expected_crc) = decode_frame_header(&buf[..WAL_FRAME_HEADER_LEN])?;
            prop_assert_eq!(payload_len, payload.len());
            prop_assert_eq!(&buf[WAL_FRAME_HEADER_LEN..], payload.as_slice());
            verify_frame_crc(&buf[WAL_FRAME_HEADER_LEN..], expected_crc)?;
        }

        #[test]
        fn should_match_append_frame_when_payload_is_encoded_in_place(
            prefix in proptest::collection::vec(any::<u8>(), 0..64),
            payload in proptest::collection::vec(any::<u8>(), 0..8192)
        ) {
            // Arrange
            let mut expected = prefix.clone();
            append_frame(&mut expected, &payload)?;
            let mut actual = prefix;

            // Act
            append_frame_encoded(&mut actual, |dst| {
                dst.extend_from_slice(&payload);
                Ok(())
            })?;

            // Assert
            prop_assert_eq!(actual, expected);
        }

        #[test]
        fn should_detect_crc_mismatch_after_payload_corruption(
            payload in proptest::collection::vec(any::<u8>(), 1..4096),
            flip_index in 0usize..4096
        ) {
            // Arrange
            let mut buf = Vec::new();
            append_frame(&mut buf, &payload)?;
            let (payload_len, expected_crc) = decode_frame_header(&buf[..WAL_FRAME_HEADER_LEN])?;
            prop_assert_eq!(payload_len, payload.len());

            let payload_start = WAL_FRAME_HEADER_LEN;
            let idx = payload_start + (flip_index % payload.len());
            buf[idx] ^= 0x5a;

            // Act
            let err = verify_frame_crc(&buf[payload_start..], expected_crc).unwrap_err();

            // Assert
            prop_assert!(matches!(err, MidgeError::Corruption(_)));
        }
    }

    fn framed_put(key: &[u8], sequence: u64) -> Vec<u8> {
        let record = crate::wal::WalRecord::new(
            crate::wal::WalOpKind::Put,
            bytes::Bytes::copy_from_slice(key),
            Some(bytes::Bytes::from_static(b"value")),
            sequence,
            1,
        );
        let payload = crate::wal::encoding::encode(&record).expect("encode record");
        let mut framed = Vec::new();
        append_frame(&mut framed, &payload).expect("frame record");
        framed
    }

    fn step_at(data: &[u8], pos: u64, limits: FrameLimits) -> Result<FrameStep, FrameError> {
        next_frame(&data, &"test", pos, limits)
    }

    #[test]
    fn should_classify_short_final_frame_as_incomplete_tail() {
        // Arrange: a crash truncated the last append mid-frame.
        let mut data = framed_put(b"a", 1);
        let torn_start = data.len() as u64;
        data.extend_from_slice(&framed_put(b"b", 2)[..5]);

        // Act
        let step = step_at(&data, torn_start, FrameLimits::default());

        // Assert
        assert!(
            step.err().is_some_and(|error| error.is_incomplete_tail()),
            "a torn final append is a tolerable tail"
        );
    }

    #[test]
    fn should_classify_zero_filled_tail_as_incomplete_tail() {
        // Arrange: preallocated space follows the last record.
        let mut data = framed_put(b"a", 1);
        let zero_start = data.len() as u64;
        data.extend_from_slice(&[0u8; 64]);

        // Act
        let step = step_at(&data, zero_start, FrameLimits::default());

        // Assert
        assert!(step.err().is_some_and(|error| error.is_incomplete_tail()));
    }

    #[test]
    fn should_classify_overrun_that_hides_a_later_frame_as_corrupt() {
        // Arrange: a length that runs past EOF while a verified frame still
        // follows it would discard durable data if treated as a torn tail.
        let mut data = framed_put(b"a", 1);
        let overrun_start = data.len();
        data.extend_from_slice(&framed_put(b"b", 2));
        data.extend_from_slice(&framed_put(b"c", 3));
        let hidden_len = u32::try_from(data.len().saturating_mul(4)).expect("test length fits u32");
        data[overrun_start..overrun_start + 4].copy_from_slice(&hidden_len.to_le_bytes());

        // Act
        let step = step_at(&data, overrun_start as u64, FrameLimits::default());

        // Assert
        let error = step.err().expect("overrun must fail");
        assert!(
            !error.is_incomplete_tail(),
            "an overrun hiding a verified frame is corruption, not a torn tail"
        );
    }

    #[test]
    fn should_reject_frame_larger_than_the_configured_bound() {
        // Arrange
        let data = framed_put(b"a", 1);

        // Act
        let step = step_at(
            &data,
            0,
            FrameLimits {
                max_frame_bytes: Some(1),
                zero_tail_scan_bytes: Some(1),
            },
        );

        // Assert
        assert!(matches!(step, Err(FrameError::ResourceLimit(_))));
    }

    #[test]
    fn should_walk_every_frame_when_file_ends_on_a_boundary() {
        // Arrange
        let mut data = framed_put(b"a", 1);
        data.extend_from_slice(&framed_put(b"b", 2));

        // Act
        let mut pos = 0;
        let mut frames = 0;
        while let Ok(FrameStep::Frame { next_pos, .. }) =
            step_at(&data, pos, FrameLimits::default())
        {
            frames += 1;
            pos = next_pos;
        }

        // Assert
        assert_eq!(frames, 2);
        assert!(matches!(
            step_at(&data, pos, FrameLimits::default()),
            Ok(FrameStep::Eof)
        ));
    }
}

/// Where a frame walk stopped.
pub(crate) enum FrameStep {
    /// A complete, CRC-verified frame payload and the offset after it.
    Frame {
        payload: bytes::Bytes,
        next_pos: u64,
    },
    /// The walk reached the end of the file exactly on a frame boundary.
    Eof,
}

/// Why a frame walk stopped short.
///
/// The distinction drives durability policy: an incomplete tail is a torn
/// final append that recovery may drop, while corruption inside the verified
/// prefix must not be silently discarded.
pub(crate) enum FrameError {
    /// A torn final append: recovery may keep the verified prefix.
    IncompleteTail(MidgeError),
    /// Damage that a caller must not treat as a torn tail.
    Corrupt(MidgeError),
    /// The frame exceeds a configured bound.
    ResourceLimit(MidgeError),
}

impl FrameError {
    pub(crate) fn into_error(self) -> MidgeError {
        match self {
            Self::IncompleteTail(error) | Self::Corrupt(error) | Self::ResourceLimit(error) => {
                error
            }
        }
    }

    pub(crate) fn is_incomplete_tail(&self) -> bool {
        matches!(self, Self::IncompleteTail(_))
    }
}

/// Bounds applied while walking frames.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct FrameLimits {
    /// Largest payload a caller will accept, when it reads into memory it
    /// has budgeted.
    pub(crate) max_frame_bytes: Option<usize>,
    /// Bytes to scan past a zero header before concluding a zero-filled tail.
    /// `None` scans to end of file.
    pub(crate) zero_tail_scan_bytes: Option<usize>,
}

/// Byte source for a frame walk: an in-memory snapshot or an open file.
pub(crate) trait FrameBytes {
    fn len(&self) -> Result<u64, MidgeError>;
    fn read(&self, pos: u64, len: u64) -> Result<bytes::Bytes, MidgeError>;
}

impl FrameBytes for &[u8] {
    fn len(&self) -> Result<u64, MidgeError> {
        u64::try_from(<[u8]>::len(self))
            .map_err(|_| MidgeError::Corruption("WAL snapshot length does not fit u64".into()))
    }

    fn read(&self, pos: u64, len: u64) -> Result<bytes::Bytes, MidgeError> {
        let start = usize::try_from(pos)
            .map_err(|_| MidgeError::Corruption("WAL frame offset does not fit memory".into()))?;
        let read_len = usize::try_from(len)
            .map_err(|_| MidgeError::Corruption("WAL frame length does not fit memory".into()))?;
        let end = start.checked_add(read_len).ok_or_else(|| {
            MidgeError::Corruption("WAL frame end does not fit memory".to_string())
        })?;
        if end > <[u8]>::len(self) {
            return Err(MidgeError::Corruption(
                "WAL frame read past end of snapshot".to_string(),
            ));
        }
        Ok(bytes::Bytes::copy_from_slice(&self[start..end]))
    }
}

/// Frame source over an open WAL file, accumulating read time.
pub(crate) struct FileFrames<'a> {
    file: &'a dyn crate::io::File,
    path: &'a crate::io::FsPath,
    read_ns: std::cell::Cell<u128>,
}

impl<'a> FileFrames<'a> {
    pub(crate) fn new(file: &'a dyn crate::io::File, path: &'a crate::io::FsPath) -> Self {
        Self {
            file,
            path,
            read_ns: std::cell::Cell::new(0),
        }
    }

    /// Nanoseconds spent reading through this source.
    pub(crate) fn read_ns(&self) -> u128 {
        self.read_ns.get()
    }
}

impl FrameBytes for FileFrames<'_> {
    fn len(&self) -> Result<u64, MidgeError> {
        self.file.len().map_err(Into::into)
    }

    fn read(&self, pos: u64, len: u64) -> Result<bytes::Bytes, MidgeError> {
        let started = std::time::Instant::now();
        let bytes = self.file.read_at(pos, len).map_err(MidgeError::from)?;
        self.read_ns.set(
            self.read_ns
                .get()
                .saturating_add(started.elapsed().as_nanos()),
        );
        let expected = usize::try_from(len).map_err(|_| {
            MidgeError::Corruption(format!(
                "WAL read length does not fit memory at offset {pos} in {}: {len}",
                self.path
            ))
        })?;
        if bytes.len() != expected {
            return Err(MidgeError::Corruption(format!(
                "WAL read at offset {pos} in {} returned {} bytes, expected {expected}",
                self.path,
                bytes.len()
            )));
        }
        Ok(bytes)
    }
}

/// Read the next frame at `pos`, classifying every way the walk can stop.
///
/// This is the single owner of frame parsing: end-of-file, torn tails,
/// zero-filled tails, a length that hides a verified later frame, CRC, and
/// the configured bounds. Callers decode the payload themselves, because
/// what a valid record means differs between replay, prune and validation.
pub(crate) fn next_frame<S: FrameBytes>(
    source: &S,
    context: &dyn std::fmt::Display,
    pos: u64,
    limits: FrameLimits,
) -> Result<FrameStep, FrameError> {
    let file_len = source.len().map_err(FrameError::Corrupt)?;
    if pos == file_len {
        return Ok(FrameStep::Eof);
    }
    if pos > file_len {
        return Err(FrameError::Corrupt(MidgeError::Corruption(format!(
            "WAL replay read past EOF at pos {pos} in {context} (file_len={file_len})"
        ))));
    }
    let header_len = WAL_FRAME_HEADER_LEN as u64;
    let available = file_len.saturating_sub(pos);
    if available < header_len {
        return Err(FrameError::IncompleteTail(MidgeError::Corruption(format!(
            "Incomplete WAL frame header at pos {pos} in {context} (need {WAL_FRAME_HEADER_LEN} bytes, have {available})"
        ))));
    }

    let header = source.read(pos, header_len).map_err(FrameError::Corrupt)?;
    let payload_start = pos + header_len;
    if header.iter().all(|byte| *byte == 0)
        && zero_filled_to_end(source, payload_start, file_len, limits)?
    {
        return Err(FrameError::IncompleteTail(MidgeError::Corruption(format!(
            "Zero-filled WAL tail at pos {pos} in {context}"
        ))));
    }

    let (payload_len, expected_crc) = decode_frame_header(&header).map_err(FrameError::Corrupt)?;
    let payload_end = payload_start
        .checked_add(payload_len as u64)
        .ok_or_else(|| {
            FrameError::Corrupt(MidgeError::Corruption(format!(
                "WAL frame length overflow at pos {pos} in {context} (len={payload_len})"
            )))
        })?;
    if payload_end > file_len {
        // A length that overruns EOF is a torn tail unless a verified frame
        // still follows it, which would mean the overrun hides durable data.
        let hides_verified_suffix = hides_verified_frame(source, payload_start, file_len, limits)?;
        let error = MidgeError::Corruption(if hides_verified_suffix {
            format!(
                "WAL frame length at pos {pos} in {context} overruns EOF and hides a verified later frame (len={payload_len}, file_len={file_len})"
            )
        } else {
            format!(
                "Incomplete WAL record at pos {pos} in {context} (len={payload_len}, file_len={file_len})"
            )
        });
        return Err(if hides_verified_suffix {
            FrameError::Corrupt(error)
        } else {
            FrameError::IncompleteTail(error)
        });
    }
    if limits
        .max_frame_bytes
        .is_some_and(|max_frame_bytes| payload_len > max_frame_bytes)
    {
        return Err(FrameError::ResourceLimit(MidgeError::ResourceLimit(
            format!(
                "WAL frame needs {payload_len} bytes, exceeding {}-byte replay frame limit",
                limits.max_frame_bytes.unwrap_or_default()
            ),
        )));
    }

    let payload = source
        .read(payload_start, payload_len as u64)
        .map_err(FrameError::Corrupt)?;
    verify_frame_crc(&payload, expected_crc).map_err(FrameError::Corrupt)?;
    Ok(FrameStep::Frame {
        payload,
        next_pos: payload_end,
    })
}

/// Whether every byte from `pos` to the end (or the configured scan bound)
/// is zero, which marks preallocated space rather than a torn record.
fn zero_filled_to_end<S: FrameBytes>(
    source: &S,
    pos: u64,
    file_len: u64,
    limits: FrameLimits,
) -> Result<bool, FrameError> {
    let mut offset = pos;
    let chunk_len = limits
        .zero_tail_scan_bytes
        .map_or(1024 * 1024, |bytes| bytes.max(1));
    while offset < file_len {
        let len = chunk_len.min(usize_saturating(file_len - offset));
        let chunk = source
            .read(offset, len as u64)
            .map_err(FrameError::Corrupt)?;
        if chunk.iter().any(|byte| *byte != 0) {
            return Ok(false);
        }
        offset = offset.saturating_add(len as u64);
    }
    Ok(true)
}

/// Whether a CRC-verified, decodable frame starts after a truncated length,
/// which proves the overrun hides durable data instead of a torn tail.
///
/// With a frame bound configured the suffix is scanned in bounded chunks, so
/// a streaming reader never materializes an unbounded tail; a candidate that
/// exceeds the bound fails closed rather than being called a torn tail.
fn hides_verified_frame<S: FrameBytes>(
    source: &S,
    payload_start: u64,
    file_len: u64,
    limits: FrameLimits,
) -> Result<bool, FrameError> {
    let header_len = WAL_FRAME_HEADER_LEN;
    let Some(max_frame_bytes) = limits.max_frame_bytes else {
        let suffix_start = payload_start.saturating_sub(header_len as u64);
        let suffix = source
            .read(suffix_start, file_len.saturating_sub(suffix_start))
            .map_err(FrameError::Corrupt)?;
        return Ok(contains_verified_frame(&suffix));
    };

    let overlap = header_len + 2;
    let mut pos = payload_start;
    while file_len.saturating_sub(pos) > overlap as u64 {
        let len = usize_saturating((file_len - pos).min(max_frame_bytes as u64));
        let bytes = source.read(pos, len as u64).map_err(FrameError::Corrupt)?;
        for candidate_start in header_len..=bytes.len().saturating_sub(3) {
            if !crate::wal::encoding::has_current_record_prefix(&bytes[candidate_start..]) {
                continue;
            }
            let Ok((payload_len, crc)) =
                decode_frame_header(&bytes[candidate_start - header_len..candidate_start])
            else {
                continue;
            };
            let absolute_start = pos + candidate_start as u64;
            if payload_len as u64 > file_len.saturating_sub(absolute_start) {
                continue;
            }
            if payload_len > max_frame_bytes {
                // A plausible later frame exceeds the configured verification
                // budget. Fail closed instead of calling the prefix a torn tail.
                return Err(FrameError::ResourceLimit(MidgeError::ResourceLimit(
                    "WAL suffix candidate exceeds replay frame limit".into(),
                )));
            }
            let payload = source
                .read(absolute_start, payload_len as u64)
                .map_err(FrameError::Corrupt)?;
            if verify_frame_crc(&payload, crc).is_ok()
                && crate::wal::encoding::decode_view(&payload).is_ok()
            {
                return Ok(true);
            }
        }
        pos += len as u64 - overlap as u64;
    }
    Ok(false)
}

fn usize_saturating(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

/// Whether `bytes` contains a complete frame whose payload verifies and
/// decodes, scanning for a current record prefix.
pub(crate) fn contains_verified_frame(bytes: &[u8]) -> bool {
    if bytes.len() < WAL_FRAME_HEADER_LEN.saturating_add(3) {
        return false;
    }
    for payload_start in WAL_FRAME_HEADER_LEN..=bytes.len().saturating_sub(3) {
        if !crate::wal::encoding::has_current_record_prefix(&bytes[payload_start..]) {
            continue;
        }
        let header_start = payload_start - WAL_FRAME_HEADER_LEN;
        let Ok((payload_len, expected_crc)) =
            decode_frame_header(&bytes[header_start..payload_start])
        else {
            continue;
        };
        let Some(payload_end) = payload_start.checked_add(payload_len) else {
            continue;
        };
        if payload_end > bytes.len() {
            continue;
        }
        let payload = &bytes[payload_start..payload_end];
        if verify_frame_crc(payload, expected_crc).is_ok()
            && crate::wal::encoding::decode_view(payload).is_ok()
        {
            return true;
        }
    }
    false
}
