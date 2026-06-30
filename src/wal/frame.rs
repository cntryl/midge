use crate::common::{MidgeError, MidgeResult};

pub const WAL_FRAME_HEADER_LEN: usize = 8;
pub const WAL_MAX_RECORD_LEN: usize = 64 * 1024 * 1024;

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
}
