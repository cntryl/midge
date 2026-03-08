use crate::common::{MidgeError, MidgeResult};

pub const WAL_FRAME_HEADER_LEN: usize = 8;
pub const WAL_MAX_RECORD_LEN: usize = 64 * 1024 * 1024;

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

pub fn append_frame(dst: &mut Vec<u8>, payload: &[u8]) -> MidgeResult<()> {
    let frame_len = encoded_frame_len(payload.len())?;
    let crc = crc32c::crc32c(payload);

    dst.reserve(frame_len);
    dst.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    dst.extend_from_slice(&crc.to_le_bytes());
    dst.extend_from_slice(payload);
    Ok(())
}

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

pub fn verify_frame_crc(payload: &[u8], expected_crc: u32) -> MidgeResult<()> {
    let actual_crc = crc32c::crc32c(payload);
    if actual_crc != expected_crc {
        return Err(MidgeError::Corruption(format!(
            "WAL frame CRC mismatch: expected {expected_crc:#010x}, got {actual_crc:#010x}"
        )));
    }
    Ok(())
}
