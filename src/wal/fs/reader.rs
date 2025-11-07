use std::fs::OpenOptions;
use std::io::Read;

use crate::common::tlv::{parse_u32, parse_u64, parse_u8, tags, TlvReader};
use crate::error::MidgeResult;
use crate::wal::encoding::decompress_value;
use crate::wal::WalRecord;

use super::writer::WAL_MAGIC_V1;

/// Replay an entire WAL file at a given path. Supports v1 TLV format with
/// standard header (magic + start_sequence) and verifies per-record CRCs.
pub fn replay_wal_file(path: &std::path::Path) -> MidgeResult<Vec<WalRecord>> {
    let mut file = OpenOptions::new().read(true).open(path)?;

    // Read and validate v1 header
    let mut magic = [0u8; 8];
    let mut seqbuf = [0u8; 8];
    file.read_exact(&mut magic)?;
    file.read_exact(&mut seqbuf)?;

    if &magic != WAL_MAGIC_V1 {
        return Err(crate::error::MidgeError::Corruption {
            message: format!("Invalid WAL magic: expected v1, got {:?}", magic),
        });
    }

    let mut records = Vec::new();
    loop {
        // Read CRC32
        let mut crc_buf = [0u8; 4];
        match file.read_exact(&mut crc_buf) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let crc_stored = u32::from_le_bytes(crc_buf);

        // Read length (v1 TLV format includes length prefix)
        let mut len_buf = [0u8; 4];
        file.read_exact(&mut len_buf)?;
        let body_len = u32::from_le_bytes(len_buf) as usize;

        // Read TLV body
        let mut body = vec![0u8; body_len];
        file.read_exact(&mut body)?;

        // Verify CRC32-C
        let crc_calc = crc32c::crc32c(&body);
        if crc_calc != crc_stored {
            return Err(crate::error::MidgeError::Corruption {
                message: "WAL v1 CRC mismatch".to_string(),
            });
        }

        // Parse TLV fields
        let reader = TlvReader::new(&body);
        let mut op = None;
        let mut cf_id = None;
        let mut seq = None;
        let mut key = None;
        let mut value = None;
        let mut expiration = None;
        let mut range_end = None;
        let mut txn_id = None;
        let mut compression = None;

        for (tag, field_value) in reader {
            match tag {
                tags::OPERATION => {
                    op = Some(parse_u8(field_value)?);
                }
                tags::CF_ID => {
                    cf_id = Some(parse_u32(field_value)?);
                }
                tags::SEQUENCE => {
                    seq = Some(parse_u64(field_value)?);
                }
                tags::KEY => {
                    key = Some(bytes::Bytes::copy_from_slice(field_value));
                }
                tags::VALUE => {
                    value = Some(bytes::Bytes::copy_from_slice(field_value));
                }
                tags::VALUE_COMPRESSED => {
                    // Compressed value - will decompress after reading compression type
                    value = Some(bytes::Bytes::copy_from_slice(field_value));
                }
                tags::COMPRESSION => {
                    compression = Some(parse_u8(field_value)?);
                }
                tags::EXPIRATION => {
                    expiration = Some(parse_u64(field_value)?);
                }
                tags::RANGE_END => {
                    range_end = Some(bytes::Bytes::copy_from_slice(field_value));
                }
                tags::TRANSACTION_ID => {
                    txn_id = Some(parse_u64(field_value)?);
                }
                _ => {
                    // Unknown tag - skip (forward compatibility)
                }
            }
        }

        // Decompress value if needed
        if let (Some(comp_type), Some(ref compressed_value)) = (compression, &value) {
            let decompressed = decompress_value(comp_type, compressed_value)?;
            value = Some(bytes::Bytes::from(decompressed));
        }

        // Validate required fields
        let op = op.ok_or_else(|| crate::error::MidgeError::Corruption {
            message: "WAL v1 record missing op field".to_string(),
        })?;
        let cf_id = cf_id.unwrap_or(0);
        let seq = seq.ok_or_else(|| crate::error::MidgeError::Corruption {
            message: "WAL v1 record missing sequence field".to_string(),
        })?;
        // Key is optional for TxnBegin/TxnCommit markers
        let key = key.unwrap_or_else(bytes::Bytes::new);

        let op_kind = crate::wal::WalOpKind::from_wire_format(op)?;

        let mut rec = WalRecord::new_cf(
            crate::column_family::ColumnFamilyId::new(cf_id),
            op_kind,
            key,
            value,
            seq,
        );
        rec.expiration = expiration;
        rec.range_end = range_end;
        rec.txn_id = txn_id;

        records.push(rec);
    }

    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn should_handle_empty_wal_file() {
        // Arrange
        let dir = TempDir::new().expect("temp dir");
        let wal_path = dir.path().join("wal.log");

        // Create empty file
        std::fs::write(&wal_path, b"").expect("create empty file");

        // Act: try to replay empty file
        let result = replay_wal_file(&wal_path);

        // Assert: should handle gracefully (empty WAL returns error or empty)
        // Empty file will fail on reading magic, which is expected
        assert!(result.is_err() || result.unwrap().is_empty());
    }

    #[test]
    fn should_detect_invalid_magic_in_replay_wal_file() {
        // Arrange
        let dir = TempDir::new().expect("temp dir");
        let wal_path = dir.path().join("bad_wal.log");

        // Write invalid magic
        std::fs::write(&wal_path, b"BADMAGIC12345678").expect("write bad file");

        // Act: try to replay
        let result = replay_wal_file(&wal_path);

        // Assert: should detect corruption
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(matches!(e, crate::error::MidgeError::Corruption { .. }));
        }
    }
}
