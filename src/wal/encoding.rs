//! WAL record encoding/decoding
//!
//! v2 WAL payload format: magic + version + TLVs.
//!
//! Framing:
//! - WAL file stores records as: `<u32 len><payload bytes>` (handled by the WAL IO layer)
//! - This module encodes/decodes the *payload bytes*.
//!
//! Payload wire format:
//! - `MAGIC: [u8; 2]` (currently `b"MW"`)
//! - `VERSION: u8` (currently `1`)
//! - repeated TLV fields:
//!   - `tag: u8`
//!   - `len: u32` (little-endian)
//!   - `val: [u8; len]`
//!
//! Required fields:
//! - OP, CF_ID, SEQ, KEY
//!
//! Optional fields:
//! - VALUE, EXPIRATION, RANGE_END, TXN_ID, COMPRESSION
//!
//! Unknown tags are skipped (forward-compatible). Duplicate tags are accepted; the
//! *last* occurrence wins.

use crate::common::{MidgeError, MidgeResult};
use crate::wal::types::{WalOpKind, WalRecord};
use bytes::{Buf, BufMut, Bytes, BytesMut};

const MAGIC: [u8; 2] = *b"MW";
const VERSION: u8 = 1;
const PREFIX_LEN: usize = 3; // MAGIC (2) + VERSION (1)
const TLV_HEADER_LEN: usize = 1 + 4; // tag (1) + len (4)

/// WAL TLV tags.
pub mod tags {
    pub const OP: u8 = 1;
    pub const CF_ID: u8 = 2;
    pub const SEQ: u8 = 3;
    pub const KEY: u8 = 4;

    pub const VALUE: u8 = 5;
    pub const EXPIRATION: u8 = 6;
    pub const RANGE_END: u8 = 7;
    pub const TXN_ID: u8 = 8;
    pub const COMPRESSION: u8 = 9;
    pub const WRITER_EPOCH: u8 = 10;
}

/// Borrowed zero-copy WAL record view.
///
/// This is the preferred decode representation for hot paths.
#[derive(Debug, Clone, Copy)]
pub struct WalRecordView<'a> {
    pub cf_id: u32,
    pub op: WalOpKind,
    pub key: &'a [u8],
    pub value: Option<&'a [u8]>,
    pub seq: u64,
    pub expiration: Option<u64>,
    pub range_end: Option<&'a [u8]>,
    pub txn_id: Option<u64>,
    pub writer_epoch: u64,
    pub compression: Option<u8>,
}

fn corruption(msg: impl Into<String>) -> MidgeError {
    MidgeError::Corruption(msg.into())
}

#[inline]
fn put_tlv(buf: &mut BytesMut, tag: u8, val: &[u8]) {
    buf.put_u8(tag);
    buf.put_u32_le(val.len() as u32);
    buf.extend_from_slice(val);
}

#[inline]
fn put_u8(buf: &mut BytesMut, tag: u8, v: u8) {
    put_tlv(buf, tag, &[v]);
}

#[inline]
fn put_u32(buf: &mut BytesMut, tag: u8, v: u32) {
    put_tlv(buf, tag, &v.to_le_bytes());
}

#[inline]
fn put_u64(buf: &mut BytesMut, tag: u8, v: u64) {
    put_tlv(buf, tag, &v.to_le_bytes());
}

#[inline]
fn scan_tlvs<'a>(
    mut data: &'a [u8],
    mut f: impl FnMut(u8, &'a [u8]) -> MidgeResult<()>,
) -> MidgeResult<()> {
    while !data.is_empty() {
        if data.len() < TLV_HEADER_LEN {
            return Err(corruption(format!(
                "truncated TLV header: need {}, have {}",
                TLV_HEADER_LEN,
                data.len()
            )));
        }

        let tag = data[0];
        let len = u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize;
        data = &data[TLV_HEADER_LEN..];

        if data.len() < len {
            return Err(corruption(format!(
                "truncated TLV value for tag {}: need {}, have {}",
                tag,
                len,
                data.len()
            )));
        }

        let (val, rest) = data.split_at(len);
        data = rest;

        f(tag, val)?;
    }
    Ok(())
}

/// Helper: add to capacity or return error on overflow (avoids silent cap and potential OOM).
fn add_capacity(capacity: usize, delta: usize) -> MidgeResult<usize> {
    capacity
        .checked_add(delta)
        .ok_or_else(|| MidgeError::InvalidArgument("record size overflow".into()))
}

/// Encode a WAL record to bytes (v2 payload).
pub fn encode(record: &WalRecord) -> MidgeResult<Bytes> {
    // Use checked adds to avoid overflow on 32-bit systems or huge values.
    let mut capacity = PREFIX_LEN;
    capacity = add_capacity(capacity, TLV_HEADER_LEN + 1)?; // OP
    capacity = add_capacity(capacity, TLV_HEADER_LEN + 4)?; // CF_ID
    capacity = add_capacity(capacity, TLV_HEADER_LEN + 8)?; // SEQ
    capacity = add_capacity(capacity, TLV_HEADER_LEN + record.key.len())?;

    if let Some(v) = &record.value {
        capacity = add_capacity(capacity, TLV_HEADER_LEN + v.len())?;
    }

    if record.expiration.is_some() {
        capacity = add_capacity(capacity, TLV_HEADER_LEN + 8)?;
    }
    if let Some(r) = &record.range_end {
        capacity = add_capacity(capacity, TLV_HEADER_LEN + r.len())?;
    }
    if record.txn_id.is_some() {
        capacity = add_capacity(capacity, TLV_HEADER_LEN + 8)?;
    }
    // writer_epoch is always present
    capacity = add_capacity(capacity, TLV_HEADER_LEN + 8)?;
    if record.compression.is_some() {
        capacity = add_capacity(capacity, TLV_HEADER_LEN + 1)?;
    }

    let mut buf = BytesMut::with_capacity(capacity);

    buf.extend_from_slice(&MAGIC);
    buf.put_u8(VERSION);

    put_u8(&mut buf, tags::OP, record.op.to_wire_format());
    put_u32(&mut buf, tags::CF_ID, record.cf_id);
    put_u64(&mut buf, tags::SEQ, record.seq);
    put_tlv(&mut buf, tags::KEY, &record.key);

    put_u64(&mut buf, tags::WRITER_EPOCH, record.writer_epoch);

    if let Some(v) = &record.value {
        // Preserve Some(empty) distinctly from None by always emitting VALUE when present.
        let (write_val, comp_byte) = crate::sst::compression::compress_wal_value(v);
        put_tlv(&mut buf, tags::VALUE, &write_val);
        if let Some(cb) = comp_byte {
            put_u8(&mut buf, tags::COMPRESSION, cb);
        } else if let Some(c) = record.compression {
            put_u8(&mut buf, tags::COMPRESSION, c);
        }
    } else if let Some(c) = record.compression {
        put_u8(&mut buf, tags::COMPRESSION, c);
    }

    if let Some(exp) = record.expiration {
        put_u64(&mut buf, tags::EXPIRATION, exp);
    }
    if let Some(r) = &record.range_end {
        put_tlv(&mut buf, tags::RANGE_END, r);
    }
    if let Some(txn_id) = record.txn_id {
        put_u64(&mut buf, tags::TXN_ID, txn_id);
    }

    Ok(buf.freeze())
}

/// Zero-copy decode into a borrowed view.
pub fn decode_view<'a>(data: &'a [u8]) -> MidgeResult<WalRecordView<'a>> {
    if data.len() < PREFIX_LEN {
        return Err(corruption("truncated WAL payload prefix"));
    }
    if data[0..2] != MAGIC {
        return Err(corruption("invalid WAL payload magic"));
    }
    if data[2] != VERSION {
        return Err(corruption("unsupported WAL payload version"));
    }

    let mut op = None;
    let mut cf_id = None;
    let mut seq = None;
    let mut key = None;

    let mut value = None;
    let mut expiration = None;
    let mut range_end = None;
    let mut txn_id = None;
    let mut writer_epoch = None;
    let mut compression = None;

    scan_tlvs(&data[PREFIX_LEN..], |tag, val| {
        match tag {
            tags::OP => {
                if val.len() != 1 {
                    return Err(corruption("bad OP length"));
                }
                op = Some(WalOpKind::from_wire_format(val[0])?);
            }
            tags::CF_ID => {
                if val.len() != 4 {
                    return Err(corruption(format!("bad CF_ID length: {}", val.len())));
                }
                cf_id = Some(u32::from_le_bytes([val[0], val[1], val[2], val[3]]));
            }
            tags::SEQ => {
                if val.len() != 8 {
                    return Err(corruption(format!("bad SEQ length: {}", val.len())));
                }
                seq = Some(u64::from_le_bytes([
                    val[0], val[1], val[2], val[3], val[4], val[5], val[6], val[7],
                ]));
            }
            tags::KEY => {
                key = Some(val);
            }
            tags::VALUE => {
                value = Some(val);
            }
            tags::EXPIRATION => {
                if val.len() != 8 {
                    return Err(corruption("bad EXPIRATION length"));
                }
                expiration = Some(u64::from_le_bytes([
                    val[0], val[1], val[2], val[3], val[4], val[5], val[6], val[7],
                ]));
            }
            tags::RANGE_END => {
                range_end = Some(val);
            }
            tags::TXN_ID => {
                if val.len() != 8 {
                    return Err(corruption("bad TXN_ID length"));
                }
                txn_id = Some(u64::from_le_bytes([
                    val[0], val[1], val[2], val[3], val[4], val[5], val[6], val[7],
                ]));
            }
            tags::WRITER_EPOCH => {
                if val.len() != 8 {
                    return Err(corruption("bad WRITER_EPOCH length"));
                }
                writer_epoch = Some(u64::from_le_bytes([
                    val[0], val[1], val[2], val[3], val[4], val[5], val[6], val[7],
                ]));
            }
            tags::COMPRESSION => {
                if val.len() != 1 {
                    return Err(corruption("bad COMPRESSION length"));
                }
                compression = Some(val[0]);
            }
            _ => {
                // forward compatible: skip unknown tags
            }
        }
        Ok(())
    })?;

    Ok(WalRecordView {
        op: op.ok_or_else(|| corruption("missing OP"))?,
        cf_id: cf_id.ok_or_else(|| corruption("missing CF_ID"))?,
        seq: seq.ok_or_else(|| corruption("missing SEQ"))?,
        key: key.ok_or_else(|| corruption("missing KEY"))?,
        value,
        expiration,
        range_end,
        txn_id,
        writer_epoch: writer_epoch.ok_or_else(|| corruption("missing WRITER_EPOCH"))?,
        compression,
    })
}

/// Compatibility adapter: decode into owned `WalRecord`.
///
/// This allocates for key/value/range_end. Prefer [`decode_view`] in hot paths.
///
/// If the record carries a `COMPRESSION` tag, the value is transparently
/// decompressed before being returned.
pub fn decode(bytes: impl Buf) -> MidgeResult<WalRecord> {
    // WAL frames must be contiguous; enforce this.
    let data = bytes.chunk();
    if data.len() != bytes.remaining() {
        return Err(corruption("non-contiguous WAL buffer"));
    }

    let view = decode_view(data)?;

    // Decompress value if a compression tag is present
    let value = match view.value {
        Some(raw_val) => {
            let decompressed =
                crate::sst::compression::decompress_wal_value(raw_val, view.compression)?;
            Some(decompressed)
        }
        None => None,
    };

    Ok(WalRecord {
        cf_id: view.cf_id,
        op: view.op,
        key: Bytes::copy_from_slice(view.key),
        value,
        seq: view.seq,
        expiration: view.expiration,
        range_end: view.range_end.map(Bytes::copy_from_slice),
        txn_id: view.txn_id,
        writer_epoch: view.writer_epoch,
        compression: None, // Decompressed — no longer carries a compression tag
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn should_roundtrip_put_when_value_present() {
        // Arrange
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key"),
            Some(Bytes::from_static(b"value")),
            42,
            1,
        );

        // Act
        let encoded = encode(&record).unwrap();
        let decoded = decode(&encoded[..]).unwrap();

        // Assert
        assert_eq!(decoded.op, record.op);
        assert_eq!(decoded.cf_id, record.cf_id);
        assert_eq!(decoded.seq, record.seq);
        assert_eq!(decoded.key, record.key);
        assert_eq!(decoded.value, record.value);
    }

    #[test]
    fn should_roundtrip_delete_when_value_absent() {
        // Arrange
        let record = WalRecord::new(WalOpKind::Delete, Bytes::from_static(b"k"), None, 7, 1);

        // Act
        let encoded = encode(&record).unwrap();
        let decoded = decode(&encoded[..]).unwrap();

        // Assert
        assert_eq!(decoded.op, WalOpKind::Delete);
        assert_eq!(decoded.value, None);
        assert_eq!(decoded.key, record.key);
    }

    #[test]
    fn should_roundtrip_empty_value_distinct_from_delete() {
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"k"),
            Some(Bytes::from_static(b"")),
            8,
            1,
        );

        let encoded = encode(&record).unwrap();
        let decoded = decode(&encoded[..]).unwrap();

        assert_eq!(decoded.op, WalOpKind::Put);
        assert_eq!(decoded.value, Some(Bytes::from_static(b"")));
    }

    #[test]
    fn should_roundtrip_writer_epoch() {
        // Arrange
        let mut record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"k"),
            Some(Bytes::from_static(b"v")),
            99,
            1,
        );
        record.writer_epoch = 0x1234_5678_9abc_def0;

        // Act
        let encoded = encode(&record).unwrap();
        let decoded = decode(&encoded[..]).unwrap();

        // Assert
        assert_eq!(decoded.seq, 99);
        assert_eq!(decoded.writer_epoch, record.writer_epoch);
    }

    #[test]
    fn should_skip_unknown_tags_when_decoding() {
        // Arrange
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key"),
            Some(Bytes::from_static(b"value")),
            1,
            1,
        );
        let mut encoded = encode(&record).unwrap().to_vec();

        // Inject an unknown tag (250) with 3 bytes of data.
        encoded.push(250);
        encoded.extend_from_slice(&3u32.to_le_bytes());
        encoded.extend_from_slice(b"xyz");

        // Act
        let decoded = decode(&encoded[..]).unwrap();

        // Assert
        assert_eq!(decoded.op, record.op);
        assert_eq!(decoded.key, record.key);
        assert_eq!(decoded.value, record.value);
    }

    #[test]
    fn should_error_when_magic_invalid() {
        // Arrange
        let bad = Bytes::from_static(b"ZZ\x01");

        // Act
        let err = decode(&bad[..]).unwrap_err();

        // Assert
        match err {
            MidgeError::Corruption(_) => {}
            other => panic!("expected corruption error, got: {:?}", other),
        }
    }

    #[test]
    fn should_error_when_required_fields_missing() {
        // Arrange
        let mut payload = Vec::new();
        payload.extend_from_slice(&MAGIC);
        payload.push(VERSION);

        // Act
        let err = decode(&payload[..]).unwrap_err();

        // Assert
        match err {
            MidgeError::Corruption(_) => {}
            other => panic!("expected corruption error, got: {:?}", other),
        }
    }

    #[test]
    fn should_error_when_tlv_header_truncated() {
        // Arrange
        let mut payload = Vec::new();
        payload.extend_from_slice(&MAGIC);
        payload.push(VERSION);
        payload.push(tags::OP); // tag only; missing 4-byte length

        // Act
        let err = decode(&payload[..]).unwrap_err();

        // Assert
        match err {
            MidgeError::Corruption(_) => {}
            other => panic!("expected corruption error, got: {:?}", other),
        }
    }

    proptest! {
        #[test]
        fn should_roundtrip_arbitrary_put_records(
            cf_id in any::<u32>(),
            seq in any::<u64>(),
            writer_epoch in any::<u64>(),
            key in proptest::collection::vec(any::<u8>(), 1..33),
            value in proptest::collection::vec(any::<u8>(), 0..65),
            expiration in proptest::option::of(any::<u64>()),
            txn_id in proptest::option::of(any::<u64>()),
        ) {
            let mut record = WalRecord::new_cf(
                cf_id,
                WalOpKind::Put,
                Bytes::from(key.clone()),
                Some(Bytes::from(value.clone())),
                seq,
                writer_epoch,
            );
            record.expiration = expiration;
            record.txn_id = txn_id;

            let encoded = encode(&record).unwrap();
            let decoded = decode(&encoded[..]).unwrap();

            prop_assert_eq!(decoded.cf_id, cf_id);
            prop_assert_eq!(decoded.op, WalOpKind::Put);
            prop_assert_eq!(decoded.seq, seq);
            prop_assert_eq!(decoded.writer_epoch, writer_epoch);
            prop_assert_eq!(decoded.key, Bytes::from(key));
            prop_assert_eq!(decoded.value, Some(Bytes::from(value)));
            prop_assert_eq!(decoded.expiration, expiration);
            prop_assert_eq!(decoded.txn_id, txn_id);
        }
    }
}
