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
//! - OP, `CF_ID`, SEQ, KEY
//!
//! Optional fields:
//! - VALUE, EXPIRATION, `RANGE_END`, `TXN_ID`, COMPRESSION
//!
//! Unknown tags are skipped (forward-compatible). Duplicate tags are accepted; the
//! *last* occurrence wins.

use crate::common::{MidgeError, MidgeResult};
use crate::wal::types::{WalOpKind, WalRecord};
use bytes::{Buf, Bytes};

const MAGIC: [u8; 2] = *b"MW";
const VERSION: u8 = 1;
const TXN_BATCH_MAGIC: [u8; 2] = *b"TB";
const TXN_BATCH_VERSION: u8 = 1;
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnBatchRecord {
    pub cf_id: u32,
    pub op: WalOpKind,
    pub key: Bytes,
    pub value: Option<Bytes>,
    pub seq: u64,
    pub expiration: Option<u64>,
    pub range_end: Option<Bytes>,
}

#[derive(Debug, Clone, Copy)]
pub struct TxnBatchEncodeRecord<'a> {
    pub cf_id: u32,
    pub op: WalOpKind,
    pub key: &'a [u8],
    pub value: Option<&'a [u8]>,
    pub seq: u64,
    pub expiration: Option<u64>,
    pub range_end: Option<&'a [u8]>,
    pub txn_id: Option<u64>,
    pub writer_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedTxnBatch {
    pub txn_id: u64,
    pub begin_seq: u64,
    pub commit_seq: u64,
    pub writer_epoch: u64,
    pub records: Vec<TxnBatchRecord>,
}

struct TxnBatchHeader {
    txn_id: u64,
    begin_seq: u64,
    commit_seq: u64,
    op_count: usize,
}

const TXN_BATCH_MIN_PREFIX_LEN: usize = 3 + (8 * 3) + 4;
const TXN_BATCH_RECORD_FIXED_LEN: usize = 1 + 4 + 8 + 1 + 1 + 1;

fn corruption(msg: impl Into<String>) -> MidgeError {
    MidgeError::Corruption(msg.into())
}

fn payload_capacity(record: &WalRecord) -> MidgeResult<usize> {
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

    Ok(capacity)
}

#[inline]
fn push_tlv(buf: &mut Vec<u8>, tag: u8, val: &[u8]) -> MidgeResult<()> {
    let len = u32::try_from(val.len())
        .map_err(|_| MidgeError::InvalidArgument("TLV value exceeds u32::MAX".into()))?;
    buf.push(tag);
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(val);
    Ok(())
}

#[inline]
fn push_u8(buf: &mut Vec<u8>, tag: u8, v: u8) -> MidgeResult<()> {
    push_tlv(buf, tag, &[v])
}

#[inline]
fn push_u32(buf: &mut Vec<u8>, tag: u8, v: u32) -> MidgeResult<()> {
    push_tlv(buf, tag, &v.to_le_bytes())
}

#[inline]
fn push_u64(buf: &mut Vec<u8>, tag: u8, v: u64) -> MidgeResult<()> {
    push_tlv(buf, tag, &v.to_le_bytes())
}

#[inline]
fn put_tlv(buf: &mut Vec<u8>, tag: u8, val: &[u8]) -> MidgeResult<()> {
    push_tlv(buf, tag, val)
}

#[inline]
fn put_u8(buf: &mut Vec<u8>, tag: u8, v: u8) -> MidgeResult<()> {
    push_u8(buf, tag, v)
}

#[inline]
fn put_u32(buf: &mut Vec<u8>, tag: u8, v: u32) -> MidgeResult<()> {
    push_u32(buf, tag, v)
}

#[inline]
fn put_u64(buf: &mut Vec<u8>, tag: u8, v: u64) -> MidgeResult<()> {
    push_u64(buf, tag, v)
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

enum EncodedValue<'a> {
    Borrowed(&'a [u8]),
    Owned(Bytes),
}

impl EncodedValue<'_> {
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Borrowed(bytes) => bytes,
            Self::Owned(bytes) => bytes.as_ref(),
        }
    }
}

/// Encode a WAL record to bytes (v2 payload).
///
/// # Errors
///
/// Returns an error when record sizing overflows or value compression fails.
pub fn encode(record: &WalRecord) -> MidgeResult<Bytes> {
    let mut buf = Vec::with_capacity(payload_capacity(record)?);
    encode_into(record, &mut buf)?;
    Ok(Bytes::from(buf))
}

/// Encode a WAL record payload directly into an existing buffer.
///
/// # Errors
///
/// Returns an error when record sizing overflows or a TLV field exceeds `u32::MAX`.
pub fn encode_into(record: &WalRecord, buf: &mut Vec<u8>) -> MidgeResult<()> {
    let start_len = buf.len();
    if let Err(error) = encode_into_inner(record, buf) {
        buf.truncate(start_len);
        return Err(error);
    }
    Ok(())
}

fn encode_into_inner(record: &WalRecord, buf: &mut Vec<u8>) -> MidgeResult<()> {
    buf.reserve(payload_capacity(record)?);
    buf.extend_from_slice(&MAGIC);
    buf.push(VERSION);

    put_u8(buf, tags::OP, record.op.to_wire_format())?;
    put_u32(buf, tags::CF_ID, record.cf_id)?;
    put_u64(buf, tags::SEQ, record.seq)?;
    put_tlv(buf, tags::KEY, &record.key)?;

    put_u64(buf, tags::WRITER_EPOCH, record.writer_epoch)?;

    if let Some(v) = &record.value {
        // Preserve Some(empty) distinctly from None by always emitting VALUE when present.
        let (write_val, comp_byte) = if v.len() < crate::sst::compression::MIN_COMPRESS_SIZE {
            (EncodedValue::Borrowed(v.as_ref()), None)
        } else {
            let (value, comp_byte) = crate::sst::compression::compress_wal_value(v);
            (EncodedValue::Owned(value), comp_byte)
        };
        put_tlv(buf, tags::VALUE, write_val.as_slice())?;
        if let Some(cb) = comp_byte {
            put_u8(buf, tags::COMPRESSION, cb)?;
        } else if let Some(c) = record.compression {
            put_u8(buf, tags::COMPRESSION, c)?;
        }
    } else if let Some(c) = record.compression {
        put_u8(buf, tags::COMPRESSION, c)?;
    }

    if let Some(exp) = record.expiration {
        put_u64(buf, tags::EXPIRATION, exp)?;
    }
    if let Some(r) = &record.range_end {
        put_tlv(buf, tags::RANGE_END, r)?;
    }
    if let Some(txn_id) = record.txn_id {
        put_u64(buf, tags::TXN_ID, txn_id)?;
    }

    Ok(())
}

/// Zero-copy decode into a borrowed view.
///
/// # Errors
///
/// Returns `MidgeError::Corruption` when the payload prefix or any TLV field is malformed.
pub fn decode_view(data: &[u8]) -> MidgeResult<WalRecordView<'_>> {
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
/// This allocates for `key/value/range_end`. Prefer [`decode_view`] in hot paths.
///
/// If the record carries a `COMPRESSION` tag, the value is transparently
/// decompressed before being returned.
/// Decode an owned WAL record from a payload buffer.
///
/// # Errors
///
/// Returns an error when the payload is malformed or value decompression fails.
pub fn decode(mut bytes: impl Buf) -> MidgeResult<WalRecord> {
    // WAL frames must be contiguous; enforce this.
    let data = bytes.chunk();
    if data.len() != bytes.remaining() {
        return Err(corruption("non-contiguous WAL buffer"));
    }
    let owned = bytes.copy_to_bytes(bytes.remaining());
    let view = decode_view(owned.as_ref())?;

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

fn push_len_prefixed_bytes(buf: &mut Vec<u8>, value: &[u8]) -> MidgeResult<()> {
    let len = u32::try_from(value.len()).map_err(|_| {
        MidgeError::InvalidArgument("transaction batch field exceeds u32::MAX".into())
    })?;
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(value);
    Ok(())
}

fn txn_batch_len_prefixed_field_len(value: &[u8]) -> MidgeResult<usize> {
    let _ = u32::try_from(value.len()).map_err(|_| {
        MidgeError::InvalidArgument("transaction batch field exceeds u32::MAX".into())
    })?;
    add_capacity(4, value.len())
}

fn txn_batch_payload_records_encoded_len(
    records: &[TxnBatchEncodeRecord<'_>],
) -> MidgeResult<usize> {
    let _ = u32::try_from(records.len()).map_err(|_| {
        MidgeError::InvalidArgument("transaction batch op_count exceeds u32::MAX".into())
    })?;

    let mut len = TXN_BATCH_MIN_PREFIX_LEN;
    for record in records {
        len = add_capacity(len, TXN_BATCH_RECORD_FIXED_LEN)?;
        if record.expiration.is_some() {
            len = add_capacity(len, 8)?;
        }
        len = add_capacity(len, txn_batch_len_prefixed_field_len(record.key)?)?;
        if let Some(value) = record.value {
            len = add_capacity(len, txn_batch_len_prefixed_field_len(value)?)?;
        }
        if let Some(range_end) = record.range_end {
            len = add_capacity(len, txn_batch_len_prefixed_field_len(range_end)?)?;
        }
    }
    Ok(len)
}

fn read_exact_slice<'a>(input: &mut &'a [u8], len: usize, field: &str) -> MidgeResult<&'a [u8]> {
    if input.len() < len {
        return Err(corruption(format!(
            "truncated transaction batch {field}: need {len}, have {}",
            input.len()
        )));
    }
    let (head, tail) = input.split_at(len);
    *input = tail;
    Ok(head)
}

fn read_u8(input: &mut &[u8], field: &str) -> MidgeResult<u8> {
    Ok(read_exact_slice(input, 1, field)?[0])
}

fn read_u32(input: &mut &[u8], field: &str) -> MidgeResult<u32> {
    let raw = read_exact_slice(input, 4, field)?;
    Ok(u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]))
}

fn read_u64(input: &mut &[u8], field: &str) -> MidgeResult<u64> {
    let raw = read_exact_slice(input, 8, field)?;
    Ok(u64::from_le_bytes([
        raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
    ]))
}

fn read_len_prefixed_bytes(input: &mut &[u8], field: &str) -> MidgeResult<Bytes> {
    let len = usize::try_from(read_u32(input, field)?).unwrap_or(usize::MAX);
    let raw = read_exact_slice(input, len, field)?;
    Ok(Bytes::copy_from_slice(raw))
}

/// Encode an owned set of WAL records as a nested transaction batch payload.
///
/// # Errors
///
/// Returns an error if the batch is empty, sequence metadata is inconsistent,
/// a record contains nested transaction markers, metadata does not match the
/// outer transaction, or encoded lengths exceed `u32::MAX`.
pub fn encode_txn_batch_payload(
    txn_id: u64,
    begin_seq: u64,
    commit_seq: u64,
    writer_epoch: u64,
    records: &[WalRecord],
) -> MidgeResult<Bytes> {
    let records: Vec<_> = records
        .iter()
        .map(|record| TxnBatchEncodeRecord {
            cf_id: record.cf_id,
            op: record.op,
            key: record.key.as_ref(),
            value: record.value.as_ref().map(Bytes::as_ref),
            seq: record.seq,
            expiration: record.expiration,
            range_end: record.range_end.as_ref().map(Bytes::as_ref),
            txn_id: record.txn_id,
            writer_epoch: record.writer_epoch,
        })
        .collect();
    encode_txn_batch_payload_records(txn_id, begin_seq, commit_seq, writer_epoch, &records)
}

/// Encode borrowed WAL record views as a nested transaction batch payload.
///
/// # Errors
///
/// Returns an error if the batch is empty, sequence metadata is inconsistent,
/// a record contains nested transaction markers, metadata does not match the
/// outer transaction, or encoded lengths exceed `u32::MAX`.
pub fn encode_txn_batch_payload_records(
    txn_id: u64,
    begin_seq: u64,
    commit_seq: u64,
    writer_epoch: u64,
    records: &[TxnBatchEncodeRecord<'_>],
) -> MidgeResult<Bytes> {
    if records.is_empty() {
        return Err(MidgeError::InvalidArgument(
            "transaction batch must contain at least one operation".into(),
        ));
    }
    if commit_seq <= begin_seq {
        return Err(MidgeError::InvalidArgument(
            "transaction batch commit sequence must be greater than begin sequence".into(),
        ));
    }
    let expected_op_count = u64::try_from(records.len()).unwrap_or(u64::MAX);
    if commit_seq - begin_seq - 1 != expected_op_count {
        return Err(MidgeError::InvalidArgument(
            "transaction batch sequence span does not match operation count".into(),
        ));
    }

    let expected_payload_len = txn_batch_payload_records_encoded_len(records)?;
    let mut buf = Vec::with_capacity(expected_payload_len);
    buf.extend_from_slice(&TXN_BATCH_MAGIC);
    buf.push(TXN_BATCH_VERSION);
    buf.extend_from_slice(&txn_id.to_le_bytes());
    buf.extend_from_slice(&begin_seq.to_le_bytes());
    buf.extend_from_slice(&commit_seq.to_le_bytes());
    buf.extend_from_slice(
        &u32::try_from(records.len())
            .map_err(|_| {
                MidgeError::InvalidArgument("transaction batch op_count exceeds u32::MAX".into())
            })?
            .to_le_bytes(),
    );

    for (index, record) in records.iter().enumerate() {
        if matches!(
            record.op,
            WalOpKind::TxnBegin | WalOpKind::TxnCommit | WalOpKind::TxnBatch
        ) {
            return Err(MidgeError::InvalidArgument(
                "transaction batch cannot contain nested transaction markers".into(),
            ));
        }
        let expected_seq = begin_seq + 1 + u64::try_from(index).unwrap_or(u64::MAX);
        if record.seq != expected_seq {
            return Err(MidgeError::InvalidArgument(format!(
                "transaction batch contains non-contiguous sequence {} at op index {index} (expected {expected_seq})",
                record.seq
            )));
        }
        if record.txn_id != Some(txn_id) {
            return Err(MidgeError::InvalidArgument(format!(
                "transaction batch record {index} has mismatched txn_id {:?} (expected {txn_id})",
                record.txn_id
            )));
        }
        if record.writer_epoch != writer_epoch {
            return Err(MidgeError::InvalidArgument(format!(
                "transaction batch record {index} has mismatched writer_epoch {} (expected {writer_epoch})",
                record.writer_epoch
            )));
        }

        buf.push(record.op.to_wire_format());
        buf.extend_from_slice(&record.cf_id.to_le_bytes());
        buf.extend_from_slice(&record.seq.to_le_bytes());
        match record.expiration {
            Some(expiration) => {
                buf.push(1);
                buf.extend_from_slice(&expiration.to_le_bytes());
            }
            None => buf.push(0),
        }
        push_len_prefixed_bytes(&mut buf, record.key)?;
        match record.value {
            Some(value) => {
                buf.push(1);
                push_len_prefixed_bytes(&mut buf, value)?;
            }
            None => buf.push(0),
        }
        match record.range_end {
            Some(range_end) => {
                buf.push(1);
                push_len_prefixed_bytes(&mut buf, range_end)?;
            }
            None => buf.push(0),
        }
    }

    Ok(Bytes::from(buf))
}

/// Decode and validate a nested transaction batch payload.
///
/// # Errors
///
/// Returns corruption if the payload magic/version, outer metadata, operation
/// count, sequence range, per-operation fields, flags, or trailing bytes are
/// malformed.
pub fn decode_txn_batch_payload(
    outer_record: &WalRecord,
    payload: &[u8],
) -> MidgeResult<DecodedTxnBatch> {
    let (header, mut input) = decode_txn_batch_header(outer_record, payload)?;
    let records = decode_txn_batch_records(&mut input, header.begin_seq, header.op_count)?;
    if !input.is_empty() {
        return Err(corruption("transaction batch payload has trailing bytes"));
    }

    Ok(DecodedTxnBatch {
        txn_id: header.txn_id,
        begin_seq: header.begin_seq,
        commit_seq: header.commit_seq,
        writer_epoch: outer_record.writer_epoch,
        records,
    })
}

fn decode_txn_batch_header<'a>(
    outer_record: &WalRecord,
    payload: &'a [u8],
) -> MidgeResult<(TxnBatchHeader, &'a [u8])> {
    if payload.len() < TXN_BATCH_MIN_PREFIX_LEN {
        return Err(corruption("truncated transaction batch payload prefix"));
    }
    if payload[0..2] != TXN_BATCH_MAGIC {
        return Err(corruption("invalid transaction batch payload magic"));
    }
    if payload[2] != TXN_BATCH_VERSION {
        return Err(corruption("unsupported transaction batch payload version"));
    }

    let mut input = &payload[3..];
    let txn_id = read_u64(&mut input, "txn_id")?;
    let begin_seq = read_u64(&mut input, "begin_seq")?;
    let commit_seq = read_u64(&mut input, "commit_seq")?;
    let op_count = usize::try_from(read_u32(&mut input, "op_count")?).unwrap_or(usize::MAX);

    let header = TxnBatchHeader {
        txn_id,
        begin_seq,
        commit_seq,
        op_count,
    };
    validate_txn_batch_header(outer_record, &header)?;
    Ok((header, input))
}

fn validate_txn_batch_header(outer_record: &WalRecord, header: &TxnBatchHeader) -> MidgeResult<()> {
    if header.op_count == 0 {
        return Err(corruption("transaction batch payload has empty op_count"));
    }
    if outer_record.txn_id != Some(header.txn_id) {
        return Err(corruption(format!(
            "transaction batch outer txn_id {:?} does not match payload txn_id {}",
            outer_record.txn_id, header.txn_id
        )));
    }
    if outer_record.seq != header.commit_seq {
        return Err(corruption(format!(
            "transaction batch outer seq {} does not match payload commit_seq {}",
            outer_record.seq, header.commit_seq
        )));
    }
    if header.commit_seq <= header.begin_seq {
        return Err(corruption(
            "transaction batch payload commit_seq must be greater than begin_seq",
        ));
    }
    let expected_count =
        usize::try_from(header.commit_seq - header.begin_seq - 1).unwrap_or(usize::MAX);
    if header.op_count != expected_count {
        return Err(corruption(format!(
            "transaction batch payload op_count {} does not match sequence span {expected_count}",
            header.op_count
        )));
    }
    Ok(())
}

fn decode_txn_batch_records(
    input: &mut &[u8],
    begin_seq: u64,
    op_count: usize,
) -> MidgeResult<Vec<TxnBatchRecord>> {
    let mut records = Vec::with_capacity(op_count);
    for index in 0..op_count {
        records.push(decode_txn_batch_record(input, begin_seq, index)?);
    }
    Ok(records)
}

fn decode_txn_batch_record(
    input: &mut &[u8],
    begin_seq: u64,
    index: usize,
) -> MidgeResult<TxnBatchRecord> {
    let op = WalOpKind::from_wire_format(read_u8(input, "op")?)?;
    if matches!(
        op,
        WalOpKind::TxnBegin | WalOpKind::TxnCommit | WalOpKind::TxnBatch
    ) {
        return Err(corruption(
            "transaction batch payload cannot contain nested transaction markers",
        ));
    }
    let cf_id = read_u32(input, "cf_id")?;
    let seq = read_u64(input, "seq")?;
    let expected_seq = begin_seq + 1 + u64::try_from(index).unwrap_or(u64::MAX);
    if seq != expected_seq {
        return Err(corruption(format!(
            "transaction batch payload sequence {seq} at op index {index} does not match expected {expected_seq}"
        )));
    }
    let expiration = read_optional_u64(input, "expiration")?;
    let key = read_len_prefixed_bytes(input, "key")?;
    let value = read_optional_bytes(input, "value")?;
    let range_end = read_optional_bytes(input, "range_end")?;
    Ok(TxnBatchRecord {
        cf_id,
        op,
        key,
        value,
        seq,
        expiration,
        range_end,
    })
}

fn read_optional_u64(input: &mut &[u8], field: &str) -> MidgeResult<Option<u64>> {
    match read_u8(input, &format!("{field} flag"))? {
        0 => Ok(None),
        1 => Ok(Some(read_u64(input, field)?)),
        flag => Err(corruption(format!(
            "invalid transaction batch {field} flag {flag}"
        ))),
    }
}

fn read_optional_bytes(input: &mut &[u8], field: &str) -> MidgeResult<Option<Bytes>> {
    match read_u8(input, &format!("{field} flag"))? {
        0 => Ok(None),
        1 => Ok(Some(read_len_prefixed_bytes(input, field)?)),
        flag => Err(corruption(format!(
            "invalid transaction batch {field} flag {flag}"
        ))),
    }
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
        // Arrange
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"k"),
            Some(Bytes::from_static(b"")),
            8,
            1,
        );

        // Act
        let encoded = encode(&record).unwrap();
        let decoded = decode(&encoded[..]).unwrap();

        // Assert
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
            other => panic!("expected corruption error, got: {other:?}"),
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
            other => panic!("expected corruption error, got: {other:?}"),
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
            other => panic!("expected corruption error, got: {other:?}"),
        }
    }

    #[test]
    fn should_roundtrip_transaction_batch_payload() {
        // Arrange
        let mut put = WalRecord::new_cf(
            7,
            WalOpKind::Insert,
            Bytes::from_static(b"k1"),
            Some(Bytes::from_static(b"v1")),
            11,
            9,
        );
        put.txn_id = Some(42);

        let mut delete_range = WalRecord::new_cf(
            3,
            WalOpKind::DeleteRange,
            Bytes::from_static(b"a"),
            None,
            12,
            9,
        );
        delete_range.txn_id = Some(42);
        delete_range.range_end = Some(Bytes::from_static(b"z"));

        let payload =
            encode_txn_batch_payload(42, 10, 13, 9, &[put.clone(), delete_range.clone()]).unwrap();
        let mut outer = WalRecord::new_cf(
            0,
            WalOpKind::TxnBatch,
            Bytes::from_static(b"txn"),
            Some(payload.clone()),
            13,
            9,
        );
        outer.txn_id = Some(42);

        let decoded = decode_txn_batch_payload(&outer, &payload).unwrap();

        // Act
        // Assert
        assert_eq!(decoded.txn_id, 42);
        assert_eq!(decoded.begin_seq, 10);
        assert_eq!(decoded.commit_seq, 13);
        assert_eq!(decoded.records.len(), 2);
        assert_eq!(decoded.records[0].op, WalOpKind::Insert);
        assert_eq!(decoded.records[0].seq, 11);
        assert_eq!(decoded.records[1].op, WalOpKind::DeleteRange);
        assert_eq!(decoded.records[1].range_end, Some(Bytes::from_static(b"z")));
    }

    #[test]
    fn should_estimate_transaction_batch_payload_encoded_length() {
        // Arrange
        let records = [
            TxnBatchEncodeRecord {
                cf_id: 7,
                op: WalOpKind::Put,
                key: b"alpha".as_ref(),
                value: Some(b"value".as_ref()),
                seq: 11,
                expiration: Some(123),
                range_end: None,
                txn_id: Some(42),
                writer_epoch: 9,
            },
            TxnBatchEncodeRecord {
                cf_id: 3,
                op: WalOpKind::DeleteRange,
                key: b"a".as_ref(),
                value: None,
                seq: 12,
                expiration: None,
                range_end: Some(b"z".as_ref()),
                txn_id: Some(42),
                writer_epoch: 9,
            },
        ];

        // Act
        let expected_len = txn_batch_payload_records_encoded_len(&records).unwrap();
        let payload = encode_txn_batch_payload_records(42, 10, 13, 9, &records).unwrap();

        // Assert
        assert_eq!(expected_len, payload.len());
    }

    #[test]
    fn should_reject_transaction_batch_payload_with_sequence_gap() {
        // Arrange
        let mut put = WalRecord::new_cf(
            0,
            WalOpKind::Put,
            Bytes::from_static(b"k"),
            Some(Bytes::from_static(b"v")),
            12,
            5,
        );
        put.txn_id = Some(77);

        let error = encode_txn_batch_payload(77, 10, 12, 5, &[put]).unwrap_err();
        // Act
        // Assert
        assert!(error.to_string().contains("non-contiguous sequence"));
    }

    #[test]
    fn should_reject_transaction_batch_payload_when_outer_metadata_mismatches() {
        // Arrange
        let mut put = WalRecord::new_cf(
            0,
            WalOpKind::Put,
            Bytes::from_static(b"k"),
            Some(Bytes::from_static(b"v")),
            2,
            1,
        );
        put.txn_id = Some(5);
        let payload = encode_txn_batch_payload(5, 1, 3, 1, &[put]).unwrap();
        let mut outer = WalRecord::new_cf(
            0,
            WalOpKind::TxnBatch,
            Bytes::from_static(b"txn"),
            Some(payload.clone()),
            99,
            1,
        );
        outer.txn_id = Some(6);

        let error = decode_txn_batch_payload(&outer, &payload).unwrap_err();
        // Act
        // Assert
        assert!(error.to_string().contains("outer txn_id"));
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
            // Arrange
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

            // Act
            let encoded = encode(&record).unwrap();
            let decoded = decode(&encoded[..]).unwrap();

            // Assert
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
