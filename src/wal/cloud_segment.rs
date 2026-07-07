//! Cloud WAL segment helpers owned by the WAL subsystem.
//!
//! Hybrid storage owns cloud orchestration and deletion policy, but WAL owns
//! the segment key format and byte-level interpretation of WAL frames.

use super::types::{WalOpKind, WalRecord};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DataCoverageRecord {
    pub(crate) cf_id: u32,
    pub(crate) key: Vec<u8>,
    pub(crate) range_end: Option<Vec<u8>>,
    pub(crate) seq: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SegmentValidation {
    pub(crate) max_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SegmentReadback {
    pub(crate) validation: SegmentValidation,
    pub(crate) data_records: Vec<DataCoverageRecord>,
}

#[must_use]
pub(crate) fn file_name(segment_id: u64) -> String {
    super::segment_file_name(segment_id)
}

#[must_use]
pub(crate) fn object_key(segment_id: u64) -> String {
    super::segment_object_key(segment_id)
}

pub(crate) fn validate_bytes(
    key: &str,
    data: &[u8],
    expected_max_sequence: u64,
) -> Result<SegmentReadback, String> {
    if data.is_empty() {
        return Err(format!("cloud WAL segment '{key}' is empty"));
    }

    let mut pos = 0usize;
    let mut records = 0usize;
    let mut observed_max_sequence = 0u64;
    let mut data_records = Vec::new();
    while pos < data.len() {
        let header_end = pos
            .checked_add(super::frame::WAL_FRAME_HEADER_LEN)
            .ok_or_else(|| format!("cloud WAL segment '{key}' frame offset overflow"))?;
        if header_end > data.len() {
            return Err(format!(
                "cloud WAL segment '{key}' has incomplete frame header at offset {pos}"
            ));
        }

        let (payload_len, expected_crc) = super::frame::decode_frame_header(&data[pos..header_end])
            .map_err(|error| format!("cloud WAL segment '{key}' frame header: {error}"))?;
        let payload_end = header_end
            .checked_add(payload_len)
            .ok_or_else(|| format!("cloud WAL segment '{key}' payload offset overflow"))?;
        if payload_end > data.len() {
            return Err(format!(
                "cloud WAL segment '{key}' has incomplete record at offset {pos}"
            ));
        }

        let payload = &data[header_end..payload_end];
        super::frame::verify_frame_crc(payload, expected_crc)
            .map_err(|error| format!("cloud WAL segment '{key}' frame CRC: {error}"))?;
        let record = super::encoding::decode(payload)
            .map_err(|error| format!("cloud WAL segment '{key}' record decode: {error}"))?;
        observed_max_sequence = observed_max_sequence.max(record.seq);
        append_data_coverage_records(key, &record, &mut data_records)?;
        records += 1;
        pos = payload_end;
    }

    if records == 0 {
        return Err(format!("cloud WAL segment '{key}' contains no records"));
    }
    if observed_max_sequence < expected_max_sequence {
        return Err(format!(
            "cloud WAL segment '{key}' max sequence {observed_max_sequence} is below expected {expected_max_sequence}"
        ));
    }
    if observed_max_sequence > expected_max_sequence {
        return Err(format!(
            "cloud WAL segment '{key}' max sequence {observed_max_sequence} exceeds expected {expected_max_sequence}"
        ));
    }

    Ok(SegmentReadback {
        validation: SegmentValidation {
            max_sequence: observed_max_sequence,
        },
        data_records,
    })
}

fn append_data_coverage_records(
    key: &str,
    record: &WalRecord,
    data_records: &mut Vec<DataCoverageRecord>,
) -> Result<(), String> {
    if matches!(record.op, WalOpKind::TxnBatch) {
        let payload = record
            .value
            .as_ref()
            .ok_or_else(|| format!("cloud WAL segment '{key}' txn batch missing payload"))?;
        let batch = super::encoding::decode_txn_batch_payload(record, payload)
            .map_err(|error| format!("cloud WAL segment '{key}' txn batch decode: {error}"))?;
        for batch_record in batch.records {
            push_data_coverage_record(
                key,
                batch_record.cf_id,
                batch_record.op,
                batch_record.key.as_ref(),
                batch_record
                    .range_end
                    .as_ref()
                    .map(std::convert::AsRef::as_ref),
                batch_record.seq,
                data_records,
            )?;
        }
    } else {
        push_data_coverage_record(
            key,
            record.cf_id,
            record.op,
            record.key.as_ref(),
            record.range_end.as_ref().map(std::convert::AsRef::as_ref),
            record.seq,
            data_records,
        )?;
    }

    Ok(())
}

fn push_data_coverage_record(
    segment_key: &str,
    cf_id: u32,
    op: WalOpKind,
    key: &[u8],
    range_end: Option<&[u8]>,
    seq: u64,
    data_records: &mut Vec<DataCoverageRecord>,
) -> Result<(), String> {
    match op {
        WalOpKind::Put | WalOpKind::Insert | WalOpKind::Delete => {
            data_records.push(DataCoverageRecord {
                cf_id,
                key: key.to_vec(),
                range_end: None,
                seq,
            });
        }
        WalOpKind::DeleteRange => {
            let range_end = range_end.ok_or_else(|| {
                format!("cloud WAL segment '{segment_key}' delete range missing range_end")
            })?;
            data_records.push(DataCoverageRecord {
                cf_id,
                key: key.to_vec(),
                range_end: Some(range_end.to_vec()),
                seq,
            });
        }
        WalOpKind::TxnBegin | WalOpKind::TxnCommit | WalOpKind::TxnBatch => {}
    }
    Ok(())
}
