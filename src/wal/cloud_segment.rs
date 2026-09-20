//! Cloud WAL segment helpers owned by the WAL subsystem.
//!
//! Hybrid storage owns cloud orchestration and deletion policy, but WAL owns
//! the segment key format and byte-level interpretation of WAL frames.

use super::types::{WalOpKind, WalOpRole, WalRecord};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DataCoverageRecord {
    pub(crate) cf_id: u32,
    pub(crate) op: WalOpKind,
    pub(crate) key: Vec<u8>,
    pub(crate) value: Option<Vec<u8>>,
    pub(crate) expiration: Option<u64>,
    pub(crate) range_end: Option<Vec<u8>>,
    pub(crate) seq: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SegmentValidation {
    pub(crate) max_sequence: u64,
    pub(crate) writer_epoch: u64,
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
pub(crate) fn object_key(segment_id: u64, writer_epoch: u64) -> String {
    super::segment_object_key(segment_id, writer_epoch)
}

/// Validate a segment without collecting coverage records.
///
/// Coverage collection owns a second copy of every key and value, which a
/// caller that only checks the segment does not need.
pub(crate) fn validate_bytes(
    key: &str,
    data: &[u8],
    expected_max_sequence: u64,
) -> Result<SegmentValidation, String> {
    let validation = validate_segment_bytes(key, data)?;
    check_expected_max_sequence(key, validation.max_sequence, expected_max_sequence)?;
    Ok(validation)
}

/// Validate a segment and collect the records prune coverage compares.
pub(crate) fn validate_bytes_with_coverage(
    key: &str,
    data: &[u8],
    expected_max_sequence: u64,
) -> Result<SegmentReadback, String> {
    let readback = inspect_bytes(key, data)?;
    check_expected_max_sequence(key, readback.validation.max_sequence, expected_max_sequence)?;
    Ok(readback)
}

/// Frame-, CRC- and shape-check every record, returning only the segment's
/// validation summary.
pub(crate) fn validate_segment_bytes(key: &str, data: &[u8]) -> Result<SegmentValidation, String> {
    inspect(key, data, false).map(|readback| readback.validation)
}

fn check_expected_max_sequence(
    key: &str,
    observed_max_sequence: u64,
    expected_max_sequence: u64,
) -> Result<(), String> {
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

    Ok(())
}

pub(crate) fn inspect_bytes(key: &str, data: &[u8]) -> Result<SegmentReadback, String> {
    inspect(key, data, true)
}

fn inspect(key: &str, data: &[u8], collect_coverage: bool) -> Result<SegmentReadback, String> {
    if data.is_empty() {
        return Err(format!("cloud WAL segment '{key}' is empty"));
    }

    let mut pos = 0u64;
    let mut records = 0usize;
    let mut observed_max_sequence = 0u64;
    let mut observed_writer_epoch = None;
    let mut data_records = Vec::new();
    loop {
        // A sealed segment is complete by construction, so any short or torn
        // frame is corruption here rather than a tolerable tail.
        let step = super::frame::next_frame(&data, &key, pos, super::frame::FrameLimits::default())
            .map_err(|error| format!("cloud WAL segment '{key}' frame: {}", error.into_error()))?;
        let (payload, next_pos) = match step {
            super::frame::FrameStep::Eof => break,
            super::frame::FrameStep::Frame { payload, next_pos } => (payload, next_pos),
        };
        let record = super::encoding::decode(payload.as_ref())
            .map_err(|error| format!("cloud WAL segment '{key}' record decode: {error}"))?;
        if let Some(expected_epoch) = observed_writer_epoch {
            if record.writer_epoch != expected_epoch {
                return Err(format!(
                    "cloud WAL segment '{key}' mixes writer epochs {expected_epoch} and {}",
                    record.writer_epoch
                ));
            }
        } else {
            observed_writer_epoch = Some(record.writer_epoch);
        }
        observed_max_sequence = observed_max_sequence.max(record.seq);
        if collect_coverage {
            append_data_coverage_records(key, &record, &mut data_records)?;
        }
        records += 1;
        pos = next_pos;
    }

    if records == 0 {
        return Err(format!("cloud WAL segment '{key}' contains no records"));
    }
    Ok(SegmentReadback {
        validation: SegmentValidation {
            max_sequence: observed_max_sequence,
            writer_epoch: observed_writer_epoch.unwrap_or_default(),
        },
        data_records,
    })
}

fn append_data_coverage_records(
    key: &str,
    record: &WalRecord,
    data_records: &mut Vec<DataCoverageRecord>,
) -> Result<(), String> {
    if record.op.is_transaction_batch() {
        let payload = record
            .value
            .as_ref()
            .ok_or_else(|| format!("cloud WAL segment '{key}' txn batch missing payload"))?;
        let batch = super::encoding::decode_txn_batch_payload(record, payload)
            .map_err(|error| format!("cloud WAL segment '{key}' txn batch decode: {error}"))?;
        for batch_record in batch.records {
            push_data_coverage_record(
                key,
                DataCoverageRecord {
                    cf_id: batch_record.cf_id,
                    op: batch_record.op,
                    key: batch_record.key.to_vec(),
                    value: batch_record.value.map(|value| value.to_vec()),
                    expiration: batch_record.expiration,
                    range_end: batch_record.range_end.map(|end| end.to_vec()),
                    seq: batch_record.seq,
                },
                data_records,
            )?;
        }
    } else {
        push_data_coverage_record(
            key,
            DataCoverageRecord {
                cf_id: record.cf_id,
                op: record.op,
                key: record.key.to_vec(),
                value: record.value.as_ref().map(|value| value.to_vec()),
                expiration: record.expiration,
                range_end: record.range_end.as_ref().map(|end| end.to_vec()),
                seq: record.seq,
            },
            data_records,
        )?;
    }

    Ok(())
}

fn push_data_coverage_record(
    segment_key: &str,
    mut record: DataCoverageRecord,
    data_records: &mut Vec<DataCoverageRecord>,
) -> Result<(), String> {
    match record.op.role() {
        WalOpRole::ValueWrite | WalOpRole::PointDelete => {
            record.range_end = None;
            data_records.push(record);
        }
        WalOpRole::RangeDelete => {
            if record.range_end.is_none() {
                return Err(format!(
                    "cloud WAL segment '{segment_key}' delete range missing range_end"
                ));
            }
            record.value = None;
            record.expiration = None;
            data_records.push(record);
        }
        WalOpRole::TransactionBegin
        | WalOpRole::TransactionCommit
        | WalOpRole::TransactionBatch => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn append_record_frame(bytes: &mut Vec<u8>, sequence: u64, writer_epoch: u64) {
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key"),
            Some(Bytes::from_static(b"value")),
            sequence,
            writer_epoch,
        );
        let payload = crate::wal::encoding::encode(&record).unwrap();
        crate::wal::frame::append_frame(bytes, &payload).unwrap();
    }

    #[test]
    fn should_reject_segment_given_records_from_multiple_writer_epochs() {
        // Arrange
        let mut bytes = Vec::new();
        append_record_frame(&mut bytes, 1, 41);
        append_record_frame(&mut bytes, 2, 42);

        // Act
        let result = inspect_bytes("wal/00000000000000000001.wal", &bytes);

        // Assert
        let error = result.unwrap_err();
        assert!(error.contains("mixes writer epochs 41 and 42"), "{error}");
    }

    #[test]
    fn should_extract_exact_coverage_metadata_from_transaction_batch() {
        // Arrange
        let mut put = WalRecord::new_cf(
            3,
            WalOpKind::Insert,
            Bytes::from_static(b"put-key"),
            Some(Bytes::from_static(b"put-value")),
            2,
            7,
        );
        put.expiration = Some(123_456);
        put.txn_id = Some(11);
        let mut delete = WalRecord::new_cf(
            4,
            WalOpKind::Delete,
            Bytes::from_static(b"delete-key"),
            None,
            3,
            7,
        );
        delete.txn_id = Some(11);
        let mut delete_range = WalRecord::new_cf(
            5,
            WalOpKind::DeleteRange,
            Bytes::from_static(b"range-start"),
            None,
            4,
            7,
        );
        delete_range.range_end = Some(Bytes::from_static(b"range-end"));
        delete_range.txn_id = Some(11);
        let payload = crate::wal::encoding::encode_txn_batch_payload(
            11,
            1,
            5,
            7,
            &[put, delete, delete_range],
        )
        .expect("encode transaction batch");
        let mut batch = WalRecord::new_cf(
            0,
            WalOpKind::TxnBatch,
            Bytes::from_static(b"txn"),
            Some(payload),
            5,
            7,
        );
        batch.txn_id = Some(11);
        let encoded = crate::wal::encoding::encode(&batch).expect("encode outer batch record");
        let mut bytes = Vec::new();
        crate::wal::frame::append_frame(&mut bytes, &encoded).expect("append batch frame");

        // Act
        let readback = inspect_bytes("wal/00000000000000000001.wal", &bytes)
            .expect("inspect transaction batch");

        // Assert
        assert_eq!(
            readback.data_records,
            vec![
                DataCoverageRecord {
                    cf_id: 3,
                    op: WalOpKind::Insert,
                    key: b"put-key".to_vec(),
                    value: Some(b"put-value".to_vec()),
                    expiration: Some(123_456),
                    range_end: None,
                    seq: 2,
                },
                DataCoverageRecord {
                    cf_id: 4,
                    op: WalOpKind::Delete,
                    key: b"delete-key".to_vec(),
                    value: None,
                    expiration: None,
                    range_end: None,
                    seq: 3,
                },
                DataCoverageRecord {
                    cf_id: 5,
                    op: WalOpKind::DeleteRange,
                    key: b"range-start".to_vec(),
                    value: None,
                    expiration: None,
                    range_end: Some(b"range-end".to_vec()),
                    seq: 4,
                },
            ]
        );
    }
}
