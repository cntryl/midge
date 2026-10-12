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

/// What a sealed segment's header and footer prove about it (#843).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SegmentValidation {
    /// The footer's last sequence.
    pub(crate) max_sequence: u64,
    /// The header's writer epoch.
    pub(crate) writer_epoch: u64,
    /// The header's segment id.
    pub(crate) id: u64,
}

impl SegmentValidation {
    /// What the segment says it is.
    pub(crate) fn identity(self) -> crate::wal::format::SegmentIdentity {
        crate::wal::format::SegmentIdentity {
            id: self.id,
            epoch: self.writer_epoch,
            last_seq: self.max_sequence,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SegmentReadback {
    pub(crate) validation: SegmentValidation,
    pub(crate) data_records: Vec<DataCoverageRecord>,
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
    inspect(key, data, false, true).map(|readback| readback.validation)
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

/// Inspect a sealed segment the cloud holds or will hold: it must hold a
/// record.
pub(crate) fn inspect_bytes(key: &str, data: &[u8]) -> Result<SegmentReadback, String> {
    inspect(key, data, true, true)
}

/// Inspect a locally sealed segment for retention, which may hold no record.
pub(crate) fn inspect_sealed_bytes(key: &str, data: &[u8]) -> Result<SegmentReadback, String> {
    inspect(key, data, true, false)
}

/// Read a whole sealed segment: its header, every record frame, and the
/// footer that must end it, with one [`SegmentScan`].
fn inspect(
    key: &str,
    data: &[u8],
    collect_coverage: bool,
    require_records: bool,
) -> Result<SegmentReadback, String> {
    let mut scan = super::format::SegmentScan::start(data)
        .map_err(|error| format!("cloud WAL segment '{key}' header: {error}"))?;
    let mut pos = super::format::HEADER_LEN as u64;
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
        let scanned = scan
            .frame(&payload)
            .map_err(|error| format!("cloud WAL segment '{key}': {error}"))?;
        pos = next_pos;
        if scanned == super::format::ScannedFrame::Footer {
            break;
        }
        let record = super::encoding::decode(payload.as_ref())
            .map_err(|error| format!("cloud WAL segment '{key}' record decode: {error}"))?;
        scan.observe(record.seq, record.cf_id)
            .map_err(|error| format!("cloud WAL segment '{key}': {error}"))?;
        if collect_coverage {
            append_data_coverage_records(key, &record, &mut data_records)?;
        }
    }
    scan.finish(data.len() as u64)
        .map_err(|error| format!("cloud WAL segment '{key}': {error}"))?;
    let Some(footer) = scan.footer() else {
        return Err(format!("cloud WAL segment '{key}' ends without its footer"));
    };
    if require_records && footer.record_count == 0 {
        return Err(format!("cloud WAL segment '{key}' contains no records"));
    }
    Ok(SegmentReadback {
        validation: SegmentValidation {
            max_sequence: footer.last_seq,
            writer_epoch: scan.header().epoch,
            id: scan.header().id,
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
    use crate::wal::format::segment::SegmentBuilder;
    use crate::wal::format::ChainLink;
    use bytes::Bytes;

    fn put(sequence: u64) -> WalRecord {
        WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key"),
            Some(Bytes::from_static(b"value")),
            sequence,
        )
    }

    #[test]
    fn should_report_segment_identity_when_validating() {
        // Arrange
        let (bytes, _) = SegmentBuilder::new(4, 41, ChainLink::GENESIS)
            .record(&put(1))
            .record(&put(2))
            .seal();

        // Act
        let validation = validate_segment_bytes("wal/x", &bytes).expect("valid segment");

        // Assert
        assert_eq!(
            validation,
            SegmentValidation {
                max_sequence: 2,
                writer_epoch: 41,
                id: 4,
            }
        );
    }

    #[test]
    fn should_reject_object_whose_header_epoch_differs_from_catalog() {
        // Arrange: the catalog says epoch 42; the object's header says 41.
        let (bytes, _) = SegmentBuilder::new(4, 41, ChainLink::GENESIS)
            .record(&put(1))
            .seal();

        let entry = |id, epoch| {
            crate::wal::cloud_catalog::PublishedWalSegment::from_validated_bytes(
                id, 1, epoch, &bytes,
            )
        };

        // Act
        let wrong_epoch = entry(4, 42).validate_bytes(&bytes);
        let wrong_id = entry(5, 41).validate_bytes(&bytes);
        let matching = entry(4, 41).validate_bytes(&bytes);

        // Assert
        assert!(wrong_epoch.unwrap_err().contains("writer epoch 41"));
        assert!(wrong_id.unwrap_err().contains("segment 4"));
        assert!(matching.is_ok());
    }

    #[test]
    fn should_reject_unsealed_segment_as_cloud_object() {
        // Arrange: an open segment, records but no footer.
        let bytes = SegmentBuilder::new(4, 41, ChainLink::GENESIS)
            .record(&put(1))
            .open();

        // Act
        let result = inspect_bytes("wal/x", &bytes);

        // Assert
        let error = result.unwrap_err();
        assert!(error.contains("without its footer"), "{error}");
    }

    #[test]
    fn should_reject_sealed_segment_without_records_as_cloud_object() {
        // Arrange
        let (bytes, _) = SegmentBuilder::new(4, 41, ChainLink::GENESIS).seal();

        // Act
        let cloud = inspect_bytes("wal/x", &bytes);
        let local = inspect_sealed_bytes("wal/x", &bytes);

        // Assert
        assert!(cloud.unwrap_err().contains("no records"));
        assert_eq!(local.expect("local retention reads it").data_records, []);
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
        );
        put.expiration = Some(123_456);
        put.txn_id = Some(11);
        let mut delete = WalRecord::new_cf(
            3,
            WalOpKind::Delete,
            Bytes::from_static(b"delete-key"),
            None,
            3,
        );
        delete.txn_id = Some(11);
        let mut delete_range = WalRecord::new_cf(
            3,
            WalOpKind::DeleteRange,
            Bytes::from_static(b"range-start"),
            None,
            4,
        );
        delete_range.range_end = Some(Bytes::from_static(b"range-end"));
        delete_range.txn_id = Some(11);
        let payload = crate::wal::encoding::encode_txn_batch_payload(
            11,
            1,
            5,
            3,
            &[put, delete, delete_range],
        )
        .expect("encode transaction batch");
        let mut batch = WalRecord::new_cf(
            3,
            WalOpKind::TxnBatch,
            Bytes::from_static(b"txn"),
            Some(payload),
            5,
        );
        batch.txn_id = Some(11);
        let (bytes, _) = SegmentBuilder::new(1, 7, ChainLink::GENESIS)
            .record(&batch)
            .seal();

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
                    cf_id: 3,
                    op: WalOpKind::Delete,
                    key: b"delete-key".to_vec(),
                    value: None,
                    expiration: None,
                    range_end: None,
                    seq: 3,
                },
                DataCoverageRecord {
                    cf_id: 3,
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
