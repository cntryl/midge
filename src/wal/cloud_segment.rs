//! Byte validation for sealed cloud WAL objects. Retention consumes the verified
//! persisted summary; it never materializes keys or rereads payloads for coverage.

/// The verified header and footer of a sealed, nonempty segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SegmentValidation {
    pub(crate) writer_epoch: u64,
    pub(crate) prev: crate::wal::format::ChainLink,
    pub(crate) facts: crate::wal::format::SealedFacts,
}

impl SegmentValidation {
    pub(crate) fn identity(&self) -> crate::wal::format::SegmentIdentity {
        crate::wal::format::SegmentIdentity {
            epoch: self.writer_epoch,
            prev: self.prev,
            link: self.facts.link,
        }
    }

    pub(crate) fn max_sequence(&self) -> u64 {
        self.facts.link.last_seq
    }
}

pub(crate) fn validate_bytes(
    key: &str,
    data: &[u8],
    expected_max_sequence: u64,
) -> Result<SegmentValidation, String> {
    let validation = validate_segment_bytes(key, data)?;
    if validation.max_sequence() != expected_max_sequence {
        return Err(format!("cloud WAL segment '{key}' max sequence {} differs from expected {expected_max_sequence}", validation.max_sequence()));
    }
    Ok(validation)
}

/// Verify every frame, CRC, record shape, family maximum and sealed digest.
pub(crate) fn validate_segment_bytes(key: &str, data: &[u8]) -> Result<SegmentValidation, String> {
    let mut scan = super::format::SegmentScan::start(data)
        .map_err(|error| format!("cloud WAL segment '{key}' header: {error}"))?;
    let mut pos = super::format::HEADER_LEN as u64;
    loop {
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
        if record.op.is_transaction_batch() {
            let payload = record
                .value
                .as_ref()
                .ok_or_else(|| format!("cloud WAL segment '{key}' txn batch missing payload"))?;
            super::encoding::decode_txn_batch_payload(&record, payload)
                .map_err(|error| format!("cloud WAL segment '{key}' txn batch decode: {error}"))?;
        }
        scan.observe(record.seq, record.cf_id)
            .map_err(|error| format!("cloud WAL segment '{key}': {error}"))?;
    }
    scan.finish(data.len() as u64)
        .map_err(|error| format!("cloud WAL segment '{key}': {error}"))?;
    let footer = scan
        .footer()
        .ok_or_else(|| format!("cloud WAL segment '{key}' ends without its footer"))?;
    if footer.record_count == 0 {
        return Err(format!("cloud WAL segment '{key}' contains no records"));
    }
    Ok(SegmentValidation {
        writer_epoch: scan.header().epoch,
        prev: scan.header().prev,
        facts: footer.sealed_facts(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::format::segment::SegmentBuilder;
    use crate::wal::format::ChainLink;
    use crate::wal::{WalOpKind, WalRecord};
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
    fn should_report_sealed_facts_when_validating_a_segment() {
        // Arrange
        let (bytes, link) = SegmentBuilder::new(4, 41, ChainLink::GENESIS)
            .record(&put(1))
            .record(&put(2))
            .seal();
        // Act
        let validation = validate_segment_bytes("wal/x", &bytes).unwrap();
        // Assert
        assert_eq!(validation.writer_epoch, 41);
        assert_eq!(validation.prev, ChainLink::GENESIS);
        assert_eq!(validation.facts.link, link);
        assert_eq!(validation.facts.max_seq_per_cf, [(0, 2)]);
        assert_eq!(validation.facts.record_count, 2);
    }

    #[test]
    fn should_reject_unsealed_or_empty_segment_as_cloud_object() {
        // Arrange
        let open = SegmentBuilder::new(4, 41, ChainLink::GENESIS)
            .record(&put(1))
            .open();
        let (empty, _) = SegmentBuilder::new(4, 41, ChainLink::GENESIS).seal();
        // Act
        let open_result = validate_segment_bytes("wal/x", &open);
        let empty_result = validate_segment_bytes("wal/x", &empty);
        // Assert
        assert!(open_result.unwrap_err().contains("without its footer"));
        assert!(empty_result.unwrap_err().contains("no records"));
    }

    #[test]
    fn should_include_commit_sequence_when_summarizing_a_resident_transaction() {
        // Arrange
        let mut member =
            WalRecord::new_cf(3, WalOpKind::Delete, Bytes::from_static(b"key"), None, 2);
        member.txn_id = Some(11);
        let payload =
            crate::wal::encoding::encode_txn_batch_payload(11, 1, 3, 3, &[member]).unwrap();
        let mut batch = WalRecord::new_cf(
            3,
            WalOpKind::TxnBatch,
            Bytes::from_static(b"txn"),
            Some(payload),
            3,
        );
        batch.txn_id = Some(11);
        let (bytes, _) = SegmentBuilder::new(1, 7, ChainLink::GENESIS)
            .record(&batch)
            .seal();
        // Act
        let validation = validate_segment_bytes("wal/x", &bytes).unwrap();
        // Assert
        assert_eq!(validation.facts.max_seq_per_cf, [(3, 3)]);
        assert_eq!(validation.facts.record_count, 1);
    }
}
