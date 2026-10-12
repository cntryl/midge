use super::*;
use crate::wal::types::{WalOpKind, WalRecord};
use bytes::Bytes;

fn put(seq: u64, cf: u32) -> WalRecord {
    WalRecord::new_cf(
        cf,
        WalOpKind::Put,
        Bytes::from_static(b"key"),
        Some(Bytes::from_static(b"value")),
        seq,
        1,
    )
}

fn header(id: SegmentId) -> SegmentHeader {
    SegmentHeader {
        id,
        epoch: 3,
        holder: [9; 16],
        first_seq: 5,
        prev: ChainLink {
            id: id - 1,
            last_seq: 4,
            digest: [1; 16],
        },
    }
}

/// Scan `bytes` the way every walker does, returning the scan.
fn scan(bytes: &[u8]) -> MidgeResult<SegmentScan> {
    let mut scan = SegmentScan::start(bytes)?;
    let mut pos = HEADER_LEN as u64;
    loop {
        let step = crate::wal::frame::next_frame(
            &bytes,
            &"test",
            pos,
            crate::wal::frame::FrameLimits::default(),
        )
        .map_err(crate::wal::frame::FrameError::into_error)?;
        let crate::wal::frame::FrameStep::Frame { payload, next_pos } = step else {
            return Ok(scan);
        };
        if scan.frame(&payload)? == ScannedFrame::Record {
            let record = crate::wal::encoding::decode_view(&payload)?;
            scan.observe(record.seq, record.cf_id)?;
        }
        pos = next_pos;
    }
}

#[test]
fn should_round_trip_header_when_encoding() {
    // Arrange
    let header = header(7);

    // Act
    let decoded = SegmentHeader::decode(&header.encode());

    // Assert
    assert_eq!(decoded.expect("decode"), header);
}

#[test]
fn should_reject_header_when_crc_mismatches() {
    // Arrange
    let mut bytes = header(7).encode();
    bytes[20] ^= 1;

    // Act
    let result = SegmentHeader::decode(&bytes);

    // Assert
    assert!(
        matches!(&result, Err(MidgeError::Corruption(message)) if message.contains("checksum")),
        "{result:?}"
    );
}

#[test]
fn should_seal_digest_of_bytes_before_footer() {
    // Arrange
    let builder = SegmentBuilder::new(2, 1, ChainLink::GENESIS)
        .record(&put(1, 0))
        .record(&put(2, 4));
    let open = builder.open();

    // Act
    let (sealed, link) = builder.seal();
    let scanned = scan(&sealed).expect("scan sealed segment");

    // Assert
    assert_eq!(&sealed[..open.len()], open.as_slice());
    assert_eq!(
        link.digest,
        xxhash_rust::xxh3::xxh3_128(&open).to_le_bytes()
    );
    assert_eq!(link.last_seq, 2);
    let footer = scanned.footer().expect("sealed");
    assert_eq!(footer.sealed_facts().link, link);
    assert_eq!(footer.max_seq_per_cf, vec![(0, 1), (4, 2)]);
    assert_eq!(footer.data_len, open.len() as u64);
}

#[test]
fn should_fail_when_bytes_follow_footer() {
    // Arrange
    let (mut sealed, _) = SegmentBuilder::new(2, 1, ChainLink::GENESIS)
        .record(&put(1, 0))
        .seal();
    let payload = crate::wal::encoding::encode(&put(2, 0)).expect("encode");
    crate::wal::frame::append_frame(&mut sealed, &payload).expect("frame");

    // Act
    let result = scan(&sealed);

    // Assert
    assert!(
        matches!(&result, Err(MidgeError::Corruption(message)) if message.contains("follow the footer")),
        "{result:?}"
    );
}

#[test]
fn should_fail_when_footer_family_maxima_disagree_with_records() {
    // Arrange: a footer naming family 5 where the record is in family 0.
    let builder = SegmentBuilder::new(2, 1, ChainLink::GENESIS).record(&put(1, 0));
    let mut bytes = builder.open();
    let mut facts = SealedFacts::of(
        &builder.header(),
        &lenient_tail(&bytes[HEADER_LEN..]),
        bytes.len() as u64,
        xxhash_rust::xxh3::xxh3_128(&bytes).to_le_bytes(),
    );
    facts.max_seq_per_cf = vec![(5, 1)];
    bytes.extend(Footer::new(&facts, 1).encode_frame().expect("footer"));

    // Act
    let result = scan(&bytes);

    // Assert
    assert!(
        matches!(&result, Err(MidgeError::Corruption(message)) if message.contains("does not match its records")),
        "{result:?}"
    );
}

#[test]
fn should_reject_record_below_first_sequence_of_its_segment() {
    // Arrange
    let bytes = SegmentBuilder::new(2, 1, ChainLink::GENESIS)
        .first_seq(10)
        .record(&put(9, 0))
        .open();

    // Act
    let result = scan(&bytes);

    // Assert
    assert!(
        matches!(&result, Err(MidgeError::Corruption(message)) if message.contains("conflicting or repeated WAL sequence 9")),
        "{result:?}"
    );
}

#[test]
fn should_round_trip_footer_payload_when_encoding() {
    // Arrange
    let footer = Footer {
        id: 4,
        record_count: 3,
        first_seq: 10,
        last_seq: 12,
        data_len: 400,
        max_seq_per_cf: vec![(0, 11), (2, 12)],
        digest: [3; 16],
    };

    // Act
    let frame = footer.encode_frame().expect("encode");
    let decoded = Footer::decode_payload(&frame[crate::wal::frame::WAL_FRAME_HEADER_LEN..]);

    // Assert
    assert_eq!(decoded.expect("decode"), footer);
}

#[test]
fn should_count_a_header_only_segment_as_holding_no_records() {
    // Arrange
    let lengths = [0, 64, HEADER_LEN as u64, HEADER_LEN as u64 + 1];

    // Act
    let empty = lengths.map(holds_no_records);

    // Assert
    assert_eq!(empty, [true, true, true, false]);
}
