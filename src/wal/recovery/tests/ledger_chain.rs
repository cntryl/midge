//! Strict replay of a ledger's members checks every file against the chain
//! and against the facts the ledger recorded for it (#843).

use super::*;
use crate::io::MockFs;
use crate::runtime::wal_history::{fixture, WalHistory};
use crate::wal::format::segment::{SegmentBuilder, SegmentHeader, HEADER_LEN};

fn segment_path(id: crate::wal::ledger::SegmentId) -> FsPath {
    FsPath::new(crate::wal::segment_file_name(id))
}

fn read_segment(wal: &dyn Fs, id: crate::wal::ledger::SegmentId) -> Vec<u8> {
    crate::io::read_all(wal, &segment_path(id))
        .expect("read the segment")
        .to_vec()
}

fn replace_segment(wal: &dyn Fs, id: crate::wal::ledger::SegmentId, bytes: &[u8]) {
    let mut file = wal
        .open(
            &segment_path(id),
            crate::io::OpenOptions {
                mode: crate::io::OpenMode::ReadWrite,
                create: true,
                create_new: false,
                truncate: true,
            },
        )
        .expect("open the segment");
    file.write_at(0, Bytes::copy_from_slice(bytes))
        .expect("write the segment");
}

/// A WAL directory whose ledger sealed segments 1 and 2, one put each, and
/// left segment 3 open.
fn ledgered_wal() -> (Arc<dyn Fs>, WalHistory) {
    let wal: Arc<dyn Fs> = Arc::new(MockFs::new());
    let mut history = WalHistory::open(Arc::clone(&wal), 1, 1).expect("open the WAL ledger");
    for (key, sequence) in [(&b"one"[..], 1), (&b"two"[..], 2)] {
        let id = history.ledger().open_segment();
        let mut bytes = read_segment(wal.as_ref(), id);
        bytes.extend(encode_frame(&WalRecord::new(
            WalOpKind::Put,
            Bytes::copy_from_slice(key),
            Some(Bytes::from_static(b"value")),
            sequence,
        )));
        replace_segment(wal.as_ref(), id, &bytes);
        fixture::rotate(&mut history).expect("seal the segment");
    }
    (wal, history)
}

/// Replay every ledger member strictly, as local recovery does.
fn replay_members(wal: &dyn Fs, history: &WalHistory) -> MidgeResult<RecoveryStats> {
    let segments = ReplaySegments::from_ledger(history.ledger(), segment_path);
    let mut memtables = HashMap::new();
    replay_wal_with_frontiers_within(
        wal,
        &segments,
        &mut memtables,
        ReplayPolicy::Strict,
        &crate::metadata::frontiers::FlushFrontiers::default(),
        None,
    )
}

fn assert_corruption(result: MidgeResult<RecoveryStats>, expected: &str) {
    match result {
        Err(MidgeError::Corruption(message)) => assert!(
            message.contains(expected),
            "expected corruption naming `{expected}`, got: {message}"
        ),
        other => panic!("expected corruption naming `{expected}`, got {other:?}"),
    }
}

#[test]
fn should_replay_every_member_when_files_match_the_ledger() {
    // Arrange
    let (wal, history) = ledgered_wal();

    // Act
    let stats = replay_members(wal.as_ref(), &history).expect("strict replay");

    // Assert
    assert_eq!(stats.record_count, 2);
    assert_eq!(stats.max_sequence, Some(2));
    assert!(!stats.had_corruption);
}

#[test]
fn should_fail_strict_replay_when_member_header_names_another_segment() {
    // Arrange: segment 2's file holds segment 1's bytes.
    let (wal, history) = ledgered_wal();
    let one = read_segment(wal.as_ref(), 1);
    replace_segment(wal.as_ref(), 2, &one);

    // Act
    let result = replay_members(wal.as_ref(), &history);

    // Assert
    assert_corruption(result, "holds the header of segment 1");
}

#[test]
fn should_fail_strict_replay_when_sealed_member_digest_mismatches() {
    // Arrange: a record of segment 1 is replaced by another valid frame of
    // the same length, so only the footer's digest can tell.
    let (wal, history) = ledgered_wal();
    let mut one = read_segment(wal.as_ref(), 1);
    let forged = encode_frame(&WalRecord::new(
        WalOpKind::Put,
        Bytes::from_static(b"ONE"),
        Some(Bytes::from_static(b"value")),
        1,
    ));
    one[HEADER_LEN..HEADER_LEN + forged.len()].copy_from_slice(&forged);
    replace_segment(wal.as_ref(), 1, &one);

    // Act
    let result = replay_members(wal.as_ref(), &history);

    // Assert
    assert_corruption(result, "does not match its records");
}

#[test]
fn should_fail_strict_replay_when_member_links_a_different_predecessor() {
    // Arrange: segment 2's header names a predecessor digest segment 1 does
    // not have.
    let (wal, history) = ledgered_wal();
    let mut two = read_segment(wal.as_ref(), 2);
    let mut header = SegmentHeader::decode(&two).expect("segment header");
    header.prev.digest[0] ^= 0xff;
    two[..HEADER_LEN].copy_from_slice(&header.encode());
    replace_segment(wal.as_ref(), 2, &two);

    // Act
    let result = replay_members(wal.as_ref(), &history);

    // Assert
    assert_corruption(result, "links segment 1");
}

#[test]
fn should_fail_strict_replay_when_sealed_file_differs_from_ledger_facts() {
    // Arrange: segment 2 is replaced by a sealed segment with the same header
    // whose footer matches its own different record.
    let (wal, history) = ledgered_wal();
    let header = SegmentHeader::decode(&read_segment(wal.as_ref(), 2)).expect("segment header");
    let (substitute, _) = SegmentBuilder::from_header(header)
        .record(&WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"substitute"),
            Some(Bytes::from_static(b"value")),
            2,
        ))
        .seal();
    replace_segment(wal.as_ref(), 2, &substitute);

    // Act
    let result = replay_members(wal.as_ref(), &history);

    // Assert
    assert_corruption(result, "not the ledger's");
}

#[test]
fn should_fail_strict_replay_when_sealed_member_ends_without_footer() {
    // Arrange: segment 1 loses its footer but keeps every record.
    let (wal, history) = ledgered_wal();
    let one = read_segment(wal.as_ref(), 1);
    let record_len = encode_frame(&WalRecord::new(
        WalOpKind::Put,
        Bytes::from_static(b"one"),
        Some(Bytes::from_static(b"value")),
        1,
    ))
    .len();
    replace_segment(wal.as_ref(), 1, &one[..HEADER_LEN + record_len]);

    // Act
    let result = replay_members(wal.as_ref(), &history);

    // Assert
    assert_corruption(result, "ends without its footer");
}

#[test]
fn should_fail_strict_replay_when_open_member_belongs_to_another_database() {
    // Arrange: segment 3, the open member, is replaced by another database's
    // segment whose header matches every fact but the holder, holding a
    // record.
    let (wal, history) = ledgered_wal();
    let mut header = SegmentHeader::decode(&read_segment(wal.as_ref(), 3)).expect("segment header");
    header.holder = [0x42; 16];
    let foreign = SegmentBuilder::from_header(header)
        .record(&WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"foreign"),
            Some(Bytes::from_static(b"value")),
            3,
        ))
        .open();
    replace_segment(wal.as_ref(), 3, &foreign);

    // Act
    let result = replay_members(wal.as_ref(), &history);

    // Assert
    assert_corruption(result, "holder");
}
