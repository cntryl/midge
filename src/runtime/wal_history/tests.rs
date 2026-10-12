//! The segment ledger adapter: genesis, rotation files, epoch start,
//! retirement, salvage and rebase.
//!
//! Crash cases run on a simulated device: after every power-loss outcome the
//! WAL must reopen, and every member the ledger names must still hold the
//! bytes it held when it became history.

use super::{replay_segments, SalvageCut, SetAside, WalHistory};
use crate::common::MidgeError;
use crate::io::{Durability, Fs, FsPath, OpenMode, OpenOptions, ReadFs};
use crate::simulation::crash_fs::{CrashDevice, Fault, FaultEffect, FaultError, FaultOp};
use crate::wal::format::{ChainLink, SegmentHeader, HEADER_LEN};
use crate::wal::ledger::SegmentId;
use std::collections::BTreeMap;
use std::sync::Arc;

const POWER_LOSS_SEEDS: u64 = 64;

fn wal(device: &CrashDevice) -> Arc<dyn Fs> {
    crate::io::subdirectory(Arc::new(device.fs()), crate::wal::WAL_DIR)
}

/// [`wal`] as the read-only view [`WalHistory::read`] takes.
fn wal_view(device: &CrashDevice) -> Arc<dyn ReadFs> {
    wal(device)
}

fn name(id: SegmentId) -> String {
    crate::wal::segment_file_name(id)
}

fn key(id: SegmentId) -> String {
    format!("{}/{}", crate::wal::WAL_DIR, name(id))
}

/// Open the WAL at segment 1 under epoch 0, starting it if needed.
fn open(device: &CrashDevice) -> WalHistory {
    WalHistory::open(wal(device), 1, 0).expect("open the WAL history")
}

/// The frame of a put at `seq`, the sequence segment `id` uses.
fn put_frame(id: SegmentId, seq: u64) -> Vec<u8> {
    let record = crate::wal::WalRecord::new(
        crate::wal::WalOpKind::Put,
        bytes::Bytes::from(format!("segment-{id}")),
        Some(bytes::Bytes::from_static(b"value")),
        seq,
    );
    let payload = crate::wal::encoding::encode(&record).expect("encode record");
    let mut frame = Vec::new();
    crate::wal::frame::append_frame(&mut frame, &payload).expect("frame record");
    frame
}

/// Durably append the frames of `seqs` to segment `id`, as a synced writer
/// would.
fn write_records(wal: &dyn Fs, id: SegmentId, seqs: impl IntoIterator<Item = u64>) {
    let mut file = wal
        .open(
            &FsPath::new(name(id)),
            OpenOptions {
                mode: OpenMode::ReadWrite,
                create: false,
                create_new: false,
                truncate: false,
            },
        )
        .expect("open segment");
    for seq in seqs {
        let offset = file.len().expect("segment length");
        file.write_at(offset, bytes::Bytes::from(put_frame(id, seq)))
            .expect("write segment");
    }
    file.sync(Durability::Durable).expect("sync segment");
}

/// Rotate `history` from its open segment to the next, as the WAL actor does.
fn rotate(history: &mut WalHistory) -> SegmentId {
    super::fixture::rotate(history).expect("rotate")
}

/// A WAL whose segments 1..=4 each hold two records, `10 * id + 1` and
/// `10 * id + 2`, with 1..=3 sealed and 4 open.
fn four_segments(device: &CrashDevice) -> WalHistory {
    let mut history = open(device);
    write_records(wal(device).as_ref(), 1, [11, 12]);
    for id in 2..=4 {
        assert_eq!(rotate(&mut history), id);
        write_records(wal(device).as_ref(), id, [10 * id + 1, 10 * id + 2]);
    }
    history
}

fn members(history: &WalHistory) -> Vec<SegmentId> {
    history.ledger().members().collect()
}

/// The bytes each of `ids` holds now.
fn segment_bytes(
    device: &CrashDevice,
    ids: impl IntoIterator<Item = SegmentId>,
) -> BTreeMap<SegmentId, Vec<u8>> {
    let files = device.files();
    ids.into_iter()
        .map(|id| (id, files[&key(id)].clone()))
        .collect()
}

/// Reopen after a crash; every member must still have its file, and those
/// in `expected` their bytes.
fn reopen_with_intact_members(
    device: &CrashDevice,
    expected: &BTreeMap<SegmentId, Vec<u8>>,
) -> WalHistory {
    let history = open(device);
    let files = device.files();
    for id in history.ledger().members() {
        let held = files
            .get(&key(id))
            .unwrap_or_else(|| panic!("member {id} lost its file"));
        if let Some(bytes) = expected.get(&id) {
            assert_eq!(held, bytes, "member {id} lost bytes it held as history");
        }
    }
    history
}

fn has_salvage_copy(files: &BTreeMap<String, Vec<u8>>, id: SegmentId, bytes: &[u8]) -> bool {
    files.iter().any(|(path, held)| {
        path.contains("/salvaged-") && path.ends_with(&name(id)) && held.as_slice() == bytes
    })
}

fn header_of(device: &CrashDevice, id: SegmentId) -> SegmentHeader {
    SegmentHeader::decode(&device.files()[&key(id)]).expect("segment header")
}

/// Replay every member strictly and return the sequences applied.
fn replayed_sequences(device: &CrashDevice, history: &WalHistory) -> Vec<u64> {
    let storage = wal(device);
    let segments = replay_segments(history.ledger(), &FsPath::new(""));
    let mut memtables = std::collections::HashMap::new();
    crate::wal::recovery::streaming::replay_wal_with_options(
        storage.as_ref(),
        &segments,
        &mut memtables,
        crate::wal::recovery::ReplayPolicy::Strict,
        None,
        crate::wal::recovery::streaming::StreamingReplayLimits::local(),
        crate::wal::recovery::streaming::ReplayOptions::default(),
        &mut |_, _| Ok(()),
    )
    .expect("strict replay");
    let mut sequences: Vec<u64> = memtables
        .values()
        .flat_map(|table| table.iter_all())
        .map(|(_, _, sequence)| sequence)
        .collect();
    sequences.sort_unstable();
    sequences
}

#[test]
fn should_create_first_segment_with_ledger_when_opening_new_wal() {
    // Arrange
    let device = CrashDevice::new();

    // Act
    let history = WalHistory::open(wal(&device), 7, 3).expect("genesis");

    // Assert
    assert_eq!(members(&history), vec![7]);
    assert_eq!(history.ledger().next_segment_id(), 8);
    let header = header_of(&device, 7);
    assert_eq!((header.id, header.epoch, header.first_seq), (7, 3, 1));
    assert_eq!(header.prev, ChainLink::GENESIS);
    assert_eq!(device.files()[&key(7)].len(), HEADER_LEN);
}

#[test]
fn should_refuse_genesis_when_segment_files_exist_without_ledger() {
    // Arrange: history this ledger never recorded.
    let device = CrashDevice::new();
    let stray = wal(&device);
    stray.create_dir_all(&FsPath::new("")).expect("WAL dir");
    let (bytes, _) = crate::wal::format::segment::SegmentBuilder::new(3, 0, ChainLink::GENESIS)
        .bytes(&put_frame(3, 1))
        .seal();
    stray
        .open(
            &FsPath::new(name(3)),
            OpenOptions {
                mode: OpenMode::ReadWrite,
                create: true,
                create_new: true,
                truncate: false,
            },
        )
        .and_then(|mut file| file.write_at(0, bytes::Bytes::from(bytes)))
        .expect("write stray segment");

    // Act
    let result = WalHistory::open(wal(&device), 1, 0);

    // Assert
    assert!(matches!(result, Err(MidgeError::RecoveryFailed(_))));
}

#[test]
fn should_adopt_header_only_segment_when_genesis_crashed_before_ledger() {
    // Arrange: genesis wrote segment 1's header, then the process crashed
    // before LEDGER existed.
    let device = CrashDevice::new();
    let stray = wal(&device);
    stray.create_dir_all(&FsPath::new("")).expect("WAL dir");
    let header = SegmentHeader {
        id: 1,
        epoch: 0,
        holder: [5; 16],
        first_seq: 1,
        prev: ChainLink::GENESIS,
    };
    stray
        .open(
            &FsPath::new(name(1)),
            OpenOptions {
                mode: OpenMode::ReadWrite,
                create: true,
                create_new: true,
                truncate: false,
            },
        )
        .and_then(|mut file| file.write_at(0, bytes::Bytes::copy_from_slice(&header.encode())))
        .expect("write header-only segment");
    device.process_crash();

    // Act
    let read = WalHistory::read(&wal_view(&device));
    let history = WalHistory::open(wal(&device), 1, 2);

    // Assert: a header holds no record, so a new history starts over it.
    assert!(matches!(read, Ok(None)), "{read:?}");
    let history = history.expect("genesis over the header-only segment");
    assert_eq!(members(&history), vec![1]);
    assert_eq!(header_of(&device, 1).epoch, 2);
}

#[test]
fn should_sweep_unrecorded_next_segment_when_rotation_crashed_before_recording_it() {
    // Arrange: a rotation created segment 2, then the process crashed before
    // the ledger recorded it.
    let device = CrashDevice::new();
    let mut history = open(&device);
    write_records(wal(&device).as_ref(), 1, [1]);
    let facts = super::fixture::lenient_seal(&history).expect("seal facts");
    let _unrecorded = history
        .prepare_rotation(facts, 2)
        .expect("create next segment");
    assert!(device.files().contains_key(&key(2)));
    device.process_crash();

    // Act
    let mut reopened = reopen_with_intact_members(&device, &BTreeMap::new());

    // Assert: segment 1 is still open and the orphan is gone; the next
    // rotation creates segment 2 again.
    assert_eq!(members(&reopened), vec![1]);
    assert!(!device.files().contains_key(&key(2)));
    assert_eq!(rotate(&mut reopened), 2);
}

#[test]
fn should_refuse_creating_segment_over_unrecorded_records() {
    // Arrange: segment 2 holds records no ledger edit accounts for.
    let device = CrashDevice::new();
    let mut history = open(&device);
    let facts = super::fixture::lenient_seal(&history).expect("seal facts");
    let _unrecorded = history
        .prepare_rotation(facts.clone(), 1)
        .expect("create next segment");
    write_records(wal(&device).as_ref(), 2, [5]);

    // Act
    let created = history.prepare_rotation(facts, 1);
    let swept = history.sweep_orphans();

    // Assert
    assert!(matches!(created, Err(MidgeError::Corruption(_))));
    assert!(matches!(swept, Err(MidgeError::Corruption(_))));
}

#[test]
fn should_link_each_segment_to_the_footer_before_it_when_rotating() {
    // Arrange
    let device = CrashDevice::new();

    // Act
    let history = four_segments(&device);

    // Assert
    for id in 2..=4 {
        let sealed = history
            .ledger()
            .expected(id - 1)
            .and_then(|member| member.sealed.clone())
            .expect("the member before is sealed");
        let header = header_of(&device, id);
        assert_eq!(header.prev, sealed.link, "segment {id}");
        assert_eq!(header.first_seq, sealed.link.last_seq + 1, "segment {id}");
        assert_eq!(
            crate::wal::format::segment::footer_link(&device.files()[&key(id - 1)])
                .expect("footer"),
            Some(sealed.link)
        );
    }
    assert_eq!(
        replayed_sequences(&device, &history),
        vec![11, 12, 21, 22, 31, 32, 41, 42]
    );
}

#[test]
fn should_keep_retired_segment_out_of_history_when_its_unlink_fails() {
    // Arrange: removing segment 1 fails after the ledger retires it.
    let device = CrashDevice::new();
    let mut history = four_segments(&device);
    let kept = segment_bytes(&device, 3..=4);
    let removed_bytes = segment_bytes(&device, [2])[&2].len() as u64;
    let fault = device.inject(Fault {
        op: FaultOp::RemoveFile,
        path: key(1),
        nth: 1,
        effect: FaultEffect::Before,
        error: FaultError::Io,
    });

    // Act
    let retired = history.retire_below(3).expect("retire");
    device.process_crash();
    let reopened = reopen_with_intact_members(&device, &kept);

    // Assert
    assert!(device.fired(fault));
    assert_eq!(retired.left, vec![1]);
    assert_eq!(retired.removed, vec![(2, removed_bytes)]);
    assert_eq!(members(&reopened), vec![3, 4]);
    assert!(!device.files().contains_key(&key(1)));
}

#[test]
fn should_reopen_prefix_of_history_when_power_is_lost_after_retirement() {
    for seed in 0..POWER_LOSS_SEEDS {
        // Arrange
        let device = CrashDevice::new();
        let mut history = four_segments(&device);
        let bytes = segment_bytes(&device, 1..=4);
        history.retire_below(3).expect("retire");

        // Act
        device.power_loss(seed);
        let reopened = reopen_with_intact_members(&device, &bytes);

        // Assert: either the retirement is durable or nothing changed.
        let members = members(&reopened);
        assert!(
            members == vec![3, 4] || members == vec![1, 2, 3, 4],
            "seed {seed}: members {members:?}"
        );
    }
}

#[test]
fn should_reopen_prefix_of_history_when_power_is_lost_after_rotation() {
    for seed in 0..POWER_LOSS_SEEDS {
        // Arrange
        let device = CrashDevice::new();
        let mut history = four_segments(&device);
        let sealed = segment_bytes(&device, 1..=3);
        let next = rotate(&mut history);

        // Act
        device.power_loss(seed);
        let reopened = reopen_with_intact_members(&device, &sealed);

        // Assert: the rotation is durable or absent, never half applied.
        let members = members(&reopened);
        assert!(
            members == vec![1, 2, 3, 4, next] || members == vec![1, 2, 3, 4],
            "seed {seed}: members {members:?}"
        );
    }
}

#[test]
fn should_reopen_with_sealed_segment_when_power_is_lost_after_footer_before_rotate() {
    for seed in 0..POWER_LOSS_SEEDS {
        // Arrange: segment 4 got its footer, synced, and the process lost
        // power before the ledger recorded the rotation.
        let device = CrashDevice::new();
        let mut history = four_segments(&device);
        let facts = super::fixture::seal_open_for_test(&mut history).expect("footer");
        device.power_loss(seed);

        // Act
        let mut reopened = open(&device);
        let sealed = reopened.start_epoch(1, 100).expect("start the next epoch");

        // Assert: the footer counts as the seal; nothing is lost.
        assert_eq!(
            sealed.map(|sealed| sealed.facts),
            Some(facts.clone()),
            "seed {seed}"
        );
        assert_eq!(members(&reopened), vec![1, 2, 3, 4, 5], "seed {seed}");
        assert_eq!(
            replayed_sequences(&device, &reopened),
            vec![11, 12, 21, 22, 31, 32, 41, 42],
            "seed {seed}"
        );
    }
}

#[test]
fn should_record_footered_open_member_sealed_when_rotation_crashed_before_ledger() {
    // Arrange: the open segment ends in a footer the ledger never recorded.
    let device = CrashDevice::new();
    let mut history = four_segments(&device);
    let facts = super::fixture::seal_open_for_test(&mut history).expect("footer");
    let before = segment_bytes(&device, [4]);
    device.process_crash();
    let mut reopened = open(&device);

    // Act
    let sealed = reopened.start_epoch(0, 100).expect("start the epoch");

    // Assert: the footer is the seal, its bytes untouched.
    assert_eq!(sealed.map(|sealed| sealed.facts), Some(facts.clone()));
    assert_eq!(segment_bytes(&device, [4]), before);
    let member = reopened.ledger().expected(4).expect("member 4").clone();
    assert_eq!(member.sealed, Some(facts));
    assert_eq!(reopened.ledger().open_segment(), 5);
}

#[test]
fn should_seal_recovered_tail_durably_when_starting_an_epoch() {
    for seed in 0..POWER_LOSS_SEEDS {
        // Arrange: segment 4's last record was written but never synced.
        let device = CrashDevice::new();
        let mut history = four_segments(&device);
        let wal = wal(&device);
        let mut file = wal
            .open(
                &FsPath::new(name(4)),
                OpenOptions {
                    mode: OpenMode::ReadWrite,
                    create: false,
                    create_new: false,
                    truncate: false,
                },
            )
            .expect("open segment 4");
        let offset = file.len().expect("length");
        file.write_at(offset, bytes::Bytes::from(put_frame(4, 43)))
            .expect("buffered write");
        drop(file);

        // Act: the next epoch starts, then power is lost.
        history.start_epoch(1, 100).expect("start the epoch");
        device.power_loss(seed);
        let reopened = open(&device);

        // Assert: the tail the epoch sealed survived.
        assert_eq!(
            replayed_sequences(&device, &reopened),
            vec![11, 12, 21, 22, 31, 32, 41, 42, 43],
            "seed {seed}"
        );
    }
}

/// `len` bytes WAL compression cannot shrink.
fn incompressible(len: usize) -> Vec<u8> {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

#[test]
fn should_keep_recovered_tail_when_power_is_lost_after_its_footer_append() {
    for seed in 0..POWER_LOSS_SEEDS {
        // Arrange: segment 4 ends in a record that spans pages and was never
        // synced; the epoch start appends the footer, then power is lost
        // before anything more reaches the device.
        let device = CrashDevice::new();
        let mut history = four_segments(&device);
        let wal = wal(&device);
        let record = crate::wal::WalRecord::new(
            crate::wal::WalOpKind::Put,
            bytes::Bytes::from_static(b"segment-4-large"),
            Some(bytes::Bytes::from(incompressible(3 * 4096))),
            43,
        );
        let payload = crate::wal::encoding::encode(&record).expect("encode record");
        let mut frame = Vec::new();
        crate::wal::frame::append_frame(&mut frame, &payload).expect("frame record");
        let mut file = wal
            .open(
                &FsPath::new(name(4)),
                OpenOptions {
                    mode: OpenMode::ReadWrite,
                    create: false,
                    create_new: false,
                    truncate: false,
                },
            )
            .expect("open segment 4");
        let offset = file.len().expect("length");
        file.write_at(offset, bytes::Bytes::from(frame))
            .expect("buffered write");
        drop(file);
        let footer_write = device.inject(Fault {
            op: FaultOp::Write,
            path: key(4),
            nth: 1,
            effect: FaultEffect::After,
            error: FaultError::Io,
        });

        // Act
        let interrupted = history.start_epoch(1, 100);
        device.disarm_faults();
        device.power_loss(seed);
        let reopened = open(&device);

        // Assert: whatever the device kept, the segment replays strictly,
        // and the record the epoch start read is never lost behind a kept
        // footer.
        assert!(interrupted.is_err(), "seed {seed}");
        assert!(device.fired(footer_write), "seed {seed}");
        let sequences = replayed_sequences(&device, &reopened);
        assert!(
            sequences.starts_with(&[11, 12, 21, 22, 31, 32, 41, 42]),
            "seed {seed}: {sequences:?}"
        );
    }
}

#[test]
fn should_not_seal_empty_open_segment_when_reopening() {
    // Arrange: the open segment holds no record, written by epoch 0.
    let device = CrashDevice::new();
    let mut history = four_segments(&device);
    rotate(&mut history);
    let replaced = history.ledger().open_member().open;

    // Act
    let kept = history.start_epoch(0, 100).expect("same epoch");
    let reopened = history.start_epoch(1, 200).expect("new epoch");

    // Assert: the same epoch keeps it; a new one replaces it, unsealed.
    assert_eq!((kept, reopened), (None, None));
    assert_eq!(members(&history), vec![1, 2, 3, 4, 6]);
    let open = history.ledger().open_member().open;
    assert_eq!(open.prev, replaced.prev);
    assert_eq!((open.epoch, open.first_seq), (1, 200));
    assert!(history.ledger().expected(5).is_none());
    assert!(!device.files().contains_key(&key(5)));
}

#[test]
fn should_copy_dropped_members_aside_when_salvage_sets_history_aside() {
    // Arrange
    let device = CrashDevice::new();
    let mut history = four_segments(&device);
    let dropped = segment_bytes(&device, 3..=4);

    // Act
    history
        .set_aside(SetAside::From(3), 9)
        .expect("set aside from 3");

    // Assert: 3 and 4 left history, their bytes are kept aside, and history
    // continues in a new segment at the requested floor, linked to 2.
    assert_eq!(members(&history), vec![1, 2, 9]);
    let files = device.files();
    for (id, bytes) in dropped {
        assert!(has_salvage_copy(&files, id, &bytes), "no copy of {id}");
        assert!(!files.contains_key(&key(id)));
    }
    let two = history
        .ledger()
        .expected(2)
        .and_then(|member| member.sealed.clone())
        .expect("2 is sealed");
    assert_eq!(header_of(&device, 9).prev, two.link);
}

#[test]
fn should_link_new_open_to_cut_segment_after_salvage() {
    // Arrange: replay applied only the first record of segment 2.
    let device = CrashDevice::new();
    let mut history = four_segments(&device);
    let original = segment_bytes(&device, [2])[&2].clone();
    let valid_bytes = (HEADER_LEN + put_frame(2, 21).len()) as u64;

    // Act
    history
        .set_aside(
            SetAside::After(SalvageCut {
                segment: 2,
                valid_bytes,
            }),
            0,
        )
        .expect("set aside after segment 2");

    // Assert: segment 2 is resealed over its prefix, and the new open
    // segment links that seal; a strict replay accepts the chain.
    assert_eq!(members(&history), vec![1, 2, 5]);
    let cut = history
        .ledger()
        .expected(2)
        .and_then(|member| member.sealed.clone())
        .expect("2 is sealed again");
    assert_eq!(cut.record_count, 1);
    assert_eq!(cut.data_len, valid_bytes);
    assert_eq!(header_of(&device, 5).prev, cut.link);
    let files = device.files();
    let prefix = usize::try_from(valid_bytes).expect("small");
    assert_eq!(&files[&key(2)][..prefix], &original[..prefix]);
    assert!(has_salvage_copy(&files, 2, &original));
    assert_eq!(replayed_sequences(&device, &history), vec![11, 12, 21]);
}

#[test]
fn should_reopen_without_losing_kept_history_when_power_is_lost_during_salvage() {
    for seed in 0..POWER_LOSS_SEEDS {
        // Arrange
        let device = CrashDevice::new();
        let mut history = four_segments(&device);
        let bytes = segment_bytes(&device, 1..=4);
        let valid_bytes = (HEADER_LEN + put_frame(2, 21).len()) as u64;
        history
            .set_aside(
                SetAside::After(SalvageCut {
                    segment: 2,
                    valid_bytes,
                }),
                0,
            )
            .expect("set aside after segment 2");

        // Act
        device.power_loss(seed);
        let reopened = open(&device);

        // Assert: the set-aside is durable or absent, and no byte salvage
        // dropped is lost: each segment is still in place or kept aside.
        let members = members(&reopened);
        let files = device.files();
        assert!(
            members == vec![1, 2, 5] || members == vec![1, 2, 3, 4],
            "seed {seed}: members {members:?}"
        );
        assert_eq!(files[&key(1)], bytes[&1], "seed {seed}");
        for (id, held) in bytes.iter().filter(|(id, _)| **id >= 2) {
            let in_place = files.get(&key(*id)) == Some(held);
            assert!(
                in_place || has_salvage_copy(&files, *id, held),
                "seed {seed}: segment {id} lost"
            );
        }
        if members == vec![1, 2, 5] {
            assert!(has_salvage_copy(&files, 2, &bytes[&2]), "seed {seed}");
        }
    }
}

#[test]
fn should_set_aside_again_when_power_was_lost_after_members_moved() {
    // Arrange: the dropped members moved aside, but the ledger never
    // recorded the set-aside, so it still names them.
    let device = CrashDevice::new();
    let mut history = four_segments(&device);
    let dropped = segment_bytes(&device, 3..=4);
    let fault = device.inject(Fault {
        op: FaultOp::Write,
        path: format!(
            "{}/{}",
            crate::wal::WAL_DIR,
            crate::wal::ledger_store::LEDGER_FILE
        ),
        nth: 1,
        effect: FaultEffect::Before,
        error: FaultError::Io,
    });
    let interrupted = history.set_aside(SetAside::From(3), 0);
    device.process_crash();
    device.disarm_faults();
    let mut reopened = open(&device);

    // Act: the next salvage stops at the first missing member.
    reopened
        .set_aside(SetAside::From(3), 0)
        .expect("set aside again");

    // Assert
    assert!(device.fired(fault));
    assert!(interrupted.is_err());
    assert_eq!(members(&reopened), vec![1, 2, 5]);
    let files = device.files();
    for (id, bytes) in dropped {
        assert!(has_salvage_copy(&files, id, &bytes), "segment {id} lost");
    }
}

#[test]
fn should_retire_after_healing_when_an_earlier_ledger_write_failed() {
    // Arrange: the ledger's next append fails, as on a full disk.
    let device = CrashDevice::new();
    let mut history = four_segments(&device);
    let first = segment_bytes(&device, [1])[&1].len() as u64;
    let fault = device.inject(Fault {
        op: FaultOp::Write,
        path: format!(
            "{}/{}",
            crate::wal::WAL_DIR,
            crate::wal::ledger_store::LEDGER_FILE
        ),
        nth: 1,
        effect: FaultEffect::Before,
        error: FaultError::Io,
    });
    let failed = history.retire_below(3);
    let kept = device.files();

    // Act
    let retried = history.retire_below(2);

    // Assert: the failed retirement removed nothing, and the next one heals
    // the ledger and goes ahead.
    assert!(device.fired(fault));
    assert!(failed.is_err());
    for id in 1..=4 {
        assert!(kept.contains_key(&key(id)), "segment {id} removed");
    }
    assert_eq!(
        retried.expect("retire after healing").removed,
        vec![(1, first)]
    );
    assert_eq!(members(&history), vec![2, 3, 4]);
}

#[test]
fn should_not_record_set_aside_when_the_cut_fails() {
    // Arrange: cutting segment 2 back to its replayed prefix fails.
    let device = CrashDevice::new();
    let mut history = four_segments(&device);
    let fault = device.inject(Fault {
        op: FaultOp::Write,
        path: key(2),
        nth: 1,
        effect: FaultEffect::Before,
        error: FaultError::Io,
    });

    // Act
    let result = history.set_aside(
        SetAside::After(SalvageCut {
            segment: 2,
            valid_bytes: (HEADER_LEN + put_frame(2, 21).len()) as u64,
        }),
        0,
    );

    // Assert: the ledger never records a set-aside whose cut segment still
    // holds the bytes past its replayed prefix, which strict replay rejects.
    assert!(device.fired(fault));
    assert!(result.is_err());
    assert_eq!(members(&history), vec![1, 2, 3, 4]);
}

#[test]
fn should_link_catalog_head_when_rebasing() {
    // Arrange: another writer published through segment 7.
    let device = CrashDevice::new();
    let mut history = four_segments(&device);
    let kept = segment_bytes(&device, 1..=4);
    let head = ChainLink {
        id: 7,
        last_seq: 700,
        digest: [7; 16],
    };

    // Act
    history.rebase(head, 8).expect("rebase");

    // Assert
    assert_eq!(members(&history), vec![8]);
    assert_eq!(history.ledger().anchor(), head);
    let header = header_of(&device, 8);
    assert_eq!((header.prev, header.first_seq), (head, 701));
    let files = device.files();
    for (id, bytes) in kept {
        assert!(has_salvage_copy(&files, id, &bytes), "segment {id} lost");
        assert!(!files.contains_key(&key(id)));
    }
}
