//! The segment ledger adapter: genesis, rotation files, retirement and salvage.
//!
//! Crash cases run on a simulated device: after every power-loss outcome the
//! WAL must reopen, and every member the ledger names must still hold the
//! bytes it held when it became history.

use super::{replay_segments, SalvageCut, SetAside, WalHistory};
use crate::common::MidgeError;
use crate::io::{Durability, Fs, FsPath, OpenMode, OpenOptions};
use crate::simulation::crash_fs::{CrashDevice, Fault, FaultEffect, FaultError, FaultOp};
use crate::wal::ledger::SegmentId;
use std::collections::BTreeMap;
use std::sync::Arc;

const POWER_LOSS_SEEDS: u64 = 64;

fn wal(device: &CrashDevice) -> Arc<dyn Fs> {
    crate::io::subdirectory(Arc::new(device.fs()), crate::wal::WAL_DIR)
}

fn name(id: SegmentId) -> String {
    crate::wal::segment_file_name(id)
}

fn key(id: SegmentId) -> String {
    format!("{}/{}", crate::wal::WAL_DIR, name(id))
}

/// Durably append `bytes` to segment `id`, as a synced writer would.
fn write_segment(wal: &dyn Fs, id: SegmentId, bytes: &[u8]) {
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
    let offset = file.len().expect("segment length");
    file.write_at(offset, bytes::Bytes::copy_from_slice(bytes))
        .expect("write segment");
    file.sync(Durability::Durable).expect("sync segment");
}

/// Rotate `history` from its open segment to the next, as the WAL actor does.
fn rotate(history: &mut WalHistory) -> SegmentId {
    super::fixture::rotate(history).expect("rotate")
}

/// A WAL whose segments 1..=4 hold `segment-N`, with 4 open.
fn four_segments(device: &CrashDevice) -> WalHistory {
    let mut history = WalHistory::open(wal(device), 1).expect("genesis");
    write_segment(wal(device).as_ref(), 1, b"segment-1");
    for id in 2..=4 {
        assert_eq!(rotate(&mut history), id);
        write_segment(wal(device).as_ref(), id, format!("segment-{id}").as_bytes());
    }
    history
}

fn members(history: &WalHistory) -> Vec<SegmentId> {
    history.ledger().members().collect()
}

fn segment_bytes(ids: impl IntoIterator<Item = SegmentId>) -> BTreeMap<SegmentId, Vec<u8>> {
    ids.into_iter()
        .map(|id| (id, format!("segment-{id}").into_bytes()))
        .collect()
}

/// Reopen after a crash; every member must still have its file, and those
/// in `expected` their bytes.
fn reopen_with_intact_members(
    device: &CrashDevice,
    expected: &BTreeMap<SegmentId, Vec<u8>>,
) -> WalHistory {
    let history = WalHistory::open(wal(device), 1).expect("reopen after the crash");
    history.sweep_orphans().expect("sweep after the crash");
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

#[test]
fn should_create_first_segment_with_ledger_when_opening_new_wal() {
    // Arrange
    let device = CrashDevice::new();

    // Act
    let history = WalHistory::open(wal(&device), 7).expect("genesis");

    // Assert
    assert_eq!(members(&history), vec![7]);
    assert_eq!(history.ledger().next_segment_id(), 8);
    assert!(device.files().contains_key(&key(7)));
}

#[test]
fn should_refuse_genesis_when_segment_files_exist_without_ledger() {
    // Arrange: history this ledger never recorded.
    let device = CrashDevice::new();
    let stray = wal(&device);
    stray.create_dir_all(&FsPath::new("")).expect("WAL dir");
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
        .expect("create stray segment");
    write_segment(stray.as_ref(), 3, b"unrecorded");

    // Act
    let result = WalHistory::open(wal(&device), 1);

    // Assert
    assert!(matches!(result, Err(MidgeError::RecoveryFailed(_))));
}

#[test]
fn should_adopt_empty_next_segment_when_rotation_crashed_before_recording_it() {
    // Arrange: a rotation created segment 2, then the process crashed before
    // the ledger recorded it.
    let device = CrashDevice::new();
    let mut history = WalHistory::open(wal(&device), 1).expect("genesis");
    let _unrecorded = history.prepare_rotation().expect("create next segment");
    device.process_crash();

    // Act
    let mut reopened = reopen_with_intact_members(&device, &BTreeMap::new());
    let opened = rotate(&mut reopened);

    // Assert
    assert_eq!(opened, 2);
    assert_eq!(members(&reopened), vec![1, 2]);
}

#[test]
fn should_refuse_creating_segment_over_unrecorded_data() {
    // Arrange: segment 2 holds bytes no ledger edit accounts for.
    let device = CrashDevice::new();
    let mut history = WalHistory::open(wal(&device), 1).expect("genesis");
    let _unrecorded = history.prepare_rotation().expect("create next segment");
    write_segment(wal(&device).as_ref(), 2, b"unrecorded");

    // Act
    let created = history.prepare_rotation();
    let swept = history.sweep_orphans();

    // Assert
    assert!(matches!(created, Err(MidgeError::Corruption(_))));
    assert!(matches!(swept, Err(MidgeError::Corruption(_))));
}

#[test]
fn should_replay_members_in_order_with_open_segment_last() {
    // Arrange
    let device = CrashDevice::new();
    let history = four_segments(&device);

    // Act
    let segments = replay_segments(history.ledger(), &FsPath::new(crate::wal::WAL_DIR));

    // Assert
    assert_eq!(
        format!("{segments:?}"),
        format!(
            "{:?}",
            crate::wal::recovery::ReplaySegments::new(
                [1, 2, 3].map(crate::wal::segment_key),
                Some(crate::wal::segment_key(4)),
            )
        )
    );
}

#[test]
fn should_keep_retired_segment_out_of_history_when_its_unlink_fails() {
    // Arrange: removing segment 1 fails after the ledger retires it.
    let device = CrashDevice::new();
    let mut history = four_segments(&device);
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
    let reopened = reopen_with_intact_members(&device, &segment_bytes(3..=4));

    // Assert
    assert!(device.fired(fault));
    assert_eq!(retired, vec![1, 2]);
    assert_eq!(members(&reopened), vec![3, 4]);
    assert!(!device.files().contains_key(&key(1)));
}

#[test]
fn should_reopen_prefix_of_history_when_power_is_lost_after_retirement() {
    for seed in 0..POWER_LOSS_SEEDS {
        // Arrange
        let device = CrashDevice::new();
        let mut history = four_segments(&device);
        history.retire_below(3).expect("retire");

        // Act
        device.power_loss(seed);
        let reopened = reopen_with_intact_members(&device, &segment_bytes(1..=4));

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
        let next = rotate(&mut history);

        // Act
        device.power_loss(seed);
        let reopened = reopen_with_intact_members(&device, &segment_bytes(1..=4));

        // Assert: the rotation is durable or absent, never half applied.
        let members = members(&reopened);
        assert!(
            members == vec![1, 2, 3, 4, next] || members == vec![1, 2, 3, 4],
            "seed {seed}: members {members:?}"
        );
    }
}

#[test]
fn should_copy_dropped_members_aside_when_salvage_sets_history_aside() {
    // Arrange
    let device = CrashDevice::new();
    let mut history = four_segments(&device);

    // Act
    history
        .set_aside(SetAside::From(3), 9)
        .expect("set aside from 3");

    // Assert: 3 and 4 left history, their bytes are kept aside, and history
    // continues in a new segment at the requested floor.
    assert_eq!(members(&history), vec![1, 2, 9]);
    let files = device.files();
    for (id, bytes) in segment_bytes(3..=4) {
        assert!(has_salvage_copy(&files, id, &bytes), "no copy of {id}");
        assert!(!files.contains_key(&key(id)));
    }
}

#[test]
fn should_cut_segment_back_to_replayed_prefix_when_salvage_stops_inside_it() {
    // Arrange: replay applied the first 7 bytes of segment 2.
    let device = CrashDevice::new();
    let mut history = four_segments(&device);

    // Act
    history
        .set_aside(
            SetAside::After(SalvageCut {
                segment: 2,
                valid_bytes: 7,
            }),
            0,
        )
        .expect("set aside after segment 2");

    // Assert
    assert_eq!(members(&history), vec![1, 2, 5]);
    let files = device.files();
    assert_eq!(files[&key(2)], b"segment".to_vec());
    assert!(has_salvage_copy(&files, 2, b"segment-2"));
}

#[test]
fn should_reopen_without_losing_kept_history_when_power_is_lost_during_salvage() {
    for seed in 0..POWER_LOSS_SEEDS {
        // Arrange
        let device = CrashDevice::new();
        let mut history = four_segments(&device);
        history
            .set_aside(
                SetAside::After(SalvageCut {
                    segment: 2,
                    valid_bytes: 7,
                }),
                0,
            )
            .expect("set aside after segment 2");

        // Act
        device.power_loss(seed);
        let reopened = reopen_with_intact_members(&device, &segment_bytes(1..=1));

        // Assert: the set-aside is durable or absent. A durable one kept its
        // salvage copies; an absent one kept every member whole.
        let members = members(&reopened);
        let files = device.files();
        if members == vec![1, 2, 5] {
            for (id, bytes) in segment_bytes(2..=4) {
                assert!(has_salvage_copy(&files, id, &bytes), "seed {seed}: {id}");
            }
        } else {
            assert_eq!(members, vec![1, 2, 3, 4], "seed {seed}");
            for (id, bytes) in segment_bytes(2..=4) {
                assert_eq!(files[&key(id)], bytes, "seed {seed}: member {id} changed");
            }
        }
    }
}

#[test]
fn should_retire_after_healing_when_an_earlier_ledger_write_failed() {
    // Arrange: the ledger's next append fails, as on a full disk.
    let device = CrashDevice::new();
    let mut history = four_segments(&device);
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
    assert_eq!(retried.expect("retire after healing"), vec![1]);
    assert_eq!(members(&history), vec![2, 3, 4]);
}
