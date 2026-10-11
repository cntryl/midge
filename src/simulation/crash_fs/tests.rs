//! The crash model against the probes that disqualified the earlier draft
//! (#810 review). Each test enumerates every legal post-crash state rather
//! than asserting one happy outcome.

use super::{CrashDevice, CrashFs, Decision, Fault, FaultEffect, FaultError, FaultOp, PAGE_BYTES};
use crate::io::{Durability, Fs, FsError, FsPath, OpenMode, OpenOptions};
use bytes::Bytes;
use std::collections::BTreeSet;

const CREATE: OpenOptions = OpenOptions {
    mode: OpenMode::ReadWrite,
    create: true,
    create_new: false,
    truncate: false,
};

const READ_ONLY: OpenOptions = OpenOptions {
    mode: OpenMode::ReadOnly,
    create: false,
    create_new: false,
    truncate: false,
};

fn path(value: &str) -> FsPath {
    FsPath::new(value)
}

/// Write `bytes` to `name`, then make its data and its entry durable.
fn durable_file(fs: &CrashFs, name: &str, bytes: &[u8]) {
    let mut file = fs.open(&path(name), CREATE).expect("create file");
    file.write_at(0, Bytes::copy_from_slice(bytes))
        .expect("write file");
    file.sync(Durability::Durable).expect("sync file");
    let parent = name.rsplit_once('/').map_or("", |(parent, _)| parent);
    fs.sync_dir(&path(parent), Durability::Durable)
        .expect("sync parent");
}

fn read(fs: &CrashFs, name: &str) -> Option<Vec<u8>> {
    let file = fs.open(&path(name), READ_ONLY).ok()?;
    let len = file.len().expect("file length");
    Some(file.read_at(0, len).expect("read file").to_vec())
}

fn names(fs: &CrashFs, directory: &str) -> BTreeSet<String> {
    fs.list_dir(&path(directory))
        .map(|entries| entries.into_iter().map(|entry| entry.name).collect())
        .unwrap_or_default()
}

/// Every distinct state `observe` sees across every power-loss outcome.
fn outcomes<T: Ord>(device: &CrashDevice, observe: impl Fn(&CrashFs) -> T) -> BTreeSet<T> {
    let decisions = device.decisions();
    assert!(decisions.len() <= 16, "too many decisions: {decisions:?}");
    (0..1_u32 << decisions.len())
        .map(|mask| {
            let fork = device.fork();
            fork.power_loss_with(|decision: &Decision| {
                let index = decisions
                    .iter()
                    .position(|known| known == decision)
                    .expect("decision is enumerated");
                mask & (1 << index) != 0
            });
            observe(&fork.fs())
        })
        .collect()
}

#[test]
fn should_persist_unsynced_pages_in_any_order_when_power_is_lost() {
    // Arrange: a durable empty file gets two unsynced pages.
    let device = CrashDevice::new();
    let fs = device.fs();
    durable_file(&fs, "segment", b"");
    let mut file = fs.open(&path("segment"), CREATE).expect("open file");
    file.write_at(0, Bytes::from(vec![b'a'; PAGE_BYTES]))
        .expect("first page");
    file.write_at(PAGE_BYTES as u64, Bytes::from(vec![b'b'; PAGE_BYTES]))
        .expect("second page");
    drop(file);

    // Act
    let pages = outcomes(&device, |fs| {
        read(fs, "segment")
            .expect("file survives")
            .chunks(PAGE_BYTES)
            .map(|page| page[0])
            .collect::<Vec<_>>()
    });

    // Assert: either page may persist without the other, and a persisted
    // length may expose zero-filled pages.
    assert_eq!(
        pages,
        BTreeSet::from([
            vec![],
            vec![0, 0],
            vec![0, b'b'],
            vec![b'a', 0],
            vec![b'a', b'b'],
        ])
    );
}

#[test]
fn should_settle_each_unsynced_entry_independently_when_power_is_lost() {
    // Arrange
    let device = CrashDevice::new();
    let fs = device.fs();
    durable_file(&fs, "old", b"old");
    fs.remove_file(&path("old")).expect("unlink old");
    durable_file_data_only(&fs, "new");

    // Act
    let listings = outcomes(&device, |fs| names(fs, ""));

    // Assert
    let set = |items: &[&str]| {
        items
            .iter()
            .map(ToString::to_string)
            .collect::<BTreeSet<_>>()
    };
    assert_eq!(
        listings,
        BTreeSet::from([set(&[]), set(&["old"]), set(&["new"]), set(&["new", "old"])])
    );
}

/// Create `name` with synced data but an unsynced entry.
fn durable_file_data_only(fs: &CrashFs, name: &str) {
    let mut file = fs.open(&path(name), CREATE).expect("create file");
    file.write_at(0, Bytes::from_static(b"new"))
        .expect("write file");
    file.sync(Durability::Durable).expect("sync file");
}

#[test]
fn should_move_renamed_file_atomically_when_power_is_lost() {
    // Arrange
    let device = CrashDevice::new();
    let fs = device.fs();
    fs.create_dir_all(&path("a")).expect("create a");
    fs.create_dir_all(&path("b")).expect("create b");
    durable_file(&fs, "a/seg", b"segment");
    fs.rename_atomic(&path("a/seg"), &path("b/seg"))
        .expect("rename");

    // Act
    let places = outcomes(&device, |fs| (read(fs, "a/seg"), read(fs, "b/seg")));

    // Assert: never in both directories and never in neither.
    assert_eq!(
        places,
        BTreeSet::from([
            (Some(b"segment".to_vec()), None),
            (None, Some(b"segment".to_vec())),
        ])
    );
}

#[test]
fn should_commit_both_sides_when_syncing_one_directory_of_a_rename() {
    // Arrange
    let device = CrashDevice::new();
    let fs = device.fs();
    fs.create_dir_all(&path("a")).expect("create a");
    fs.create_dir_all(&path("b")).expect("create b");
    durable_file(&fs, "a/seg", b"segment");
    fs.rename_atomic(&path("a/seg"), &path("b/seg"))
        .expect("rename");

    // Act
    fs.sync_dir(&path("b"), Durability::Durable)
        .expect("sync destination");
    let places = outcomes(&device, |fs| (read(fs, "a/seg"), read(fs, "b/seg")));

    // Assert
    assert_eq!(places, BTreeSet::from([(None, Some(b"segment".to_vec()))]));
}

#[test]
fn should_leave_partial_tree_when_power_is_lost_after_directory_removal() {
    // Arrange
    let device = CrashDevice::new();
    let fs = device.fs();
    fs.create_dir_all(&path("staging")).expect("create staging");
    durable_file(&fs, "staging/x", b"x");
    durable_file(&fs, "staging/y", b"y");

    // Act
    fs.remove_dir_all(&path("staging")).expect("remove staging");
    let trees = outcomes(&device, |fs| {
        fs.exists(&path("staging"))
            .expect("probe staging")
            .then(|| names(fs, "staging"))
    });

    // Assert: deletion is per entry, so any subset of the tree can remain.
    let set = |items: &[&str]| {
        items
            .iter()
            .map(ToString::to_string)
            .collect::<BTreeSet<_>>()
    };
    assert_eq!(
        trees,
        BTreeSet::from([
            None,
            Some(set(&[])),
            Some(set(&["x"])),
            Some(set(&["y"])),
            Some(set(&["x", "y"])),
        ])
    );
}

#[test]
fn should_report_success_over_lost_data_when_an_earlier_sync_failed() {
    // Arrange: the first sync fails and drops the dirty page, as a kernel
    // does after a writeback error.
    let device = CrashDevice::new();
    let fs = device.fs();
    durable_file(&fs, "wal", b"");
    let fault = device.inject(Fault {
        op: FaultOp::Sync,
        path: "wal".into(),
        nth: 1,
        effect: FaultEffect::Before,
        error: FaultError::Io,
    });
    let mut file = fs.open(&path("wal"), CREATE).expect("open file");
    file.write_at(0, Bytes::from(vec![b'r'; PAGE_BYTES]))
        .expect("write record");

    // Act
    let failed = file.sync(Durability::Durable);
    let retried = file.sync(Durability::Durable);
    let visible = file.read_at(0, 1).expect("read live page");
    drop(file);
    let durable = outcomes(&device, |fs| read(fs, "wal"));

    // Assert
    assert!(device.fired(fault));
    assert!(matches!(failed, Err(FsError::Io(_))), "{failed:?}");
    assert!(retried.is_ok(), "the retried sync reports success");
    assert_eq!(
        visible.as_ref(),
        b"r",
        "the page cache still shows the record"
    );
    assert_eq!(durable, BTreeSet::from([Some(vec![0; PAGE_BYTES])]));
}

#[test]
fn should_not_apply_write_when_fault_precedes_effect() {
    // Arrange
    let device = CrashDevice::new();
    let fs = device.fs();
    durable_file(&fs, "file", b"before");
    let fault = device.inject(Fault {
        op: FaultOp::Write,
        path: "file".into(),
        nth: 1,
        effect: FaultEffect::Before,
        error: FaultError::NoSpace,
    });
    let mut file = fs.open(&path("file"), CREATE).expect("open file");

    // Act
    let result = file.write_at(0, Bytes::from_static(b"after!"));
    drop(file);

    // Assert
    assert!(device.fired(fault));
    assert!(matches!(result, Err(FsError::NoSpace(_))), "{result:?}");
    assert_eq!(read(&fs, "file"), Some(b"before".to_vec()));
}

#[test]
fn should_apply_rename_when_fault_follows_effect() {
    // Arrange
    let device = CrashDevice::new();
    let fs = device.fs();
    durable_file(&fs, "tmp", b"data");
    let fault = device.inject(Fault {
        op: FaultOp::Rename,
        path: "tmp".into(),
        nth: 1,
        effect: FaultEffect::After,
        error: FaultError::Io,
    });

    // Act
    let result = fs.rename_atomic(&path("tmp"), &path("final"));

    // Assert
    assert!(device.fired(fault));
    assert!(matches!(result, Err(FsError::Io(_))), "{result:?}");
    assert_eq!(read(&fs, "final"), Some(b"data".to_vec()));
    assert_eq!(read(&fs, "tmp"), None);
}

#[test]
fn should_fence_crashed_process_when_it_acts_after_crash() {
    // Arrange
    let device = CrashDevice::new();
    let crashed = device.fs();
    durable_file(&crashed, "segment", b"data");
    let handle = crashed
        .open_persistent_handle(&path("segment"), CREATE)
        .expect("open handle");

    // Act
    device.process_crash();
    let path_operation = crashed.remove_file(&path("segment"));
    let handle_operation = handle.read_at(0, 4);

    // Assert
    assert!(path_operation.is_err());
    assert!(handle_operation.is_err());
    assert_eq!(read(&device.fs(), "segment"), Some(b"data".to_vec()));
}

#[test]
fn should_keep_page_cache_when_process_crashes_before_power_loss() {
    // Arrange: an unsynced write, then a process crash.
    let device = CrashDevice::new();
    let fs = device.fs();
    durable_file(&fs, "journal", b"");
    let mut file = fs.open(&path("journal"), CREATE).expect("open file");
    file.write_at(0, Bytes::from_static(b"edit"))
        .expect("write edit");
    drop(file);

    // Act
    device.process_crash();
    let after_process_crash = read(&device.fs(), "journal");
    let after_power_loss = outcomes(&device, |fs| read(fs, "journal"));

    // Assert: the restarted process sees the write, but it was never made
    // durable, so a later power loss may drop it.
    assert_eq!(after_process_crash, Some(b"edit".to_vec()));
    assert!(after_power_loss.contains(&Some(Vec::new())));
    assert!(after_power_loss.contains(&Some(b"edit".to_vec())));
}

#[test]
fn should_keep_durable_state_when_power_is_lost() {
    // Arrange
    let device = CrashDevice::new();
    let fs = device.fs();
    fs.create_dir_all(&path("wal")).expect("create wal");
    durable_file(&fs, "wal/1.wal", b"synced");

    // Act
    let decisions = device.decisions();
    let survivors = outcomes(&device, |fs| read(fs, "wal/1.wal"));

    // Assert: created directories and synced files leave nothing to decide.
    assert_eq!(decisions, Vec::new());
    assert_eq!(survivors, BTreeSet::from([Some(b"synced".to_vec())]));
}

#[test]
fn should_keep_choice_when_unrelated_file_is_added() {
    // Arrange: the same unsynced file, with and without an earlier unrelated
    // file that also has unsynced pages.
    let build = |unrelated: bool| {
        let device = CrashDevice::new();
        let fs = device.fs();
        if unrelated {
            let mut other = fs.open(&path("aaa-unrelated"), CREATE).expect("create");
            other
                .write_at(0, Bytes::from(vec![1; 3 * PAGE_BYTES]))
                .expect("write unrelated");
        }
        durable_file(&fs, "segment", b"");
        let mut file = fs.open(&path("segment"), CREATE).expect("open file");
        file.write_at(0, Bytes::from(vec![7; 4 * PAGE_BYTES]))
            .expect("write segment");
        device
    };

    for seed in 0..32 {
        // Act
        let alone = build(false);
        let with_unrelated = build(true);
        alone.power_loss(seed);
        with_unrelated.power_loss(seed);

        // Assert
        assert_eq!(
            read(&alone.fs(), "segment"),
            read(&with_unrelated.fs(), "segment"),
            "seed {seed}"
        );
    }
}

#[test]
fn should_keep_unlinked_open_file_readable_until_its_handle_closes() {
    // Arrange
    let device = CrashDevice::new();
    let fs = device.fs();
    durable_file(&fs, "pinned", b"pinned");
    let handle = fs
        .open_persistent_handle(&path("pinned"), READ_ONLY)
        .expect("pin file");

    // Act
    fs.remove_file(&path("pinned")).expect("unlink");
    let pinned = handle.read_at(0, 6).expect("read pinned file");

    // Assert
    assert_eq!(pinned.as_ref(), b"pinned");
    assert!(!fs.exists(&path("pinned")).expect("probe name"));
}
