//! The crash model against the probes that disqualified the earlier draft
//! and its first revision (#810, #817 reviews). Each test enumerates every
//! state a power loss can leave rather than asserting one happy outcome.

use super::{CrashDevice, CrashFs, Fault, FaultEffect, FaultError, FaultOp, PAGE_BYTES};
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

fn parent_of(name: &str) -> &str {
    name.rsplit_once('/').map_or("", |(parent, _)| parent)
}

/// Write `bytes` to `name`, then make its data and its entry durable.
fn durable_file(fs: &CrashFs, name: &str, bytes: &[u8]) {
    synced_data_file(fs, name, bytes);
    fs.sync_dir(&path(parent_of(name)), Durability::Durable)
        .expect("sync parent");
}

/// Write `bytes` to `name` and sync the data, leaving its entry unsynced.
fn synced_data_file(fs: &CrashFs, name: &str, bytes: &[u8]) {
    let mut file = fs.open(&path(name), CREATE).expect("create file");
    file.write_at(0, Bytes::copy_from_slice(bytes))
        .expect("write file");
    file.sync(Durability::Durable).expect("sync file");
}

fn write(fs: &CrashFs, name: &str, offset: usize, bytes: &[u8]) {
    let mut file = fs.open(&path(name), CREATE).expect("open file");
    file.write_at(offset as u64, Bytes::copy_from_slice(bytes))
        .expect("write file");
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

fn set(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(ToString::to_string).collect()
}

/// Every distinct state `observe` sees across every power-loss outcome.
fn outcomes<T: Ord>(device: &CrashDevice, observe: impl Fn(&CrashFs) -> T) -> BTreeSet<T> {
    let decisions = device.decisions();
    let total: usize = decisions.iter().map(|decision| decision.options).product();
    assert!(total <= 1 << 14, "too many outcomes: {decisions:?}");
    (0..total)
        .map(|mut index| {
            let choices: Vec<usize> = decisions
                .iter()
                .map(|decision| {
                    let choice = index % decision.options;
                    index /= decision.options;
                    choice
                })
                .collect();
            let fork = device.fork();
            fork.power_loss_with(|decision| {
                choices[decisions
                    .iter()
                    .position(|known| known == decision)
                    .expect("decision is enumerated")]
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
    write(&fs, "segment", 0, &[b'a'; PAGE_BYTES]);
    write(&fs, "segment", PAGE_BYTES, &[b'b'; PAGE_BYTES]);

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
            vec![0],
            vec![b'a'],
            vec![0, 0],
            vec![0, b'b'],
            vec![b'a', 0],
            vec![b'a', b'b'],
        ])
    );
}

#[test]
fn should_tear_new_file_in_subdirectory_when_power_is_lost() {
    // Arrange: an unsynced file in a durable subdirectory.
    let device = CrashDevice::new();
    let fs = device.fs();
    fs.create_dir_all(&path("wal")).expect("create wal");
    write(&fs, "wal/seg", 0, &[7; 100]);

    // Act
    let files = outcomes(&device, |fs| read(fs, "wal/seg"));

    // Assert
    assert_eq!(
        files,
        BTreeSet::from([None, Some(vec![]), Some(vec![0; 100]), Some(vec![7; 100])])
    );
}

#[test]
fn should_keep_ordered_prefix_of_entry_changes_when_power_is_lost() {
    // Arrange: unlink old, then create new, with no directory sync.
    let device = CrashDevice::new();
    let fs = device.fs();
    durable_file(&fs, "old", b"old");
    fs.remove_file(&path("old")).expect("unlink old");
    synced_data_file(&fs, "new", b"new");

    // Act
    let listings = outcomes(&device, |fs| names(fs, ""));

    // Assert: metadata reaches the device in order, so the create never
    // survives without the earlier unlink.
    assert_eq!(
        listings,
        BTreeSet::from([set(&["old"]), set(&[]), set(&["new"])])
    );
}

#[test]
fn should_keep_unlinked_name_absent_or_recreated_when_power_is_lost() {
    // Arrange
    let device = CrashDevice::new();
    let fs = device.fs();
    durable_file(&fs, "name", b"old");
    fs.remove_file(&path("name")).expect("unlink");
    synced_data_file(&fs, "name", b"new");

    // Act
    let contents = outcomes(&device, |fs| read(fs, "name"));

    // Assert
    assert_eq!(
        contents,
        BTreeSet::from([None, Some(b"old".to_vec()), Some(b"new".to_vec())])
    );
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
fn should_commit_only_through_last_change_when_syncing_a_directory() {
    // Arrange: a/x changes before b/y, and b/y before a/z.
    let device = CrashDevice::new();
    let fs = device.fs();
    fs.create_dir_all(&path("a")).expect("create a");
    fs.create_dir_all(&path("b")).expect("create b");
    synced_data_file(&fs, "a/x", b"x");
    synced_data_file(&fs, "b/y", b"y");
    let before_later_change = device.fork();
    synced_data_file(&fs, "a/z", b"z");

    // Act
    fs.sync_dir(&path("a"), Durability::Durable)
        .expect("sync a after z");
    before_later_change
        .fs()
        .sync_dir(&path("a"), Durability::Durable)
        .expect("sync a before z");
    let committed_through_z = outcomes(&device, |fs| names(fs, "b"));
    let committed_through_x = outcomes(&before_later_change, |fs| names(fs, "b"));

    // Assert: a journal commit carries every earlier change, but nothing
    // after the directory's last one.
    assert_eq!(committed_through_z, BTreeSet::from([set(&["y"])]));
    assert_eq!(committed_through_x, BTreeSet::from([set(&[]), set(&["y"])]));
}

#[test]
fn should_leave_orphan_temp_file_when_staged_rename_is_not_synced() {
    // Arrange: the staging pattern without its final directory sync.
    let device = CrashDevice::new();
    let fs = device.fs();
    synced_data_file(&fs, "tmp", b"staged");
    fs.rename_atomic(&path("tmp"), &path("final"))
        .expect("publish");

    // Act
    let listings = outcomes(&device, |fs| names(fs, ""));

    // Assert
    assert_eq!(
        listings,
        BTreeSet::from([set(&[]), set(&["tmp"]), set(&["final"])])
    );
}

#[test]
fn should_leave_sealed_segment_without_successor_when_rotation_is_cut() {
    // Arrange: seal the active segment, then create its successor.
    let device = CrashDevice::new();
    let fs = device.fs();
    durable_file(&fs, "active", b"one");
    fs.rename_atomic(&path("active"), &path("sealed-1"))
        .expect("seal");
    synced_data_file(&fs, "active", b"two");

    // Act
    let listings = outcomes(&device, |fs| names(fs, ""));

    // Assert
    assert_eq!(
        listings,
        BTreeSet::from([
            set(&["active"]),
            set(&["sealed-1"]),
            set(&["active", "sealed-1"]),
        ])
    );
}

#[test]
fn should_leave_any_prefix_of_tree_when_power_is_lost_after_removal() {
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

    // Assert: deletion is entry by entry, so a partial tree can remain.
    assert_eq!(
        trees,
        BTreeSet::from([
            None,
            Some(set(&[])),
            Some(set(&["y"])),
            Some(set(&["x", "y"])),
        ])
    );
}

#[test]
fn should_leave_empty_file_when_truncating_rewrite_is_cut() {
    // Arrange: O_TRUNC over 100 durable bytes, then a 50-byte rewrite.
    let device = CrashDevice::new();
    let fs = device.fs();
    durable_file(&fs, "manifest", &[b'x'; 100]);
    let mut file = fs
        .open(
            &path("manifest"),
            OpenOptions {
                truncate: true,
                ..CREATE
            },
        )
        .expect("truncate");
    file.write_at(0, Bytes::from(vec![b'y'; 50]))
        .expect("rewrite");
    drop(file);

    // Act
    let contents = outcomes(&device, |fs| read(fs, "manifest"));

    // Assert: never the freed old bytes beside the rewrite.
    assert_eq!(
        contents,
        BTreeSet::from([
            Some(vec![]),
            Some(vec![0; 50]),
            Some(vec![b'y'; 50]),
            Some(vec![b'x'; 100]),
        ])
    );
}

#[test]
fn should_end_inside_record_when_appends_are_partly_written_back() {
    // Arrange: two appends spanning a page boundary.
    let device = CrashDevice::new();
    let fs = device.fs();
    durable_file(&fs, "wal", b"");
    write(&fs, "wal", 0, &[b'a'; 5000]);
    write(&fs, "wal", 5000, &[b'b'; 4000]);

    // Act
    let lengths = outcomes(&device, |fs| read(fs, "wal").map(|bytes| bytes.len()));

    // Assert
    assert_eq!(
        lengths,
        BTreeSet::from([Some(0), Some(4096), Some(5000), Some(8192), Some(9000)])
    );
}

#[test]
fn should_persist_any_version_of_page_written_twice() {
    // Arrange
    let device = CrashDevice::new();
    let fs = device.fs();
    durable_file(&fs, "page", b"orig");
    write(&fs, "page", 0, b"aaaa");
    write(&fs, "page", 0, b"bbbb");

    // Act
    let contents = outcomes(&device, |fs| read(fs, "page"));

    // Assert
    assert_eq!(
        contents,
        BTreeSet::from([
            Some(b"orig".to_vec()),
            Some(b"aaaa".to_vec()),
            Some(b"bbbb".to_vec()),
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
fn should_persist_data_when_sync_fault_follows_effect() {
    // Arrange
    let device = CrashDevice::new();
    let fs = device.fs();
    durable_file(&fs, "wal", b"");
    let fault = device.inject(Fault {
        op: FaultOp::Sync,
        path: "wal".into(),
        nth: 1,
        effect: FaultEffect::After,
        error: FaultError::NoSpace,
    });
    let mut file = fs.open(&path("wal"), CREATE).expect("open file");
    file.write_at(0, Bytes::from_static(b"record"))
        .expect("write record");

    // Act
    let result = file.sync(Durability::Durable);
    drop(file);
    let durable = outcomes(&device, |fs| read(fs, "wal"));

    // Assert: the error is ambiguous; the record is durable anyway.
    assert!(device.fired(fault));
    assert!(matches!(result, Err(FsError::NoSpace(_))), "{result:?}");
    assert_eq!(durable, BTreeSet::from([Some(b"record".to_vec())]));
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
fn should_apply_write_when_fault_follows_effect() {
    // Arrange
    let device = CrashDevice::new();
    let fs = device.fs();
    durable_file(&fs, "file", b"before");
    let fault = device.inject(Fault {
        op: FaultOp::Write,
        path: "file".into(),
        nth: 1,
        effect: FaultEffect::After,
        error: FaultError::Io,
    });
    let mut file = fs.open(&path("file"), CREATE).expect("open file");

    // Act
    let result = file.write_at(0, Bytes::from_static(b"after!"));
    drop(file);

    // Assert
    assert!(device.fired(fault));
    assert!(matches!(result, Err(FsError::Io(_))), "{result:?}");
    assert_eq!(read(&fs, "file"), Some(b"after!".to_vec()));
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
fn should_fail_nth_matching_call_when_rules_share_calls() {
    // Arrange: two rules count the same calls; a rename counts once even
    // though both of its paths match.
    let device = CrashDevice::new();
    let fs = device.fs();
    fs.create_dir_all(&path("wal")).expect("create wal");
    let second = device.inject(Fault {
        op: FaultOp::Rename,
        path: "wal/".into(),
        nth: 2,
        effect: FaultEffect::Before,
        error: FaultError::Io,
    });
    let third = device.inject(Fault {
        op: FaultOp::Rename,
        path: "/wal".into(),
        nth: 3,
        effect: FaultEffect::Before,
        error: FaultError::NoSpace,
    });
    for name in ["wal/a", "wal/b", "wal/c"] {
        durable_file(&fs, name, b"x");
    }

    // Act
    let first_call = fs.rename_atomic(&path("wal/a"), &path("wal/a2"));
    let second_call = fs.rename_atomic(&path("wal/b"), &path("wal/b2"));
    let third_call = fs.rename_atomic(&path("wal/c"), &path("wal/c2"));
    let exhausted = fs.rename_atomic(&path("wal/a2"), &path("walrus"));

    // Assert
    assert!(first_call.is_ok(), "{first_call:?}");
    assert!(
        matches!(second_call, Err(FsError::Io(_))),
        "{second_call:?}"
    );
    assert!(
        matches!(third_call, Err(FsError::NoSpace(_))),
        "{third_call:?}"
    );
    assert!(exhausted.is_ok(), "{exhausted:?}");
    assert!(device.fired(second));
    assert!(device.fired(third));
}

#[test]
fn should_not_count_after_fault_as_fired_when_operation_fails_itself() {
    // Arrange
    let device = CrashDevice::new();
    let fs = device.fs();
    let fault = device.inject(Fault {
        op: FaultOp::RemoveFile,
        path: String::new(),
        nth: 1,
        effect: FaultEffect::After,
        error: FaultError::Io,
    });

    // Act
    let result = fs.remove_file(&path("missing"));

    // Assert
    assert!(matches!(result, Err(FsError::NotFound(_))), "{result:?}");
    assert!(!device.fired(fault));
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
    write(&fs, "journal", 0, b"edit");

    // Act
    device.process_crash();
    let after_process_crash = read(&device.fs(), "journal");
    let after_power_loss = outcomes(&device, |fs| read(fs, "journal"));

    // Assert: the restarted process sees the write, but it was never made
    // durable, so a later power loss may drop it.
    assert_eq!(after_process_crash, Some(b"edit".to_vec()));
    assert_eq!(
        after_power_loss,
        BTreeSet::from([Some(vec![]), Some(vec![0; 4]), Some(b"edit".to_vec())])
    );
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
    // Arrange: the same unsynced file, with and without an unrelated file
    // that also has unsynced pages.
    let build = |unrelated: bool| {
        let device = CrashDevice::new();
        let fs = device.fs();
        if unrelated {
            durable_file(&fs, "aaa-unrelated", b"");
            write(&fs, "aaa-unrelated", 0, &[1; 3 * PAGE_BYTES]);
        }
        fs.create_dir_all(&path("wal")).expect("create wal");
        durable_file(&fs, "wal/segment", b"");
        write(&fs, "wal/segment", 0, &[7; 4 * PAGE_BYTES]);
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
            read(&alone.fs(), "wal/segment"),
            read(&with_unrelated.fs(), "wal/segment"),
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

#[test]
fn should_name_files_apart_when_path_is_reused() {
    // Arrange: a file is unlinked and its path reused, both with unsynced
    // pages.
    let device = CrashDevice::new();
    let fs = device.fs();
    durable_file(&fs, "final", b"");
    write(&fs, "final", 0, b"old");
    fs.remove_file(&path("final")).expect("unlink");
    write(&fs, "final", 0, b"new");

    // Act
    let decisions = device.decisions();

    // Assert: each inode keeps its own choices.
    let subjects: BTreeSet<String> = decisions
        .iter()
        .map(|decision| format!("{:?}", decision.subject))
        .collect();
    assert_eq!(subjects.len(), decisions.len(), "{decisions:?}");
    assert!(
        subjects.iter().any(|subject| subject.contains("final@2")),
        "{subjects:?}"
    );
}
