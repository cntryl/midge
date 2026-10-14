//! Contract tests that hold `MockFs` and `CrashFs` to the observable behavior
//! of `RealFs`.

use super::traits::{DirEntry, Fs, FsError, FsPath, OpenMode, OpenOptions};
use super::{MockFs, RealFs};
use bytes::Bytes;

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

/// Run `check` against a fresh `RealFs` rooted in a tempdir, a fresh `MockFs`
/// and a fresh `CrashFs`, labelling failures with the backend name.
fn for_each_backend(check: impl Fn(&str, &dyn Fs)) {
    let temp_dir = tempfile::tempdir().expect("create tempdir");
    let real = RealFs::new(temp_dir.path()).expect("create RealFs");
    check("RealFs", &real);
    check("MockFs", &MockFs::new());
    check(
        "CrashFs",
        &crate::simulation::crash_fs::CrashDevice::new().fs(),
    );
}

/// Run `check` against the backends that model `RealFs` error classes and
/// lengths exactly. `MockFs` is knowingly lenient where these tests probe.
fn for_each_faithful_backend(check: impl Fn(&str, &dyn Fs)) {
    let temp_dir = tempfile::tempdir().expect("create tempdir");
    let real = RealFs::new(temp_dir.path()).expect("create RealFs");
    check("RealFs", &real);
    check(
        "CrashFs",
        &crate::simulation::crash_fs::CrashDevice::new().fs(),
    );
}

fn write_file(fs: &dyn Fs, path: &str, data: &'static [u8]) {
    let path = FsPath::new(path);
    if let Some(parent) = std::path::Path::new(&path.0).parent() {
        fs.create_dir_all(&FsPath::new(parent.to_string_lossy()))
            .expect("create parent directory");
    }
    let mut file = fs.open(&path, CREATE).expect("create file");
    file.append(Bytes::from_static(data)).expect("write file");
}

fn sorted_entries(mut entries: Vec<DirEntry>) -> Vec<(String, bool)> {
    entries.sort_by(|left, right| left.name.cmp(&right.name));
    entries
        .into_iter()
        .map(|entry| (entry.name, entry.is_dir))
        .collect()
}

#[test]
fn should_preserve_missing_owned_handle_support_when_filesystem_views_are_wrapped() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("readable.sst"), b"visible").unwrap();
    std::fs::create_dir(directory.path().join("view")).unwrap();
    std::fs::write(directory.path().join("view/readable.sst"), b"visible").unwrap();
    let inner: std::sync::Arc<dyn Fs> =
        std::sync::Arc::new(super::transient_read::TransientReadFs {
            inner: RealFs::new(directory.path()).unwrap(),
            fail_from: u64::MAX,
        });
    let scope = crate::common::DeadlineScope::new(crate::common::OperationDeadline::unbounded());
    let views = [
        std::sync::Arc::clone(&inner),
        super::scope_fs(std::sync::Arc::clone(&inner), scope),
        super::subdirectory(std::sync::Arc::clone(&inner), "view"),
        crate::metadata::accounted_fs::account_fs(
            inner,
            std::sync::Arc::new(parking_lot::Mutex::new(
                crate::metadata::accounting::Ledger::default(),
            )),
        ),
    ];

    for view in views {
        // Act
        let file = view.open_read(&FsPath::new("readable.sst")).unwrap();
        let bytes = file.read_at(0, 7).unwrap();
        let has_owned_handles = view.persistent_handle_capability().is_some();
        let persistent = crate::io::open_persistent_handle(
            view.as_ref(),
            &FsPath::new("readable.sst"),
            READ_ONLY,
        );

        // Assert
        assert_eq!(bytes, Bytes::from_static(b"visible"));
        assert!(!has_owned_handles);
        assert!(matches!(persistent, Err(FsError::Unsupported(_))));
    }
}

#[test]
fn should_preserve_native_lock_exclusion_when_writable_filesystem_views_are_wrapped() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir(directory.path().join("nested")).unwrap();
    let inner: std::sync::Arc<dyn Fs> = std::sync::Arc::new(RealFs::new(directory.path()).unwrap());
    let scope = crate::common::DeadlineScope::new(crate::common::OperationDeadline::unbounded());
    let views = [
        (
            super::scope_fs(std::sync::Arc::clone(&inner), scope),
            "owner.lock",
        ),
        (
            super::subdirectory(std::sync::Arc::clone(&inner), "nested"),
            "nested/owner.lock",
        ),
        (
            crate::metadata::accounted_fs::account_fs(
                std::sync::Arc::clone(&inner),
                std::sync::Arc::new(parking_lot::Mutex::new(
                    crate::metadata::accounting::Ledger::default(),
                )),
            ),
            "owner.lock",
        ),
    ];

    for (view, underlying_path) in views {
        // Act
        let held = inner
            .try_lock_exclusive(&FsPath::new(underlying_path))
            .unwrap();
        let contended = view.try_lock_exclusive(&FsPath::new("owner.lock"));
        drop(held);
        let acquired = view.try_lock_exclusive(&FsPath::new("owner.lock"));

        // Assert
        assert!(matches!(contended, Err(FsError::AlreadyExists(_))));
        assert!(acquired.is_ok());
    }
}

#[test]
fn should_return_not_found_when_opening_missing_file_without_create() {
    for_each_backend(|backend, fs| {
        // Arrange
        let path = FsPath::new("missing.sst");

        // Act
        let persistent = crate::io::open_persistent_handle(fs, &path, READ_ONLY).map(|_| ());
        let error = fs.open(&path, READ_ONLY).map(|_| ()).unwrap_err();

        // Assert
        assert!(
            matches!(error, FsError::NotFound(_)),
            "{backend}: {error:?}"
        );
        assert!(
            matches!(persistent, Err(FsError::NotFound(_))),
            "{backend}: {persistent:?}"
        );
        assert!(
            !fs.exists(&path).unwrap(),
            "{backend}: open must not create"
        );
    });
}

#[test]
fn should_match_read_only_open_when_opening_for_read() {
    for_each_backend(|backend, fs| {
        // Arrange
        write_file(fs, "dir/table.sst", b"durable bytes");
        let path = FsPath::new("dir/table.sst");
        let missing = FsPath::new("dir/missing.sst");

        // Act
        let read = fs.open_read(&path).and_then(|file| file.read_at(0, 13));
        let opened = fs
            .open(&path, READ_ONLY)
            .and_then(|file| file.read_at(0, 13));
        let absent = fs.open_read(&missing).map(|_| ());

        // Assert
        assert_eq!(read.unwrap(), opened.unwrap(), "{backend}");
        assert!(
            matches!(absent, Err(FsError::NotFound(_))),
            "{backend}: {absent:?}"
        );
        assert!(
            !fs.exists(&missing).unwrap(),
            "{backend}: open_read must not create"
        );
    });
}

#[test]
fn should_list_only_immediate_children_when_listing_directory() {
    for_each_backend(|backend, fs| {
        // Arrange
        write_file(fs, "wal/000001.log", b"a");
        write_file(fs, "wal/nested/000002.log", b"b");
        write_file(fs, "wal_catalog/entry", b"c");

        // Act
        let entries = sorted_entries(fs.list_dir(&FsPath::new("wal")).unwrap());

        // Assert
        assert_eq!(
            entries,
            vec![
                ("000001.log".to_string(), false),
                ("nested".to_string(), true),
            ],
            "{backend}"
        );
    });
}

#[test]
fn should_report_not_found_when_listing_missing_directory() {
    for_each_backend(|backend, fs| {
        // Arrange
        let missing = FsPath::new("never-created");

        // Act
        let result = fs.list_dir(&missing);

        // Assert
        assert!(
            matches!(result, Err(FsError::NotFound(_))),
            "{backend}: {result:?}"
        );
    });
}

#[test]
fn should_not_remove_prefix_sibling_when_removing_directory() {
    for_each_backend(|backend, fs| {
        // Arrange
        write_file(fs, "cloud_recovery/staged", b"a");
        write_file(fs, "cloud_recovery_old/kept", b"b");

        // Act
        fs.remove_dir_all(&FsPath::new("cloud_recovery")).unwrap();

        // Assert
        assert!(
            !fs.exists(&FsPath::new("cloud_recovery/staged")).unwrap(),
            "{backend}"
        );
        assert!(
            fs.exists(&FsPath::new("cloud_recovery_old/kept")).unwrap(),
            "{backend}: prefix sibling must survive"
        );
    });
}

#[test]
fn should_error_on_short_read_when_reading_past_eof() {
    for_each_backend(|backend, fs| {
        // Arrange
        write_file(fs, "short.bin", b"abcd");
        let path = FsPath::new("short.bin");
        let file = fs.open(&path, READ_ONLY).unwrap();
        let persistent = crate::io::open_persistent_handle(fs, &path, READ_ONLY).unwrap();

        // Act
        let result = file.read_at(2, 8);
        let persistent_result = persistent.read_at(2, 8);

        // Assert
        assert!(
            matches!(result, Err(FsError::Io(_))),
            "{backend}: {result:?}"
        );
        assert!(
            matches!(persistent_result, Err(FsError::Io(_))),
            "{backend}: {persistent_result:?}"
        );
        assert_eq!(file.read_at(0, 4).unwrap().as_ref(), b"abcd", "{backend}");
    });
}

#[test]
fn should_report_directories_as_existing_when_created_or_holding_files() {
    for_each_backend(|backend, fs| {
        // Arrange
        fs.create_dir_all(&FsPath::new("wal"))
            .expect("create empty directory");
        write_file(fs, "sst/table.sst", b"sst");

        // Act
        let empty = fs
            .exists(&FsPath::new("wal"))
            .expect("probe empty directory");
        let populated = fs
            .exists(&FsPath::new("sst"))
            .expect("probe populated directory");
        let missing = fs
            .exists(&FsPath::new("missing"))
            .expect("probe missing directory");
        let listed = fs
            .list_dir(&FsPath::new("wal"))
            .expect("list empty directory");

        // Assert
        assert!(empty, "{backend}: a created directory exists");
        assert!(populated, "{backend}: a directory holding files exists");
        assert!(!missing, "{backend}: a missing directory does not exist");
        assert!(
            listed.is_empty(),
            "{backend}: an empty directory lists nothing"
        );
    });
}

#[test]
fn should_create_missing_parents_when_opening_with_create() {
    for_each_backend(|backend, fs| {
        // Arrange
        let path = FsPath::new("wal/epoch/1.wal");

        // Act
        let opened = fs.open(&path, CREATE).map(|_| ());

        // Assert
        assert!(opened.is_ok(), "{backend}: {opened:?}");
        assert!(
            fs.exists(&FsPath::new("wal/epoch")).expect("probe parent"),
            "{backend}"
        );
    });
}

#[test]
fn should_report_not_found_when_renaming_missing_source() {
    for_each_backend(|backend, fs| {
        // Arrange
        let from = FsPath::new("missing");

        // Act
        let result = fs.rename_atomic(&from, &FsPath::new("target"));

        // Assert
        assert!(
            matches!(result, Err(FsError::NotFound(_))),
            "{backend}: {result:?}"
        );
    });
}

#[test]
fn should_report_io_when_syncing_missing_directory() {
    for_each_faithful_backend(|backend, fs| {
        // Arrange
        let path = FsPath::new("missing");

        // Act
        let result = fs.sync_dir(&path, super::traits::Durability::Durable);

        // Assert
        assert!(
            matches!(result, Err(FsError::Io(_))),
            "{backend}: {result:?}"
        );
    });
}

#[test]
fn should_keep_length_when_writing_nothing_past_end() {
    for_each_faithful_backend(|backend, fs| {
        // Arrange
        let path = FsPath::new("empty.bin");
        let mut file = fs.open(&path, CREATE).expect("create file");

        // Act
        file.write_at(10, Bytes::new()).expect("write nothing");

        // Assert
        assert_eq!(file.len().expect("file length"), 0, "{backend}");
    });
}

#[test]
fn should_report_already_exists_when_creating_new_over_existing_file() {
    for_each_backend(|backend, fs| {
        // Arrange
        let path = FsPath::new("exists.bin");
        write_file(fs, "exists.bin", b"original contents");
        let before = fs
            .open(&path, READ_ONLY)
            .expect("open original file")
            .read_at(0, 17)
            .expect("read original file");

        // Act
        let result = fs
            .open(
                &path,
                OpenOptions {
                    create_new: true,
                    ..CREATE
                },
            )
            .map(|_| ());

        // Assert
        assert!(
            matches!(result, Err(FsError::AlreadyExists(_))),
            "{backend}: {result:?}"
        );
        let after = fs
            .open(&path, READ_ONLY)
            .expect("reopen original file")
            .read_at(0, 17)
            .expect("read original file after rejected create_new");
        assert_eq!(after, before, "{backend}: rejected open changed the file");
    });
}

#[test]
fn should_refuse_read_only_create_when_opening() {
    for_each_faithful_backend(|backend, fs| {
        // Arrange
        let path = FsPath::new("read-only.bin");

        // Act
        let result = fs
            .open(
                &path,
                OpenOptions {
                    create: true,
                    ..READ_ONLY
                },
            )
            .map(|_| ());

        // Assert
        assert!(
            matches!(result, Err(FsError::Io(_))),
            "{backend}: {result:?}"
        );
        assert!(!fs.exists(&path).expect("probe file"), "{backend}");
    });
}

#[test]
fn should_ignore_unsafe_sync_when_directory_is_missing() {
    for_each_faithful_backend(|backend, fs| {
        // Arrange
        let path = FsPath::new("missing");

        // Act
        let result = fs.sync_dir(&path, super::traits::Durability::Unsafe);

        // Assert
        assert!(result.is_ok(), "{backend}: {result:?}");
    });
}

#[test]
fn should_read_nothing_past_end_when_length_is_zero() {
    for_each_faithful_backend(|backend, fs| {
        // Arrange
        write_file(fs, "four.bin", b"abcd");
        let file = fs.open(&FsPath::new("four.bin"), READ_ONLY).unwrap();

        // Act
        let result = file.read_at(10, 0);

        // Assert
        assert!(
            result.as_ref().is_ok_and(Bytes::is_empty),
            "{backend}: {result:?}"
        );
    });
}
