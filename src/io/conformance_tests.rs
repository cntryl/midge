//! Contract tests that hold `MockFs` to the observable behavior of `RealFs`.

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

/// Run `check` against a fresh `RealFs` rooted in a tempdir and a fresh
/// `MockFs`, labelling failures with the backend name.
fn for_each_backend(check: impl Fn(&str, &dyn Fs)) {
    let temp_dir = tempfile::tempdir().expect("create tempdir");
    let real = RealFs::new(temp_dir.path()).expect("create RealFs");
    check("RealFs", &real);
    check("MockFs", &MockFs::new());
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
fn should_return_not_found_when_opening_missing_file_without_create() {
    for_each_backend(|backend, fs| {
        // Arrange
        let path = FsPath::new("missing.sst");

        // Act
        let persistent = fs.open_persistent_handle(&path, READ_ONLY).map(|_| ());
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
        let persistent = fs.open_persistent_handle(&path, READ_ONLY).unwrap();

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
