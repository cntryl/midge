//! Filesystem contract coverage across accounting and Core wrappers.

use bytes::Bytes;
use cntryl_midge_core::common::{DeadlineScope, OperationDeadline};
use cntryl_midge_core::io::{
    open_persistent_handle, scope_fs, subdirectory, transient_read::TransientReadFs, Fs, FsError,
    FsPath, OpenMode, OpenOptions, RealFs,
};
use cntryl_midge_durability::metadata::{accounted_fs::account_fs, accounting::Ledger};
use std::sync::Arc;

const READ_ONLY: OpenOptions = OpenOptions {
    mode: OpenMode::ReadOnly,
    create: false,
    create_new: false,
    truncate: false,
};

#[test]
fn should_preserve_missing_owned_handle_support_when_filesystem_views_are_wrapped() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("readable.sst"), b"visible").unwrap();
    std::fs::create_dir(directory.path().join("view")).unwrap();
    std::fs::write(directory.path().join("view/readable.sst"), b"visible").unwrap();
    let inner: Arc<dyn Fs> = Arc::new(TransientReadFs {
        inner: RealFs::new(directory.path()).unwrap(),
        fail_from: u64::MAX,
    });
    let scope = DeadlineScope::new(OperationDeadline::unbounded());
    let views = [
        Arc::clone(&inner),
        scope_fs(Arc::clone(&inner), scope),
        subdirectory(Arc::clone(&inner), "view"),
        account_fs(inner, Arc::new(parking_lot::Mutex::new(Ledger::default()))),
    ];

    for view in views {
        // Act
        let file = view.open_read(&FsPath::new("readable.sst")).unwrap();
        let bytes = file.read_at(0, 7).unwrap();
        let has_owned_handles = view.persistent_handle_capability().is_some();
        let persistent =
            open_persistent_handle(view.as_ref(), &FsPath::new("readable.sst"), READ_ONLY);

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
    let inner: Arc<dyn Fs> = Arc::new(RealFs::new(directory.path()).unwrap());
    let scope = DeadlineScope::new(OperationDeadline::unbounded());
    let views = [
        (scope_fs(Arc::clone(&inner), scope), "owner.lock"),
        (
            subdirectory(Arc::clone(&inner), "nested"),
            "nested/owner.lock",
        ),
        (
            account_fs(
                Arc::clone(&inner),
                Arc::new(parking_lot::Mutex::new(Ledger::default())),
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
