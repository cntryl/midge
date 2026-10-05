//! Cooperative filesystem views for a shared ownership-transfer deadline.

use super::traits::{DirEntry, Metadata, ReadObserver};
use super::{Durability, File, Fs, FsError, FsPath, FsResult, HostAddressing, OpenOptions};
use crate::common::DeadlineScope;
use bytes::Bytes;
use std::sync::Arc;

pub(crate) fn scope_fs(inner: Arc<dyn Fs>, scope: DeadlineScope) -> Arc<dyn Fs> {
    Arc::new(ScopedFs { inner, scope })
}

struct ScopedFs {
    inner: Arc<dyn Fs>,
    scope: DeadlineScope,
}

struct ScopedFile<'a> {
    inner: Box<dyn File + 'a>,
    scope: DeadlineScope,
}

fn check(scope: &DeadlineScope, context: &str) -> FsResult<()> {
    scope
        .check(context)
        .map_err(|error| FsError::Timeout(error.to_string()))
}

fn checked<T>(
    scope: &DeadlineScope,
    context: &str,
    operation: impl FnOnce() -> FsResult<T>,
) -> FsResult<T> {
    check(scope, context)?;
    let value = operation()?;
    check(scope, context)?;
    Ok(value)
}

impl Fs for ScopedFs {
    fn local_output_view(&self) -> Option<Arc<dyn Fs>> {
        self.inner
            .local_output_view()
            .map(|inner| scope_fs(inner, self.scope.clone()))
    }

    fn with_read_observer(&self, observer: Arc<dyn ReadObserver>) -> Option<Arc<dyn Fs>> {
        self.inner
            .with_read_observer(observer)
            .map(|inner| scope_fs(inner, self.scope.clone()))
    }

    fn immutable_read_view(&self, path: &FsPath) -> FsResult<Option<Arc<dyn Fs>>> {
        checked(&self.scope, "filesystem immutable view", || {
            self.inner.immutable_read_view(path)
        })
        .map(|view| view.map(|inner| scope_fs(inner, self.scope.clone())))
    }

    fn host_addressing(&self) -> Option<HostAddressing<'_>> {
        self.inner.host_addressing()
    }

    fn coordination_key(&self) -> u64 {
        self.inner.coordination_key()
    }

    fn open(&self, path: &FsPath, options: OpenOptions) -> FsResult<Box<dyn File + '_>> {
        let inner = checked(&self.scope, "filesystem open", || {
            self.inner.open(path, options)
        })?;
        Ok(Box::new(ScopedFile {
            inner,
            scope: self.scope.clone(),
        }))
    }

    fn open_persistent_handle(
        &self,
        path: &FsPath,
        options: OpenOptions,
    ) -> FsResult<Box<dyn File>> {
        let inner = checked(&self.scope, "filesystem persistent open", || {
            self.inner.open_persistent_handle(path, options)
        })?;
        Ok(Box::new(ScopedFile {
            inner,
            scope: self.scope.clone(),
        }))
    }

    fn remove_file(&self, path: &FsPath) -> FsResult<()> {
        checked(&self.scope, "filesystem removal", || {
            self.inner.remove_file(path)
        })
    }

    fn exists(&self, path: &FsPath) -> FsResult<bool> {
        checked(&self.scope, "filesystem existence", || {
            self.inner.exists(path)
        })
    }

    fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
        checked(&self.scope, "filesystem metadata", || {
            self.inner.metadata(path)
        })
    }

    fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
        checked(&self.scope, "filesystem directory creation", || {
            self.inner.create_dir_all(path)
        })
    }

    fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
        checked(&self.scope, "filesystem directory listing", || {
            self.inner.list_dir(path)
        })
    }

    fn remove_dir_all(&self, path: &FsPath) -> FsResult<()> {
        checked(&self.scope, "filesystem directory removal", || {
            self.inner.remove_dir_all(path)
        })
    }

    fn sync_dir(&self, path: &FsPath, durability: Durability) -> FsResult<()> {
        checked(&self.scope, "filesystem directory sync", || {
            self.inner.sync_dir(path, durability)
        })
    }

    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        checked(&self.scope, "filesystem rename", || {
            self.inner.rename_atomic(from, to)
        })
    }
}

impl File for ScopedFile<'_> {
    fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes> {
        checked(&self.scope, "filesystem file read", || {
            self.inner.read_at(offset, len)
        })
    }

    fn write_at(&mut self, offset: u64, data: Bytes) -> FsResult<()> {
        checked(&self.scope, "filesystem file write", || {
            self.inner.write_at(offset, data)
        })
    }

    fn truncate(&mut self, len: u64) -> FsResult<()> {
        checked(&self.scope, "filesystem truncation", || {
            self.inner.truncate(len)
        })
    }

    fn append(&mut self, data: Bytes) -> FsResult<u64> {
        checked(&self.scope, "filesystem append", || self.inner.append(data))
    }

    fn len(&self) -> FsResult<u64> {
        checked(&self.scope, "filesystem length", || self.inner.len())
    }

    fn sync(&mut self, durability: Durability) -> FsResult<()> {
        checked(&self.scope, "filesystem sync", || {
            self.inner.sync(durability)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::OperationDeadline;
    use crate::io::OpenMode;

    fn read_write() -> OpenOptions {
        OpenOptions {
            mode: OpenMode::ReadWrite,
            create: true,
            create_new: false,
            truncate: false,
        }
    }

    fn active_scope() -> DeadlineScope {
        DeadlineScope::new(OperationDeadline::from_budget(
            std::time::Duration::from_secs(5),
        ))
    }

    #[test]
    fn should_reject_local_mutation_when_scope_was_cancelled_before_submission() {
        // Arrange: use the actual local filesystem and one retained private view.
        let directory = tempfile::tempdir().unwrap();
        let inner: Arc<dyn Fs> = Arc::new(crate::io::RealFs::new(directory.path()).unwrap());
        let scope = active_scope();
        let view = scope_fs(inner, scope.clone());
        scope.cancel();

        // Act
        let result = view.open(&FsPath::new("cancelled.wal"), read_write());

        // Assert: cancellation prevents calling the create operation at all.
        assert!(matches!(result, Err(FsError::Timeout(_))));
        assert!(!directory.path().join("cancelled.wal").exists());
    }

    #[test]
    fn should_cancel_retained_persistent_file_when_its_owner_cancels_scope() {
        // Arrange: the private Fs can be dropped while the static handle survives.
        let directory = tempfile::tempdir().unwrap();
        let inner: Arc<dyn Fs> = Arc::new(crate::io::RealFs::new(directory.path()).unwrap());
        let scope = active_scope();
        let view = scope_fs(inner, scope.clone());
        let mut file = view
            .open_persistent_handle(&FsPath::new("retained.wal"), read_write())
            .unwrap();
        file.append(Bytes::from_static(b"acknowledged bytes"))
            .unwrap();
        drop(view);

        // Act
        scope.cancel();
        let read = file.read_at(0, 18);
        let append = file.append(Bytes::from_static(b"late write"));
        let truncate = file.truncate(0);

        // Assert: all operations consult the same scope after handle transfer.
        assert!(matches!(read, Err(FsError::Timeout(_))));
        assert!(matches!(append, Err(FsError::Timeout(_))));
        assert!(matches!(truncate, Err(FsError::Timeout(_))));
        assert_eq!(
            std::fs::read(directory.path().join("retained.wal")).unwrap(),
            b"acknowledged bytes"
        );
    }

    struct CancelAfterRead<'a> {
        inner: Box<dyn File + 'a>,
        scope: DeadlineScope,
    }

    impl File for CancelAfterRead<'_> {
        fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes> {
            let bytes = self.inner.read_at(offset, len)?;
            self.scope.cancel();
            Ok(bytes)
        }

        fn write_at(&mut self, offset: u64, bytes: Bytes) -> FsResult<()> {
            self.inner.write_at(offset, bytes)
        }

        fn append(&mut self, bytes: Bytes) -> FsResult<u64> {
            self.inner.append(bytes)
        }

        fn len(&self) -> FsResult<u64> {
            self.inner.len()
        }

        fn sync(&mut self, durability: Durability) -> FsResult<()> {
            self.inner.sync(durability)
        }
    }

    #[test]
    fn should_reject_completed_local_read_when_cancellation_precedes_its_result() {
        // Arrange: a genuine local read completes, then the owner cancels
        // before ScopedFile can deliver its result. No read error is injected.
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("manifest"), b"durable metadata").unwrap();
        let inner = crate::io::RealFs::new(directory.path()).unwrap();
        let scope = active_scope();
        let file = ScopedFile {
            inner: Box::new(CancelAfterRead {
                inner: inner.open(&FsPath::new("manifest"), read_write()).unwrap(),
                scope: scope.clone(),
            }),
            scope,
        };

        // Act
        let result = file.read_at(0, 16);

        // Assert: the successful read is not admitted as a late recovery result.
        assert!(matches!(result, Err(FsError::Timeout(_))));
        assert_eq!(
            std::fs::read(directory.path().join("manifest")).unwrap(),
            b"durable metadata"
        );
    }

    #[test]
    fn should_disarm_retained_local_file_when_startup_owner_completes_transfer() {
        // Arrange: complete real filesystem creation before capturing the clock.
        let directory = tempfile::tempdir().unwrap();
        let inner: Arc<dyn Fs> = Arc::new(crate::io::RealFs::new(directory.path()).unwrap());
        let path = FsPath::new("accepted.wal");
        drop(inner.open_persistent_handle(&path, read_write()).unwrap());
        let deadline = OperationDeadline::from_budget(std::time::Duration::from_secs(5));
        let scope = DeadlineScope::new(deadline);
        let view = scope_fs(inner, scope.clone());
        let mut file = view.open_persistent_handle(&path, read_write()).unwrap();
        scope.complete().unwrap();
        drop(view);

        // Act: normal runtime writes outlive the original startup budget.
        while !deadline.is_expired() {
            std::thread::sleep(deadline.remaining());
        }
        let ambiguous = scope.cancel();
        let result = file.append(Bytes::from_static(b"normal runtime"));
        file.sync(Durability::Durable).unwrap();

        // Assert: completed transfer disarms even the retained persistent file.
        assert!(deadline.is_expired());
        assert!(!ambiguous);
        assert!(!scope.deadline().is_bounded());
        assert_eq!(result.unwrap(), 0);
        assert_eq!(
            file.read_at(0, 14).unwrap(),
            Bytes::from_static(b"normal runtime")
        );
        assert_eq!(
            std::fs::read(directory.path().join("accepted.wal")).unwrap(),
            b"normal runtime"
        );
    }

    struct ReadObserverStub;

    impl ReadObserver for ReadObserverStub {
        fn remote_range_started(&self) {}
        fn remote_range_completed(&self, _: u64, _: std::time::Duration, _: bool) {}
    }

    struct CapabilityFs(Arc<dyn Fs>);

    impl Fs for CapabilityFs {
        fn local_output_view(&self) -> Option<Arc<dyn Fs>> {
            Some(Arc::clone(&self.0))
        }
        fn with_read_observer(&self, _: Arc<dyn ReadObserver>) -> Option<Arc<dyn Fs>> {
            Some(Arc::clone(&self.0))
        }
        fn immutable_read_view(&self, _: &FsPath) -> FsResult<Option<Arc<dyn Fs>>> {
            Ok(Some(Arc::clone(&self.0)))
        }
        fn host_addressing(&self) -> Option<HostAddressing<'_>> {
            self.0.host_addressing()
        }
        fn coordination_key(&self) -> u64 {
            self.0.coordination_key()
        }
        fn open(&self, path: &FsPath, options: OpenOptions) -> FsResult<Box<dyn File + '_>> {
            self.0.open(path, options)
        }
        fn open_persistent_handle(
            &self,
            path: &FsPath,
            options: OpenOptions,
        ) -> FsResult<Box<dyn File>> {
            self.0.open_persistent_handle(path, options)
        }
        fn remove_file(&self, path: &FsPath) -> FsResult<()> {
            self.0.remove_file(path)
        }
        fn exists(&self, path: &FsPath) -> FsResult<bool> {
            self.0.exists(path)
        }
        fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
            self.0.metadata(path)
        }
        fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
            self.0.create_dir_all(path)
        }
        fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
            self.0.list_dir(path)
        }
        fn remove_dir_all(&self, path: &FsPath) -> FsResult<()> {
            self.0.remove_dir_all(path)
        }
        fn sync_dir(&self, path: &FsPath, durability: Durability) -> FsResult<()> {
            self.0.sync_dir(path, durability)
        }
        fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
            self.0.rename_atomic(from, to)
        }
    }

    #[test]
    fn should_preserve_filesystem_context_when_capabilities_derive_views() {
        // Arrange: all three capability branches delegate to a real local Fs.
        let directory = tempfile::tempdir().unwrap();
        let inner: Arc<dyn Fs> = Arc::new(crate::io::RealFs::new(directory.path()).unwrap());
        let scope = active_scope();
        let view = scope_fs(Arc::new(CapabilityFs(Arc::clone(&inner))), scope.clone());
        let derived = [
            view.local_output_view().unwrap(),
            view.with_read_observer(Arc::new(ReadObserverStub)).unwrap(),
            view.immutable_read_view(&FsPath::new("table.sst"))
                .unwrap()
                .unwrap(),
        ];

        // Act
        scope.cancel();

        // Assert: capability derivation cannot expose an unscoped escape view.
        for child in derived {
            assert_eq!(child.host_addressing(), inner.host_addressing());
            assert_eq!(child.coordination_key(), inner.coordination_key());
            assert!(matches!(
                child.exists(&FsPath::new("table.sst")),
                Err(FsError::Timeout(_))
            ));
        }
        assert!(matches!(
            view.immutable_read_view(&FsPath::new("late.sst")),
            Err(FsError::Timeout(_))
        ));
    }
}
