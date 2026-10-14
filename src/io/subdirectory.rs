//! Root a filesystem capability below an existing adapter without constructing
//! another device. Every view shares the parent's fault and crash behavior, so
//! a component handed `wal/` or `sst/` cannot bypass the engine's injected root.

use super::traits::{DirEntry, Metadata, ReadObserver};
use super::{Durability, File, Fs, FsError, FsPath, FsResult, HostAddressing, OpenOptions, ReadFs};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

/// Return a view of `inner` rooted at the relative directory `prefix`.
///
/// The directory itself is not created; callers create it through the parent
/// when they need it to exist.
pub(crate) fn subdirectory(inner: Arc<dyn Fs>, prefix: &str) -> Arc<dyn Fs> {
    let host = inner
        .host_addressing()
        .map(|frame| (frame.root.join(prefix), frame.named_root.join(prefix)));
    Arc::new(Subdirectory {
        inner,
        prefix: prefix.into(),
        host,
    })
}

struct Subdirectory {
    inner: Arc<dyn Fs>,
    prefix: PathBuf,
    /// The view's canonical and named host roots, when the parent has them.
    host: Option<(PathBuf, PathBuf)>,
}

impl Subdirectory {
    fn path(&self, path: &FsPath) -> FsResult<FsPath> {
        let supplied = Path::new(&path.0);
        let relative = if supplied.is_absolute() {
            self.host_addressing()
                .ok_or_else(|| {
                    FsError::Io("unrooted filesystem view rejects absolute path".into())
                })?
                .relative(supplied)
                .ok_or_else(|| FsError::Io("path escapes filesystem view".into()))?
        } else {
            supplied
        };
        if relative
            .components()
            .any(|part| !matches!(part, Component::Normal(_) | Component::CurDir))
        {
            return Err(FsError::Io("path escapes filesystem view".into()));
        }
        // FsPath is a portable key space: in-memory adapters use '/' on every
        // host, whatever separator PathBuf would display.
        let joined = self.prefix.join(relative);
        let parts: Vec<_> = joined
            .components()
            .filter_map(|component| match component {
                Component::Normal(name) => Some(name.to_string_lossy()),
                _ => None,
            })
            .collect();
        Ok(FsPath::new(parts.join("/")))
    }

    fn rewrap(&self, inner: Arc<dyn Fs>) -> Arc<dyn Fs> {
        subdirectory(inner, &self.prefix.to_string_lossy())
    }
}

impl ReadFs for Subdirectory {
    fn open_read(&self, path: &FsPath) -> FsResult<Box<dyn crate::io::ReadFile + '_>> {
        self.open(path, OpenOptions::READ_ONLY)
            .map(|file| file as Box<dyn crate::io::ReadFile>)
    }

    fn exists(&self, path: &FsPath) -> FsResult<bool> {
        self.inner.exists(&self.path(path)?)
    }

    fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
        self.inner.metadata(&self.path(path)?)
    }

    fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
        self.inner.list_dir(&self.path(path)?)
    }
}

impl Fs for Subdirectory {
    fn persistent_handle_capability(&self) -> Option<&dyn super::traits::PersistentHandleFs> {
        Some(self)
    }

    fn local_output_view(&self) -> Option<Arc<dyn Fs>> {
        self.inner
            .local_output_view()
            .map(|inner| self.rewrap(inner))
    }

    fn with_read_observer(&self, observer: Arc<dyn ReadObserver>) -> Option<Arc<dyn Fs>> {
        self.inner
            .with_read_observer(observer)
            .map(|inner| self.rewrap(inner))
    }

    fn immutable_read_view(&self, path: &FsPath) -> FsResult<Option<Arc<dyn Fs>>> {
        self.inner
            .immutable_read_view(&self.path(path)?)
            .map(|view| view.map(|inner| self.rewrap(inner)))
    }

    fn host_addressing(&self) -> Option<HostAddressing<'_>> {
        self.host
            .as_ref()
            .map(|(root, named_root)| HostAddressing { root, named_root })
    }

    fn coordination_key(&self) -> u64 {
        self.inner.coordination_key()
    }

    fn try_lock_exclusive(&self, path: &FsPath) -> FsResult<Box<dyn super::traits::FsLock>> {
        self.inner.try_lock_exclusive(&self.path(path)?)
    }

    fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File + '_>> {
        self.inner.open(&self.path(path)?, opts)
    }

    fn remove_file(&self, path: &FsPath) -> FsResult<()> {
        self.inner.remove_file(&self.path(path)?)
    }

    fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
        self.inner.create_dir_all(&self.path(path)?)
    }

    fn remove_dir_all(&self, path: &FsPath) -> FsResult<()> {
        self.inner.remove_dir_all(&self.path(path)?)
    }

    fn sync_dir(&self, path: &FsPath, dur: Durability) -> FsResult<()> {
        self.inner.sync_dir(&self.path(path)?, dur)
    }

    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        self.inner.rename_atomic(&self.path(from)?, &self.path(to)?)
    }
}

impl super::traits::PersistentHandleFs for Subdirectory {
    fn open_persistent(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File>> {
        self.inner.open_persistent_handle(&self.path(path)?, opts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{MockFs, OpenMode, RealFs};
    use bytes::Bytes;

    fn create() -> OpenOptions {
        OpenOptions {
            mode: OpenMode::ReadWrite,
            create: true,
            create_new: false,
            truncate: false,
        }
    }

    #[test]
    fn should_address_parent_keys_when_view_wraps_memory_storage() {
        // Arrange
        let fs = Arc::new(MockFs::new());
        let view = subdirectory(fs.clone(), "wal");

        // Act
        let mut file = view.open(&FsPath::new("segment"), create()).unwrap();
        file.append(Bytes::from_static(b"history")).unwrap();

        // Assert
        assert_eq!(fs.get_file("wal/segment"), Some(b"history".to_vec()));
        assert!(view.exists(&FsPath::new("segment")).unwrap());
    }

    #[test]
    fn should_reject_paths_that_escape_the_view() {
        // Arrange
        let view = subdirectory(Arc::new(MockFs::new()), "wal");

        // Act
        let escaped = view.exists(&FsPath::new("../manifest"));

        // Assert
        assert!(escaped.is_err());
    }

    #[test]
    fn should_reject_absolute_paths_when_parent_has_no_host_frame() {
        // Arrange
        let view = subdirectory(Arc::new(MockFs::new()), "wal");

        // Act
        let result = view.exists(&FsPath::new("/tmp/segment"));

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_resolve_absolute_host_paths_inside_the_view_root() {
        // Arrange
        let root = tempfile::tempdir().unwrap();
        let fs = RealFs::new(root.path()).unwrap();
        fs.create_dir_all(&FsPath::new("sst")).unwrap();
        let view = subdirectory(Arc::new(fs), "sst");
        let host_root = view.host_addressing().unwrap().root.to_path_buf();

        // Act
        let mut file = view
            .open(
                &FsPath::new(host_root.join("output").to_string_lossy()),
                create(),
            )
            .unwrap();
        file.append(Bytes::from_static(b"sst")).unwrap();

        // Assert
        assert_eq!(std::fs::read(host_root.join("output")).unwrap(), b"sst");
        assert!(view
            .exists(&FsPath::new(root.path().join("outside").to_string_lossy()))
            .is_err());
    }

    #[test]
    fn should_rename_within_the_view_when_both_paths_are_relative() {
        // Arrange
        let fs = Arc::new(MockFs::new());
        let view = subdirectory(fs.clone(), "wal");
        view.open(&FsPath::new("a.tmp"), create())
            .unwrap()
            .append(Bytes::from_static(b"x"))
            .unwrap();

        // Act
        view.rename_atomic(&FsPath::new("a.tmp"), &FsPath::new("a"))
            .unwrap();

        // Assert
        assert_eq!(fs.get_file("wal/a"), Some(b"x".to_vec()));
        assert_eq!(fs.get_file("wal/a.tmp"), None);
    }
}
