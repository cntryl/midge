//! Root a filesystem capability below an existing adapter without constructing
//! another device or bypassing its fault schedule.

use super::traits::{DirEntry, Metadata, ReadObserver};
use super::{Durability, File, Fs, FsError, FsPath, FsResult, HostAddressing, OpenOptions};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

pub(crate) fn subdirectory(inner: Arc<dyn Fs>, prefix: &str) -> Arc<dyn Fs> {
    let host_root = inner.host_addressing().map(|frame| frame.root.join(prefix));
    let anchor = inner
        .host_addressing()
        .and_then(|frame| frame.anchor.map(Path::to_path_buf));
    Arc::new(Subdirectory {
        inner,
        prefix: prefix.into(),
        host_root,
        anchor,
    })
}

struct Subdirectory {
    inner: Arc<dyn Fs>,
    prefix: PathBuf,
    host_root: Option<PathBuf>,
    anchor: Option<PathBuf>,
}

impl Subdirectory {
    fn path(&self, path: &FsPath) -> FsResult<FsPath> {
        let supplied = Path::new(&path.0);
        let relative = if supplied.is_absolute() {
            supplied
                .strip_prefix(self.host_root.as_deref().ok_or_else(|| {
                    FsError::Io("unrooted adapter rejects absolute child path".into())
                })?)
                .map_err(|_| FsError::Io("path escapes filesystem view".into()))?
        } else {
            supplied
        };
        if relative
            .components()
            .any(|part| !matches!(part, Component::Normal(_) | Component::CurDir))
        {
            return Err(FsError::Io("path escapes filesystem view".into()));
        }
        Ok(FsPath::new(self.prefix.join(relative).to_string_lossy()))
    }
}

impl Fs for Subdirectory {
    fn local_output_view(&self) -> Option<Arc<dyn Fs>> {
        self.inner
            .local_output_view()
            .map(|inner| subdirectory(inner, &self.prefix.to_string_lossy()))
    }
    fn with_read_observer(&self, observer: Arc<dyn ReadObserver>) -> Option<Arc<dyn Fs>> {
        self.inner
            .with_read_observer(observer)
            .map(|inner| subdirectory(inner, &self.prefix.to_string_lossy()))
    }
    fn immutable_read_view(&self, path: &FsPath) -> FsResult<Option<Arc<dyn Fs>>> {
        self.inner
            .immutable_read_view(&self.path(path)?)
            .map(|view| view.map(|inner| subdirectory(inner, &self.prefix.to_string_lossy())))
    }
    fn host_addressing(&self) -> Option<HostAddressing<'_>> {
        self.host_root.as_deref().map(|root| HostAddressing {
            root,
            anchor: self.anchor.as_deref(),
        })
    }
    fn coordination_key(&self) -> u64 {
        self.inner.coordination_key()
    }
    fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File + '_>> {
        self.inner.open(&self.path(path)?, opts)
    }
    fn open_persistent_handle(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File>> {
        self.inner.open_persistent_handle(&self.path(path)?, opts)
    }
    fn remove_file(&self, path: &FsPath) -> FsResult<()> {
        self.inner.remove_file(&self.path(path)?)
    }
    fn exists(&self, path: &FsPath) -> FsResult<bool> {
        self.inner.exists(&self.path(path)?)
    }
    fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
        self.inner.metadata(&self.path(path)?)
    }
    fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
        self.inner.create_dir_all(&self.path(path)?)
    }
    fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
        self.inner.list_dir(&self.path(path)?)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{
        crash_fs::{CrashFs, CrashMode},
        OpenMode, RealFs,
    };
    use bytes::Bytes;

    #[test]
    fn should_share_crash_barriers_when_worker_uses_subdirectory_view() {
        // Arrange
        let fs = CrashFs::new();
        fs.create_dir_all(&FsPath::new("wal")).unwrap();
        fs.sync_dir(&FsPath::new(""), Durability::Durable).unwrap();
        let view = subdirectory(Arc::new(fs.clone()), "wal");
        let mut file = view
            .open_persistent_handle(
                &FsPath::new("active"),
                OpenOptions {
                    mode: OpenMode::ReadWrite,
                    create: true,
                    create_new: false,
                    truncate: false,
                },
            )
            .unwrap();
        file.append(Bytes::from_static(b"committed")).unwrap();
        file.sync(Durability::Durable).unwrap();
        view.sync_dir(&FsPath::new(""), Durability::Durable)
            .unwrap();
        // Act
        fs.crash(CrashMode::DiscardVolatile);
        // Assert
        assert_eq!(view.metadata(&FsPath::new("active")).unwrap().len, 9);
        assert!(file.len().is_err());
        assert!(view.exists(&FsPath::new("../outside")).is_err());
    }

    #[test]
    fn should_preserve_host_address_frame_when_worker_receives_absolute_output_path() {
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
                OpenOptions {
                    mode: OpenMode::ReadWrite,
                    create: true,
                    create_new: false,
                    truncate: false,
                },
            )
            .unwrap();
        file.append(Bytes::from_static(b"sst")).unwrap();
        // Assert
        assert_eq!(std::fs::read(host_root.join("output")).unwrap(), b"sst");
        assert!(view
            .exists(&FsPath::new(root.path().join("outside").to_string_lossy()))
            .is_err());
    }
}
