//! Immutable remote SST range views. Normal readers use cloud authority;
//! explicit salvage overrides name only startup-verified local copies.

use super::{StorageBackend, StorageEvent, StorageObjectMetadata, StorageOutcome};
use crate::io::traits::{DirEntry, Metadata};
use crate::io::{Durability, File, Fs, FsError, FsPath, FsResult, OpenMode, OpenOptions};
use bytes::Bytes;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

pub(crate) struct RemoteSstFs {
    local: Arc<dyn Fs>,
    cloud: Arc<dyn StorageBackend>,
    timeout: Duration,
    pinned: Option<Arc<RemoteSstFile>>,
    deadline: Option<crate::common::OperationDeadline>,
    verified_local_overrides: Arc<std::collections::HashSet<String>>,
}

struct RemoteSstFile {
    cloud: Arc<dyn StorageBackend>,
    key: String,
    metadata: StorageObjectMetadata,
    timeout: Duration,
    deadline: Option<crate::common::OperationDeadline>,
}

/// A salvage view addresses the exact local file verified during recovery, even
/// when a compaction caller opens it by basename instead of `sst/<basename>`.
struct VerifiedLocalSstFs {
    local: Arc<dyn Fs>,
    path: FsPath,
}

impl RemoteSstFs {
    pub(crate) fn new(
        local: Arc<dyn Fs>,
        cloud: Arc<dyn StorageBackend>,
        timeout: Duration,
    ) -> Self {
        Self {
            local,
            cloud,
            timeout,
            pinned: None,
            deadline: None,
            verified_local_overrides: Arc::default(),
        }
    }

    pub(crate) fn for_object(
        local: Arc<dyn Fs>,
        cloud: Arc<dyn StorageBackend>,
        key: String,
        metadata: StorageObjectMetadata,
        timeout: Duration,
    ) -> Self {
        Self {
            local,
            cloud: Arc::clone(&cloud),
            timeout,
            deadline: None,
            verified_local_overrides: Arc::default(),
            pinned: Some(Arc::new(RemoteSstFile {
                cloud,
                key,
                metadata,
                timeout,
                deadline: None,
            })),
        }
    }

    pub(crate) fn with_deadline(mut self, deadline: crate::common::OperationDeadline) -> Self {
        self.deadline = Some(deadline);
        if let Some(file) = &mut self.pinned {
            Arc::get_mut(file)
                .expect("new remote SST view is exclusively owned")
                .deadline = Some(deadline);
        }
        self
    }

    pub(crate) fn with_verified_local_overrides(
        mut self,
        names: std::collections::HashSet<String>,
    ) -> Self {
        self.verified_local_overrides = Arc::new(names);
        self
    }

    fn remote_file(&self, path: &FsPath) -> FsResult<Arc<RemoteSstFile>> {
        if let Some(file) = &self.pinned {
            return Ok(Arc::clone(file));
        }
        let name = Path::new(&path.0)
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| {
                Path::new(name)
                    .extension()
                    .is_some_and(|extension| extension == "sst")
            })
            .ok_or_else(|| FsError::NotFound(path.0.clone()))?;
        let key = format!("sst/{name}");
        let (tx, rx) = std::sync::mpsc::channel();
        let timeout = self
            .deadline
            .map_or(self.timeout, |deadline| deadline.clamp(self.timeout));
        self.cloud.submit_range_head(&key, timeout, tx);
        let metadata = match rx.recv_timeout(timeout) {
            Ok(StorageEvent::HeadComplete {
                result: StorageOutcome::Ok(metadata),
                ..
            }) => metadata,
            Ok(StorageEvent::HeadComplete {
                result: StorageOutcome::Err(error),
                ..
            }) => return Err(storage_error(error)),
            Ok(event) => {
                return Err(FsError::Io(format!(
                    "unexpected remote SST HEAD: {event:?}"
                )))
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                return Err(FsError::Timeout("remote SST HEAD timed out".into()));
            }
            Err(error) => {
                return Err(FsError::Unavailable(format!(
                    "remote SST HEAD failed: {error}"
                )))
            }
        };
        if !metadata.same_version(&metadata) {
            return Err(FsError::Corruption(
                "remote SST has no stable object identity".into(),
            ));
        }
        Ok(Arc::new(RemoteSstFile {
            cloud: Arc::clone(&self.cloud),
            key,
            metadata,
            timeout: self.timeout,
            deadline: self.deadline,
        }))
    }
}

fn storage_error(error: String) -> FsError {
    if crate::storage::storage_error_is_timeout(&error) {
        FsError::Timeout(error)
    } else if error.starts_with("not found:") {
        FsError::NotFound(error)
    } else if error.starts_with("precondition failed:") {
        FsError::Corruption(error)
    } else {
        FsError::Io(error)
    }
}

fn read_only_error() -> FsError {
    FsError::Unsupported("remote SST views are read only".into())
}

impl File for Arc<RemoteSstFile> {
    fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes> {
        let end = offset
            .checked_add(len)
            .filter(|end| *end <= self.metadata.size)
            .ok_or_else(|| FsError::Corruption("remote SST range exceeds object bounds".into()))?;
        if len == 0 {
            return Ok(Bytes::new());
        }
        let timeout = self
            .deadline
            .map_or(self.timeout, |deadline| deadline.clamp(self.timeout));
        if timeout.is_zero() {
            return Err(FsError::Timeout("remote SST range deadline expired".into()));
        }
        let (tx, rx) = std::sync::mpsc::channel();
        self.cloud
            .submit_read_range(&self.key, offset, end, self.metadata.clone(), timeout, tx);
        let bytes = rx
            .recv_timeout(timeout)
            .map_err(|error| match error {
                std::sync::mpsc::RecvTimeoutError::Timeout => {
                    FsError::Timeout("remote SST range timed out".into())
                }
                std::sync::mpsc::RecvTimeoutError::Disconnected => {
                    FsError::Unavailable("remote SST range callback disconnected".into())
                }
            })?
            .map_err(storage_error)?;
        if u64::try_from(bytes.len()).ok() != Some(len) {
            return Err(FsError::Corruption(
                "remote SST range response length mismatch".into(),
            ));
        }
        Ok(Bytes::from(bytes))
    }
    fn write_at(&mut self, _offset: u64, _data: Bytes) -> FsResult<()> {
        Err(read_only_error())
    }
    fn append(&mut self, _data: Bytes) -> FsResult<u64> {
        Err(read_only_error())
    }
    fn len(&self) -> FsResult<u64> {
        Ok(self.metadata.size)
    }
    fn sync(&mut self, _dur: Durability) -> FsResult<()> {
        Ok(())
    }
    fn close(self: Box<Self>) -> FsResult<()> {
        Ok(())
    }
}

impl Fs for RemoteSstFs {
    fn coordination_key(&self) -> u64 {
        self.local.coordination_key()
    }
    fn immutable_read_view(&self, path: &FsPath) -> FsResult<Option<Arc<dyn Fs>>> {
        if self.pinned.is_none() {
            if let Some(name) = Path::new(&path.0)
                .file_name()
                .and_then(|name| name.to_str())
            {
                if self.verified_local_overrides.contains(name) {
                    return Ok(Some(Arc::new(VerifiedLocalSstFs {
                        local: Arc::clone(&self.local),
                        path: FsPath::new(format!("sst/{name}")),
                    })));
                }
            }
        }
        Ok(Some(Arc::new(Self {
            local: Arc::clone(&self.local),
            cloud: Arc::clone(&self.cloud),
            timeout: self.timeout,
            deadline: self.deadline,
            verified_local_overrides: Arc::clone(&self.verified_local_overrides),
            pinned: Some(self.remote_file(path)?),
        })))
    }
    fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File + '_>> {
        if self.pinned.is_none() {
            match self.local.open(path, opts) {
                Ok(file) => return Ok(file),
                Err(FsError::NotFound(_)) if opts.mode == OpenMode::ReadOnly && !opts.create => {}
                Err(error) => return Err(error),
            }
        }
        if opts.mode != OpenMode::ReadOnly || opts.create || opts.create_new || opts.truncate {
            return Err(read_only_error());
        }
        Ok(Box::new(self.remote_file(path)?))
    }
    fn open_persistent_handle(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File>> {
        if self.pinned.is_none() && self.local.exists(path)? {
            return self.local.open_persistent_handle(path, opts);
        }
        if opts.mode != OpenMode::ReadOnly || opts.create || opts.create_new || opts.truncate {
            return Err(read_only_error());
        }
        Ok(Box::new(self.remote_file(path)?))
    }
    fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
        if self.pinned.is_none() {
            match self.local.metadata(path) {
                Ok(metadata) => return Ok(metadata),
                Err(FsError::NotFound(_)) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(Metadata {
            len: self.remote_file(path)?.metadata.size,
        })
    }
    fn exists(&self, path: &FsPath) -> FsResult<bool> {
        match self.metadata(path) {
            Ok(_) => Ok(true),
            Err(FsError::NotFound(_)) => Ok(false),
            Err(error) => Err(error),
        }
    }
    fn remove_file(&self, path: &FsPath) -> FsResult<()> {
        self.local.remove_file(path)
    }
    fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
        self.local.create_dir_all(path)
    }
    fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
        self.local.list_dir(path)
    }
    fn remove_dir_all(&self, path: &FsPath) -> FsResult<()> {
        self.local.remove_dir_all(path)
    }
    fn sync_dir(&self, path: &FsPath, dur: Durability) -> FsResult<()> {
        self.local.sync_dir(path, dur)
    }
    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        self.local.rename_atomic(from, to)
    }
}

impl Fs for VerifiedLocalSstFs {
    fn coordination_key(&self) -> u64 {
        self.local.coordination_key()
    }
    fn open(&self, _path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File + '_>> {
        if opts.mode != OpenMode::ReadOnly || opts.create || opts.create_new || opts.truncate {
            return Err(read_only_error());
        }
        self.local.open(&self.path, opts)
    }
    fn open_persistent_handle(&self, _path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File>> {
        if opts.mode != OpenMode::ReadOnly || opts.create || opts.create_new || opts.truncate {
            return Err(read_only_error());
        }
        self.local.open_persistent_handle(&self.path, opts)
    }
    fn metadata(&self, _path: &FsPath) -> FsResult<Metadata> {
        self.local.metadata(&self.path)
    }
    fn exists(&self, _path: &FsPath) -> FsResult<bool> {
        self.local.exists(&self.path)
    }
    fn remove_file(&self, _path: &FsPath) -> FsResult<()> {
        Err(read_only_error())
    }
    fn create_dir_all(&self, _path: &FsPath) -> FsResult<()> {
        Err(read_only_error())
    }
    fn list_dir(&self, _path: &FsPath) -> FsResult<Vec<DirEntry>> {
        Err(read_only_error())
    }
    fn remove_dir_all(&self, _path: &FsPath) -> FsResult<()> {
        Err(read_only_error())
    }
    fn sync_dir(&self, _path: &FsPath, _dur: Durability) -> FsResult<()> {
        Err(read_only_error())
    }
    fn rename_atomic(&self, _from: &FsPath, _to: &FsPath) -> FsResult<()> {
        Err(read_only_error())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::traits::{SstFactory, SstStateReader};

    struct RecordingBackend {
        inner: super::super::filesystem::FileSystem,
        ranges: std::sync::Mutex<Vec<(u64, u64)>>,
    }

    impl StorageBackend for RecordingBackend {
        fn submit_read(&self, _key: &str, _callback: super::super::StorageCallback) {
            panic!("whole SST read is forbidden");
        }
        fn submit_write(&self, key: &str, bytes: Vec<u8>, callback: super::super::StorageCallback) {
            self.inner.submit_write(key, bytes, callback);
        }
        fn submit_delete(&self, key: &str, callback: super::super::StorageCallback) {
            self.inner.submit_delete(key, callback);
        }
        fn submit_list(&self, prefix: &str, callback: super::super::StorageCallback) {
            self.inner.submit_list(prefix, callback);
        }
        fn submit_range_head(
            &self,
            key: &str,
            timeout: Duration,
            callback: super::super::StorageCallback,
        ) {
            self.inner.submit_range_head(key, timeout, callback);
        }
        fn submit_read_range(
            &self,
            key: &str,
            start: u64,
            end: u64,
            expected: StorageObjectMetadata,
            timeout: Duration,
            callback: super::super::RangeReadCallback,
        ) {
            self.ranges
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push((start, end));
            self.inner
                .submit_read_range(key, start, end, expected, timeout, callback);
        }
    }

    #[test]
    fn should_use_only_verified_salvage_file_when_reader_opens_by_basename(
    ) -> crate::common::MidgeResult<()> {
        // Arrange
        let local = tempfile::tempdir()?;
        let remote = tempfile::tempdir()?;
        let local_fs = Arc::new(crate::io::RealFs::new(local.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(local_fs.clone(), 4096);
        let mut writer = factory.create()?;
        writer.add_with_meta(b"key", Some(b"verified"), 9, 0, None)?;
        let bytes = writer.finish_bytes()?;
        std::fs::create_dir_all(local.path().join("sst"))?;
        std::fs::write(local.path().join("sst/allowed.sst"), &bytes)?;
        std::fs::write(local.path().join("sst/other.sst"), &bytes)?;
        std::fs::write(
            local.path().join("allowed.sst"),
            b"wrong same-name local file",
        )?;
        let fs = Arc::new(
            RemoteSstFs::new(
                local_fs,
                Arc::new(super::super::filesystem::FileSystem::new(remote.path())?),
                Duration::from_secs(5),
            )
            .with_verified_local_overrides(std::collections::HashSet::from(["allowed.sst".into()])),
        );
        // Act
        let reader = crate::sst::fs::SstFileIo::open("allowed.sst", fs.clone())?;
        let value = reader.get_state_at(b"key", 9)?;
        let unverified = crate::sst::fs::SstFileIo::open("sst/other.sst", fs);
        // Assert
        assert!(
            matches!(value, crate::sst::types::KeyState::Value(bytes, 9, None, _) if bytes.as_ref() == b"verified")
        );
        assert!(
            unverified.is_err(),
            "unlisted local files must use remote authority"
        );
        Ok(())
    }

    #[test]
    fn should_stream_remote_sst_via_bounded_ranges_when_local_cache_is_corrupt(
    ) -> crate::common::MidgeResult<()> {
        // Arrange
        let local = tempfile::tempdir()?;
        let remote = tempfile::tempdir()?;
        let cloud = Arc::new(RecordingBackend {
            inner: super::super::filesystem::FileSystem::new(remote.path())?,
            ranges: std::sync::Mutex::new(Vec::new()),
        });
        let local_fs = Arc::new(crate::io::RealFs::new(local.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(local_fs.clone(), 4096);
        let mut writer = factory.create()?;
        for index in 0_u32..1024 {
            let mut value = vec![0; 1024];
            for (offset, byte) in value.iter_mut().enumerate() {
                *byte = u8::try_from(offset.wrapping_mul(73).wrapping_add(index as usize) % 251)
                    .expect("remainder fits u8");
            }
            writer.add_with_meta(&index.to_be_bytes(), Some(&value), 9, 0, None)?;
        }
        let bytes = writer.finish_bytes()?;
        let size = bytes.len() as u64;
        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_write("sst/remote.sst", bytes, tx);
        rx.recv().unwrap();
        std::fs::write(local.path().join("remote.sst"), b"corrupt local cache")?;
        let fs = Arc::new(RemoteSstFs::new(
            local_fs,
            cloud.clone(),
            Duration::from_secs(5),
        ));
        // Act
        let reader = crate::sst::fs::SstFileIo::open("remote.sst", fs)?;
        let open_bytes: u64 = cloud
            .ranges
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(start, end)| end - start)
            .sum();
        let state = reader.get_state_at(&500_u32.to_be_bytes(), 9)?;
        let count = Box::new(reader)
            .raw_version_cursor(None, None)?
            .collect::<crate::common::MidgeResult<Vec<_>>>()?
            .len();
        // Assert
        assert!(
            open_bytes < size / 4,
            "opening read {open_bytes} bytes from {size}-byte object"
        );
        assert!(matches!(
            state,
            crate::sst::types::KeyState::Value(_, 9, None, _)
        ));
        assert_eq!(count, 1024);
        assert!(cloud
            .ranges
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .all(|(start, end)| end - start < size / 4));
        assert_eq!(
            std::fs::read(local.path().join("remote.sst"))?,
            b"corrupt local cache"
        );
        Ok(())
    }

    #[test]
    fn should_fail_pinned_remote_range_when_object_replaced_with_same_length(
    ) -> crate::common::MidgeResult<()> {
        // Arrange
        let local = tempfile::tempdir()?;
        let remote = tempfile::tempdir()?;
        let cloud = Arc::new(super::super::filesystem::FileSystem::new(remote.path())?);
        std::fs::create_dir_all(remote.path().join("sst"))?;
        std::fs::write(remote.path().join("sst/remote.sst"), b"old bytes")?;
        let fs = RemoteSstFs::new(
            Arc::new(crate::io::RealFs::new(local.path())?),
            cloud,
            Duration::from_secs(5),
        );
        let path = FsPath::new("remote.sst");
        let pinned = fs.immutable_read_view(&path)?.unwrap();
        let opts = OpenOptions {
            mode: OpenMode::ReadOnly,
            create: false,
            create_new: false,
            truncate: false,
        };
        let file = pinned.open(&path, opts)?;
        assert_eq!(file.read_at(0, 3)?.as_ref(), b"old");
        // Act
        std::fs::write(remote.path().join("replacement"), b"new bytes")?;
        std::fs::rename(
            remote.path().join("replacement"),
            remote.path().join("sst/remote.sst"),
        )?;
        let result = file.read_at(0, 3);
        // Assert
        assert!(matches!(result, Err(FsError::Corruption(_))));
        Ok(())
    }

    #[test]
    fn should_read_remote_sst_without_creating_local_file() -> crate::common::MidgeResult<()> {
        // Arrange
        let local = tempfile::tempdir()?;
        let remote = tempfile::tempdir()?;
        let cloud = Arc::new(super::super::filesystem::FileSystem::new(remote.path())?);
        let fs = Arc::new(crate::io::RealFs::new(local.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs.clone(), 4096);
        let mut writer = factory.create()?;
        writer.add_with_meta(b"key", Some(b"value"), 9, 0, None)?;
        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_write("sst/remote.sst", writer.finish_bytes()?, tx);
        rx.recv().expect("write completion");
        let ranges = Arc::new(RemoteSstFs::new(fs, cloud, Duration::from_secs(5)));
        // Act
        let reader = crate::sst::fs::SstFileIo::open("remote.sst", ranges)?;
        let actual = reader.get_state_at(b"key", 9)?;
        // Assert
        assert!(
            matches!(actual, crate::sst::types::KeyState::Value(value, 9, None, _) if value.as_ref() == b"value")
        );
        assert!(!local.path().join("remote.sst").exists());
        Ok(())
    }
}
