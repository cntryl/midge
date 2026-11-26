//! Immutable remote SST range views. Normal readers use cloud authority;
//! explicit salvage overrides name only startup-verified local copies.

use super::remote_block_cache::RemoteBlockCache;
use super::{StorageBackend, StorageEvent, StorageObjectMetadata, StorageOutcome};
use bytes::Bytes;
use cntryl_midge_core::io::traits::{DirEntry, Metadata};
use cntryl_midge_core::io::{
    Durability, File, Fs, FsError, FsPath, FsResult, HostAddressing, OpenMode, OpenOptions, ReadFs,
};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

pub struct RemoteSstFs {
    local: Arc<dyn Fs>,
    cloud: Arc<dyn StorageBackend>,
    timeout: Duration,
    pinned: Option<Arc<RemoteSstFile>>,
    deadline: Option<cntryl_midge_core::common::OperationDeadline>,
    verified_local_overrides: Arc<std::collections::HashSet<String>>,
    observer: Option<Arc<dyn cntryl_midge_core::io::traits::ReadObserver>>,
    /// Local disk cache of fetched ranges, shared by every view (#938).
    cache: Option<Arc<RemoteBlockCache>>,
}

#[derive(Clone)]
struct RemoteSstFile {
    cloud: Arc<dyn StorageBackend>,
    key: String,
    metadata: StorageObjectMetadata,
    timeout: Duration,
    deadline: Option<cntryl_midge_core::common::OperationDeadline>,
    observer: Option<Arc<dyn cntryl_midge_core::io::traits::ReadObserver>>,
    cache: Option<Arc<RemoteBlockCache>>,
}

/// Bytes fetched when a read touches the object's tail: the footer and, for
/// most SSTs, every metadata block behind it, so a reader open costs one range
/// read instead of one per block (#938).
const TAIL_PREFETCH_BYTES: u64 = 64 * 1024;

impl RemoteSstFile {
    /// The cache key for this pinned object version. Identity includes the
    /// provider's version so a replaced object never serves a stale range.
    fn cache_identity(&self) -> String {
        format!(
            "{}|{}|{}",
            self.key,
            self.metadata.etag,
            self.metadata.generation.as_deref().unwrap_or("")
        )
    }
}

struct RangeObservation<'a> {
    observer: Option<&'a dyn cntryl_midge_core::io::traits::ReadObserver>,
    started: std::time::Instant,
    returned_bytes: u64,
    failed: bool,
}

impl Drop for RangeObservation<'_> {
    fn drop(&mut self) {
        if let Some(observer) = self.observer {
            observer.remote_range_completed(
                self.returned_bytes,
                self.started.elapsed(),
                self.failed,
            );
        }
    }
}

/// A salvage view addresses the exact local file verified during recovery, even
/// when a compaction caller opens it by basename instead of `sst/<basename>`.
struct VerifiedLocalSstFs {
    local: Arc<dyn Fs>,
    path: FsPath,
}

impl RemoteSstFs {
    pub fn new(local: Arc<dyn Fs>, cloud: Arc<dyn StorageBackend>, timeout: Duration) -> Self {
        Self {
            local,
            cloud,
            timeout,
            pinned: None,
            deadline: None,
            verified_local_overrides: Arc::default(),
            observer: None,
            cache: None,
        }
    }

    pub fn for_object(
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
            observer: None,
            cache: None,
            pinned: Some(Arc::new(RemoteSstFile {
                cloud,
                key,
                metadata,
                timeout,
                deadline: None,
                observer: None,
                cache: None,
            })),
        }
    }

    /// Serve fetched ranges from, and record them into, a local disk cache.
    #[must_use]
    pub fn with_block_cache(mut self, cache: Arc<RemoteBlockCache>) -> Self {
        if let Some(file) = &mut self.pinned {
            Arc::get_mut(file)
                .expect("new remote SST view is exclusively owned")
                .cache = Some(Arc::clone(&cache));
        }
        self.cache = Some(cache);
        self
    }

    #[must_use]
    pub fn with_deadline(mut self, deadline: cntryl_midge_core::common::OperationDeadline) -> Self {
        self.deadline = Some(deadline);
        if let Some(file) = &mut self.pinned {
            Arc::get_mut(file)
                .expect("new remote SST view is exclusively owned")
                .deadline = Some(deadline);
        }
        self
    }

    #[must_use]
    pub fn with_verified_local_overrides(
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
        self.cloud.submit_range_head_request(
            super::StorageRequest::new(
                &key,
                self.deadline.unwrap_or_else(|| {
                    cntryl_midge_core::common::OperationDeadline::from_budget(timeout)
                }),
                timeout,
            ),
            tx,
        );
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
            observer: self.observer.clone(),
            cache: self.cache.clone(),
        }))
    }
}

fn storage_error(error: crate::StorageError) -> FsError {
    use crate::StorageErrorKind;
    let kind = error.kind();
    let message = error.into_message();
    match kind {
        StorageErrorKind::Timeout => FsError::Timeout(message),
        StorageErrorKind::NotFound => FsError::NotFound(message),
        StorageErrorKind::PreconditionFailed | StorageErrorKind::Corruption => {
            FsError::Corruption(message)
        }
        StorageErrorKind::Unauthorized
        | StorageErrorKind::Transport
        | StorageErrorKind::Protocol
        | StorageErrorKind::Io
        | StorageErrorKind::ResourceLimit => FsError::Io(message),
    }
}

fn immutable_sst_error() -> FsError {
    FsError::Unsupported("remote SST views are read only".into())
}

struct RemoteSstHandle(Arc<RemoteSstFile>);

impl cntryl_midge_core::io::ReadFile for RemoteSstHandle {
    fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes> {
        let file = &self.0;
        let end = offset
            .checked_add(len)
            .filter(|end| *end <= file.metadata.size)
            .ok_or_else(|| FsError::Corruption("remote SST range exceeds object bounds".into()))?;
        if len == 0 {
            return Ok(Bytes::new());
        }
        let identity = file.cache.as_ref().map(|_| file.cache_identity());
        if let (Some(cache), Some(identity)) = (&file.cache, &identity) {
            if let Some(bytes) = cache.get(identity, offset, len) {
                return Ok(bytes);
            }
        }
        // A read that reaches the object's end is the footer: with a cache,
        // fetch the whole tail so the metadata reads that follow are hits.
        let fetch_offset = if identity.is_some() && end == file.metadata.size {
            end.saturating_sub(TAIL_PREFETCH_BYTES).min(offset)
        } else {
            offset
        };
        let timeout = file
            .deadline
            .map_or(file.timeout, |deadline| deadline.clamp(file.timeout));
        if timeout.is_zero() {
            return Err(FsError::Timeout("remote SST range deadline expired".into()));
        }
        let (tx, rx) = std::sync::mpsc::channel();
        let mut observation = RangeObservation {
            observer: file.observer.as_deref(),
            started: std::time::Instant::now(),
            returned_bytes: 0,
            failed: true,
        };
        if let Some(observer) = observation.observer {
            observer.remote_range_started();
        }
        file.cloud.submit_range_read_request(
            super::StorageRequest::new(
                &file.key,
                file.deadline.unwrap_or_else(|| {
                    cntryl_midge_core::common::OperationDeadline::from_budget(timeout)
                }),
                timeout,
            )
            .with_precondition(super::StoragePrecondition::IfMatch(file.metadata.clone())),
            fetch_offset..end,
            tx,
        );
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
        observation.returned_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if u64::try_from(bytes.len()).ok() != Some(end - fetch_offset) {
            return Err(FsError::Corruption(
                "remote SST range response length mismatch".into(),
            ));
        }
        observation.failed = false;
        let fetched = Bytes::from(bytes);
        if let (Some(cache), Some(identity)) = (&file.cache, &identity) {
            cache.insert(identity, fetch_offset, &fetched);
        }
        let start = usize::try_from(offset - fetch_offset)
            .map_err(|_| FsError::Corruption("remote SST range offset overflow".into()))?;
        let stop = usize::try_from(end - fetch_offset)
            .map_err(|_| FsError::Corruption("remote SST range offset overflow".into()))?;
        Ok(fetched.slice(start..stop))
    }

    fn len(&self) -> FsResult<u64> {
        let file = &self.0;
        Ok(file.metadata.size)
    }
}

impl File for RemoteSstHandle {
    fn write_at(&mut self, _offset: u64, _data: Bytes) -> FsResult<()> {
        Err(immutable_sst_error())
    }
    fn append(&mut self, _data: Bytes) -> FsResult<u64> {
        Err(immutable_sst_error())
    }

    fn sync(&mut self, _dur: Durability) -> FsResult<()> {
        Ok(())
    }
}

impl ReadFs for RemoteSstFs {
    fn open_read(&self, path: &FsPath) -> FsResult<Box<dyn cntryl_midge_core::io::ReadFile + '_>> {
        self.open(path, OpenOptions::READ_ONLY)
            .map(|file| file as Box<dyn cntryl_midge_core::io::ReadFile>)
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
            is_dir: false,
        })
    }

    fn exists(&self, path: &FsPath) -> FsResult<bool> {
        match self.metadata(path) {
            Ok(_) => Ok(true),
            Err(FsError::NotFound(_)) => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
        self.local.list_dir(path)
    }
}

impl Fs for RemoteSstFs {
    fn try_lock_exclusive(
        &self,
        path: &FsPath,
    ) -> FsResult<Box<dyn cntryl_midge_core::io::FsLock>> {
        self.local.try_lock_exclusive(path)
    }

    fn persistent_handle_capability(
        &self,
    ) -> Option<&dyn cntryl_midge_core::io::traits::PersistentHandleFs> {
        Some(self)
    }

    fn local_output_view(&self) -> Option<Arc<dyn Fs>> {
        Some(Arc::clone(&self.local))
    }

    fn with_read_observer(
        &self,
        observer: Arc<dyn cntryl_midge_core::io::traits::ReadObserver>,
    ) -> Option<Arc<dyn Fs>> {
        Some(Arc::new(Self {
            local: Arc::clone(&self.local),
            cloud: Arc::clone(&self.cloud),
            timeout: self.timeout,
            deadline: self.deadline,
            verified_local_overrides: Arc::clone(&self.verified_local_overrides),
            pinned: self.pinned.as_ref().map(|file| {
                Arc::new(RemoteSstFile {
                    observer: Some(Arc::clone(&observer)),
                    ..file.as_ref().clone()
                })
            }),
            observer: Some(observer),
            cache: self.cache.clone(),
        }))
    }

    fn host_addressing(&self) -> Option<HostAddressing<'_>> {
        // Writes and directory operations forward to `local`, so callers must
        // address this filesystem through the same host-path frame.
        self.local.host_addressing()
    }
    fn coordination_key(&self) -> u64 {
        self.local.coordination_key()
    }
    fn immutable_read_view(&self, path: &FsPath) -> FsResult<Option<Arc<dyn Fs>>> {
        if self.pinned.is_none() {
            // Same-level repair runs are local, non-authoritative inputs. The
            // generic SST factory asks for an immutable view before opening a
            // compaction cursor; pinning the remote basename here would look
            // up an object that must never have been uploaded.
            if Path::new(&path.0)
                .components()
                .any(|component| component.as_os_str() == ".compaction-repair")
            {
                return Ok(Some(Arc::clone(&self.local)));
            }
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
            observer: self.observer.clone(),
            cache: self.cache.clone(),
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
            return Err(immutable_sst_error());
        }
        Ok(Box::new(RemoteSstHandle(self.remote_file(path)?)))
    }
    fn remove_file(&self, path: &FsPath) -> FsResult<()> {
        self.local.remove_file(path)
    }
    fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
        self.local.create_dir_all(path)
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

impl cntryl_midge_core::io::traits::PersistentHandleFs for RemoteSstFs {
    fn open_persistent(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File>> {
        if self.pinned.is_none() && self.local.exists(path)? {
            return cntryl_midge_core::io::open_persistent_handle(self.local.as_ref(), path, opts);
        }
        if opts.mode != OpenMode::ReadOnly || opts.create || opts.create_new || opts.truncate {
            return Err(immutable_sst_error());
        }
        Ok(Box::new(RemoteSstHandle(self.remote_file(path)?)))
    }
}

impl ReadFs for VerifiedLocalSstFs {
    fn open_read(&self, path: &FsPath) -> FsResult<Box<dyn cntryl_midge_core::io::ReadFile + '_>> {
        self.open(path, OpenOptions::READ_ONLY)
            .map(|file| file as Box<dyn cntryl_midge_core::io::ReadFile>)
    }

    fn metadata(&self, _path: &FsPath) -> FsResult<Metadata> {
        self.local.metadata(&self.path)
    }

    fn exists(&self, _path: &FsPath) -> FsResult<bool> {
        self.local.exists(&self.path)
    }

    fn list_dir(&self, _path: &FsPath) -> FsResult<Vec<DirEntry>> {
        Err(immutable_sst_error())
    }
}

impl Fs for VerifiedLocalSstFs {
    fn persistent_handle_capability(
        &self,
    ) -> Option<&dyn cntryl_midge_core::io::traits::PersistentHandleFs> {
        self.local
            .persistent_handle_capability()
            .map(|_| self as &dyn cntryl_midge_core::io::traits::PersistentHandleFs)
    }

    fn coordination_key(&self) -> u64 {
        self.local.coordination_key()
    }
    fn open(&self, _path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File + '_>> {
        if opts.mode != OpenMode::ReadOnly || opts.create || opts.create_new || opts.truncate {
            return Err(immutable_sst_error());
        }
        self.local.open(&self.path, opts)
    }
    fn remove_file(&self, _path: &FsPath) -> FsResult<()> {
        Err(immutable_sst_error())
    }
    fn create_dir_all(&self, _path: &FsPath) -> FsResult<()> {
        Err(immutable_sst_error())
    }
    fn remove_dir_all(&self, _path: &FsPath) -> FsResult<()> {
        Err(immutable_sst_error())
    }
    fn sync_dir(&self, _path: &FsPath, _dur: Durability) -> FsResult<()> {
        Err(immutable_sst_error())
    }
    fn rename_atomic(&self, _from: &FsPath, _to: &FsPath) -> FsResult<()> {
        Err(immutable_sst_error())
    }
}

impl cntryl_midge_core::io::traits::PersistentHandleFs for VerifiedLocalSstFs {
    fn open_persistent(&self, _path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File>> {
        if opts.mode != OpenMode::ReadOnly || opts.create || opts.create_new || opts.truncate {
            return Err(immutable_sst_error());
        }
        cntryl_midge_core::io::open_persistent_handle(self.local.as_ref(), &self.path, opts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_preserve_missing_owned_handle_support_when_verified_local_file_is_wrapped() {
        // Arrange
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("verified.sst"), b"verified").unwrap();
        let local: Arc<dyn Fs> = Arc::new(cntryl_midge_core::io::transient_read::TransientReadFs {
            inner: cntryl_midge_core::io::RealFs::new(directory.path()).unwrap(),
            fail_from: u64::MAX,
        });
        let view = VerifiedLocalSstFs {
            local,
            path: FsPath::new("verified.sst"),
        };

        // Act
        let file = view.open_read(&FsPath::new("sst/verified.sst")).unwrap();
        let bytes = file.read_at(0, 8).unwrap();

        // Assert
        assert_eq!(bytes, Bytes::from_static(b"verified"));
        assert!(view.persistent_handle_capability().is_none());
        assert!(matches!(
            cntryl_midge_core::io::open_persistent_handle(
                &view,
                &FsPath::new("sst/verified.sst"),
                OpenOptions::READ_ONLY
            ),
            Err(FsError::Unsupported(_))
        ));
    }

    #[test]
    fn should_fail_pinned_remote_range_when_object_replaced_with_same_length(
    ) -> cntryl_midge_core::common::MidgeResult<()> {
        // Arrange
        let local = tempfile::tempdir()?;
        let remote = tempfile::tempdir()?;
        let cloud = Arc::new(super::super::filesystem::FileSystem::new(remote.path())?);
        std::fs::create_dir_all(remote.path().join("sst"))?;
        std::fs::write(remote.path().join("sst/remote.sst"), b"old bytes")?;
        let fs = RemoteSstFs::new(
            Arc::new(
                cntryl_midge_core::io::RealFs::new(local.path()).map_err(FsError::into_midge)?,
            ),
            cloud,
            Duration::from_secs(5),
        );
        let path = FsPath::new("remote.sst");
        let pinned = fs
            .immutable_read_view(&path)
            .map_err(FsError::into_midge)?
            .unwrap();
        let opts = OpenOptions {
            mode: OpenMode::ReadOnly,
            create: false,
            create_new: false,
            truncate: false,
        };
        let file = pinned.open(&path, opts).map_err(FsError::into_midge)?;
        assert_eq!(
            file.read_at(0, 3).map_err(FsError::into_midge)?.as_ref(),
            b"old"
        );
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
}
