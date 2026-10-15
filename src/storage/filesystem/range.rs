use super::{
    fs, FileSystem, Hash, Hasher, Path, StorageCallback, StorageEvent, StorageObjectMetadata,
    StorageOutcome, CONDITIONAL_LOCK_STRIPES, MUTATION_LOCKS,
};

pub(super) fn range_io_error(error: &std::io::Error) -> crate::storage::StorageError {
    if error.kind() == std::io::ErrorKind::NotFound {
        crate::storage::StorageError::not_found(error)
    } else {
        crate::storage::StorageError::io(error)
    }
}

#[cfg(not(windows))]
fn range_metadata(
    metadata: &fs::Metadata,
) -> Result<StorageObjectMetadata, crate::storage::StorageError> {
    if !metadata.is_file() {
        return Err("range reads require an ordinary immutable file".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok(StorageObjectMetadata {
            size: metadata.len(),
            etag: format!(
                "fs:{}:{}:{}:{}:{}:{}",
                metadata.dev(),
                metadata.ino(),
                metadata.mtime(),
                metadata.mtime_nsec(),
                metadata.ctime(),
                metadata.ctime_nsec()
            ),
            generation: None,
        })
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        Err("filesystem backend lacks stable range identity on this platform".into())
    }
}

pub(super) fn range_file_metadata(
    file: &fs::File,
) -> Result<StorageObjectMetadata, crate::storage::StorageError> {
    #[cfg(windows)]
    {
        windows::range_metadata(file)
    }
    #[cfg(not(windows))]
    {
        range_metadata(&file.metadata().map_err(|error| range_io_error(&error))?)
    }
}

pub(super) fn range_path_metadata(
    path: &Path,
) -> Result<StorageObjectMetadata, crate::storage::StorageError> {
    #[cfg(windows)]
    {
        let file = fs::File::open(path).map_err(|error| range_io_error(&error))?;
        range_file_metadata(&file)
    }
    #[cfg(not(windows))]
    {
        range_metadata(&fs::metadata(path).map_err(|error| range_io_error(&error))?)
    }
}

/// Cross-process lock file for `full_path`. Keys share a fixed set of stripe
/// files, so the lock directory stays bounded no matter how many distinct
/// objects are written or deleted; a collision only serializes two keys.
/// Each operation holds at most one stripe, always after its in-process
/// mutex, so sharing a stripe cannot deadlock.
pub(super) fn process_lock_stripe_name(full_path: &Path) -> String {
    let identity = full_path.to_string_lossy();
    let stripe = crc32c::crc32c(identity.as_bytes()) as usize % CONDITIONAL_LOCK_STRIPES;
    format!("stripe-{stripe:02x}.lock")
}

pub(super) fn mutation_stripe(full_path: &Path) -> usize {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    full_path.hash(&mut hasher);
    usize::try_from(hasher.finish()).unwrap_or(0) % CONDITIONAL_LOCK_STRIPES
}

/// Holds a mutation stripe. With failpoints compiled in it also holds the
/// failpoint read gate, taken before the stripe: a failpoint evaluated under
/// the stripe must not wait on the gate while a failpoint test that holds the
/// gate waits on the same stripe for an unrelated key.
pub(super) struct MutationGuard {
    _stripe: parking_lot::MutexGuard<'static, ()>,
    #[cfg(feature = "failpoints")]
    _failpoint_gate: Option<parking_lot::RwLockReadGuard<'static, ()>>,
}

pub(super) fn mutation_lock(full_path: &Path) -> MutationGuard {
    #[cfg(feature = "failpoints")]
    let failpoint_gate = crate::failpoints::read_gate();
    MutationGuard {
        _stripe: MUTATION_LOCKS[mutation_stripe(full_path)].lock(),
        #[cfg(feature = "failpoints")]
        _failpoint_gate: failpoint_gate,
    }
}

impl FileSystem {
    pub(super) fn range_head_within(
        &self,
        key: &str,
        timeout: std::time::Duration,
        callback: &StorageCallback,
    ) {
        let result = (|| {
            if timeout.is_zero() {
                return Err(crate::storage::storage_timeout_error(
                    "range HEAD timed out",
                ));
            }
            let path = self.full_path(key)?;
            let _lock = mutation_lock(&path);
            let _process_lock = self.acquire_process_lock(&path)?;
            range_path_metadata(&path)
        })();
        let result = match result {
            Ok(metadata) => StorageOutcome::Ok(metadata),
            Err(error) => StorageOutcome::Err(error),
        };
        let _ = callback.send(StorageEvent::HeadComplete {
            key: key.to_string(),
            result,
        });
    }

    pub(super) fn read_range_within(
        &self,
        key: &str,
        start: u64,
        end: u64,
        expected: &StorageObjectMetadata,
        timeout: std::time::Duration,
        callback: &crate::storage::RangeReadCallback,
    ) {
        use std::io::{Read, Seek, SeekFrom};
        let result = (|| {
            if timeout.is_zero() {
                return Err(crate::storage::storage_timeout_error(
                    "range read timed out",
                ));
            }
            if start >= end || end > expected.size {
                return Err("invalid remote SST byte range".into());
            }
            let path = self.full_path(key)?;
            let _lock = mutation_lock(&path);
            let _process_lock = self.acquire_process_lock(&path)?;
            let mut file = fs::File::open(&path).map_err(|error| range_io_error(&error))?;
            let metadata = range_file_metadata(&file)?;
            if !metadata.same_version(expected) {
                return Err(crate::storage::StorageError::precondition_failed(
                    "remote SST version changed",
                ));
            }
            let len = usize::try_from(end - start).map_err(|error| error.to_string())?;
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(len)
                .map_err(|error| error.to_string())?;
            bytes.resize(len, 0);
            file.seek(SeekFrom::Start(start))
                .map_err(|error| range_io_error(&error))?;
            file.read_exact(&mut bytes)
                .map_err(|error| range_io_error(&error))?;
            let after = range_file_metadata(&file)?;
            if !after.same_version(expected) {
                return Err(crate::storage::StorageError::precondition_failed(
                    "remote SST changed during range read",
                ));
            }
            Ok(bytes)
        })();
        let _ = callback.send(result);
    }

    pub(super) fn head_within(&self, key: &str, callback: &StorageCallback) {
        let result = self
            .full_path(key)
            .and_then(|path| {
                let _lock = mutation_lock(&path);
                let _process_lock = self.acquire_process_lock(&path)?;
                let bytes = fs::read(&path).map_err(|error| {
                    if error.kind() == std::io::ErrorKind::NotFound {
                        crate::storage::StorageError::not_found(format!(
                            "read {}: {error}",
                            path.display()
                        ))
                    } else {
                        crate::storage::StorageError::io(format!(
                            "read {}: {error}",
                            path.display()
                        ))
                    }
                })?;
                Ok(StorageObjectMetadata::content_crc(
                    bytes.len() as u64,
                    &bytes,
                ))
            })
            .map_or_else(StorageOutcome::Err, StorageOutcome::Ok);
        let _ = callback.send(StorageEvent::HeadComplete {
            key: key.to_string(),
            result,
        });
    }

    pub(super) fn metadata_read_within(
        &self,
        key: &str,
        timeout: std::time::Duration,
        callback: &crate::storage::MetadataReadCallback,
    ) {
        let result = (|| {
            if timeout.is_zero() {
                return Err(crate::storage::storage_timeout_error(
                    "metadata read has no remaining budget",
                ));
            }
            let path = self.full_path(key)?;
            let _lock = mutation_lock(&path);
            let _process_lock = self.acquire_process_lock(&path)?;
            let bytes = fs::read(&path).map_err(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    crate::storage::StorageError::not_found(format!(
                        "read {}: {error}",
                        path.display()
                    ))
                } else {
                    crate::storage::StorageError::io(format!("read {}: {error}", path.display()))
                }
            })?;
            let metadata = StorageObjectMetadata::content_crc(bytes.len() as u64, &bytes);
            Ok((bytes, metadata))
        })();
        let _ = callback.send(result);
    }
}
