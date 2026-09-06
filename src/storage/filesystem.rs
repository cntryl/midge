//! Filesystem storage backend implementing `StorageBackend` trait (hot path).
//!
//! Provides synchronous local filesystem storage via callback-based operations.
//! Executes immediately but conforms to the async-compatible `StorageBackend` trait.
//!
//! **On the hot path** for:
//! - Local SST cache reads/writes
//! - WAL segment fallback (before cloud upload)
//! - Test backends via `HybridStorage`
//!
//! Design is callback-driven to integrate with `CloudExecutor` and avoid blocking
//! the main engine thread.

use crate::common::MidgeResult;
use crate::storage::{
    StorageBackend, StorageCallback, StorageEvent, StorageObjectMetadata, StorageOutcome,
};
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::Write;
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::path::{Component, Path, PathBuf};
use std::sync::LazyLock;

#[cfg(test)]
mod range_identity_tests;
#[cfg(windows)]
mod windows;

const CONDITIONAL_LOCK_STRIPES: usize = 64;
static MUTATION_LOCKS: LazyLock<Vec<parking_lot::Mutex<()>>> = LazyLock::new(|| {
    (0..CONDITIONAL_LOCK_STRIPES)
        .map(|_| parking_lot::Mutex::new(()))
        .collect()
});

/// Cross-process companion to the in-process stripe lock. Conditional
/// `If-Match` operations must serialize against ordinary writers too; a
/// process-local mutex alone cannot provide that guarantee.
#[cfg(unix)]
struct ProcessMutationLock {
    file: std::fs::File,
}

#[cfg(unix)]
impl Drop for ProcessMutationLock {
    fn drop(&mut self) {
        // SAFETY: `file` remains open for the lifetime of the lock and the
        // descriptor was successfully opened by `acquire_process_lock`.
        let _ = unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
    }
}

#[cfg(windows)]
struct ProcessMutationLock {
    file: std::fs::File,
}

#[cfg(windows)]
impl Drop for ProcessMutationLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

#[cfg(not(any(unix, windows)))]
struct ProcessMutationLock;

/// Filesystem-based storage backend
///
/// Implements `StorageBackend` synchronously. Suitable for local file storage.
/// All operations execute immediately and send completion events via callback.
pub struct FileSystem {
    base_path: PathBuf,
}

impl FileSystem {
    /// Create a new filesystem storage backend.
    pub fn new<P: AsRef<Path>>(base_path: P) -> MidgeResult<Self> {
        let path = base_path.as_ref().to_path_buf();
        fs::create_dir_all(&path)?; // Ensure base dir exists
        Ok(Self {
            base_path: fs::canonicalize(path)?,
        })
    }

    /// Compute a sanitized full path for a given key.
    fn full_path(&self, key: &str) -> Result<PathBuf, String> {
        // Prevent absolute paths or path traversal outside the base directory.
        // Treat the key as a relative, forward-slash-friendly path.
        let mut out = self.base_path.clone();
        for component in Path::new(key).components() {
            match component {
                Component::Normal(part) => out.push(part),
                Component::CurDir
                | Component::ParentDir
                | Component::RootDir
                | Component::Prefix(_) => {}
            }
        }
        let relative = out
            .strip_prefix(&self.base_path)
            .map_err(|error| format!("storage path escaped base directory: {error}"))?;
        let mut current = self.base_path.clone();
        for component in relative.components() {
            let Component::Normal(part) = component else {
                continue;
            };
            current.push(part);
            match fs::symlink_metadata(&current) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    return Err(format!(
                        "storage path contains a symlink: {}",
                        current.display()
                    ));
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
                Err(error) => {
                    return Err(format!(
                        "inspect storage path component {}: {error}",
                        current.display()
                    ));
                }
            }
        }
        Ok(out)
    }

    fn acquire_process_lock(&self, full_path: &Path) -> Result<ProcessMutationLock, String> {
        #[cfg(unix)]
        {
            let lock_dir = self.base_path.join(".midge-locks");
            fs::create_dir_all(&lock_dir).map_err(|error| {
                format!("create lock directory {}: {error}", lock_dir.display())
            })?;
            let identity = full_path.to_string_lossy();
            let lock_name = format!("{:08x}.lock", crc32c::crc32c(identity.as_bytes()));
            let file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(lock_dir.join(lock_name))
                .map_err(|error| format!("open conditional mutation lock: {error}"))?;
            // SAFETY: `file` is a valid open descriptor. `LOCK_EX` requests an
            // advisory exclusive lock which is released in `Drop` above.
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } != 0 {
                return Err(format!(
                    "acquire conditional mutation lock: {}",
                    std::io::Error::last_os_error()
                ));
            }
            Ok(ProcessMutationLock { file })
        }

        #[cfg(windows)]
        {
            let lock_dir = self.base_path.join(".midge-locks");
            fs::create_dir_all(&lock_dir).map_err(|error| {
                format!("create lock directory {}: {error}", lock_dir.display())
            })?;
            let identity = full_path.to_string_lossy();
            let lock_name = format!("{:08x}.lock", crc32c::crc32c(identity.as_bytes()));
            let file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(lock_dir.join(lock_name))
                .map_err(|error| format!("open conditional mutation lock: {error}"))?;
            file.lock()
                .map_err(|error| format!("acquire conditional mutation lock: {error}"))?;
            Ok(ProcessMutationLock { file })
        }

        #[cfg(not(any(unix, windows)))]
        {
            let _ = full_path;
            let lock_dir = self.base_path.join(".midge-locks");
            fs::create_dir_all(&lock_dir).map_err(|error| {
                format!("create lock directory {}: {error}", lock_dir.display())
            })?;
            Ok(ProcessMutationLock)
        }
    }
}

fn write_file_with_parents(full_path: &Path, data: Vec<u8>) -> StorageOutcome<()> {
    if let Some(parent) = full_path.parent() {
        if let Err(e) = fs::create_dir_all(parent) {
            return StorageOutcome::Err(format!("mkdir {}: {e}", parent.display()));
        }
    }

    match fs::write(full_path, data) {
        Ok(()) => StorageOutcome::Ok(()),
        Err(e) => StorageOutcome::Err(format!("write {}: {e}", full_path.display())),
    }
}

fn range_io_error(error: &std::io::Error) -> String {
    if error.kind() == std::io::ErrorKind::NotFound {
        format!("not found: {error}")
    } else {
        error.to_string()
    }
}

#[cfg(not(windows))]
fn range_metadata(metadata: &fs::Metadata) -> Result<StorageObjectMetadata, String> {
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

fn range_file_metadata(file: &fs::File) -> Result<StorageObjectMetadata, String> {
    #[cfg(windows)]
    {
        windows::range_metadata(file)
    }
    #[cfg(not(windows))]
    {
        range_metadata(&file.metadata().map_err(|error| range_io_error(&error))?)
    }
}

fn range_path_metadata(path: &Path) -> Result<StorageObjectMetadata, String> {
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

fn mutation_lock(full_path: &Path) -> parking_lot::MutexGuard<'static, ()> {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    full_path.hash(&mut hasher);
    let stripe = usize::try_from(hasher.finish()).unwrap_or(0) % CONDITIONAL_LOCK_STRIPES;
    MUTATION_LOCKS[stripe].lock()
}

fn create_file_new_with_parents(full_path: &Path, data: &[u8]) -> StorageOutcome<()> {
    if let Some(parent) = full_path.parent() {
        if let Err(error) = fs::create_dir_all(parent) {
            return StorageOutcome::Err(format!("mkdir {}: {error}", parent.display()));
        }
    }

    let mut file = match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(full_path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            return StorageOutcome::Err("precondition failed: object already exists".to_string());
        }
        Err(error) => {
            return StorageOutcome::Err(format!("create {}: {error}", full_path.display()));
        }
    };

    if let Err(error) = file.write_all(data).and_then(|()| file.sync_all()) {
        let _ = fs::remove_file(full_path);
        return StorageOutcome::Err(format!("write {}: {error}", full_path.display()));
    }
    StorageOutcome::Ok(())
}

impl StorageBackend for FileSystem {
    fn submit_range_head(
        &self,
        key: &str,
        timeout: std::time::Duration,
        callback: StorageCallback,
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

    fn submit_read_range(
        &self,
        key: &str,
        start: u64,
        end: u64,
        expected: StorageObjectMetadata,
        timeout: std::time::Duration,
        callback: crate::storage::RangeReadCallback,
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
            if !metadata.same_version(&expected) {
                return Err("precondition failed: remote SST version changed".into());
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
            if !after.same_version(&expected) {
                return Err("precondition failed: remote SST changed during range read".into());
            }
            Ok(bytes)
        })();
        let _ = callback.send(result);
    }

    fn submit_read_with_metadata(
        &self,
        key: &str,
        timeout: std::time::Duration,
        callback: crate::storage::MetadataReadCallback,
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
                    format!("not found: read {}: {error}", path.display())
                } else {
                    format!("read {}: {error}", path.display())
                }
            })?;
            let metadata = StorageObjectMetadata::content_crc(bytes.len() as u64, &bytes);
            Ok((bytes, metadata))
        })();
        let _ = callback.send(result);
    }

    fn submit_read(&self, key: &str, callback: StorageCallback) {
        let full_path = match self.full_path(key) {
            Ok(path) => path,
            Err(error) => {
                let _ = callback.send(StorageEvent::ReadComplete {
                    key: key.to_string(),
                    result: StorageOutcome::Err(error),
                });
                return;
            }
        };

        let outcome = match fs::read(&full_path) {
            Ok(bytes) => StorageOutcome::Ok(bytes),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                StorageOutcome::Err(format!("not found: read {}: {e}", full_path.display()))
            }
            Err(e) => StorageOutcome::Err(format!("read {}: {e}", full_path.display())),
        };

        let _ = callback.send(StorageEvent::ReadComplete {
            key: key.to_string(),
            result: outcome,
        });
    }

    fn submit_write(&self, key: &str, data: Vec<u8>, callback: StorageCallback) {
        let full_path = match self.full_path(key) {
            Ok(path) => path,
            Err(error) => {
                let _ = callback.send(StorageEvent::WriteComplete {
                    key: key.to_string(),
                    result: StorageOutcome::Err(error),
                });
                return;
            }
        };
        let _lock = mutation_lock(&full_path);
        let _process_lock = match self.acquire_process_lock(&full_path) {
            Ok(lock) => lock,
            Err(error) => {
                let _ = callback.send(StorageEvent::WriteComplete {
                    key: key.to_string(),
                    result: StorageOutcome::Err(error),
                });
                return;
            }
        };

        // Always try to create parent directories if present.
        let outcome = if let Some(parent) = full_path.parent() {
            if let Err(e) = fs::create_dir_all(parent) {
                StorageOutcome::Err(format!("mkdir {}: {e}", parent.display()))
            } else if let Err(e) = fs::write(&full_path, data) {
                StorageOutcome::Err(format!("write {}: {e}", full_path.display()))
            } else {
                StorageOutcome::Ok(())
            }
        } else {
            // Path has no parent (e.g., "foo") — still attempt the write.
            match fs::write(&full_path, data) {
                Ok(()) => StorageOutcome::Ok(()),
                Err(e) => StorageOutcome::Err(format!("write {}: {e}", full_path.display())),
            }
        };

        let _ = callback.send(StorageEvent::WriteComplete {
            key: key.to_string(),
            result: outcome,
        });
    }

    fn submit_write_with_headers(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: StorageCallback,
    ) {
        if headers.is_empty() {
            self.submit_write(key, data, callback);
            return;
        }

        let full_path = match self.full_path(key) {
            Ok(path) => path,
            Err(error) => {
                let _ = callback.send(StorageEvent::WriteComplete {
                    key: key.to_string(),
                    result: StorageOutcome::Err(error),
                });
                return;
            }
        };
        let if_none_match = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("if-none-match"))
            .map(|(_, value)| value.trim().to_string());
        let if_match = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("if-match"))
            .map(|(_, value)| value.trim().trim_matches('"').to_string());

        let _lock = mutation_lock(&full_path);
        let _process_lock = match self.acquire_process_lock(&full_path) {
            Ok(lock) => lock,
            Err(error) => {
                let _ = callback.send(StorageEvent::WriteComplete {
                    key: key.to_string(),
                    result: StorageOutcome::Err(error),
                });
                return;
            }
        };
        let outcome = if let Some(expected) = if_match {
            let identity = if expected.starts_with("fs:") {
                range_path_metadata(&full_path).map(|metadata| metadata.etag)
            } else {
                fs::read(&full_path)
                    .map_err(|error| error.to_string())
                    .map(|data| StorageObjectMetadata::content_crc(data.len() as u64, &data).etag)
            };
            match identity {
                Ok(current) => {
                    if current == expected {
                        write_file_with_parents(&full_path, data)
                    } else {
                        StorageOutcome::Err("precondition failed: etag mismatch".to_string())
                    }
                }
                Err(error) => StorageOutcome::Err(format!(
                    "precondition failed: read {}: {error}",
                    full_path.display()
                )),
            }
        } else if if_none_match.as_deref() == Some("*") {
            create_file_new_with_parents(&full_path, &data)
        } else {
            StorageOutcome::Err("conditional write requires a supported precondition".to_string())
        };

        let _ = callback.send(StorageEvent::WriteComplete {
            key: key.to_string(),
            result: outcome,
        });
    }

    fn submit_delete(&self, key: &str, callback: StorageCallback) {
        let full_path = match self.full_path(key) {
            Ok(path) => path,
            Err(error) => {
                let _ = callback.send(StorageEvent::DeleteComplete {
                    key: key.to_string(),
                    result: StorageOutcome::Err(error),
                });
                return;
            }
        };
        let _lock = mutation_lock(&full_path);
        let _process_lock = match self.acquire_process_lock(&full_path) {
            Ok(lock) => lock,
            Err(error) => {
                let _ = callback.send(StorageEvent::DeleteComplete {
                    key: key.to_string(),
                    result: StorageOutcome::Err(error),
                });
                return;
            }
        };

        let outcome = match fs::remove_file(&full_path) {
            Ok(()) => StorageOutcome::Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                StorageOutcome::Err(format!("not found: delete {}: {e}", full_path.display()))
            }
            Err(e) => StorageOutcome::Err(format!("delete {}: {e}", full_path.display())),
        };

        let _ = callback.send(StorageEvent::DeleteComplete {
            key: key.to_string(),
            result: outcome,
        });
    }

    fn submit_delete_with_headers(
        &self,
        key: &str,
        headers: Vec<(String, String)>,
        callback: StorageCallback,
    ) {
        if headers.is_empty() {
            self.submit_delete(key, callback);
            return;
        }

        let full_path = match self.full_path(key) {
            Ok(path) => path,
            Err(error) => {
                let _ = callback.send(StorageEvent::DeleteComplete {
                    key: key.to_string(),
                    result: StorageOutcome::Err(error),
                });
                return;
            }
        };
        let _lock = mutation_lock(&full_path);
        let _process_lock = match self.acquire_process_lock(&full_path) {
            Ok(lock) => lock,
            Err(error) => {
                let _ = callback.send(StorageEvent::DeleteComplete {
                    key: key.to_string(),
                    result: StorageOutcome::Err(error),
                });
                return;
            }
        };
        let outcome = if let Some((_, expected)) = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("if-match"))
        {
            let identity = if expected.trim_matches('"').starts_with("fs:") {
                range_path_metadata(&full_path).map(|metadata| metadata.etag)
            } else {
                fs::read(&full_path)
                    .map_err(|error| error.to_string())
                    .map(|data| StorageObjectMetadata::content_crc(data.len() as u64, &data).etag)
            };
            match identity {
                Ok(current) => {
                    if current == expected.trim_matches('"') {
                        match fs::remove_file(&full_path) {
                            Ok(()) => StorageOutcome::Ok(()),
                            Err(error) => StorageOutcome::Err(format!(
                                "delete {}: {error}",
                                full_path.display()
                            )),
                        }
                    } else {
                        StorageOutcome::Err("precondition failed: etag mismatch".to_string())
                    }
                }
                Err(error) => StorageOutcome::Err(format!(
                    "precondition failed: read {}: {error}",
                    full_path.display()
                )),
            }
        } else {
            StorageOutcome::Err(
                "conditional delete requires a supported If-Match precondition".to_string(),
            )
        };

        let _ = callback.send(StorageEvent::DeleteComplete {
            key: key.to_string(),
            result: outcome,
        });
    }

    #[cfg(test)]
    fn submit_list(&self, prefix: &str, callback: StorageCallback) {
        let full = match self.full_path(prefix) {
            Ok(path) => path,
            Err(error) => {
                let _ = callback.send(StorageEvent::ListComplete {
                    prefix: prefix.to_string(),
                    result: StorageOutcome::Err(error),
                });
                return;
            }
        };

        let outcome = if full.is_dir() {
            match fs::read_dir(&full) {
                Ok(iter) => {
                    let mut items: Vec<String> = Vec::new();

                    for entry in iter.flatten() {
                        if let Some(name) = entry.file_name().to_str() {
                            if name != ".midge-locks" {
                                items.push(name.to_string());
                            }
                        }
                    }

                    StorageOutcome::Ok(items)
                }
                Err(e) => StorageOutcome::Err(format!("list {}: {e}", full.display())),
            }
        } else {
            StorageOutcome::Ok(Vec::new())
        };

        let _ = callback.send(StorageEvent::ListComplete {
            prefix: prefix.to_string(),
            result: outcome,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{mpsc, Arc, Barrier};
    use tempfile::TempDir;

    // =========== Write Tests ===========

    #[test]
    fn should_write_file_successfully() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();
        let data = b"test data".to_vec();

        // Act
        fs.submit_write("test.txt", data.clone(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::WriteComplete { key, result } => {
                assert_eq!(key, "test.txt");
                assert!(result.is_ok());
                // Verify file was actually written
                let content = std::fs::read(temp_dir.path().join("test.txt")).unwrap();
                assert_eq!(content, data);
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[test]
    fn should_write_empty_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_write("empty.txt", vec![], tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::WriteComplete { result, .. } => {
                assert!(result.is_ok());
                let content = std::fs::read(temp_dir.path().join("empty.txt")).unwrap();
                assert!(content.is_empty());
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[test]
    fn should_write_large_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();
        let large_data = vec![42u8; 1_000_000];

        // Act
        fs.submit_write("large.bin", large_data.clone(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::WriteComplete { result, .. } => {
                assert!(result.is_ok());
                let content = std::fs::read(temp_dir.path().join("large.bin")).unwrap();
                assert_eq!(content, large_data);
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[test]
    fn should_create_parent_directories() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_write("subdir/nested/file.txt", b"data".to_vec(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::WriteComplete { result, .. } => {
                assert!(result.is_ok());
                assert!(temp_dir.path().join("subdir/nested/file.txt").exists());
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[test]
    fn should_overwrite_existing_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let path = temp_dir.path().join("file.txt");
        std::fs::write(&path, b"old").unwrap();

        // Act
        let (tx, rx) = mpsc::channel();
        fs.submit_write("file.txt", b"new".to_vec(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::WriteComplete { result, .. } => {
                assert!(result.is_ok());
                let content = std::fs::read(&path).unwrap();
                assert_eq!(content, b"new");
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[test]
    fn should_write_binary_data() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();
        let binary_data = vec![0u8, 1u8, 255u8, 254u8];

        // Act
        fs.submit_write("binary.bin", binary_data.clone(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::WriteComplete { result, .. } => {
                assert!(result.is_ok());
                let content = std::fs::read(temp_dir.path().join("binary.bin")).unwrap();
                assert_eq!(content, binary_data);
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[test]
    fn should_create_file_when_if_none_match_star_and_missing() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let path = temp_dir.path().join("conditional-create.txt");
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_write_with_headers(
            "conditional-create.txt",
            b"new".to_vec(),
            vec![("If-None-Match".into(), "*".into())],
            tx,
        );
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::WriteComplete { result, .. } => {
                assert!(result.is_ok());
                assert_eq!(std::fs::read(&path).unwrap(), b"new");
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[test]
    fn should_reject_create_when_if_none_match_star_and_existing() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let path = temp_dir.path().join("conditional-existing.txt");
        std::fs::write(&path, b"old").unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_write_with_headers(
            "conditional-existing.txt",
            b"new".to_vec(),
            vec![("If-None-Match".into(), "*".into())],
            tx,
        );
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::WriteComplete { result, .. } => {
                assert!(result.is_err());
                assert_eq!(std::fs::read(&path).unwrap(), b"old");
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[test]
    fn should_allow_exactly_one_concurrent_conditional_create() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let backend = Arc::new(FileSystem::new(temp_dir.path()).unwrap());
        let contenders = 8;
        let barrier = Arc::new(Barrier::new(contenders));

        // Act
        let joins: Vec<_> = (0..contenders)
            .map(|index| {
                let backend = Arc::clone(&backend);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let (tx, rx) = mpsc::channel();
                    barrier.wait();
                    backend.submit_write_with_headers(
                        "racing-create.txt",
                        format!("writer-{index}").into_bytes(),
                        vec![("If-None-Match".into(), "*".into())],
                        tx,
                    );
                    match rx.recv().expect("conditional create response") {
                        StorageEvent::WriteComplete { result, .. } => result.is_ok(),
                        other => panic!("unexpected response: {other:?}"),
                    }
                })
            })
            .collect();
        let success_count = joins
            .into_iter()
            .map(|join| usize::from(join.join().expect("create contender panicked")))
            .sum::<usize>();

        // Assert
        assert_eq!(success_count, 1);
        assert!(temp_dir.path().join("racing-create.txt").exists());
    }

    #[test]
    fn should_allow_one_winner_when_compare_swap_updates_race() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let key = "racing-cas.txt";
        let initial = b"initial".to_vec();
        std::fs::write(temp_dir.path().join(key), &initial).unwrap();
        let etag = StorageObjectMetadata::content_crc(initial.len() as u64, &initial).etag;
        let backend = Arc::new(FileSystem::new(temp_dir.path()).unwrap());
        let contenders = 8;
        let barrier = Arc::new(Barrier::new(contenders));

        // Act
        let joins: Vec<_> = (0..contenders)
            .map(|index| {
                let backend = Arc::clone(&backend);
                let barrier = Arc::clone(&barrier);
                let etag = etag.clone();
                std::thread::spawn(move || {
                    let (tx, rx) = mpsc::channel();
                    barrier.wait();
                    backend.submit_write_with_headers(
                        key,
                        format!("writer-{index}").into_bytes(),
                        vec![("If-Match".into(), etag)],
                        tx,
                    );
                    match rx.recv().expect("conditional update response") {
                        StorageEvent::WriteComplete { result, .. } => result.is_ok(),
                        other => panic!("unexpected response: {other:?}"),
                    }
                })
            })
            .collect();
        let success_count = joins
            .into_iter()
            .map(|join| usize::from(join.join().expect("CAS contender panicked")))
            .sum::<usize>();

        // Assert
        assert_eq!(success_count, 1);
        assert_ne!(std::fs::read(temp_dir.path().join(key)).unwrap(), initial);
    }

    // =========== Read Tests ===========

    #[test]
    fn should_read_existing_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let data = b"hello world";
        std::fs::write(temp_dir.path().join("test.txt"), data).unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_read("test.txt", tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::ReadComplete { key, result } => {
                assert_eq!(key, "test.txt");
                match result {
                    StorageOutcome::Ok(content) => assert_eq!(content, data),
                    StorageOutcome::Err(e) => panic!("Read failed: {e}"),
                }
            }
            _ => panic!("Expected ReadComplete"),
        }
    }

    #[test]
    fn should_fail_reading_nonexistent_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_read("nonexistent.txt", tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::ReadComplete { result, .. } => {
                assert!(result.is_err());
            }
            _ => panic!("Expected ReadComplete"),
        }
    }

    #[test]
    fn should_read_large_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let data = vec![42u8; 1_000_000];
        std::fs::write(temp_dir.path().join("large.bin"), &data).unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_read("large.bin", tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::ReadComplete { result, .. } => match result {
                StorageOutcome::Ok(content) => assert_eq!(content, data),
                StorageOutcome::Err(e) => panic!("Read failed: {e}"),
            },
            _ => panic!("Expected ReadComplete"),
        }
    }

    #[test]
    fn should_read_empty_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        std::fs::write(temp_dir.path().join("empty.txt"), b"").unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_read("empty.txt", tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::ReadComplete { result, .. } => match result {
                StorageOutcome::Ok(content) => assert!(content.is_empty()),
                StorageOutcome::Err(e) => panic!("Read failed: {e}"),
            },
            _ => panic!("Expected ReadComplete"),
        }
    }

    #[test]
    fn should_read_binary_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let data = vec![0u8, 1u8, 255u8, 254u8];
        std::fs::write(temp_dir.path().join("binary.bin"), &data).unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_read("binary.bin", tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::ReadComplete { result, .. } => match result {
                StorageOutcome::Ok(content) => assert_eq!(content, data),
                StorageOutcome::Err(e) => panic!("Read failed: {e}"),
            },
            _ => panic!("Expected ReadComplete"),
        }
    }

    // =========== Delete Tests ===========

    #[test]
    fn should_delete_existing_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("file.txt");
        std::fs::write(&path, b"data").unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_delete("file.txt", tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::DeleteComplete { result, .. } => {
                assert!(result.is_ok());
                assert!(!path.exists());
            }
            _ => panic!("Expected DeleteComplete"),
        }
    }

    #[test]
    fn should_fail_deleting_nonexistent_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_delete("nonexistent.txt", tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::DeleteComplete { result, .. } => {
                assert!(result.is_err());
            }
            _ => panic!("Expected DeleteComplete"),
        }
    }

    #[test]
    fn should_delete_nested_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(temp_dir.path().join("subdir")).unwrap();
        let path = temp_dir.path().join("subdir/file.txt");
        std::fs::write(&path, b"data").unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_delete("subdir/file.txt", tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::DeleteComplete { result, .. } => {
                assert!(result.is_ok());
                assert!(!path.exists());
            }
            _ => panic!("Expected DeleteComplete"),
        }
    }

    #[test]
    fn should_delete_file_when_if_match_header_matches_content() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("conditional.txt");
        let data = b"data".to_vec();
        std::fs::write(&path, &data).unwrap();
        let etag = StorageObjectMetadata::content_crc(data.len() as u64, &data).etag;
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_delete_with_headers("conditional.txt", vec![("If-Match".into(), etag)], tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::DeleteComplete { result, .. } => {
                assert!(result.is_ok());
                assert!(!path.exists());
            }
            _ => panic!("Expected DeleteComplete"),
        }
    }

    #[test]
    fn should_reject_conditional_delete_when_if_match_header_mismatches() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("conditional-stale.txt");
        std::fs::write(&path, b"data").unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_delete_with_headers(
            "conditional-stale.txt",
            vec![("If-Match".into(), "crc32c:00000000".into())],
            tx,
        );
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::DeleteComplete { result, .. } => {
                assert!(result.is_err());
                assert!(path.exists());
            }
            _ => panic!("Expected DeleteComplete"),
        }
    }

    // =========== List Tests ===========

    #[test]
    fn should_list_directory_contents() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        std::fs::write(temp_dir.path().join("file1.txt"), b"data1").unwrap();
        std::fs::write(temp_dir.path().join("file2.txt"), b"data2").unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_list("", tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::ListComplete { result, .. } => match result {
                StorageOutcome::Ok(items) => {
                    assert_eq!(items.len(), 2);
                    assert!(items.contains(&"file1.txt".to_string()));
                    assert!(items.contains(&"file2.txt".to_string()));
                }
                StorageOutcome::Err(e) => panic!("List failed: {e}"),
            },
            _ => panic!("Expected ListComplete"),
        }
    }

    #[test]
    fn should_hide_internal_process_lock_directory_from_list_results() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (write_tx, write_rx) = mpsc::channel();
        fs.submit_write("visible.txt", b"data".to_vec(), write_tx);
        assert!(matches!(
            write_rx.recv().expect("write response"),
            StorageEvent::WriteComplete {
                result: StorageOutcome::Ok(()),
                ..
            }
        ));
        let (list_tx, list_rx) = mpsc::channel();

        // Act
        fs.submit_list("", list_tx);

        // Assert
        match list_rx.recv().expect("list response") {
            StorageEvent::ListComplete {
                result: StorageOutcome::Ok(items),
                ..
            } => {
                assert_eq!(items, vec!["visible.txt".to_string()]);
            }
            other => panic!("unexpected list response: {other:?}"),
        }
    }

    #[test]
    fn should_list_empty_directory() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_list("", tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::ListComplete { result, .. } => match result {
                StorageOutcome::Ok(items) => assert!(items.is_empty()),
                StorageOutcome::Err(e) => panic!("List failed: {e}"),
            },
            _ => panic!("Expected ListComplete"),
        }
    }

    #[test]
    fn should_list_nonexistent_directory() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_list("nonexistent", tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::ListComplete { result, .. } => match result {
                StorageOutcome::Ok(items) => assert!(items.is_empty()),
                StorageOutcome::Err(e) => panic!("List failed: {e}"),
            },
            _ => panic!("Expected ListComplete"),
        }
    }

    #[test]
    fn should_sanitize_path_traversal() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act - Try to use path traversal
        fs.submit_write("../escape_dir/evil.txt", b"x".to_vec(), tx);
        let event = rx.recv().unwrap();

        // Assert - Should succeed but write inside base_path
        match event {
            StorageEvent::WriteComplete { result, .. } => {
                assert!(result.is_ok());
                assert!(temp_dir.path().join("escape_dir/evil.txt").exists());
                assert!(!temp_dir.path().join("../escape_dir/evil.txt").exists());
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[test]
    fn should_handle_absolute_paths() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act - Try absolute path
        fs.submit_write("/etc/passwd", b"x".to_vec(), tx);
        let event = rx.recv().unwrap();

        // Assert - Should sanitize
        match event {
            StorageEvent::WriteComplete { result, .. } => {
                assert!(result.is_ok());
                assert!(temp_dir.path().join("etc/passwd").exists());
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn should_reject_write_through_symlinked_parent() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let outside_dir = TempDir::new().unwrap();
        std::os::unix::fs::symlink(outside_dir.path(), temp_dir.path().join("link")).unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_write("link/escaped.txt", b"must stay inside".to_vec(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::WriteComplete { result, .. } => assert!(result.is_err()),
            _ => panic!("Expected WriteComplete"),
        }
        assert!(!outside_dir.path().join("escaped.txt").exists());
    }

    #[test]
    fn should_construct_with_custom_base_path() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_write("marker.txt", b"custom base path".to_vec(), tx);
        let event = rx.recv().unwrap();

        // Assert: the write must land under the base path passed to `new`, not some default.
        match event {
            StorageEvent::WriteComplete { result, .. } => assert!(result.is_ok()),
            _ => panic!("Expected WriteComplete"),
        }
        assert_eq!(
            fs::read(temp_dir.path().join("marker.txt")).unwrap(),
            b"custom base path"
        );
    }

    #[test]
    fn should_create_base_directory_if_missing() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let new_dir = temp_dir.path().join("new_base");
        assert!(!new_dir.exists());

        // Act
        let fs = FileSystem::new(&new_dir);

        // Assert
        assert!(fs.is_ok());
        assert!(new_dir.exists());
    }
}
