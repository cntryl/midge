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

use crate::common::{MidgeError, MidgeResult};
use crate::storage::{
    StorageBackend, StorageCallback, StorageEvent, StorageObjectMetadata, StorageOutcome,
    StoragePrecondition, StorageRequest,
};
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::Write;
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::path::{Component, Path, PathBuf};
use std::sync::LazyLock;

#[cfg(test)]
mod inventory_tests;
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
    fn full_path(&self, key: &str) -> Result<PathBuf, crate::storage::StorageError> {
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
        let relative = out.strip_prefix(&self.base_path).map_err(|error| {
            crate::storage::StorageError::io(format!(
                "storage path escaped base directory: {error}"
            ))
        })?;
        let mut current = self.base_path.clone();
        for component in relative.components() {
            let Component::Normal(part) = component else {
                continue;
            };
            current.push(part);
            match fs::symlink_metadata(&current) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    return Err(crate::storage::StorageError::io(format!(
                        "storage path contains a symlink: {}",
                        current.display()
                    )));
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
                Err(error) => {
                    return Err(crate::storage::StorageError::io(format!(
                        "inspect storage path component {}: {error}",
                        current.display()
                    )));
                }
            }
        }
        Ok(out)
    }

    fn acquire_process_lock(
        &self,
        full_path: &Path,
    ) -> Result<ProcessMutationLock, crate::storage::StorageError> {
        #[cfg(unix)]
        {
            let lock_dir = self.base_path.join(".midge-locks");
            fs::create_dir_all(&lock_dir).map_err(|error| {
                format!("create lock directory {}: {error}", lock_dir.display())
            })?;
            let lock_name = process_lock_stripe_name(full_path);
            let file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(lock_dir.join(lock_name))
                .map_err(|error| {
                    crate::storage::StorageError::io(format!(
                        "open conditional mutation lock: {error}"
                    ))
                })?;
            // SAFETY: `file` is a valid open descriptor. `LOCK_EX` requests an
            // advisory exclusive lock which is released in `Drop` above.
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } != 0 {
                return Err(crate::storage::StorageError::io(format!(
                    "acquire conditional mutation lock: {}",
                    std::io::Error::last_os_error()
                )));
            }
            Ok(ProcessMutationLock { file })
        }

        #[cfg(windows)]
        {
            let lock_dir = self.base_path.join(".midge-locks");
            fs::create_dir_all(&lock_dir).map_err(|error| {
                format!("create lock directory {}: {error}", lock_dir.display())
            })?;
            let lock_name = process_lock_stripe_name(full_path);
            let file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(lock_dir.join(lock_name))
                .map_err(|error| {
                    crate::storage::StorageError::io(format!(
                        "open conditional mutation lock: {error}"
                    ))
                })?;
            file.lock().map_err(|error| {
                crate::storage::StorageError::io(format!(
                    "acquire conditional mutation lock: {error}"
                ))
            })?;
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

include!("filesystem/publication.rs");
include!("filesystem/range.rs");
include!("filesystem/inventory.rs");
include!("filesystem/backend.rs");

#[cfg(test)]
include!("filesystem/tests.rs");
