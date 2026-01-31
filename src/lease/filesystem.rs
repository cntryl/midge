//! Filesystem-based primary lease using exclusive file locks.
//!
//! This implementation uses platform-specific file locking (flock on Unix, LockFileEx on Windows)
//! to provide exclusive access guarantees. It is suitable for:
//!
//! - Local deployments (single machine)
//! - Testing and development
//! - Fallback when cloud leases are unavailable
//!
//! ## Safety guarantees
//!
//! - **Exclusive locking**: Only one process can hold the lock
//! - **Automatic release on crash**: Lock is released when process terminates
//! - **Cross-platform**: Works on Linux, macOS, Windows
//!
//! ## Limitations
//!
//! - **Not suitable for distributed deployments** (different machines)
//! - No built-in TTL (relies on OS to release lock on process death)
//! - May not work correctly on NFS or other network filesystems

use super::traits::{LeaseError, LeaseGuard, PrimaryLease};
use crate::io::{Durability, File, Fs, FsPath, OpenMode, OpenOptions, RealFs};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const LEASE_FILE_NAME: &str = ".midge_primary_lease.lock";
const DEFAULT_TTL_SECS: u64 = 30;

/// Filesystem-based lease implementation.
pub struct FileSystemLease {
    fs: Arc<dyn Fs>,
    lease_file_path: FsPath,
    holder_id: String,
    lock_file: Mutex<Option<Box<dyn File>>>,
    acquired: AtomicBool,
}

impl FileSystemLease {
    /// Create a new filesystem lease for the given database path.
    pub fn new(db_path: PathBuf) -> Self {
        let fs = Arc::new(RealFs::new(&db_path).expect("failed to create filesystem for lease"));
        let lease_file_path = FsPath::new(LEASE_FILE_NAME);
        let holder_id = format!(
            "{}@{}",
            std::process::id(),
            hostname::get()
                .unwrap_or_else(|_| std::ffi::OsString::from("unknown"))
                .to_string_lossy()
        );

        Self {
            fs,
            lease_file_path,
            holder_id,
            lock_file: Mutex::new(None),
            acquired: AtomicBool::new(false),
        }
    }

    /// Try to acquire the exclusive file lock.
    fn try_lock(&self) -> Result<Box<dyn File>, LeaseError> {
        // Open/create the lock file (use persistent handle for 'static lifetime)
        let opts = OpenOptions {
            mode: OpenMode::ReadWrite,
            create: true,
            create_new: false,
            truncate: false,
        };

        let file = self
            .fs
            .open_persistent_handle(&self.lease_file_path, opts)
            .map_err(|e| LeaseError::IoError(format!("failed to open lease file: {}", e)))?;

        // Try to acquire exclusive lock (non-blocking)
        file.try_lock_exclusive().map_err(|e| {
            LeaseError::AcquisitionFailed(format!("another instance holds the lease: {}", e))
        })?;

        Ok(file)
    }

    /// Release the file lock.
    fn unlock(&self, file: &dyn File) -> Result<(), LeaseError> {
        file.unlock()
            .map_err(|e| LeaseError::IoError(format!("failed to unlock: {}", e)))
    }
}

impl PrimaryLease for FileSystemLease {
    fn try_acquire(&self) -> Result<LeaseGuard, LeaseError> {
        if self.acquired.load(Ordering::Acquire) {
            return Err(LeaseError::AcquisitionFailed(
                "lease already acquired by this instance".to_string(),
            ));
        }

        // Try to acquire the lock
        let mut file = self.try_lock()?;

        // Write holder info to the file
        let holder_info = format!(
            "holder: {}\npid: {}\ntime: {}\n",
            self.holder_id,
            std::process::id(),
            chrono::Utc::now().to_rfc3339()
        );
        file.write_at(0, bytes::Bytes::from(holder_info))?;
        file.sync(Durability::Durable)?;

        // Store the file handle
        *self
            .lock_file
            .lock()
            .expect("failed to lock lock_file mutex to store file") = Some(file);
        self.acquired.store(true, Ordering::Release);

        tracing::info!(
            holder_id = %self.holder_id,
            path = %self.lease_file_path.0,
            "primary lease acquired"
        );

        // Create guard - on drop/release, it will call the lease's release method
        // We need to keep the lease alive, but we can't clone it since lock_file isn't cloneable
        // The guard doesn't actually need to do anything - Engine::drop will call release()
        Ok(LeaseGuard::new(|| {
            // No-op: actual release happens in Engine::drop via self.release()
        }))
    }

    fn renew(&self) -> Result<(), LeaseError> {
        if !self.acquired.load(Ordering::Acquire) {
            return Err(LeaseError::RenewalFailed("lease not acquired".to_string()));
        }

        // For filesystem locks, renewal is a no-op since the OS maintains the lock
        // as long as the process is alive and the file is open.
        // We just verify the lock is still held by checking the file is still open.
        let lock_guard = self
            .lock_file
            .lock()
            .expect("failed to lock lock_file mutex during renew");
        if lock_guard.is_none() {
            return Err(LeaseError::RenewalFailed(
                "lock file was closed".to_string(),
            ));
        }

        Ok(())
    }

    fn release(&self) -> Result<(), LeaseError> {
        if !self.acquired.load(Ordering::Acquire) {
            return Ok(()); // Idempotent
        }

        let mut lock_guard = self
            .lock_file
            .lock()
            .expect("failed to lock lock_file mutex during release");
        if let Some(file) = lock_guard.take() {
            self.unlock(file.as_ref())?;
            drop(file); // Close file
            tracing::info!(
                holder_id = %self.holder_id,
                "primary lease released"
            );
        }

        self.acquired.store(false, Ordering::Release);
        Ok(())
    }

    fn ttl(&self) -> Duration {
        // Filesystem locks don't have a TTL; they're held until process exits.
        // Return a reasonable value for heartbeat scheduling.
        Duration::from_secs(DEFAULT_TTL_SECS)
    }

    fn holder_id(&self) -> String {
        self.holder_id.clone()
    }
}

impl FileSystemLease {
    /// Create a clone suitable for embedding in the guard's release callback.
    fn clone_for_guard(&self) -> Self {
        Self {
            fs: Arc::clone(&self.fs),
            lease_file_path: self.lease_file_path.clone(),
            holder_id: self.holder_id.clone(),
            lock_file: Mutex::new(None), // Not cloning the file handle
            acquired: AtomicBool::new(self.acquired.load(Ordering::Acquire)),
        }
    }
}

// Safety: FileSystemLease uses interior mutability correctly
unsafe impl Send for FileSystemLease {}
unsafe impl Sync for FileSystemLease {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn should_acquire_release_lease_when_no_contention() {
        let temp_dir = tempfile::tempdir().unwrap();
        let lease = Arc::new(FileSystemLease::new(temp_dir.path().to_path_buf()));

        let _guard = lease.try_acquire().unwrap();
        assert!(lease.acquired.load(Ordering::Acquire));

        // Explicitly release via the PrimaryLease interface
        lease.release().unwrap();
        assert!(!lease.acquired.load(Ordering::Acquire));
    }

    #[test]
    fn should_fail_acquisition_when_lease_already_held() {
        let temp_dir = tempfile::tempdir().unwrap();
        let lease1 = Arc::new(FileSystemLease::new(temp_dir.path().to_path_buf()));
        let lease2 = Arc::new(FileSystemLease::new(temp_dir.path().to_path_buf()));

        let _guard1 = lease1.try_acquire().unwrap();

        // Second acquisition should fail
        let result = lease2.try_acquire();
        assert!(matches!(result, Err(LeaseError::AcquisitionFailed(_))));
    }

    #[test]
    fn should_acquire_after_release_when_lease_freed() {
        let temp_dir = tempfile::tempdir().unwrap();
        let lease1 = Arc::new(FileSystemLease::new(temp_dir.path().to_path_buf()));
        let lease2 = Arc::new(FileSystemLease::new(temp_dir.path().to_path_buf()));

        let _guard1 = lease1.try_acquire().unwrap();
        lease1.release().unwrap();

        // Second acquisition should succeed
        let _guard2 = lease2.try_acquire().unwrap();
    }

    #[test]
    fn should_renew_successfully_when_lease_held() {
        let temp_dir = tempfile::tempdir().unwrap();
        let lease = FileSystemLease::new(temp_dir.path().to_path_buf());

        let _guard = lease.try_acquire().unwrap();
        assert!(lease.renew().is_ok());
    }
}
