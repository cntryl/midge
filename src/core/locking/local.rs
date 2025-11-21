//! Local file-based database lock.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use crate::error::{MidgeError, MidgeResult};

use super::meta::LockMeta;
use super::renewal::{renewal_interval_from_ttl, RenewalThread};
use super::traits::DbLock;

/// File-based lock for local database directories
pub struct LocalFileLock {
    /// Path to lock file (db_path/LOCK)
    lock_path: PathBuf,

    /// Current lock metadata
    meta: Option<LockMeta>,

    /// TTL in milliseconds
    ttl_ms: u32,

    /// Background renewal thread
    renewal: RenewalThread,
}

impl LocalFileLock {
    /// Create new local file lock
    pub fn new(db_path: &Path, ttl_ms: u32) -> Self {
        let lock_path = db_path.join("LOCK");

        Self {
            lock_path,
            meta: None,
            ttl_ms,
            renewal: RenewalThread::new(),
        }
    }

    /// Try to create lock file exclusively
    fn try_create(&mut self, meta: &LockMeta) -> MidgeResult<()> {
        let data = meta.encode()?;

        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&self.lock_path)
            .map_err(|e| {
                if e.kind() == io::ErrorKind::AlreadyExists {
                    MidgeError::DatabaseLocked
                } else {
                    MidgeError::Io(e)
                }
            })?;

        file.write_all(&data)?;
        file.sync_all()?;

        self.meta = Some(meta.clone());
        Ok(())
    }

    /// Read existing lock file
    fn read_existing(&self) -> MidgeResult<Option<LockMeta>> {
        match fs::read(&self.lock_path) {
            Ok(data) => {
                let meta = LockMeta::decode(&data)?;
                Ok(Some(meta))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(MidgeError::Io(e)),
        }
    }

    /// Atomic renewal via temp file + rename
    fn atomic_write(&self, meta: &LockMeta) -> MidgeResult<()> {
        let data = meta.encode()?;
        let tmp_path = self.lock_path.with_extension("tmp");

        // Write to temp file
        let mut file = File::create(&tmp_path)?;
        file.write_all(&data)?;
        file.sync_all()?;
        drop(file);

        // Atomic rename
        fs::rename(&tmp_path, &self.lock_path)?;

        Ok(())
    }

    /// Start background renewal thread
    fn start_renewal_thread(&mut self) {
        let lock_path = self.lock_path.clone();
        let renewal_interval = renewal_interval_from_ttl(self.ttl_ms);

        self.renewal.start(renewal_interval, move || {
            // Renew lock by reading, updating timestamp, and writing back
            if let Ok(data) = fs::read(&lock_path) {
                if let Ok(mut meta) = LockMeta::decode(&data) {
                    meta.renew();
                    if let Ok(new_data) = meta.encode() {
                        let tmp_path = lock_path.with_extension("tmp");
                        if let Ok(mut file) = File::create(&tmp_path) {
                            if file.write_all(&new_data).is_ok() && file.sync_all().is_ok() {
                                let _ = fs::rename(&tmp_path, &lock_path);
                            }
                        }
                    }
                }
            }
        });
    }
}

impl DbLock for LocalFileLock {
    fn try_acquire(&mut self, timeout: Duration) -> MidgeResult<()> {
        let start = Instant::now();
        let mut delay = Duration::from_millis(100);
        let mut tries: usize = 0;

        let meta = LockMeta::new(self.ttl_ms);

        loop {
            // Try to create lock file
            match self.try_create(&meta) {
                Ok(()) => {
                    // Start renewal thread
                    self.start_renewal_thread();
                    return Ok(());
                }
                Err(MidgeError::DatabaseLocked) => {
                    // Lock file exists, check if expired
                    if let Some(existing) = self.read_existing()? {
                        if existing.is_released() || existing.expired() {
                            // Try to take over
                            if fs::remove_file(&self.lock_path).is_ok() {
                                continue; // Retry acquisition
                            }
                        }
                    }

                    // Still locked, check timeout
                    if start.elapsed() >= timeout {
                        return Err(MidgeError::DatabaseLocked);
                    }

                    // Exponential backoff with jitter
                    thread::sleep(delay);
                    tries += 1;
                    if start.elapsed() > Duration::from_millis(250) {
                        tracing::warn!(elapsed_ms = %start.elapsed().as_millis(), tries, "LocalFileLock.try_acquire is blocking for >250ms");
                    }
                    delay = std::cmp::min(delay * 2, Duration::from_secs(5));
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn renew(&mut self) -> MidgeResult<()> {
        if let Some(ref mut meta) = self.meta {
            meta.renew();
            let meta_clone = meta.clone();
            self.atomic_write(&meta_clone)?;
        }
        Ok(())
    }

    fn release(&mut self) -> MidgeResult<()> {
        // Stop renewal thread
        self.renewal.stop();

        if let Some(ref mut meta) = self.meta {
            meta.mark_released();
            let meta_clone = meta.clone();
            let _ = self.atomic_write(&meta_clone); // Best effort
        }

        let _ = fs::remove_file(&self.lock_path); // Best effort
        self.meta = None;

        Ok(())
    }

    fn is_held(&self) -> bool {
        self.meta.is_some()
    }
}

impl Drop for LocalFileLock {
    fn drop(&mut self) {
        // Release will stop renewal thread and clean up
        let _ = self.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn should_acquire_local_file_lock() {
        // Arrange
        let tmp_dir = TempDir::new().unwrap();
        let mut lock = LocalFileLock::new(tmp_dir.path(), 5000);

        // Act
        lock.try_acquire(Duration::from_secs(1)).unwrap();

        // Assert
        assert!(lock.is_held());
        let lock_path = tmp_dir.path().join("LOCK");
        assert!(lock_path.exists());

        // Cleanup
        lock.release().unwrap();
    }

    #[test]
    fn should_release_local_file_lock() {
        // Arrange
        let tmp_dir = TempDir::new().unwrap();
        let mut lock = LocalFileLock::new(tmp_dir.path(), 5000);
        lock.try_acquire(Duration::from_secs(1)).unwrap();

        // Act
        lock.release().unwrap();

        // Assert
        assert!(!lock.is_held());
    }

    #[test]
    fn should_block_concurrent_acquire_when_lock_held() {
        // Arrange
        let tmp_dir = TempDir::new().unwrap();
        let mut lock1 = LocalFileLock::new(tmp_dir.path(), 5000);
        lock1.try_acquire(Duration::from_secs(1)).unwrap();
        let mut lock2 = LocalFileLock::new(tmp_dir.path(), 5000);

        // Act
        let result = lock2.try_acquire(Duration::from_millis(200));

        // Assert
        assert!(result.is_err());

        // Cleanup
        lock1.release().unwrap();
    }

    #[test]
    fn should_acquire_after_lock_released() {
        // Arrange
        let tmp_dir = TempDir::new().unwrap();
        let mut lock1 = LocalFileLock::new(tmp_dir.path(), 5000);
        lock1.try_acquire(Duration::from_secs(1)).unwrap();
        lock1.release().unwrap();
        let mut lock2 = LocalFileLock::new(tmp_dir.path(), 5000);

        // Act
        let result = lock2.try_acquire(Duration::from_secs(1));

        // Assert
        assert!(result.is_ok());

        // Cleanup
        lock2.release().unwrap();
    }

    #[test]
    fn should_takeover_lock_given_expired_lease() {
        // Arrange
        let tmp_dir = TempDir::new().unwrap();
        let mut lock1 = LocalFileLock::new(tmp_dir.path(), 100); // 100ms TTL
        lock1.try_acquire(Duration::from_secs(1)).unwrap();
        lock1.release().unwrap(); // Release to stop renewal thread
        let lock_path = tmp_dir.path().join("LOCK");
        let mut expired_meta = LockMeta::new(100);
        expired_meta.renewed_at -= 200; // 200ms ago
        let data = expired_meta.encode().unwrap();
        fs::write(&lock_path, data).unwrap();
        let mut lock2 = LocalFileLock::new(tmp_dir.path(), 5000);

        // Act
        let result = lock2.try_acquire(Duration::from_secs(1));

        // Assert
        assert!(result.is_ok());

        // Cleanup
        lock2.release().unwrap();
    }

    #[test]
    fn should_renew_lock_periodically_given_background_thread() {
        // Arrange
        let tmp_dir = TempDir::new().unwrap();
        let mut lock = LocalFileLock::new(tmp_dir.path(), 200); // 200ms TTL
        lock.try_acquire(Duration::from_secs(1)).unwrap();
        let lock_path = tmp_dir.path().join("LOCK");
        let data1 = fs::read(&lock_path).unwrap();
        let meta1 = LockMeta::decode(&data1).unwrap();

        // Act
        std::thread::sleep(Duration::from_millis(150));

        // Assert
        let data2 = fs::read(&lock_path).unwrap();
        let meta2 = LockMeta::decode(&data2).unwrap();
        assert!(meta2.renewed_at > meta1.renewed_at);

        // Cleanup
        lock.release().unwrap();
    }

    #[test]
    fn should_update_timestamp_given_manual_renewal() {
        // Arrange
        let tmp_dir = TempDir::new().unwrap();
        let mut lock = LocalFileLock::new(tmp_dir.path(), 5000);
        lock.try_acquire(Duration::from_secs(1)).unwrap();
        let original_renewed = lock.meta.as_ref().unwrap().renewed_at;
        std::thread::sleep(Duration::from_millis(100));

        // Act
        lock.renew().unwrap();

        // Assert
        let new_renewed = lock.meta.as_ref().unwrap().renewed_at;
        assert!(new_renewed > original_renewed);

        // Cleanup
        lock.release().unwrap();
    }
}
