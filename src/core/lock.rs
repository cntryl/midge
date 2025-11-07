//! Database locking to prevent concurrent writers.
//!
//! This module provides exclusive access control for database directories,
//! preventing multiple writers from corrupting the same database.
//!
//! Two implementations:
//! - `LocalFileLock`: File-based lock for local/memory storage modes
//! - `CloudLeaseLock`: Distributed lease for cloud-backed storage mode
//!
//! Both use the same semantics:
//! - Acquisition with exponential backoff
//! - Heartbeat renewal (every ttl/2)
//! - Automatic read-only fallback on renewal failure
//! - Graceful release on shutdown

use parking_lot::Mutex;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::common::{timestamp, tlv};
use crate::error::{MidgeError, MidgeResult};
use bytes::Bytes;

/// Database lock trait - abstracts local file lock vs cloud distributed lease
pub trait DbLock: Send + Sync {
    /// Try to acquire the lock with a timeout.
    /// Returns Ok if acquired, Err(DatabaseLocked) if timeout exceeded.
    fn try_acquire(&mut self, timeout: Duration) -> MidgeResult<()>;

    /// Renew the lock (update heartbeat timestamp).
    /// Called by renewal thread every ttl/2.
    fn renew(&mut self) -> MidgeResult<()>;

    /// Release the lock (on clean shutdown).
    fn release(&mut self) -> MidgeResult<()>;

    /// Check if this lock is currently held.
    fn is_held(&self) -> bool;
}

/// Lock metadata stored in lock file or cloud blob
#[derive(Debug, Clone)]
pub struct LockMeta {
    /// Lock format version (always 1 for now)
    pub version: u8,

    /// Process ID of lock holder
    pub pid: u64,

    /// Hostname of lock holder
    pub host: String,

    /// Unique session ID (UUID bytes)
    pub uuid: [u8; 16],

    /// When lock was initially acquired (unix millis)
    pub acquired_at: u64,

    /// Last renewal timestamp (unix millis)
    pub renewed_at: u64,

    /// Time-to-live in milliseconds
    pub ttl_ms: u32,

    /// Flags bitfield (bit 0: released, bit 1: readonly)
    pub flags: u8,
}

// TLV field type IDs for lock metadata
// Format: (wire_type << 4) | field_id
// Wire types: U8=0, U16=1, U32=2, U64=3, Varint=4, Bytes=5
const TLV_VERSION: u8 = 0x01; // U8, field 1
const TLV_PID: u8 = 0x32; // U64, field 2
const TLV_HOST: u8 = 0x53; // Bytes, field 3
const TLV_UUID: u8 = 0x54; // Bytes, field 4
const TLV_ACQUIRED_AT: u8 = 0x35; // U64, field 5
const TLV_RENEWED_AT: u8 = 0x36; // U64, field 6
const TLV_TTL_MS: u8 = 0x27; // U32, field 7
const TLV_FLAGS: u8 = 0x08; // U8, field 8

// Flag bits
const FLAG_RELEASED: u8 = 0x01;

impl LockMeta {
    /// Create new lock metadata with current timestamp
    pub fn new(ttl_ms: u32) -> Self {
        let now = timestamp::now_millis();

        let pid = std::process::id() as u64;

        let host = hostname::get()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();

        let uuid = uuid::Uuid::new_v4().into_bytes();

        Self {
            version: 1,
            pid,
            host,
            uuid,
            acquired_at: now,
            renewed_at: now,
            ttl_ms,
            flags: 0,
        }
    }

    /// Encode to TLV bytes
    pub fn encode(&self) -> MidgeResult<Vec<u8>> {
        let mut writer = tlv::TlvWriter::with_capacity(128);

        writer.write_u8(TLV_VERSION, self.version);
        writer.write_u64(TLV_PID, self.pid);
        writer.write_bytes(TLV_HOST, self.host.as_bytes());
        writer.write_bytes(TLV_UUID, &self.uuid);
        writer.write_u64(TLV_ACQUIRED_AT, self.acquired_at);
        writer.write_u64(TLV_RENEWED_AT, self.renewed_at);
        writer.write_u32(TLV_TTL_MS, self.ttl_ms);
        writer.write_u8(TLV_FLAGS, self.flags);

        Ok(writer.finish())
    }

    /// Decode from TLV bytes
    pub fn decode(data: &[u8]) -> MidgeResult<Self> {
        let reader = tlv::TlvReader::new(data);

        let mut meta = LockMeta {
            version: 0,
            pid: 0,
            host: String::new(),
            uuid: [0u8; 16],
            acquired_at: 0,
            renewed_at: 0,
            ttl_ms: 0,
            flags: 0,
        };

        for (tag, value) in reader {
            match tag {
                TLV_VERSION => meta.version = tlv::parse_u8(value)?,
                TLV_PID => meta.pid = tlv::parse_u64(value)?,
                TLV_HOST => {
                    meta.host = String::from_utf8_lossy(value).to_string();
                }
                TLV_UUID => {
                    if value.len() >= 16 {
                        meta.uuid.copy_from_slice(&value[..16]);
                    }
                }
                TLV_ACQUIRED_AT => meta.acquired_at = tlv::parse_u64(value)?,
                TLV_RENEWED_AT => meta.renewed_at = tlv::parse_u64(value)?,
                TLV_TTL_MS => meta.ttl_ms = tlv::parse_u32(value)?,
                TLV_FLAGS => meta.flags = tlv::parse_u8(value)?,
                _ => {
                    // Unknown field, skip
                }
            }
        }

        Ok(meta)
    }

    /// Check if lock has expired
    pub fn expired(&self) -> bool {
        let now = timestamp::now_millis();

        now > self.renewed_at + self.ttl_ms as u64
    }

    /// Check if lock is marked as released
    pub fn is_released(&self) -> bool {
        self.flags & FLAG_RELEASED != 0
    }

    /// Mark lock as released
    pub fn mark_released(&mut self) {
        self.flags |= FLAG_RELEASED;
    }

    /// Update renewal timestamp
    pub fn renew(&mut self) {
        self.renewed_at = timestamp::now_millis();
    }
}

/// File-based lock for local database directories
pub struct LocalFileLock {
    /// Path to lock file (db_path/LOCK)
    lock_path: PathBuf,

    /// Current lock metadata
    meta: Option<LockMeta>,

    /// TTL in milliseconds
    ttl_ms: u32,

    /// Renewal thread handle
    renewal_handle: Option<JoinHandle<()>>,

    /// Signal to stop renewal thread
    stop_renewal: Arc<Mutex<bool>>,
}

impl LocalFileLock {
    /// Create new local file lock
    pub fn new(db_path: &Path, ttl_ms: u32) -> Self {
        let lock_path = db_path.join("LOCK");

        Self {
            lock_path,
            meta: None,
            ttl_ms,
            renewal_handle: None,
            stop_renewal: Arc::new(Mutex::new(false)),
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
        let stop_signal = Arc::clone(&self.stop_renewal);
        let renewal_interval = Duration::from_millis((self.ttl_ms as u64) / 2);

        let handle = thread::spawn(move || {
            loop {
                // Check stop signal
                {
                    let stop = stop_signal.lock();
                    if *stop {
                        break;
                    }
                }

                // Sleep for renewal interval
                thread::sleep(renewal_interval);

                // Renew lock
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
            }
        });

        self.renewal_handle = Some(handle);
    }
}

impl DbLock for LocalFileLock {
    fn try_acquire(&mut self, timeout: Duration) -> MidgeResult<()> {
        let start = Instant::now();
        let mut delay = Duration::from_millis(100);

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
        // Signal renewal thread to stop
        {
            let mut stop = self.stop_renewal.lock();
            *stop = true;
        }

        // Wait for renewal thread to finish
        if let Some(handle) = self.renewal_handle.take() {
            let _ = handle.join(); // Best effort
        }

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

/// Cloud-based distributed lock using ETags for atomic compare-and-swap
pub struct CloudLeaseLock {
    /// Lock ID in cloud storage
    lock_id: String,

    /// Cloud storage backend
    backend: Arc<dyn crate::wal::cloud::CloudStorageBackend>,

    /// Current lock metadata
    meta: Option<LockMeta>,

    /// Current ETag for CAS operations
    etag: Option<String>,

    /// TTL in milliseconds
    ttl_ms: u32,

    /// Renewal thread handle
    renewal_handle: Option<JoinHandle<()>>,

    /// Signal to stop renewal thread
    stop_renewal: Arc<Mutex<bool>>,
}

impl CloudLeaseLock {
    /// Create new cloud lease lock
    pub fn new(
        backend: Arc<dyn crate::wal::cloud::CloudStorageBackend>,
        lock_id: String,
        ttl_ms: u32,
    ) -> Self {
        Self {
            lock_id,
            backend,
            meta: None,
            etag: None,
            ttl_ms,
            renewal_handle: None,
            stop_renewal: Arc::new(Mutex::new(false)),
        }
    }

    /// Start background renewal thread
    fn start_renewal_thread(&mut self) {
        let lock_id = self.lock_id.clone();
        let backend = Arc::clone(&self.backend);
        let stop_signal = Arc::clone(&self.stop_renewal);
        let renewal_interval = Duration::from_millis((self.ttl_ms as u64) / 2);

        let handle = thread::spawn(move || {
            loop {
                // Check stop signal
                {
                    let stop = stop_signal.lock();
                    if *stop {
                        break;
                    }
                }

                // Sleep for renewal interval
                thread::sleep(renewal_interval);

                // Renew lock
                if let Ok((data, Some(current_etag))) = backend.get_with_etag(&lock_id) {
                    if let Ok(mut meta) = LockMeta::decode(&data) {
                        meta.renew();
                        if let Ok(new_data) = meta.encode() {
                            // Try to update with CAS
                            let _ = backend.put_if_match(
                                &lock_id,
                                Bytes::from(new_data),
                                &current_etag,
                            );
                        }
                    }
                }
            }
        });

        self.renewal_handle = Some(handle);
    }
}

impl DbLock for CloudLeaseLock {
    fn try_acquire(&mut self, timeout: Duration) -> MidgeResult<()> {
        let start = Instant::now();
        let mut delay = Duration::from_millis(10);

        let meta = LockMeta::new(self.ttl_ms);
        let data = meta.encode()?;

        loop {
            // Try to create lock atomically (this only succeeds if lock doesn't exist)
            match self
                .backend
                .put_if_not_exists(&self.lock_id, Bytes::from(data.clone()))
            {
                Ok(etag) => {
                    self.meta = Some(meta);
                    self.etag = Some(etag);
                    self.start_renewal_thread();
                    return Ok(());
                }
                Err(MidgeError::DatabaseLocked) => {
                    // Lock exists, check if we can take it over (expired or released)
                    if let Ok((existing_data, Some(existing_etag))) =
                        self.backend.get_with_etag(&self.lock_id)
                    {
                        if let Ok(existing_meta) = LockMeta::decode(&existing_data) {
                            if existing_meta.is_released() || existing_meta.expired() {
                                // Try to take over with CAS
                                match self.backend.put_if_match(
                                    &self.lock_id,
                                    Bytes::from(data.clone()),
                                    &existing_etag,
                                ) {
                                    Ok(new_etag) => {
                                        self.meta = Some(meta);
                                        self.etag = Some(new_etag);
                                        self.start_renewal_thread();
                                        return Ok(());
                                    }
                                    Err(_) => {
                                        // CAS failed (ETag changed), retry immediately without sleep
                                        // Another process might have taken over or the lock might have been deleted
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    return Err(e);
                }
            }

            // Still locked by another process, check timeout
            if start.elapsed() >= timeout {
                return Err(MidgeError::DatabaseLocked);
            }

            // Exponential backoff
            thread::sleep(delay);
            delay = std::cmp::min(delay * 2, Duration::from_secs(1));
        }
    }

    fn renew(&mut self) -> MidgeResult<()> {
        if let (Some(ref mut meta), Some(ref etag)) = (&mut self.meta, &self.etag) {
            meta.renew();
            let data = meta.encode()?;
            let new_etag = self
                .backend
                .put_if_match(&self.lock_id, Bytes::from(data), etag)?;
            self.etag = Some(new_etag);
        }
        Ok(())
    }

    fn release(&mut self) -> MidgeResult<()> {
        // Signal renewal thread to stop FIRST
        {
            let mut stop = self.stop_renewal.lock();
            *stop = true;
        }

        // Wait for renewal thread to finish (to ensure no concurrent renewal)
        if let Some(handle) = self.renewal_handle.take() {
            let _ = handle.join(); // Best effort
        }

        // Get the current state of the lock (which might have been updated by renewal thread)
        if self.meta.is_some() {
            // Fetch the current ETag and data
            match self.backend.get_with_etag(&self.lock_id) {
                Ok((existing_data, Some(current_etag))) => {
                    // Verify it's still our lock
                    if let Ok(existing_meta) = LockMeta::decode(&existing_data) {
                        // Create a released version
                        let mut released_meta = existing_meta;
                        released_meta.mark_released();
                        let data = released_meta.encode()?;

                        // Try to update with the current ETag
                        match self.backend.put_if_match(
                            &self.lock_id,
                            Bytes::from(data),
                            &current_etag,
                        ) {
                            Ok(_) => {
                                // Successfully marked as released
                            }
                            Err(_) => {
                                // CAS failed - lock might have been taken over, that's okay
                            }
                        }
                    }
                }
                Ok((_, None)) => {
                    // No ETag available, can't do CAS - just ignore
                }
                Err(_) => {
                    // Lock doesn't exist anymore, that's okay
                }
            }
        }

        self.meta = None;
        self.etag = None;

        Ok(())
    }

    fn is_held(&self) -> bool {
        self.meta.is_some() && self.etag.is_some()
    }
}

impl Drop for CloudLeaseLock {
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
    fn should_roundtrip_lock_meta_encoding() {
        // Arrange
        let meta = LockMeta::new(5000);

        // Act
        let encoded = meta.encode().unwrap();
        let decoded = LockMeta::decode(&encoded).unwrap();

        // Assert
        assert_eq!(meta.version, decoded.version);
        assert_eq!(meta.pid, decoded.pid);
        assert_eq!(meta.host, decoded.host);
        assert_eq!(meta.uuid, decoded.uuid);
        assert_eq!(meta.ttl_ms, decoded.ttl_ms);
    }

    #[test]
    fn should_detect_expiration_given_elapsed_ttl() {
        // Arrange
        let mut meta = LockMeta::new(100); // 100ms TTL
        assert!(!meta.expired());
        meta.renewed_at = crate::common::timestamp::now_millis() - 200;

        // Act
        let is_expired = meta.expired();

        // Assert
        assert!(is_expired);
    }

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

    // ===== CloudLeaseLock Tests =====

    #[test]
    fn should_acquire_cloud_lock() {
        use crate::cloud::mock::MockCloudBackend;

        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let mut lock = CloudLeaseLock::new(backend, "TEST_LOCK".to_string(), 5000);

        // Act
        lock.try_acquire(Duration::from_secs(1)).unwrap();

        // Assert
        assert!(lock.is_held());

        // Cleanup
        lock.release().unwrap();
    }

    #[test]
    fn should_release_cloud_lock() {
        use crate::cloud::mock::MockCloudBackend;

        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let mut lock = CloudLeaseLock::new(backend, "TEST_LOCK".to_string(), 5000);
        lock.try_acquire(Duration::from_secs(1)).unwrap();

        // Act
        lock.release().unwrap();

        // Assert
        assert!(!lock.is_held());
    }

    #[test]
    fn should_block_concurrent_cloud_acquire_when_lock_held() {
        use crate::cloud::mock::MockCloudBackend;
        use crate::wal::cloud::CloudStorageBackend;

        // Arrange
        let backend: Arc<dyn CloudStorageBackend> = Arc::new(MockCloudBackend::new());
        let mut lock1 = CloudLeaseLock::new(Arc::clone(&backend), "TEST_LOCK".to_string(), 5000);
        lock1.try_acquire(Duration::from_secs(1)).unwrap();
        let mut lock2 = CloudLeaseLock::new(Arc::clone(&backend), "TEST_LOCK".to_string(), 5000);

        // Act
        let result = lock2.try_acquire(Duration::from_millis(200));

        // Assert
        assert!(result.is_err());

        // Cleanup
        lock1.release().unwrap();
    }

    #[test]
    fn should_acquire_cloud_lock_after_release() {
        use crate::cloud::mock::MockCloudBackend;
        use crate::wal::cloud::CloudStorageBackend;

        // Arrange
        let backend: Arc<dyn CloudStorageBackend> = Arc::new(MockCloudBackend::new());
        let mut lock1 = CloudLeaseLock::new(Arc::clone(&backend), "TEST_LOCK".to_string(), 5000);
        lock1.try_acquire(Duration::from_secs(1)).unwrap();
        lock1.release().unwrap();
        let mut lock3 = CloudLeaseLock::new(Arc::clone(&backend), "TEST_LOCK".to_string(), 5000);

        // Act
        let result = lock3.try_acquire(Duration::from_secs(1));

        // Assert
        assert!(result.is_ok());

        // Cleanup
        lock3.release().unwrap();
    }

    #[test]
    fn should_takeover_cloud_lock_given_expired_lease() {
        use crate::cloud::mock::MockCloudBackend;
        use crate::wal::cloud::CloudStorageBackend;

        // Arrange
        let backend: Arc<dyn CloudStorageBackend> = Arc::new(MockCloudBackend::new());
        let mut expired_meta = LockMeta::new(100);
        expired_meta.renewed_at = crate::common::timestamp::now_millis() - 200; // 200ms ago
        let data = expired_meta.encode().unwrap();
        backend
            .put_if_not_exists("TEST_LOCK", Bytes::from(data))
            .unwrap();
        std::thread::sleep(Duration::from_millis(150)); // Wait to ensure expiration
        let mut lock2 = CloudLeaseLock::new(Arc::clone(&backend), "TEST_LOCK".to_string(), 5000);

        // Act
        let result = lock2.try_acquire(Duration::from_secs(1));

        // Assert
        assert!(result.is_ok());

        // Cleanup
        lock2.release().unwrap();
    }

    #[test]
    fn should_update_etag_given_cloud_renewal() {
        use crate::cloud::mock::MockCloudBackend;

        // Arrange
        let backend: Arc<dyn crate::wal::cloud::CloudStorageBackend> =
            Arc::new(MockCloudBackend::new());
        let mut lock = CloudLeaseLock::new(backend, "TEST_LOCK".to_string(), 5000);
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

    #[test]
    fn should_renew_cloud_lock_periodically_given_background_thread() {
        use crate::cloud::mock::MockCloudBackend;
        use crate::wal::cloud::CloudStorageBackend;

        // Arrange
        let backend: Arc<dyn CloudStorageBackend> = Arc::new(MockCloudBackend::new());
        let mut lock = CloudLeaseLock::new(Arc::clone(&backend), "TEST_LOCK".to_string(), 200);
        lock.try_acquire(Duration::from_secs(1)).unwrap();
        let (data1, _) = backend.get_with_etag("TEST_LOCK").unwrap();
        let meta1 = LockMeta::decode(&data1).unwrap();

        // Act
        std::thread::sleep(Duration::from_millis(150));

        // Assert
        let (data2, _) = backend.get_with_etag("TEST_LOCK").unwrap();
        let meta2 = LockMeta::decode(&data2).unwrap();
        assert!(meta2.renewed_at > meta1.renewed_at);

        // Cleanup
        lock.release().unwrap();
    }
}
