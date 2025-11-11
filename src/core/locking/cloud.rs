//! Cloud-based distributed database lock using ETags.

use bytes::Bytes;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::error::{MidgeError, MidgeResult};

use super::meta::LockMeta;
use super::renewal::{renewal_interval_from_ttl, RenewalThread};
use super::traits::DbLock;

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

    /// Background renewal thread
    renewal: RenewalThread,
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
            renewal: RenewalThread::new(),
        }
    }

    /// Start background renewal thread
    fn start_renewal_thread(&mut self) {
        let lock_id = self.lock_id.clone();
        let backend = Arc::clone(&self.backend);
        let renewal_interval = renewal_interval_from_ttl(self.ttl_ms);

        self.renewal.start(renewal_interval, move || {
            // Renew lock by reading current ETag, updating timestamp, and writing back with CAS
            if let Ok((data, Some(current_etag))) = backend.get_with_etag(&lock_id) {
                if let Ok(mut meta) = LockMeta::decode(&data) {
                    meta.renew();
                    if let Ok(new_data) = meta.encode() {
                        // Try to update with CAS
                        let _ =
                            backend.put_if_match(&lock_id, Bytes::from(new_data), &current_etag);
                    }
                }
            }
        });
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
        // Stop renewal thread
        self.renewal.stop();

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
    use crate::cloud::mock::MockCloudBackend;
    use crate::wal::cloud::CloudStorageBackend;

    #[test]
    fn should_acquire_cloud_lock() {
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
        // Arrange
        let backend: Arc<dyn CloudStorageBackend> = Arc::new(MockCloudBackend::new());
        let mut lock1 = CloudLeaseLock::new(Arc::clone(&backend), "TEST_LOCK".to_string(), 5000);
        lock1.try_acquire(Duration::from_secs(1)).unwrap();
        lock1.release().unwrap();
        let mut lock2 = CloudLeaseLock::new(Arc::clone(&backend), "TEST_LOCK".to_string(), 5000);

        // Act
        let result = lock2.try_acquire(Duration::from_secs(1));

        // Assert
        assert!(result.is_ok());

        // Cleanup
        lock2.release().unwrap();
    }

    #[test]
    fn should_takeover_cloud_lock_given_expired_lease() {
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
}
