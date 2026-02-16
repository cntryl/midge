//! Local placeholder for a cloud-backed primary lease.
//!
//! IMPORTANT: this implementation only writes a coordination file to the local
//! `local_cache_path` and does NOT perform any remote conditional PUTs or use a
//! cloud backend. It therefore does NOT provide distributed exclusivity and is
//! suitable only for single-node testing and local development.
//!
//! Replace with a cloud-backed implementation (conditional writes / provider
//! lease APIs) before using in multi-node production deployments.

use super::fs_leader_store::FsLeaderStore;
use super::traits::{LeaderStore, LeaseError, LeaseGuard, PrimaryLease};
use crate::io::RealFs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Default TTL for cloud leases (30 seconds).
const DEFAULT_CLOUD_LEASE_TTL_SECS: u64 = 30;

/// Key used for the lease object in cloud storage.
const LEASE_OBJECT_KEY: &str = "midge_primary_lease.json";

/// Cloud storage lease configuration.
#[derive(Debug, Clone)]
pub struct CloudLeaseConfig {
    /// Bucket / container name.
    pub bucket: String,
    /// Object key prefix (e.g. `"databases/myapp/"`).
    pub prefix: String,
    /// Optional endpoint override (for S3-compatible providers).
    pub endpoint: Option<String>,
    /// Optional region.
    pub region: Option<String>,
}

/// Local-only placeholder for a cloud-backed lease implementation.
///
/// This implementation stores a coordination document only in the local cache
/// directory (not remotely). It is intended as a scaffold for a future
/// cloud-backed implementation and does NOT provide distributed exclusivity.
///
/// The local coordination document looks like:
/// ```text
/// holder_id: <pid@host>
/// acquired_at: <rfc3339>
/// expires_at: <rfc3339>
/// ```
pub struct CloudStorageLease {
    /// Cloud provider configuration.
    config: CloudLeaseConfig,
    /// Local cache path where lease coordination file is staged.
    local_cache_path: std::path::PathBuf,
    /// Unique identity of this holder (pid@hostname).
    holder_id: String,
    /// TTL for the lease.
    ttl: Duration,
    /// Whether we currently hold the lease.
    acquired: AtomicBool,
    /// Timestamp when lease was last renewed (for local staleness check).
    last_renewal: Mutex<Option<Instant>>,
    /// Epoch from the leader store, set after successful acquisition.
    acquired_epoch: std::sync::atomic::AtomicU64,
    /// Leader store for epoch-based fencing (backed by local cache path).
    leader_store: Option<Arc<FsLeaderStore>>,
}

impl CloudStorageLease {
    /// Create a new cloud storage lease.
    ///
    /// `local_cache_path` must be the local staging directory for cloud storage.
    /// The lease coordination file will be written here.
    pub fn new(config: CloudLeaseConfig, local_cache_path: std::path::PathBuf) -> Self {
        let holder_id = format!(
            "{}@{}",
            std::process::id(),
            hostname::get()
                .unwrap_or_else(|_| std::ffi::OsString::from("unknown"))
                .to_string_lossy()
        );

        // Attempt to create an FsLeaderStore backed by the local cache path
        // for epoch-based fencing.  Failure is non-fatal (degrades to epoch 0).
        let leader_store = RealFs::new(&local_cache_path)
            .ok()
            .map(|fs| Arc::new(FsLeaderStore::new(Arc::new(fs))));

        Self {
            config,
            local_cache_path,
            holder_id,
            ttl: Duration::from_secs(DEFAULT_CLOUD_LEASE_TTL_SECS),
            acquired: AtomicBool::new(false),
            last_renewal: Mutex::new(None),
            acquired_epoch: std::sync::atomic::AtomicU64::new(0),
            leader_store,
        }
    }

    /// Full object key for the lease file.
    ///
    /// Scaffolding for a future cloud-backed implementation — currently unused
    /// for remote writes (local-only implementation). Kept for diagnostic use.
    #[allow(dead_code)]
    fn lease_key(&self) -> String {
        if self.config.prefix.is_empty() {
            LEASE_OBJECT_KEY.to_string()
        } else {
            let prefix = self.config.prefix.trim_end_matches('/');
            format!("{}/{}", prefix, LEASE_OBJECT_KEY)
        }
    }

    /// Path to the local lease coordination file.
    fn local_lease_path(&self) -> std::path::PathBuf {
        self.local_cache_path.join(LEASE_OBJECT_KEY)
    }

    /// Read the current lease state from the local coordination file.
    fn read_lease_file(&self) -> Option<LeaseDocument> {
        let path = self.local_lease_path();
        let content = std::fs::read_to_string(&path).ok()?;
        parse_lease_document(&content)
    }

    /// Write a lease document to the local coordination file.
    fn write_lease_file(&self, doc: &LeaseDocument) -> Result<(), LeaseError> {
        let path = self.local_lease_path();

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| LeaseError::IoError(format!("failed to create lease dir: {}", e)))?;
        }

        let content = format_lease_document(doc);
        std::fs::write(&path, content)
            .map_err(|e| LeaseError::IoError(format!("failed to write lease file: {}", e)))
    }

    /// Remove the local lease coordination file.
    fn remove_lease_file(&self) -> Result<(), LeaseError> {
        let path = self.local_lease_path();
        if path.exists() {
            std::fs::remove_file(&path)
                .map_err(|e| LeaseError::IoError(format!("failed to remove lease file: {}", e)))?;
        }
        Ok(())
    }
}

impl PrimaryLease for CloudStorageLease {
    fn try_acquire(self: std::sync::Arc<Self>) -> Result<LeaseGuard, LeaseError> {
        // Borrow the inner value for field access (auto-deref handles Arc -> &T)
        let inner: &Self = &self;

        if inner.acquired.load(Ordering::Acquire) {
            return Err(LeaseError::AcquisitionFailed(
                "lease already acquired by this instance".to_string(),
            ));
        }

        // Check if an existing lease is still valid (held by another instance)
        if let Some(existing) = inner.read_lease_file() {
            if existing.holder_id != inner.holder_id && !existing.is_expired() {
                return Err(LeaseError::AcquisitionFailed(format!(
                    "another instance holds the lease (holder: {}, expires: {})",
                    existing.holder_id, existing.expires_at
                )));
            }
        }

        // Write our lease
        let now = chrono::Utc::now();
        let doc = LeaseDocument {
            holder_id: inner.holder_id.clone(),
            acquired_at: now.to_rfc3339(),
            expires_at: (now + chrono::Duration::seconds(inner.ttl.as_secs() as i64)).to_rfc3339(),
        };
        inner.write_lease_file(&doc)?;

        // Acquire epoch from local leader store for fencing.
        let epoch = if let Some(ref store) = inner.leader_store {
            let record = store.acquire_leadership(&inner.holder_id)?;
            record.epoch
        } else {
            0
        };
        inner
            .acquired_epoch
            .store(epoch, std::sync::atomic::Ordering::Release);

        inner.acquired.store(true, Ordering::Release);
        *inner.last_renewal.lock().expect("poisoned") = Some(Instant::now());

        tracing::info!(
            holder_id = %inner.holder_id,
            bucket = %inner.config.bucket,
            lease_key = %inner.lease_key(),
            "cloud storage lease acquired"
        );

        // Token-style guard: dropping the guard does NOT release the lease.
        Ok(LeaseGuard::token())
    }

    fn renew(&self) -> Result<(), LeaseError> {
        if !self.acquired.load(Ordering::Acquire) {
            return Err(LeaseError::RenewalFailed("lease not acquired".to_string()));
        }

        // Verify we still hold the lease
        if let Some(existing) = self.read_lease_file() {
            if existing.holder_id != self.holder_id {
                self.acquired.store(false, Ordering::Release);
                return Err(LeaseError::RenewalFailed(format!(
                    "lease stolen by another instance (holder: {})",
                    existing.holder_id
                )));
            }
        } else {
            // Lease file disappeared — write a fresh one
            tracing::warn!("lease file missing during renewal, re-acquiring");
        }

        // Write renewed lease
        let now = chrono::Utc::now();
        let doc = LeaseDocument {
            holder_id: self.holder_id.clone(),
            acquired_at: now.to_rfc3339(),
            expires_at: (now + chrono::Duration::seconds(self.ttl.as_secs() as i64)).to_rfc3339(),
        };
        self.write_lease_file(&doc)?;
        *self.last_renewal.lock().expect("poisoned") = Some(Instant::now());

        tracing::trace!("cloud storage lease renewed");

        // Also validate epoch is still current with leader store.
        if let Some(ref store) = self.leader_store {
            let expected = self
                .acquired_epoch
                .load(std::sync::atomic::Ordering::Acquire);
            if expected > 0 {
                store.validate_epoch(expected)?;
            }
        }

        Ok(())
    }

    fn release(&self) -> Result<(), LeaseError> {
        if !self.acquired.load(Ordering::Acquire) {
            return Ok(()); // Idempotent
        }

        self.remove_lease_file()?;
        self.acquired.store(false, Ordering::Release);

        tracing::info!(
            holder_id = %self.holder_id,
            "cloud storage lease released"
        );

        Ok(())
    }

    fn ttl(&self) -> Duration {
        self.ttl
    }

    fn holder_id(&self) -> String {
        self.holder_id.clone()
    }

    fn epoch(&self) -> u64 {
        self.acquired_epoch.load(Ordering::Acquire)
    }

    fn get_leader_store(&self) -> Option<Arc<dyn LeaderStore>> {
        self.leader_store
            .as_ref()
            .map(|s| Arc::clone(s) as Arc<dyn LeaderStore>)
    }
}
// SAFETY: CloudStorageLease is Send + Sync because:
// - `config`, `local_cache_path`, `holder_id`, `ttl` are immutable after construction.
// - `acquired` uses `AtomicBool` for lock-free thread-safe access.
// - `acquired_epoch` uses `AtomicU64` for lock-free thread-safe access.
// - `last_renewal` uses `Mutex` for interior mutability with proper synchronization.
// - `leader_store` is `Option<Arc<FsLeaderStore>>` — Arc is Send + Sync.
unsafe impl Send for CloudStorageLease {}
unsafe impl Sync for CloudStorageLease {}

/// Parsed lease document.
#[derive(Debug, Clone)]
struct LeaseDocument {
    holder_id: String,
    acquired_at: String,
    expires_at: String,
}

impl LeaseDocument {
    /// Check if the lease has expired based on `expires_at`.
    fn is_expired(&self) -> bool {
        let Ok(expires) = chrono::DateTime::parse_from_rfc3339(&self.expires_at) else {
            // If we can't parse the expiry, treat as expired (safe default)
            return true;
        };
        chrono::Utc::now() > expires
    }
}

/// Format a lease document as a simple key-value text file.
///
/// Uses a simple line-based format rather than pulling in serde_json,
/// keeping dependencies minimal for the lease subsystem.
fn format_lease_document(doc: &LeaseDocument) -> String {
    format!(
        "holder_id: {}\nacquired_at: {}\nexpires_at: {}\n",
        doc.holder_id, doc.acquired_at, doc.expires_at
    )
}

/// Parse a lease document from the simple key-value text format.
fn parse_lease_document(content: &str) -> Option<LeaseDocument> {
    let mut holder_id = None;
    let mut acquired_at = None;
    let mut expires_at = None;

    for line in content.lines() {
        if let Some(value) = line.strip_prefix("holder_id: ") {
            holder_id = Some(value.to_string());
        } else if let Some(value) = line.strip_prefix("acquired_at: ") {
            acquired_at = Some(value.to_string());
        } else if let Some(value) = line.strip_prefix("expires_at: ") {
            expires_at = Some(value.to_string());
        }
    }

    Some(LeaseDocument {
        holder_id: holder_id?,
        acquired_at: acquired_at?,
        expires_at: expires_at?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::Arc;

    fn test_config() -> CloudLeaseConfig {
        CloudLeaseConfig {
            bucket: "test-bucket".to_string(),
            prefix: "test/prefix".to_string(),
            endpoint: None,
            region: None,
        }
    }

    fn temp_cache_path() -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "midge_cloud_lease_test_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn should_acquire_lease_when_no_existing_lease() {
        // Arrange
        let cache_path = temp_cache_path();
        let lease = Arc::new(CloudStorageLease::new(test_config(), cache_path.clone()));

        // Act
        let result = Arc::clone(&lease).try_acquire();

        // Assert
        assert!(result.is_ok());
        assert!(lease_file_exists(&cache_path));
    }

    #[test]
    fn should_reject_double_acquire_when_already_held() {
        // Arrange
        let cache_path = temp_cache_path();
        let lease = Arc::new(CloudStorageLease::new(test_config(), cache_path));

        // Act
        let _guard = Arc::clone(&lease).try_acquire().unwrap();
        let result = Arc::clone(&lease).try_acquire();

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_reject_acquire_when_another_holder_active() {
        // Arrange
        let cache_path = temp_cache_path();

        // Simulate another holder's lease
        let now = chrono::Utc::now();
        let other_doc = LeaseDocument {
            holder_id: "other_process@other_host".to_string(),
            acquired_at: now.to_rfc3339(),
            expires_at: (now + chrono::Duration::seconds(60)).to_rfc3339(),
        };
        let lease_path = cache_path.join(LEASE_OBJECT_KEY);
        std::fs::write(&lease_path, format_lease_document(&other_doc)).unwrap();

        let lease = Arc::new(CloudStorageLease::new(test_config(), cache_path));

        // Act
        let result = Arc::clone(&lease).try_acquire();

        // Assert
        assert!(result.is_err());
        if let Err(LeaseError::AcquisitionFailed(msg)) = result {
            assert!(msg.contains("another"));
        }
    }

    #[test]
    fn should_acquire_lease_when_existing_lease_expired() {
        // Arrange
        let cache_path = temp_cache_path();

        // Simulate an expired lease from another holder
        let past = chrono::Utc::now() - chrono::Duration::seconds(120);
        let expired_doc = LeaseDocument {
            holder_id: "old_process@old_host".to_string(),
            acquired_at: (past - chrono::Duration::seconds(60)).to_rfc3339(),
            expires_at: past.to_rfc3339(),
        };
        let lease_path = cache_path.join(LEASE_OBJECT_KEY);
        std::fs::write(&lease_path, format_lease_document(&expired_doc)).unwrap();

        let lease = Arc::new(CloudStorageLease::new(test_config(), cache_path));

        // Act
        let result = Arc::clone(&lease).try_acquire();

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_renew_lease_when_held() {
        // Arrange
        let cache_path = temp_cache_path();
        let lease = Arc::new(CloudStorageLease::new(test_config(), cache_path));
        let _guard = Arc::clone(&lease).try_acquire().unwrap();

        // Act
        let result = lease.renew();

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_fail_renew_when_not_acquired() {
        // Arrange
        let cache_path = temp_cache_path();
        let lease = Arc::new(CloudStorageLease::new(test_config(), cache_path));

        // Act
        let result = lease.renew();

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_release_lease_when_held() {
        // Arrange
        let cache_path = temp_cache_path();
        let lease = Arc::new(CloudStorageLease::new(test_config(), cache_path.clone()));
        let _guard = Arc::clone(&lease).try_acquire().unwrap();

        // Act
        let result = lease.release();

        // Assert
        assert!(result.is_ok());
        assert!(!lease_file_exists(&cache_path));
    }

    #[test]
    fn should_allow_reacquire_after_release() {
        // Arrange
        let cache_path = temp_cache_path();
        let lease = Arc::new(CloudStorageLease::new(test_config(), cache_path));
        let guard = Arc::clone(&lease).try_acquire().unwrap();
        guard.release();
        lease.release().unwrap();

        // Act
        let result = Arc::clone(&lease).try_acquire();

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_return_correct_ttl() {
        // Arrange
        let cache_path = temp_cache_path();
        let lease = Arc::new(CloudStorageLease::new(test_config(), cache_path));

        // Act
        let ttl = lease.ttl();

        // Assert
        assert_eq!(ttl, Duration::from_secs(DEFAULT_CLOUD_LEASE_TTL_SECS));
    }

    #[test]
    fn should_format_holder_id_with_process_info() {
        // Arrange
        let cache_path = temp_cache_path();
        let lease = Arc::new(CloudStorageLease::new(test_config(), cache_path));

        // Act
        let holder = lease.holder_id();

        // Assert
        assert!(holder.contains('@'));
        assert!(holder.contains(&std::process::id().to_string()));
    }

    #[test]
    fn should_construct_lease_key_with_prefix() {
        // Arrange
        let cache_path = temp_cache_path();
        let lease = CloudStorageLease::new(test_config(), cache_path);

        // Act
        let key = lease.lease_key();

        // Assert
        assert_eq!(key, format!("test/prefix/{}", LEASE_OBJECT_KEY));
    }

    #[test]
    fn should_construct_lease_key_without_prefix() {
        // Arrange
        let config = CloudLeaseConfig {
            bucket: "bucket".to_string(),
            prefix: String::new(),
            endpoint: None,
            region: None,
        };
        let cache_path = temp_cache_path();
        let lease = Arc::new(CloudStorageLease::new(config, cache_path));

        // Act
        let key = lease.lease_key();

        // Assert
        assert_eq!(key, LEASE_OBJECT_KEY);
    }

    #[test]
    fn should_parse_lease_document_roundtrip() {
        // Arrange
        let doc = LeaseDocument {
            holder_id: "123@host".to_string(),
            acquired_at: "2026-02-07T12:00:00Z".to_string(),
            expires_at: "2026-02-07T12:00:30Z".to_string(),
        };

        // Act
        let serialized = format_lease_document(&doc);
        let parsed = parse_lease_document(&serialized);

        // Assert
        let parsed = parsed.unwrap();
        assert_eq!(parsed.holder_id, "123@host");
        assert_eq!(parsed.acquired_at, "2026-02-07T12:00:00Z");
        assert_eq!(parsed.expires_at, "2026-02-07T12:00:30Z");
    }

    #[test]
    fn should_detect_expired_lease() {
        // Arrange
        let past = chrono::Utc::now() - chrono::Duration::seconds(60);
        let doc = LeaseDocument {
            holder_id: "test".to_string(),
            acquired_at: (past - chrono::Duration::seconds(30)).to_rfc3339(),
            expires_at: past.to_rfc3339(),
        };

        // Act
        let expired = doc.is_expired();

        // Assert
        assert!(expired);
    }

    #[test]
    fn should_detect_active_lease() {
        // Arrange
        let future = chrono::Utc::now() + chrono::Duration::seconds(60);
        let doc = LeaseDocument {
            holder_id: "test".to_string(),
            acquired_at: chrono::Utc::now().to_rfc3339(),
            expires_at: future.to_rfc3339(),
        };

        // Act
        let expired = doc.is_expired();

        // Assert
        assert!(!expired);
    }

    fn lease_file_exists(cache_path: &std::path::Path) -> bool {
        cache_path.join(LEASE_OBJECT_KEY).exists()
    }
}
