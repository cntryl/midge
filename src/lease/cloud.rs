//! Cloud-backed primary lease.
//!
//! Real cloud mode coordinates through a provider-backed lease object using
//! conditional create/update semantics. The local cache path is still used for
//! staged diagnostics only; provider-backed fencing epochs live in the remote
//! lease document.
//!
//! Filesystem-simulated cloud mode can still construct this type without a
//! provider backend; that path remains local-only for deterministic tests.

use super::fs_leader_store::FsLeaderStore;
use super::traits::{LeaderRecord, LeaderStore, LeaseError, LeaseGuard, PrimaryLease};
use crate::io::RealFs;
use crate::storage::cloud::{CloudEvent, CloudOutcome, CloudStorage, ObjectMetadata};
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
}

/// Provider-backed view of the lease document used by WAL epoch fencing.
struct ProviderLeaderStore {
    cloud: Arc<CloudStorage>,
    ttl: Duration,
}

impl ProviderLeaderStore {
    fn new(cloud: Arc<CloudStorage>, ttl: Duration) -> Self {
        Self { cloud, ttl }
    }
}

impl LeaderStore for ProviderLeaderStore {
    fn acquire_leadership(&self, holder_id: &str) -> Result<LeaderRecord, LeaseError> {
        let existing_head = provider_head(&self.cloud)?;
        let existing = provider_read_doc(&self.cloud)?;

        if let Some(existing) = existing.as_ref() {
            if !existing.is_expired() {
                return Err(LeaseError::AcquisitionFailed(format!(
                    "another instance holds the lease (holder: {}, expires: {})",
                    existing.holder_id, existing.expires_at
                )));
            }
        }

        let previous_epoch = existing
            .as_ref()
            .and_then(|document| document.epoch)
            .unwrap_or(0);
        let epoch = previous_epoch.checked_add(1).ok_or_else(|| {
            LeaseError::AcquisitionFailed("cloud lease epoch overflow".to_string())
        })?;
        let now = chrono::Utc::now();
        let document = LeaseDocument {
            epoch: Some(epoch),
            holder_id: holder_id.to_string(),
            acquired_at: now.to_rfc3339(),
            expires_at: (now
                + chrono::Duration::seconds(CloudStorageLease::lease_ttl_seconds_i64(self.ttl)))
            .to_rfc3339(),
        };
        let headers = match existing_head {
            Some(metadata) => mutation_precondition_headers(&metadata).ok_or_else(|| {
                LeaseError::AcquisitionFailed(
                    "existing cloud lease has no conditional update token".to_string(),
                )
            })?,
            None => vec![("If-None-Match".to_string(), "*".to_string())],
        };
        provider_write_doc(&self.cloud, &document, headers)?;

        Ok(LeaderRecord {
            epoch,
            holder_id: document.holder_id,
            acquired_at: document.acquired_at,
        })
    }

    fn read_current(&self) -> Result<Option<LeaderRecord>, LeaseError> {
        Ok(
            provider_read_doc(&self.cloud)?.map(|document| LeaderRecord {
                epoch: document.epoch.unwrap_or(0),
                holder_id: document.holder_id,
                acquired_at: document.acquired_at,
            }),
        )
    }
}

/// Primary lease implementation for cloud-backed storage.
///
/// When constructed with `new_provider_backed`, the coordination document is
/// written to object storage with conditional create/update headers. When
/// constructed with `new`, it falls back to the local coordination file used by
/// filesystem-simulated cloud tests.
///
/// The coordination document looks like:
/// ```text
/// epoch: <monotonic fencing token>
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
    /// Epoch from the active coordination store, set after successful acquisition.
    acquired_epoch: std::sync::atomic::AtomicU64,
    /// Active leader store: filesystem-backed for simulation, provider-backed otherwise.
    leader_store: Option<Arc<dyn LeaderStore>>,
    /// Real cloud object backend for distributed lease coordination.
    cloud: Option<Arc<CloudStorage>>,
}

impl CloudStorageLease {
    fn lease_ttl_seconds_i64(duration: Duration) -> i64 {
        i64::try_from(duration.as_secs()).unwrap_or(i64::MAX)
    }

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
            .map(|fs| Arc::new(FsLeaderStore::new(Arc::new(fs))) as Arc<dyn LeaderStore>);

        Self {
            config,
            local_cache_path,
            holder_id,
            ttl: Duration::from_secs(DEFAULT_CLOUD_LEASE_TTL_SECS),
            acquired: AtomicBool::new(false),
            last_renewal: Mutex::new(None),
            acquired_epoch: std::sync::atomic::AtomicU64::new(0),
            leader_store,
            cloud: None,
        }
    }

    pub fn new_provider_backed(
        config: CloudLeaseConfig,
        local_cache_path: std::path::PathBuf,
        cloud: Arc<CloudStorage>,
    ) -> Self {
        let mut lease = Self::new(config, local_cache_path);
        lease.leader_store = Some(Arc::new(ProviderLeaderStore::new(
            Arc::clone(&cloud),
            lease.ttl,
        )));
        lease.cloud = Some(cloud);
        lease
    }

    /// Full object key for the lease file.
    ///
    /// Logical object key for the lease file. `CloudStorage` applies the
    /// configured namespace/prefix, so remote writes use `LEASE_OBJECT_KEY`
    /// directly and keep this helper for diagnostics.
    fn lease_key(&self) -> String {
        if self.config.prefix.is_empty() {
            LEASE_OBJECT_KEY.to_string()
        } else {
            let prefix = self.config.prefix.trim_end_matches('/');
            format!("{prefix}/{LEASE_OBJECT_KEY}")
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
                .map_err(|e| LeaseError::IoError(format!("failed to create lease dir: {e}")))?;
        }

        let content = format_lease_document(doc);
        std::fs::write(&path, content)
            .map_err(|e| LeaseError::IoError(format!("failed to write lease file: {e}")))
    }

    /// Remove the local lease coordination file.
    fn remove_lease_file(&self) -> Result<(), LeaseError> {
        let path = self.local_lease_path();
        if path.exists() {
            std::fs::remove_file(&path)
                .map_err(|e| LeaseError::IoError(format!("failed to remove lease file: {e}")))?;
        }
        Ok(())
    }

    fn remote_head(&self) -> Result<Option<ObjectMetadata>, LeaseError> {
        let Some(cloud) = self.cloud.as_ref() else {
            return Ok(None);
        };
        provider_head(cloud)
    }

    fn remote_read_doc(&self) -> Result<Option<LeaseDocument>, LeaseError> {
        let Some(cloud) = self.cloud.as_ref() else {
            return Ok(None);
        };
        provider_read_doc(cloud)
    }

    fn remote_write_doc(
        &self,
        doc: &LeaseDocument,
        headers: Vec<(String, String)>,
    ) -> Result<(), LeaseError> {
        let Some(cloud) = self.cloud.as_ref() else {
            return self.write_lease_file(doc);
        };
        provider_write_doc(cloud, doc, headers)
    }

    fn remote_release_if_still_holder(&self) -> Result<(), LeaseError> {
        let current = self.remote_read_doc()?;
        let Some(current) = current else {
            return Ok(());
        };

        let expected_epoch = self.acquired_epoch.load(Ordering::Acquire);
        if current.holder_id != self.holder_id || current.epoch != Some(expected_epoch) {
            tracing::warn!(
                holder_id = %self.holder_id,
                current_holder = %current.holder_id,
                expected_epoch,
                current_epoch = ?current.epoch,
                "skipping cloud lease release because the remote holder or epoch changed"
            );
            return Ok(());
        }

        let metadata = self.remote_head()?;
        let Some(metadata) = metadata else {
            tracing::warn!(
                holder_id = %self.holder_id,
                "skipping cloud lease release because the lease has no HEAD metadata"
            );
            return Ok(());
        };
        let Some(headers) = mutation_precondition_headers(&metadata) else {
            tracing::warn!(
                holder_id = %self.holder_id,
                "skipping cloud lease release because no conditional update token is available"
            );
            return Ok(());
        };

        let released = LeaseDocument {
            expires_at: (chrono::Utc::now() - chrono::Duration::milliseconds(1)).to_rfc3339(),
            ..current
        };
        self.remote_write_doc(&released, headers)
    }

    fn read_current_doc(&self) -> Result<Option<LeaseDocument>, LeaseError> {
        if self.cloud.is_some() {
            self.remote_read_doc()
        } else {
            Ok(self.read_lease_file())
        }
    }

    fn write_current_doc(
        &self,
        doc: &LeaseDocument,
        headers: Vec<(String, String)>,
    ) -> Result<(), LeaseError> {
        if self.cloud.is_some() {
            self.remote_write_doc(doc, headers)
        } else {
            self.write_lease_file(doc)
        }
    }

    fn delete_current_doc(&self) -> Result<(), LeaseError> {
        if self.cloud.is_some() {
            self.remote_release_if_still_holder()
        } else {
            if let Some(current) = self.read_lease_file() {
                if current.holder_id != self.holder_id {
                    return Ok(());
                }
            }
            self.remove_lease_file()
        }
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

        let epoch = if inner.cloud.is_some() {
            let store = inner.leader_store.as_ref().ok_or_else(|| {
                LeaseError::IoError("provider-backed lease has no leader store".to_string())
            })?;
            store.acquire_leadership(&inner.holder_id)?.epoch
        } else {
            let existing = inner.read_current_doc()?;

            // Any unexpired document represents a live lease. This also prevents two
            // instances in the same process from sharing a holder identifier.
            if let Some(existing) = existing.as_ref() {
                if !existing.is_expired() {
                    return Err(LeaseError::AcquisitionFailed(format!(
                        "another instance holds the lease (holder: {}, expires: {})",
                        existing.holder_id, existing.expires_at
                    )));
                }
            }

            let now = chrono::Utc::now();
            let document = LeaseDocument {
                epoch: None,
                holder_id: inner.holder_id.clone(),
                acquired_at: now.to_rfc3339(),
                expires_at: (now
                    + chrono::Duration::seconds(Self::lease_ttl_seconds_i64(inner.ttl)))
                .to_rfc3339(),
            };
            inner.write_current_doc(&document, Vec::new())?;

            if let Some(store) = inner.leader_store.as_ref() {
                store.acquire_leadership(&inner.holder_id)?.epoch
            } else {
                0
            }
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

        // Verify we still hold the lease and fencing epoch.
        let existing = self.read_current_doc()?;
        if let Some(existing) = existing.as_ref() {
            let expected_epoch = self.acquired_epoch.load(Ordering::Acquire);
            let epoch_changed = self.cloud.is_some() && existing.epoch != Some(expected_epoch);
            if existing.holder_id != self.holder_id || epoch_changed {
                self.acquired.store(false, Ordering::Release);
                return Err(LeaseError::RenewalFailed(format!(
                    "lease stolen by another instance (holder: {}, epoch: {:?})",
                    existing.holder_id, existing.epoch
                )));
            }
        } else if self.cloud.is_some() {
            self.acquired.store(false, Ordering::Release);
            return Err(LeaseError::RenewalFailed(
                "cloud lease document disappeared".to_string(),
            ));
        } else {
            // Lease file disappeared — write a fresh one
            tracing::warn!("lease file missing during renewal, re-acquiring");
        }

        let metadata = self.remote_head()?;

        // Write renewed lease
        let now = chrono::Utc::now();
        let doc = LeaseDocument {
            epoch: existing.as_ref().and_then(|document| document.epoch),
            holder_id: self.holder_id.clone(),
            acquired_at: existing
                .as_ref()
                .map_or_else(|| now.to_rfc3339(), |document| document.acquired_at.clone()),
            expires_at: (now + chrono::Duration::seconds(Self::lease_ttl_seconds_i64(self.ttl)))
                .to_rfc3339(),
        };
        let headers = match metadata {
            Some(metadata) if !metadata.etag.is_empty() => {
                vec![("If-Match".to_string(), metadata.etag)]
            }
            Some(_) if self.cloud.is_some() => {
                return Err(LeaseError::RenewalFailed(
                    "cloud lease has no ETag for conditional renewal".to_string(),
                ))
            }
            None if self.cloud.is_some() => {
                vec![("If-None-Match".to_string(), "*".to_string())]
            }
            Some(_) | None => Vec::new(),
        };
        self.write_current_doc(&doc, headers)?;
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

        self.delete_current_doc()?;
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
        self.leader_store.as_ref().map(Arc::clone)
    }
}
// SAFETY: CloudStorageLease is Send + Sync because:
// - `config`, `local_cache_path`, `holder_id`, `ttl` are immutable after construction.
// - `acquired` uses `AtomicBool` for lock-free thread-safe access.
// - `acquired_epoch` uses `AtomicU64` for lock-free thread-safe access.
// - `last_renewal` uses `Mutex` for interior mutability with proper synchronization.
// - `leader_store` is `Option<Arc<dyn LeaderStore>>`; the trait requires Send + Sync.
unsafe impl Send for CloudStorageLease {}
unsafe impl Sync for CloudStorageLease {}

/// Parsed lease document.
#[derive(Debug, Clone)]
struct LeaseDocument {
    epoch: Option<u64>,
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
/// Uses a simple line-based format rather than pulling in `serde_json`,
/// keeping dependencies minimal for the lease subsystem.
fn format_lease_document(doc: &LeaseDocument) -> String {
    let epoch = doc
        .epoch
        .map_or_else(String::new, |epoch| format!("epoch: {epoch}\n"));
    format!(
        "{epoch}holder_id: {}\nacquired_at: {}\nexpires_at: {}\n",
        doc.holder_id, doc.acquired_at, doc.expires_at,
    )
}

/// Parse a lease document from the simple key-value text format.
fn parse_lease_document(content: &str) -> Option<LeaseDocument> {
    let mut epoch = None;
    let mut holder_id = None;
    let mut acquired_at = None;
    let mut expires_at = None;

    for line in content.lines() {
        if let Some(value) = line.strip_prefix("epoch: ") {
            epoch = Some(value.parse::<u64>().ok()?);
        } else if let Some(value) = line.strip_prefix("holder_id: ") {
            holder_id = Some(value.to_string());
        } else if let Some(value) = line.strip_prefix("acquired_at: ") {
            acquired_at = Some(value.to_string());
        } else if let Some(value) = line.strip_prefix("expires_at: ") {
            expires_at = Some(value.to_string());
        }
    }

    Some(LeaseDocument {
        epoch,
        holder_id: holder_id?,
        acquired_at: acquired_at?,
        expires_at: expires_at?,
    })
}

fn is_remote_not_found(error: &str) -> bool {
    let lowered = error.to_ascii_lowercase();
    lowered.contains("not found")
        || lowered.contains("notfound")
        || lowered.contains("404")
        || lowered.contains("nosuchkey")
        || lowered.contains("blobnotfound")
}

fn mutation_precondition_headers(metadata: &ObjectMetadata) -> Option<Vec<(String, String)>> {
    if let Some(generation) = metadata
        .generation
        .as_ref()
        .filter(|value| !value.is_empty())
    {
        return Some(vec![(
            "x-goog-if-generation-match".to_string(),
            generation.clone(),
        )]);
    }

    if !metadata.etag.is_empty() {
        return Some(vec![("If-Match".to_string(), metadata.etag.clone())]);
    }

    None
}

fn provider_head(cloud: &CloudStorage) -> Result<Option<ObjectMetadata>, LeaseError> {
    let (tx, rx) = std::sync::mpsc::channel();
    cloud.submit_head(LEASE_OBJECT_KEY, tx);
    match rx.recv_timeout(Duration::from_secs(30)) {
        Ok(CloudEvent::Head { result, .. }) => match result {
            CloudOutcome::Ok(metadata) => Ok(Some(metadata)),
            CloudOutcome::Err(error) if is_remote_not_found(&error) => Ok(None),
            CloudOutcome::Err(error) => Err(LeaseError::IoError(format!(
                "cloud lease HEAD failed: {error}"
            ))),
        },
        Ok(other) => Err(LeaseError::IoError(format!(
            "unexpected cloud lease HEAD response: {other:?}"
        ))),
        Err(error) => Err(LeaseError::IoError(format!(
            "cloud lease HEAD timed out: {error}"
        ))),
    }
}

fn provider_read_doc(cloud: &CloudStorage) -> Result<Option<LeaseDocument>, LeaseError> {
    let (tx, rx) = std::sync::mpsc::channel();
    cloud.submit_get(LEASE_OBJECT_KEY, tx);
    match rx.recv_timeout(Duration::from_secs(30)) {
        Ok(CloudEvent::Get { result, .. }) => match result {
            CloudOutcome::Ok(bytes) => {
                let content = String::from_utf8(bytes).map_err(|error| {
                    LeaseError::IoError(format!("cloud lease document is not UTF-8: {error}"))
                })?;
                parse_lease_document(&content).map(Some).ok_or_else(|| {
                    LeaseError::IoError("cloud lease document is malformed".to_string())
                })
            }
            CloudOutcome::Err(error) if is_remote_not_found(&error) => Ok(None),
            CloudOutcome::Err(error) => Err(LeaseError::IoError(format!(
                "cloud lease GET failed: {error}"
            ))),
        },
        Ok(other) => Err(LeaseError::IoError(format!(
            "unexpected cloud lease GET response: {other:?}"
        ))),
        Err(error) => Err(LeaseError::IoError(format!(
            "cloud lease GET timed out: {error}"
        ))),
    }
}

fn provider_write_doc(
    cloud: &CloudStorage,
    document: &LeaseDocument,
    headers: Vec<(String, String)>,
) -> Result<(), LeaseError> {
    let (tx, rx) = std::sync::mpsc::channel();
    cloud.submit_put(
        LEASE_OBJECT_KEY,
        format_lease_document(document).into_bytes(),
        headers,
        tx,
    );
    match rx.recv_timeout(Duration::from_secs(30)) {
        Ok(CloudEvent::Put { result, .. }) => match result {
            CloudOutcome::Ok(()) => Ok(()),
            CloudOutcome::Err(error) => Err(LeaseError::AcquisitionFailed(format!(
                "cloud lease conditional write failed: {error}"
            ))),
        },
        Ok(other) => Err(LeaseError::IoError(format!(
            "unexpected cloud lease PUT response: {other:?}"
        ))),
        Err(error) => Err(LeaseError::IoError(format!(
            "cloud lease PUT timed out: {error}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::Arc;

    static TEMP_PATH_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    fn test_config() -> CloudLeaseConfig {
        CloudLeaseConfig {
            bucket: "test-bucket".to_string(),
            prefix: "test/prefix".to_string(),
        }
    }

    fn temp_cache_path() -> PathBuf {
        let counter = TEMP_PATH_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "midge_cloud_lease_test_{}_{}_{}",
            std::process::id(),
            counter,
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
            epoch: None,
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
            epoch: None,
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
    fn should_not_delete_provider_lease_owned_by_new_holder_on_stale_release() {
        // Arrange
        let cache_path = temp_cache_path();
        let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
        let lease = Arc::new(CloudStorageLease::new_provider_backed(
            test_config(),
            cache_path,
            Arc::clone(&cloud),
        ));
        let _guard = Arc::clone(&lease).try_acquire().unwrap();

        let now = chrono::Utc::now();
        let new_holder_doc = LeaseDocument {
            epoch: Some(lease.epoch().saturating_add(1)),
            holder_id: "new-holder@host".to_string(),
            acquired_at: now.to_rfc3339(),
            expires_at: (now + chrono::Duration::seconds(60)).to_rfc3339(),
        };
        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_put(
            LEASE_OBJECT_KEY,
            format_lease_document(&new_holder_doc).into_bytes(),
            vec![],
            tx,
        );
        let _ = rx.recv().unwrap();

        // Act
        lease.release().unwrap();

        // Assert
        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_get(LEASE_OBJECT_KEY, tx);
        let event = rx.recv().unwrap();
        let bytes = match event {
            CloudEvent::Get {
                result: CloudOutcome::Ok(bytes),
                ..
            } => bytes,
            other => panic!("expected surviving lease doc, got {other:?}"),
        };
        let content = String::from_utf8(bytes).unwrap();
        let doc = parse_lease_document(&content).unwrap();
        assert_eq!(doc.holder_id, "new-holder@host");
    }

    #[test]
    fn should_increment_remote_epoch_across_provider_cache_directories() {
        // Arrange
        let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
        let first = Arc::new(CloudStorageLease::new_provider_backed(
            test_config(),
            temp_cache_path(),
            Arc::clone(&cloud),
        ));
        let second = Arc::new(CloudStorageLease::new_provider_backed(
            test_config(),
            temp_cache_path(),
            cloud,
        ));
        assert!(first.get_leader_store().is_some());
        assert!(second.get_leader_store().is_some());

        // Act
        let _first_guard = Arc::clone(&first)
            .try_acquire()
            .expect("acquire first lease");
        let first_epoch = first.epoch();
        first.release().expect("release first lease");
        let _second_guard = Arc::clone(&second)
            .try_acquire()
            .expect("acquire second lease");

        // Assert
        assert_eq!(first_epoch, 1);
        assert_eq!(second.epoch(), 2);
    }

    #[test]
    fn should_validate_provider_epoch_through_leader_store() {
        // Arrange
        let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
        let lease = Arc::new(CloudStorageLease::new_provider_backed(
            test_config(),
            temp_cache_path(),
            Arc::clone(&cloud),
        ));
        let _guard = Arc::clone(&lease)
            .try_acquire()
            .expect("acquire provider lease");
        let acquired_epoch = lease.epoch();
        let leader_store = lease
            .get_leader_store()
            .expect("provider lease should expose its remote leader store");
        let now = chrono::Utc::now();
        let newer = LeaseDocument {
            epoch: Some(acquired_epoch + 1),
            holder_id: "new-holder@host".to_string(),
            acquired_at: now.to_rfc3339(),
            expires_at: (now + chrono::Duration::seconds(60)).to_rfc3339(),
        };

        // Act
        let current_result = leader_store.validate_epoch(acquired_epoch);
        put_remote_lease(&cloud, format_lease_document(&newer));
        let stale_result = leader_store.validate_epoch(acquired_epoch);

        // Assert
        assert!(current_result.is_ok());
        assert!(matches!(stale_result, Err(LeaseError::RenewalFailed(_))));
    }

    #[test]
    fn should_not_overwrite_newer_provider_epoch_on_stale_release() {
        // Arrange
        let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
        let lease = Arc::new(CloudStorageLease::new_provider_backed(
            test_config(),
            temp_cache_path(),
            Arc::clone(&cloud),
        ));
        let _guard = Arc::clone(&lease)
            .try_acquire()
            .expect("acquire provider lease");
        let now = chrono::Utc::now();
        let newer = LeaseDocument {
            epoch: Some(2),
            holder_id: lease.holder_id(),
            acquired_at: now.to_rfc3339(),
            expires_at: (now + chrono::Duration::seconds(60)).to_rfc3339(),
        };
        put_remote_lease(&cloud, format_lease_document(&newer));

        // Act
        lease.release().expect("stale release is harmless");

        // Assert
        let current = parse_lease_document(&read_remote_lease(&cloud)).expect("parse remote lease");
        assert_eq!(current.epoch, Some(2));
        assert!(!current.is_expired());
    }

    #[test]
    fn should_increment_expired_remote_epoch_on_provider_failover() {
        // Arrange
        let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
        let past = chrono::Utc::now() - chrono::Duration::seconds(60);
        put_remote_lease(
            &cloud,
            format!(
                "epoch: 41\nholder_id: old-holder@host\nacquired_at: {}\nexpires_at: {}\n",
                (past - chrono::Duration::seconds(30)).to_rfc3339(),
                past.to_rfc3339()
            ),
        );
        let lease = Arc::new(CloudStorageLease::new_provider_backed(
            test_config(),
            temp_cache_path(),
            cloud,
        ));

        // Act
        let _guard = Arc::clone(&lease)
            .try_acquire()
            .expect("acquire expired provider lease");

        // Assert
        assert_eq!(lease.epoch(), 42);
    }

    #[test]
    fn should_respect_active_legacy_provider_lease_until_expiry() {
        // Arrange
        let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
        let now = chrono::Utc::now();
        put_remote_lease(
            &cloud,
            format!(
                "holder_id: legacy-holder@host\nacquired_at: {}\nexpires_at: {}\n",
                now.to_rfc3339(),
                (now + chrono::Duration::seconds(60)).to_rfc3339()
            ),
        );
        let lease = Arc::new(CloudStorageLease::new_provider_backed(
            test_config(),
            temp_cache_path(),
            Arc::clone(&cloud),
        ));

        // Act
        let active_result = Arc::clone(&lease).try_acquire();
        let past = now - chrono::Duration::seconds(60);
        put_remote_lease(
            &cloud,
            format!(
                "holder_id: legacy-holder@host\nacquired_at: {}\nexpires_at: {}\n",
                (past - chrono::Duration::seconds(30)).to_rfc3339(),
                past.to_rfc3339()
            ),
        );
        let _guard = Arc::clone(&lease)
            .try_acquire()
            .expect("acquire expired legacy lease");

        // Assert
        assert!(active_result.is_err());
        assert_eq!(lease.epoch(), 1);
    }

    #[test]
    fn should_preserve_remote_epoch_when_provider_lease_renews() {
        // Arrange
        let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
        let lease = Arc::new(CloudStorageLease::new_provider_backed(
            test_config(),
            temp_cache_path(),
            Arc::clone(&cloud),
        ));
        let _guard = Arc::clone(&lease)
            .try_acquire()
            .expect("acquire provider lease");

        // Act
        lease.renew().expect("renew provider lease");
        let content = read_remote_lease(&cloud);

        // Assert
        assert!(content.lines().any(|line| line == "epoch: 1"));
        assert_eq!(lease.epoch(), 1);
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
        assert_eq!(key, format!("test/prefix/{LEASE_OBJECT_KEY}"));
    }

    #[test]
    fn should_construct_lease_key_without_prefix() {
        // Arrange
        let config = CloudLeaseConfig {
            bucket: "bucket".to_string(),
            prefix: String::new(),
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
            epoch: Some(7),
            holder_id: "123@host".to_string(),
            acquired_at: "2026-02-07T12:00:00Z".to_string(),
            expires_at: "2026-02-07T12:00:30Z".to_string(),
        };

        // Act
        let serialized = format_lease_document(&doc);
        let parsed = parse_lease_document(&serialized);

        // Assert
        let parsed = parsed.unwrap();
        assert_eq!(parsed.epoch, Some(7));
        assert_eq!(parsed.holder_id, "123@host");
        assert_eq!(parsed.acquired_at, "2026-02-07T12:00:00Z");
        assert_eq!(parsed.expires_at, "2026-02-07T12:00:30Z");
    }

    #[test]
    fn should_detect_expired_lease() {
        // Arrange
        let past = chrono::Utc::now() - chrono::Duration::seconds(60);
        let doc = LeaseDocument {
            epoch: None,
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
            epoch: None,
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

    fn put_remote_lease(cloud: &CloudStorage, content: String) {
        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_put(LEASE_OBJECT_KEY, content.into_bytes(), vec![], tx);
        match rx.recv().expect("receive remote lease put") {
            CloudEvent::Put {
                result: CloudOutcome::Ok(()),
                ..
            } => {}
            other => panic!("expected remote lease put, got {other:?}"),
        }
    }

    fn read_remote_lease(cloud: &CloudStorage) -> String {
        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_get(LEASE_OBJECT_KEY, tx);
        let bytes = match rx.recv().expect("receive remote lease get") {
            CloudEvent::Get {
                result: CloudOutcome::Ok(bytes),
                ..
            } => bytes,
            other => panic!("expected remote lease get, got {other:?}"),
        };
        String::from_utf8(bytes).expect("remote lease is UTF-8")
    }
}
