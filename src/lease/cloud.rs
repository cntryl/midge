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
use super::traits::{
    LeaderRecord, LeaderStore, LeaseError, LeaseGuard, LeaseValidity, PrimaryLease,
};
use crate::io::{staging, Fs, FsError, FsPath, OpenMode, OpenOptions, RealFs};
use crate::storage::cloud::{CloudEvent, CloudOutcome, CloudStorage, ObjectMetadata};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Default TTL for cloud leases (30 seconds).
const DEFAULT_CLOUD_LEASE_TTL_SECS: u64 = 30;
// Provider HTTP clients use a 10-second request timeout. Renewal admission
// retains one extra second so a timed-out request cannot still land after the
// holder's monotonic expiry.
const RENEWAL_WRITE_DEADLINE_MARGIN: Duration = Duration::from_secs(11);

/// Key used for the lease object in cloud storage.
const LEASE_OBJECT_KEY: &str = crate::cloud_layout::CloudObjectLayout::LEASE_OBJECT_KEY;

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
    owner_token: String,
    validity: Arc<LeaseValidity>,
    clock_skew_tolerance: Duration,
}

impl ProviderLeaderStore {
    fn new(
        cloud: Arc<CloudStorage>,
        ttl: Duration,
        owner_token: String,
        validity: Arc<LeaseValidity>,
        clock_skew_tolerance: Duration,
    ) -> Self {
        Self {
            cloud,
            ttl,
            owner_token,
            validity,
            clock_skew_tolerance,
        }
    }
}

impl LeaderStore for ProviderLeaderStore {
    fn acquire_leadership(&self, holder_id: &str) -> Result<LeaderRecord, LeaseError> {
        let current = provider_read_doc_with_metadata(&self.cloud, Duration::from_secs(30))?;
        let (existing, metadata) = match current {
            Some((document, metadata)) => (Some(document), Some(metadata)),
            None => (None, None),
        };

        if let Some(existing) = existing.as_ref() {
            if !existing.is_expired_with_tolerance(self.clock_skew_tolerance)? {
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
        let epoch = previous_epoch
            .checked_add(1)
            .ok_or(LeaseError::EpochExhausted)?;
        let monotonic_now = Instant::now();
        let now = chrono::Utc::now();
        let valid_until = monotonic_now + self.ttl;
        let document = LeaseDocument {
            epoch: Some(epoch),
            holder_id: holder_id.to_string(),
            owner_token: Some(self.owner_token.clone()),
            acquired_at: now.to_rfc3339(),
            expires_at: (now
                + chrono::Duration::seconds(CloudStorageLease::lease_ttl_seconds_i64(self.ttl)))
            .to_rfc3339(),
        };
        let headers = match metadata {
            Some(metadata) => mutation_precondition_headers(&metadata).ok_or_else(|| {
                LeaseError::IoError(
                    "existing cloud lease has no conditional update token".to_string(),
                )
            })?,
            None => vec![("If-None-Match".to_string(), "*".to_string())],
        };
        provider_write_doc(&self.cloud, &document, headers)?;
        self.validity.activate(epoch, valid_until)?;

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
/// owner_token: <random per-instance token>
/// acquired_at: <rfc3339>
/// expires_at: <rfc3339>
/// ```
pub struct CloudStorageLease {
    /// Cloud provider configuration.
    config: CloudLeaseConfig,
    /// Unique identity of this holder (pid@hostname).
    holder_id: String,
    /// Random per-instance token persisted in every lease mutation.
    owner_token: String,
    /// TTL for the lease.
    ttl: Duration,
    /// Whether we currently hold the lease.
    acquired: AtomicBool,
    /// Monotonic validity for the current cloud-lease acquisition.
    validity: Arc<LeaseValidity>,
    /// Epoch from the active coordination store, set after successful acquisition.
    acquired_epoch: std::sync::atomic::AtomicU64,
    /// Active leader store: filesystem-backed for simulation, provider-backed otherwise.
    leader_store: Option<Arc<dyn LeaderStore>>,
    /// Concrete local store used to serialize simulated-cloud document mutations.
    local_leader_store: Option<Arc<FsLeaderStore>>,
    /// Filesystem used for durable temp-write and atomic-rename publication.
    local_fs: Option<Arc<dyn Fs>>,
    /// Real cloud object backend for distributed lease coordination.
    cloud: Option<Arc<CloudStorage>>,
    clock_skew_tolerance: Duration,
}

impl CloudStorageLease {
    fn lease_ttl_seconds_i64(duration: Duration) -> i64 {
        i64::try_from(duration.as_secs()).unwrap_or(i64::MAX)
    }

    pub(crate) fn lease_validity(&self) -> Arc<LeaseValidity> {
        Arc::clone(&self.validity)
    }

    /// Create a new cloud storage lease.
    ///
    /// `local_cache_path` must be the local staging directory for cloud storage.
    /// The lease coordination file will be written here.
    pub fn new(config: CloudLeaseConfig, local_cache_path: impl AsRef<std::path::Path>) -> Self {
        let holder_id = format!(
            "{}@{}",
            std::process::id(),
            hostname::get()
                .unwrap_or_else(|_| std::ffi::OsString::from("unknown"))
                .to_string_lossy()
        );
        let owner_token = uuid::Uuid::new_v4().to_string();

        let mut local_fs = None;
        let mut local_leader_store = None;
        let leader_store = match RealFs::new(local_cache_path) {
            Ok(fs) => {
                let fs: Arc<dyn Fs> = Arc::new(fs);
                let local_store = Arc::new(FsLeaderStore::new(Arc::clone(&fs)));
                let leader_store: Arc<dyn LeaderStore> = Arc::clone(&local_store) as _;
                local_fs = Some(fs);
                local_leader_store = Some(local_store);
                Some(leader_store)
            }
            Err(error) => {
                tracing::warn!(%error, "failed to initialize simulated-cloud lease store");
                None
            }
        };

        Self {
            config,
            holder_id,
            owner_token,
            ttl: Duration::from_secs(DEFAULT_CLOUD_LEASE_TTL_SECS),
            acquired: AtomicBool::new(false),
            validity: Arc::new(LeaseValidity::new()),
            acquired_epoch: std::sync::atomic::AtomicU64::new(0),
            leader_store,
            local_leader_store,
            local_fs,
            cloud: None,
            clock_skew_tolerance: Duration::from_secs(DEFAULT_CLOUD_LEASE_TTL_SECS / 2),
        }
    }

    /// Override the bounded wall-clock skew allowance used for takeover.
    pub fn with_clock_skew_tolerance(mut self, tolerance: Duration) -> Result<Self, LeaseError> {
        if tolerance > self.ttl {
            return Err(LeaseError::Internal(format!(
                "lease clock-skew tolerance {tolerance:?} exceeds TTL {:?}",
                self.ttl
            )));
        }
        self.clock_skew_tolerance = tolerance;
        if let Some(cloud) = self.cloud.as_ref() {
            self.leader_store = Some(Arc::new(ProviderLeaderStore::new(
                Arc::clone(cloud),
                self.ttl,
                self.owner_token.clone(),
                Arc::clone(&self.validity),
                tolerance,
            )));
        }
        Ok(self)
    }

    pub(crate) fn new_with_clock_skew_tolerance(
        config: CloudLeaseConfig,
        local_cache_path: impl AsRef<std::path::Path>,
        clock_skew_tolerance: Duration,
    ) -> Self {
        let lease = Self::new(config, local_cache_path);
        lease
            .with_clock_skew_tolerance(clock_skew_tolerance)
            .expect("engine validates lease clock-skew tolerance")
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
            lease.owner_token.clone(),
            Arc::clone(&lease.validity),
            Duration::from_secs(DEFAULT_CLOUD_LEASE_TTL_SECS / 2),
        )));
        lease.cloud = Some(cloud);
        lease
    }

    pub(crate) fn new_provider_backed_with_clock_skew_tolerance(
        config: CloudLeaseConfig,
        local_cache_path: std::path::PathBuf,
        cloud: Arc<CloudStorage>,
        clock_skew_tolerance: Duration,
    ) -> Self {
        let lease = Self::new_provider_backed(config, local_cache_path, cloud);
        lease
            .with_clock_skew_tolerance(clock_skew_tolerance)
            .expect("engine validates lease clock-skew tolerance")
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

    /// Read the current lease state from the local coordination file.
    fn read_lease_file(&self) -> Result<Option<LeaseDocument>, LeaseError> {
        let fs = self.local_filesystem()?;
        let path = FsPath::new(LEASE_OBJECT_KEY);
        match fs.exists(&path) {
            Ok(false) => return Ok(None),
            Ok(true) => {}
            Err(error) => {
                return Err(LeaseError::IoError(format!(
                    "failed to check lease file existence: {error}"
                )));
            }
        }
        let metadata = match fs.metadata(&path) {
            Ok(metadata) => metadata,
            Err(FsError::NotFound(_)) => return Ok(None),
            Err(error) => {
                return Err(LeaseError::IoError(format!(
                    "failed to read lease file metadata: {error}"
                )));
            }
        };
        let file = match fs.open(
            &path,
            OpenOptions {
                mode: OpenMode::ReadOnly,
                create: false,
                create_new: false,
                truncate: false,
            },
        ) {
            Ok(file) => file,
            Err(FsError::NotFound(_)) => return Ok(None),
            Err(error) => {
                return Err(LeaseError::IoError(format!(
                    "failed to open lease file: {error}"
                )));
            }
        };
        let bytes = file
            .read_at(0, metadata.len)
            .map_err(|error| LeaseError::IoError(format!("failed to read lease file: {error}")))?;
        let content = String::from_utf8(bytes.to_vec()).map_err(|error| {
            LeaseError::Indeterminate(format!(
                "local lease coordination document is not UTF-8: {error}"
            ))
        })?;
        parse_lease_document(&content).map(Some).ok_or_else(|| {
            LeaseError::Indeterminate("local lease coordination document is malformed".to_string())
        })
    }

    /// Write a lease document to the local coordination file.
    fn write_lease_file(&self, doc: &LeaseDocument) -> Result<(), LeaseError> {
        let fs = self.local_filesystem()?;
        let content = format_lease_document(doc);
        let temp_path = FsPath::new(format!("{LEASE_OBJECT_KEY}.{}.tmp", self.owner_token));
        staging::stage_bytes(
            fs,
            &temp_path,
            &FsPath::new(LEASE_OBJECT_KEY),
            content.as_bytes(),
            LeaseError::IoError,
        )
    }

    /// Remove the local lease coordination file.
    fn remove_lease_file(&self) -> Result<(), LeaseError> {
        let fs = self.local_filesystem()?;
        match fs.remove_file(&FsPath::new(LEASE_OBJECT_KEY)) {
            Ok(()) | Err(FsError::NotFound(_)) => Ok(()),
            Err(error) => Err(LeaseError::IoError(format!(
                "failed to remove lease file: {error}"
            ))),
        }
    }

    fn remote_release_if_still_holder(&self) -> Result<(), LeaseError> {
        let Some(cloud) = self.cloud.as_ref() else {
            return Ok(());
        };
        let current = provider_read_doc_with_metadata(cloud, Duration::from_secs(30))?;
        let Some((current, metadata)) = current else {
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

        let expected_epoch = self.acquired_epoch.load(Ordering::Acquire);
        if !self.owns_document(&current, expected_epoch) {
            tracing::warn!(
                holder_id = %self.holder_id,
                current_holder = %current.holder_id,
                expected_epoch,
                current_epoch = ?current.epoch,
                "skipping cloud lease release because the remote holder or epoch changed"
            );
            return Ok(());
        }

        // Preserve the fencing epoch so the next holder must advance it.  The
        // released timestamp is placed beyond the configured skew window;
        // merely writing `now - 1ms` would keep the record live for callers
        // using the safe default tolerance.
        let release_offset = chrono::Duration::from_std(
            self.clock_skew_tolerance
                .saturating_add(Duration::from_millis(1)),
        )
        .unwrap_or(chrono::Duration::MAX);
        let released = LeaseDocument {
            expires_at: (chrono::Utc::now() - release_offset).to_rfc3339(),
            ..current
        };
        provider_write_doc(cloud, &released, headers)
    }

    fn local_store(&self) -> Result<&Arc<FsLeaderStore>, LeaseError> {
        self.local_leader_store.as_ref().ok_or_else(|| {
            LeaseError::IoError("simulated-cloud lease has no conditional leader store".to_string())
        })
    }

    fn local_filesystem(&self) -> Result<&Arc<dyn Fs>, LeaseError> {
        self.local_fs.as_ref().ok_or_else(|| {
            LeaseError::IoError("simulated-cloud lease filesystem is unavailable".to_string())
        })
    }

    fn owns_document(&self, document: &LeaseDocument, expected_epoch: u64) -> bool {
        document.holder_id == self.holder_id
            && document.owner_token.as_deref() == Some(self.owner_token.as_str())
            && document.epoch == Some(expected_epoch)
    }

    fn acquire_local(&self) -> Result<u64, LeaseError> {
        let store = self.local_store()?;
        let monotonic_now = Instant::now();
        let now = chrono::Utc::now();
        let valid_until = monotonic_now + self.ttl;
        let record = store.acquire_leadership_after_validation_and_publish(
            &self.holder_id,
            |_| {
                if let Some(existing) = self.read_lease_file()? {
                    if !existing.is_expired_with_tolerance(self.clock_skew_tolerance)? {
                        return Err(LeaseError::AcquisitionFailed(format!(
                            "another instance holds the lease (holder: {}, expires: {})",
                            existing.holder_id, existing.expires_at
                        )));
                    }
                }
                Ok(())
            },
            |record| {
                let document = LeaseDocument {
                    epoch: Some(record.epoch),
                    holder_id: self.holder_id.clone(),
                    owner_token: Some(self.owner_token.clone()),
                    acquired_at: now.to_rfc3339(),
                    expires_at: (now
                        + chrono::Duration::seconds(Self::lease_ttl_seconds_i64(self.ttl)))
                    .to_rfc3339(),
                };
                self.write_lease_file(&document)
            },
        )?;
        self.validity.activate(record.epoch, valid_until)?;
        Ok(record.epoch)
    }

    fn renew_local(&self) -> Result<(), LeaseError> {
        let expected_epoch = self.acquired_epoch.load(Ordering::Acquire);
        let store = self.local_store()?;
        let valid_until = store.with_exclusive_lock(&self.holder_id, || {
            let current = self.read_lease_file()?.ok_or_else(|| {
                self.acquired.store(false, Ordering::Release);
                LeaseError::RenewalFailed("simulated-cloud lease document disappeared".to_string())
            })?;
            if !self.owns_document(&current, expected_epoch) {
                self.acquired.store(false, Ordering::Release);
                return Err(LeaseError::RenewalFailed(format!(
                    "simulated-cloud lease ownership changed (holder: {}, epoch: {:?})",
                    current.holder_id, current.epoch
                )));
            }

            match store.read_current()? {
                Some(record)
                    if record.holder_id == self.holder_id && record.epoch == expected_epoch => {}
                Some(record) => {
                    self.acquired.store(false, Ordering::Release);
                    return Err(LeaseError::RenewalFailed(format!(
                        "simulated-cloud leader changed (holder: {}, epoch: {})",
                        record.holder_id, record.epoch
                    )));
                }
                None => {
                    self.acquired.store(false, Ordering::Release);
                    return Err(LeaseError::RenewalFailed(
                        "simulated-cloud leader record disappeared".to_string(),
                    ));
                }
            }

            let monotonic_now = Instant::now();
            let now = chrono::Utc::now();
            let valid_until = monotonic_now + self.ttl;
            let renewed = LeaseDocument {
                epoch: Some(expected_epoch),
                holder_id: self.holder_id.clone(),
                owner_token: Some(self.owner_token.clone()),
                acquired_at: current.acquired_at,
                expires_at: (now
                    + chrono::Duration::seconds(Self::lease_ttl_seconds_i64(self.ttl)))
                .to_rfc3339(),
            };
            self.validity.remaining(expected_epoch)?;
            self.write_lease_file(&renewed)?;
            Ok(valid_until)
        })?;

        if let Err(error) = store.refresh_timestamp(&self.holder_id, expected_epoch) {
            self.acquired.store(false, Ordering::Release);
            return Err(LeaseError::RenewalFailed(error.to_string()));
        }
        if let Err(error) = self.validity.advance(expected_epoch, valid_until) {
            let _ = self.release_local_if_still_holder();
            self.acquired.store(false, Ordering::Release);
            return Err(error);
        }
        Ok(())
    }

    fn release_local_if_still_holder(&self) -> Result<(), LeaseError> {
        let expected_epoch = self.acquired_epoch.load(Ordering::Acquire);
        let store = self.local_store()?;
        let removed = store.with_exclusive_lock(&self.holder_id, || {
            let Some(current) = self.read_lease_file()? else {
                return Ok(false);
            };
            if !self.owns_document(&current, expected_epoch) {
                tracing::warn!(
                    holder_id = %self.holder_id,
                    current_holder = %current.holder_id,
                    expected_epoch,
                    current_epoch = ?current.epoch,
                    "skipping simulated-cloud lease release because ownership changed"
                );
                return Ok(false);
            }
            self.remove_lease_file()?;
            Ok(true)
        })?;

        if removed && expected_epoch > 0 {
            store.release_if_owner(&self.holder_id, expected_epoch)?;
        }
        Ok(())
    }
}

impl PrimaryLease for CloudStorageLease {
    fn try_acquire(self: std::sync::Arc<Self>) -> Result<LeaseGuard, LeaseError> {
        // Borrow the inner value for field access (auto-deref handles Arc -> &T)
        let inner: &Self = &self;

        if inner.acquired.load(Ordering::Acquire) {
            return Err(LeaseError::AlreadyAcquired(
                "lease already acquired by this instance".to_string(),
            ));
        }

        let epoch = if inner.cloud.is_some() {
            let store = inner.leader_store.as_ref().ok_or_else(|| {
                LeaseError::IoError("provider-backed lease has no leader store".to_string())
            })?;
            store.acquire_leadership(&inner.holder_id)?.epoch
        } else {
            inner.acquire_local()?
        };
        inner
            .acquired_epoch
            .store(epoch, std::sync::atomic::Ordering::Release);

        inner.acquired.store(true, Ordering::Release);
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
        let expected_epoch = self.acquired_epoch.load(Ordering::Acquire);
        let result = (|| {
            if !self.acquired.load(Ordering::Acquire) {
                return Err(LeaseError::RenewalFailed("lease not acquired".to_string()));
            }
            self.validity.remaining(expected_epoch)?;
            if self.cloud.is_none() {
                self.renew_local()?;
                tracing::trace!("simulated-cloud storage lease renewed");
                return Ok(());
            }

            let (existing, metadata) = provider_read_doc_with_metadata(
                self.cloud
                    .as_ref()
                    .expect("provider lease has cloud backend"),
                self.validity.remaining(expected_epoch)?,
            )?
            .ok_or_else(|| {
                LeaseError::RenewalFailed("cloud lease document disappeared".to_string())
            })?;
            let headers = mutation_precondition_headers(&metadata).ok_or_else(|| {
                LeaseError::RenewalFailed(
                    "cloud lease has no token for conditional renewal".to_string(),
                )
            })?;
            // The document and mutation token came from the same provider GET. If
            // it changes afterward, the conditional write fails closed.
            if !self.owns_document(&existing, expected_epoch) {
                return Err(LeaseError::RenewalFailed(format!(
                    "cloud lease ownership changed (holder: {}, epoch: {:?})",
                    existing.holder_id, existing.epoch
                )));
            }

            // Write renewed lease
            let monotonic_now = Instant::now();
            let now = chrono::Utc::now();
            let valid_until = monotonic_now + self.ttl;
            let doc = LeaseDocument {
                epoch: Some(expected_epoch),
                holder_id: self.holder_id.clone(),
                owner_token: Some(self.owner_token.clone()),
                acquired_at: existing.acquired_at,
                expires_at: (now
                    + chrono::Duration::seconds(Self::lease_ttl_seconds_i64(self.ttl)))
                .to_rfc3339(),
            };
            let remaining = self.validity.remaining(expected_epoch)?;
            let write_timeout = remaining
                .checked_sub(RENEWAL_WRITE_DEADLINE_MARGIN)
                .filter(|timeout| !timeout.is_zero())
                .ok_or_else(|| {
                    LeaseError::RenewalFailed(
                        "insufficient monotonic validity remains for bounded provider renewal"
                            .to_string(),
                    )
                })?;
            if let Err(error) = provider_write_doc_with_timeout(
                self.cloud
                    .as_ref()
                    .expect("provider lease has cloud backend"),
                &doc,
                headers,
                write_timeout,
            ) {
                if matches!(error, LeaseError::IoError(_) | LeaseError::Indeterminate(_)) {
                    spawn_ambiguous_renewal_reconciler(
                        Arc::clone(
                            self.cloud
                                .as_ref()
                                .expect("provider lease has cloud backend"),
                        ),
                        doc,
                        self.clock_skew_tolerance,
                    );
                }
                return Err(error);
            }
            if let Err(error) = self.validity.advance(expected_epoch, valid_until) {
                // The conditional write may have landed as the monotonic deadline
                // expired. Best-effort CAS expiry prevents a fenced process from
                // extending takeover latency; failure remains fail-closed.
                let _ = self.remote_release_if_still_holder();
                return Err(error);
            }

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
        })();

        if let Err(error) = result {
            self.validity.fence(expected_epoch);
            self.acquired.store(false, Ordering::Release);
            self.acquired_epoch.store(0, Ordering::Release);
            return Err(error);
        }
        Ok(())
    }

    fn release(&self) -> Result<(), LeaseError> {
        if !self.acquired.load(Ordering::Acquire) {
            return Ok(()); // Idempotent
        }

        let released_epoch = self.acquired_epoch.load(Ordering::Acquire);
        if self.cloud.is_some() {
            self.remote_release_if_still_holder()?;
        } else {
            self.release_local_if_still_holder()?;
        }
        self.validity.deactivate(released_epoch);
        self.acquired.store(false, Ordering::Release);
        self.acquired_epoch.store(0, Ordering::Release);

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
// - `config`, paths, holder identities, and `ttl` are immutable after construction.
// - `acquired` uses `AtomicBool` for lock-free thread-safe access.
// - `acquired_epoch` uses `AtomicU64` for lock-free thread-safe access.
// - `last_renewal` uses `Mutex` for interior mutability with proper synchronization.
// - Filesystem and leader-store trait objects require Send + Sync.
unsafe impl Send for CloudStorageLease {}
unsafe impl Sync for CloudStorageLease {}

/// Parsed lease document.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LeaseDocument {
    epoch: Option<u64>,
    holder_id: String,
    owner_token: Option<String>,
    acquired_at: String,
    expires_at: String,
}

impl LeaseDocument {
    /// Check if the lease has expired based on `expires_at`.
    #[cfg(test)]
    fn is_expired(&self) -> Result<bool, LeaseError> {
        self.is_expired_with_tolerance(Duration::ZERO)
    }

    fn is_expired_with_tolerance(&self, tolerance: Duration) -> Result<bool, LeaseError> {
        let expires = chrono::DateTime::parse_from_rfc3339(&self.expires_at).map_err(|_| {
            LeaseError::Indeterminate(format!(
                "cloud lease expiry is invalid; ownership is ambiguous (holder: {}, epoch: {:?})",
                self.holder_id, self.epoch
            ))
        })?;
        let tolerance = chrono::Duration::from_std(tolerance).map_err(|_| {
            LeaseError::Internal("lease clock-skew tolerance is out of range".to_string())
        })?;
        Ok(chrono::Utc::now() > expires + tolerance)
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
    let owner_token = doc
        .owner_token
        .as_ref()
        .map_or_else(String::new, |token| format!("owner_token: {token}\n"));
    format!(
        "{epoch}holder_id: {}\n{owner_token}acquired_at: {}\nexpires_at: {}\n",
        doc.holder_id, doc.acquired_at, doc.expires_at,
    )
}

/// Parse a lease document from the simple key-value text format.
fn parse_lease_document(content: &str) -> Option<LeaseDocument> {
    let mut epoch = None;
    let mut holder_id = None;
    let mut owner_token = None;
    let mut acquired_at = None;
    let mut expires_at = None;

    for line in content.lines() {
        if let Some(value) = line.strip_prefix("epoch: ") {
            epoch = Some(value.parse::<u64>().ok()?);
        } else if let Some(value) = line.strip_prefix("holder_id: ") {
            holder_id = Some(value.to_string());
        } else if let Some(value) = line.strip_prefix("owner_token: ") {
            owner_token = Some(value.to_string());
        } else if let Some(value) = line.strip_prefix("acquired_at: ") {
            acquired_at = Some(value.to_string());
        } else if let Some(value) = line.strip_prefix("expires_at: ") {
            expires_at = Some(value.to_string());
        }
    }

    Some(LeaseDocument {
        epoch,
        holder_id: holder_id?,
        owner_token,
        acquired_at: acquired_at?,
        expires_at: expires_at?,
    })
}

fn mutation_precondition_headers(metadata: &ObjectMetadata) -> Option<Vec<(String, String)>> {
    crate::storage::cloud::object_match_precondition_headers(
        &metadata.etag,
        metadata.generation.as_deref(),
    )
}

/// Classify a non-not-found [`CloudError`] from a lease HEAD/GET into the
/// matching [`LeaseError`]. A malformed/ambiguous response gets its own
/// bucket distinct from a plain I/O failure, since the former means the
/// object exists but its state can't be trusted either way.
fn classify_lease_read_error(
    operation: &str,
    error: &crate::storage::cloud::CloudError,
) -> LeaseError {
    use crate::storage::cloud::CloudError;
    match error {
        #[cfg(any(test, feature = "cloud-common"))]
        CloudError::NotFound(_) => unreachable!("callers must check is_not_found() first"),
        CloudError::Protocol(msg) => {
            LeaseError::Indeterminate(format!("cloud lease {operation} response: {msg}"))
        }
        other => LeaseError::IoError(format!("cloud lease {operation} failed: {other}")),
    }
}

fn provider_read_doc(cloud: &CloudStorage) -> Result<Option<LeaseDocument>, LeaseError> {
    provider_read_doc_with_timeout(cloud, Duration::from_secs(30))
}

fn provider_read_doc_with_metadata(
    cloud: &CloudStorage,
    timeout: Duration,
) -> Result<Option<(LeaseDocument, ObjectMetadata)>, LeaseError> {
    let (tx, rx) = std::sync::mpsc::channel();
    cloud.submit_get_with_metadata(LEASE_OBJECT_KEY, tx);
    match rx.recv_timeout(timeout) {
        Ok(CloudEvent::GetWithMetadata {
            key: returned_key,
            result,
        }) => {
            drop(returned_key);
            match result {
                CloudOutcome::Ok((bytes, metadata)) => {
                    let content = String::from_utf8(bytes).map_err(|error| {
                        LeaseError::Indeterminate(format!(
                            "cloud lease document is not UTF-8: {error}"
                        ))
                    })?;
                    let document = parse_lease_document(&content).ok_or_else(|| {
                        LeaseError::Indeterminate("cloud lease document is malformed".to_string())
                    })?;
                    if mutation_precondition_headers(&metadata).is_none() {
                        return Err(LeaseError::IoError(
                            "existing cloud lease has no conditional update token".to_string(),
                        ));
                    }
                    Ok(Some((document, metadata)))
                }
                CloudOutcome::Err(error) if error.is_not_found() => Ok(None),
                CloudOutcome::Err(error) => Err(classify_lease_read_error("GET", &error)),
            }
        }
        Ok(other) => Err(LeaseError::IoError(format!(
            "unexpected metadata-bearing cloud lease GET response: {other:?}"
        ))),
        Err(error) => Err(LeaseError::IoError(format!(
            "cloud lease GET timed out: {error}"
        ))),
    }
}

fn provider_read_doc_with_timeout(
    cloud: &CloudStorage,
    timeout: Duration,
) -> Result<Option<LeaseDocument>, LeaseError> {
    let (tx, rx) = std::sync::mpsc::channel();
    cloud.submit_get(LEASE_OBJECT_KEY, tx);
    match rx.recv_timeout(timeout) {
        Ok(CloudEvent::Get { result, .. }) => match result {
            CloudOutcome::Ok(bytes) => {
                let content = String::from_utf8(bytes).map_err(|error| {
                    LeaseError::Indeterminate(format!("cloud lease document is not UTF-8: {error}"))
                })?;
                parse_lease_document(&content).map(Some).ok_or_else(|| {
                    LeaseError::Indeterminate("cloud lease document is malformed".to_string())
                })
            }
            CloudOutcome::Err(error) if error.is_not_found() => Ok(None),
            CloudOutcome::Err(error) => Err(classify_lease_read_error("GET", &error)),
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
    provider_write_doc_with_timeout(cloud, document, headers, Duration::from_secs(30))
}

fn provider_write_doc_with_timeout(
    cloud: &CloudStorage,
    document: &LeaseDocument,
    mut headers: Vec<(String, String)>,
    timeout: Duration,
) -> Result<(), LeaseError> {
    let timeout_ms = u64::try_from(timeout.as_millis())
        .unwrap_or(u64::MAX)
        .max(1);
    headers.push((
        crate::storage::cloud::REQUEST_TIMEOUT_HEADER.to_string(),
        timeout_ms.to_string(),
    ));
    let (tx, rx) = std::sync::mpsc::channel();
    cloud.submit_put(
        LEASE_OBJECT_KEY,
        format_lease_document(document).into_bytes(),
        headers,
        tx,
    );
    match rx.recv_timeout(timeout) {
        Ok(CloudEvent::Put { result, .. }) => match result {
            CloudOutcome::Ok(()) => Ok(()),
            // Only a genuine conditional-write race — another writer's PUT
            // already changed the object out from under our If-Match /
            // If-None-Match precondition — is confirmed contention. Every
            // other failure (auth, transport, server error, malformed
            // response) means the outcome is unknown, not that someone else
            // holds the lease.
            CloudOutcome::Err(error) => reconcile_ambiguous_lease_write(
                cloud,
                document,
                error.is_precondition_failed(),
                format!("cloud lease conditional write failed: {error}"),
            ),
        },
        Ok(other) => reconcile_ambiguous_lease_write(
            cloud,
            document,
            false,
            format!("unexpected cloud lease PUT response: {other:?}"),
        ),
        Err(error) => reconcile_ambiguous_lease_write(
            cloud,
            document,
            false,
            format!("cloud lease PUT timed out: {error}"),
        ),
    }
}

fn reconcile_ambiguous_lease_write(
    cloud: &CloudStorage,
    expected: &LeaseDocument,
    response_was_precondition_failure: bool,
    original_error: String,
) -> Result<(), LeaseError> {
    match provider_read_doc(cloud) {
        Ok(Some(actual)) if actual == *expected => {
            tracing::info!(
                holder_id = %expected.holder_id,
                epoch = ?expected.epoch,
                "confirmed ambiguous cloud lease write by readback"
            );
            Ok(())
        }
        Ok(_) if response_was_precondition_failure => {
            Err(LeaseError::AcquisitionFailed(format!(
                "cloud lease conditional write lost a precondition race: {original_error}"
            )))
        }
        Ok(_) => Err(LeaseError::IoError(original_error)),
        Err(read_error) => Err(LeaseError::Indeterminate(format!(
            "{original_error}; cloud lease readback could not determine whether the write landed: {read_error}"
        ))),
    }
}

fn spawn_ambiguous_renewal_reconciler(
    cloud: Arc<CloudStorage>,
    expected: LeaseDocument,
    clock_skew_tolerance: Duration,
) {
    let expected_expiry = chrono::DateTime::parse_from_rfc3339(&expected.expires_at).map_or_else(
        |_| chrono::Utc::now(),
        |value| value.with_timezone(&chrono::Utc),
    );
    let tolerance =
        chrono::Duration::from_std(clock_skew_tolerance).unwrap_or(chrono::Duration::MAX);
    let reconcile_until = expected_expiry
        .checked_add_signed(tolerance)
        .and_then(|value| value.checked_add_signed(chrono::Duration::seconds(1)))
        .unwrap_or(expected_expiry);

    let spawn = std::thread::Builder::new()
        .name("midge-lease-renew-reconciler".to_string())
        .spawn(move || {
            let poll_interval = if cfg!(test) {
                Duration::from_millis(10)
            } else {
                Duration::from_millis(100)
            };
            while chrono::Utc::now() <= reconcile_until {
                if let Ok(Some((current, metadata))) =
                    provider_read_doc_with_metadata(&cloud, Duration::from_millis(500))
                {
                    let same_authority = current.epoch == expected.epoch
                        && current.holder_id == expected.holder_id
                        && current.owner_token == expected.owner_token;
                    if !same_authority {
                        // A different authority won. Its provider identity
                        // is never used by this cleanup obligation.
                        return;
                    }
                    if !current
                        .is_expired_with_tolerance(clock_skew_tolerance)
                        .unwrap_or(false)
                    {
                        if let Some(headers) = mutation_precondition_headers(&metadata) {
                            let release_offset =
                                clock_skew_tolerance.saturating_add(Duration::from_millis(1));
                            let chrono_offset = chrono::Duration::from_std(release_offset)
                                .unwrap_or(chrono::Duration::MAX);
                            let expired = LeaseDocument {
                                expires_at: (chrono::Utc::now() - chrono_offset).to_rfc3339(),
                                ..current
                            };
                            let _ = provider_write_doc_with_timeout(
                                &cloud,
                                &expired,
                                headers,
                                Duration::from_millis(500),
                            );
                        }
                    }
                }
                // Absence and transient read failure are not completion proof:
                // an already-accepted PUT may still become visible, so retain
                // the obligation through the bound.
                std::thread::sleep(poll_interval);
            }
        });
    if let Err(error) = spawn {
        tracing::error!(%error, "failed to start ambiguous lease-renewal reconciler");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::MidgeError;
    use std::path::PathBuf;
    use std::sync::Arc;

    static TEMP_PATH_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    fn test_config() -> CloudLeaseConfig {
        CloudLeaseConfig {
            bucket: "test-bucket".to_string(),
            prefix: "test/prefix".to_string(),
        }
    }

    /// Test double that intercepts conditional PUTs (the lease acquisition
    /// path) and returns a scripted [`crate::storage::cloud::CloudError`]
    /// instead of delegating to the wrapped backend. Everything else
    /// delegates to an inner `MockCloudBackend`, so HEAD/GET/DELETE/LIST
    /// still behave normally.
    struct ScriptedConditionalPutBackend {
        inner: crate::storage::cloud::MockCloudBackend,
        scripted_error: crate::storage::cloud::CloudError,
        apply_before_error: bool,
    }

    impl crate::storage::cloud::CloudBackend for ScriptedConditionalPutBackend {
        fn submit_put(
            &self,
            key: &str,
            data: Vec<u8>,
            headers: Vec<(String, String)>,
            callback: crate::storage::cloud::CloudCallback,
        ) {
            let is_conditional = headers.iter().any(|(name, _)| {
                name.eq_ignore_ascii_case("if-match") || name.eq_ignore_ascii_case("if-none-match")
            });
            if is_conditional {
                if self.apply_before_error {
                    let (inner_callback, inner_result) = std::sync::mpsc::channel();
                    crate::storage::cloud::CloudBackend::submit_put(
                        &self.inner,
                        key,
                        data,
                        headers,
                        inner_callback,
                    );
                    let applied = inner_result
                        .recv_timeout(Duration::from_secs(1))
                        .expect("scripted lease write should complete");
                    assert!(
                        matches!(
                            applied,
                            crate::storage::cloud::CloudEvent::Put {
                                result: crate::storage::cloud::CloudOutcome::Ok(()),
                                ..
                            }
                        ),
                        "scripted backend must apply the conditional write before losing its response"
                    );
                }
                let _ = callback.send(crate::storage::cloud::CloudEvent::Put {
                    key: key.to_string(),
                    result: Err(self.scripted_error.clone()),
                });
                return;
            }
            crate::storage::cloud::CloudBackend::submit_put(
                &self.inner,
                key,
                data,
                headers,
                callback,
            );
        }

        fn submit_get(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
            crate::storage::cloud::CloudBackend::submit_get(&self.inner, key, callback);
        }

        fn submit_get_with_metadata(
            &self,
            key: &str,
            callback: crate::storage::cloud::CloudCallback,
        ) {
            crate::storage::cloud::CloudBackend::submit_get_with_metadata(
                &self.inner,
                key,
                callback,
            );
        }

        fn submit_get_range(
            &self,
            key: &str,
            start: u64,
            end: Option<u64>,
            callback: crate::storage::cloud::CloudCallback,
        ) {
            crate::storage::cloud::CloudBackend::submit_get_range(
                &self.inner,
                key,
                start,
                end,
                callback,
            );
        }

        fn submit_delete(
            &self,
            key: &str,
            headers: Vec<(String, String)>,
            callback: crate::storage::cloud::CloudCallback,
        ) {
            crate::storage::cloud::CloudBackend::submit_delete(&self.inner, key, headers, callback);
        }

        fn submit_list(&self, prefix: &str, callback: crate::storage::cloud::CloudCallback) {
            crate::storage::cloud::CloudBackend::submit_list(&self.inner, prefix, callback);
        }

        fn submit_head(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
            crate::storage::cloud::CloudBackend::submit_head(&self.inner, key, callback);
        }
    }

    fn lease_with_scripted_conditional_put_error(
        scripted_error: crate::storage::cloud::CloudError,
    ) -> Arc<CloudStorageLease> {
        let backend = Arc::new(ScriptedConditionalPutBackend {
            inner: crate::storage::cloud::MockCloudBackend::new(),
            scripted_error,
            apply_before_error: false,
        });
        let cloud = Arc::new(crate::storage::cloud::CloudStorage::new(
            backend,
            "midge".to_string(),
        ));
        Arc::new(CloudStorageLease::new_provider_backed(
            test_config(),
            temp_cache_path(),
            cloud,
        ))
    }

    fn lease_with_applied_conditional_put_error(
        scripted_error: crate::storage::cloud::CloudError,
    ) -> Arc<CloudStorageLease> {
        let backend = Arc::new(ScriptedConditionalPutBackend {
            inner: crate::storage::cloud::MockCloudBackend::new(),
            scripted_error,
            apply_before_error: true,
        });
        let cloud = Arc::new(crate::storage::cloud::CloudStorage::new(
            backend,
            "midge".to_string(),
        ));
        Arc::new(CloudStorageLease::new_provider_backed(
            test_config(),
            temp_cache_path(),
            cloud,
        ))
    }

    /// Test double whose HEAD responses always report no etag and no
    /// generation, regardless of what the wrapped backend actually stored —
    /// simulating a provider response that omits the fields a conditional
    /// write needs.
    struct NoCasTokenBackend {
        inner: crate::storage::cloud::MockCloudBackend,
    }

    struct ValidateFailureLeaderStore {
        inner: Arc<dyn LeaderStore>,
    }

    struct BlockingRenewalBackend {
        inner: crate::storage::cloud::MockCloudBackend,
        conditional_puts: std::sync::atomic::AtomicUsize,
        renewal_seen: std::sync::Mutex<Option<std::sync::mpsc::Sender<()>>>,
        allow_renewal: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    }

    type PendingPut = (String, Vec<u8>, Vec<(String, String)>);

    struct LateCommitRenewalBackend {
        inner: crate::storage::cloud::MockCloudBackend,
        conditional_puts: std::sync::atomic::AtomicUsize,
        pending: std::sync::Mutex<Option<PendingPut>>,
        old_read_seen: std::sync::Mutex<Option<std::sync::mpsc::Sender<()>>>,
    }

    impl LateCommitRenewalBackend {
        fn commit_pending(&self) {
            let (key, data, headers) = self
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
                .expect("late renewal PUT is pending");
            let (callback, result) = std::sync::mpsc::channel();
            crate::storage::cloud::CloudBackend::submit_put(
                &self.inner,
                &key,
                data,
                headers,
                callback,
            );
            assert!(matches!(
                result.recv_timeout(Duration::from_secs(5)),
                Ok(crate::storage::cloud::CloudEvent::Put {
                    result: crate::storage::cloud::CloudOutcome::Ok(()),
                    ..
                })
            ));
        }
    }

    impl crate::storage::cloud::CloudBackend for LateCommitRenewalBackend {
        fn submit_put(
            &self,
            key: &str,
            data: Vec<u8>,
            headers: Vec<(String, String)>,
            callback: crate::storage::cloud::CloudCallback,
        ) {
            let conditional = headers.iter().any(|(name, _)| {
                name.eq_ignore_ascii_case("if-match") || name.eq_ignore_ascii_case("if-none-match")
            });
            if conditional
                && self
                    .conditional_puts
                    .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
                    == 1
            {
                *self
                    .pending
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) =
                    Some((key.to_string(), data, headers));
                let _ = callback.send(crate::storage::cloud::CloudEvent::Put {
                    key: key.to_string(),
                    result: Err(crate::storage::cloud::CloudError::Transport(
                        "scripted renewal timeout before late commit".to_string(),
                    )),
                });
                return;
            }
            crate::storage::cloud::CloudBackend::submit_put(
                &self.inner,
                key,
                data,
                headers,
                callback,
            );
        }

        fn submit_get(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
            if self
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_some()
            {
                if let Some(signal) = self
                    .old_read_seen
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
                {
                    signal.send(()).expect("signal old reconciliation read");
                }
            }
            crate::storage::cloud::CloudBackend::submit_get(&self.inner, key, callback);
        }

        fn submit_get_with_metadata(
            &self,
            key: &str,
            callback: crate::storage::cloud::CloudCallback,
        ) {
            crate::storage::cloud::CloudBackend::submit_get_with_metadata(
                &self.inner,
                key,
                callback,
            );
        }

        fn submit_get_range(
            &self,
            key: &str,
            start: u64,
            end: Option<u64>,
            callback: crate::storage::cloud::CloudCallback,
        ) {
            crate::storage::cloud::CloudBackend::submit_get_range(
                &self.inner,
                key,
                start,
                end,
                callback,
            );
        }

        fn submit_delete(
            &self,
            key: &str,
            headers: Vec<(String, String)>,
            callback: crate::storage::cloud::CloudCallback,
        ) {
            crate::storage::cloud::CloudBackend::submit_delete(&self.inner, key, headers, callback);
        }

        fn submit_list(&self, prefix: &str, callback: crate::storage::cloud::CloudCallback) {
            crate::storage::cloud::CloudBackend::submit_list(&self.inner, prefix, callback);
        }

        fn submit_head(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
            crate::storage::cloud::CloudBackend::submit_head(&self.inner, key, callback);
        }
    }

    impl crate::storage::cloud::CloudBackend for BlockingRenewalBackend {
        fn submit_put(
            &self,
            key: &str,
            data: Vec<u8>,
            headers: Vec<(String, String)>,
            callback: crate::storage::cloud::CloudCallback,
        ) {
            let conditional = headers.iter().any(|(name, _)| {
                name.eq_ignore_ascii_case("if-match") || name.eq_ignore_ascii_case("if-none-match")
            });
            if conditional
                && self
                    .conditional_puts
                    .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
                    == 1
            {
                if let Some(signal) = self
                    .renewal_seen
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
                {
                    signal.send(()).expect("signal blocked renewal");
                }
                self.allow_renewal
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .recv()
                    .expect("release blocked renewal");
            }
            crate::storage::cloud::CloudBackend::submit_put(
                &self.inner,
                key,
                data,
                headers,
                callback,
            );
        }

        fn submit_get(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
            crate::storage::cloud::CloudBackend::submit_get(&self.inner, key, callback);
        }

        fn submit_get_with_metadata(
            &self,
            key: &str,
            callback: crate::storage::cloud::CloudCallback,
        ) {
            crate::storage::cloud::CloudBackend::submit_get_with_metadata(
                &self.inner,
                key,
                callback,
            );
        }

        fn submit_get_range(
            &self,
            key: &str,
            start: u64,
            end: Option<u64>,
            callback: crate::storage::cloud::CloudCallback,
        ) {
            crate::storage::cloud::CloudBackend::submit_get_range(
                &self.inner,
                key,
                start,
                end,
                callback,
            );
        }

        fn submit_delete(
            &self,
            key: &str,
            headers: Vec<(String, String)>,
            callback: crate::storage::cloud::CloudCallback,
        ) {
            crate::storage::cloud::CloudBackend::submit_delete(&self.inner, key, headers, callback);
        }

        fn submit_list(&self, prefix: &str, callback: crate::storage::cloud::CloudCallback) {
            crate::storage::cloud::CloudBackend::submit_list(&self.inner, prefix, callback);
        }

        fn submit_head(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
            crate::storage::cloud::CloudBackend::submit_head(&self.inner, key, callback);
        }
    }

    impl LeaderStore for ValidateFailureLeaderStore {
        fn acquire_leadership(&self, holder_id: &str) -> Result<LeaderRecord, LeaseError> {
            self.inner.acquire_leadership(holder_id)
        }

        fn read_current(&self) -> Result<Option<LeaderRecord>, LeaseError> {
            self.inner.read_current()
        }

        fn validate_epoch(&self, _expected_epoch: u64) -> Result<(), LeaseError> {
            Err(LeaseError::RenewalFailed(
                "scripted post-renew validation failure".to_string(),
            ))
        }
    }

    impl crate::storage::cloud::CloudBackend for NoCasTokenBackend {
        fn submit_put(
            &self,
            key: &str,
            data: Vec<u8>,
            headers: Vec<(String, String)>,
            callback: crate::storage::cloud::CloudCallback,
        ) {
            crate::storage::cloud::CloudBackend::submit_put(
                &self.inner,
                key,
                data,
                headers,
                callback,
            );
        }

        fn submit_get(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
            crate::storage::cloud::CloudBackend::submit_get(&self.inner, key, callback);
        }

        fn submit_get_with_metadata(
            &self,
            key: &str,
            callback: crate::storage::cloud::CloudCallback,
        ) {
            let (tx, rx) = std::sync::mpsc::channel();
            crate::storage::cloud::CloudBackend::submit_get_with_metadata(&self.inner, key, tx);
            let event = match rx.recv() {
                Ok(crate::storage::cloud::CloudEvent::GetWithMetadata {
                    key,
                    result: Ok((bytes, metadata)),
                }) => crate::storage::cloud::CloudEvent::GetWithMetadata {
                    key,
                    result: Ok((
                        bytes,
                        crate::storage::cloud::ObjectMetadata::new(metadata.size, String::new(), 0),
                    )),
                },
                Ok(other) => other,
                Err(error) => crate::storage::cloud::CloudEvent::GetWithMetadata {
                    key: key.to_string(),
                    result: Err(crate::storage::cloud::CloudError::Transport(
                        error.to_string(),
                    )),
                },
            };
            let _ = callback.send(event);
        }

        fn submit_get_range(
            &self,
            key: &str,
            start: u64,
            end: Option<u64>,
            callback: crate::storage::cloud::CloudCallback,
        ) {
            crate::storage::cloud::CloudBackend::submit_get_range(
                &self.inner,
                key,
                start,
                end,
                callback,
            );
        }

        fn submit_delete(
            &self,
            key: &str,
            headers: Vec<(String, String)>,
            callback: crate::storage::cloud::CloudCallback,
        ) {
            crate::storage::cloud::CloudBackend::submit_delete(&self.inner, key, headers, callback);
        }

        fn submit_list(&self, prefix: &str, callback: crate::storage::cloud::CloudCallback) {
            crate::storage::cloud::CloudBackend::submit_list(&self.inner, prefix, callback);
        }

        fn submit_head(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
            let (tx, rx) = std::sync::mpsc::channel();
            crate::storage::cloud::CloudBackend::submit_head(&self.inner, key, tx);
            let event = match rx.recv() {
                Ok(crate::storage::cloud::CloudEvent::Head {
                    key,
                    result: Ok(metadata),
                }) => crate::storage::cloud::CloudEvent::Head {
                    key,
                    result: Ok(crate::storage::cloud::ObjectMetadata::new(
                        metadata.size,
                        String::new(),
                        0,
                    )),
                },
                Ok(other) => other,
                Err(_) => return,
            };
            let _ = callback.send(event);
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

    // === Provider conditional-write classification: the whole point of
    // typing CloudError before stringification is that only a genuine
    // precondition race becomes LeaseHeld. Every other failure mode must
    // surface as LeaseUnavailable (via LeaseError::IoError), never as
    // confirmed contention. ===

    #[test]
    fn should_not_treat_unauthorized_conditional_write_as_confirmed_contention() {
        // Arrange
        let lease = lease_with_scripted_conditional_put_error(
            crate::storage::cloud::CloudError::Unauthorized("403 Forbidden".to_string()),
        );

        // Act
        let result = Arc::clone(&lease).try_acquire();

        // Assert
        let Err(error) = result else {
            panic!("expected an unauthorized conditional write to fail");
        };
        assert!(
            matches!(error, LeaseError::IoError(_)),
            "an auth failure is not proof another instance holds the lease, got: {error:?}"
        );
        assert!(matches!(
            MidgeError::from(error),
            MidgeError::LeaseUnavailable(_)
        ));
    }

    #[test]
    fn should_not_treat_server_error_conditional_write_as_confirmed_contention() {
        // Arrange
        let lease = lease_with_scripted_conditional_put_error(
            crate::storage::cloud::CloudError::ServerError("500 Internal Server Error".to_string()),
        );

        // Act
        let result = Arc::clone(&lease).try_acquire();

        // Assert
        let Err(error) = result else {
            panic!("expected a server-error conditional write to fail");
        };
        assert!(
            matches!(error, LeaseError::IoError(_)),
            "a provider outage is not proof another instance holds the lease, got: {error:?}"
        );
    }

    #[test]
    fn should_not_treat_transport_failure_conditional_write_as_confirmed_contention() {
        // Arrange
        let lease = lease_with_scripted_conditional_put_error(
            crate::storage::cloud::CloudError::Transport("connection reset".to_string()),
        );

        // Act
        let result = Arc::clone(&lease).try_acquire();

        // Assert
        let Err(error) = result else {
            panic!("expected a transport-failure conditional write to fail");
        };
        assert!(
            matches!(error, LeaseError::IoError(_)),
            "a network failure is not proof another instance holds the lease, got: {error:?}"
        );
    }

    #[test]
    fn should_treat_precondition_failed_conditional_write_as_confirmed_contention() {
        // Arrange
        let lease = lease_with_scripted_conditional_put_error(
            crate::storage::cloud::CloudError::PreconditionFailed("412".to_string()),
        );

        // Act
        let result = Arc::clone(&lease).try_acquire();

        // Assert
        let Err(error) = result else {
            panic!("expected a lost precondition race to fail acquisition");
        };
        assert!(
            matches!(error, LeaseError::AcquisitionFailed(_)),
            "a genuine conditional-write race is confirmed contention, got: {error:?}"
        );
        assert!(matches!(MidgeError::from(error), MidgeError::LeaseHeld(_)));
    }

    #[test]
    fn should_confirm_own_lease_write_by_readback_given_success_response_is_lost() {
        // Arrange
        let lease = lease_with_applied_conditional_put_error(
            crate::storage::cloud::CloudError::ServerError(
                "503 response lost after apply".to_string(),
            ),
        );

        // Act
        let _guard = Arc::clone(&lease)
            .try_acquire()
            .expect("readback should confirm the caller's applied lease document");

        // Assert
        assert!(lease.epoch() > 0);
        assert!(lease.acquired.load(Ordering::Acquire));
    }

    #[test]
    fn should_map_lease_unavailable_error_types_distinctly_through_midge_error() {
        // Arrange
        let lease = lease_with_scripted_conditional_put_error(
            crate::storage::cloud::CloudError::ServerError("500".to_string()),
        );

        // Act
        let result = Arc::clone(&lease).try_acquire();
        let Err(error) = result else {
            panic!("expected a server-error conditional write to fail");
        };
        let midge_error = MidgeError::from(error);

        // Assert: exact public variant, not conflated with LeaseHeld.
        assert!(matches!(midge_error, MidgeError::LeaseUnavailable(_)));
    }

    #[test]
    fn should_not_treat_missing_cas_token_as_confirmed_contention() {
        // Arrange: seed an already-expired lease document so acquisition
        // proceeds to a takeover attempt instead of short-circuiting on
        // "another instance holds it".
        let backend = Arc::new(NoCasTokenBackend {
            inner: crate::storage::cloud::MockCloudBackend::new(),
        });
        let cloud = Arc::new(crate::storage::cloud::CloudStorage::new(
            backend,
            "midge".to_string(),
        ));
        let now = chrono::Utc::now();
        let expired = LeaseDocument {
            epoch: Some(1),
            holder_id: "old-holder@host".to_string(),
            owner_token: Some("old-token".to_string()),
            acquired_at: (now - chrono::Duration::seconds(120)).to_rfc3339(),
            expires_at: (now - chrono::Duration::seconds(60)).to_rfc3339(),
        };
        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_put(
            LEASE_OBJECT_KEY,
            format_lease_document(&expired).into_bytes(),
            vec![],
            tx,
        );
        rx.recv().expect("seed expired lease document");

        let lease = Arc::new(CloudStorageLease::new_provider_backed(
            test_config(),
            temp_cache_path(),
            cloud,
        ));

        // Act
        let result = Arc::clone(&lease).try_acquire();

        // Assert
        let Err(error) = result else {
            panic!("expected takeover to fail without a conditional-update token");
        };
        assert!(
            matches!(error, LeaseError::IoError(_)),
            "a missing CAS token is not proof another instance holds the lease, got: {error:?}"
        );
        assert!(matches!(
            MidgeError::from(error),
            MidgeError::LeaseUnavailable(_)
        ));
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
            owner_token: None,
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
            owner_token: None,
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
    fn should_refuse_simulated_takeover_given_malformed_expiry() {
        // Arrange
        let cache_path = temp_cache_path();
        let document = "epoch: 41\nholder_id: ambiguous-holder@host\nowner_token: ambiguous-token\nacquired_at: 2026-07-31T12:00:00Z\nexpires_at: not-a-timestamp\n";
        std::fs::write(cache_path.join(LEASE_OBJECT_KEY), document).unwrap();
        let lease = Arc::new(CloudStorageLease::new(test_config(), cache_path.clone()));

        // Act
        let result = Arc::clone(&lease).try_acquire();

        // Assert
        assert!(matches!(result, Err(LeaseError::Indeterminate(_))));
        assert_eq!(
            std::fs::read_to_string(cache_path.join(LEASE_OBJECT_KEY)).unwrap(),
            document
        );
        assert_eq!(lease.epoch(), 0);
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
    fn should_repair_malformed_expiry_when_current_simulated_owner_renews() {
        // Arrange
        let cache_path = temp_cache_path();
        let lease = Arc::new(CloudStorageLease::new(test_config(), cache_path));
        let _guard = Arc::clone(&lease).try_acquire().expect("acquire lease");
        let mut owned = lease
            .read_lease_file()
            .expect("read lease")
            .expect("lease exists");
        owned.expires_at = "not-a-timestamp".to_string();
        lease
            .write_lease_file(&owned)
            .expect("write malformed expiry");

        // Act
        let result = lease.renew();

        // Assert
        assert!(result.is_ok());
        let repaired = lease
            .read_lease_file()
            .expect("read repaired lease")
            .expect("repaired lease exists");
        assert!(chrono::DateTime::parse_from_rfc3339(&repaired.expires_at).is_ok());
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
    fn should_not_extend_persisted_lease_given_watchdog_fenced_before_renewal() {
        // Arrange
        let lease = Arc::new(CloudStorageLease::new(test_config(), temp_cache_path()));
        let _guard = Arc::clone(&lease).try_acquire().expect("acquire lease");
        let epoch = lease.epoch();
        let before = lease
            .read_lease_file()
            .expect("read lease document")
            .expect("lease document exists");
        lease.validity.fence(epoch);

        // Act
        let result = lease.renew();
        let after = lease
            .read_lease_file()
            .expect("read lease document")
            .expect("lease document remains");

        // Assert
        assert!(matches!(result, Err(LeaseError::RenewalFailed(_))));
        assert_eq!(after, before);
        assert!(!lease.acquired.load(Ordering::Acquire));
        assert_eq!(lease.acquired_epoch.load(Ordering::Acquire), 0);
        assert!(lease.validity.remaining(epoch).is_err());
    }

    #[test]
    fn should_clear_direct_lease_state_when_post_renew_validation_fails() {
        // Arrange
        let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
        let mut lease =
            CloudStorageLease::new_provider_backed(test_config(), temp_cache_path(), cloud);
        let inner = lease
            .leader_store
            .take()
            .expect("provider leader store installed");
        lease.leader_store = Some(Arc::new(ValidateFailureLeaderStore { inner }));
        let lease = Arc::new(lease);
        let _guard = Arc::clone(&lease).try_acquire().expect("acquire lease");
        let epoch = lease.epoch();

        // Act
        let result = lease.renew();

        // Assert
        assert!(matches!(result, Err(LeaseError::RenewalFailed(_))));
        assert!(!lease.acquired.load(Ordering::Acquire));
        assert_eq!(lease.acquired_epoch.load(Ordering::Acquire), 0);
        assert!(lease.validity.remaining(epoch).is_err());
    }

    #[test]
    fn should_not_resurrect_authority_when_provider_put_lands_after_watchdog_fence() {
        // Arrange
        let (renewal_seen_tx, renewal_seen_rx) = std::sync::mpsc::channel();
        let (allow_renewal_tx, allow_renewal_rx) = std::sync::mpsc::channel();
        let backend = Arc::new(BlockingRenewalBackend {
            inner: crate::storage::cloud::MockCloudBackend::new(),
            conditional_puts: std::sync::atomic::AtomicUsize::new(0),
            renewal_seen: std::sync::Mutex::new(Some(renewal_seen_tx)),
            allow_renewal: std::sync::Mutex::new(allow_renewal_rx),
        });
        let cloud = Arc::new(crate::storage::cloud::CloudStorage::new(
            backend,
            "midge".to_string(),
        ));
        let lease = Arc::new(CloudStorageLease::new_provider_backed(
            test_config(),
            temp_cache_path(),
            Arc::clone(&cloud),
        ));
        let _guard = Arc::clone(&lease).try_acquire().expect("acquire lease");
        let epoch = lease.epoch();
        let renewing = {
            let lease = Arc::clone(&lease);
            std::thread::spawn(move || lease.renew())
        };
        renewal_seen_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("renewal reached backing store");

        // Act: deterministically model monotonic expiry while the provider
        // request is live, then let that stale PUT land.
        lease.validity.fence(epoch);
        allow_renewal_tx.send(()).expect("release renewal PUT");
        let result = renewing.join().expect("renewal thread");

        // Assert
        assert!(matches!(result, Err(LeaseError::RenewalFailed(_))));
        assert!(!lease.acquired.load(Ordering::Acquire));
        assert_eq!(lease.acquired_epoch.load(Ordering::Acquire), 0);
        assert!(lease.validity.remaining(epoch).is_err());
        let persisted = provider_read_doc(&cloud)
            .expect("read reconciled lease")
            .expect("lease record retained for epoch history");
        assert!(
            persisted
                .is_expired_with_tolerance(lease.clock_skew_tolerance)
                .expect("parse reconciled expiry"),
            "late provider side effect must be conditionally expired"
        );
    }

    #[test]
    fn should_eventually_expire_late_renewal_after_timeout_readback_saw_old_object() {
        // Arrange
        let (old_read_tx, old_read_rx) = std::sync::mpsc::channel();
        let backend = Arc::new(LateCommitRenewalBackend {
            inner: crate::storage::cloud::MockCloudBackend::new(),
            conditional_puts: std::sync::atomic::AtomicUsize::new(0),
            pending: std::sync::Mutex::new(None),
            old_read_seen: std::sync::Mutex::new(Some(old_read_tx)),
        });
        let cloud = Arc::new(crate::storage::cloud::CloudStorage::new(
            Arc::clone(&backend) as Arc<dyn crate::storage::cloud::CloudBackend>,
            "midge".to_string(),
        ));
        let stale = Arc::new(CloudStorageLease::new_provider_backed(
            test_config(),
            temp_cache_path(),
            Arc::clone(&cloud),
        ));
        let _guard = Arc::clone(&stale)
            .try_acquire()
            .expect("acquire stale lease");

        // Act: renewal reports timeout, its immediate ambiguity read observes
        // the old record, and only then does the accepted PUT become visible.
        let renew_result = stale.renew();
        old_read_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("immediate reconciliation read old object");
        backend.commit_pending();

        let cleanup_deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let current = provider_read_doc(&cloud)
                .expect("read eventual cleanup state")
                .expect("lease record retained");
            if current
                .is_expired_with_tolerance(stale.clock_skew_tolerance)
                .expect("parse cleanup expiry")
            {
                break;
            }
            assert!(
                Instant::now() < cleanup_deadline,
                "late committed renewal was not eventually expired"
            );
            std::thread::yield_now();
        }

        let takeover = Arc::new(CloudStorageLease::new_provider_backed(
            test_config(),
            temp_cache_path(),
            Arc::clone(&cloud),
        ));
        let _takeover_guard = Arc::clone(&takeover)
            .try_acquire()
            .expect("acquire after reconciled renewal");
        let takeover_epoch = takeover.epoch();
        let takeover_owner = takeover.owner_token.clone();
        std::thread::sleep(Duration::from_millis(30));
        let after_takeover = provider_read_doc(&cloud)
            .expect("read takeover lease")
            .expect("takeover lease remains");

        // Assert
        assert!(matches!(renew_result, Err(LeaseError::IoError(_))));
        assert!(!stale.acquired.load(Ordering::Acquire));
        assert_eq!(stale.acquired_epoch.load(Ordering::Acquire), 0);
        assert_eq!(after_takeover.epoch, Some(takeover_epoch));
        assert_eq!(
            after_takeover.owner_token.as_deref(),
            Some(takeover_owner.as_str())
        );
        assert!(!after_takeover
            .is_expired_with_tolerance(takeover.clock_skew_tolerance)
            .expect("parse takeover expiry"));
    }

    #[test]
    fn should_reject_excessive_public_clock_skew_override_given_safe_default() {
        // Arrange
        let default_lease = CloudStorageLease::new(test_config(), temp_cache_path());
        let excessive = Duration::from_secs(DEFAULT_CLOUD_LEASE_TTL_SECS + 1);

        // Act
        let override_result = CloudStorageLease::new(test_config(), temp_cache_path())
            .with_clock_skew_tolerance(excessive);

        // Assert
        assert_eq!(
            default_lease.clock_skew_tolerance,
            Duration::from_secs(DEFAULT_CLOUD_LEASE_TTL_SECS / 2)
        );
        assert!(matches!(override_result, Err(LeaseError::Internal(_))));
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
    fn should_allow_removing_missing_simulated_lease() {
        // Arrange
        let cache_path = temp_cache_path();
        let lease = CloudStorageLease::new(test_config(), cache_path);

        // Act
        let result = lease.remove_lease_file();

        // Assert
        assert!(result.is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn should_reject_simulated_lease_read_through_symlink() {
        // Arrange
        let cache_path = temp_cache_path();
        let outside_path = temp_cache_path().join("outside-lease");
        let now = chrono::Utc::now();
        let document = LeaseDocument {
            epoch: Some(1),
            holder_id: "outside-holder@host".to_string(),
            owner_token: Some("outside-owner-token".to_string()),
            acquired_at: now.to_rfc3339(),
            expires_at: (now + chrono::Duration::seconds(60)).to_rfc3339(),
        };
        std::fs::write(&outside_path, format_lease_document(&document)).unwrap();
        std::os::unix::fs::symlink(&outside_path, cache_path.join(LEASE_OBJECT_KEY)).unwrap();
        let lease = CloudStorageLease::new(test_config(), cache_path);

        // Act
        let result = lease.read_lease_file();

        // Assert
        assert!(matches!(result, Err(LeaseError::IoError(_))));
    }

    #[cfg(unix)]
    #[test]
    fn should_reject_simulated_lease_removal_through_symlink() {
        // Arrange
        let cache_path = temp_cache_path();
        let outside_path = temp_cache_path().join("outside-lease");
        std::fs::write(&outside_path, "outside lease").unwrap();
        let lease_path = cache_path.join(LEASE_OBJECT_KEY);
        std::os::unix::fs::symlink(&outside_path, &lease_path).unwrap();
        let lease = CloudStorageLease::new(test_config(), cache_path);

        // Act
        let result = lease.remove_lease_file();

        // Assert
        assert!(matches!(result, Err(LeaseError::IoError(_))));
        assert!(lease_path.symlink_metadata().is_ok());
        assert_eq!(
            std::fs::read_to_string(outside_path).unwrap(),
            "outside lease"
        );
    }

    #[test]
    fn should_preserve_newer_owner_given_stale_guard_drop_when_releasing() {
        // Arrange
        let cache_path = temp_cache_path();
        let stale = Arc::new(
            CloudStorageLease::new(test_config(), cache_path.clone())
                .with_clock_skew_tolerance(Duration::ZERO)
                .expect("zero skew tolerance"),
        );
        let _stale_guard = Arc::clone(&stale).try_acquire().unwrap();
        let mut expired = stale
            .read_lease_file()
            .expect("read stale lease")
            .expect("stale lease exists");
        expired.expires_at = (chrono::Utc::now() - chrono::Duration::seconds(1)).to_rfc3339();
        stale.write_lease_file(&expired).unwrap();

        let current = Arc::new(
            CloudStorageLease::new(test_config(), cache_path.clone())
                .with_clock_skew_tolerance(Duration::ZERO)
                .expect("zero skew tolerance"),
        );
        assert_eq!(stale.holder_id(), current.holder_id());
        let _current_guard = Arc::clone(&current).try_acquire().unwrap();

        // Act
        stale.release().unwrap();

        // Assert
        assert!(lease_file_exists(&cache_path));
    }

    #[test]
    fn should_not_renew_newer_simulated_lease_from_stale_same_process_holder() {
        // Arrange
        let cache_path = temp_cache_path();
        let stale = Arc::new(
            CloudStorageLease::new(test_config(), cache_path.clone())
                .with_clock_skew_tolerance(Duration::ZERO)
                .expect("zero skew tolerance"),
        );
        let _stale_guard = Arc::clone(&stale).try_acquire().unwrap();
        let mut expired = stale
            .read_lease_file()
            .expect("read stale lease")
            .expect("stale lease exists");
        expired.expires_at = (chrono::Utc::now() - chrono::Duration::seconds(1)).to_rfc3339();
        stale.write_lease_file(&expired).unwrap();

        let current = Arc::new(
            CloudStorageLease::new(test_config(), cache_path)
                .with_clock_skew_tolerance(Duration::ZERO)
                .expect("zero skew tolerance"),
        );
        let _current_guard = Arc::clone(&current).try_acquire().unwrap();
        let before = current
            .read_lease_file()
            .expect("read current lease")
            .expect("current lease exists");

        // Act
        let result = stale.renew();

        // Assert
        assert!(matches!(result, Err(LeaseError::RenewalFailed(_))));
        let after = current
            .read_lease_file()
            .expect("read current lease")
            .expect("current lease exists");
        assert_eq!(
            format_lease_document(&before),
            format_lease_document(&after)
        );
        assert!(current.renew().is_ok());
    }

    #[test]
    fn should_persist_owner_token_in_simulated_lease_document() {
        // Arrange
        let cache_path = temp_cache_path();
        let lease = Arc::new(CloudStorageLease::new(test_config(), cache_path));

        // Act
        let _guard = Arc::clone(&lease).try_acquire().unwrap();
        let document = lease
            .read_lease_file()
            .expect("read lease")
            .expect("lease exists");

        // Assert
        assert_eq!(
            document.owner_token.as_deref(),
            Some(lease.owner_token.as_str())
        );
    }

    #[test]
    fn should_persist_epoch_in_simulated_lease_document() {
        // Arrange
        let cache_path = temp_cache_path();
        let lease = Arc::new(CloudStorageLease::new(test_config(), cache_path));

        // Act
        let _guard = Arc::clone(&lease).try_acquire().unwrap();
        let document = lease
            .read_lease_file()
            .expect("read lease")
            .expect("lease exists");

        // Assert
        assert_eq!(document.epoch, Some(lease.epoch()));
    }

    #[test]
    fn should_allow_only_one_concurrent_simulated_lease_acquisition() {
        // Arrange
        let cache_path = temp_cache_path();
        let first = Arc::new(CloudStorageLease::new(test_config(), cache_path.clone()));
        let second = Arc::new(CloudStorageLease::new(test_config(), cache_path));
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let first_thread = {
            let first = Arc::clone(&first);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                first.try_acquire()
            })
        };
        let second_thread = {
            let second = Arc::clone(&second);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                second.try_acquire()
            })
        };

        // Act
        barrier.wait();
        let results = [
            first_thread.join().expect("first acquirer panicked"),
            second_thread.join().expect("second acquirer panicked"),
        ];

        // Assert
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    }

    #[test]
    fn should_not_delete_new_holder_lease_given_stale_process_release_when_releasing() {
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
            owner_token: Some("new-holder-token".to_string()),
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
    fn should_grant_exactly_one_acquisition_given_concurrent_provider_racers_when_racing() {
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
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let spawn = |lease: Arc<CloudStorageLease>| {
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                lease.try_acquire()
            })
        };
        let first_thread = spawn(first);
        let second_thread = spawn(second);

        // Act
        barrier.wait();
        let results = [first_thread.join().unwrap(), second_thread.join().unwrap()];

        // Assert
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
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
            owner_token: Some("new-holder-token".to_string()),
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
            owner_token: Some("new-owner-token".to_string()),
            acquired_at: now.to_rfc3339(),
            expires_at: (now + chrono::Duration::seconds(60)).to_rfc3339(),
        };
        put_remote_lease(&cloud, format_lease_document(&newer));

        // Act
        lease.release().expect("stale release is harmless");

        // Assert
        let current = parse_lease_document(&read_remote_lease(&cloud)).expect("parse remote lease");
        assert_eq!(current.epoch, Some(2));
        assert!(!current.is_expired().expect("valid expiry"));
    }

    #[test]
    fn should_increment_remote_epoch_given_expired_provider_lease_when_reacquiring() {
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
    fn should_refuse_provider_takeover_given_malformed_expiry() {
        // Arrange
        let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
        let document = "epoch: 41\nholder_id: ambiguous-holder@host\nowner_token: ambiguous-token\nacquired_at: 2026-07-31T12:00:00Z\nexpires_at: not-a-timestamp\n";
        put_remote_lease(&cloud, document.to_string());
        let lease = Arc::new(CloudStorageLease::new_provider_backed(
            test_config(),
            temp_cache_path(),
            Arc::clone(&cloud),
        ));

        // Act
        let result = Arc::clone(&lease).try_acquire();

        // Assert
        assert!(matches!(result, Err(LeaseError::Indeterminate(_))));
        assert_eq!(read_remote_lease(&cloud), document);
        assert_eq!(lease.epoch(), 0);
    }

    #[test]
    fn should_repair_malformed_expiry_when_current_provider_owner_renews() {
        // Arrange
        let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
        let lease = Arc::new(CloudStorageLease::new_provider_backed(
            test_config(),
            temp_cache_path(),
            Arc::clone(&cloud),
        ));
        let _guard = Arc::clone(&lease).try_acquire().expect("acquire lease");
        let mut owned = parse_lease_document(&read_remote_lease(&cloud)).expect("parse lease");
        owned.expires_at = "not-a-timestamp".to_string();
        put_remote_lease(&cloud, format_lease_document(&owned));

        // Act
        let result = lease.renew();

        // Assert
        assert!(result.is_ok());
        let repaired = parse_lease_document(&read_remote_lease(&cloud)).expect("parse repaired");
        assert!(chrono::DateTime::parse_from_rfc3339(&repaired.expires_at).is_ok());
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
    fn should_report_provider_ownership_change_when_owner_token_changes() {
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
        let successor = LeaseDocument {
            epoch: Some(lease.epoch()),
            holder_id: lease.holder_id(),
            owner_token: Some("successor-owner-token".to_string()),
            acquired_at: now.to_rfc3339(),
            expires_at: (now + chrono::Duration::seconds(60)).to_rfc3339(),
        };
        put_remote_lease(&cloud, format_lease_document(&successor));

        // Act
        let result = lease.renew();

        // Assert
        assert!(matches!(
            result,
            Err(LeaseError::RenewalFailed(message))
                if message.contains("ownership changed")
        ));
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
            owner_token: Some("owner-token".to_string()),
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
        assert_eq!(parsed.owner_token.as_deref(), Some("owner-token"));
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
            owner_token: None,
            acquired_at: (past - chrono::Duration::seconds(30)).to_rfc3339(),
            expires_at: past.to_rfc3339(),
        };

        // Act
        let expired = doc.is_expired().expect("valid expiry");

        // Assert
        assert!(expired);
    }

    #[test]
    fn should_delay_takeover_until_clock_skew_tolerance_elapses() {
        // Arrange
        let now = chrono::Utc::now();
        let document = LeaseDocument {
            epoch: Some(7),
            holder_id: "skewed-holder".to_string(),
            owner_token: Some("token".to_string()),
            acquired_at: (now - chrono::Duration::seconds(30)).to_rfc3339(),
            expires_at: (now - chrono::Duration::seconds(5)).to_rfc3339(),
        };

        // Act
        let without_tolerance = document.is_expired_with_tolerance(Duration::ZERO).unwrap();
        let with_tolerance = document
            .is_expired_with_tolerance(Duration::from_secs(15))
            .unwrap();

        // Assert
        assert!(without_tolerance);
        assert!(!with_tolerance);
    }

    #[test]
    fn should_detect_active_lease() {
        // Arrange
        let future = chrono::Utc::now() + chrono::Duration::seconds(60);
        let doc = LeaseDocument {
            epoch: None,
            holder_id: "test".to_string(),
            owner_token: None,
            acquired_at: chrono::Utc::now().to_rfc3339(),
            expires_at: future.to_rfc3339(),
        };

        // Act
        let expired = doc.is_expired().expect("valid expiry");

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
