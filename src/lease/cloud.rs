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
use std::sync::{Arc, Mutex};
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
    clock_skew_tolerance: Mutex<Duration>,
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
            clock_skew_tolerance: Mutex::new(clock_skew_tolerance),
        }
    }
}

impl LeaderStore for ProviderLeaderStore {
    fn acquire_leadership(&self, holder_id: &str) -> Result<LeaderRecord, LeaseError> {
        let current = provider_read_doc_with_metadata(&self.cloud, self.cloud.callback_timeout())?;
        let (existing, metadata) = match current {
            Some((document, metadata)) => (Some(document), Some(metadata)),
            None => (None, None),
        };

        if let Some(existing) = existing.as_ref() {
            if !existing.is_expired_with_tolerance(self.clock_skew_tolerance())? {
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

    fn validate_epoch_with_timeout(
        &self,
        expected_holder_id: &str,
        expected_epoch: u64,
        timeout: Duration,
    ) -> Result<(), LeaseError> {
        match provider_read_doc_with_timeout(&self.cloud, timeout)? {
            Some(document)
                if document.epoch == Some(expected_epoch)
                    && document.holder_id == expected_holder_id =>
            {
                Ok(())
            }
            Some(document) => Err(LeaseError::RenewalFailed(format!(
                "epoch/holder mismatch: expected holder={expected_holder_id} epoch={expected_epoch}, found holder={} epoch={}",
                document.holder_id,
                document.epoch.unwrap_or(0)
            ))),
            None => Err(LeaseError::RenewalFailed(
                "leader record missing".to_string(),
            )),
        }
    }

    fn renew_leadership(&self, holder_id: &str, expected_epoch: u64) -> Result<(), LeaseError> {
        let (existing, metadata) =
            provider_read_doc_with_metadata(&self.cloud, self.validity.remaining(expected_epoch)?)?
                .ok_or_else(|| {
                    LeaseError::RenewalFailed("cloud lease document disappeared".to_string())
                })?;
        let headers = mutation_precondition_headers(&metadata).ok_or_else(|| {
            LeaseError::RenewalFailed(
                "cloud lease has no token for conditional renewal".to_string(),
            )
        })?;
        if !owns_document(&existing, holder_id, &self.owner_token, expected_epoch) {
            return Err(LeaseError::RenewalFailed(format!(
                "cloud lease ownership changed (holder: {}, epoch: {:?})",
                existing.holder_id, existing.epoch
            )));
        }

        let monotonic_now = Instant::now();
        let now = chrono::Utc::now();
        let valid_until = monotonic_now + self.ttl;
        let document = LeaseDocument {
            epoch: Some(expected_epoch),
            holder_id: holder_id.to_string(),
            owner_token: Some(self.owner_token.clone()),
            acquired_at: existing.acquired_at,
            expires_at: (now
                + chrono::Duration::seconds(CloudStorageLease::lease_ttl_seconds_i64(self.ttl)))
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
        if let Err(error) =
            provider_write_doc_with_timeout(&self.cloud, &document, headers, write_timeout)
        {
            if matches!(error, LeaseError::IoError(_) | LeaseError::Indeterminate(_)) {
                spawn_ambiguous_renewal_reconciler(
                    Arc::clone(&self.cloud),
                    document,
                    self.clock_skew_tolerance(),
                );
            }
            return Err(error);
        }
        if let Err(error) = self.validity.advance(expected_epoch, valid_until) {
            let _ = self.release_leadership(holder_id, expected_epoch);
            return Err(error);
        }
        Ok(())
    }

    fn release_leadership(&self, holder_id: &str, expected_epoch: u64) -> Result<(), LeaseError> {
        let Some((current, metadata)) =
            provider_read_doc_with_metadata(&self.cloud, self.cloud.callback_timeout())?
        else {
            return Ok(());
        };
        let Some(headers) = mutation_precondition_headers(&metadata) else {
            tracing::warn!(%holder_id, "skipping cloud lease release without conditional token");
            return Ok(());
        };
        if !owns_document(&current, holder_id, &self.owner_token, expected_epoch) {
            tracing::warn!(%holder_id, expected_epoch, "skipping stale cloud lease release");
            return Ok(());
        }
        let release_offset = chrono::Duration::from_std(
            self.clock_skew_tolerance()
                .saturating_add(Duration::from_millis(1)),
        )
        .unwrap_or(chrono::Duration::MAX);
        let released = LeaseDocument {
            expires_at: (chrono::Utc::now() - release_offset).to_rfc3339(),
            ..current
        };
        provider_write_doc(&self.cloud, &released, headers)
    }

    fn set_clock_skew_tolerance(&self, tolerance: Duration) -> Result<(), LeaseError> {
        *self.clock_skew_tolerance.lock().map_err(|_| {
            LeaseError::Internal("cloud lease tolerance lock poisoned".to_string())
        })? = tolerance;
        Ok(())
    }
}

impl ProviderLeaderStore {
    fn clock_skew_tolerance(&self) -> Duration {
        *self
            .clock_skew_tolerance
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn owns_document(
    document: &LeaseDocument,
    holder_id: &str,
    owner_token: &str,
    expected_epoch: u64,
) -> bool {
    document.holder_id == holder_id
        && document.owner_token.as_deref() == Some(owner_token)
        && document.epoch == Some(expected_epoch)
}

struct UnavailableLeaderStore {
    error: String,
}

impl UnavailableLeaderStore {
    fn new(error: String) -> Self {
        Self { error }
    }

    fn failure(&self) -> LeaseError {
        LeaseError::IoError(format!(
            "simulated-cloud lease store is unavailable: {}",
            self.error
        ))
    }
}

impl LeaderStore for UnavailableLeaderStore {
    fn acquire_leadership(&self, _holder_id: &str) -> Result<LeaderRecord, LeaseError> {
        Err(self.failure())
    }

    fn read_current(&self) -> Result<Option<LeaderRecord>, LeaseError> {
        Err(self.failure())
    }

    fn set_clock_skew_tolerance(&self, _tolerance: Duration) -> Result<(), LeaseError> {
        Ok(())
    }
}

/// Filesystem-backed cloud lease simulation selected once during construction.
struct SimulatedLeaderStore {
    inner: FsLeaderStore,
    fs: Arc<dyn Fs>,
    owner_token: String,
    ttl: Duration,
    validity: Arc<LeaseValidity>,
    clock_skew_tolerance: Mutex<Duration>,
}

impl SimulatedLeaderStore {
    fn new(
        fs: Arc<dyn Fs>,
        owner_token: String,
        ttl: Duration,
        validity: Arc<LeaseValidity>,
        clock_skew_tolerance: Duration,
    ) -> Self {
        Self {
            inner: FsLeaderStore::new(Arc::clone(&fs)),
            fs,
            owner_token,
            ttl,
            validity,
            clock_skew_tolerance: Mutex::new(clock_skew_tolerance),
        }
    }

    fn clock_skew_tolerance(&self) -> Duration {
        *self
            .clock_skew_tolerance
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn read_document(&self) -> Result<Option<LeaseDocument>, LeaseError> {
        let path = FsPath::new(LEASE_OBJECT_KEY);
        match self.fs.exists(&path) {
            Ok(false) => return Ok(None),
            Ok(true) => {}
            Err(error) => {
                return Err(LeaseError::IoError(format!(
                    "failed to check lease file existence: {error}"
                )))
            }
        }
        let metadata = match self.fs.metadata(&path) {
            Ok(metadata) => metadata,
            Err(FsError::NotFound(_)) => return Ok(None),
            Err(error) => {
                return Err(LeaseError::IoError(format!(
                    "failed to read lease file metadata: {error}"
                )))
            }
        };
        let file = match self.fs.open(
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
                )))
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

    fn write_document(&self, document: &LeaseDocument) -> Result<(), LeaseError> {
        let content = format_lease_document(document);
        let temp_path = FsPath::new(format!("{LEASE_OBJECT_KEY}.{}.tmp", self.owner_token));
        staging::stage_bytes(
            &self.fs,
            &temp_path,
            &FsPath::new(LEASE_OBJECT_KEY),
            content.as_bytes(),
            LeaseError::IoError,
        )
    }

    fn remove_document(&self) -> Result<(), LeaseError> {
        match self.fs.remove_file(&FsPath::new(LEASE_OBJECT_KEY)) {
            Ok(()) | Err(FsError::NotFound(_)) => Ok(()),
            Err(error) => Err(LeaseError::IoError(format!(
                "failed to remove lease file: {error}"
            ))),
        }
    }
}

impl LeaderStore for SimulatedLeaderStore {
    fn acquire_leadership(&self, holder_id: &str) -> Result<LeaderRecord, LeaseError> {
        let monotonic_now = Instant::now();
        let now = chrono::Utc::now();
        let valid_until = monotonic_now + self.ttl;
        let record = self.inner.acquire_leadership_after_validation_and_publish(
            holder_id,
            |_| {
                if let Some(existing) = self.read_document()? {
                    if !existing.is_expired_with_tolerance(self.clock_skew_tolerance())? {
                        return Err(LeaseError::AcquisitionFailed(format!(
                            "another instance holds the lease (holder: {}, expires: {})",
                            existing.holder_id, existing.expires_at
                        )));
                    }
                }
                Ok(())
            },
            |record| {
                self.write_document(&LeaseDocument {
                    epoch: Some(record.epoch),
                    holder_id: holder_id.to_string(),
                    owner_token: Some(self.owner_token.clone()),
                    acquired_at: now.to_rfc3339(),
                    expires_at: (now
                        + chrono::Duration::seconds(CloudStorageLease::lease_ttl_seconds_i64(
                            self.ttl,
                        )))
                    .to_rfc3339(),
                })
            },
            0,
        )?;
        self.validity.activate(record.epoch, valid_until)?;
        Ok(record)
    }

    fn read_current(&self) -> Result<Option<LeaderRecord>, LeaseError> {
        self.inner.read_current()
    }

    fn renew_leadership(&self, holder_id: &str, expected_epoch: u64) -> Result<(), LeaseError> {
        let valid_until = self.inner.with_exclusive_lock(holder_id, || {
            let current = self.read_document()?.ok_or_else(|| {
                LeaseError::RenewalFailed("simulated-cloud lease document disappeared".to_string())
            })?;
            if !owns_document(&current, holder_id, &self.owner_token, expected_epoch) {
                return Err(LeaseError::RenewalFailed(format!(
                    "simulated-cloud lease ownership changed (holder: {}, epoch: {:?})",
                    current.holder_id, current.epoch
                )));
            }
            match self.inner.read_current()? {
                Some(record) if record.holder_id == holder_id && record.epoch == expected_epoch => {
                }
                Some(record) => {
                    return Err(LeaseError::RenewalFailed(format!(
                        "simulated-cloud leader changed (holder: {}, epoch: {})",
                        record.holder_id, record.epoch
                    )))
                }
                None => {
                    return Err(LeaseError::RenewalFailed(
                        "simulated-cloud leader record disappeared".to_string(),
                    ))
                }
            }
            let monotonic_now = Instant::now();
            let now = chrono::Utc::now();
            let valid_until = monotonic_now + self.ttl;
            self.validity.remaining(expected_epoch)?;
            self.write_document(&LeaseDocument {
                epoch: Some(expected_epoch),
                holder_id: holder_id.to_string(),
                owner_token: Some(self.owner_token.clone()),
                acquired_at: current.acquired_at,
                expires_at: (now
                    + chrono::Duration::seconds(CloudStorageLease::lease_ttl_seconds_i64(
                        self.ttl,
                    )))
                .to_rfc3339(),
            })?;
            Ok(valid_until)
        })?;
        self.inner.refresh_timestamp(holder_id, expected_epoch)?;
        if let Err(error) = self.validity.advance(expected_epoch, valid_until) {
            let _ = self.release_leadership(holder_id, expected_epoch);
            return Err(error);
        }
        Ok(())
    }

    fn release_leadership(&self, holder_id: &str, expected_epoch: u64) -> Result<(), LeaseError> {
        let removed = self.inner.with_exclusive_lock(holder_id, || {
            let Some(current) = self.read_document()? else {
                return Ok(false);
            };
            if !owns_document(&current, holder_id, &self.owner_token, expected_epoch) {
                tracing::warn!(%holder_id, expected_epoch, "skipping stale simulated-cloud lease release");
                return Ok(false);
            }
            self.remove_document()?;
            Ok(true)
        })?;
        if removed && expected_epoch > 0 {
            self.inner.release_if_owner(holder_id, expected_epoch)?;
        }
        Ok(())
    }

    fn set_clock_skew_tolerance(&self, tolerance: Duration) -> Result<(), LeaseError> {
        *self.clock_skew_tolerance.lock().map_err(|_| {
            LeaseError::Internal("simulated lease tolerance lock poisoned".to_string())
        })? = tolerance;
        Ok(())
    }

    #[cfg(test)]
    fn read_test_coordination_document(&self) -> Result<Option<String>, LeaseError> {
        self.read_document()
            .map(|document| document.map(|document| format_lease_document(&document)))
    }

    #[cfg(test)]
    fn write_test_coordination_document(&self, content: &str) -> Result<(), LeaseError> {
        let document = parse_lease_document(content).ok_or_else(|| {
            LeaseError::Internal("test lease coordination document is malformed".to_string())
        })?;
        self.write_document(&document)
    }

    #[cfg(test)]
    fn remove_test_coordination_document(&self) -> Result<(), LeaseError> {
        self.remove_document()
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
    /// Random per-instance token exposed to backend-focused tests.
    #[cfg(test)]
    owner_token: String,
    /// TTL for the lease.
    ttl: Duration,
    /// Whether we currently hold the lease.
    acquired: AtomicBool,
    /// Monotonic validity for the current cloud-lease acquisition.
    validity: Arc<LeaseValidity>,
    /// Epoch from the active coordination store, set after successful acquisition.
    acquired_epoch: std::sync::atomic::AtomicU64,
    /// Backend selected once at construction; all lifecycle dispatch uses this trait.
    leader_store: Arc<dyn LeaderStore>,
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

        let ttl = Duration::from_secs(DEFAULT_CLOUD_LEASE_TTL_SECS);
        let clock_skew_tolerance = Duration::from_secs(DEFAULT_CLOUD_LEASE_TTL_SECS / 2);
        let validity = Arc::new(LeaseValidity::new());
        let leader_store: Arc<dyn LeaderStore> = match RealFs::new(local_cache_path) {
            Ok(fs) => {
                let fs: Arc<dyn Fs> = Arc::new(fs);
                Arc::new(SimulatedLeaderStore::new(
                    fs,
                    owner_token.clone(),
                    ttl,
                    Arc::clone(&validity),
                    clock_skew_tolerance,
                ))
            }
            Err(error) => Arc::new(UnavailableLeaderStore::new(error.to_string())),
        };

        Self {
            config,
            holder_id,
            #[cfg(test)]
            owner_token,
            ttl,
            acquired: AtomicBool::new(false),
            validity,
            acquired_epoch: std::sync::atomic::AtomicU64::new(0),
            leader_store,
            clock_skew_tolerance,
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
        self.leader_store.set_clock_skew_tolerance(tolerance)?;
        self.clock_skew_tolerance = tolerance;
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
        _local_cache_path: std::path::PathBuf,
        cloud: Arc<CloudStorage>,
    ) -> Self {
        let holder_id = format!(
            "{}@{}",
            std::process::id(),
            hostname::get()
                .unwrap_or_else(|_| std::ffi::OsString::from("unknown"))
                .to_string_lossy()
        );
        let owner_token = uuid::Uuid::new_v4().to_string();
        let ttl = Duration::from_secs(DEFAULT_CLOUD_LEASE_TTL_SECS);
        let clock_skew_tolerance = Duration::from_secs(DEFAULT_CLOUD_LEASE_TTL_SECS / 2);
        let validity = Arc::new(LeaseValidity::new());
        let leader_store = Arc::new(ProviderLeaderStore::new(
            cloud,
            ttl,
            owner_token.clone(),
            Arc::clone(&validity),
            clock_skew_tolerance,
        ));
        Self {
            config,
            holder_id,
            #[cfg(test)]
            owner_token,
            ttl,
            acquired: AtomicBool::new(false),
            validity,
            acquired_epoch: std::sync::atomic::AtomicU64::new(0),
            leader_store,
            clock_skew_tolerance,
        }
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

    #[cfg(test)]
    fn read_lease_file(&self) -> Result<Option<LeaseDocument>, LeaseError> {
        self.leader_store
            .read_test_coordination_document()?
            .map(|content| {
                parse_lease_document(&content).ok_or_else(|| {
                    LeaseError::Indeterminate(
                        "local lease coordination document is malformed".to_string(),
                    )
                })
            })
            .transpose()
    }

    #[cfg(test)]
    fn write_lease_file(&self, document: &LeaseDocument) -> Result<(), LeaseError> {
        self.leader_store
            .write_test_coordination_document(&format_lease_document(document))
    }

    #[cfg(test)]
    fn remove_lease_file(&self) -> Result<(), LeaseError> {
        self.leader_store.remove_test_coordination_document()
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

        let epoch = inner
            .leader_store
            .acquire_leadership(&inner.holder_id)?
            .epoch;
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
            self.leader_store
                .renew_leadership(&self.holder_id, expected_epoch)?;
            self.leader_store
                .validate_epoch(&self.holder_id, expected_epoch)?;
            tracing::trace!("cloud storage lease renewed");
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
        self.leader_store
            .release_leadership(&self.holder_id, released_epoch)?;
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
        Some(Arc::clone(&self.leader_store))
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
    provider_read_doc_with_timeout(cloud, cloud.callback_timeout())
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
    provider_write_doc_with_timeout(cloud, document, headers, cloud.callback_timeout())
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
#[path = "cloud/tests.rs"]
mod tests;
