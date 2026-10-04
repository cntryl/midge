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
    CloudMetadataGeneration, CloudMetadataHead, LeaderRecord, LeaderStore, LeaseError, LeaseGuard,
    LeaseValidity, PrimaryLease,
};
use crate::io::{staging, Fs, FsError, FsPath, OpenMode, OpenOptions, RealFs};
use crate::storage::cloud::{CloudError, CloudEvent, CloudOutcome, CloudStorage, ObjectMetadata};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Default TTL for cloud leases (30 seconds).
const DEFAULT_CLOUD_LEASE_TTL_SECS: u64 = 30;
// Provider HTTP clients use a 10-second request timeout. Renewal admission
// retains one extra second so a timed-out request cannot still land after the
// holder's monotonic expiry.
pub(crate) const RENEWAL_WRITE_DEADLINE_MARGIN: Duration = Duration::from_secs(11);

/// Key used for the lease object in cloud storage.
const LEASE_OBJECT_KEY: &str = crate::cloud_layout::CloudObjectLayout::LEASE_OBJECT_KEY;
const AUTHORITY_SENTINEL_KEY: &str = "metadata/authority-initialized.v1";
const AUTHORITY_SENTINEL_PENDING_BODY: &[u8] = b"midge cloud metadata authority v2 pending\n";
const AUTHORITY_SENTINEL_BODY: &[u8] = b"midge cloud metadata authority v2 initialized\n";
const METADATA_GENERATIONS_PREFIX: &str = "metadata/generations/";
const MAX_LEASE_DOCUMENT_BYTES: usize = 4096;

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

enum MetadataPointerCasOutcome {
    Committed,
    Retry,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AuthoritySentinelState {
    Missing,
    Pending,
    Active,
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

    fn require_live_authority(
        &self,
        document: &LeaseDocument,
        expected_epoch: u64,
    ) -> Result<(), LeaseError> {
        self.validity.remaining(expected_epoch)?;
        if document.is_expired_with_tolerance(self.clock_skew_tolerance())? {
            return Err(LeaseError::RenewalFailed(
                "cloud lease expired during metadata publication".to_string(),
            ));
        }
        Ok(())
    }

    fn write_metadata_pointer(
        &self,
        replacement: &LeaseDocument,
        metadata: &ObjectMetadata,
        generation: &CloudMetadataGeneration,
        holder_id: &str,
        expected_epoch: u64,
        deadline: Instant,
    ) -> Result<MetadataPointerCasOutcome, LeaseError> {
        if format_lease_document(replacement).len() > MAX_LEASE_DOCUMENT_BYTES {
            return Err(LeaseError::Internal(
                "cloud metadata generation exceeds lease document size bound".to_string(),
            ));
        }
        let validity_write_budget = self
            .validity
            .remaining(expected_epoch)?
            .checked_sub(RENEWAL_WRITE_DEADLINE_MARGIN)
            .filter(|budget| !budget.is_zero())
            .ok_or_else(|| {
                LeaseError::RenewalFailed(
                    "insufficient monotonic validity remains for metadata publication".to_string(),
                )
            })?;
        let write_timeout = deadline
            .saturating_duration_since(Instant::now())
            .min(self.cloud.callback_timeout())
            .min(validity_write_budget);
        if write_timeout.is_zero() {
            return Err(LeaseError::Timeout(
                "cloud metadata publication deadline exhausted before conditional commit"
                    .to_string(),
            ));
        }
        let headers = mutation_precondition_headers(metadata).ok_or_else(|| {
            LeaseError::IoError(
                "cloud lease has no conditional metadata publication token".to_string(),
            )
        })?;
        match provider_write_doc_with_timeout(&self.cloud, replacement, headers, write_timeout) {
            Ok(()) => {
                self.require_live_authority(replacement, expected_epoch)?;
                Ok(MetadataPointerCasOutcome::Committed)
            }
            Err(LeaseError::AcquisitionFailed(_)) => {
                // Definite rejection requires a fresh authority read before retry.
                Ok(MetadataPointerCasOutcome::Retry)
            }
            Err(error) => {
                // The provider may have applied the CAS but lost its response,
                // while a heartbeat renewed the same pointer. Readback proves
                // success only if the caller still owns this exact epoch.
                let read_timeout = deadline
                    .saturating_duration_since(Instant::now())
                    .min(self.cloud.callback_timeout());
                if !read_timeout.is_zero() {
                    if let Ok(Some(actual)) =
                        provider_read_doc_with_timeout(&self.cloud, read_timeout)
                    {
                        if owns_document(&actual, holder_id, &self.owner_token, expected_epoch)
                            && actual.committed_metadata.as_ref() == Some(generation)
                        {
                            self.require_live_authority(&actual, expected_epoch)?;
                            return Ok(MetadataPointerCasOutcome::Committed);
                        }
                    }
                }
                Err(error)
            }
        }
    }
}

impl LeaderStore for ProviderLeaderStore {
    fn acquire_leadership(&self, holder_id: &str) -> Result<LeaderRecord, LeaseError> {
        self.acquire_leadership_with_minimum_epoch(holder_id, 0)
    }

    fn acquire_leadership_with_minimum_epoch(
        &self,
        holder_id: &str,
        minimum_epoch: u64,
    ) -> Result<LeaderRecord, LeaseError> {
        let current = provider_read_doc_with_metadata(&self.cloud, self.cloud.callback_timeout())?;
        let (existing, metadata) = match current {
            Some((document, metadata)) => (Some(document), Some(metadata)),
            None => (None, None),
        };

        if existing
            .as_ref()
            .is_some_and(|document| document.version == LeaseDocumentVersion::Legacy)
        {
            return Err(legacy_metadata_migration_error());
        }

        let sentinel =
            provider_read_authority_sentinel(&self.cloud, self.cloud.callback_timeout())?;
        match (existing.as_ref(), sentinel) {
            (None, AuthoritySentinelState::Missing) => {
                // A pending marker survives a failed first lease create. It
                // becomes active only after the lease CAS has succeeded.
                provider_create_authority_sentinel(&self.cloud, self.cloud.callback_timeout())?;
            }
            (None, AuthoritySentinelState::Pending) | (Some(_), AuthoritySentinelState::Active) => {
            }
            (None, AuthoritySentinelState::Active) => {
                return Err(missing_initialized_lease_error());
            }
            (Some(document), AuthoritySentinelState::Pending)
                if document.committed_metadata.is_none() => {}
            (Some(_), AuthoritySentinelState::Missing) => {
                return Err(missing_authority_sentinel_error());
            }
            (Some(_), AuthoritySentinelState::Pending) => {
                return Err(LeaseError::Indeterminate(
                    "pending cloud metadata authority marker accompanies committed metadata"
                        .to_string(),
                ));
            }
        }

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
            .max(minimum_epoch)
            .checked_add(1)
            .ok_or(LeaseError::EpochExhausted)?;
        let monotonic_now = Instant::now();
        let now = chrono::Utc::now();
        let valid_until = monotonic_now + self.ttl;
        let document = LeaseDocument {
            version: LeaseDocumentVersion::V2,
            epoch: Some(epoch),
            holder_id: holder_id.to_string(),
            owner_token: Some(self.owner_token.clone()),
            acquired_at: now.to_rfc3339(),
            expires_at: (now + CloudStorageLease::persisted_lease_duration(self.ttl)).to_rfc3339(),
            committed_metadata: existing.and_then(|document| document.committed_metadata),
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
        provider_activate_authority_sentinel(&self.cloud, self.cloud.callback_timeout())?;
        if provider_read_doc_with_timeout(&self.cloud, self.cloud.callback_timeout())?.as_ref()
            != Some(&document)
        {
            return Err(LeaseError::RenewalFailed(
                "cloud lease changed before metadata authority marker activation completed"
                    .to_string(),
            ));
        }
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

    fn read_committed_metadata(&self, timeout: Duration) -> Result<CloudMetadataHead, LeaseError> {
        if timeout.is_zero() {
            return Err(LeaseError::Timeout(
                "cloud metadata authority read deadline exhausted".to_string(),
            ));
        }
        let started = Instant::now();
        match provider_read_doc_with_timeout(
            &self.cloud,
            timeout.min(self.cloud.callback_timeout()),
        )? {
            None => {
                let remaining = timeout.saturating_sub(started.elapsed());
                if remaining.is_zero() {
                    return Err(LeaseError::Timeout(
                        "cloud metadata authority read deadline exhausted before sentinel check"
                            .to_string(),
                    ));
                }
                match provider_read_authority_sentinel(
                    &self.cloud,
                    remaining.min(self.cloud.callback_timeout()),
                )? {
                    AuthoritySentinelState::Active => Err(missing_initialized_lease_error()),
                    AuthoritySentinelState::Missing => Ok(CloudMetadataHead::MissingLease),
                    AuthoritySentinelState::Pending => Err(LeaseError::Indeterminate(
                        "pending cloud metadata authority marker has no lease document".to_string(),
                    )),
                }
            }
            Some(document) if document.version == LeaseDocumentVersion::Legacy => {
                Err(legacy_metadata_migration_error())
            }
            Some(document) => {
                let remaining = timeout.saturating_sub(started.elapsed());
                if remaining.is_zero() {
                    return Err(LeaseError::Timeout(
                        "cloud metadata authority read deadline exhausted before sentinel check"
                            .to_string(),
                    ));
                }
                provider_require_authority_sentinel(
                    &self.cloud,
                    remaining.min(self.cloud.callback_timeout()),
                )?;
                Ok(document
                    .committed_metadata
                    .map_or(CloudMetadataHead::Uncommitted, CloudMetadataHead::Committed))
            }
        }
    }

    fn publish_committed_metadata(
        &self,
        holder_id: &str,
        expected_epoch: u64,
        expected_previous: Option<&CloudMetadataGeneration>,
        generation: CloudMetadataGeneration,
        timeout: Duration,
    ) -> Result<(), LeaseError> {
        validate_metadata_generation(&generation)?;
        let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
            LeaseError::Internal("cloud metadata publication deadline is out of range".to_string())
        })?;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(LeaseError::Timeout(
                    "cloud metadata publication deadline exhausted".to_string(),
                ));
            }
            let read_timeout = remaining
                .min(self.cloud.callback_timeout())
                .min(self.validity.remaining(expected_epoch)?);
            let (current, metadata) = provider_read_doc_with_metadata(&self.cloud, read_timeout)?
                .ok_or_else(|| {
                LeaseError::RenewalFailed(
                    "cloud lease disappeared before metadata publication".to_string(),
                )
            })?;
            if current.version == LeaseDocumentVersion::Legacy {
                return Err(legacy_metadata_migration_error());
            }
            let sentinel_timeout = deadline
                .saturating_duration_since(Instant::now())
                .min(self.cloud.callback_timeout());
            if sentinel_timeout.is_zero() {
                return Err(LeaseError::Timeout(
                    "cloud metadata publication deadline exhausted before sentinel check"
                        .to_string(),
                ));
            }
            provider_require_authority_sentinel(&self.cloud, sentinel_timeout)?;
            if !owns_document(&current, holder_id, &self.owner_token, expected_epoch) {
                return Err(LeaseError::RenewalFailed(
                    "cloud lease ownership changed before metadata publication".to_string(),
                ));
            }
            self.require_live_authority(&current, expected_epoch)?;
            if current.committed_metadata.as_ref() != expected_previous {
                if current.committed_metadata.as_ref() == Some(&generation) {
                    return Ok(());
                }
                return Err(LeaseError::RenewalFailed(
                    "cloud metadata authority changed during publication".to_string(),
                ));
            }
            if let Some(committed) = current.committed_metadata.as_ref() {
                if committed.manifest_sequence > generation.manifest_sequence {
                    return Err(LeaseError::RenewalFailed(format!(
                        "cloud metadata sequence {} is ahead of local sequence {}",
                        committed.manifest_sequence, generation.manifest_sequence
                    )));
                }
                if committed == &generation {
                    return Ok(());
                }
            }
            let replacement = LeaseDocument {
                committed_metadata: Some(generation.clone()),
                ..current
            };
            match self.write_metadata_pointer(
                &replacement,
                &metadata,
                &generation,
                holder_id,
                expected_epoch,
                deadline,
            )? {
                MetadataPointerCasOutcome::Committed => return Ok(()),
                MetadataPointerCasOutcome::Retry => {}
            }
        }
    }

    fn validate_epoch_with_timeout(
        &self,
        expected_holder_id: &str,
        expected_epoch: u64,
        timeout: Duration,
    ) -> Result<(), LeaseError> {
        let read_timeout = timeout.min(self.validity.remaining(expected_epoch)?);
        match provider_read_doc_with_timeout(&self.cloud, read_timeout)? {
            Some(document)
                if document.version == LeaseDocumentVersion::V2
                    && owns_document(
                        &document,
                        expected_holder_id,
                        &self.owner_token,
                        expected_epoch,
                    ) =>
            {
                if document.is_expired_with_tolerance(self.clock_skew_tolerance())? {
                    return Err(LeaseError::RenewalFailed(
                        "cloud lease expired before epoch validation".to_string(),
                    ));
                }
                self.validity.remaining(expected_epoch).map(|_| ())
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

    fn validate_epoch(
        &self,
        expected_holder_id: &str,
        expected_epoch: u64,
    ) -> Result<(), LeaseError> {
        self.validate_epoch_with_timeout(
            expected_holder_id,
            expected_epoch,
            self.cloud.callback_timeout(),
        )
    }

    fn renew_leadership(&self, holder_id: &str, expected_epoch: u64) -> Result<(), LeaseError> {
        let renewal_window = self
            .validity
            .remaining(expected_epoch)?
            .checked_sub(RENEWAL_WRITE_DEADLINE_MARGIN)
            .filter(|window| !window.is_zero())
            .ok_or_else(|| {
                LeaseError::RenewalFailed(
                    "insufficient monotonic validity remains for bounded provider renewal"
                        .to_string(),
                )
            })?;
        let deadline = Instant::now().checked_add(renewal_window).ok_or_else(|| {
            LeaseError::Internal("cloud lease renewal deadline is out of range".to_string())
        })?;
        loop {
            let read_timeout = deadline
                .saturating_duration_since(Instant::now())
                .min(self.cloud.callback_timeout())
                .min(self.validity.remaining(expected_epoch)?);
            if read_timeout.is_zero() {
                return Err(LeaseError::RenewalFailed(
                    "cloud lease renewal crossed its bounded provider deadline".to_string(),
                ));
            }
            let (existing, metadata) = provider_read_doc_with_metadata(&self.cloud, read_timeout)?
                .ok_or_else(|| {
                    LeaseError::RenewalFailed("cloud lease document disappeared".to_string())
                })?;
            if existing.version != LeaseDocumentVersion::V2
                || !owns_document(&existing, holder_id, &self.owner_token, expected_epoch)
            {
                return Err(LeaseError::RenewalFailed(format!(
                    "cloud lease ownership changed (holder: {}, epoch: {:?})",
                    existing.holder_id, existing.epoch
                )));
            }
            // A malformed expiry cannot be taken over by another conforming
            // writer; the current locally valid owner may repair it. A valid
            // expiry that has passed is an actual loss of authority.
            match existing.is_expired_with_tolerance(self.clock_skew_tolerance()) {
                Ok(true) => {
                    return Err(LeaseError::RenewalFailed(
                        "cloud lease expired before renewal".to_string(),
                    ));
                }
                Ok(false) | Err(LeaseError::Indeterminate(_)) => {}
                Err(error) => return Err(error),
            }
            let headers = mutation_precondition_headers(&metadata).ok_or_else(|| {
                LeaseError::RenewalFailed(
                    "cloud lease has no token for conditional renewal".to_string(),
                )
            })?;
            let monotonic_now = Instant::now();
            let valid_until = monotonic_now + self.ttl;
            let document = LeaseDocument {
                expires_at: (chrono::Utc::now()
                    + CloudStorageLease::persisted_lease_duration(self.ttl))
                .to_rfc3339(),
                ..existing
            };
            let write_timeout = deadline
                .saturating_duration_since(Instant::now())
                .min(self.cloud.callback_timeout());
            if write_timeout.is_zero() {
                return Err(LeaseError::RenewalFailed(
                    "cloud lease renewal crossed its bounded provider deadline".to_string(),
                ));
            }
            match provider_write_doc_with_timeout(&self.cloud, &document, headers, write_timeout) {
                Ok(()) => {
                    if let Err(error) = self.validity.advance(expected_epoch, valid_until) {
                        let _ = self.release_leadership(holder_id, expected_epoch);
                        return Err(error);
                    }
                    return Ok(());
                }
                Err(LeaseError::AcquisitionFailed(_)) => {
                    // This PUT was definitely rejected. Retry only after
                    // rereading the same holder, token, epoch and live expiry.
                }
                Err(error) => {
                    if matches!(
                        error,
                        LeaseError::IoError(_)
                            | LeaseError::Indeterminate(_)
                            | LeaseError::Timeout(_)
                    ) {
                        // A delayed PUT may still land. Fence before the
                        // reconciler can conditionally expire its result.
                        self.validity.fence(expected_epoch);
                        spawn_ambiguous_renewal_reconciler(
                            Arc::clone(&self.cloud),
                            document,
                            self.clock_skew_tolerance(),
                        );
                    }
                    return Err(error);
                }
            }
        }
    }

    fn release_leadership(&self, holder_id: &str, expected_epoch: u64) -> Result<(), LeaseError> {
        let release_offset = chrono::Duration::from_std(
            self.clock_skew_tolerance()
                .saturating_add(Duration::from_millis(1)),
        )
        .unwrap_or(chrono::Duration::MAX);
        let deadline = Instant::now()
            .checked_add(self.cloud.callback_timeout())
            .ok_or_else(|| {
                LeaseError::Internal("cloud lease release deadline is out of range".to_string())
            })?;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(LeaseError::Timeout(
                    "cloud lease release crossed its provider deadline".to_string(),
                ));
            }
            let Some((current, metadata)) =
                provider_read_doc_with_metadata(&self.cloud, remaining)?
            else {
                return Ok(());
            };
            if current.version != LeaseDocumentVersion::V2 {
                tracing::warn!(%holder_id, "skipping cloud lease release after legacy document replacement");
                return Ok(());
            }
            if !owns_document(&current, holder_id, &self.owner_token, expected_epoch) {
                tracing::warn!(%holder_id, expected_epoch, "skipping stale cloud lease release");
                return Ok(());
            }
            let headers = mutation_precondition_headers(&metadata).ok_or_else(|| {
                LeaseError::IoError("cloud lease release lacks a conditional token".to_string())
            })?;
            let released = LeaseDocument {
                expires_at: (chrono::Utc::now() - release_offset).to_rfc3339(),
                ..current
            };
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(LeaseError::Timeout(
                    "cloud lease release crossed its provider deadline".to_string(),
                ));
            }
            match provider_write_doc_with_timeout(&self.cloud, &released, headers, remaining) {
                Ok(()) => return Ok(()),
                Err(LeaseError::AcquisitionFailed(_)) => {
                    // This PUT was definitely rejected. Re-read the current
                    // epoch and preserve its committed pointer.
                }
                Err(error) => return Err(error),
            }
        }
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

    fn acquire_leadership_with_minimum_epoch(
        &self,
        _holder_id: &str,
        _minimum_epoch: u64,
    ) -> Result<LeaderRecord, LeaseError> {
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
        self.acquire_leadership_with_minimum_epoch(holder_id, 0)
    }

    fn acquire_leadership_with_minimum_epoch(
        &self,
        holder_id: &str,
        minimum_epoch: u64,
    ) -> Result<LeaderRecord, LeaseError> {
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
                    version: LeaseDocumentVersion::Legacy,
                    epoch: Some(record.epoch),
                    holder_id: holder_id.to_string(),
                    owner_token: Some(self.owner_token.clone()),
                    acquired_at: now.to_rfc3339(),
                    expires_at: (now + CloudStorageLease::persisted_lease_duration(self.ttl))
                        .to_rfc3339(),
                    committed_metadata: None,
                })
            },
            minimum_epoch,
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
                expires_at: (now + CloudStorageLease::persisted_lease_duration(self.ttl))
                    .to_rfc3339(),
                ..current
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
/// Provider-backed coordination uses a V2 document. Its required field names
/// differ from the legacy format so older binaries fail to parse and refuse
/// takeover rather than discarding the committed metadata pointer. Existing
/// provider V1 documents require an explicit offline migration. Simulated
/// cloud keeps its local coordination format for deterministic tests.
///
/// The legacy local coordination document looks like:
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
    /// Identity retained for a conditional release retry after local writes are fenced.
    pending_release_epoch: Mutex<Option<u64>>,
    /// Backend selected once at construction; all lifecycle dispatch uses this trait.
    leader_store: Arc<dyn LeaderStore>,
    clock_skew_tolerance: Duration,
}

impl CloudStorageLease {
    /// Lease length written to the shared document. It keeps the TTL's full
    /// precision: truncating to whole seconds would publish an expiry earlier
    /// than the holder's own validity, letting another writer take over while
    /// this one still accepts writes.
    fn persisted_lease_duration(duration: Duration) -> chrono::Duration {
        chrono::Duration::from_std(duration).unwrap_or(chrono::Duration::MAX)
    }

    pub(crate) fn lease_validity(&self) -> Arc<LeaseValidity> {
        Arc::clone(&self.validity)
    }

    /// Create a new cloud storage lease.
    ///
    /// `local_cache_path` must be the local staging directory for cloud storage.
    /// The lease coordination file will be written here.
    #[cfg(test)]
    pub fn new(config: CloudLeaseConfig, local_cache_path: impl AsRef<std::path::Path>) -> Self {
        Self::new_with_ttl(
            config,
            local_cache_path,
            Duration::from_secs(DEFAULT_CLOUD_LEASE_TTL_SECS),
        )
    }

    fn new_with_ttl(
        config: CloudLeaseConfig,
        local_cache_path: impl AsRef<std::path::Path>,
        ttl: Duration,
    ) -> Self {
        let holder_id = format!(
            "{}@{}",
            std::process::id(),
            hostname::get()
                .unwrap_or_else(|_| std::ffi::OsString::from("unknown"))
                .to_string_lossy()
        );
        let owner_token = uuid::Uuid::new_v4().to_string();

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
            pending_release_epoch: Mutex::new(None),
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

    pub(crate) fn new_with_clock_skew_tolerance_and_ttl(
        config: CloudLeaseConfig,
        local_cache_path: impl AsRef<std::path::Path>,
        clock_skew_tolerance: Duration,
        ttl: Duration,
    ) -> Self {
        Self::new_with_ttl(config, local_cache_path, ttl)
            .with_clock_skew_tolerance(clock_skew_tolerance)
            .expect("engine validates lease clock-skew tolerance")
    }

    #[cfg(test)]
    pub fn new_provider_backed(
        config: CloudLeaseConfig,
        local_cache_path: std::path::PathBuf,
        cloud: Arc<CloudStorage>,
    ) -> Self {
        Self::new_provider_backed_with_ttl(
            config,
            local_cache_path,
            cloud,
            Duration::from_secs(DEFAULT_CLOUD_LEASE_TTL_SECS),
        )
    }

    fn new_provider_backed_with_ttl(
        config: CloudLeaseConfig,
        _local_cache_path: std::path::PathBuf,
        cloud: Arc<CloudStorage>,
        ttl: Duration,
    ) -> Self {
        let holder_id = format!(
            "{}@{}",
            std::process::id(),
            hostname::get()
                .unwrap_or_else(|_| std::ffi::OsString::from("unknown"))
                .to_string_lossy()
        );
        let owner_token = uuid::Uuid::new_v4().to_string();
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
            pending_release_epoch: Mutex::new(None),
            leader_store,
            clock_skew_tolerance,
        }
    }

    pub(crate) fn new_provider_backed_with_clock_skew_tolerance_and_ttl(
        config: CloudLeaseConfig,
        local_cache_path: std::path::PathBuf,
        cloud: Arc<CloudStorage>,
        clock_skew_tolerance: Duration,
        ttl: Duration,
    ) -> Self {
        Self::new_provider_backed_with_ttl(config, local_cache_path, cloud, ttl)
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
        self.try_acquire_with_minimum_epoch(0)
    }

    fn try_acquire_with_minimum_epoch(
        self: std::sync::Arc<Self>,
        minimum_epoch: u64,
    ) -> Result<LeaseGuard, LeaseError> {
        // Borrow the inner value for field access (auto-deref handles Arc -> &T)
        let inner: &Self = &self;
        let pending_release = inner
            .pending_release_epoch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        if pending_release.is_some() {
            return Err(LeaseError::AlreadyAcquired(
                "prior cloud lease release is still pending".to_string(),
            ));
        }

        if inner.acquired.load(Ordering::Acquire) {
            return Err(LeaseError::AlreadyAcquired(
                "lease already acquired by this instance".to_string(),
            ));
        }

        let epoch = inner
            .leader_store
            .acquire_leadership_with_minimum_epoch(&inner.holder_id, minimum_epoch)?
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
            // The conditional PUT proves ownership; no separate validation.
            self.leader_store
                .renew_leadership(&self.holder_id, expected_epoch)?;
            tracing::trace!("cloud storage lease renewed");
            Ok(())
        })();

        if let Err(error) = result {
            if self
                .validity
                .is_transient_renewal_failure(&error, expected_epoch)
            {
                return Err(error);
            }
            self.validity.fence(expected_epoch);
            let mut pending_release = self
                .pending_release_epoch
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if self.acquired.load(Ordering::Acquire)
                && self.acquired_epoch.load(Ordering::Acquire) == expected_epoch
            {
                // Writes are fenced, but a persisted lease from this epoch
                // may still need a conditional cleanup attempt.
                *pending_release = Some(expected_epoch);
                self.acquired.store(false, Ordering::Release);
                self.acquired_epoch.store(0, Ordering::Release);
            }
            return Err(error);
        }
        Ok(())
    }

    fn release(&self) -> Result<(), LeaseError> {
        let mut pending_release = self
            .pending_release_epoch
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let released_epoch = if let Some(epoch) = *pending_release {
            epoch
        } else if self.acquired.load(Ordering::Acquire) {
            self.acquired_epoch.load(Ordering::Acquire)
        } else {
            return Ok(()); // Idempotent after confirmed release.
        };
        let result = self
            .leader_store
            .release_leadership(&self.holder_id, released_epoch);
        // A failed provider release leaves the remote lease to expire on its
        // own. The local writer must still stop accepting writes immediately.
        self.validity.deactivate(released_epoch);
        self.acquired.store(false, Ordering::Release);
        self.acquired_epoch.store(0, Ordering::Release);
        *pending_release = result.as_ref().err().map(|_| released_epoch);

        if result.is_ok() {
            tracing::info!(holder_id = %self.holder_id, "cloud storage lease released");
        }
        result
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeaseDocumentVersion {
    Legacy,
    V2,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LeaseDocument {
    version: LeaseDocumentVersion,
    epoch: Option<u64>,
    holder_id: String,
    owner_token: Option<String>,
    acquired_at: String,
    expires_at: String,
    committed_metadata: Option<CloudMetadataGeneration>,
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

/// Format a lease document as a simple line-based text file. V2 encodes its
/// bounded metadata descriptor as JSON on one line.
fn format_lease_document(doc: &LeaseDocument) -> String {
    match doc.version {
        LeaseDocumentVersion::Legacy => {
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
        LeaseDocumentVersion::V2 => {
            let metadata = serde_json::to_string(&doc.committed_metadata)
                .expect("metadata generation has infallible JSON serialization");
            format!(
                "lease_version: 2\nfencing_epoch: {}\nholder: {}\nowner: {}\nacquired: {}\nexpires: {}\nmetadata: {metadata}\n",
                doc.epoch.expect("V2 lease has an epoch"),
                doc.holder_id,
                doc.owner_token.as_deref().expect("V2 lease has an owner token"),
                doc.acquired_at,
                doc.expires_at,
            )
        }
    }
}

/// Parse a lease document from the simple key-value text format.
fn parse_lease_document(content: &str) -> Option<LeaseDocument> {
    if content.len() > MAX_LEASE_DOCUMENT_BYTES {
        return None;
    }
    if content
        .lines()
        .any(|line| line.starts_with("lease_version:"))
    {
        return parse_v2_lease_document(content);
    }
    parse_legacy_lease_document(content)
}

fn parse_legacy_lease_document(content: &str) -> Option<LeaseDocument> {
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
        version: LeaseDocumentVersion::Legacy,
        epoch,
        holder_id: holder_id?,
        owner_token,
        acquired_at: acquired_at?,
        expires_at: expires_at?,
        committed_metadata: None,
    })
}

fn parse_v2_lease_document(content: &str) -> Option<LeaseDocument> {
    if !content.ends_with('\n') || content.contains('\r') {
        return None;
    }
    let mut version = None;
    let mut epoch = None;
    let mut holder_id = None;
    let mut owner_token = None;
    let mut acquired_at = None;
    let mut expires_at = None;
    let mut committed_metadata = None;
    for line in content.lines() {
        let (name, value) = line.split_once(": ")?;
        match name {
            "lease_version" if version.replace(value).is_none() => {}
            "fencing_epoch" if epoch.replace(value.parse::<u64>().ok()?).is_none() => {}
            "holder" if holder_id.replace(value.to_string()).is_none() => {}
            "owner" if owner_token.replace(value.to_string()).is_none() => {}
            "acquired" if acquired_at.replace(value.to_string()).is_none() => {}
            "expires" if expires_at.replace(value.to_string()).is_none() => {}
            "metadata"
                if committed_metadata
                    .replace(serde_json::from_str::<Option<CloudMetadataGeneration>>(value).ok()?)
                    .is_none() => {}
            _ => return None,
        }
    }
    if version? != "2" {
        return None;
    }
    let epoch = epoch?;
    let holder_id = holder_id?;
    let owner_token = owner_token?;
    let acquired_at = acquired_at?;
    let expires_at = expires_at?;
    if epoch == 0 || holder_id.is_empty() || owner_token.is_empty() || acquired_at.is_empty() {
        return None;
    }
    let committed_metadata = committed_metadata?;
    if let Some(generation) = committed_metadata.as_ref() {
        validate_metadata_generation(generation).ok()?;
    }
    Some(LeaseDocument {
        version: LeaseDocumentVersion::V2,
        epoch: Some(epoch),
        holder_id,
        owner_token: Some(owner_token),
        acquired_at,
        expires_at,
        committed_metadata,
    })
}

fn validate_metadata_generation(generation: &CloudMetadataGeneration) -> Result<(), LeaseError> {
    if generation.objects.is_empty()
        || generation.objects.len() > crate::metadata::files::CLOUD_MIRRORED.len()
    {
        return Err(LeaseError::Internal(
            "cloud metadata generation has an invalid object count".to_string(),
        ));
    }
    let mut seen_files = std::collections::HashSet::new();
    for object in &generation.objects {
        if !crate::metadata::files::CLOUD_MIRRORED.contains(&object.file_name.as_str())
            || !seen_files.insert(object.file_name.as_str())
        {
            return Err(LeaseError::Internal(format!(
                "cloud metadata generation has an invalid or duplicate filename '{}'",
                object.file_name
            )));
        }
        let Some(relative) = object.object_key.strip_prefix(METADATA_GENERATIONS_PREFIX) else {
            return Err(LeaseError::Internal(format!(
                "cloud metadata '{}' has an invalid immutable object key",
                object.file_name
            )));
        };
        let Some((generation_id, file_name)) = relative.split_once('/') else {
            return Err(LeaseError::Internal(format!(
                "cloud metadata '{}' has an invalid immutable object key",
                object.file_name
            )));
        };
        let parsed_id = uuid::Uuid::parse_str(generation_id).map_err(|_| {
            LeaseError::Internal(format!(
                "cloud metadata '{}' has an invalid generation ID",
                object.file_name
            ))
        })?;
        if parsed_id.to_string() != generation_id || file_name != object.file_name {
            return Err(LeaseError::Internal(format!(
                "cloud metadata '{}' has a noncanonical immutable object key",
                object.file_name
            )));
        }
    }
    if !seen_files.contains(crate::metadata::files::FORMAT)
        || !seen_files.contains(crate::metadata::files::MANIFEST_SNAPSHOT)
    {
        return Err(LeaseError::Internal(
            "cloud metadata generation lacks FORMAT or manifest snapshot".to_string(),
        ));
    }
    Ok(())
}

fn legacy_metadata_migration_error() -> LeaseError {
    LeaseError::Indeterminate(
        "legacy cloud lease document requires an offline metadata-authority migration".to_string(),
    )
}

fn missing_initialized_lease_error() -> LeaseError {
    LeaseError::Indeterminate(
        "cloud metadata authority was initialized but the lease document is missing; refusing to reset its committed pointer"
            .to_string(),
    )
}

fn missing_authority_sentinel_error() -> LeaseError {
    LeaseError::Indeterminate(
        "V2 cloud lease is missing its permanent metadata authority sentinel".to_string(),
    )
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
        CloudError::Timeout(_) => {
            LeaseError::Timeout(format!("cloud lease {operation} failed: {error}"))
        }
        other => LeaseError::IoError(format!("cloud lease {operation} failed: {other}")),
    }
}

fn provider_read_authority_sentinel(
    cloud: &CloudStorage,
    timeout: Duration,
) -> Result<AuthoritySentinelState, LeaseError> {
    let (tx, rx) = std::sync::mpsc::channel();
    cloud.submit_get(AUTHORITY_SENTINEL_KEY, tx);
    match rx.recv_timeout(timeout) {
        Ok(CloudEvent::Get { result, .. }) => match result {
            CloudOutcome::Ok(bytes) => parse_authority_sentinel(&bytes),
            CloudOutcome::Err(error) if error.is_not_found() => Ok(AuthoritySentinelState::Missing),
            CloudOutcome::Err(error) => Err(classify_lease_read_error("sentinel GET", &error)),
        },
        Ok(other) => Err(LeaseError::Indeterminate(format!(
            "unexpected cloud metadata authority sentinel response: {other:?}"
        ))),
        Err(error) => Err(LeaseError::Timeout(format!(
            "cloud metadata authority sentinel GET: {error}"
        ))),
    }
}

fn parse_authority_sentinel(bytes: &[u8]) -> Result<AuthoritySentinelState, LeaseError> {
    if bytes == AUTHORITY_SENTINEL_PENDING_BODY {
        Ok(AuthoritySentinelState::Pending)
    } else if bytes == AUTHORITY_SENTINEL_BODY {
        Ok(AuthoritySentinelState::Active)
    } else {
        Err(LeaseError::Indeterminate(
            "cloud metadata authority sentinel is malformed".to_string(),
        ))
    }
}

fn provider_read_authority_sentinel_with_metadata(
    cloud: &CloudStorage,
    timeout: Duration,
) -> Result<Option<(AuthoritySentinelState, ObjectMetadata)>, LeaseError> {
    let (tx, rx) = std::sync::mpsc::channel();
    cloud.submit_get_with_metadata(AUTHORITY_SENTINEL_KEY, tx);
    match rx.recv_timeout(timeout) {
        Ok(CloudEvent::GetWithMetadata { result, .. }) => match result {
            CloudOutcome::Ok((bytes, metadata)) => {
                Ok(Some((parse_authority_sentinel(&bytes)?, metadata)))
            }
            CloudOutcome::Err(error) if error.is_not_found() => Ok(None),
            CloudOutcome::Err(error) => Err(classify_lease_read_error("sentinel GET", &error)),
        },
        Ok(other) => Err(LeaseError::Indeterminate(format!(
            "unexpected cloud metadata authority sentinel response: {other:?}"
        ))),
        Err(error) => Err(LeaseError::Timeout(format!(
            "cloud metadata authority sentinel GET: {error}"
        ))),
    }
}

fn provider_require_authority_sentinel(
    cloud: &CloudStorage,
    timeout: Duration,
) -> Result<(), LeaseError> {
    match provider_read_authority_sentinel(cloud, timeout)? {
        AuthoritySentinelState::Active => Ok(()),
        AuthoritySentinelState::Missing => Err(missing_authority_sentinel_error()),
        AuthoritySentinelState::Pending => Err(LeaseError::Indeterminate(
            "cloud metadata authority sentinel has not been activated".to_string(),
        )),
    }
}

fn provider_create_authority_sentinel(
    cloud: &CloudStorage,
    timeout: Duration,
) -> Result<(), LeaseError> {
    let mut headers = vec![("If-None-Match".to_string(), "*".to_string())];
    crate::storage::cloud::set_request_timeout_header(&mut headers, timeout);
    let (tx, rx) = std::sync::mpsc::channel();
    cloud.submit_put(
        AUTHORITY_SENTINEL_KEY,
        AUTHORITY_SENTINEL_PENDING_BODY.to_vec(),
        headers,
        tx,
    );
    match rx.recv_timeout(timeout) {
        Ok(CloudEvent::Put {
            result: CloudOutcome::Ok(()),
            ..
        }) => Ok(()),
        Ok(CloudEvent::Put {
            result: CloudOutcome::Err(error),
            ..
        }) if error.is_precondition_failed() => Err(LeaseError::Indeterminate(
            "another initializer created the cloud metadata authority sentinel before the lease"
                .to_string(),
        )),
        Ok(CloudEvent::Put {
            result: CloudOutcome::Err(error),
            ..
        }) => Err(LeaseError::IoError(format!(
            "cloud metadata authority sentinel create failed: {error}"
        ))),
        Ok(other) => Err(LeaseError::Indeterminate(format!(
            "unexpected cloud metadata authority sentinel create response: {other:?}"
        ))),
        Err(error) => Err(LeaseError::Timeout(format!(
            "cloud metadata authority sentinel create: {error}"
        ))),
    }
}

fn provider_activate_authority_sentinel(
    cloud: &CloudStorage,
    timeout: Duration,
) -> Result<(), LeaseError> {
    let started = Instant::now();
    let Some((state, metadata)) = provider_read_authority_sentinel_with_metadata(cloud, timeout)?
    else {
        return Err(missing_authority_sentinel_error());
    };
    match state {
        AuthoritySentinelState::Active => return Ok(()),
        AuthoritySentinelState::Missing => return Err(missing_authority_sentinel_error()),
        AuthoritySentinelState::Pending => {}
    }
    let headers = mutation_precondition_headers(&metadata).ok_or_else(|| {
        LeaseError::IoError("pending cloud metadata authority sentinel has no CAS token".into())
    })?;
    let remaining = timeout.saturating_sub(started.elapsed());
    if remaining.is_zero() {
        return Err(LeaseError::Timeout(
            "cloud metadata authority sentinel activation deadline exhausted".into(),
        ));
    }
    let mut headers = headers;
    crate::storage::cloud::set_request_timeout_header(&mut headers, remaining);
    let (tx, rx) = std::sync::mpsc::channel();
    cloud.submit_put(
        AUTHORITY_SENTINEL_KEY,
        AUTHORITY_SENTINEL_BODY.to_vec(),
        headers,
        tx,
    );
    let result = match rx.recv_timeout(remaining) {
        Ok(CloudEvent::Put {
            result: CloudOutcome::Ok(()),
            ..
        }) => return Ok(()),
        Ok(CloudEvent::Put {
            result: CloudOutcome::Err(error),
            ..
        }) => LeaseError::IoError(format!(
            "cloud metadata authority sentinel activation failed: {error}"
        )),
        Ok(other) => LeaseError::Indeterminate(format!(
            "unexpected cloud metadata authority sentinel activation response: {other:?}"
        )),
        Err(error) => LeaseError::Timeout(format!(
            "cloud metadata authority sentinel activation timed out: {error}"
        )),
    };
    let remaining = timeout.saturating_sub(started.elapsed());
    if !remaining.is_zero()
        && matches!(
            provider_read_authority_sentinel(cloud, remaining.min(cloud.callback_timeout())),
            Ok(AuthoritySentinelState::Active)
        )
    {
        return Ok(());
    }
    Err(result)
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
        Err(error) => Err(LeaseError::Timeout(format!("cloud lease GET: {error}"))),
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
        Err(error) => Err(LeaseError::Timeout(format!("cloud lease GET: {error}"))),
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
    let started = Instant::now();
    crate::storage::cloud::set_request_timeout_header(&mut headers, timeout);
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
            // These typed responses establish that this PUT did not commit.
            // A fresh read can retry the CAS under the caller's existing
            // deadline; failure of that read does not make this PUT uncertain.
            CloudOutcome::Err(
                error @ (CloudError::PreconditionFailed(_) | CloudError::ConditionalConflict(_)),
            ) => Err(LeaseError::AcquisitionFailed(format!(
                "cloud lease conditional write was rejected: {error}"
            ))),
            CloudOutcome::Err(error) => reconcile_ambiguous_lease_write(
                cloud,
                document,
                format!("cloud lease conditional write failed: {error}"),
                timeout.saturating_sub(started.elapsed()),
            ),
        },
        Ok(other) => reconcile_ambiguous_lease_write(
            cloud,
            document,
            format!("unexpected cloud lease PUT response: {other:?}"),
            timeout.saturating_sub(started.elapsed()),
        ),
        Err(error) => reconcile_ambiguous_lease_write(
            cloud,
            document,
            format!("cloud lease PUT timed out: {error}"),
            timeout.saturating_sub(started.elapsed()),
        ),
    }
}

fn reconcile_ambiguous_lease_write(
    cloud: &CloudStorage,
    expected: &LeaseDocument,
    original_error: String,
    remaining: Duration,
) -> Result<(), LeaseError> {
    if remaining.is_zero() {
        return Err(LeaseError::Indeterminate(format!(
            "{original_error}; cloud lease write deadline expired before readback"
        )));
    }
    match provider_read_doc_with_timeout(cloud, remaining.min(cloud.callback_timeout())) {
        Ok(Some(actual)) if actual == *expected => {
            tracing::info!(
                holder_id = %expected.holder_id,
                epoch = ?expected.epoch,
                "confirmed ambiguous cloud lease write by readback"
            );
            Ok(())
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
