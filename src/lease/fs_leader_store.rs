//! Filesystem-based leader store using CAS-via-rename.
//!
//! Provides distributed-safe leader election for local disk, NFS, SMB, and any
//! backend that supports atomic rename. Lease records use compare-and-swap via
//! rename; short-lived advisory locks fence admitted WAL effects and serialize
//! lease transitions with them.
//!
//! ## CAS protocol
//!
//! 1. Read current leader record from `.midge_leader` (if any).
//! 2. Write `LeaderRecord { epoch: current + 1, .. }` to a temp file.
//! 3. Fsync the temp file.
//! 4. Atomic rename temp → `.midge_leader`.
//! 5. Fsync the parent directory.
//! 6. Re-read `.midge_leader` to confirm our `holder_id` won the race.

use super::traits::{
    format_leader_record, parse_leader_record, LeaderRecord, LeaderStore, LeaseError,
};
use crate::io::{staging, Durability, Fs, FsError, FsLock, FsPath, OpenMode, OpenOptions};
use std::sync::Arc;
use std::time::Duration;

/// Well-known filename for the leader record.
const LEADER_RECORD_FILE: &str = ".midge_leader";
const LEADER_LOCK_FILE: &str = ".midge_leader.lock";
const WRITER_EFFECT_LOCK_FILE: &str = ".midge_writer_effect.v2.lock";
const WRITER_EFFECT_FALLBACK_LOCK_FILE: &str = ".midge_writer_effect.v2.fallback.lock";
const METADATA_POINTER_FILE: &str = ".midge_shared_metadata";

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct MetadataPointer {
    format_version: u32,
    generation: super::CloudMetadataGeneration,
}

/// Exact timestamp written by owned clean release, retaining the fencing epoch.
pub(super) const RELEASED_TIMESTAMP: &str = "1970-01-01T00:00:00Z";

/// Filesystem-based `LeaderStore` using temp-write + fsync + atomic rename.
pub struct FsLeaderStore {
    fs: Arc<dyn Fs>,
    clock: crate::lease::clock::LeaseClock,
}

impl FsLeaderStore {
    /// Create a new filesystem leader store backed by the given `Fs`, which
    /// stamps its records from `clock`.
    pub(crate) fn new(fs: Arc<dyn Fs>, clock: crate::lease::clock::LeaseClock) -> Self {
        Self { fs, clock }
    }

    /// Build the temp file name for CAS.
    ///
    /// Includes PID, a per-process counter, and thread ID to ensure
    /// uniqueness even when multiple threads/instances call concurrently.
    fn temp_path() -> FsPath {
        static TEMP_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = TEMP_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        FsPath::new(format!(".midge_leader.{}.{}.tmp", std::process::id(), n))
    }

    fn lock_contents(holder_id: &str, owner_token: &str, created_at: &str) -> String {
        format!("holder_id={holder_id}\nowner_token={owner_token}\ncreated_at={created_at}\n")
    }

    fn read_lock_contents(&self, lock_path: &FsPath) -> Result<Option<String>, LeaseError> {
        match self.fs.exists(lock_path) {
            Ok(false) => return Ok(None),
            Ok(true) => {}
            Err(error) => {
                return Err(LeaseError::IoError(format!(
                    "failed to check leader lock presence: {error}"
                )))
            }
        }

        let file = self
            .fs
            .open(
                lock_path,
                OpenOptions {
                    mode: OpenMode::ReadOnly,
                    create: false,
                    create_new: false,
                    truncate: false,
                },
            )
            .map_err(|error| LeaseError::IoError(format!("failed to open leader lock: {error}")))?;
        let len = file.len().map_err(|error| {
            LeaseError::IoError(format!("failed to read leader lock length: {error}"))
        })?;
        if len == 0 {
            return Ok(Some(String::new()));
        }
        let data = file
            .read_at(0, len)
            .map_err(|error| LeaseError::IoError(format!("failed to read leader lock: {error}")))?;
        String::from_utf8(data.to_vec())
            .map(Some)
            .map_err(|error| LeaseError::IoError(format!("leader lock is not UTF-8: {error}")))
    }

    fn parse_lock_owner_token(content: &str) -> Option<&str> {
        content
            .lines()
            .find_map(|line| line.strip_prefix("owner_token="))
            .filter(|value| !value.is_empty())
    }

    /// Remove the acquisition lock only while it still carries the ownership
    /// token observed by the caller. A stale guard or cleanup attempt must not
    /// unlink a lock created by a newer acquirer.
    fn remove_lock_if_owner(
        &self,
        lock_path: &FsPath,
        expected_owner_token: &str,
        durability: Durability,
    ) -> Result<bool, LeaseError> {
        let Some(content) = self.read_lock_contents(lock_path)? else {
            return Ok(false);
        };
        if Self::parse_lock_owner_token(&content) != Some(expected_owner_token) {
            return Ok(false);
        }

        self.fs.remove_file(lock_path).map_err(|error| {
            LeaseError::IoError(format!("failed to remove lease lock: {error}"))
        })?;
        if durability == Durability::Durable {
            self.fs
                .sync_dir(&FsPath::new("."), Durability::Durable)
                .map_err(|error| {
                    LeaseError::IoError(format!("failed to persist leader lock removal: {error}"))
                })?;
        }
        Ok(true)
    }

    fn acquire_lock(&self, holder_id: &str) -> Result<LockGuard<'_>, LeaseError> {
        self.acquire_lock_at(holder_id, &FsPath::new(LEADER_LOCK_FILE), "lease mutation")
    }

    /// Serialize a lease transition with any admitted writer effect. A
    /// transition waits for an in-flight effect to finish rather than
    /// failing release and forcing the next opener to wait for the full TTL.
    fn acquire_effect_transition_locks(
        &self,
        holder_id: &str,
    ) -> Result<LockGuard<'_>, LeaseError> {
        let deadline = crate::common::OperationDeadline::from_budget(
            crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
        );
        let mut contended_path = FsPath::new(WRITER_EFFECT_LOCK_FILE);
        loop {
            if deadline.is_expired() {
                return Err(LeaseError::Timeout(format!(
                    "lease transition timed out waiting for writer storage effect lock {contended_path}"
                )));
            }
            match self.try_acquire_effect_lock(holder_id, Durability::Durable) {
                Ok(lock) => return Ok(lock),
                Err(LockAttemptError::Contended(path)) => {
                    contended_path = path;
                    std::thread::sleep(deadline.remaining().min(Duration::from_millis(1)));
                }
                Err(LockAttemptError::Failed(error)) => return Err(error),
            }
        }
    }

    #[cfg(test)]
    fn acquire_effect_lock_once(
        &self,
        holder_id: &str,
        fallback_durability: Durability,
    ) -> Result<LockGuard<'_>, LeaseError> {
        self.try_acquire_effect_lock(holder_id, fallback_durability)
            .map_err(|error| match error {
                LockAttemptError::Contended(path) => LeaseError::AcquisitionFailed(format!(
                    "another writer storage effect is in progress for holder {holder_id} at {path}"
                )),
                LockAttemptError::Failed(error) => error,
            })
    }

    fn try_acquire_effect_lock(
        &self,
        holder_id: &str,
        fallback_durability: Durability,
    ) -> Result<LockGuard<'_>, LockAttemptError> {
        let path = FsPath::new(WRITER_EFFECT_LOCK_FILE);
        match self.fs.try_lock_exclusive(&path) {
            Ok(lock) => Ok(LockGuard::filesystem(lock)),
            Err(FsError::Unsupported(_)) => self.try_acquire_lock_file(
                holder_id,
                FsPath::new(WRITER_EFFECT_FALLBACK_LOCK_FILE),
                "writer storage effect",
                fallback_durability,
            ),
            Err(FsError::AlreadyExists(_)) => Err(LockAttemptError::Contended(path)),
            Err(error) => Err(LockAttemptError::Failed(LeaseError::IoError(format!(
                "failed to acquire writer storage effect lock {path}: {error}"
            )))),
        }
    }

    fn acquire_writer_effect_lock_until(
        &self,
        holder_id: &str,
        timeout: Duration,
    ) -> Result<LockGuard<'_>, LeaseError> {
        let deadline = crate::common::OperationDeadline::from_budget(timeout);
        let mut contended_path = FsPath::new(WRITER_EFFECT_LOCK_FILE);
        loop {
            if deadline.is_expired() {
                return Err(LeaseError::Timeout(
                    format!("writer storage effect timed out waiting for same-epoch effect at {contended_path}"),
                ));
            }
            match self.try_acquire_effect_lock(holder_id, Durability::Unsafe) {
                Ok(lock) => return Ok(lock),
                Err(LockAttemptError::Contended(path)) => {
                    contended_path = path;
                    // Lease transitions use this same effect lock, so epoch
                    // validation remains stable while an admitted effect is
                    // finishing. Validate once after acquiring the lock.
                    std::thread::sleep(deadline.remaining().min(Duration::from_millis(1)));
                }
                Err(LockAttemptError::Failed(error)) => return Err(error),
            }
        }
    }

    fn try_acquire_lock_file(
        &self,
        holder_id: &str,
        lock_path: FsPath,
        purpose: &str,
        durability: Durability,
    ) -> Result<LockGuard<'_>, LockAttemptError> {
        let now = self.clock.now();
        let owner_token = uuid::Uuid::new_v4().to_string();
        let payload = Self::lock_contents(holder_id, &owner_token, &now.to_rfc3339());

        match self.fs.open(
            &lock_path,
            OpenOptions {
                mode: OpenMode::ReadWrite,
                create: true,
                create_new: true,
                truncate: false,
            },
        ) {
            Ok(mut lock_file) => {
                let initialized = lock_file
                    .write_at(0, bytes::Bytes::from(payload))
                    .and_then(|()| lock_file.sync(durability));
                drop(lock_file);
                if let Err(error) = initialized {
                    if let Err(remove_error) = self.fs.remove_file(&lock_path) {
                        tracing::warn!(path = %lock_path, %remove_error, "failed to remove partially initialized lease lock");
                    }
                    return Err(LockAttemptError::Failed(LeaseError::IoError(format!(
                        "failed to initialize {purpose} lock {lock_path}: {error}"
                    ))));
                }
                Ok(LockGuard::file(
                    self,
                    owner_token,
                    lock_path,
                    durability,
                ))
            }
            Err(FsError::AlreadyExists(_)) => Err(LockAttemptError::Contended(lock_path)),
            Err(error) => Err(LockAttemptError::Failed(LeaseError::AcquisitionFailed(
                format!(
                    "failed to create {purpose} lock {lock_path}: {error}; automatic stale-lock removal is unsafe because filesystem renames are not cancellable"
                ),
            ))),
        }
    }

    fn acquire_lock_at(
        &self,
        holder_id: &str,
        lock_path: &FsPath,
        purpose: &str,
    ) -> Result<LockGuard<'_>, LeaseError> {
        match self.try_acquire_lock_file(holder_id, lock_path.clone(), purpose, Durability::Durable) {
            Ok(lock) => Ok(lock),
            Err(LockAttemptError::Contended(path)) => Err(LeaseError::AcquisitionFailed(format!(
                "another {purpose} is in progress at {path}; automatic stale-lock removal is unsafe because filesystem renames are not cancellable"
            ))),
            Err(LockAttemptError::Failed(error)) => Err(error),
        }
    }
}

enum LockAttemptError {
    Contended(FsPath),
    Failed(LeaseError),
}

impl LeaderStore for FsLeaderStore {
    fn read_committed_metadata(
        &self,
        timeout: std::time::Duration,
    ) -> Result<super::CloudMetadataHead, LeaseError> {
        if timeout.is_zero() {
            return Err(LeaseError::Timeout(
                "shared metadata pointer read deadline exhausted".into(),
            ));
        }
        let scope = crate::common::DeadlineScope::new(
            crate::common::OperationDeadline::from_budget(timeout),
        );
        let scoped = Self::new(
            crate::io::scope_fs(Arc::clone(&self.fs), scope.clone()),
            self.clock.clone(),
        );
        let current = scoped.read_current()?;
        let path = FsPath::new(METADATA_POINTER_FILE);
        let file = match scoped.fs.open(&path, OpenOptions::READ_ONLY) {
            Ok(file) => file,
            Err(crate::io::FsError::NotFound(_)) => {
                return Ok(if current.is_some() {
                    super::CloudMetadataHead::Uncommitted
                } else {
                    super::CloudMetadataHead::MissingLease
                })
            }
            Err(error) => {
                return Err(LeaseError::IoError(format!(
                    "read shared metadata pointer: {error}"
                )))
            }
        };
        if current.is_none() {
            return Err(LeaseError::Indeterminate(
                "shared metadata exists without its fencing record".into(),
            ));
        }
        let len = file
            .len()
            .map_err(|error| LeaseError::IoError(error.to_string()))?;
        if len > 64 * 1024 {
            return Err(LeaseError::Indeterminate(
                "shared metadata pointer exceeds size limit".into(),
            ));
        }
        let bytes = file
            .read_at(0, len)
            .map_err(|error| LeaseError::IoError(error.to_string()))?;
        let pointer: MetadataPointer = serde_json::from_slice(&bytes).map_err(|error| {
            LeaseError::Indeterminate(format!("invalid shared metadata pointer: {error}"))
        })?;
        if pointer.format_version != 1 {
            return Err(LeaseError::Indeterminate(
                "unsupported shared metadata pointer format".into(),
            ));
        }
        super::cloud::validate_metadata_generation(&pointer.generation)?;
        scope
            .check("shared metadata pointer read completion")
            .map_err(|error| LeaseError::Timeout(error.to_string()))?;
        Ok(super::CloudMetadataHead::Committed(pointer.generation))
    }

    fn publish_committed_metadata(
        &self,
        holder_id: &str,
        expected_epoch: u64,
        expected_previous: Option<&super::CloudMetadataGeneration>,
        generation: super::CloudMetadataGeneration,
        timeout: std::time::Duration,
    ) -> Result<(), LeaseError> {
        if timeout.is_zero() {
            return Err(LeaseError::Timeout(
                "shared metadata publication deadline exhausted".into(),
            ));
        }
        super::cloud::validate_metadata_generation(&generation)?;
        let scope = crate::common::DeadlineScope::new(
            crate::common::OperationDeadline::from_budget(timeout),
        );
        let fs = crate::io::scope_fs(Arc::clone(&self.fs), scope.clone());
        let scoped = Self::new(Arc::clone(&fs), self.clock.clone());
        scoped.with_exclusive_lock(holder_id, || {
            scope
                .check("shared metadata mutation lock")
                .map_err(|error| LeaseError::Timeout(error.to_string()))?;
            scoped.validate_epoch(holder_id, expected_epoch)?;
            let previous = match scoped.read_committed_metadata(scope.deadline().remaining())? {
                super::CloudMetadataHead::Committed(previous) => Some(previous),
                super::CloudMetadataHead::Uncommitted => None,
                super::CloudMetadataHead::MissingLease => {
                    return Err(LeaseError::RenewalFailed(
                        "shared metadata lease disappeared".into(),
                    ))
                }
            };
            scope
                .check("shared metadata predecessor read")
                .map_err(|error| LeaseError::Timeout(error.to_string()))?;
            if previous.as_ref() != expected_previous {
                if previous.as_ref() == Some(&generation) {
                    return Ok(());
                }
                return Err(LeaseError::RenewalFailed(
                    "shared metadata predecessor changed".into(),
                ));
            }
            generation.validate_successor(previous.as_ref())?;
            if previous.as_ref() == Some(&generation) {
                return Ok(());
            }
            let pointer = MetadataPointer {
                format_version: 1,
                generation,
            };
            let bytes = serde_json::to_vec(&pointer)
                .map_err(|error| LeaseError::Internal(error.to_string()))?;
            staging::stage_bytes_with_hook(
                &fs,
                &Self::temp_path(),
                &FsPath::new(METADATA_POINTER_FILE),
                &bytes,
                || {
                    scope
                        .check("shared metadata pointer commit")
                        .map_err(|error| LeaseError::Timeout(error.to_string()))
                },
                LeaseError::IoError,
            )
        })
    }

    fn acquire_leadership(&self, holder_id: &str) -> Result<LeaderRecord, LeaseError> {
        self.acquire_leadership_after_validation(holder_id, |_| Ok(()))
    }

    fn acquire_leadership_with_minimum_epoch(
        &self,
        holder_id: &str,
        minimum_epoch: u64,
    ) -> Result<LeaderRecord, LeaseError> {
        self.acquire_leadership_after_validation_and_publish(
            holder_id,
            |_| Ok(()),
            |_| Ok(()),
            minimum_epoch,
        )
    }

    fn read_current(&self) -> Result<Option<LeaderRecord>, LeaseError> {
        let path = FsPath::new(LEADER_RECORD_FILE);

        match self.fs.exists(&path) {
            Ok(true) => {}
            Ok(false) => {
                if self
                    .fs
                    .exists(&FsPath::new(METADATA_POINTER_FILE))
                    .map_err(|error| LeaseError::IoError(error.to_string()))?
                {
                    return Err(LeaseError::Indeterminate(
                        "shared metadata pointer exists without its fencing record".into(),
                    ));
                }
                return Ok(None);
            }
            Err(e) => return Err(LeaseError::IoError(format!("exists check failed: {e}"))),
        }

        let opts = OpenOptions {
            mode: OpenMode::ReadOnly,
            create: false,
            create_new: false,
            truncate: false,
        };

        let file = match self.fs.open(&path, opts) {
            Ok(f) => f,
            // The record may have been removed between exists() and open().
            Err(crate::io::FsError::NotFound(_)) => return Ok(None),
            Err(e) => {
                return Err(LeaseError::IoError(format!(
                    "failed to open leader record: {e}"
                )));
            }
        };

        let len = file.len().map_err(|e| {
            LeaseError::IoError(format!("failed to read leader record length: {e}"))
        })?;

        // Only a genuinely absent file (ENOENT, handled above/below) means
        // "no lease ever held" — an *existing* file that is empty,
        // non-UTF-8, or missing a required field means something was there
        // and its state can't be trusted, which is a stricter, distinct
        // condition from absence. Fail closed as Indeterminate rather than
        // collapsing it into Ok(None), which would silently reset fencing
        // state for the next acquirer. lsm-spec format/lease.md §7, §6 item 3.
        if len == 0 {
            return Err(LeaseError::Indeterminate(
                "leader record file exists but is empty".to_string(),
            ));
        }

        let data = file
            .read_at(0, len)
            .map_err(|e| LeaseError::IoError(format!("failed to read leader record: {e}")))?;

        let content = String::from_utf8(data.to_vec())
            .map_err(|e| LeaseError::Indeterminate(format!("leader record not UTF-8: {e}")))?;

        parse_leader_record(&content).map(Some).ok_or_else(|| {
            LeaseError::Indeterminate(
                "leader record is missing one or more required fields".to_string(),
            )
        })
    }

    fn with_writer_epoch(
        &self,
        expected_holder_id: &str,
        expected_epoch: u64,
        timeout: std::time::Duration,
        operation: &mut dyn FnMut(),
    ) -> Result<(), LeaseError> {
        if timeout.is_zero() {
            return Err(LeaseError::Timeout(
                "writer effect deadline exhausted".into(),
            ));
        }
        let _effect_lock = self.acquire_writer_effect_lock_until(expected_holder_id, timeout)?;
        self.validate_epoch(expected_holder_id, expected_epoch)?;
        operation();
        Ok(())
    }

    fn validate_epoch(
        &self,
        expected_holder_id: &str,
        expected_epoch: u64,
    ) -> Result<(), LeaseError> {
        match self.read_current()? {
            Some(record)
                if record.holder_id == expected_holder_id
                    && record.epoch == expected_epoch
                    && record.acquired_at != RELEASED_TIMESTAMP =>
            {
                Ok(())
            }
            Some(record) => Err(LeaseError::RenewalFailed(format!(
                "epoch/holder mismatch or released lease: expected holder={expected_holder_id} epoch={expected_epoch}, found holder={} epoch={}",
                record.holder_id, record.epoch
            ))),
            None => Err(LeaseError::RenewalFailed(
                "leader record missing".to_string(),
            )),
        }
    }

    fn validate_write_admission(
        &self,
        _expected_holder_id: &str,
        _expected_epoch: u64,
        _timeout: Duration,
    ) -> Result<(), LeaseError> {
        // The filesystem writer's per-effect fence validates and holds the
        // epoch lock around each actual append and sync. A separate admission
        // read would duplicate that work without narrowing the effect race.
        Ok(())
    }

    fn validate_epoch_before_sync(
        &self,
        _expected_holder_id: &str,
        _expected_epoch: u64,
        _timeout: Duration,
    ) -> Result<(), LeaseError> {
        // `with_writer_epoch` validates under the same lock as physical fsync.
        Ok(())
    }
}

enum LockGuardKind<'a> {
    File {
        store: &'a FsLeaderStore,
        owner_token: String,
        lock_path: FsPath,
        durability: Durability,
    },
    Filesystem {
        _lock: Box<dyn FsLock>,
    },
}

struct LockGuard<'a> {
    kind: LockGuardKind<'a>,
}

impl<'a> LockGuard<'a> {
    fn file(
        store: &'a FsLeaderStore,
        owner_token: String,
        lock_path: FsPath,
        durability: Durability,
    ) -> Self {
        Self {
            kind: LockGuardKind::File {
                store,
                owner_token,
                lock_path,
                durability,
            },
        }
    }

    fn filesystem(lock: Box<dyn FsLock>) -> Self {
        Self {
            kind: LockGuardKind::Filesystem { _lock: lock },
        }
    }

    fn remove_file_lock_now(&self) -> Result<bool, LeaseError> {
        match &self.kind {
            LockGuardKind::File {
                store,
                owner_token,
                lock_path,
                durability,
            } => store.remove_lock_if_owner(lock_path, owner_token, *durability),
            LockGuardKind::Filesystem { .. } => Ok(false),
        }
    }
}

impl Drop for LockGuard<'_> {
    fn drop(&mut self) {
        if let LockGuardKind::File {
            store,
            owner_token,
            lock_path,
            durability,
        } = &self.kind
        {
            if let Err(error) = store.remove_lock_if_owner(lock_path, owner_token, *durability) {
                tracing::warn!(%error, "failed to release lease lock");
            }
        }
    }
}

impl FsLeaderStore {
    pub(crate) fn acquire_leadership_after_validation<F>(
        &self,
        holder_id: &str,
        validate_current: F,
    ) -> Result<LeaderRecord, LeaseError>
    where
        F: FnOnce(Option<&LeaderRecord>) -> Result<(), LeaseError>,
    {
        self.acquire_leadership_after_validation_and_publish(
            holder_id,
            validate_current,
            |_| Ok(()),
            0,
        )
    }

    /// As [`Self::acquire_leadership_after_validation_and_publish`], but
    /// guaranteeing the granted epoch is at least
    /// `max(current_epoch, minimum_epoch) + 1`. See
    /// `LeaderStore::acquire_leadership_with_minimum_epoch`.
    pub(crate) fn acquire_leadership_after_validation_and_publish<V, P>(
        &self,
        holder_id: &str,
        validate_current: V,
        publish: P,
        minimum_epoch: u64,
    ) -> Result<LeaderRecord, LeaseError>
    where
        V: FnOnce(Option<&LeaderRecord>) -> Result<(), LeaseError>,
        P: FnOnce(&LeaderRecord) -> Result<(), LeaseError>,
    {
        let _effect_lock = self.acquire_effect_transition_locks(holder_id)?;
        let _lock = self.acquire_lock(holder_id)?;
        let current = self.read_current()?;
        validate_current(current.as_ref())?;
        let record = self.acquire_inner(holder_id, minimum_epoch)?;
        publish(&record)?;
        Ok(record)
    }

    pub(crate) fn with_exclusive_lock<T, F>(
        &self,
        holder_id: &str,
        operation: F,
    ) -> Result<T, LeaseError>
    where
        F: FnOnce() -> Result<T, LeaseError>,
    {
        let _lock = self.acquire_lock(holder_id)?;
        operation()
    }

    /// Refresh the leader record's `acquired_at` timestamp without changing
    /// the epoch.  Used by the heartbeat to keep the record "fresh" so that
    /// another process can distinguish a live writer from a crashed one.
    ///
    /// Returns `Ok(())` if the record was refreshed, or an error if the
    /// current record does not match the expected holder/epoch.
    pub fn refresh_timestamp(
        &self,
        holder_id: &str,
        expected_epoch: u64,
    ) -> Result<(), LeaseError> {
        let _lock = self.acquire_lock(holder_id)?;
        // Read current record and verify ownership
        let current = self.read_current()?;
        match current {
            Some(rec)
                if rec.holder_id == holder_id
                    && rec.epoch == expected_epoch
                    && rec.acquired_at != RELEASED_TIMESTAMP => {}
            Some(rec) => {
                return Err(LeaseError::RenewalFailed(format!(
                    "leader record changed: expected holder={} epoch={}, got holder={} epoch={}",
                    holder_id, expected_epoch, rec.holder_id, rec.epoch
                )));
            }
            None => {
                return Err(LeaseError::RenewalFailed(
                    "leader record missing during refresh".to_string(),
                ));
            }
        }

        // Write updated record with fresh timestamp (same epoch)
        let now = self.clock.now().to_rfc3339();
        let refreshed = LeaderRecord {
            epoch: expected_epoch,
            holder_id: holder_id.to_string(),
            acquired_at: now,
        };

        let content = format_leader_record(&refreshed);
        let temp = Self::temp_path();
        let target = FsPath::new(LEADER_RECORD_FILE);

        staging::stage_bytes(
            &self.fs,
            &temp,
            &target,
            content.as_bytes(),
            LeaseError::IoError,
        )?;

        let _ = self.fs.sync_dir(&FsPath::new("."), Durability::Durable);

        match self.read_current()? {
            Some(record) if record.holder_id == holder_id && record.epoch == expected_epoch => {}
            Some(record) => {
                return Err(LeaseError::RenewalFailed(format!(
                    "leader record changed after refresh: expected holder={holder_id} epoch={expected_epoch}, got holder={} epoch={}",
                    record.holder_id, record.epoch
                )));
            }
            None => {
                return Err(LeaseError::RenewalFailed(
                    "leader record disappeared after refresh".to_string(),
                ));
            }
        }

        Ok(())
    }

    /// Mark a lease released only if the current record still belongs to the
    /// expected holder and epoch. A stale releaser must never overwrite a new
    /// leader's fencing record.
    pub fn release_if_owner(
        &self,
        holder_id: &str,
        expected_epoch: u64,
    ) -> Result<bool, LeaseError> {
        let _effect_lock = self.acquire_effect_transition_locks(holder_id)?;
        let lock = self.acquire_lock(holder_id)?;
        let Some(current) = self.read_current()? else {
            return Ok(false);
        };
        if current.holder_id != holder_id || current.epoch != expected_epoch {
            return Ok(false);
        }

        let released = LeaderRecord {
            epoch: expected_epoch,
            holder_id: holder_id.to_string(),
            acquired_at: RELEASED_TIMESTAMP.to_string(),
        };
        let temp = Self::temp_path();
        staging::stage_bytes(
            &self.fs,
            &temp,
            &FsPath::new(LEADER_RECORD_FILE),
            format_leader_record(&released).as_bytes(),
            LeaseError::IoError,
        )?;
        lock.remove_file_lock_now()?;
        Ok(true)
    }

    /// Core CAS logic, called under the lock file held by `acquire_leadership`.
    ///
    /// The granted epoch is `max(current_epoch, minimum_epoch) + 1` — passing
    /// `minimum_epoch: 0` reproduces the unconditional `current_epoch + 1`
    /// behavior exactly.
    fn acquire_inner(
        &self,
        holder_id: &str,
        minimum_epoch: u64,
    ) -> Result<LeaderRecord, LeaseError> {
        // 1. Read current leader record (epoch 0 if absent)
        let current_epoch = match self.read_current()? {
            Some(rec) => rec.epoch,
            None => 0,
        };

        let new_epoch = current_epoch
            .max(minimum_epoch)
            .checked_add(1)
            .ok_or(LeaseError::EpochExhausted)?;
        let now = self.clock.now().to_rfc3339();

        let new_record = LeaderRecord {
            epoch: new_epoch,
            holder_id: holder_id.to_string(),
            acquired_at: now,
        };

        let content = format_leader_record(&new_record);
        let temp = Self::temp_path();
        let target = FsPath::new(LEADER_RECORD_FILE);

        staging::stage_bytes(
            &self.fs,
            &temp,
            &target,
            content.as_bytes(),
            LeaseError::IoError,
        )?;

        // Fsync parent directory
        let _ = self.fs.sync_dir(&FsPath::new("."), Durability::Durable);

        // Re-read to confirm we won
        match self.read_current()? {
            Some(rec) if rec.holder_id == holder_id && rec.epoch == new_epoch => {
                tracing::info!(
                    epoch = new_epoch,
                    holder_id = holder_id,
                    "leadership acquired via CAS-rename"
                );
                Ok(new_record)
            }
            Some(rec) => Err(LeaseError::AcquisitionFailed(format!(
                "lost CAS race: expected holder={} epoch={}, got holder={} epoch={}",
                holder_id, new_epoch, rec.holder_id, rec.epoch
            ))),
            None => Err(LeaseError::IoError(
                "leader record missing after rename".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{MockFs, ReadFs};
    use std::sync::Barrier;
    use std::thread;

    fn make_store() -> FsLeaderStore {
        FsLeaderStore::new(
            Arc::new(MockFs::new()),
            crate::lease::clock::LeaseClock::default(),
        )
    }

    fn metadata_generation(horizon: u64) -> crate::lease::CloudMetadataGeneration {
        crate::lease::CloudMetadataGeneration {
            id: crate::lease::SharedGenerationId::FIRST,
            manifest_horizon: horizon,
            objects: [
                crate::metadata::files::FORMAT,
                crate::metadata::files::MANIFEST_SNAPSHOT,
            ]
            .into_iter()
            .map(|file_name| crate::lease::CloudMetadataObject {
                file_name: file_name.into(),
                object_key: format!("metadata/generations/{}/{file_name}", uuid::Uuid::new_v4()),
                len: 1,
                crc32c: 1,
            })
            .collect(),
        }
    }

    #[test]
    fn should_reject_stale_metadata_predecessor_without_changing_the_pointer() {
        // Arrange
        let store = make_store();
        let record = store.acquire_leadership("writer").unwrap();
        let first = metadata_generation(10);
        let timeout = std::time::Duration::from_secs(1);
        store
            .publish_committed_metadata("writer", record.epoch, None, first.clone(), timeout)
            .unwrap();
        let mut second = metadata_generation(11);
        second.id = first.id.checked_next().unwrap();
        store
            .publish_committed_metadata(
                "writer",
                record.epoch,
                Some(&first),
                second.clone(),
                timeout,
            )
            .unwrap();
        let mut competing = metadata_generation(12);
        competing.id = second.id.checked_next().unwrap();

        // Act
        let result = store.publish_committed_metadata(
            "writer",
            record.epoch,
            Some(&first),
            competing,
            timeout,
        );

        // Assert
        assert!(matches!(result, Err(LeaseError::RenewalFailed(_))));
        assert_eq!(
            store.read_committed_metadata(timeout).unwrap(),
            crate::lease::CloudMetadataHead::Committed(second)
        );
    }

    #[test]
    fn should_refuse_a_new_filesystem_epoch_when_committed_metadata_loses_its_fencing_record() {
        // Arrange
        let store = make_store();
        let record = store.acquire_leadership("writer").unwrap();
        let first = metadata_generation(10);
        let timeout = std::time::Duration::from_secs(1);
        store
            .publish_committed_metadata("writer", record.epoch, None, first, timeout)
            .unwrap();
        store
            .fs
            .remove_file(&FsPath::new(LEADER_RECORD_FILE))
            .unwrap();

        // Act
        let result = store.acquire_leadership("successor");

        // Assert
        assert!(matches!(result, Err(LeaseError::Indeterminate(_))));
        assert!(matches!(
            store.read_committed_metadata(timeout),
            Err(LeaseError::Indeterminate(_))
        ));
    }

    #[test]
    fn should_preserve_committed_metadata_when_a_successor_acquires_the_filesystem_lease() {
        // Arrange
        let store = make_store();
        let first = store.acquire_leadership("first").unwrap();
        let generation = super::super::traits::CloudMetadataGeneration {
            id: crate::lease::SharedGenerationId::FIRST,
            manifest_horizon: 1,
            objects: [
                crate::metadata::files::FORMAT,
                crate::metadata::files::MANIFEST_SNAPSHOT,
            ]
            .into_iter()
            .map(|file_name| super::super::traits::CloudMetadataObject {
                file_name: file_name.into(),
                object_key: format!("metadata/generations/{}/{file_name}", uuid::Uuid::new_v4()),
                len: 1,
                crc32c: 1,
            })
            .collect(),
        };

        // Act
        store
            .publish_committed_metadata(
                "first",
                first.epoch,
                None,
                generation.clone(),
                std::time::Duration::from_secs(1),
            )
            .unwrap();
        store.acquire_leadership("second").unwrap();

        // Assert
        assert_eq!(
            store
                .read_committed_metadata(std::time::Duration::from_secs(1))
                .unwrap(),
            super::super::traits::CloudMetadataHead::Committed(generation)
        );
    }

    #[test]
    fn should_acquire_leadership_when_no_existing_record() {
        // Arrange
        let store = make_store();

        // Act
        let record = store.acquire_leadership("test@host").unwrap();

        // Assert
        assert_eq!(record.epoch, 1);
        assert_eq!(record.holder_id, "test@host");
    }

    #[test]
    fn should_increment_epoch_when_reacquiring() {
        // Arrange
        let store = make_store();
        let first = store.acquire_leadership("node-a").unwrap();

        // Act
        let second = store.acquire_leadership("node-b").unwrap();

        // Assert
        assert_eq!(first.epoch, 1);
        assert_eq!(second.epoch, 2);
        assert_eq!(second.holder_id, "node-b");
    }

    #[test]
    fn should_read_current_when_record_exists() {
        // Arrange
        let store = make_store();
        store.acquire_leadership("holder-1").unwrap();

        // Act
        let current = store.read_current().unwrap();

        // Assert
        assert!(current.is_some());
        let rec = current.unwrap();
        assert_eq!(rec.epoch, 1);
        assert_eq!(rec.holder_id, "holder-1");
    }

    #[test]
    fn should_report_epoch_exhausted_when_fencing_epoch_cannot_advance() {
        // Arrange
        let store = make_store();
        let maxed_record = LeaderRecord {
            epoch: u64::MAX,
            holder_id: "prior-holder".to_string(),
            acquired_at: chrono::Utc::now().to_rfc3339(),
        };
        store
            .fs
            .open(
                &FsPath::new(LEADER_RECORD_FILE),
                OpenOptions {
                    mode: OpenMode::ReadWrite,
                    create: true,
                    create_new: true,
                    truncate: false,
                },
            )
            .expect("create leader record")
            .write_at(0, bytes::Bytes::from(format_leader_record(&maxed_record)))
            .expect("write maxed leader record");

        // Act
        let result = store.acquire_leadership("new-holder");

        // Assert
        assert!(
            matches!(result, Err(LeaseError::EpochExhausted)),
            "expected EpochExhausted, got: {result:?}"
        );
    }

    fn write_raw_leader_record(store: &FsLeaderStore, bytes: &[u8]) {
        store
            .fs
            .open(
                &FsPath::new(LEADER_RECORD_FILE),
                OpenOptions {
                    mode: OpenMode::ReadWrite,
                    create: true,
                    create_new: true,
                    truncate: false,
                },
            )
            .expect("create leader record")
            .write_at(0, bytes::Bytes::copy_from_slice(bytes))
            .expect("write raw leader record");
    }

    #[test]
    fn should_return_indeterminate_when_leader_record_is_empty() {
        // Arrange: an existing-but-empty file is not the same as a genuinely
        // absent one — something was there and can't be trusted.
        let store = make_store();
        write_raw_leader_record(&store, b"");

        // Act
        let result = store.read_current();

        // Assert
        assert!(
            matches!(result, Err(LeaseError::Indeterminate(_))),
            "{result:?}"
        );
    }

    #[test]
    fn should_return_indeterminate_when_leader_record_is_missing_a_field() {
        // Arrange
        let store = make_store();
        write_raw_leader_record(&store, b"epoch: 1\nholder_id: node-a\n"); // no acquired_at

        // Act
        let result = store.read_current();

        // Assert
        assert!(
            matches!(result, Err(LeaseError::Indeterminate(_))),
            "{result:?}"
        );
    }

    #[test]
    fn should_return_indeterminate_when_leader_record_is_not_utf8() {
        // Arrange
        let store = make_store();
        write_raw_leader_record(&store, &[0xFF, 0xFE, 0xFD]);

        // Act
        let result = store.read_current();

        // Assert
        assert!(
            matches!(result, Err(LeaseError::Indeterminate(_))),
            "{result:?}"
        );
    }

    #[test]
    fn should_return_none_when_leader_record_file_is_genuinely_absent() {
        // Arrange
        let store = make_store();

        // Act
        let result = store.read_current();

        // Assert
        assert!(matches!(result, Ok(None)), "{result:?}");
    }

    #[test]
    fn should_fail_closed_acquisition_when_existing_leader_record_is_corrupt() {
        // Arrange: acquisition must not treat a corrupted record as a clean
        // slate and hand out epoch 1 as if unclaimed.
        let store = make_store();
        write_raw_leader_record(&store, b"");

        // Act
        let result = store.acquire_leadership("new-holder");

        // Assert
        assert!(
            matches!(result, Err(LeaseError::Indeterminate(_))),
            "{result:?}"
        );
    }

    #[test]
    fn should_validate_epoch_when_matching() {
        // Arrange
        let store = make_store();
        store.acquire_leadership("node-1").unwrap();

        // Act
        let result = store.validate_epoch("node-1", 1);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_fail_validate_epoch_when_stale() {
        // Arrange
        let store = make_store();
        store.acquire_leadership("node-1").unwrap();
        store.acquire_leadership("node-2").unwrap();

        // Act
        let result = store.validate_epoch("node-2", 1);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_fail_validate_epoch_when_holder_id_mismatches_despite_matching_epoch() {
        // Arrange: a forged/tampered record can carry a matching epoch with
        // a different holder_id — the shared default validate_epoch must
        // reject that, not just compare epoch.
        let store = make_store();
        store.acquire_leadership("node-1").unwrap();

        // Act
        let result = store.validate_epoch("impostor", 1);

        // Assert
        assert!(result.is_err(), "{result:?}");
    }

    #[test]
    fn should_skip_writer_effect_when_successor_owns_the_epoch() {
        // Arrange
        let store = make_store();
        let predecessor = store.acquire_leadership("predecessor").unwrap();
        let successor = store.acquire_leadership("successor").unwrap();
        let mut effect_ran = false;

        // Act
        let result = store.with_writer_epoch(
            "predecessor",
            predecessor.epoch,
            std::time::Duration::from_secs(1),
            &mut || effect_ran = true,
        );

        // Assert
        assert!(matches!(result, Err(LeaseError::RenewalFailed(_))));
        assert!(!effect_ran);
        assert_eq!(
            store.read_current().unwrap().unwrap().epoch,
            successor.epoch
        );
    }

    #[test]
    fn should_finish_an_admitted_writer_effect_before_successor_acquisition() {
        // Arrange
        let fs: Arc<dyn Fs> = Arc::new(MockFs::new());
        let predecessor_store = Arc::new(FsLeaderStore::new(
            Arc::clone(&fs),
            crate::lease::clock::LeaseClock::default(),
        ));
        let successor_store = FsLeaderStore::new(fs, crate::lease::clock::LeaseClock::default());
        let predecessor = predecessor_store.acquire_leadership("predecessor").unwrap();
        let effect_entered = Arc::new(Barrier::new(2));
        let effect_continue = Arc::new(Barrier::new(2));
        let effect_store = Arc::clone(&predecessor_store);
        let successor_store = Arc::new(successor_store);
        let effect_entered_worker = Arc::clone(&effect_entered);
        let effect_continue_worker = Arc::clone(&effect_continue);

        // Act: hold the real writer-effect fence after validation. A successor
        // using another store instance must wait until that admitted effect ends.
        let writer = thread::spawn(move || {
            let mut operation = || {
                effect_entered_worker.wait();
                effect_continue_worker.wait();
            };
            effect_store.with_writer_epoch(
                "predecessor",
                predecessor.epoch,
                std::time::Duration::from_secs(1),
                &mut operation,
            )
        });
        effect_entered.wait();
        let successor_attempt = {
            let successor_store = Arc::clone(&successor_store);
            thread::spawn(move || successor_store.acquire_leadership("successor"))
        };
        effect_continue.wait();
        writer.join().expect("writer thread completed").unwrap();
        let successor = successor_attempt
            .join()
            .expect("successor attempt completed")
            .expect("successor acquires after writer effect completes");

        // Assert
        assert_eq!(successor.holder_id, "successor");
        assert!(successor.epoch > predecessor.epoch);
    }

    #[test]
    fn should_wait_for_an_admitted_writer_effect_before_release() {
        // Arrange
        let fs: Arc<dyn Fs> = Arc::new(MockFs::new());
        let store = Arc::new(FsLeaderStore::new(
            fs,
            crate::lease::clock::LeaseClock::default(),
        ));
        let owner = store.acquire_leadership("owner").unwrap();
        let effect_entered = Arc::new(Barrier::new(2));
        let effect_continue = Arc::new(Barrier::new(2));
        let effect_store = Arc::clone(&store);
        let effect_entered_worker = Arc::clone(&effect_entered);
        let effect_continue_worker = Arc::clone(&effect_continue);

        // Act: hold an admitted effect while clean release attempts to change
        // the epoch's authority record.
        let writer = thread::spawn(move || {
            let mut operation = || {
                effect_entered_worker.wait();
                effect_continue_worker.wait();
            };
            effect_store.with_writer_epoch(
                "owner",
                owner.epoch,
                Duration::from_secs(1),
                &mut operation,
            )
        });
        effect_entered.wait();

        let release_store = Arc::clone(&store);
        let (started, started_receiver) = std::sync::mpsc::sync_channel(1);
        let (released, released_receiver) = std::sync::mpsc::sync_channel(1);
        let release = thread::spawn(move || {
            started.send(()).expect("report release start");
            let result = release_store.release_if_owner("owner", owner.epoch);
            released.send(result).expect("report release result");
        });
        started_receiver.recv().expect("release attempt started");
        let result_during_effect = released_receiver.recv_timeout(Duration::from_millis(100));

        // Always unblock and join before asserting, including when release
        // returns too early.
        effect_continue.wait();
        writer.join().expect("writer effect completed").unwrap();
        release.join().expect("release thread completed");
        let release_result = released_receiver
            .recv()
            .expect("release result was reported");

        // Assert
        assert!(
            result_during_effect.is_err(),
            "lease release returned while a writer effect was still admitted"
        );
        assert!(release_result.expect("release succeeds after effect completes"));
        let successor = store.acquire_leadership("successor").unwrap();
        assert!(successor.epoch > owner.epoch);
    }

    #[test]
    fn should_strictly_increase_epoch_when_multiple_acquisitions() {
        // Arrange
        let store = make_store();

        // Act
        let e1 = store.acquire_leadership("a").unwrap().epoch;
        let e2 = store.acquire_leadership("b").unwrap().epoch;
        let e3 = store.acquire_leadership("c").unwrap().epoch;
        let e4 = store.acquire_leadership("a").unwrap().epoch;

        // Assert
        assert!(e1 < e2);
        assert!(e2 < e3);
        assert!(e3 < e4);
    }

    #[test]
    fn should_respect_minimum_epoch_floor_above_current_on_disk_epoch() {
        // Arrange
        let store = make_store();
        store.acquire_leadership("node-a").unwrap(); // epoch 1

        // Act
        let record = store
            .acquire_leadership_with_minimum_epoch("node-b", 100)
            .unwrap();

        // Assert
        assert_eq!(record.epoch, 101);
        assert_eq!(record.holder_id, "node-b");
    }

    #[test]
    fn should_ignore_minimum_epoch_floor_below_current_on_disk_epoch() {
        // Arrange
        let store = make_store();
        store.acquire_leadership("node-a").unwrap(); // epoch 1
        store.acquire_leadership("node-b").unwrap(); // epoch 2

        // Act
        let record = store
            .acquire_leadership_with_minimum_epoch("node-c", 1)
            .unwrap();

        // Assert: floor below the current epoch changes nothing — the
        // granted epoch is still current_epoch + 1.
        assert_eq!(record.epoch, 3);
    }

    #[test]
    fn should_default_minimum_epoch_floor_to_zero_reproducing_current_behavior() {
        // Arrange
        let store = make_store();

        // Act
        let via_default = store.acquire_leadership("node-a").unwrap();
        let via_explicit_floor =
            LeaderStore::acquire_leadership_with_minimum_epoch(&store, "node-b", 0).unwrap();

        // Assert
        assert_eq!(via_default.epoch, 1);
        assert_eq!(via_explicit_floor.epoch, 2);
    }

    #[test]
    fn should_fail_closed_at_epoch_exhaustion_even_with_minimum_epoch_floor() {
        // Arrange
        let store = make_store();

        // Act
        let result = store.acquire_leadership_with_minimum_epoch("node-a", u64::MAX);

        // Assert
        assert!(
            matches!(result, Err(LeaseError::EpochExhausted)),
            "{result:?}"
        );
    }

    #[test]
    fn should_reject_stale_release_after_new_holder_acquires() {
        // Arrange
        let store = make_store();
        let first = store.acquire_leadership("node-a").unwrap();
        let second = store.acquire_leadership("node-b").unwrap();

        // Act
        let released = store.release_if_owner("node-a", first.epoch).unwrap();

        // Assert
        assert!(!released);
        assert_eq!(store.read_current().unwrap().unwrap().holder_id, "node-b");
        assert!(second.epoch > first.epoch);
    }

    #[test]
    fn should_reject_writer_effect_after_clean_release() {
        // Arrange
        let store = make_store();
        let released = store.acquire_leadership("node-a").unwrap();
        assert!(store.release_if_owner("node-a", released.epoch).unwrap());
        let mut effect_ran = false;

        // Act
        let result = store.with_writer_epoch(
            "node-a",
            released.epoch,
            std::time::Duration::from_secs(1),
            &mut || effect_ran = true,
        );

        // Assert
        assert!(matches!(result, Err(LeaseError::RenewalFailed(_))));
        assert!(!effect_ran);
    }

    #[test]
    fn should_allow_successor_after_power_loss_follows_completed_release() {
        // Arrange
        let device = crate::simulation::crash_fs::CrashDevice::new();
        let first_store = FsLeaderStore::new(
            Arc::new(device.fs()),
            crate::lease::clock::LeaseClock::default(),
        );
        let first = first_store.acquire_leadership("first").unwrap();

        // Act
        assert!(first_store.release_if_owner("first", first.epoch).unwrap());
        device.power_loss(0);
        let successor_store = FsLeaderStore::new(
            Arc::new(device.fs()),
            crate::lease::clock::LeaseClock::default(),
        );
        let successor = successor_store.acquire_leadership("successor").unwrap();

        // Assert
        assert!(successor.epoch > first.epoch);
        assert_eq!(successor.holder_id, "successor");
    }

    #[test]
    fn should_reject_stale_refresh_after_new_holder_acquires() {
        // Arrange
        let store = make_store();
        let first = store.acquire_leadership("node-a").unwrap();
        store.acquire_leadership("node-b").unwrap();

        // Act
        let result = store.refresh_timestamp("node-a", first.epoch);

        // Assert
        assert!(result.is_err());
        assert_eq!(store.read_current().unwrap().unwrap().holder_id, "node-b");
    }

    #[test]
    fn should_fail_closed_when_an_acquisition_lock_is_not_initialized_yet() {
        // Arrange
        let fs = Arc::new(MockFs::new());
        let store = FsLeaderStore::new(
            Arc::clone(&fs) as Arc<dyn Fs>,
            crate::lease::clock::LeaseClock::default(),
        );
        let lock_path = FsPath::new(LEADER_LOCK_FILE);
        fs.open(
            &lock_path,
            OpenOptions {
                mode: OpenMode::ReadWrite,
                create: true,
                create_new: true,
                truncate: false,
            },
        )
        .expect("create an intentionally empty acquisition lock");

        // Act
        let result = store.acquire_leadership("node-b");

        // Assert
        assert!(matches!(result, Err(LeaseError::AcquisitionFailed(_))));
        assert!(fs.exists(&lock_path).expect("lock presence check"));
    }

    #[test]
    fn should_fail_closed_when_power_loss_interrupts_acquisition_lock() {
        // Arrange: stop at the real acquisition mutation lock before the
        // leader record can be published.
        let device = crate::simulation::crash_fs::CrashDevice::new();
        let interrupted_store = FsLeaderStore::new(
            Arc::new(device.fs()),
            crate::lease::clock::LeaseClock::default(),
        );
        let lock = interrupted_store
            .acquire_effect_lock_once("interrupted", Durability::Durable)
            .expect("acquire writer effect lock");

        // Act
        device.power_loss(0);
        for seed in 0..64 {
            let fork = device.fork();
            fork.power_loss(seed);
            let successor_store = FsLeaderStore::new(
                Arc::new(fork.fs()),
                crate::lease::clock::LeaseClock::default(),
            );

            // Assert: the uncommitted acquisition either leaves its lock and
            // blocks a new owner, or loses the lock name and permits a fresh
            // first epoch. It never publishes a false predecessor owner.
            match successor_store.acquire_leadership("successor") {
                Ok(record) => {
                    assert_eq!(record.epoch, 1);
                    assert_eq!(record.holder_id, "successor");
                }
                Err(LeaseError::AcquisitionFailed(_)) => {
                    assert!(successor_store.read_current().unwrap().is_none());
                }
                Err(error) => panic!("unexpected interrupted acquisition result: {error:?}"),
            }
        }
        drop(lock);
    }

    #[test]
    fn should_reject_takeover_given_renewal_write_still_owns_mutation_lock() {
        // Arrange
        let fs = Arc::new(MockFs::new());
        let store = FsLeaderStore::new(
            Arc::clone(&fs) as Arc<dyn Fs>,
            crate::lease::clock::LeaseClock::default(),
        );
        let old_guard = store.acquire_lock("node-a").expect("acquire old lock");
        let old_owner_token = match &old_guard.kind {
            LockGuardKind::File { owner_token, .. } => owner_token,
            LockGuardKind::Filesystem { .. } => panic!("lease mutation uses a file lock"),
        };
        let stale_timestamp = (chrono::Utc::now() - chrono::Duration::days(1)).to_rfc3339();
        let lock_path = FsPath::new(LEADER_LOCK_FILE);
        let mut lock_file = fs
            .open(
                &lock_path,
                OpenOptions {
                    mode: OpenMode::ReadWrite,
                    create: false,
                    create_new: false,
                    truncate: true,
                },
            )
            .expect("open old lock for deterministic aging");
        lock_file
            .write_at(
                0,
                bytes::Bytes::from(FsLeaderStore::lock_contents(
                    "node-a",
                    old_owner_token,
                    &stale_timestamp,
                )),
            )
            .expect("age old lock");
        drop(lock_file);

        // Act
        let result = store.acquire_lock("node-b");

        // Assert
        assert!(matches!(result, Err(LeaseError::AcquisitionFailed(_))));
        let current = store
            .read_lock_contents(&lock_path)
            .expect("read owned lock")
            .expect("owned lock remains present");
        assert_eq!(
            FsLeaderStore::parse_lock_owner_token(&current),
            Some(old_owner_token.as_str())
        );
        drop(old_guard);
        assert!(!fs.exists(&lock_path).expect("lock presence check"));
    }

    #[test]
    fn should_allow_only_one_concurrent_acquisition_lock_owner() {
        // Arrange
        const CONTENDERS: usize = 8;
        let fs = Arc::new(MockFs::new());
        let store = Arc::new(FsLeaderStore::new(
            Arc::clone(&fs) as Arc<dyn Fs>,
            crate::lease::clock::LeaseClock::default(),
        ));
        let start = Arc::new(Barrier::new(CONTENDERS));
        let attempts_complete = Arc::new(Barrier::new(CONTENDERS));
        let mut handles = Vec::with_capacity(CONTENDERS);
        for contender in 0..CONTENDERS {
            let store = Arc::clone(&store);
            let start = Arc::clone(&start);
            let attempts_complete = Arc::clone(&attempts_complete);
            handles.push(thread::spawn(move || {
                start.wait();
                let guard = store.acquire_lock(&format!("node-{contender}"));
                attempts_complete.wait();
                guard.is_ok()
            }));
        }

        // Act
        let acquired = handles
            .into_iter()
            .map(|handle| handle.join().expect("acquisition thread panicked"))
            .filter(|acquired| *acquired)
            .count();

        // Assert
        assert_eq!(acquired, 1);
        assert!(
            !fs.exists(&FsPath::new(LEADER_LOCK_FILE))
                .expect("lock presence check"),
            "winning guard should remove its own acquisition lock"
        );
    }
}

#[cfg(test)]
mod lock_init_failure_tests {
    use super::*;
    use crate::io::traits::{DirEntry, Metadata};
    use crate::io::{File, FsResult, MockFs, ReadFs};
    use std::sync::atomic::{AtomicBool, Ordering};

    struct FailingLockWriteFile<'a> {
        inner: Box<dyn File + 'a>,
    }

    impl crate::io::ReadFile for FailingLockWriteFile<'_> {
        fn read_at(&self, offset: u64, len: u64) -> FsResult<bytes::Bytes> {
            self.inner.read_at(offset, len)
        }

        fn len(&self) -> FsResult<u64> {
            self.inner.len()
        }
    }

    impl File for FailingLockWriteFile<'_> {
        fn write_at(&mut self, _offset: u64, _data: bytes::Bytes) -> FsResult<()> {
            Err(crate::io::FsError::Io("No space left on device".into()))
        }
        fn append(&mut self, data: bytes::Bytes) -> FsResult<u64> {
            self.inner.append(data)
        }

        fn sync(&mut self, dur: Durability) -> FsResult<()> {
            self.inner.sync(dur)
        }
    }

    /// Fails writes to the leader lock file while `fail_lock_writes` is set.
    struct LockWriteFaultFs {
        inner: MockFs,
        fail_lock_writes: AtomicBool,
        unsupported_effect_locks: AtomicBool,
        /// Opening the leader record fails with an I/O error whose path
        /// happens to contain "not found".
        fail_record_open: AtomicBool,
    }

    impl ReadFs for LockWriteFaultFs {
        fn open_read(&self, path: &FsPath) -> FsResult<Box<dyn crate::io::ReadFile + '_>> {
            self.open(path, OpenOptions::READ_ONLY)
                .map(|file| file as Box<dyn crate::io::ReadFile>)
        }

        fn exists(&self, path: &FsPath) -> FsResult<bool> {
            self.inner.exists(path)
        }

        fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
            self.inner.metadata(path)
        }

        fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
            self.inner.list_dir(path)
        }
    }

    impl Fs for LockWriteFaultFs {
        fn try_lock_exclusive(&self, path: &FsPath) -> FsResult<Box<dyn FsLock>> {
            if path.0 == WRITER_EFFECT_LOCK_FILE
                && self.unsupported_effect_locks.load(Ordering::SeqCst)
            {
                return Err(crate::io::FsError::Unsupported(
                    "injected unsupported filesystem lock".into(),
                ));
            }
            self.inner.try_lock_exclusive(path)
        }

        fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File + '_>> {
            if path.0 == LEADER_RECORD_FILE && self.fail_record_open.load(Ordering::SeqCst) {
                return Err(crate::io::FsError::Io(
                    "open /srv/not found/db/.midge_leader: permission denied".into(),
                ));
            }
            let file = self.inner.open(path, opts)?;
            if path.0 == LEADER_LOCK_FILE && self.fail_lock_writes.load(Ordering::SeqCst) {
                return Ok(Box::new(FailingLockWriteFile { inner: file }));
            }
            Ok(file)
        }
        fn remove_file(&self, path: &FsPath) -> FsResult<()> {
            self.inner.remove_file(path)
        }
        fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
            self.inner.create_dir_all(path)
        }
        fn remove_dir_all(&self, path: &FsPath) -> FsResult<()> {
            self.inner.remove_dir_all(path)
        }
        fn sync_dir(&self, path: &FsPath, dur: Durability) -> FsResult<()> {
            self.inner.sync_dir(path, dur)
        }
        fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
            self.inner.rename_atomic(from, to)
        }
    }

    #[test]
    fn should_not_leave_leader_lock_behind_when_lock_payload_write_fails() {
        // Arrange: this process created the lock file, so it is provably ours
        // to remove. Leaving it would block every later lease mutation.
        let fs = Arc::new(LockWriteFaultFs {
            inner: MockFs::new(),
            fail_lock_writes: AtomicBool::new(true),
            unsupported_effect_locks: AtomicBool::new(false),
            fail_record_open: AtomicBool::new(false),
        });
        let store = FsLeaderStore::new(
            fs.clone() as Arc<dyn Fs>,
            crate::lease::clock::LeaseClock::default(),
        );

        // Act
        let failed = store.acquire_leadership("writer-a");
        fs.fail_lock_writes.store(false, Ordering::SeqCst);
        let recovered = store.acquire_leadership("writer-a");

        // Assert
        assert!(
            failed.is_err(),
            "the injected lock write failure must surface"
        );
        recovered.expect("a failed lock initialization must not wedge later acquisitions");
        assert!(!fs.exists(&FsPath::new(LEADER_LOCK_FILE)).unwrap());
    }

    #[test]
    fn should_report_leader_record_error_when_open_fails_with_io_error_on_path_containing_not_found(
    ) {
        // Arrange
        let fs = Arc::new(LockWriteFaultFs {
            inner: MockFs::new(),
            fail_lock_writes: AtomicBool::new(false),
            unsupported_effect_locks: AtomicBool::new(false),
            fail_record_open: AtomicBool::new(false),
        });
        let store = FsLeaderStore::new(
            fs.clone() as Arc<dyn Fs>,
            crate::lease::clock::LeaseClock::default(),
        );
        store.acquire_leadership("writer-a").expect("acquire");
        fs.fail_record_open.store(true, Ordering::SeqCst);

        // Act
        let result = store.read_current();

        // Assert: a permission error is not an absent record.
        assert!(matches!(result, Err(LeaseError::IoError(_))), "{result:?}");
    }

    #[test]
    fn should_use_portable_effect_lock_when_filesystem_locks_are_unsupported() {
        // Arrange: the filesystem cannot provide OS locks, so writer effects
        // must serialize through the portable create-new lock file.
        let fs = Arc::new(LockWriteFaultFs {
            inner: MockFs::new(),
            fail_lock_writes: AtomicBool::new(false),
            unsupported_effect_locks: AtomicBool::new(true),
            fail_record_open: AtomicBool::new(false),
        });
        let store = FsLeaderStore::new(
            fs.clone() as Arc<dyn Fs>,
            crate::lease::LeaseClock::default(),
        );
        let leader = store.acquire_leadership("writer").expect("acquire lease");
        let mut effect_ran = false;

        // Act
        store
            .with_writer_epoch("writer", leader.epoch, Duration::from_secs(1), &mut || {
                effect_ran = true;
                assert!(fs
                    .exists(&FsPath::new(WRITER_EFFECT_FALLBACK_LOCK_FILE))
                    .expect("fallback lock check"));
            })
            .expect("portable effect lock should admit a valid lease");

        // Assert
        assert!(effect_ran);
        assert!(!fs
            .exists(&FsPath::new(WRITER_EFFECT_FALLBACK_LOCK_FILE))
            .expect("fallback lock cleanup check"));
    }
}
