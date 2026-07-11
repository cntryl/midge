//! Filesystem-based leader store using CAS-via-rename.
//!
//! Provides distributed-safe leader election for local disk, NFS, SMB, and
//! any backend that supports atomic rename.  Does NOT use file locks.
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
use crate::io::{staging, Durability, Fs, FsPath, OpenMode, OpenOptions};
use std::sync::Arc;

/// Well-known filename for the leader record.
const LEADER_RECORD_FILE: &str = ".midge_leader";
const LEADER_LOCK_FILE: &str = ".midge_leader.lock";
const STALE_LOCK_TIMEOUT_SECS: i64 = 30;

/// Filesystem-based `LeaderStore` using temp-write + fsync + atomic rename.
pub struct FsLeaderStore {
    fs: Arc<dyn Fs>,
}

impl FsLeaderStore {
    /// Create a new filesystem leader store backed by the given `Fs`.
    pub fn new(fs: Arc<dyn Fs>) -> Self {
        Self { fs }
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

    fn read_lock_contents(&self) -> Result<Option<String>, LeaseError> {
        let lock_path = FsPath::new(LEADER_LOCK_FILE);
        match self.fs.exists(&lock_path) {
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
                &lock_path,
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

    fn parse_lock_created_at(content: &str) -> Option<chrono::DateTime<chrono::Utc>> {
        content
            .lines()
            .find_map(|line| line.strip_prefix("created_at="))
            .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
            .map(|value| value.with_timezone(&chrono::Utc))
    }

    fn parse_lock_owner_token(content: &str) -> Option<&str> {
        content
            .lines()
            .find_map(|line| line.strip_prefix("owner_token="))
            .filter(|value| !value.is_empty())
    }

    fn lock_is_stale(content: &str, now: chrono::DateTime<chrono::Utc>) -> bool {
        // An empty or malformed lock can be observed in the small interval
        // after `create_new` succeeds and before its owner writes the
        // timestamp. Treating it as stale lets another acquirer unlink a live
        // lock and creates a split-brain window. Fail closed unless a valid,
        // demonstrably old timestamp proves the lock is abandoned.
        Self::parse_lock_owner_token(content).is_some()
            && Self::parse_lock_created_at(content).is_some_and(|created_at| {
                now.signed_duration_since(created_at).num_seconds() >= STALE_LOCK_TIMEOUT_SECS
            })
    }

    /// Remove the acquisition lock only while it still carries the ownership
    /// token observed by the caller. A stale guard or cleanup attempt must not
    /// unlink a lock created by a newer acquirer.
    fn remove_lock_if_owner(&self, expected_owner_token: &str) -> Result<bool, LeaseError> {
        let Some(content) = self.read_lock_contents()? else {
            return Ok(false);
        };
        if Self::parse_lock_owner_token(&content) != Some(expected_owner_token) {
            return Ok(false);
        }

        self.fs
            .remove_file(&FsPath::new(LEADER_LOCK_FILE))
            .map_err(|error| {
                LeaseError::IoError(format!("failed to remove leader lock: {error}"))
            })?;
        Ok(true)
    }

    fn acquire_lock(&self, holder_id: &str) -> Result<LockGuard<'_>, LeaseError> {
        let lock_path = FsPath::new(LEADER_LOCK_FILE);
        let now = chrono::Utc::now();
        let owner_token = uuid::Uuid::new_v4().to_string();
        let payload = Self::lock_contents(holder_id, &owner_token, &now.to_rfc3339());

        for _ in 0..2 {
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
                    lock_file
                        .write_at(0, bytes::Bytes::from(payload.clone()))
                        .map_err(|error| {
                            LeaseError::IoError(format!(
                                "failed to initialize leader lock: {error}"
                            ))
                        })?;
                    let _ = lock_file.sync(Durability::Durable);
                    return Ok(LockGuard {
                        store: self,
                        owner_token,
                    });
                }
                Err(error) => {
                    let content = self.read_lock_contents()?;
                    if let Some(existing) = content
                        .as_deref()
                        .filter(|existing| Self::lock_is_stale(existing, now))
                    {
                        let stale_owner_token = Self::parse_lock_owner_token(existing)
                            .expect("stale lock validation requires an ownership token");
                        let _removed = self.remove_lock_if_owner(stale_owner_token)?;
                        continue;
                    }
                    return Err(LeaseError::AcquisitionFailed(format!(
                        "another acquire is in progress: {error}"
                    )));
                }
            }
        }

        Err(LeaseError::AcquisitionFailed(
            "leader acquisition lock remained unavailable after stale-lock cleanup".to_string(),
        ))
    }
}

impl LeaderStore for FsLeaderStore {
    fn acquire_leadership(&self, holder_id: &str) -> Result<LeaderRecord, LeaseError> {
        self.acquire_leadership_after_validation(holder_id, |_| Ok(()))
    }

    fn read_current(&self) -> Result<Option<LeaderRecord>, LeaseError> {
        let path = FsPath::new(LEADER_RECORD_FILE);

        match self.fs.exists(&path) {
            Ok(true) => {}
            Ok(false) => return Ok(None),
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
            Err(e) => {
                // File may have been removed between exists() and open()
                let msg = e.to_string();
                if msg.contains("not found") || msg.contains("No such file") {
                    return Ok(None);
                }
                return Err(LeaseError::IoError(format!(
                    "failed to open leader record: {e}"
                )));
            }
        };

        let len = file.len().map_err(|e| {
            LeaseError::IoError(format!("failed to read leader record length: {e}"))
        })?;

        if len == 0 {
            return Ok(None);
        }

        let data = file
            .read_at(0, len)
            .map_err(|e| LeaseError::IoError(format!("failed to read leader record: {e}")))?;

        let content = String::from_utf8(data.to_vec())
            .map_err(|e| LeaseError::IoError(format!("leader record not UTF-8: {e}")))?;

        Ok(parse_leader_record(&content))
    }
}

struct LockGuard<'a> {
    store: &'a FsLeaderStore,
    owner_token: String,
}

impl Drop for LockGuard<'_> {
    fn drop(&mut self) {
        if let Err(error) = self.store.remove_lock_if_owner(&self.owner_token) {
            tracing::warn!(%error, "failed to release leader acquisition lock");
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
        let _lock = self.acquire_lock(holder_id)?;
        let current = self.read_current()?;
        validate_current(current.as_ref())?;
        self.acquire_inner(holder_id)
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
            Some(rec) if rec.holder_id == holder_id && rec.epoch == expected_epoch => {}
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
        let now = chrono::Utc::now().to_rfc3339();
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
        let _lock = self.acquire_lock(holder_id)?;
        let Some(current) = self.read_current()? else {
            return Ok(false);
        };
        if current.holder_id != holder_id || current.epoch != expected_epoch {
            return Ok(false);
        }

        let released = LeaderRecord {
            epoch: expected_epoch,
            holder_id: holder_id.to_string(),
            acquired_at: "1970-01-01T00:00:00Z".to_string(),
        };
        let temp = Self::temp_path();
        staging::stage_bytes(
            &self.fs,
            &temp,
            &FsPath::new(LEADER_RECORD_FILE),
            format_leader_record(&released).as_bytes(),
            LeaseError::IoError,
        )?;
        let _ = self.fs.sync_dir(&FsPath::new("."), Durability::Durable);
        Ok(true)
    }

    /// Core CAS logic, called under the lock file held by `acquire_leadership`.
    fn acquire_inner(&self, holder_id: &str) -> Result<LeaderRecord, LeaseError> {
        // 1. Read current leader record (epoch 0 if absent)
        let current_epoch = match self.read_current()? {
            Some(rec) => rec.epoch,
            None => 0,
        };

        let new_epoch = current_epoch
            .checked_add(1)
            .ok_or_else(|| LeaseError::AcquisitionFailed("leader epoch exhausted".to_string()))?;
        let now = chrono::Utc::now().to_rfc3339();

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
    use crate::io::MockFs;
    use std::sync::Barrier;
    use std::thread;

    fn make_store() -> FsLeaderStore {
        FsLeaderStore::new(Arc::new(MockFs::new()))
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
    fn should_return_none_when_no_record_exists() {
        // Arrange
        let store = make_store();

        // Act
        let current = store.read_current().unwrap();

        // Assert
        assert!(current.is_none());
    }

    #[test]
    fn should_validate_epoch_when_matching() {
        // Arrange
        let store = make_store();
        store.acquire_leadership("node-1").unwrap();

        // Act
        let result = store.validate_epoch(1);

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
        let result = store.validate_epoch(1);

        // Assert
        assert!(result.is_err());
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
        let store = FsLeaderStore::new(Arc::clone(&fs) as Arc<dyn Fs>);
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
    fn should_preserve_new_acquisition_lock_when_old_guard_is_dropped() {
        // Arrange
        let fs = Arc::new(MockFs::new());
        let store = FsLeaderStore::new(Arc::clone(&fs) as Arc<dyn Fs>);
        let old_guard = store.acquire_lock("node-a").expect("acquire old lock");
        let stale_timestamp = (chrono::Utc::now()
            - chrono::Duration::seconds(STALE_LOCK_TIMEOUT_SECS + 1))
        .to_rfc3339();
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
                    &old_guard.owner_token,
                    &stale_timestamp,
                )),
            )
            .expect("age old lock");
        drop(lock_file);

        let new_guard = store
            .acquire_lock("node-a")
            .expect("replace stale lock for same holder identity");
        let new_owner_token = new_guard.owner_token.clone();
        assert_ne!(old_guard.owner_token, new_owner_token);

        // Act
        drop(old_guard);

        // Assert
        let current = store
            .read_lock_contents()
            .expect("read replacement lock")
            .expect("replacement lock remains present");
        assert_eq!(
            FsLeaderStore::parse_lock_owner_token(&current),
            Some(new_owner_token.as_str())
        );
        drop(new_guard);
        assert!(!fs.exists(&lock_path).expect("lock presence check"));
    }

    #[test]
    fn should_allow_only_one_concurrent_acquisition_lock_owner() {
        // Arrange
        const CONTENDERS: usize = 8;
        let fs = Arc::new(MockFs::new());
        let store = Arc::new(FsLeaderStore::new(Arc::clone(&fs) as Arc<dyn Fs>));
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

    #[test]
    fn should_roundtrip_leader_record_format() {
        // Arrange
        let rec = LeaderRecord {
            epoch: 42,
            holder_id: "123@host".to_string(),
            acquired_at: "2026-02-16T12:00:00Z".to_string(),
        };

        // Act
        let serialized = format_leader_record(&rec);
        let parsed = parse_leader_record(&serialized);

        // Assert
        let parsed = parsed.unwrap();
        assert_eq!(parsed, rec);
    }
}
