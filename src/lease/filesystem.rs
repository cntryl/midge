//! Filesystem-based primary lease using CAS-via-rename leader records.
//!
//! Previous implementation used OS file locks (flock / `LockFileEx`).
//! This version uses a persistent leader record (`.midge_leader`) that
//! contains a monotonically increasing epoch, holder identity, and a
//! timestamp.  Leadership is acquired via atomic rename (CAS-via-rename)
//! which works correctly on local filesystems, NFS, and shared mounts.
//!
//! ## Safety guarantees
//!
//! - **Fencing token**: Every leader gets a strictly increasing epoch
//! - **CAS semantics**: Two simultaneous acquirers — exactly one wins
//! - **Works on NFS/EFS**: rename is atomic on POSIX, including NFS v3+
//! - **No flock dependency**: avoids advisory-lock pitfalls
//!
//! ## Limitations
//!
//! - Requires a writable filesystem (not suitable for read-only media)
//! - Leader record is a small file; no built-in expiry without heartbeat

use super::fs_leader_store::FsLeaderStore;
use super::traits::{LeaderStore, LeaseError, LeaseGuard, PrimaryLease};
use crate::io::{Fs, MockFs, RealFs};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

const DEFAULT_TTL_SECS: u64 = 30;

/// Per-process counter to ensure each `FileSystemLease` instance gets a unique
/// `holder_id`, even when multiple Engine instances are opened in the same process.
static LEASE_INSTANCE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Filesystem-based lease backed by a CAS-via-rename leader store.
pub struct FileSystemLease {
    leader_store: Arc<FsLeaderStore>,
    holder_id: String,
    acquired: AtomicBool,
    /// Epoch obtained during the most recent successful acquisition.
    acquired_epoch: AtomicU64,
}

impl FileSystemLease {
    /// Create a new filesystem lease for the given database path.
    ///
    /// # Arguments
    /// * `db_path` - Path to the database directory
    /// * `use_mock_fs` - If true, uses `MockFs` (for in-memory mode); otherwise uses `RealFs`
    pub fn new(db_path: &Path, use_mock_fs: bool) -> Result<Self, LeaseError> {
        let fs: Arc<dyn Fs> = if use_mock_fs {
            Arc::new(MockFs::new())
        } else {
            let realfs = RealFs::new(db_path).map_err(|e| LeaseError::IoError(e.to_string()))?;
            Arc::new(realfs)
        };

        let instance = LEASE_INSTANCE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let holder_id = format!(
            "{}.{}@{}",
            std::process::id(),
            instance,
            hostname::get()
                .unwrap_or_else(|_| std::ffi::OsString::from("unknown"))
                .to_string_lossy()
        );

        let leader_store = Arc::new(FsLeaderStore::new(fs));

        Ok(Self {
            leader_store,
            holder_id,
            acquired: AtomicBool::new(false),
            acquired_epoch: AtomicU64::new(0),
        })
    }

    /// Return a reference to the underlying leader store (for injection into `WalActor`).
    pub fn leader_store(&self) -> &Arc<FsLeaderStore> {
        &self.leader_store
    }
}

/// Write a raw leader-record payload through the `FsLeaderStore`'s underlying Fs.
/// Used during release to stamp a stale timestamp without going through the
/// ownership check in `refresh_timestamp`.
fn write_leader_record_raw(store: &FsLeaderStore, content: &str) -> Result<(), LeaseError> {
    store.write_raw(content)
}

impl PrimaryLease for FileSystemLease {
    fn try_acquire(self: Arc<Self>) -> Result<LeaseGuard, LeaseError> {
        if self.acquired.load(Ordering::Acquire) {
            return Err(LeaseError::AcquisitionFailed(
                "lease already acquired by this instance".to_string(),
            ));
        }

        // Check if another holder is currently active (within TTL window).
        // This prevents a second process from silently superseding a live writer.
        // If the existing holder crashed (record older than TTL), we allow take-over.
        if let Ok(Some(existing)) = self.leader_store.read_current() {
            if existing.holder_id != self.holder_id {
                // Parse acquired_at to check freshness
                if let Ok(acquired_at) = chrono::DateTime::parse_from_rfc3339(&existing.acquired_at)
                {
                    let age = chrono::Utc::now()
                        .signed_duration_since(acquired_at)
                        .to_std()
                        .unwrap_or(Duration::from_secs(u64::MAX));
                    if age < Duration::from_secs(DEFAULT_TTL_SECS * 2) {
                        return Err(LeaseError::AcquisitionFailed(format!(
                            "another Midge instance is already running against this storage \
                             (holder: {}, epoch: {}, acquired {}s ago)",
                            existing.holder_id,
                            existing.epoch,
                            age.as_secs()
                        )));
                    }
                    tracing::warn!(
                        old_holder = %existing.holder_id,
                        old_epoch = existing.epoch,
                        age_secs = age.as_secs(),
                        "taking over stale leader record (previous holder likely crashed)"
                    );
                }
            }
        }

        // Acquire leadership through the CAS-via-rename leader store
        let record = self
            .leader_store
            .acquire_leadership(&self.holder_id)
            .map_err(|e| LeaseError::AcquisitionFailed(e.to_string()))?;

        let epoch = record.epoch;
        self.acquired_epoch.store(epoch, Ordering::Release);
        self.acquired.store(true, Ordering::Release);

        tracing::info!(
            holder_id = %self.holder_id,
            epoch = epoch,
            "primary lease acquired via leader store"
        );

        Ok(LeaseGuard::for_lease(self))
    }

    fn renew(&self) -> Result<(), LeaseError> {
        if !self.acquired.load(Ordering::Acquire) {
            return Err(LeaseError::RenewalFailed("lease not acquired".to_string()));
        }

        // Validate that our epoch is still current AND refresh the timestamp
        // so other processes can see we're still alive.
        let our_epoch = self.acquired_epoch.load(Ordering::Acquire);
        self.leader_store
            .refresh_timestamp(&self.holder_id, our_epoch)
            .map_err(|e| LeaseError::RenewalFailed(e.to_string()))
    }

    fn release(&self) -> Result<(), LeaseError> {
        if !self.acquired.load(Ordering::Acquire) {
            return Ok(()); // Idempotent
        }

        // Rewrite the leader record with an ancient timestamp so that:
        // 1. The epoch value is preserved for the next acquirer to increment
        // 2. The staleness check sees it as expired, allowing re-acquisition
        let our_epoch = self.acquired_epoch.load(Ordering::Acquire);
        if our_epoch > 0 {
            let released_record = crate::lease::traits::LeaderRecord {
                epoch: our_epoch,
                holder_id: self.holder_id.clone(),
                acquired_at: "1970-01-01T00:00:00Z".to_string(),
            };
            let content = crate::lease::traits::format_leader_record(&released_record);
            // Best-effort write — if it fails the heartbeat TTL will expire naturally
            let _ = write_leader_record_raw(&self.leader_store, &content);
        }

        self.acquired.store(false, Ordering::Release);
        self.acquired_epoch.store(0, Ordering::Release);

        tracing::info!(
            holder_id = %self.holder_id,
            "primary lease released"
        );

        Ok(())
    }

    fn ttl(&self) -> Duration {
        Duration::from_secs(DEFAULT_TTL_SECS)
    }

    fn holder_id(&self) -> String {
        self.holder_id.clone()
    }

    fn epoch(&self) -> u64 {
        self.acquired_epoch.load(Ordering::Acquire)
    }

    fn get_leader_store(&self) -> Option<Arc<dyn LeaderStore>> {
        Some(Arc::clone(&self.leader_store) as Arc<dyn LeaderStore>)
    }
}

// SAFETY: FileSystemLease is Send + Sync because:
// - `leader_store` (FsLeaderStore) contains Arc<dyn Fs> which is Send + Sync.
// - `holder_id` is immutable after construction.
// - `acquired` uses AtomicBool for lock-free thread-safe access.
// - `acquired_epoch` uses AtomicU64 for lock-free thread-safe access.
unsafe impl Send for FileSystemLease {}
unsafe impl Sync for FileSystemLease {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_acquire_release_lease_when_no_contention() {
        // Arrange
        let temp_dir = tempfile::tempdir().unwrap();
        let lease = Arc::new(FileSystemLease::new(temp_dir.path(), false).unwrap());

        // Act
        let _guard = Arc::clone(&lease).try_acquire().unwrap();
        let acquired_before_release = lease.acquired.load(Ordering::Acquire);
        let epoch = lease.epoch();
        lease.release().unwrap();
        let acquired_after_release = lease.acquired.load(Ordering::Acquire);

        // Assert
        assert!(acquired_before_release);
        assert!(!acquired_after_release);
        assert!(epoch > 0, "epoch should be positive after acquisition");
    }

    #[test]
    fn should_increment_epoch_on_reacquisition() {
        // Arrange
        let temp_dir = tempfile::tempdir().unwrap();
        let lease1 = Arc::new(FileSystemLease::new(temp_dir.path(), false).unwrap());

        // Act - acquire, release, acquire again
        let _guard1 = Arc::clone(&lease1).try_acquire().unwrap();
        let epoch1 = lease1.epoch();
        lease1.release().unwrap();

        let lease2 = Arc::new(FileSystemLease::new(temp_dir.path(), false).unwrap());
        let _guard2 = Arc::clone(&lease2).try_acquire().unwrap();
        let epoch2 = lease2.epoch();

        // Assert
        assert!(
            epoch2 > epoch1,
            "epoch should increase: {epoch2} > {epoch1}"
        );
    }

    #[test]
    fn should_fail_double_acquire() {
        // Arrange
        let temp_dir = tempfile::tempdir().unwrap();
        let lease = Arc::new(FileSystemLease::new(temp_dir.path(), false).unwrap());
        let _guard = Arc::clone(&lease).try_acquire().unwrap();

        // Act
        let result = Arc::clone(&lease).try_acquire();

        // Assert
        assert!(matches!(result, Err(LeaseError::AcquisitionFailed(_))));
    }

    #[test]
    fn should_renew_when_epoch_current() {
        // Arrange
        let temp_dir = tempfile::tempdir().unwrap();
        let lease = Arc::new(FileSystemLease::new(temp_dir.path(), false).unwrap());
        let _guard = Arc::clone(&lease).try_acquire().unwrap();

        // Act
        let result = lease.renew();

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_fail_renew_when_not_acquired() {
        // Arrange
        let temp_dir = tempfile::tempdir().unwrap();
        let lease = Arc::new(FileSystemLease::new(temp_dir.path(), false).unwrap());

        // Act
        let result = lease.renew();

        // Assert
        assert!(matches!(result, Err(LeaseError::RenewalFailed(_))));
    }
}
