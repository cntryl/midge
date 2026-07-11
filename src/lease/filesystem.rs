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
}

impl PrimaryLease for FileSystemLease {
    fn try_acquire(self: Arc<Self>) -> Result<LeaseGuard, LeaseError> {
        if self.acquired.load(Ordering::Acquire) {
            return Err(LeaseError::AcquisitionFailed(
                "lease already acquired by this instance".to_string(),
            ));
        }

        // Check freshness while holding the acquisition lock so a concurrent
        // winner cannot publish a live record between validation and CAS.
        let record = self
            .leader_store
            .acquire_leadership_after_validation(&self.holder_id, |existing| {
                let Some(existing) = existing else {
                    return Ok(());
                };
                if existing.holder_id == self.holder_id {
                    return Ok(());
                }

                let acquired_at = chrono::DateTime::parse_from_rfc3339(&existing.acquired_at)
                    .map_err(|_| {
                        LeaseError::AcquisitionFailed(format!(
                            "another Midge instance may still own this storage; leader timestamp \
                             is invalid (holder: {}, epoch: {})",
                            existing.holder_id, existing.epoch
                        ))
                    })?;
                let signed_age = chrono::Utc::now().signed_duration_since(acquired_at);
                if signed_age < chrono::Duration::zero() {
                    return Err(LeaseError::AcquisitionFailed(format!(
                        "another Midge instance may still own this storage; leader timestamp is \
                         in the future (holder: {}, epoch: {})",
                        existing.holder_id, existing.epoch
                    )));
                }
                let age = signed_age.to_std().map_err(|_| {
                    LeaseError::AcquisitionFailed(format!(
                        "another Midge instance may still own this storage; leader age is \
                         ambiguous (holder: {}, epoch: {})",
                        existing.holder_id, existing.epoch
                    ))
                })?;
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
                Ok(())
            })
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

        let our_epoch = self.acquired_epoch.load(Ordering::Acquire);
        if our_epoch > 0 {
            // Conditional release preserves a newer holder's record if this
            // lease was fenced between shutdown admission and release.
            let _ = self
                .leader_store
                .release_if_owner(&self.holder_id, our_epoch);
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
    use super::super::traits::{format_leader_record, LeaderRecord};
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

    #[test]
    fn should_reject_takeover_when_leader_timestamp_is_in_the_future() {
        // Arrange
        let temp_dir = tempfile::tempdir().unwrap();
        let future_record = LeaderRecord {
            epoch: 41,
            holder_id: "future-node".to_string(),
            acquired_at: (chrono::Utc::now() + chrono::Duration::minutes(5)).to_rfc3339(),
        };
        std::fs::write(
            temp_dir.path().join(".midge_leader"),
            format_leader_record(&future_record),
        )
        .expect("write future leader record");
        let lease = Arc::new(FileSystemLease::new(temp_dir.path(), false).unwrap());

        // Act
        let result = Arc::clone(&lease).try_acquire();

        // Assert
        assert!(matches!(result, Err(LeaseError::AcquisitionFailed(_))));
        assert_eq!(
            lease
                .leader_store
                .read_current()
                .expect("read current leader"),
            Some(future_record),
            "an ambiguous future timestamp must retain the existing leader indefinitely"
        );
    }
}
