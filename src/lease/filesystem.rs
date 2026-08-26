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
use super::traits::{LeaderStore, LeaseError, LeaseGuard, LeaseValidity, PrimaryLease};
use crate::io::{Fs, MockFs, RealFs};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[cfg(test)]
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
    validity: Arc<LeaseValidity>,
    ttl: Duration,
    clock_skew_tolerance: Duration,
}

impl FileSystemLease {
    /// Create a new filesystem lease for the given database path.
    ///
    /// # Arguments
    /// * `db_path` - Path to the database directory
    /// * `use_mock_fs` - If true, uses `MockFs` (for in-memory mode); otherwise uses `RealFs`
    #[cfg(test)]
    pub fn new(db_path: &Path, use_mock_fs: bool) -> Result<Self, LeaseError> {
        Self::new_with_ttl_and_clock_skew_tolerance(
            db_path,
            use_mock_fs,
            Duration::from_secs(DEFAULT_TTL_SECS),
            Duration::from_secs(DEFAULT_TTL_SECS / 2),
        )
    }

    pub(crate) fn new_with_ttl_and_clock_skew_tolerance(
        db_path: &Path,
        use_mock_fs: bool,
        ttl: Duration,
        clock_skew_tolerance: Duration,
    ) -> Result<Self, LeaseError> {
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
            validity: Arc::new(LeaseValidity::new()),
            ttl,
            clock_skew_tolerance,
        })
    }

    pub(crate) fn lease_validity(&self) -> Arc<LeaseValidity> {
        Arc::clone(&self.validity)
    }
}

impl PrimaryLease for FileSystemLease {
    fn try_acquire(self: Arc<Self>) -> Result<LeaseGuard, LeaseError> {
        if self.acquired.load(Ordering::Acquire) {
            return Err(LeaseError::AlreadyAcquired(
                "lease already acquired by this instance".to_string(),
            ));
        }

        // Check freshness while holding the acquisition lock so a concurrent
        // winner cannot publish a live record between validation and CAS.
        // The validation closure's own error variant (Indeterminate for an
        // unparseable/ambiguous record, AcquisitionFailed for confirmed
        // contention) propagates verbatim — it must not be collapsed into a
        // single blanket variant here, or every distinction made below would
        // be silently discarded.
        let record =
            self.leader_store
                .acquire_leadership_after_validation(&self.holder_id, |existing| {
                    let Some(existing) = existing else {
                        return Ok(());
                    };
                    if existing.holder_id == self.holder_id {
                        return Ok(());
                    }

                    let acquired_at = chrono::DateTime::parse_from_rfc3339(&existing.acquired_at)
                        .map_err(|_| {
                        LeaseError::Indeterminate(format!(
                            "leader timestamp is invalid; ownership is ambiguous (holder: {}, \
                             epoch: {})",
                            existing.holder_id, existing.epoch
                        ))
                    })?;
                    let signed_age = chrono::Utc::now().signed_duration_since(acquired_at);
                    if signed_age < chrono::Duration::zero() {
                        return Err(LeaseError::Indeterminate(format!(
                        "leader timestamp is in the future; ownership is ambiguous (holder: {}, \
                         epoch: {})",
                        existing.holder_id, existing.epoch
                    )));
                    }
                    let age = signed_age.to_std().map_err(|_| {
                        LeaseError::Indeterminate(format!(
                            "leader age is ambiguous (holder: {}, epoch: {})",
                            existing.holder_id, existing.epoch
                        ))
                    })?;
                    // A takeover is delayed by a bounded skew allowance. This
                    // trades failover latency for protection against a holder
                    // whose wall clock stepped backwards while its monotonic
                    // watchdog still considers the lease live.
                    let stale_after = self.ttl.saturating_add(self.clock_skew_tolerance);
                    if age < stale_after {
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
                })?;

        let epoch = record.epoch;
        self.validity
            .activate(epoch, std::time::Instant::now() + self.ttl())?;
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
        let our_epoch = self.acquired_epoch.load(Ordering::Acquire);
        let result = (|| {
            if !self.acquired.load(Ordering::Acquire) {
                return Err(LeaseError::RenewalFailed("lease not acquired".to_string()));
            }
            self.validity.remaining(our_epoch)?;
            self.leader_store
                .refresh_timestamp(&self.holder_id, our_epoch)
                .map_err(|error| LeaseError::RenewalFailed(error.to_string()))?;
            self.validity
                .advance(our_epoch, std::time::Instant::now() + self.ttl())
        })();
        if let Err(error) = result {
            self.validity.fence(our_epoch);
            self.acquired.store(false, Ordering::Release);
            self.acquired_epoch.store(0, Ordering::Release);
            let _ = self
                .leader_store
                .release_if_owner(&self.holder_id, our_epoch);
            return Err(error);
        }
        Ok(())
    }

    fn release(&self) -> Result<(), LeaseError> {
        if !self.acquired.load(Ordering::Acquire) {
            return Ok(()); // Idempotent
        }

        let our_epoch = self.acquired_epoch.load(Ordering::Acquire);
        if our_epoch > 0 {
            // Conditional release preserves a newer holder's record if this
            // lease was fenced between shutdown admission and release.
            self.leader_store
                .release_if_owner(&self.holder_id, our_epoch)?;
        }

        self.acquired.store(false, Ordering::Release);
        self.acquired_epoch.store(0, Ordering::Release);
        self.validity.deactivate(our_epoch);

        tracing::info!(
            holder_id = %self.holder_id,
            "primary lease released"
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
    fn should_allow_reacquisition_given_expired_filesystem_lease_when_new_holder_acquires() {
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
        assert!(matches!(result, Err(LeaseError::AlreadyAcquired(_))));
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
    fn should_reject_renewal_given_epoch_mismatch_when_renewing() {
        // Arrange
        let temp_dir = tempfile::tempdir().unwrap();
        let lease = Arc::new(FileSystemLease::new(temp_dir.path(), false).unwrap());
        let _guard = Arc::clone(&lease)
            .try_acquire()
            .expect("acquire original lease");
        let original_epoch = lease.epoch();
        let newer_record = LeaderRecord {
            epoch: original_epoch + 1,
            holder_id: "newer-holder".to_string(),
            acquired_at: chrono::Utc::now().to_rfc3339(),
        };
        std::fs::write(
            temp_dir.path().join(".midge_leader"),
            format_leader_record(&newer_record),
        )
        .expect("replace record with newer owner");

        // Act
        let result = lease.renew();

        // Assert
        assert!(matches!(
            result,
            Err(LeaseError::RenewalFailed(message))
                if message.contains("leader record changed")
                    && message.contains("newer-holder")
        ));
        assert_eq!(
            lease
                .leader_store
                .read_current()
                .expect("read newer leader record"),
            Some(newer_record),
            "stale renewal cleanup must retain the newer owner"
        );
        assert_eq!(lease.epoch(), 0);
        assert!(!lease.acquired.load(Ordering::Acquire));
    }

    #[test]
    fn should_reject_takeover_given_future_leader_timestamp_when_acquiring() {
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
        assert!(matches!(result, Err(LeaseError::Indeterminate(_))));
        assert_eq!(
            lease
                .leader_store
                .read_current()
                .expect("read current leader"),
            Some(future_record),
            "an ambiguous future timestamp must retain the existing leader indefinitely"
        );
    }

    #[test]
    fn should_take_over_only_after_configured_lease_boundary() {
        // Arrange
        let temp_dir = tempfile::tempdir().unwrap();
        let ttl = Duration::from_secs(2);
        let skew = Duration::from_secs(1);
        let live_record = LeaderRecord {
            epoch: 17,
            holder_id: "live-node".to_string(),
            acquired_at: (chrono::Utc::now() - chrono::Duration::seconds(2)).to_rfc3339(),
        };
        std::fs::write(
            temp_dir.path().join(".midge_leader"),
            format_leader_record(&live_record),
        )
        .expect("write live leader record");
        let lease = Arc::new(
            FileSystemLease::new_with_ttl_and_clock_skew_tolerance(
                temp_dir.path(),
                false,
                ttl,
                skew,
            )
            .unwrap(),
        );

        // Act
        let live_result = Arc::clone(&lease).try_acquire();
        let stale_record = LeaderRecord {
            acquired_at: (chrono::Utc::now() - chrono::Duration::seconds(4)).to_rfc3339(),
            ..live_record.clone()
        };
        std::fs::write(
            temp_dir.path().join(".midge_leader"),
            format_leader_record(&stale_record),
        )
        .expect("write stale leader record");
        let _guard = Arc::clone(&lease)
            .try_acquire()
            .expect("take over after configured boundary");

        // Assert
        assert!(matches!(live_result, Err(LeaseError::AcquisitionFailed(_))));
        assert!(lease.epoch() > stale_record.epoch);
    }

    #[test]
    fn should_reject_takeover_given_malformed_leader_timestamp_when_acquiring() {
        // Arrange
        let temp_dir = tempfile::tempdir().unwrap();
        let malformed_record = LeaderRecord {
            epoch: 23,
            holder_id: "ambiguous-node".to_string(),
            acquired_at: "not-a-timestamp".to_string(),
        };
        std::fs::write(
            temp_dir.path().join(".midge_leader"),
            format_leader_record(&malformed_record),
        )
        .expect("write malformed leader record");
        let lease = Arc::new(FileSystemLease::new(temp_dir.path(), false).unwrap());

        // Act
        let result = Arc::clone(&lease).try_acquire();

        // Assert
        assert!(matches!(result, Err(LeaseError::Indeterminate(_))));
        assert_eq!(
            lease
                .leader_store
                .read_current()
                .expect("read current leader"),
            Some(malformed_record)
        );
    }

    #[test]
    fn should_fence_stale_holder_after_higher_epoch_takes_over() {
        // Arrange
        let temp_dir = tempfile::tempdir().unwrap();
        let ttl = Duration::from_secs(2);
        let first = Arc::new(
            FileSystemLease::new_with_ttl_and_clock_skew_tolerance(
                temp_dir.path(),
                false,
                ttl,
                Duration::ZERO,
            )
            .unwrap(),
        );
        let _first_guard = Arc::clone(&first)
            .try_acquire()
            .expect("acquire first lease");
        let first_epoch = first.epoch();
        let stale_record = LeaderRecord {
            epoch: first_epoch,
            holder_id: first.holder_id(),
            acquired_at: (chrono::Utc::now() - chrono::Duration::seconds(3)).to_rfc3339(),
        };
        std::fs::write(
            temp_dir.path().join(".midge_leader"),
            format_leader_record(&stale_record),
        )
        .expect("age first leader record");
        let second = Arc::new(
            FileSystemLease::new_with_ttl_and_clock_skew_tolerance(
                temp_dir.path(),
                false,
                ttl,
                Duration::ZERO,
            )
            .unwrap(),
        );
        let _second_guard = Arc::clone(&second)
            .try_acquire()
            .expect("higher epoch should take over");
        let second_record = second
            .leader_store
            .read_current()
            .expect("read second leader")
            .expect("second leader exists");

        // Act
        let renew_result = first.renew();
        let release_result = first.release();
        let write_fence_result = first
            .leader_store
            .validate_epoch(&first.holder_id(), first_epoch);

        // Assert
        assert!(matches!(renew_result, Err(LeaseError::RenewalFailed(_))));
        assert!(release_result.is_ok());
        assert!(matches!(
            write_fence_result,
            Err(LeaseError::RenewalFailed(_))
        ));
        assert!(second_record.epoch > first_epoch);
        assert_eq!(
            second
                .leader_store
                .read_current()
                .expect("read current leader"),
            Some(second_record),
            "stale renewal and release must preserve the higher-epoch holder"
        );
    }
}
