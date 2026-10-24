//! Filesystem-based primary lease using CAS-via-rename leader records.
//!
//! This implementation uses a persistent leader record (`.midge_leader`) that
//! contains a monotonically increasing epoch, holder identity, and a
//! timestamp.  Leadership is acquired via atomic rename (CAS-via-rename)
//! on filesystems providing atomic rename and process-owned exclusive locks.
//!
//! ## Safety guarantees
//!
//! - **Fencing token**: Every leader gets a strictly increasing epoch
//! - **CAS semantics**: Two simultaneous acquirers — exactly one wins
//! - **Writer effect fencing**: exclusive filesystem locks serialize physical
//!   writes with lease transitions and release automatically on process exit
//!
//! ## Limitations
//!
//! - Requires a writable filesystem (not suitable for read-only media)
//! - Requires shared exclusive locks that are released when their owner exits
//! - Leader record is a small file; no built-in expiry without heartbeat

use super::clock::LeaseClock;
use super::fs_leader_store::FsLeaderStore;
use super::traits::{
    LeaderRecord, LeaderStore, LeaseError, LeaseGuard, LeaseValidity, PrimaryLease,
};
use crate::io::Fs;
#[cfg(test)]
use crate::io::{MockFs, RealFs};
#[cfg(test)]
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
    /// Judges whether another holder's record is stale.
    clock: LeaseClock,
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

    #[cfg(test)]
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
        Ok(Self::with_root(
            fs,
            ttl,
            clock_skew_tolerance,
            LeaseClock::default(),
        ))
    }

    /// A lease whose coordination files live in the database `root`, timed
    /// by `clock`.
    #[cfg(test)]
    pub(crate) fn with_root(
        root: Arc<dyn Fs>,
        ttl: Duration,
        clock_skew_tolerance: Duration,
        clock: LeaseClock,
    ) -> Self {
        Self::with_root_and_timeout(
            root,
            ttl,
            clock_skew_tolerance,
            clock,
            crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
        )
    }

    /// Construct a lease with the configured storage budget for transitions.
    pub(crate) fn with_root_and_timeout(
        root: Arc<dyn Fs>,
        ttl: Duration,
        clock_skew_tolerance: Duration,
        clock: LeaseClock,
        storage_io_timeout: Duration,
    ) -> Self {
        let fs = root;
        let instance = LEASE_INSTANCE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let holder_id = format!(
            "{}.{}@{}",
            std::process::id(),
            instance,
            hostname::get()
                .unwrap_or_else(|_| std::ffi::OsString::from("unknown"))
                .to_string_lossy()
        );

        let leader_store = Arc::new(FsLeaderStore::with_timeout(
            fs,
            clock.clone(),
            storage_io_timeout,
        ));

        Self {
            leader_store,
            holder_id,
            acquired: AtomicBool::new(false),
            acquired_epoch: AtomicU64::new(0),
            validity: Arc::new(LeaseValidity::new()),
            ttl,
            clock_skew_tolerance,
            clock,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_with_fs_for_test(fs: Arc<dyn Fs>, ttl: Duration) -> Self {
        Self::with_root(fs, ttl, ttl / 2, LeaseClock::default())
    }

    fn validate_takeover_record(
        &self,
        existing: Option<&LeaderRecord>,
        report_takeover: bool,
    ) -> Result<(), LeaseError> {
        let Some(existing) = existing else {
            return Ok(());
        };
        if existing.holder_id == self.holder_id {
            return Ok(());
        }

        let acquired_at =
            chrono::DateTime::parse_from_rfc3339(&existing.acquired_at).map_err(|_| {
                LeaseError::Indeterminate(format!(
                    "leader timestamp is invalid; ownership is ambiguous (holder: {}, epoch: {})",
                    existing.holder_id, existing.epoch
                ))
            })?;
        let signed_age = self.clock.now().signed_duration_since(acquired_at);
        if signed_age < chrono::Duration::zero() {
            return Err(LeaseError::Indeterminate(format!(
                "leader timestamp is in the future; ownership is ambiguous (holder: {}, epoch: {})",
                existing.holder_id, existing.epoch
            )));
        }
        let age = signed_age.to_std().map_err(|_| {
            LeaseError::Indeterminate(format!(
                "leader age is ambiguous (holder: {}, epoch: {})",
                existing.holder_id, existing.epoch
            ))
        })?;
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
        if report_takeover && existing.acquired_at != super::fs_leader_store::RELEASED_TIMESTAMP {
            tracing::warn!(
                old_holder = %existing.holder_id,
                old_epoch = existing.epoch,
                age_secs = age.as_secs(),
                "taking over stale leader record (previous holder likely crashed)"
            );
        }
        Ok(())
    }

    pub(crate) fn lease_validity(&self) -> Arc<LeaseValidity> {
        Arc::clone(&self.validity)
    }

    fn acquire_in_scope(
        self: Arc<Self>,
        minimum_epoch: u64,
        scope: Option<&crate::common::DeadlineScope>,
    ) -> Result<LeaseGuard, LeaseError> {
        if let Some(scope) = scope {
            scope
                .check("filesystem primary lease acquisition")
                .map_err(super::traits::scope_lease_error)?;
        }
        if self.acquired.load(Ordering::Acquire) {
            return Err(LeaseError::AlreadyAcquired(
                "lease already acquired by this instance".to_string(),
            ));
        }

        // Reject an ordinary second open before it takes the writer-effect
        // lock. The locked check below closes the race with a concurrent
        // acquisition; the early read keeps a failed open from delaying the
        // live writer's next WAL effect.
        let observed = match scope {
            Some(scope) => self
                .leader_store
                .read_current_with_timeout(scope.deadline().remaining())?,
            None => self.leader_store.read_current()?,
        };
        self.validate_takeover_record(observed.as_ref(), false)?;

        // The validation closure's own error variant (Indeterminate for an
        // unparseable/ambiguous record, AcquisitionFailed for confirmed
        // contention) propagates verbatim — it must not be collapsed into a
        // single blanket variant here, or every distinction made below would
        // be silently discarded.
        // Other nodes time takeover from the wall-clock stamp the store takes
        // before its write, so local validity is anchored before it too;
        // counting a slow fsync or rename here would outlive the record.
        let validity_anchor = std::time::Instant::now();
        let record = self.acquire_record(minimum_epoch, scope)?;

        let epoch = record.epoch;
        if let Err(error) = self.validity.activate(epoch, validity_anchor + self.ttl()) {
            return Err(self.cleanup_failed_acquisition(epoch, scope, error));
        }
        self.acquired_epoch.store(epoch, Ordering::Release);
        self.acquired.store(true, Ordering::Release);

        tracing::info!(
            holder_id = %self.holder_id,
            epoch = epoch,
            "primary lease acquired via leader store"
        );

        let guard = LeaseGuard::for_lease(self);
        if let Some(scope) = scope {
            scope.resolve_ambiguous_mutation();
            scope
                .check("confirmed filesystem primary lease acquisition")
                .map_err(super::traits::scope_lease_error)?;
        }
        Ok(guard)
    }

    fn acquire_record(
        &self,
        minimum_epoch: u64,
        scope: Option<&crate::common::DeadlineScope>,
    ) -> Result<LeaderRecord, LeaseError> {
        let Some(scope) = scope else {
            return self
                .leader_store
                .acquire_leadership_after_validation_and_publish(
                    &self.holder_id,
                    |existing| self.validate_takeover_record(existing, true),
                    |_| Ok(()),
                    minimum_epoch,
                );
        };
        let mut cleanup_epoch = None;
        let result = {
            let mut observe = |epoch| cleanup_epoch = Some(epoch);
            let mut acquisition = super::traits::LeaderAcquisition::within(scope, &mut observe);
            let result = self
                .leader_store
                .acquire_leadership_after_validation_with_context(
                    &self.holder_id,
                    minimum_epoch,
                    &mut acquisition,
                    |existing| self.validate_takeover_record(existing, true),
                );
            acquisition.finish(result)
        };
        result.map_err(|error| match cleanup_epoch {
            Some(epoch) => self.cleanup_failed_acquisition(epoch, Some(scope), error),
            None => error,
        })
    }

    fn cleanup_failed_acquisition(
        &self,
        epoch: u64,
        scope: Option<&crate::common::DeadlineScope>,
        error: LeaseError,
    ) -> LeaseError {
        match self.leader_store.release_if_owner(&self.holder_id, epoch) {
            Ok(_) => {
                if let Some(scope) = scope {
                    scope.resolve_ambiguous_mutation();
                }
                error
            }
            Err(cleanup_error) => LeaseError::Indeterminate(format!(
                "filesystem acquisition failed ({error}) and exact-owner cleanup failed: {cleanup_error}"
            )),
        }
    }
}

impl PrimaryLease for FileSystemLease {
    fn try_acquire(self: Arc<Self>) -> Result<LeaseGuard, LeaseError> {
        self.try_acquire_with_minimum_epoch(0)
    }

    fn try_acquire_with_minimum_epoch(
        self: Arc<Self>,
        minimum_epoch: u64,
    ) -> Result<LeaseGuard, LeaseError> {
        self.acquire_in_scope(minimum_epoch, None)
    }

    fn try_acquire_with_minimum_epoch_within(
        self: Arc<Self>,
        minimum_epoch: u64,
        scope: &crate::common::DeadlineScope,
    ) -> Result<LeaseGuard, LeaseError> {
        self.acquire_in_scope(minimum_epoch, Some(scope))
    }

    fn renew(&self) -> Result<(), LeaseError> {
        let our_epoch = self.acquired_epoch.load(Ordering::Acquire);
        let result = (|| {
            if !self.acquired.load(Ordering::Acquire) {
                return Err(LeaseError::RenewalFailed("lease not acquired".to_string()));
            }
            self.validity.remaining(our_epoch)?;
            let validity_anchor = std::time::Instant::now();
            self.leader_store
                .refresh_timestamp(&self.holder_id, our_epoch)?;
            self.validity
                .advance(our_epoch, validity_anchor + self.ttl())
        })();
        if let Err(error) = result {
            if self
                .validity
                .is_transient_renewal_failure(&error, our_epoch)
            {
                return Err(error);
            }
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

#[cfg(test)]
mod tests {
    use super::super::traits::{format_leader_record, LeaderRecord};
    use super::*;

    #[derive(Clone)]
    struct CapturedLogs(Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for CapturedLogs {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    #[test]
    fn should_log_crash_takeover_only_when_previous_holder_did_not_release() {
        // Arrange
        let dir = tempfile::tempdir().unwrap();
        let first = Arc::new(FileSystemLease::new(dir.path(), false).unwrap());
        let first_guard = Arc::clone(&first).try_acquire().unwrap();
        let epoch = first.epoch();
        first.release().unwrap();
        drop(first_guard);
        let logs = CapturedLogs(Arc::new(std::sync::Mutex::new(Vec::new())));
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(logs.clone())
            .finish();

        // Act
        tracing::subscriber::with_default(subscriber, || {
            let second = Arc::new(FileSystemLease::new(dir.path(), false).unwrap());
            let guard = Arc::clone(&second).try_acquire().unwrap();
            let second_epoch = second.epoch();
            assert!(second_epoch > epoch);
            second.release().unwrap();
            drop(guard);
            assert!(!String::from_utf8(logs.0.lock().unwrap().clone())
                .unwrap()
                .contains("previous holder likely crashed"));

            let record = LeaderRecord {
                epoch: second_epoch,
                holder_id: "expired-holder".to_string(),
                acquired_at: (chrono::Utc::now() - chrono::Duration::minutes(2)).to_rfc3339(),
            };
            std::fs::write(
                dir.path().join(".midge_leader"),
                format_leader_record(&record),
            )
            .unwrap();
            let third = Arc::new(FileSystemLease::new(dir.path(), false).unwrap());
            let _guard = Arc::clone(&third).try_acquire().unwrap();
            assert!(third.epoch() > record.epoch);
        });

        // Assert
        let output = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
        assert_eq!(output.matches("previous holder likely crashed").count(), 1);
        assert!(output.contains("old_holder=expired-holder"));
        let age = output
            .split("age_secs=")
            .nth(1)
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap()
            .parse::<u64>()
            .unwrap();
        assert!((120..150).contains(&age), "unexpected stale age: {age}");
    }

    /// In-memory filesystem whose next atomic rename fails once armed.
    struct FlakyRenameFs {
        inner: crate::io::MockFs,
        fail_next_rename: AtomicBool,
        effect_attempted: Option<std::sync::mpsc::SyncSender<()>>,
        cancel_after_leader_rename: Option<crate::common::DeadlineScope>,
    }

    impl crate::io::ReadFs for FlakyRenameFs {
        fn open_read(
            &self,
            path: &crate::io::FsPath,
        ) -> crate::io::FsResult<Box<dyn crate::io::ReadFile + '_>> {
            crate::io::Fs::open(self, path, crate::io::OpenOptions::READ_ONLY)
                .map(|file| file as Box<dyn crate::io::ReadFile>)
        }

        fn exists(&self, path: &crate::io::FsPath) -> crate::io::FsResult<bool> {
            self.inner.exists(path)
        }

        fn metadata(
            &self,
            path: &crate::io::FsPath,
        ) -> crate::io::FsResult<crate::io::traits::Metadata> {
            self.inner.metadata(path)
        }

        fn list_dir(
            &self,
            path: &crate::io::FsPath,
        ) -> crate::io::FsResult<Vec<crate::io::traits::DirEntry>> {
            self.inner.list_dir(path)
        }
    }

    impl crate::io::Fs for FlakyRenameFs {
        fn try_lock_exclusive(
            &self,
            path: &crate::io::FsPath,
        ) -> crate::io::FsResult<Box<dyn crate::io::FsLock>> {
            if path.0 == ".midge_writer_effect.v2.lock" {
                if let Some(attempted) = &self.effect_attempted {
                    let _ = attempted.try_send(());
                }
            }
            self.inner.try_lock_exclusive(path)
        }

        fn open(
            &self,
            path: &crate::io::FsPath,
            options: crate::io::OpenOptions,
        ) -> crate::io::FsResult<Box<dyn crate::io::File + '_>> {
            self.inner.open(path, options)
        }

        fn remove_file(&self, path: &crate::io::FsPath) -> crate::io::FsResult<()> {
            self.inner.remove_file(path)
        }

        fn create_dir_all(&self, path: &crate::io::FsPath) -> crate::io::FsResult<()> {
            self.inner.create_dir_all(path)
        }

        fn remove_dir_all(&self, path: &crate::io::FsPath) -> crate::io::FsResult<()> {
            self.inner.remove_dir_all(path)
        }

        fn sync_dir(
            &self,
            path: &crate::io::FsPath,
            durability: crate::io::Durability,
        ) -> crate::io::FsResult<()> {
            self.inner.sync_dir(path, durability)
        }

        fn rename_atomic(
            &self,
            from: &crate::io::FsPath,
            to: &crate::io::FsPath,
        ) -> crate::io::FsResult<()> {
            if self.fail_next_rename.swap(false, Ordering::AcqRel) {
                return Err(crate::io::FsError::Unavailable(
                    "injected transient rename failure".to_string(),
                ));
            }
            self.inner.rename_atomic(from, to)?;
            if to.0 == ".midge_leader" {
                if let Some(scope) = &self.cancel_after_leader_rename {
                    scope.cancel();
                }
            }
            Ok(())
        }
    }

    #[test]
    fn should_leave_filesystem_authority_unchanged_when_open_is_cancelled_waiting_for_effect_lock()
    {
        // Arrange
        let (attempted_tx, attempted_rx) = std::sync::mpsc::sync_channel(1);
        let fs = Arc::new(FlakyRenameFs {
            inner: crate::io::MockFs::new(),
            fail_next_rename: AtomicBool::new(false),
            effect_attempted: Some(attempted_tx),
            cancel_after_leader_rename: None,
        });
        let lease = Arc::new(FileSystemLease::new_with_fs_for_test(
            fs.clone() as Arc<dyn Fs>,
            Duration::from_secs(30),
        ));
        let held = fs
            .inner
            .try_lock_exclusive(&crate::io::FsPath::new(".midge_writer_effect.v2.lock"))
            .expect("hold predecessor effect");
        let scope = crate::common::DeadlineScope::new(
            crate::common::OperationDeadline::from_budget(Duration::from_secs(2)),
        );
        let worker_scope = scope.clone();
        let worker_lease = Arc::clone(&lease);
        let open = std::thread::spawn(move || {
            worker_lease.try_acquire_with_minimum_epoch_within(0, &worker_scope)
        });
        attempted_rx.recv().expect("open reached effect lock wait");

        // Act
        let ambiguous = scope.cancel();
        drop(held);
        let result = open.join().expect("open completes");

        // Assert
        assert!(result.is_err());
        assert!(
            lease.leader_store.read_current().unwrap().is_none(),
            "cancelled open must not publish or release a new epoch"
        );
        assert!(
            !ambiguous,
            "waiting for another effect has not admitted a lease mutation"
        );
        assert!(!crate::io::ReadFs::exists(
            fs.as_ref(),
            &crate::io::FsPath::new(".midge_leader.lock")
        )
        .unwrap());
    }

    #[test]
    fn should_preserve_filesystem_authority_when_open_deadline_expires_waiting_for_effect_lock() {
        // Arrange
        let fs: Arc<dyn Fs> = Arc::new(MockFs::new());
        let lease = Arc::new(FileSystemLease::new_with_fs_for_test(
            Arc::clone(&fs),
            Duration::from_secs(30),
        ));
        let held = fs
            .try_lock_exclusive(&crate::io::FsPath::new(".midge_writer_effect.v2.lock"))
            .expect("hold predecessor effect");
        let scope = crate::common::DeadlineScope::new(
            crate::common::OperationDeadline::from_budget(Duration::from_millis(10)),
        );
        let release = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            drop(held);
        });

        // Act
        let result = Arc::clone(&lease).try_acquire_with_minimum_epoch_within(0, &scope);
        release.join().expect("release predecessor effect");

        // Assert
        assert!(matches!(result, Err(LeaseError::Timeout(_))));
        assert!(
            lease.leader_store.read_current().unwrap().is_none(),
            "expired open must not publish or release a new epoch"
        );
        assert!(
            !scope.cancel(),
            "lock wait must not introduce lease uncertainty"
        );
    }

    #[test]
    fn should_release_exact_filesystem_owner_when_scope_cancels_after_leader_rename() {
        // Arrange
        let scope = crate::common::DeadlineScope::new(
            crate::common::OperationDeadline::from_budget(Duration::from_secs(1)),
        );
        let fs = Arc::new(FlakyRenameFs {
            inner: MockFs::new(),
            fail_next_rename: AtomicBool::new(false),
            effect_attempted: None,
            cancel_after_leader_rename: Some(scope.clone()),
        });
        let lease = Arc::new(FileSystemLease::new_with_fs_for_test(
            fs as Arc<dyn Fs>,
            Duration::from_secs(30),
        ));

        // Act: the filesystem applies the rename before the scoped wrapper
        // rejects its completion. No acquired guard is returned to the caller.
        let result = Arc::clone(&lease).try_acquire_with_minimum_epoch_within(0, &scope);
        let record = lease.leader_store.read_current().unwrap().unwrap();

        // Assert
        assert!(matches!(result, Err(LeaseError::Indeterminate(_))));
        assert_eq!(record.holder_id, lease.holder_id);
        assert_eq!(
            record.acquired_at,
            super::super::fs_leader_store::RELEASED_TIMESTAMP
        );
        assert!(!lease.acquired.load(Ordering::Acquire));
        assert!(!scope.cancel(), "exact-owner cleanup resolves uncertainty");
        let guard = Arc::clone(&lease)
            .try_acquire()
            .expect("successor open can acquire");
        assert!(lease.epoch() > record.epoch);
        drop(guard);
    }

    #[test]
    fn should_keep_filesystem_lease_active_when_one_timestamp_write_fails() {
        // Arrange
        let fs = Arc::new(FlakyRenameFs {
            inner: crate::io::MockFs::new(),
            fail_next_rename: AtomicBool::new(false),
            effect_attempted: None,
            cancel_after_leader_rename: None,
        });
        let lease = Arc::new(FileSystemLease::new_with_fs_for_test(
            Arc::clone(&fs) as Arc<dyn crate::io::Fs>,
            Duration::from_secs(30),
        ));
        let _guard = Arc::clone(&lease).try_acquire().expect("acquire lease");
        let epoch = lease.epoch();
        fs.fail_next_rename.store(true, Ordering::Release);

        // Act
        let failed = lease.renew();
        let retried = lease.renew();

        // Assert
        assert!(matches!(failed, Err(LeaseError::IoError(_))), "{failed:?}");
        assert!(retried.is_ok(), "{retried:?}");
        assert!(lease.acquired.load(Ordering::Acquire));
        assert!(lease.validity.remaining(epoch).is_ok());
    }

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
    fn should_reject_live_second_open_before_waiting_for_writer_effect() {
        // Arrange: the live writer is in a storage effect while another
        // engine tries to open the same filesystem database.
        let fs: Arc<dyn Fs> = Arc::new(MockFs::new());
        let first = Arc::new(FileSystemLease::new_with_fs_for_test(
            Arc::clone(&fs),
            Duration::from_secs(30),
        ));
        let _first_guard = Arc::clone(&first).try_acquire().expect("first lease");
        let effect_store = Arc::new(FsLeaderStore::new(Arc::clone(&fs), LeaseClock::default()));
        let effect_entered = Arc::new(std::sync::Barrier::new(2));
        let effect_continue = Arc::new(std::sync::Barrier::new(2));
        let effect_store_worker = Arc::clone(&effect_store);
        let entered_worker = Arc::clone(&effect_entered);
        let continue_worker = Arc::clone(&effect_continue);
        let holder_id = first.holder_id();
        let epoch = first.epoch();
        let effect = std::thread::spawn(move || {
            let mut operation = || {
                entered_worker.wait();
                continue_worker.wait();
            };
            effect_store_worker.with_writer_epoch(
                &holder_id,
                epoch,
                Duration::from_secs(1),
                &mut operation,
            )
        });
        effect_entered.wait();

        let second = Arc::new(FileSystemLease::new_with_fs_for_test(
            fs,
            Duration::from_secs(30),
        ));
        let (done, result) = std::sync::mpsc::sync_channel(1);
        let open_attempt = std::thread::spawn(move || {
            let result = Arc::clone(&second).try_acquire().map(|_| ());
            done.send(result).expect("report second open result");
        });

        // Act: look for the failed open while the first effect remains held.
        let result_during_effect = result.recv_timeout(Duration::from_millis(100));

        // Release and join before asserting, so a regression cannot strand a
        // worker holding the effect lock.
        effect_continue.wait();
        effect.join().expect("writer effect completed").unwrap();
        open_attempt.join().expect("second open completed");

        // Assert: the second open reports confirmed contention promptly and
        // never takes the lock away from the live writer.
        assert!(
            result_during_effect.is_ok(),
            "failed open waited for the live writer-effect lock"
        );
        assert!(matches!(
            result_during_effect.unwrap(),
            Err(LeaseError::AcquisitionFailed(_))
        ));
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

#[cfg(test)]
mod validity_anchor_tests {
    use super::*;
    use crate::io::traits::{DirEntry, Metadata};
    use crate::io::{Durability, File, FsPath, FsResult, OpenOptions, ReadFs};

    /// Stalls every rename, as a slow NFS commit would.
    struct SlowRenameFs {
        inner: MockFs,
        stall: Duration,
    }

    impl ReadFs for SlowRenameFs {
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

    impl Fs for SlowRenameFs {
        fn try_lock_exclusive(&self, path: &FsPath) -> FsResult<Box<dyn crate::io::FsLock>> {
            self.inner.try_lock_exclusive(path)
        }

        fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File + '_>> {
            self.inner.open(path, opts)
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
            std::thread::sleep(self.stall);
            self.inner.rename_atomic(from, to)
        }
    }

    fn valid_until(lease: &FileSystemLease) -> std::time::Instant {
        match lease.validity.snapshot() {
            crate::lease::traits::LeaseValidityState::Active { valid_until, .. } => valid_until,
            other => panic!("lease should be active, found {other:?}"),
        }
    }

    #[test]
    fn should_anchor_filesystem_lease_validity_before_the_leader_write_when_io_stalls() {
        // Arrange: other nodes time takeover from the wall-clock stamp taken
        // before the write, so local validity must not also count the stall.
        let ttl = Duration::from_secs(10);
        let stall = Duration::from_millis(300);
        let lease = Arc::new(FileSystemLease::new_with_fs_for_test(
            Arc::new(SlowRenameFs {
                inner: MockFs::new(),
                stall,
            }),
            ttl,
        ));

        // Act
        let before_acquire = std::time::Instant::now();
        let _guard = Arc::clone(&lease).try_acquire().expect("acquire");
        let acquired_until = valid_until(&lease);
        let before_renew = std::time::Instant::now();
        lease.renew().expect("renew");
        let renewed_until = valid_until(&lease);

        // Assert
        let slack = Duration::from_millis(100);
        assert!(
            acquired_until.duration_since(before_acquire) <= ttl + slack,
            "acquire validity counted the I/O stall"
        );
        assert!(
            renewed_until.duration_since(before_renew) <= ttl + slack,
            "renew validity counted the I/O stall"
        );
    }
}
