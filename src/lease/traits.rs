//! Core lease traits and types.

use std::fmt;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

// Type alias to improve readability and satisfy Clippy's type_complexity lint
type ReleaseFn = Arc<Mutex<Option<Box<dyn FnOnce() + Send>>>>;

/// Monotonic validity for one cloud-lease acquisition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LeaseValidityState {
    Inactive,
    Active { epoch: u64, valid_until: Instant },
    Fenced { epoch: u64 },
}

/// Shared lease validity watched independently from provider renewal I/O.
pub(crate) struct LeaseValidity {
    state: Mutex<LeaseValidityState>,
    changed: Condvar,
}

impl LeaseValidity {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(LeaseValidityState::Inactive),
            changed: Condvar::new(),
        }
    }

    pub(crate) fn activate(&self, epoch: u64, valid_until: Instant) -> Result<(), LeaseError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if valid_until <= Instant::now() {
            return Err(LeaseError::Internal(
                "cloud lease expired before acquisition completed".to_string(),
            ));
        }
        if !matches!(*state, LeaseValidityState::Inactive) {
            return Err(LeaseError::Internal(
                "cloud lease validity was already active".to_string(),
            ));
        }
        *state = LeaseValidityState::Active { epoch, valid_until };
        self.changed.notify_all();
        Ok(())
    }

    pub(crate) fn advance(&self, epoch: u64, candidate_until: Instant) -> Result<(), LeaseError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = Instant::now();
        match *state {
            LeaseValidityState::Active {
                epoch: active_epoch,
                valid_until,
            } if active_epoch == epoch && valid_until > now && candidate_until > now => {
                *state = LeaseValidityState::Active {
                    epoch,
                    valid_until: candidate_until,
                };
                self.changed.notify_all();
                Ok(())
            }
            LeaseValidityState::Active {
                epoch: active_epoch,
                ..
            } => {
                *state = LeaseValidityState::Fenced {
                    epoch: active_epoch,
                };
                self.changed.notify_all();
                Err(LeaseError::RenewalFailed(
                    "cloud lease renewal completed after monotonic validity was lost".to_string(),
                ))
            }
            LeaseValidityState::Fenced { .. } => Err(LeaseError::RenewalFailed(
                "cloud lease acquisition is terminally fenced".to_string(),
            )),
            LeaseValidityState::Inactive => Err(LeaseError::Internal(
                "cloud lease validity is inactive".to_string(),
            )),
        }
    }

    pub(crate) fn fence(&self, epoch: u64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if matches!(
            *state,
            LeaseValidityState::Active {
                epoch: active_epoch,
                ..
            } if active_epoch == epoch
        ) {
            *state = LeaseValidityState::Fenced { epoch };
            self.changed.notify_all();
        }
    }

    pub(crate) fn deactivate(&self, epoch: u64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if matches!(
            *state,
            LeaseValidityState::Active { epoch: active_epoch, .. }
                | LeaseValidityState::Fenced { epoch: active_epoch }
                if active_epoch == epoch
        ) {
            *state = LeaseValidityState::Inactive;
            self.changed.notify_all();
        }
    }

    pub(crate) fn snapshot(&self) -> LeaseValidityState {
        *self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn remaining(&self, epoch: u64) -> Result<Duration, LeaseError> {
        match self.snapshot() {
            LeaseValidityState::Active {
                epoch: active_epoch,
                valid_until,
            } if active_epoch == epoch => {
                let remaining = valid_until.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    self.fence(epoch);
                    Err(LeaseError::RenewalFailed(
                        "lease renewal crossed monotonic expiry".to_string(),
                    ))
                } else {
                    Ok(remaining)
                }
            }
            _ => Err(LeaseError::RenewalFailed(
                "lease is no longer valid for renewal".to_string(),
            )),
        }
    }

    pub(crate) fn wait_for_change(
        &self,
        observed: LeaseValidityState,
        timeout: Duration,
        running: &std::sync::atomic::AtomicBool,
    ) -> LeaseValidityState {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (state, _) = self
            .changed
            .wait_timeout_while(state, timeout, |state| {
                *state == observed && running.load(std::sync::atomic::Ordering::Acquire)
            })
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *state
    }

    pub(crate) fn notify_all(&self) {
        self.changed.notify_all();
    }
}

/// Error type for lease operations.
///
/// This taxonomy exists so `LeaseHeld` means one thing and one thing only:
/// confirmed, safe-to-retry contention — a live holder genuinely owns the
/// lease, or a conditional write/delete genuinely lost a race to a
/// concurrent acquirer. Every other failure mode gets its own variant so a
/// caller can no longer conflate "someone else holds it" with "we don't
/// know" (I/O, timeout, auth, transport, non-precondition HTTP failures),
/// "the persisted state can't be interpreted" (malformed/ambiguous), "the
/// fencing epoch is exhausted", or "this shouldn't be reachable" (an
/// internal invariant violation, not an environmental condition).
#[derive(Debug)]
pub enum LeaseError {
    /// Confirmed, safe-to-retry contention: another live holder genuinely
    /// owns the lease, or a conditional write/delete genuinely lost a race
    /// to a concurrent acquirer.
    AcquisitionFailed(String),
    /// Lease renewal failed (instance should stop accepting writes).
    RenewalFailed(String),
    /// I/O, timeout, auth, transport, or non-precondition HTTP failure. The
    /// outcome is unknown — this is NOT proof another instance holds the
    /// lease.
    IoError(String),
    /// Persisted lease state could not be interpreted (malformed or
    /// ambiguous), so ownership cannot be determined either way.
    Indeterminate(String),
    /// The lease's fencing epoch counter cannot advance any further.
    EpochExhausted,
    /// This instance already holds (or is already mid-acquiring) the lease.
    /// Not contention with another instance — the caller should back off,
    /// not treat this as evidence someone else owns it.
    AlreadyAcquired(String),
    /// An internal invariant was violated — a logic bug in this instance's
    /// own state machine, not an environmental condition another retry
    /// could resolve.
    Internal(String),
    /// Lease was already released.
    AlreadyReleased,
}

impl fmt::Display for LeaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AcquisitionFailed(msg) => write!(f, "lease acquisition failed: {msg}"),
            Self::RenewalFailed(msg) => write!(f, "lease renewal failed: {msg}"),
            Self::IoError(msg) => write!(f, "lease I/O error: {msg}"),
            Self::Indeterminate(msg) => write!(f, "lease state is indeterminate: {msg}"),
            Self::EpochExhausted => write!(f, "lease fencing epoch is exhausted"),
            Self::AlreadyAcquired(msg) => write!(f, "lease already acquired: {msg}"),
            Self::Internal(msg) => write!(f, "lease internal error: {msg}"),
            Self::AlreadyReleased => write!(f, "lease already released"),
        }
    }
}

impl std::error::Error for LeaseError {}

impl From<std::io::Error> for LeaseError {
    fn from(err: std::io::Error) -> Self {
        LeaseError::IoError(err.to_string())
    }
}

impl From<crate::io::traits::FsError> for LeaseError {
    fn from(err: crate::io::traits::FsError) -> Self {
        LeaseError::IoError(err.to_string())
    }
}

/// RAII guard for the primary lease.
///
/// Executes the provided release function when dropped (if one was supplied).
/// Some `PrimaryLease` implementations return a token-style guard whose Drop does
/// not perform release — callers must consult `PrimaryLease::try_acquire` and may
/// need to call `PrimaryLease::release()` explicitly.
/// Non-cloneable to ensure single ownership.
pub struct LeaseGuard {
    release_fn: ReleaseFn,
}

impl LeaseGuard {
    /// Create a new lease guard with the given release function.
    pub(crate) fn new(release_fn: impl FnOnce() + Send + 'static) -> Self {
        Self {
            release_fn: std::sync::Arc::new(std::sync::Mutex::new(Some(Box::new(release_fn)))),
        }
    }

    /// Create a token-style guard that does not perform any release action on Drop.
    ///
    /// Useful for lease implementations that manage release separately (for example,
    /// release is tied to the engine lifetime). The token still conveys "lease held"
    /// to callers but dropping it is a no-op.
    pub(crate) fn token() -> Self {
        Self {
            release_fn: std::sync::Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// Create a guard that will call `PrimaryLease::release()` on the provided lease
    /// when the guard is dropped (RAII semantics). Useful for callers that hold an
    /// `Arc<dyn PrimaryLease>` and want the guard to own the release lifecycle.
    pub(crate) fn for_lease(lease: std::sync::Arc<dyn PrimaryLease>) -> Self {
        Self::new(move || {
            let _ = lease.release();
        })
    }

    /// Explicitly release the lease.
    ///
    /// After this call, the guard is empty and dropping it is a no-op.
    #[cfg(test)]
    pub fn release(self) {
        if let Ok(mut guard) = self.release_fn.lock() {
            if let Some(release_fn) = guard.take() {
                release_fn();
            }
        }
    }
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.release_fn.lock() {
            if let Some(release_fn) = guard.take() {
                release_fn();
            }
        }
    }
}

/// Primary instance lease trait.
///
/// Implementations MUST provide:
/// - **Exclusive acquisition**: Only one instance can hold the lease at a time
/// - **TTL semantics**: Lease expires automatically if not renewed
/// - **Idempotent renewal**: Safe to call renewal multiple times
/// - **Safe release**: Release should succeed even if lease expired
pub trait PrimaryLease: Send + Sync {
    /// Attempt to acquire the primary lease.
    ///
    /// Returns:
    /// - `Ok(LeaseGuard)` if lease acquired successfully (the returned guard will
    ///   call `PrimaryLease::release()` on Drop)
    /// - `Err(LeaseError::AcquisitionFailed)` if another instance holds the lease
    /// - `Err(LeaseError::IoError)` for transient failures
    ///
    /// NOTE: this method takes ownership of the lease implementation via `Arc<Self>`
    /// so the returned `LeaseGuard` can hold a reference to the lease and perform
    /// an automatic release when dropped. Callers that hold an `Arc<dyn PrimaryLease>`
    /// should call this method on the `Arc` (e.g. `lease.clone().try_acquire()`).
    fn try_acquire(self: std::sync::Arc<Self>) -> Result<LeaseGuard, LeaseError>;

    /// Renew the lease (extend TTL).
    ///
    /// This must be called periodically (typically every `ttl / 2` or `ttl / 3`)
    /// to maintain the lease. Failure to renew means loss of primary status.
    ///
    /// Returns:
    /// - `Ok(())` if renewal succeeded
    /// - `Err(LeaseError::RenewalFailed)` if lease was lost or expired
    /// - `Err(LeaseError::IoError)` for transient failures
    fn renew(&self) -> Result<(), LeaseError>;

    /// Release the lease (clean shutdown).
    ///
    /// Should succeed even if the lease has already expired.
    /// Idempotent—safe to call multiple times.
    fn release(&self) -> Result<(), LeaseError>;

    /// Get the lease TTL (time-to-live).
    ///
    /// Used by the heartbeat loop to determine renewal frequency.
    fn ttl(&self) -> Duration;

    /// Get a unique identifier for this lease holder (for observability).
    fn holder_id(&self) -> String;

    /// Get the monotonic epoch acquired during leadership.
    ///
    /// Every successful `try_acquire()` must return a strictly higher epoch
    /// than any prior acquisition against the same storage.
    fn epoch(&self) -> u64;

    /// Return the underlying leader store, if available.
    ///
    /// Used to inject the leader store into the WAL actor for epoch
    /// validation at sync boundaries.  Backends that don't have a
    /// leader store (e.g. cloud placeholder) return `None`.
    fn get_leader_store(&self) -> Option<Arc<dyn LeaderStore>> {
        None
    }
}

// ───────────────────────────────────────────────────────────────────────────
// Leader record & leader store
// ───────────────────────────────────────────────────────────────────────────

/// Persistent leader record stored at a well-known path in storage.
///
/// The `epoch` field is a monotonically increasing fencing token.  Every
/// successful leadership change strictly increases the epoch.  No two
/// nodes can own the same epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderRecord {
    /// Monotonically increasing fencing token.
    pub epoch: u64,
    /// Unique identity of the current holder (e.g. `pid@hostname`).
    pub holder_id: String,
    /// RFC-3339 timestamp when leadership was acquired.
    pub acquired_at: String,
}

/// Canonical serialization of the checksummed fields, in the fixed order
/// `format_leader_record` writes them — the exact bytes the checksum covers.
fn leader_record_checksum_body(rec: &LeaderRecord) -> String {
    format!(
        "epoch: {}\nholder_id: {}\nacquired_at: {}\n",
        rec.epoch, rec.holder_id, rec.acquired_at
    )
}

/// Format a `LeaderRecord` as a simple line-based text document, with a
/// trailing CRC32C checksum over the other three fields — this is the one
/// on-disk structure whose entire purpose is preventing concurrent-writer
/// corruption via fencing, so (like WAL frames and SST blocks) it must
/// detect bit-flip corruption rather than silently accept it.
pub fn format_leader_record(rec: &LeaderRecord) -> String {
    let body = leader_record_checksum_body(rec);
    let checksum = crc32c::crc32c(body.as_bytes());
    format!("{body}checksum: {checksum}\n")
}

/// Parse a `LeaderRecord` from the line-based text format.
///
/// A record with no `checksum` field is treated as valid-but-unchecked, so
/// upgrading in place over an existing pre-checksum on-disk record doesn't
/// break it. A record that *does* carry a `checksum` field is rejected
/// (returns `None`, the same "unparseable/corrupt" outcome as a missing
/// required field) if the checksum doesn't verify or doesn't parse.
pub fn parse_leader_record(content: &str) -> Option<LeaderRecord> {
    let mut epoch = None;
    let mut holder_id = None;
    let mut acquired_at = None;
    let mut checksum = None;

    for line in content.lines() {
        if let Some(value) = line.strip_prefix("epoch: ") {
            epoch = value.parse::<u64>().ok();
        } else if let Some(value) = line.strip_prefix("holder_id: ") {
            holder_id = Some(value.to_string());
        } else if let Some(value) = line.strip_prefix("acquired_at: ") {
            acquired_at = Some(value.to_string());
        } else if let Some(value) = line.strip_prefix("checksum: ") {
            checksum = Some(value.parse::<u32>().ok()?);
        }
    }

    let record = LeaderRecord {
        epoch: epoch?,
        holder_id: holder_id?,
        acquired_at: acquired_at?,
    };

    if let Some(expected) = checksum {
        if crc32c::crc32c(leader_record_checksum_body(&record).as_bytes()) != expected {
            return None;
        }
    }

    Some(record)
}

/// Abstraction over the persistent leader record storage.
///
/// Implementations use CAS semantics (atomic-rename for filesystems,
/// conditional PUT for object stores) — never file locks.
pub trait LeaderStore: Send + Sync {
    /// Atomically acquire leadership by incrementing the epoch via CAS.
    ///
    /// On success returns the newly written `LeaderRecord` with a strictly
    /// higher epoch than the previous one.  On conflict (another node won
    /// the race) returns `LeaseError::AcquisitionFailed`.
    fn acquire_leadership(&self, holder_id: &str) -> Result<LeaderRecord, LeaseError>;

    /// Atomically acquire leadership, guaranteeing the granted epoch is at
    /// least `max(current_epoch, minimum_epoch) + 1`.
    ///
    /// `minimum_epoch` lets a caller with out-of-band knowledge of a higher
    /// epoch (disaster recovery, a multi-replica coordinator, a migration
    /// tool) guarantee the granted epoch exceeds it — the property WAL
    /// fencing (`format/wal.md` §6.4) depends on. The default of `0` does
    /// not itself close that gap; it only enables a caller who *has*
    /// out-of-band epoch knowledge to use it.
    ///
    /// The default implementation ignores `minimum_epoch` and delegates to
    /// [`Self::acquire_leadership`] — preserving today's behavior exactly
    /// for any implementor that hasn't opted in by overriding this method.
    /// `FsLeaderStore` overrides it to honor the floor.
    ///
    /// Not yet wired into engine startup — that requires reordering startup
    /// so WAL recovery's `max_epoch_seen` can be threaded in as the floor,
    /// tracked as separate follow-up work. Exercised directly by
    /// `FsLeaderStore` tests today.
    #[cfg_attr(not(test), allow(dead_code))]
    fn acquire_leadership_with_minimum_epoch(
        &self,
        holder_id: &str,
        minimum_epoch: u64,
    ) -> Result<LeaderRecord, LeaseError> {
        let _ = minimum_epoch;
        self.acquire_leadership(holder_id)
    }

    /// Read the current leader record from storage (non-locking).
    fn read_current(&self) -> Result<Option<LeaderRecord>, LeaseError>;

    /// Renew backend-specific leadership validity for the current holder.
    ///
    /// Stores that do not manage renewable leases retain the default
    /// fail-closed implementation.
    fn renew_leadership(&self, _holder_id: &str, _expected_epoch: u64) -> Result<(), LeaseError> {
        Err(LeaseError::RenewalFailed(
            "leader store does not support lease renewal".to_string(),
        ))
    }

    /// Release leadership only if the supplied holder and epoch still own it.
    fn release_leadership(&self, _holder_id: &str, _expected_epoch: u64) -> Result<(), LeaseError> {
        Err(LeaseError::Internal(
            "leader store does not support lease release".to_string(),
        ))
    }

    /// Apply the lease takeover clock-skew allowance used by this store.
    fn set_clock_skew_tolerance(&self, _tolerance: Duration) -> Result<(), LeaseError> {
        Err(LeaseError::Internal(
            "leader store does not support clock-skew configuration".to_string(),
        ))
    }

    #[cfg(test)]
    fn read_test_coordination_document(&self) -> Result<Option<String>, LeaseError> {
        Err(LeaseError::Internal(
            "leader store has no test coordination document".to_string(),
        ))
    }

    #[cfg(test)]
    fn write_test_coordination_document(&self, _content: &str) -> Result<(), LeaseError> {
        Err(LeaseError::Internal(
            "leader store has no test coordination document".to_string(),
        ))
    }

    #[cfg(test)]
    fn remove_test_coordination_document(&self) -> Result<(), LeaseError> {
        Err(LeaseError::Internal(
            "leader store has no test coordination document".to_string(),
        ))
    }

    /// Convenience: read current record and verify both the epoch and the
    /// expected holder still own it.
    ///
    /// Comparing `epoch` alone is not sufficient fencing: under the
    /// epoch-CAS acquisition protocol two legitimate holders never share one
    /// epoch through conforming writers alone, but `holder_id` is the
    /// defense against what that protocol doesn't cover — a tampered or
    /// externally rewritten leader record, a non-conforming writer sharing
    /// the directory, or a defect in another implementation's CAS logic.
    /// lsm-spec format/lease.md §5.1, §6 item 1.
    fn validate_epoch(
        &self,
        expected_holder_id: &str,
        expected_epoch: u64,
    ) -> Result<(), LeaseError> {
        match self.read_current()? {
            Some(rec) if rec.epoch == expected_epoch && rec.holder_id == expected_holder_id => {
                Ok(())
            }
            Some(rec) => Err(LeaseError::RenewalFailed(format!(
                "epoch/holder mismatch: expected holder={expected_holder_id} epoch={expected_epoch}, found holder={} epoch={}",
                rec.holder_id, rec.epoch
            ))),
            None => Err(LeaseError::RenewalFailed(
                "leader record missing".to_string(),
            )),
        }
    }

    /// Validate the current writer within a caller-supplied wait budget.
    ///
    /// Non-blocking and local stores may retain the default implementation.
    /// Provider-backed stores override this so their coordination read cannot
    /// outlive a runtime request's shared deadline.
    fn validate_epoch_with_timeout(
        &self,
        expected_holder_id: &str,
        expected_epoch: u64,
        _timeout: Duration,
    ) -> Result<(), LeaseError> {
        self.validate_epoch(expected_holder_id, expected_epoch)
    }
}

#[cfg(test)]
mod checksum_tests {
    use super::{format_leader_record, parse_leader_record, LeaderRecord};

    fn sample_record() -> LeaderRecord {
        LeaderRecord {
            epoch: 42,
            holder_id: "node-a@host".to_string(),
            acquired_at: "2026-02-16T12:00:00Z".to_string(),
        }
    }

    #[test]
    fn should_roundtrip_record_with_checksum() {
        // Arrange
        let record = sample_record();

        // Act
        let serialized = format_leader_record(&record);
        let parsed = parse_leader_record(&serialized);

        // Assert
        assert!(serialized.contains("checksum: "), "{serialized}");
        assert_eq!(parsed, Some(record));
    }

    #[test]
    fn should_reject_record_when_checksum_mismatches_a_bit_flipped_epoch() {
        // Arrange: flip the epoch after the checksum was computed, as a
        // corrupted-on-disk bit-flip would — the value still parses as a
        // smaller-but-valid u64, so only the checksum can catch it.
        let record = sample_record();
        let serialized = format_leader_record(&record);
        let corrupted = serialized.replacen("epoch: 42\n", "epoch: 41\n", 1);

        // Act
        let parsed = parse_leader_record(&corrupted);

        // Assert
        assert_eq!(
            parsed, None,
            "a checksum mismatch must be treated as corrupt"
        );
    }

    #[test]
    fn should_reject_record_when_checksum_mismatches_a_corrupted_holder_id() {
        // Arrange
        let record = sample_record();
        let serialized = format_leader_record(&record);
        let corrupted =
            serialized.replacen("holder_id: node-a@host\n", "holder_id: node-x@host\n", 1);

        // Act
        let parsed = parse_leader_record(&corrupted);

        // Assert
        assert_eq!(parsed, None);
    }

    #[test]
    fn should_accept_record_with_no_checksum_field_for_backward_compatibility() {
        // Arrange: the pre-checksum on-disk format, so upgrade-in-place
        // doesn't reject an existing valid lease record.
        let legacy = "epoch: 7\nholder_id: legacy-holder\nacquired_at: 2026-01-01T00:00:00Z\n";

        // Act
        let parsed = parse_leader_record(legacy);

        // Assert
        assert_eq!(
            parsed,
            Some(LeaderRecord {
                epoch: 7,
                holder_id: "legacy-holder".to_string(),
                acquired_at: "2026-01-01T00:00:00Z".to_string(),
            })
        );
    }

    #[test]
    fn should_reject_record_with_unparseable_checksum_field() {
        // Arrange
        let record = sample_record();
        let serialized = format_leader_record(&record);
        let corrupted = serialized.replacen(
            &format!(
                "checksum: {}",
                crc32c::crc32c(
                    format!(
                        "epoch: {}\nholder_id: {}\nacquired_at: {}\n",
                        record.epoch, record.holder_id, record.acquired_at
                    )
                    .as_bytes(),
                )
            ),
            "checksum: not-a-number",
            1,
        );

        // Act
        let parsed = parse_leader_record(&corrupted);

        // Assert
        assert_eq!(parsed, None);
    }
}
