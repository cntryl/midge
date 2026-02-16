//! Core lease traits and types.

use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

// Type alias to improve readability and satisfy Clippy's type_complexity lint
type ReleaseFn = Arc<Mutex<Option<Box<dyn FnOnce() + Send>>>>;

/// Error type for lease operations.
#[derive(Debug)]
pub enum LeaseError {
    /// Failed to acquire lease (likely held by another instance).
    AcquisitionFailed(String),
    /// Lease renewal failed (instance should stop accepting writes).
    RenewalFailed(String),
    /// I/O or backend error.
    IoError(String),
    /// Lease was already released.
    AlreadyReleased,
}

impl fmt::Display for LeaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AcquisitionFailed(msg) => write!(f, "lease acquisition failed: {}", msg),
            Self::RenewalFailed(msg) => write!(f, "lease renewal failed: {}", msg),
            Self::IoError(msg) => write!(f, "lease I/O error: {}", msg),
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

/// Format a `LeaderRecord` as a simple line-based text document.
pub fn format_leader_record(rec: &LeaderRecord) -> String {
    format!(
        "epoch: {}\nholder_id: {}\nacquired_at: {}\n",
        rec.epoch, rec.holder_id, rec.acquired_at
    )
}

/// Parse a `LeaderRecord` from the line-based text format.
pub fn parse_leader_record(content: &str) -> Option<LeaderRecord> {
    let mut epoch = None;
    let mut holder_id = None;
    let mut acquired_at = None;

    for line in content.lines() {
        if let Some(value) = line.strip_prefix("epoch: ") {
            epoch = value.parse::<u64>().ok();
        } else if let Some(value) = line.strip_prefix("holder_id: ") {
            holder_id = Some(value.to_string());
        } else if let Some(value) = line.strip_prefix("acquired_at: ") {
            acquired_at = Some(value.to_string());
        }
    }

    Some(LeaderRecord {
        epoch: epoch?,
        holder_id: holder_id?,
        acquired_at: acquired_at?,
    })
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

    /// Read the current leader record from storage (non-locking).
    fn read_current(&self) -> Result<Option<LeaderRecord>, LeaseError>;

    /// Convenience: read current record and verify the epoch matches.
    fn validate_epoch(&self, expected_epoch: u64) -> Result<(), LeaseError> {
        match self.read_current()? {
            Some(rec) if rec.epoch == expected_epoch => Ok(()),
            Some(rec) => Err(LeaseError::RenewalFailed(format!(
                "epoch mismatch: expected {}, found {} (holder: {})",
                expected_epoch, rec.epoch, rec.holder_id
            ))),
            None => Err(LeaseError::RenewalFailed(
                "leader record missing".to_string(),
            )),
        }
    }
}
