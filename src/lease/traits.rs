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
    /// - `Ok(LeaseGuard)` if lease acquired successfully
    /// - `Err(LeaseError::AcquisitionFailed)` if another instance holds the lease
    /// - `Err(LeaseError::IoError)` for transient failures
    ///
    /// Note: some implementations may return a token-style `LeaseGuard` whose `Drop`
    /// does NOT perform lease release (release is instead owned by the engine's
    /// shutdown path). Callers should consult the concrete `PrimaryLease` docs and
    /// call `release()` explicitly when required.
    ///
    /// The lease MUST be acquired before starting the engine.
    fn try_acquire(&self) -> Result<LeaseGuard, LeaseError>;

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
}
