//! Database locking traits and shared types.

use std::time::Duration;

use crate::error::MidgeResult;

/// Database lock trait - abstracts local file lock vs cloud distributed lease
pub trait DbLock: Send + Sync {
    /// Try to acquire the lock with a timeout.
    /// Returns Ok if acquired, Err(DatabaseLocked) if timeout exceeded.
    fn try_acquire(&mut self, timeout: Duration) -> MidgeResult<()>;

    /// Renew the lock (update heartbeat timestamp).
    /// Called by renewal thread every ttl/2.
    fn renew(&mut self) -> MidgeResult<()>;

    /// Release the lock (on clean shutdown).
    fn release(&mut self) -> MidgeResult<()>;

    /// Check if this lock is currently held.
    fn is_held(&self) -> bool;
}
