//! Shared wall-clock helpers for persisted timestamps.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Source of absolute Unix time used for TTL timestamps.
pub trait Clock: Send + Sync {
    /// Return absolute Unix time in milliseconds.
    fn now_millis(&self) -> u64;
}

#[derive(Clone)]
pub(crate) struct ClockHandle(pub Arc<ObservedClock>);

impl std::fmt::Debug for ClockHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ClockHandle(..)")
    }
}

#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_millis(&self) -> u64 {
        unix_time_millis()
    }
}

/// Process-local wall time that never moves backwards.
pub struct ObservedClock {
    source: Arc<dyn Clock>,
    floor: AtomicU64,
}

impl std::fmt::Debug for ObservedClock {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObservedClock")
            .field("floor", &self.floor.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl Default for ObservedClock {
    fn default() -> Self {
        Self::new(Arc::new(SystemClock))
    }
}

impl ObservedClock {
    #[must_use]
    pub fn new(source: Arc<dyn Clock>) -> Self {
        Self {
            source,
            floor: AtomicU64::new(0),
        }
    }

    /// Observe wall time while retaining a process-local nondecreasing floor.
    #[must_use]
    pub fn now_millis(&self) -> u64 {
        let observed = self.source.now_millis();
        self.floor
            .fetch_max(observed, Ordering::AcqRel)
            .max(observed)
    }
}

/// Return the Unix wall clock in milliseconds, saturating on conversion
/// failure. Persisted TTLs are always absolute millisecond timestamps.
#[must_use]
pub fn unix_time_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

/// Determine whether an absolute millisecond TTL has expired at `now_millis`.
///
/// At the exact expiration millisecond the value is no longer visible. All
/// persisted visibility paths use this predicate so an expired newest version
/// continues to mask an older value rather than allowing it to reappear.
#[must_use]
pub fn is_expired_at(expiration: Option<u64>, now_millis: u64) -> bool {
    expiration.is_some_and(|expires_at| expires_at <= now_millis)
}

/// Convert an optional relative TTL into its persisted absolute expiration.
#[must_use]
pub fn expiration_from_ttl(ttl_seconds: Option<u64>, commit_time_millis: u64) -> Option<u64> {
    ttl_seconds
        .filter(|ttl| *ttl > 0)
        .map(|ttl| commit_time_millis.saturating_add(ttl.saturating_mul(1_000)))
}
