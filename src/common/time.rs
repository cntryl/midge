//! Shared wall-clock helpers for persisted timestamps.

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
