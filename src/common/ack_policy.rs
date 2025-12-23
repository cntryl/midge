/// Policy that defines when a write is acknowledged to the caller.
///
/// This is intentionally separate from WAL durability mechanics.
///
/// - **Acknowledgment** answers: "When does `put()` return?"
/// - **Durability** answers: "When is the write guaranteed durable?"
///
/// The runtime/WAL may achieve durability using batching, fsync, or cloud replication,
/// but those mechanisms must not implicitly redefine caller-visible acknowledgment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckPolicy {
    /// Return once the write is accepted into the engine (e.g., queued/accepted).
    Immediate,

    /// Return only after local durability is guaranteed.
    ///
    /// In practice this commonly means "group commit" semantics.
    AfterLocalDurable,

    /// Return only after cloud durability is guaranteed.
    AfterCloudDurable,
}

impl Default for AckPolicy {
    fn default() -> Self {
        // Preserve current behavior: writes are acknowledged after the runtime acks
        // the WAL append (which may be gated by local durability/group commit).
        Self::AfterLocalDurable
    }
}
