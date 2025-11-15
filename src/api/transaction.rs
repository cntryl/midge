/// Transaction-level isolation modes for engine transactions.
///
/// This enum allows tests and callers to request a different isolation level
/// when beginning a transaction. By default transactions use `Snapshot`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum IsolationLevel {
    /// Snapshot isolation - transaction reads see the database as of begin sequence.
    Snapshot,

    /// Read committed - transaction reads see the latest committed value; reads
    /// are not tracked for conflict detection (weaker isolation, less conflict).
    ReadCommitted,
}

impl Default for IsolationLevel {
    fn default() -> Self {
        IsolationLevel::Snapshot
    }
}
