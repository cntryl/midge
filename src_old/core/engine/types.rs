//! Result types for engine operations.

use bytes::Bytes;

/// Result of an insert-if-not-exists operation.
///
/// Returned by [`MidgeEngine::insert_with_value`] to indicate whether the key
/// was newly inserted or already existed, along with the existing value if applicable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InsertResult {
    /// The key did not exist and was successfully inserted.
    Inserted,
    /// The key already existed. Returns the existing value.
    AlreadyExists(Bytes),
}

/// Result of a compare-and-swap operation.
///
/// Returned by [`MidgeEngine::compare_and_swap`] to indicate whether the swap
/// succeeded or failed due to a mismatch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CasResult {
    /// The swap succeeded: the old value matched and the new value was written.
    Swapped,
    /// The swap failed: the current value did not match expected. Returns the actual current value.
    Mismatch(Option<Bytes>),
}
