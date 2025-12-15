//! Merge operator trait and implementations
//!
//! Merge operators enable efficient read-modify-write patterns by allowing
//! operations to be applied lazily during reads or compaction.

use crate::common::MidgeResult;

/// Trait for merge operators
///
/// A merge operator defines how to combine multiple operands with an optional
/// base value. Merge operators must be associative for correctness.
pub trait MergeOperator: Send + Sync + std::fmt::Debug {
    /// Merge operands with an optional existing value
    ///
    /// # Arguments
    /// * `key` - The key being merged (for context)
    /// * `existing_value` - The current value if it exists (None for new keys or after delete)
    /// * `operands` - List of merge operands to apply
    ///
    /// # Returns
    /// The merged result, or None to delete the key
    fn merge(
        &self,
        key: &[u8],
        existing_value: Option<&[u8]>,
        operands: &[Vec<u8>],
    ) -> MidgeResult<Option<Vec<u8>>>;

    /// Name of this merge operator (for debugging/diagnostics)
    fn name(&self) -> &str;
}
