//! Merge operators for custom value aggregation.
//!
//! Merge operators allow applications to define custom logic for combining multiple
//! values for the same key, enabling efficient patterns like:
//! - Counters (increment without read-modify-write)
//! - Append-only logs (concatenate entries)
//! - JSON document updates (merge partial updates)
//! - Set operations (add/remove elements)
//!
//! # Example
//!
//! ```
//! use midge::{MidgeEngine, MidgeOptions};
//! use midge::merge_operator::IntegerAddOperator;
//! use bytes::Bytes;
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut opts = MidgeOptions::default();
//! let engine = MidgeEngine::open(opts)?;
//!
//! // Register merge operator for a column family
//! engine.register_merge_operator(0, Box::new(IntegerAddOperator))?;
//!
//! // Increment counter without reading
//! engine.merge(Bytes::from("page_views"), Bytes::from("1"))?;
//! engine.merge(Bytes::from("page_views"), Bytes::from("1"))?;
//! engine.merge(Bytes::from("page_views"), Bytes::from("5"))?;
//!
//! // Read combines all increments: 1 + 1 + 5 = 7
//! let count = engine.get(b"page_views")?;
//! # Ok(())
//! # }
//! ```

use crate::error::MidgeError;
use std::sync::Arc;

type Result<T> = std::result::Result<T, MidgeError>;

/// Trait for user-defined merge operators.
///
/// Merge operators define how to combine multiple values for the same key.
/// They must be **associative** to ensure correctness across different
/// compaction orderings.
///
/// # Associativity Requirement
///
/// For any values a, b, c:
/// ```text
/// merge(merge(a, b), c) == merge(a, merge(b, c))
/// ```
///
/// # Thread Safety
///
/// Merge operators must be `Send + Sync` as they're called from
/// background compaction threads and the read path.
pub trait MergeOperator: Send + Sync {
    /// Merge a base value with a delta.
    ///
    /// # Arguments
    ///
    /// * `key` - The key being merged (for context, e.g., type detection)
    /// * `base` - The existing value (None if this is the first value)
    /// * `delta` - The merge operand to apply
    ///
    /// # Returns
    ///
    /// The merged result, or an error if merging fails.
    ///
    /// # Example
    ///
    /// ```
    /// # use midge::merge_operator::MergeOperator;
    /// # use midge::error::MidgeError;
    /// struct CounterOperator;
    ///
    /// impl MergeOperator for CounterOperator {
    ///     fn merge(&self, _key: &[u8], base: Option<&[u8]>, delta: &[u8]) -> Result<Vec<u8>, MidgeError> {
    ///         let base_val = base
    ///             .and_then(|b| std::str::from_utf8(b).ok())
    ///             .and_then(|s| s.parse::<i64>().ok())
    ///             .unwrap_or(0);
    ///         
    ///         let delta_val = std::str::from_utf8(delta)
    ///             .ok()
    ///             .and_then(|s| s.parse::<i64>().ok())
    ///             .ok_or_else(|| MidgeError::InvalidData("Invalid integer".to_string()))?;
    ///         
    ///         Ok((base_val + delta_val).to_string().into_bytes())
    ///     }
    /// }
    /// ```
    fn merge(&self, key: &[u8], base: Option<&[u8]>, delta: &[u8]) -> Result<Vec<u8>>;

    /// Merge multiple deltas at once (optimization for compaction).
    ///
    /// The default implementation calls `merge()` repeatedly, but custom
    /// implementations can optimize for batching.
    ///
    /// # Arguments
    ///
    /// * `key` - The key being merged
    /// * `base` - The existing value (None if this is the first value)
    /// * `deltas` - Slice of merge operands to apply in order
    ///
    /// # Returns
    ///
    /// The final merged result after applying all deltas.
    fn merge_many(&self, key: &[u8], base: Option<&[u8]>, deltas: &[&[u8]]) -> Result<Vec<u8>> {
        let mut result = base.map(|b| b.to_vec());

        for delta in deltas {
            result = Some(self.merge(key, result.as_deref(), delta)?);
        }

        result.ok_or_else(|| MidgeError::InvalidData("No deltas provided to merge".to_string()))
    }

    /// Optional name for debugging/logging.
    fn name(&self) -> &str {
        "custom_merge_operator"
    }
}

/// Type-erased merge operator for storage.
pub type DynMergeOperator = Arc<dyn MergeOperator>;

// =============================================================================
// Built-in Operators
// =============================================================================

/// Integer addition merge operator.
///
/// Treats values as decimal-encoded integers and adds them.
/// Useful for counters, scores, statistics.
///
/// # Format
///
/// Values must be UTF-8 encoded decimal integers (e.g., "42", "-17").
///
/// # Example
///
/// ```
/// use midge::merge_operator::{IntegerAddOperator, MergeOperator};
///
/// let op = IntegerAddOperator;
/// let result = op.merge(b"key", Some(b"10"), b"5").unwrap();
/// assert_eq!(result, b"15");
///
/// let result = op.merge(b"key", None, b"42").unwrap();
/// assert_eq!(result, b"42");
/// ```
#[derive(Debug, Clone, Copy)]
pub struct IntegerAddOperator;

impl MergeOperator for IntegerAddOperator {
    fn merge(&self, _key: &[u8], base: Option<&[u8]>, delta: &[u8]) -> Result<Vec<u8>> {
        let base_val = base
            .and_then(|b| std::str::from_utf8(b).ok())
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);

        let delta_val = std::str::from_utf8(delta)
            .map_err(|_| MidgeError::InvalidData("Delta is not valid UTF-8".to_string()))?
            .parse::<i64>()
            .map_err(|_| MidgeError::InvalidData("Delta is not a valid integer".to_string()))?;

        Ok((base_val + delta_val).to_string().into_bytes())
    }

    fn name(&self) -> &str {
        "integer_add"
    }
}

/// String append merge operator.
///
/// Concatenates string values with an optional delimiter.
/// Useful for logs, event streams, CSV accumulation.
///
/// # Example
///
/// ```
/// use midge::merge_operator::{StringAppendOperator, MergeOperator};
///
/// let op = StringAppendOperator::new(b",");
/// let result = op.merge(b"key", Some(b"hello"), b"world").unwrap();
/// assert_eq!(result, b"hello,world");
///
/// let op = StringAppendOperator::new(b"\n");
/// let result = op.merge(b"key", None, b"first line").unwrap();
/// assert_eq!(result, b"first line");
/// ```
#[derive(Debug, Clone)]
pub struct StringAppendOperator {
    delimiter: Vec<u8>,
}

impl StringAppendOperator {
    /// Create a new append operator with the given delimiter.
    pub fn new(delimiter: &[u8]) -> Self {
        Self {
            delimiter: delimiter.to_vec(),
        }
    }

    /// Create an append operator with no delimiter (direct concatenation).
    pub fn no_delimiter() -> Self {
        Self {
            delimiter: Vec::new(),
        }
    }
}

impl MergeOperator for StringAppendOperator {
    fn merge(&self, _key: &[u8], base: Option<&[u8]>, delta: &[u8]) -> Result<Vec<u8>> {
        match base {
            Some(b) => {
                let mut result = b.to_vec();
                if !self.delimiter.is_empty() {
                    result.extend_from_slice(&self.delimiter);
                }
                result.extend_from_slice(delta);
                Ok(result)
            }
            None => Ok(delta.to_vec()),
        }
    }

    fn merge_many(&self, _key: &[u8], base: Option<&[u8]>, deltas: &[&[u8]]) -> Result<Vec<u8>> {
        let mut result = base.map(|b| b.to_vec()).unwrap_or_default();

        for (i, delta) in deltas.iter().enumerate() {
            if (i > 0 || base.is_some()) && !self.delimiter.is_empty() {
                result.extend_from_slice(&self.delimiter);
            }
            result.extend_from_slice(delta);
        }

        Ok(result)
    }

    fn name(&self) -> &str {
        "string_append"
    }
}

/// Binary append operator (no encoding assumptions).
///
/// Concatenates raw bytes. Useful for binary logs, protocol buffers sequences.
///
/// # Example
///
/// ```
/// use midge::merge_operator::{BytesAppendOperator, MergeOperator};
///
/// let op = BytesAppendOperator;
/// let result = op.merge(b"key", Some(&[1, 2, 3]), &[4, 5, 6]).unwrap();
/// assert_eq!(result, vec![1, 2, 3, 4, 5, 6]);
/// ```
#[derive(Debug, Clone, Copy)]
pub struct BytesAppendOperator;

impl MergeOperator for BytesAppendOperator {
    fn merge(&self, _key: &[u8], base: Option<&[u8]>, delta: &[u8]) -> Result<Vec<u8>> {
        match base {
            Some(b) => {
                let mut result = b.to_vec();
                result.extend_from_slice(delta);
                Ok(result)
            }
            None => Ok(delta.to_vec()),
        }
    }

    fn name(&self) -> &str {
        "bytes_append"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_add_integer_to_none_base_given_integer_add_operator() {
        // Arrange
        let op = IntegerAddOperator;

        // Act
        let result = op.merge(b"key", None, b"42").unwrap();

        // Assert
        assert_eq!(result, b"42");
    }

    #[test]
    fn should_add_integer_to_existing_value_given_integer_add_operator() {
        // Arrange
        let op = IntegerAddOperator;

        // Act
        let result = op.merge(b"key", Some(b"10"), b"5").unwrap();

        // Assert
        assert_eq!(result, b"15");
    }

    #[test]
    fn should_add_negative_integer_given_integer_add_operator() {
        // Arrange
        let op = IntegerAddOperator;

        // Act
        let result = op.merge(b"key", Some(b"100"), b"-30").unwrap();

        // Assert
        assert_eq!(result, b"70");
    }

    #[test]
    fn should_add_multiple_integers_given_merge_many() {
        // Arrange
        let op = IntegerAddOperator;
        let deltas = vec![b"1".as_slice(), b"2".as_slice(), b"3".as_slice()];

        // Act
        let result = op.merge_many(b"key", Some(b"10"), &deltas).unwrap();

        // Assert
        assert_eq!(result, b"16"); // 10 + 1 + 2 + 3
    }

    #[test]
    fn should_append_string_to_none_given_string_append_operator() {
        // Arrange
        let op = StringAppendOperator::new(b",");

        // Act
        let result = op.merge(b"key", None, b"hello").unwrap();

        // Assert
        assert_eq!(result, b"hello");
    }

    #[test]
    fn should_append_string_with_delimiter_given_string_append_operator() {
        // Arrange
        let op = StringAppendOperator::new(b",");

        // Act
        let result = op.merge(b"key", Some(b"hello"), b"world").unwrap();

        // Assert
        assert_eq!(result, b"hello,world");
    }

    #[test]
    fn should_append_multiple_strings_with_delimiter_given_merge_many() {
        // Arrange
        let op = StringAppendOperator::new(b"\n");
        let deltas = vec![b"line2".as_slice(), b"line3".as_slice()];

        // Act
        let result = op.merge_many(b"key", Some(b"line1"), &deltas).unwrap();

        // Assert
        assert_eq!(result, b"line1\nline2\nline3");
    }

    #[test]
    fn should_append_strings_without_delimiter_when_no_delimiter_configured() {
        // Arrange
        let op = StringAppendOperator::no_delimiter();

        // Act
        let result = op.merge(b"key", Some(b"hello"), b"world").unwrap();

        // Assert
        assert_eq!(result, b"helloworld");
    }

    #[test]
    fn should_concatenate_bytes_to_existing_value_given_bytes_append_operator() {
        // Arrange
        let op = BytesAppendOperator;

        // Act
        let result = op.merge(b"key", Some(&[1, 2, 3]), &[4, 5, 6]).unwrap();

        // Assert
        assert_eq!(result, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn should_concatenate_bytes_to_none_given_bytes_append_operator() {
        // Arrange
        let op = BytesAppendOperator;

        // Act
        let result = op.merge(b"key", None, &[1, 2, 3]).unwrap();

        // Assert
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[test]
    fn should_verify_left_associativity_given_integer_add_operator() {
        // Arrange
        let op = IntegerAddOperator;

        // Act
        let left = op.merge(b"key", Some(b"10"), b"20").unwrap();
        let left = op.merge(b"key", Some(&left), b"30").unwrap();

        // Assert
        assert_eq!(left, b"60");
    }

    #[test]
    fn should_verify_right_associativity_matches_left_given_integer_add_operator() {
        // Arrange
        let op = IntegerAddOperator;

        // Act
        let left = op.merge(b"key", Some(b"10"), b"20").unwrap();
        let left_result = op.merge(b"key", Some(&left), b"30").unwrap();
        let right = op.merge(b"key", Some(b"20"), b"30").unwrap();
        let right_result = op.merge(b"key", Some(b"10"), &right).unwrap();

        // Assert
        assert_eq!(left_result, right_result);
        assert_eq!(left_result, b"60");
    }
}
