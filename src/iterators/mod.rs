//! Iterator abstraction
//!
//! Generic iterator traits for traversing key-value data

pub mod skiplist;

pub use skiplist::SkipList;

use crate::common::MidgeResult;
use crate::sst::KvPair;

/// Forward iterator trait
pub trait Iterator: Send + Sync {
    /// Advance to the next key-value pair.
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying iterator cannot produce the next item.
    fn next(&mut self) -> MidgeResult<Option<KvPair>>;

    /// Reposition the iterator to `key` or the next greater key.
    ///
    /// # Errors
    ///
    /// Returns an error when the iterator cannot seek to the requested key.
    fn seek(&mut self, key: &[u8]) -> MidgeResult<()>;
}

/// Reverse iterator trait
pub trait ReverseIterator: Send + Sync {
    /// Move to the previous key-value pair.
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying iterator cannot produce the previous item.
    fn prev(&mut self) -> MidgeResult<Option<KvPair>>;

    /// Reposition the iterator to the last key-value pair.
    ///
    /// # Errors
    ///
    /// Returns an error when the iterator cannot seek to the final item.
    fn seek_to_last(&mut self) -> MidgeResult<()>;
}
