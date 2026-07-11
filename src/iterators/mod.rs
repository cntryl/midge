//! Iterator abstraction
//!
//! Generic iterator traits for traversing key-value data

pub mod skiplist {
    pub use crate::memtable::skiplist::*;
}

pub use skiplist::SkipList;

use crate::common::MidgeResult;
pub use crate::types::KvPair;

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
