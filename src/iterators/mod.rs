//! Iterator abstraction
//!
//! Generic iterator traits for traversing key-value data

pub mod skiplist;
pub mod merge;

pub use skiplist::SkipList;
pub use merge::{MergeIterator, SourceIterator};

use crate::sst::KvPair;
use crate::common::MidgeResult;

/// Forward iterator trait
pub trait Iterator: Send + Sync {
    fn next(&mut self) -> MidgeResult<Option<KvPair>>;
    fn seek(&mut self, key: &[u8]) -> MidgeResult<()>;
}

/// Reverse iterator trait
pub trait ReverseIterator: Send + Sync {
    fn prev(&mut self) -> MidgeResult<Option<KvPair>>;
    fn seek_to_last(&mut self) -> MidgeResult<()>;
}
