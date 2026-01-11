#![allow(dead_code)]

//! Iterator abstraction
//!
//! Generic iterator traits for traversing key-value data

pub mod merge;
pub mod skiplist;

pub use skiplist::SkipList;

use crate::common::MidgeResult;
use crate::sst::KvPair;

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
