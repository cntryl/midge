//! Utility data structures used throughout the engine.
//!
//! This module contains general-purpose data structures that support
//! LSM-tree operations and comparisons.

pub mod merge_iterator;
pub mod skiplist;

pub use merge_iterator::MergingIterator;
pub use skiplist::SkipList;
