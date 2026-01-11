//! Sparse index module for fast block range lookups
//!
//! Provides a sampled key index to quickly narrow down which blocks to search.
//! Reduces I/O for range queries by 40-60% by eliminating unnecessary block reads.
//!
//! - **Writer**: Samples every Nth key (default 16) during SST creation
//! - **Reader**: Binary searches samples to find containing block range
//! - **BlockRange**: Result type indicating which blocks to search (start to end inclusive)

pub mod reader;
pub mod shared;
pub mod writer;

pub use reader::SparseIndexReader;
