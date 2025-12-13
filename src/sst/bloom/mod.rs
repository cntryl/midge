//! Bloom filter module for fast negative lookups in SST files
//!
//! A bloom filter is a space-efficient probabilistic data structure that can quickly determine
//! if a key is definitely not in the SST (with no false negatives) or might be in it (with a
//! configurable false positive rate, typically 1%).
//!
//! This implementation uses double hashing to reduce false positives and supports both
//! serialization for storage in SST footers and in-memory queries.
//!
//! ## Two-Tier Bloom Architecture
//!
//! Midge uses a two-tier bloom filtering strategy:
//! - **SST-level bloom** (coarse gate): One filter per SST, eliminates entire SSTs
//! - **Block-level bloom** (fine gate): One filter per data block, eliminates useless block reads

pub mod block_bloom;
pub mod factory;
pub mod reader;
pub mod writer;

pub use block_bloom::BlockBloomFilter;
pub use factory::{BloomFactory, BloomFilterFactory};
pub use reader::BloomReader;
pub use writer::{BloomTestResult, BloomWriter};
