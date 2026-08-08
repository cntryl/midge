//! Bloom filter module for fast negative lookups in SST files
//!
//! A bloom filter is a space-efficient probabilistic data structure that can quickly determine
//! if a key is definitely not in the SST (with no false negatives) or might be in it (with a
//! configurable false positive rate, typically 1%).
//!
//! This implementation uses double hashing to reduce false positives and supports both
//! serialization for storage in SST footers and in-memory queries.
//!
//! ## Persisted Block Bloom Architecture
//!
//! Midge persists one bloom per data block and checks it after candidate-block
//! selection. Persisted key-range metadata provides the coarse SST-level gate.

pub mod block_bloom;
pub mod metrics;
pub mod reader;
pub mod writer;

pub use block_bloom::BlockBloomFilter;
pub use metrics::BloomMetrics;
pub use reader::BloomReader;
pub use writer::BloomWriter;
