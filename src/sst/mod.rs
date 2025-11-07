//! Top-level SST module
//!
//! This module contains all SST (Sorted String Table) related functionality:
//!
//! - **Format**: SST file format (blocks, footer, high-level structure)
//! - **Encoding**: TLV entry encoding/decoding (symmetric operations)
//! - **Traits**: Generic reader/writer contracts
//! - **Implementations**: Filesystem (`fs`), in-memory (`mem`), and cloud-backend (`cloud`) backends
//! - **Bloom Filters**: Fast key presence checks
//! - **Sparse Index**: Block-level index for efficient lookups
//! - **Cache**: Block-level and table-level LRU caching
//! - **Cloud**: Cloud storage backend and lifecycle management
//! - **File Manager**: File lifecycle and quota management

pub mod block_cache;
pub mod bloom;
pub mod bloom_cache;
pub mod cache;
pub mod cloud;
pub mod encoding;
pub mod file_manager;
pub mod format;
pub mod fs;
pub mod manifest_cache;
pub mod mem;
pub mod meta_index;
pub mod metadata_cache;
pub mod range_tombstone;
pub mod reader_common;
pub mod sparse_index;
pub mod sparse_index_cache;
pub mod table_cache;
pub mod traits;
pub mod writer_common;

pub use block_cache::{
    AdaptiveBlockCache, AdaptiveCacheStats, BlockCache, BlockKey, BlockType as CacheBlockType,
    CacheStats, CachedBlock, ShardedBlockCache,
};
pub use bloom::{BloomFilter, BloomFilterBuilder, Filter};
pub use cloud::{
    ArchiveTier, CloudSst, CloudSstFactory, CloudSstManager, CloudSstManagerConfig,
    CloudSstReaderFactory, SstCloudReader, SstCloudWriter, SstLifecycleState, SstMetadata,
    SstUploadMeta,
};
pub use file_manager::FileManager;
pub use format::{Block, BlockHandle, BlockType, DataBlockBuilder, Footer, IndexBlockBuilder};
pub use sparse_index::{IndexEntry, SparseIndex, SparseIndexBuilder};
pub use table_cache::{CachedTable, TableCache, TableCacheStats};
pub use traits::*;

pub use manifest_cache::ManifestCache;

pub use bloom_cache::BloomCache;
pub use cache::SstCache;
pub use metadata_cache::SstMetadataCache;
pub use sparse_index_cache::SparseIndexCache;

pub use fs::*;
pub use mem::*;
