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

// ─── Block cache re-exports (temporary shims for existing code) ──────────────
// These re-export names from the new block_cache module so call-sites like
// `crate::sst::BlockCacheTrait` keep compiling while we finish the new impl.
pub use block_cache::{BlockCache as BlockCacheTrait, BlockCacheStats as CacheStats};
pub use block_cache::{
    BlockCacheOptions, BlockData, BlockHandle as CacheBlockHandle, BlockKey, BlockKind,
    CfCacheStats, EvictionPolicy, ShardedBlockCache, SizeAccounting,
};

/// Create a block cache with the specified capacity.
///
/// Uses the new `ShardedBlockCache` implementation with WTinyLFU eviction
/// policy for scan resistance and high hit rates.
pub fn create_basic_cache(max_size_bytes: usize) -> std::sync::Arc<dyn BlockCacheTrait> {
    std::sync::Arc::new(ShardedBlockCache::new(
        BlockCacheOptions::with_capacity(max_size_bytes),
    ))
}

/// Create a block cache with full configuration options.
///
/// Allows customizing shards, eviction policy, size accounting, and per-CF stats.
///
/// # Example
/// ```ignore
/// use midge::sst::{create_cache_with_options, BlockCacheOptions, EvictionPolicy};
///
/// let cache = create_cache_with_options(
///     BlockCacheOptions::with_capacity(128 * 1024 * 1024)
///         .num_shards(32)
///         .eviction_policy(EvictionPolicy::WTinyLfu)
///         .per_cf_stats(true)
/// );
/// ```
pub fn create_cache_with_options(options: BlockCacheOptions) -> std::sync::Arc<ShardedBlockCache> {
    std::sync::Arc::new(ShardedBlockCache::new(options))
}

// ─── Other public re-exports ─────────────────────────────────────────────────
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
