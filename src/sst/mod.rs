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

/// Temporary factory that returns a stub cache until new impl is ready.
pub fn create_basic_cache(_max_size_bytes: usize) -> std::sync::Arc<dyn BlockCacheTrait> {
    std::sync::Arc::new(StubBlockCache)
}

/// Minimal stub so engine code compiles; always misses.
struct StubBlockCache;

impl BlockCacheTrait for StubBlockCache {
    fn get(&self, _key: &block_cache::BlockKey) -> Option<block_cache::BlockHandle> {
        None
    }
    fn insert(&self, _key: block_cache::BlockKey, data: block_cache::BlockData) -> block_cache::BlockHandle {
        block_cache::BlockHandle::unpinned(std::sync::Arc::new(data))
    }
    fn insert_if_absent(&self, key: block_cache::BlockKey, data: block_cache::BlockData) -> block_cache::BlockHandle {
        self.insert(key, data)
    }
    fn capacity_bytes(&self) -> usize { 0 }
    fn used_bytes(&self) -> usize { 0 }
    fn stats(&self) -> block_cache::BlockCacheStats { block_cache::BlockCacheStats::default() }
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
