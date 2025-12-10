//! SST (Sorted String Table) module
//!
//! Provides both in-memory memtables and on-disk SST file implementations.
//!
//! - **memtable**: In-memory skiplist-based key-value store
//! - **encoding**: TLV-based entry encoding for SST files
//! - **types**: SST file format types (blocks, footers, handles)
//! - **traits**: Reader/Writer/Factory contracts for SST implementations
//! - **fs**: Filesystem-backed SST implementation

use crate::common::MidgeResult;
use crate::iterators::SkipList;
use bytes::Bytes;
use std::sync::Arc;

pub mod bloom;
pub mod cache;
pub mod encoding;
pub mod fs;
pub mod sparse_index;
pub mod traits;
pub mod types;

pub use bloom::{BloomFactory, BloomFilterFactory, BloomReader, BloomTestResult, BloomWriter};
pub use cache::{BlockCache, CacheKey, CacheMetrics, CachePolicyType, CacheValue};
pub use fs::FsSstFactory;
pub use sparse_index::{BlockRange, IndexEntry, SparseIndexReader, SparseIndexWriter};
pub use traits::{DynSstWriter, SstFactory, SstReader, SstStateReader, SstWriter};
pub use types::{Block, BlockHandle, BlockType, Footer, KeyState, RangeTombstone, SstEntry};

/// Key-value pair
#[derive(Clone, Debug)]
pub struct KvPair {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub sequence: u64,
}

/// Memtable trait for lock-free concurrent access
pub trait Memtable: Send + Sync {
    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> MidgeResult<()>;
    fn get(&self, key: &[u8]) -> MidgeResult<Option<Vec<u8>>>;
    fn delete(&self, key: Vec<u8>) -> MidgeResult<()>;
    fn size_bytes(&self) -> usize;
}

/// SkipList-based Memtable (lock-free, MVCC-aware)
pub struct SkipListMemtable {
    skiplist: Arc<SkipList>,
    seq_generator: std::sync::atomic::AtomicU64,
    size_bytes: std::sync::atomic::AtomicUsize,
}

impl SkipListMemtable {
    pub fn new() -> Self {
        Self {
            skiplist: Arc::new(SkipList::new()),
            seq_generator: std::sync::atomic::AtomicU64::new(1),
            size_bytes: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn next_seq(&self) -> u64 {
        self.seq_generator
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Iterate over all entries in the memtable
    /// Returns (key, value, sequence) tuples in sorted order
    pub fn iter_all(&self, _max_seq: u64) -> Vec<(Vec<u8>, Option<Vec<u8>>, u64)> {
        self.skiplist
            .drain_with_meta_with_exp()
            .into_iter()
            .map(|(key, value, seq, _, _, _)| (key.to_vec(), value.map(|vb| vb.to_vec()), seq))
            .collect()
    }
}

impl Default for SkipListMemtable {
    fn default() -> Self {
        Self::new()
    }
}

impl Memtable for SkipListMemtable {
    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> MidgeResult<()> {
        let seq = self.next_seq();
        let size_delta = key.len() + value.len() + 16;
        self.skiplist
            .upsert(Bytes::from(key), Some(Bytes::from(value)), seq);
        self.size_bytes
            .fetch_add(size_delta, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    fn get(&self, key: &[u8]) -> MidgeResult<Option<Vec<u8>>> {
        Ok(self.skiplist.get(key, u64::MAX).map(|b| b.to_vec()))
    }

    fn delete(&self, key: Vec<u8>) -> MidgeResult<()> {
        let seq = self.next_seq();
        let size_delta = key.len() + 16;
        self.skiplist.delete(Bytes::from(key), seq);
        self.size_bytes
            .fetch_add(size_delta, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    fn size_bytes(&self) -> usize {
        self.size_bytes.load(std::sync::atomic::Ordering::Relaxed)
    }
}
