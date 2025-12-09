//! SST (Sorted String Table) abstraction
//!
//! Includes memtables, immutable SSTs, bloom filters, indexes

pub mod mutable;
pub mod immutable;
pub mod cache;
pub mod bloom;
pub mod trie;
pub mod sparse_index;

use crate::common::MidgeResult;
use std::sync::Arc;
use crate::iterators::SkipList;
use bytes::Bytes;

/// Key-value pair
#[derive(Clone, Debug)]
pub struct KvPair {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub sequence: u64,
}

/// Memtable trait
pub trait Memtable: Send + Sync {
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> MidgeResult<()>;
    fn get(&self, key: &[u8]) -> MidgeResult<Option<Vec<u8>>>;
    fn delete(&mut self, key: Vec<u8>) -> MidgeResult<()>;
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
}

impl Default for SkipListMemtable {
    fn default() -> Self {
        Self::new()
    }
}

impl Memtable for SkipListMemtable {
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> MidgeResult<()> {
        let seq = self.next_seq();
        let size_delta = key.len() + value.len() + 16;
        self.skiplist.upsert(Bytes::from(key), Some(Bytes::from(value)), seq);
        self.size_bytes.fetch_add(size_delta, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    fn get(&self, key: &[u8]) -> MidgeResult<Option<Vec<u8>>> {
        Ok(self.skiplist.get(key, u64::MAX).map(|b| b.to_vec()))
    }

    fn delete(&mut self, key: Vec<u8>) -> MidgeResult<()> {
        let seq = self.next_seq();
        let size_delta = key.len() + 16;
        self.skiplist.delete(Bytes::from(key), seq);
        self.size_bytes.fetch_add(size_delta, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    fn size_bytes(&self) -> usize {
        self.size_bytes.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Immutable SST reader
pub trait SstReader: Send + Sync {
    fn get(&self, key: &[u8]) -> MidgeResult<Option<Vec<u8>>>;
    fn range(&self, start: &[u8], end: &[u8]) -> MidgeResult<Vec<KvPair>>;
}

/// Immutable SST writer
pub trait SstWriter: Send + Sync {
    fn add(&mut self, pair: KvPair) -> MidgeResult<()>;
    fn finish(&mut self) -> MidgeResult<()>;
}
