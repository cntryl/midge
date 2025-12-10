//! Cached block value with metadata

use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

/// A cached block value with metadata
#[derive(Clone, Debug)]
pub struct CacheValue {
    /// The actual block data
    pub data: Arc<Bytes>,
    /// Insertion timestamp (nanoseconds since epoch)
    pub inserted_at: u64,
    /// Access count for frequency tracking
    pub access_count: Arc<AtomicU64>,
}

impl CacheValue {
    /// Create a new cached value
    pub fn new(data: Bytes) -> Self {
        Self {
            data: Arc::new(data),
            inserted_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64,
            access_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Get the size of the cached data in bytes
    pub fn size_bytes(&self) -> usize {
        self.data.len()
    }

    /// Increment access count and return the new value
    pub fn increment_access(&self) -> u64 {
        self.access_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1
    }

    /// Get current access count
    pub fn access_count(&self) -> u64 {
        self.access_count.load(std::sync::atomic::Ordering::Relaxed)
    }
}

