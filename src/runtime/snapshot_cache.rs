//! Lock-free snapshot cache for read-path bypass
//!
//! Allows `begin_tx` to capture a read snapshot without sending a message
//! through the event loop. The event loop publishes updated snapshots after
//! each write/flush/rotation, and readers load them atomically via `ArcSwap`.
//!
//! # Design
//!
//! - The event loop is the sole writer (calls `publish`)
//! - Engine threads are concurrent readers (call `load_snapshot`)
//! - `ArcSwap` provides wait-free reads and lock-free writes
//! - Snapshot includes sequence number + per-CF memtable/SST refs
//! - Consistency: sequence and memtable refs are atomically swapped together

use crate::runtime::read_snapshot::ReadSnapshot;
use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::sync::Arc;

/// Per-CF snapshot data published by the event loop.
#[derive(Debug, Clone)]
pub(crate) struct CfSnapshotData {
    pub snapshot: Arc<ReadSnapshot>,
}

/// Atomically published snapshot state for all column families.
///
/// Swapped as a single unit to ensure cross-CF consistency
/// (e.g., sequence number matches memtable state).
#[derive(Debug, Clone)]
pub(crate) struct PublishedSnapshot {
    /// Current global sequence number at publish time.
    pub sequence: u64,
    /// Per-CF snapshots keyed by CF ID.
    pub cf_snapshots: HashMap<u32, CfSnapshotData>,
}

/// Lock-free snapshot cache shared between event loop and engine threads.
///
/// Event loop calls `publish()` to atomically update the snapshot.
/// Engine threads call `load()` to get the latest snapshot without
/// any message passing or lock contention.
pub(crate) struct SnapshotCache {
    inner: ArcSwap<PublishedSnapshot>,
}

impl SnapshotCache {
    /// Create a new empty snapshot cache.
    pub fn new() -> Self {
        Self {
            inner: ArcSwap::from_pointee(PublishedSnapshot {
                sequence: 0,
                cf_snapshots: HashMap::new(),
            }),
        }
    }

    /// Atomically publish a new snapshot (called by event loop only).
    pub fn publish(&self, snapshot: PublishedSnapshot) {
        self.inner.store(Arc::new(snapshot));
    }

    /// Load the current snapshot (wait-free, called by any thread).
    pub fn load(&self) -> arc_swap::Guard<Arc<PublishedSnapshot>> {
        self.inner.load()
    }
}

impl std::fmt::Debug for SnapshotCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotCache").finish()
    }
}
