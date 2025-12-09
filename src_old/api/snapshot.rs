use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::sync::{Arc, Weak};

/// A read-only view identified by a sequence number.
///
/// Snapshots register themselves with a registry so that background tasks can
/// discover the oldest outstanding snapshot sequence (used for safe tombstone
/// garbage collection). Dropping a snapshot automatically deregisters it.
#[derive(Debug)]
pub struct Snapshot {
    pub seq: u64,
    registry: Weak<SnapshotRegistry>,
    id: u64,
}

impl Snapshot {
    pub(crate) fn new(seq: u64, registry: &Arc<SnapshotRegistry>, id: u64) -> Self {
        Self {
            seq,
            registry: Arc::downgrade(registry),
            id,
        }
    }

    /// Convenience helper to read a key at this snapshot.
    ///
    /// This calls `engine.get_at(cf, key, snapshot)` for callers that already
    /// hold a snapshot and prefer a succinct way to read from it.
    ///
    /// Returns the value visible at the snapshot (respecting MVCC semantics).
    pub fn get(
        &self,
        engine: &crate::MidgeEngine,
        cf: &crate::api::column_family::ColumnFamilyHandle,
        key: &[u8],
    ) -> crate::MidgeResult<Option<bytes::Bytes>> {
        engine.get_at(cf, key, self)
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        if let Some(reg) = self.registry.upgrade() {
            reg.unregister(self.id);
            // Record snapshot release in metrics if available
            if let Some(metrics) = &reg.metrics {
                metrics.snapshot_released();
            }
        }
    }
}

#[derive(Default)]
pub struct SnapshotRegistry {
    inner: Mutex<SnapshotRegistryState>,
    pub(crate) metrics: Option<Arc<crate::metrics::Metrics>>,
}

#[derive(Default)]
struct SnapshotRegistryState {
    next_id: u64,
    active: BTreeMap<u64, u64>,
}

impl SnapshotRegistry {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(SnapshotRegistryState::default()),
            metrics: None,
        }
    }

    pub fn with_metrics(metrics: Arc<crate::metrics::Metrics>) -> Self {
        Self {
            inner: Mutex::new(SnapshotRegistryState::default()),
            metrics: Some(metrics),
        }
    }

    #[inline]
    pub fn register(self: &Arc<Self>, seq: u64) -> Snapshot {
        let mut inner = self.inner.lock();
        let id = inner.next_id;
        inner.next_id = inner.next_id.wrapping_add(1);
        inner.active.insert(id, seq);
        Snapshot::new(seq, self, id)
    }

    /// Returns the minimum active snapshot sequence number.
    /// Used for tombstone GC safety during compaction - tombstones
    /// should not be dropped if they're visible to any active snapshot.
    #[inline]
    pub fn min_active_seq(&self) -> Option<u64> {
        let inner = self.inner.lock();
        inner.active.values().copied().min()
    }

    #[inline]
    fn unregister(&self, id: u64) {
        let mut inner = self.inner.lock();
        inner.active.remove(&id);
    }
}
