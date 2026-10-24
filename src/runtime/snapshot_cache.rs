//! Per-family snapshot publication. The event loop is the sole publisher.
//!
//! A family slot is stored before the global applied cut is released. A reader
//! first acquires that cut, then the family slot. An untouched family's sources
//! remain valid at the newer cut; a slot ahead of the cut is already fully
//! applied and carries its own sequence. No reader uses an allocated-only cut.

use crate::runtime::read_snapshot::ReadSnapshot;
use arc_swap::ArcSwap;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

struct FamilySnapshot {
    sequence: u64,
    snapshot: Arc<ReadSnapshot>,
}

pub(crate) struct SnapshotCache {
    families: DashMap<u32, ArcSwap<FamilySnapshot>>,
    applied_sequence: AtomicU64,
}

impl SnapshotCache {
    pub fn new() -> Self {
        Self {
            families: DashMap::new(),
            applied_sequence: AtomicU64::new(0),
        }
    }

    pub fn publish_family(&self, cf_id: u32, sequence: u64, snapshot: Arc<ReadSnapshot>) {
        let published = Arc::new(FamilySnapshot { sequence, snapshot });
        match self.families.entry(cf_id) {
            dashmap::mapref::entry::Entry::Occupied(slot) => slot.get().store(published),
            dashmap::mapref::entry::Entry::Vacant(slot) => {
                slot.insert(ArcSwap::from(published));
            }
        }
    }

    pub fn finish_publication(&self, sequence: u64) {
        self.applied_sequence.store(sequence, Ordering::Release);
    }

    pub fn retain_families(&self, is_live: impl Fn(u32) -> bool) {
        self.families.retain(|cf_id, _| is_live(*cf_id));
    }

    pub fn load_family(&self, cf_id: u32) -> Option<(u64, Arc<ReadSnapshot>)> {
        let applied = self.applied_sequence.load(Ordering::Acquire);
        let slot = self.families.get(&cf_id)?;
        let published = slot.load();
        Some((
            applied.max(published.sequence),
            Arc::clone(&published.snapshot),
        ))
    }
}

impl std::fmt::Debug for SnapshotCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotCache").finish_non_exhaustive()
    }
}
