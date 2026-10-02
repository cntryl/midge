//! Latest-intent index over a transaction's resident operations.
//!
//! The resident vector keeps arrival order for spilling and replay; lookups go
//! through this index instead. Point intents keep only the newest operation per
//! key. Range deletes are painted onto a disjoint-interval map: ordinals only
//! grow, so a new range always supersedes older ones over the keys it covers,
//! and every stored interval carries the newest ordinal covering it. Both
//! structures answer a lookup in `O(log n)`.
//!
//! Memory per resident operation is one point entry, or at most two interval
//! entries for a range delete, each a few `Bytes` handles that share the
//! operation's key allocations. `INTENT_ACCOUNTING_OVERHEAD` covers them.

use super::{IntentLookup, OrdinalOp, TransactionOp};
use bytes::Bytes;
use std::collections::BTreeMap;
use std::ops::Bound;

#[derive(Debug, Default)]
pub(super) struct ResidentIndex {
    points: BTreeMap<Bytes, (u64, IntentLookup)>,
    /// Disjoint `[start, end)` intervals keyed by start, each with the newest
    /// range-delete ordinal covering it.
    ranges: BTreeMap<Bytes, (Bytes, u64)>,
}

impl ResidentIndex {
    pub(super) fn clear(&mut self) {
        self.points.clear();
        self.ranges.clear();
    }

    /// Records an operation whose ordinal is newer than every recorded one.
    pub(super) fn record(&mut self, ordinal_op: &OrdinalOp) {
        match &ordinal_op.op {
            TransactionOp::Put { key, value, .. } => {
                self.points.insert(
                    key.clone(),
                    (ordinal_op.ordinal, IntentLookup::Present(value.clone())),
                );
            }
            TransactionOp::Delete { key, .. } => {
                self.points
                    .insert(key.clone(), (ordinal_op.ordinal, IntentLookup::Deleted));
            }
            TransactionOp::DeleteRange {
                start_key, end_key, ..
            } => self.paint(start_key, end_key, ordinal_op.ordinal),
        }
    }

    fn paint(&mut self, start: &Bytes, end: &Bytes, ordinal: u64) {
        if start >= end {
            return;
        }
        // Split the interval straddling `start`, then the one straddling
        // `end`, so the painted span is covered by whole intervals only.
        self.split_at(start);
        self.split_at(end);
        let covered = self
            .ranges
            .range::<[u8], _>((
                Bound::Included(start.as_ref()),
                Bound::Excluded(end.as_ref()),
            ))
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in covered {
            self.ranges.remove(&key);
        }
        self.ranges.insert(start.clone(), (end.clone(), ordinal));
    }

    /// Ensures no interval straddles `at`.
    fn split_at(&mut self, at: &Bytes) {
        let Some((start, (end, ordinal))) = self
            .ranges
            .range::<[u8], _>((Bound::Unbounded, Bound::Included(at.as_ref())))
            .next_back()
            .map(|(start, value)| (start.clone(), value.clone()))
        else {
            return;
        };
        if start == *at || end <= *at {
            return;
        }
        self.ranges.insert(start, (at.clone(), ordinal));
        self.ranges.insert(at.clone(), (end, ordinal));
    }

    /// Newest resident point intent for `key`, ignoring range deletes.
    pub(super) fn point(&self, key: &[u8]) -> Option<(u64, IntentLookup)> {
        self.points.get(key).cloned()
    }

    /// Newest resident range-delete ordinal covering `key`.
    pub(super) fn covering_range(&self, key: &[u8]) -> Option<u64> {
        let (_, (end, ordinal)) = self
            .ranges
            .range::<[u8], _>((Bound::Unbounded, Bound::Included(key)))
            .next_back()?;
        (key < end.as_ref()).then_some(*ordinal)
    }

    /// Newest resident intent for `key`, whether a point write or a covering
    /// range delete.
    pub(super) fn latest(&self, key: &[u8]) -> Option<(u64, IntentLookup)> {
        let point = self.point(key);
        match (point, self.covering_range(key)) {
            (Some((ordinal, _)), Some(range)) if range > ordinal => {
                Some((range, IntentLookup::Deleted))
            }
            (None, Some(range)) => Some((range, IntentLookup::Deleted)),
            (point, _) => point,
        }
    }

    /// Point intents with keys inside `[start, end)`, ascending.
    pub(super) fn points_in(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Vec<(Bytes, (u64, IntentLookup))> {
        if let (Some(start), Some(end)) = (start, end) {
            if start >= end {
                return Vec::new();
            }
        }
        let lower = start.map_or(Bound::Unbounded, Bound::Included);
        let upper = end.map_or(Bound::Unbounded, Bound::Excluded);
        self.points
            .range::<[u8], _>((lower, upper))
            .map(|(key, point)| (key.clone(), point.clone()))
            .collect()
    }
}
