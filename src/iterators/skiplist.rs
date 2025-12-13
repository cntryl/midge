//! Lock-Free Multi-Version Skiplist
//!
//! - Fully lock-free inserts (writers use CAS, readers are wait-free under epoch guards).
//! - Uses crossbeam-epoch for safe concurrent memory reclamation.
//! - Supports MVCC-style visibility via sequence numbers (LSM snapshot semantics).
//! - Designed for LSM memtable use: no physical deletion, tombstones only.
//!
//! Snapshot rule (LSM-style):
//!   - A version is visible to a snapshot if `vn.seq < snapshot_seq`.

use bytes::Bytes;
use crossbeam_epoch::{self as epoch, Atomic, Guard, Owned, Shared};
use std::cmp::Ordering;
use std::num::Wrapping;
use std::sync::atomic::{AtomicUsize, Ordering as AO};
use std::sync::Arc;

/// Maximum height of the skiplist towers.
///
/// Higher levels give shorter search paths while remaining cheap to maintain.
/// 20 is a common choice in production LSM engines (e.g., similar to Pebble).
const MAX_LEVEL: usize = 20;

/// Entry metadata tuple: (key, value_opt, sequence, is_tombstone)
pub type SkipListEntry = (Bytes, Option<Bytes>, u64, bool);

/// Extended entry metadata including optional expiration (Unix millis) and op type.
pub type SkipListEntryWithExp = (Bytes, Option<Bytes>, u64, bool, Option<u64>, OpType);

/// Operation type for a skiplist version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpType {
    Put,
    Merge,
    Delete,
}

impl OpType {
    /// Convert OpType to u8 for SST encoding (0=Put, 1=Insert, 2=Delete, 3=Merge)
    pub fn as_u8(&self) -> u8 {
        match self {
            OpType::Put => 0,
            OpType::Delete => 2,
            OpType::Merge => 3,
        }
    }
}

/// A version node in the lock-free version chain (newest-first).
#[derive(Debug)]
struct VersionNode {
    seq: u64,
    val: Option<Bytes>,
    exp: Option<u64>,
    op: OpType,
    next: Atomic<VersionNode>,
}

impl VersionNode {
    fn new(seq: u64, val: Option<Bytes>, exp: Option<u64>, op: OpType) -> Self {
        VersionNode {
            seq,
            val,
            exp,
            op,
            next: Atomic::null(),
        }
    }
}

/// A node in the lock-free skiplist.
///
/// Each node holds a key and a newest-first version chain. Forward pointers
/// form the skiplist tower at each level.
#[derive(Debug)]
struct Node {
    key: Bytes,
    /// Head of newest-first version chain.
    versions_head: Atomic<VersionNode>,
    /// Forward pointers per level (levels 0..level-1 are valid).
    forward: [Atomic<Node>; MAX_LEVEL],
    #[allow(dead_code)]
    level: usize,
}

impl Node {
    fn new(key: Bytes, first_version: Owned<VersionNode>, level: usize) -> Self {
        debug_assert!((1..=MAX_LEVEL).contains(&level));

        let forward: [Atomic<Node>; MAX_LEVEL] = std::array::from_fn(|_| Atomic::null());

        Node {
            key,
            versions_head: Atomic::from(first_version),
            forward,
            level,
        }
    }

    fn sentinel() -> Self {
        let empty_version = Owned::new(VersionNode::new(0, None, None, OpType::Put));
        // Sentinel uses full height so we always have a tower root.
        Node::new(Bytes::new(), empty_version, MAX_LEVEL)
    }
}

/// Lock-free skiplist with multi-version concurrency control.
///
/// All concurrent memory accesses rely on crossbeam-epoch. We never physically
/// delete nodes in the memtable (to match LSM design); deletions are represented
/// as tombstone versions.
pub struct SkipList {
    head: Arc<Node>,
    /// Highest level currently in use (1-based).
    top_level: AtomicUsize,
}

impl SkipList {
    /// Create a new lock-free skiplist.
    pub fn new() -> Self {
        let head = Arc::new(Node::sentinel());
        SkipList {
            head,
            top_level: AtomicUsize::new(1),
        }
    }

    /// Generate a random level using a thread-local RNG.
    ///
    /// Uses an xorshift64* PRNG with approximately p=1/2 probability of
    /// advancing each level. This keeps towers tall enough for efficient
    /// search while staying cheap to maintain.
    #[inline]
    fn random_level() -> usize {
        use std::cell::RefCell;

        thread_local! {
            static RNG: RefCell<Wrapping<u64>> =
                const { RefCell::new(Wrapping(0x9E37_79B9_7F4A_7C15)) };
        }

        RNG.with(|rng| {
            let mut lvl = 1;
            let mut state = rng.borrow_mut();

            // xorshift64*
            let mut x = state.0;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            *state = Wrapping(x);
            let mut rand = x.wrapping_mul(0x2545_F491_4F6C_DD1D);

            // p ≈ 1/2: bump level while low bit is 0
            while lvl < MAX_LEVEL && (rand & 1) == 0 {
                lvl += 1;
                rand >>= 1;
            }

            lvl
        })
    }

    #[inline]
    fn cmp_key(a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }

    /// Find predecessors and successors at all levels for the given key.
    ///
    /// Fills `preds` and `succs` arrays such that, at each level `l`,
    ///   preds[l].key < key <= succs[l].key   (when they exist).
    fn find<'g>(
        &self,
        key: &[u8],
        guard: &'g Guard,
        preds: &mut [Shared<'g, Node>; MAX_LEVEL],
        succs: &mut [Shared<'g, Node>; MAX_LEVEL],
    ) {
        // Start from head sentinel.
        let mut pred: Shared<'g, Node> = Shared::from(&*self.head as *const Node);

        // Approximate top level is enough for search.
        let mut level = self.top_level.load(AO::Relaxed);

        while level > 0 {
            let l = level - 1;

            let pred_ref = unsafe { pred.deref() };
            let mut curr = pred_ref.forward[l].load(AO::Acquire, guard);

            // Advance while current.key < target.
            while let Some(curr_ref) = unsafe { curr.as_ref() } {
                match Self::cmp_key(&curr_ref.key, key) {
                    Ordering::Less => {
                        pred = curr;
                        curr = curr_ref.forward[l].load(AO::Acquire, guard);
                    }
                    _ => break,
                }
            }

            preds[l] = pred;
            succs[l] = curr;
            level -= 1;
        }
    }

    /// Point search: find node for key, or null if absent.
    #[inline]
    fn find_node<'g>(&self, key: &[u8], guard: &'g Guard) -> Shared<'g, Node> {
        let mut pred: Shared<'g, Node> = Shared::from(&*self.head as *const Node);
        let mut level = self.top_level.load(AO::Relaxed);

        while level > 0 {
            let l = level - 1;
            let pred_ref = unsafe { pred.deref() };
            let mut curr = pred_ref.forward[l].load(AO::Acquire, guard);

            while let Some(curr_ref) = unsafe { curr.as_ref() } {
                match Self::cmp_key(&curr_ref.key, key) {
                    Ordering::Less => {
                        pred = curr;
                        curr = curr_ref.forward[l].load(AO::Acquire, guard);
                    }
                    Ordering::Equal => return curr,
                    Ordering::Greater => break,
                }
            }

            if l == 0 {
                return curr;
            }
            level -= 1;
        }

        Shared::null()
    }

    /// Internal helper: find first visible version at or before snapshot_seq.
    ///
    /// LSM snapshot semantics:
    ///   - visible if vn.seq < snapshot_seq (strictly less-than).
    #[inline]
    fn visible_version<'g>(
        versions_head: &Atomic<VersionNode>,
        snapshot_seq: u64,
        guard: &'g Guard,
    ) -> Option<&'g VersionNode> {
        let mut v = versions_head.load(AO::Acquire, guard);
        while let Some(vn) = unsafe { v.as_ref() } {
            if vn.seq < snapshot_seq {
                return Some(vn);
            }
            // Version nodes are immutable after publishing, relaxed is fine.
            v = vn.next.load(AO::Relaxed, guard);
        }
        None
    }

    /// Internal helper: iterate all visible versions for merge.
    #[inline]
    fn collect_visible_versions_for_merge(
        versions_head: &Atomic<VersionNode>,
        snapshot_seq: u64,
        guard: &Guard,
        out: &mut Vec<(Option<Bytes>, Option<u64>, OpType)>,
    ) {
        let mut v = versions_head.load(AO::Acquire, guard);
        while let Some(vn) = unsafe { v.as_ref() } {
            if vn.seq < snapshot_seq {
                out.push((vn.val.clone(), vn.exp, vn.op));
            }
            v = vn.next.load(AO::Relaxed, guard);
        }
    }

    /// Get the visible value at or before snapshot_seq.
    ///
    /// Returns `Some(value)` if a visible non-tombstone version exists,
    /// otherwise `None`.
    #[inline]
    pub fn get(&self, key: &[u8], snapshot_seq: u64) -> Option<Bytes> {
        let guard = &epoch::pin();
        let node_ptr = self.find_node(key, guard);

        if let Some(node) = unsafe { node_ptr.as_ref() } {
            if node.key.as_ref() == key {
                if let Some(vn) = Self::visible_version(&node.versions_head, snapshot_seq, guard) {
                    return vn.val.clone();
                }
            }
        }

        None
    }

    /// Get visible value with expiration at or before snapshot_seq.
    ///
    /// Returns:
    ///   - None           → key not visible at this snapshot
    ///   - Some(None)     → key visible as a tombstone
    ///   - Some(Some(..)) → key visible with (value, exp)
    pub fn get_visible_with_exp(
        &self,
        key: &[u8],
        snapshot_seq: u64,
    ) -> Option<Option<(Bytes, Option<u64>)>> {
        let guard = &epoch::pin();
        let node_ptr = self.find_node(key, guard);

        if let Some(node) = unsafe { node_ptr.as_ref() } {
            if node.key.as_ref() == key {
                if let Some(vn) = Self::visible_version(&node.versions_head, snapshot_seq, guard) {
                    return Some(vn.val.clone().map(|val| (val, vn.exp)));
                }
            }
        }

        None
    }

    /// Get all versions for merge resolution (visible at snapshot_seq).
    ///
    /// Newest-first ordering, including tombstones and expirations.
    pub fn get_versions_for_merge(
        &self,
        key: &[u8],
        snapshot_seq: u64,
    ) -> Vec<(Option<Bytes>, Option<u64>, OpType)> {
        let guard = &epoch::pin();
        let node_ptr = self.find_node(key, guard);

        let mut versions = Vec::new();

        if let Some(node) = unsafe { node_ptr.as_ref() } {
            if node.key.as_ref() == key {
                Self::collect_visible_versions_for_merge(
                    &node.versions_head,
                    snapshot_seq,
                    guard,
                    &mut versions,
                );
            }
        }

        versions
    }

    /// Upsert with optional expiration and OpType (lock-free, linearizable).
    pub fn upsert_exp(
        &self,
        key: Bytes,
        value: Option<Bytes>,
        seq: u64,
        exp: Option<u64>,
        op: OpType,
    ) {
        let guard = &epoch::pin();
        self.upsert_exp_internal(key, value, seq, exp, op, guard);
    }

    /// Internal upsert implementation.
    ///
    /// - If the key exists, prepend a new version to the version chain.
    /// - If the key is absent, insert a new node at a random level.
    #[allow(clippy::too_many_arguments)]
    fn upsert_exp_internal(
        &self,
        key: Bytes,
        value: Option<Bytes>,
        seq: u64,
        exp: Option<u64>,
        op: OpType,
        guard: &Guard,
    ) {
        let mut preds: [Shared<Node>; MAX_LEVEL] = [Shared::null(); MAX_LEVEL];
        let mut succs: [Shared<Node>; MAX_LEVEL] = [Shared::null(); MAX_LEVEL];

        // Initial search.
        self.find(&key, guard, &mut preds, &mut succs);

        // Case 1: Key exists – prepend new version to the version chain.
        if let Some(curr) = unsafe { succs[0].as_ref() } {
            if curr.key == key {
                loop {
                    let curr_head = curr.versions_head.load(AO::Acquire, guard);
                    let new_ver = Owned::new(VersionNode {
                        seq,
                        val: value.clone(),
                        exp,
                        op,
                        next: Atomic::from(curr_head),
                    });

                    match curr.versions_head.compare_exchange(
                        curr_head,
                        new_ver,
                        AO::AcqRel,
                        AO::Acquire,
                        guard,
                    ) {
                        Ok(_) => return, // success
                        Err(e) => {
                            // CAS failed; drop newly allocated node and retry.
                            drop(e.new);
                            continue;
                        }
                    }
                }
            }
        }

        // Case 2: Key absent – insert new node at a random level.
        let node_level = Self::random_level();

        // Raise top_level if needed so future searches can use this height.
        let _ = self.top_level.fetch_max(node_level, AO::AcqRel);

        // Stage 1: insert at level 0 (linearization point).
        let new_ptr = loop {
            // Refresh the window at level 0 to minimize CAS failures.
            self.find(&key, guard, &mut preds, &mut succs);

            // Re-check if the key was inserted by another thread while we were preparing.
            // If so, append to the existing node's version chain instead of inserting a new node.
            if let Some(curr) = unsafe { succs[0].as_ref() } {
                if curr.key == key {
                    // Key now exists; prepend version to existing node's chain.
                    loop {
                        let curr_head = curr.versions_head.load(AO::Acquire, guard);
                        let new_ver = Owned::new(VersionNode {
                            seq,
                            val: value.clone(),
                            exp,
                            op,
                            next: Atomic::from(curr_head),
                        });

                        match curr.versions_head.compare_exchange(
                            curr_head,
                            new_ver,
                            AO::AcqRel,
                            AO::Acquire,
                            guard,
                        ) {
                            Ok(_) => return, // success, version appended to existing node
                            Err(e) => {
                                drop(e.new);
                                continue;
                            }
                        }
                    }
                }
            }

            let pred0 = unsafe { preds[0].deref() };
            let succ0 = succs[0];

            // Build new node with first version.
            let first_ver = Owned::new(VersionNode::new(seq, value.clone(), exp, op));
            let new_node = Owned::new(Node::new(key.clone(), first_ver, node_level));
            let new_ptr = new_node.into_shared(guard);

            // Set level-0 forward pointer to current successor.
            unsafe { new_ptr.deref() }.forward[0].store(succ0, AO::Relaxed);

            // Validate window and splice at level 0.
            let pred_next0 = pred0.forward[0].load(AO::Acquire, guard);
            if pred_next0 != succ0 {
                // Window changed; discard and retry.
                unsafe { guard.defer_destroy(new_ptr) };
                continue;
            }

            match pred0.forward[0].compare_exchange(succ0, new_ptr, AO::AcqRel, AO::Acquire, guard)
            {
                Ok(_) => break new_ptr, // level 0 inserted
                Err(_) => {
                    // CAS failed; discard node and retry.
                    unsafe { guard.defer_destroy(new_ptr) };
                    continue;
                }
            }
        };

        // Stage 2: best-effort link higher levels (1..node_level-1).
        for l in 1..node_level {
            loop {
                self.find(&key, guard, &mut preds, &mut succs);
                let pred = unsafe { preds[l].deref() };
                let succ = succs[l];

                unsafe { new_ptr.deref() }.forward[l].store(succ, AO::Relaxed);
                let pred_next = pred.forward[l].load(AO::Acquire, guard);
                if pred_next != succ {
                    continue; // window changed, retry
                }

                if pred.forward[l]
                    .compare_exchange(succ, new_ptr, AO::AcqRel, AO::Acquire, guard)
                    .is_ok()
                {
                    break; // linked this level
                }
            }
        }
    }

    /// Insert or update with sequence number (Put).
    #[inline]
    pub fn upsert(&self, key: Bytes, value: Option<Bytes>, seq: u64) {
        self.upsert_exp(key, value, seq, None, OpType::Put);
    }

    /// Delete a key (insert tombstone).
    #[inline]
    pub fn delete(&self, key: Bytes, seq: u64) {
        self.upsert_exp(key, None, seq, None, OpType::Delete);
    }

    /// Range scan returning visible entries at snapshot_seq.
    ///
    /// Returns newest visible non-tombstone value for each key in [start, end).
    pub fn range_visible(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        snapshot_seq: u64,
    ) -> Vec<(Bytes, Bytes)> {
        let guard = &epoch::pin();

        // Find starting point.
        let start_key = start.unwrap_or(&[]);
        let mut preds: [Shared<Node>; MAX_LEVEL] = [Shared::null(); MAX_LEVEL];
        let mut succs: [Shared<Node>; MAX_LEVEL] = [Shared::null(); MAX_LEVEL];
        self.find(start_key, guard, &mut preds, &mut succs);

        let mut out = Vec::with_capacity(128);
        let mut curr = if start.is_none() {
            self.head.forward[0].load(AO::Acquire, guard)
        } else {
            succs[0]
        };

        while let Some(node) = unsafe { curr.as_ref() } {
            // Check end boundary.
            if let Some(end_key) = end {
                if node.key.as_ref() >= end_key {
                    break;
                }
            }

            if let Some(vn) = Self::visible_version(&node.versions_head, snapshot_seq, guard) {
                if let Some(ref val) = vn.val {
                    out.push((node.key.clone(), val.clone()));
                }
            }

            curr = node.forward[0].load(AO::Acquire, guard);
        }

        out
    }

    /// Get all tombstoned keys in range visible at snapshot_seq.
    pub fn tombstones_range_visible(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        snapshot_seq: u64,
    ) -> Vec<Bytes> {
        let guard = &epoch::pin();

        let start_key = start.unwrap_or(&[]);
        let mut preds: [Shared<Node>; MAX_LEVEL] = [Shared::null(); MAX_LEVEL];
        let mut succs: [Shared<Node>; MAX_LEVEL] = [Shared::null(); MAX_LEVEL];
        self.find(start_key, guard, &mut preds, &mut succs);

        let mut out = Vec::with_capacity(32);
        let mut curr = if start.is_none() {
            self.head.forward[0].load(AO::Acquire, guard)
        } else {
            succs[0]
        };

        while let Some(node) = unsafe { curr.as_ref() } {
            if let Some(end_key) = end {
                if node.key.as_ref() >= end_key {
                    break;
                }
            }

            if let Some(vn) = Self::visible_version(&node.versions_head, snapshot_seq, guard) {
                if vn.val.is_none() {
                    out.push(node.key.clone());
                }
            }

            curr = node.forward[0].load(AO::Acquire, guard);
        }

        out
    }

    /// Drain all entries with metadata and expiration (logical snapshot).
    ///
    /// This does *not* physically clear the skiplist; instead it walks all
    /// nodes and returns all versions newest-first for each key.
    pub fn drain_with_meta_with_exp(&self) -> Vec<SkipListEntryWithExp> {
        let guard = &epoch::pin();
        let mut out = Vec::with_capacity(256);

        let mut curr = self.head.forward[0].load(AO::Acquire, guard);

        while let Some(node) = unsafe { curr.as_ref() } {
            let mut vn_ptr = node.versions_head.load(AO::Acquire, guard);
            while let Some(vn) = unsafe { vn_ptr.as_ref() } {
                let is_tomb = vn.val.is_none();
                out.push((
                    node.key.clone(),
                    vn.val.clone(),
                    vn.seq,
                    is_tomb,
                    vn.exp,
                    vn.op,
                ));
                vn_ptr = vn.next.load(AO::Relaxed, guard);
            }

            curr = node.forward[0].load(AO::Acquire, guard);
        }

        out
    }

    /// Delete range by inserting tombstones for all keys in [start, end).
    ///
    /// Returns the number of keys whose visible value changed from non-tombstone
    /// to tombstone at this sequence.
    pub fn delete_range(&self, start: Option<&[u8]>, end: Option<&[u8]>, seq: u64) -> usize {
        let guard = &epoch::pin();

        // Collect all keys in the range first.
        let start_key = start.unwrap_or(&[]);
        let mut preds: [Shared<Node>; MAX_LEVEL] = [Shared::null(); MAX_LEVEL];
        let mut succs: [Shared<Node>; MAX_LEVEL] = [Shared::null(); MAX_LEVEL];
        self.find(start_key, guard, &mut preds, &mut succs);

        let mut keys_to_delete = Vec::with_capacity(32);
        let mut curr = if start.is_none() {
            self.head.forward[0].load(AO::Acquire, guard)
        } else {
            succs[0]
        };

        while let Some(node) = unsafe { curr.as_ref() } {
            if let Some(end_key) = end {
                if node.key.as_ref() >= end_key {
                    break;
                }
            }
            keys_to_delete.push(node.key.clone());
            curr = node.forward[0].load(AO::Acquire, guard);
        }

        // Insert tombstones for all keys.
        let mut changed = 0;
        for key in keys_to_delete {
            // Observe if there was a visible value before this seq.
            if self.get(&key, seq).is_some() {
                changed += 1;
            }
            self.delete(key, seq);
        }

        changed
    }

    /// Get all keys currently in the skiplist (no snapshot filtering).
    pub fn get_all_keys(&self) -> Vec<Bytes> {
        let guard = &epoch::pin();
        let mut keys = Vec::with_capacity(128);

        let mut curr = self.head.forward[0].load(AO::Acquire, guard);
        while let Some(node) = unsafe { curr.as_ref() } {
            keys.push(node.key.clone());
            curr = node.forward[0].load(AO::Acquire, guard);
        }

        keys
    }

    /// Range scan returning latest visible non-tombstone entries (no snapshot filtering).
    ///
    /// This is used by the memtable for "current state" queries.
    pub fn range(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Vec<(Bytes, Bytes)> {
        self.range_visible(start, end, u64::MAX)
    }

    /// Get all tombstoned keys in range (no snapshot filtering).
    pub fn tombstones_range(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Vec<Bytes> {
        self.tombstones_range_visible(start, end, u64::MAX)
    }
}

impl Default for SkipList {
    fn default() -> Self {
        Self::new()
    }
}

// Safety: SkipList is Send + Sync because:
// - All shared state is accessed via atomic operations and epoch-based pointers.
// - We do not use interior mutability without synchronization.
// - Memory reclamation is handled by crossbeam-epoch.
unsafe impl Send for SkipList {}
unsafe impl Sync for SkipList {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn should_insert_value() {
        // Arrange
        let sl = SkipList::new();

        // Act
        sl.upsert(Bytes::from_static(b"k"), Some(Bytes::from_static(b"v")), 1);

        // Assert
        // Insert succeeds without panic
    }

    #[test]
    fn should_get_value() {
        // Arrange
        let sl = SkipList::new();
        sl.upsert(Bytes::from_static(b"k"), Some(Bytes::from_static(b"v")), 1);

        // Act
        let got = sl.get(b"k", u64::MAX);

        // Assert
        assert_eq!(got, Some(Bytes::from_static(b"v")));
    }

    #[test]
    fn should_get_visible_by_snapshot_seq() {
        // Arrange
        let sl = SkipList::new();
        sl.upsert(
            Bytes::from_static(b"k"),
            Some(Bytes::from_static(b"v1")),
            10,
        );
        sl.upsert(
            Bytes::from_static(b"k"),
            Some(Bytes::from_static(b"v2")),
            20,
        );

        // Act
        let v_at_15 = sl.get(b"k", 15);
        let v_at_25 = sl.get(b"k", 25);

        // Assert
        assert_eq!(v_at_15, Some(Bytes::from_static(b"v1")));
        assert_eq!(v_at_25, Some(Bytes::from_static(b"v2")));
    }

    #[test]
    fn should_return_range_visible() {
        // Arrange
        let sl = SkipList::new();
        sl.upsert(Bytes::from_static(b"a"), Some(Bytes::from_static(b"1")), 1);
        sl.upsert(Bytes::from_static(b"b"), Some(Bytes::from_static(b"2")), 2);
        sl.upsert(Bytes::from_static(b"c"), None, 3); // tombstone

        // Act
        let rows = sl.range_visible(Some(b"a"), Some(b"z"), u64::MAX);

        // Assert
        assert_eq!(
            rows,
            vec![
                (Bytes::from_static(b"a"), Bytes::from_static(b"1")),
                (Bytes::from_static(b"b"), Bytes::from_static(b"2"))
            ]
        );
    }

    #[test]
    fn should_collect_tombstones_visible() {
        // Arrange
        let sl = SkipList::new();
        sl.upsert(Bytes::from_static(b"a"), Some(Bytes::from_static(b"1")), 1);
        sl.upsert(Bytes::from_static(b"b"), None, 2);
        sl.upsert(Bytes::from_static(b"c"), None, 3);

        // Act
        let t = sl.tombstones_range_visible(Some(b"a"), Some(b"z"), u64::MAX);

        // Assert
        assert_eq!(t, vec![Bytes::from_static(b"b"), Bytes::from_static(b"c")]);
    }

    #[test]
    fn should_handle_concurrent_writes() {
        // Arrange
        let sl = Arc::new(SkipList::new());
        let mut handles = vec![];

        // Act
        for i in 0..4 {
            let sl_clone = Arc::clone(&sl);
            let handle = thread::spawn(move || {
                for j in 0..100 {
                    let key = format!("key_{}", i * 100 + j);
                    let val = format!("val_{}", i * 100 + j);
                    sl_clone.upsert(
                        Bytes::from(key),
                        Some(Bytes::from(val)),
                        (i * 100 + j) as u64,
                    );
                }
            });
            handles.push(handle);
        }
        for h in handles {
            h.join().unwrap();
        }

        // Assert
        let keys = sl.get_all_keys();
        assert_eq!(keys.len(), 400);
    }

    #[test]
    fn should_support_concurrent_operations() {
        // Arrange
        let sl = Arc::new(SkipList::new());
        for i in 0..100 {
            sl.upsert(
                Bytes::from(format!("key_{}", i)),
                Some(Bytes::from(format!("val_{}", i))),
                i as u64,
            );
        }

        // Act
        let mut handles = vec![];
        for i in 0..2 {
            let sl_clone = Arc::clone(&sl);
            let handle = thread::spawn(move || {
                for j in 0..50 {
                    let key = format!("key_{}", i * 50 + j);
                    let val = format!("updated_{}", i * 50 + j);
                    sl_clone.upsert(
                        Bytes::from(key),
                        Some(Bytes::from(val)),
                        (100 + i * 50 + j) as u64,
                    );
                }
            });
            handles.push(handle);
        }
        for _ in 0..2 {
            let sl_clone = Arc::clone(&sl);
            let handle = thread::spawn(move || {
                for i in 0..100 {
                    let key = format!("key_{}", i);
                    let _ = sl_clone.get(key.as_bytes(), u64::MAX);
                }
            });
            handles.push(handle);
        }
        for h in handles {
            h.join().unwrap();
        }

        // Assert – no panics, all operations race-safe
    }

    #[test]
    fn should_get_versions_for_merge() {
        // Arrange
        let sl = SkipList::new();
        sl.upsert_exp(
            Bytes::from_static(b"k"),
            Some(Bytes::from_static(b"base")),
            10,
            None,
            OpType::Put,
        );
        sl.upsert_exp(
            Bytes::from_static(b"k"),
            Some(Bytes::from_static(b"op1")),
            20,
            None,
            OpType::Merge,
        );
        sl.upsert_exp(
            Bytes::from_static(b"k"),
            Some(Bytes::from_static(b"op2")),
            30,
            None,
            OpType::Merge,
        );

        // Act
        let versions = sl.get_versions_for_merge(b"k", u64::MAX);

        // Assert
        assert_eq!(versions.len(), 3);
        assert_eq!(versions[0].2, OpType::Merge);
        assert_eq!(versions[1].2, OpType::Merge);
        assert_eq!(versions[2].2, OpType::Put);
    }

    #[test]
    fn should_delete_range() {
        // Arrange
        let sl = SkipList::new();
        sl.upsert(Bytes::from_static(b"a"), Some(Bytes::from_static(b"1")), 1);
        sl.upsert(Bytes::from_static(b"b"), Some(Bytes::from_static(b"2")), 2);
        sl.upsert(Bytes::from_static(b"c"), Some(Bytes::from_static(b"3")), 3);

        // Act
        let changed = sl.delete_range(Some(b"a"), Some(b"c"), 10);

        // Assert
        assert_eq!(changed, 2); // a and b were deleted
        assert_eq!(sl.get(b"a", u64::MAX), None);
        assert_eq!(sl.get(b"b", u64::MAX), None);
        assert_eq!(sl.get(b"c", u64::MAX), Some(Bytes::from_static(b"3")));
    }

    #[test]
    fn should_drain_with_metadata() {
        // Arrange
        let sl = SkipList::new();
        sl.upsert(Bytes::from_static(b"a"), Some(Bytes::from_static(b"1")), 1);
        sl.upsert(Bytes::from_static(b"b"), None, 2);
        sl.upsert_exp(
            Bytes::from_static(b"c"),
            Some(Bytes::from_static(b"3")),
            3,
            Some(12345),
            OpType::Put,
        );

        // Act
        let entries = sl.drain_with_meta_with_exp();

        // Assert
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].0, Bytes::from_static(b"a"));
        assert!(!entries[0].3); // not tombstone
        assert_eq!(entries[1].0, Bytes::from_static(b"b"));
        assert!(entries[1].3); // tombstone
        assert_eq!(entries[2].4, Some(12345)); // has expiration
    }

    // ========================================================================
    // OpType enum tests
    // ========================================================================

    #[test]
    fn should_convert_optype_put_to_u8() {
        // Arrange / Act
        let code = OpType::Put.as_u8();

        // Assert: Put maps to 0
        assert_eq!(code, 0);
    }

    #[test]
    fn should_convert_optype_delete_to_u8() {
        // Arrange / Act
        let code = OpType::Delete.as_u8();

        // Assert: Delete maps to 2
        assert_eq!(code, 2);
    }

    #[test]
    fn should_convert_optype_merge_to_u8() {
        // Arrange / Act
        let code = OpType::Merge.as_u8();

        // Assert: Merge maps to 3
        assert_eq!(code, 3);
    }

    #[test]
    fn should_support_optype_equality() {
        // Arrange / Act / Assert
        assert_eq!(OpType::Put, OpType::Put);
        assert_eq!(OpType::Delete, OpType::Delete);
        assert_eq!(OpType::Merge, OpType::Merge);
        assert_ne!(OpType::Put, OpType::Delete);
        assert_ne!(OpType::Put, OpType::Merge);
    }

    #[test]
    fn should_support_optype_copy() {
        // Arrange
        let op1 = OpType::Put;

        // Act
        let op2 = op1;

        // Assert: Copy trait works
        assert_eq!(op1, op2);
    }

    // ========================================================================
    // SkipList creation and initialization tests
    // ========================================================================

    #[test]
    fn should_create_new_skiplist() {
        // Arrange / Act
        let sl = SkipList::new();

        // Assert: creation succeeds
        let keys = sl.get_all_keys();
        assert_eq!(keys.len(), 0);
    }

    #[test]
    fn should_support_default_trait() {
        // Arrange / Act
        let sl = SkipList::default();

        // Assert: same as new()
        let keys = sl.get_all_keys();
        assert_eq!(keys.len(), 0);
    }

    // ========================================================================
    // Basic upsert tests
    // ========================================================================

    #[test]
    fn should_upsert_new_key() {
        // Arrange
        let sl = SkipList::new();

        // Act
        sl.upsert(
            Bytes::from_static(b"key"),
            Some(Bytes::from_static(b"value")),
            1,
        );

        // Assert
        assert_eq!(sl.get(b"key", u64::MAX), Some(Bytes::from_static(b"value")));
    }

    #[test]
    fn should_update_existing_key() {
        // Arrange
        let sl = SkipList::new();
        sl.upsert(Bytes::from_static(b"k"), Some(Bytes::from_static(b"v1")), 1);

        // Act
        sl.upsert(Bytes::from_static(b"k"), Some(Bytes::from_static(b"v2")), 2);

        // Assert: latest version returned
        assert_eq!(sl.get(b"k", u64::MAX), Some(Bytes::from_static(b"v2")));
    }

    #[test]
    fn should_support_empty_key() {
        // Arrange
        let sl = SkipList::new();

        // Act
        sl.upsert(Bytes::new(), Some(Bytes::from_static(b"value")), 1);

        // Assert
        assert_eq!(sl.get(b"", u64::MAX), Some(Bytes::from_static(b"value")));
    }

    #[test]
    fn should_support_empty_value() {
        // Arrange
        let sl = SkipList::new();

        // Act
        sl.upsert(Bytes::from_static(b"key"), Some(Bytes::new()), 1);

        // Assert
        assert_eq!(sl.get(b"key", u64::MAX), Some(Bytes::new()));
    }

    #[test]
    fn should_support_binary_keys() {
        // Arrange
        let sl = SkipList::new();
        let binary_key = Bytes::from(vec![0u8, 1u8, 255u8, 127u8]);

        // Act
        sl.upsert(binary_key.clone(), Some(Bytes::from_static(b"val")), 1);

        // Assert
        assert_eq!(
            sl.get(binary_key.as_ref(), u64::MAX),
            Some(Bytes::from_static(b"val"))
        );
    }

    #[test]
    fn should_support_binary_values() {
        // Arrange
        let sl = SkipList::new();
        let binary_val = Bytes::from(vec![0u8, 127u8, 255u8]);

        // Act
        sl.upsert(Bytes::from_static(b"key"), Some(binary_val.clone()), 1);

        // Assert
        assert_eq!(sl.get(b"key", u64::MAX), Some(binary_val));
    }

    #[test]
    fn should_handle_large_keys() {
        // Arrange
        let sl = SkipList::new();
        let large_key = Bytes::from(vec![42u8; 10000]);

        // Act
        sl.upsert(large_key.clone(), Some(Bytes::from_static(b"val")), 1);

        // Assert
        assert_eq!(
            sl.get(large_key.as_ref(), u64::MAX),
            Some(Bytes::from_static(b"val"))
        );
    }

    #[test]
    fn should_handle_large_values() {
        // Arrange
        let sl = SkipList::new();
        let large_val = Bytes::from(vec![99u8; 10 * 1024 * 1024]); // 10MB

        // Act
        sl.upsert(Bytes::from_static(b"key"), Some(large_val.clone()), 1);

        // Assert
        assert_eq!(sl.get(b"key", u64::MAX), Some(large_val));
    }

    // ========================================================================
    // Delete and tombstone tests
    // ========================================================================

    #[test]
    fn should_delete_key() {
        // Arrange
        let sl = SkipList::new();
        sl.upsert(Bytes::from_static(b"k"), Some(Bytes::from_static(b"v")), 1);

        // Act
        sl.delete(Bytes::from_static(b"k"), 2);

        // Assert: deleted key returns None
        assert_eq!(sl.get(b"k", u64::MAX), None);
    }

    #[test]
    fn should_resurrect_deleted_key() {
        // Arrange
        let sl = SkipList::new();
        sl.upsert(Bytes::from_static(b"k"), Some(Bytes::from_static(b"v1")), 1);
        sl.delete(Bytes::from_static(b"k"), 2);

        // Act: insert new value at higher sequence
        sl.upsert(Bytes::from_static(b"k"), Some(Bytes::from_static(b"v2")), 3);

        // Assert
        assert_eq!(sl.get(b"k", u64::MAX), Some(Bytes::from_static(b"v2")));
    }

    #[test]
    fn should_mark_deleted_key_as_tombstone() {
        // Arrange
        let sl = SkipList::new();
        sl.upsert(Bytes::from_static(b"k"), Some(Bytes::from_static(b"v")), 1);
        sl.delete(Bytes::from_static(b"k"), 2);

        // Act
        let tombstones = sl.tombstones_range(None, None);

        // Assert
        assert_eq!(tombstones, vec![Bytes::from_static(b"k")]);
    }

    // ========================================================================
    // Snapshot visibility tests
    // ========================================================================

    #[test]
    fn should_hide_future_versions() {
        // Arrange
        let sl = SkipList::new();
        sl.upsert(
            Bytes::from_static(b"k"),
            Some(Bytes::from_static(b"v1")),
            10,
        );
        sl.upsert(
            Bytes::from_static(b"k"),
            Some(Bytes::from_static(b"v2")),
            20,
        );

        // Act: query at sequence 15 (between 10 and 20)
        let result = sl.get(b"k", 15);

        // Assert: should see v1 (seq 10), not v2 (seq 20)
        assert_eq!(result, Some(Bytes::from_static(b"v1")));
    }

    #[test]
    fn should_see_oldest_visible_version() {
        // Arrange
        let sl = SkipList::new();
        sl.upsert(Bytes::from_static(b"k"), Some(Bytes::from_static(b"v1")), 5);
        sl.upsert(
            Bytes::from_static(b"k"),
            Some(Bytes::from_static(b"v2")),
            10,
        );
        sl.upsert(
            Bytes::from_static(b"k"),
            Some(Bytes::from_static(b"v3")),
            15,
        );

        // Act
        let v_at_7 = sl.get(b"k", 7);
        let v_at_12 = sl.get(b"k", 12);
        let v_at_20 = sl.get(b"k", 20);

        // Assert: LSM visibility rule - oldest visible version
        assert_eq!(v_at_7, Some(Bytes::from_static(b"v1")));
        assert_eq!(v_at_12, Some(Bytes::from_static(b"v2")));
        assert_eq!(v_at_20, Some(Bytes::from_static(b"v3")));
    }

    #[test]
    fn should_return_none_at_empty_sequence() {
        // Arrange
        let sl = SkipList::new();
        sl.upsert(Bytes::from_static(b"k"), Some(Bytes::from_static(b"v")), 10);

        // Act: query at sequence less than any version
        let result = sl.get(b"k", 5);

        // Assert: no visible version
        assert!(result.is_none());
    }

    #[test]
    fn should_hide_tombstone_from_future() {
        // Arrange
        let sl = SkipList::new();
        sl.upsert(Bytes::from_static(b"k"), Some(Bytes::from_static(b"v")), 5);
        sl.delete(Bytes::from_static(b"k"), 10);

        // Act: query between write and delete
        let result = sl.get(b"k", 7);

        // Assert: should see the value before deletion
        assert_eq!(result, Some(Bytes::from_static(b"v")));
    }

    // ========================================================================
    // Range scan tests
    // ========================================================================

    #[test]
    fn should_scan_full_range() {
        // Arrange
        let sl = SkipList::new();
        sl.upsert(Bytes::from_static(b"a"), Some(Bytes::from_static(b"1")), 1);
        sl.upsert(Bytes::from_static(b"b"), Some(Bytes::from_static(b"2")), 1);
        sl.upsert(Bytes::from_static(b"c"), Some(Bytes::from_static(b"3")), 1);

        // Act
        let results = sl.range(None, None);

        // Assert
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, Bytes::from_static(b"a"));
        assert_eq!(results[1].0, Bytes::from_static(b"b"));
        assert_eq!(results[2].0, Bytes::from_static(b"c"));
    }

    #[test]
    fn should_scan_with_start_bound() {
        // Arrange
        let sl = SkipList::new();
        for i in 0..10 {
            sl.upsert(
                Bytes::from(format!("key_{}", i)),
                Some(Bytes::from(format!("val_{}", i))),
                1,
            );
        }

        // Act
        let results = sl.range(Some(b"key_5"), None);

        // Assert: starts at key_5
        assert!(results[0].0.starts_with(b"key_5"));
    }

    #[test]
    fn should_scan_with_end_bound() {
        // Arrange
        let sl = SkipList::new();
        sl.upsert(Bytes::from_static(b"a"), Some(Bytes::from_static(b"1")), 1);
        sl.upsert(Bytes::from_static(b"b"), Some(Bytes::from_static(b"2")), 1);
        sl.upsert(Bytes::from_static(b"c"), Some(Bytes::from_static(b"3")), 1);

        // Act: [a, c) should give a and b
        let results = sl.range(Some(b"a"), Some(b"c"));

        // Assert
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, Bytes::from_static(b"a"));
        assert_eq!(results[1].0, Bytes::from_static(b"b"));
    }

    #[test]
    fn should_skip_tombstones_in_range() {
        // Arrange
        let sl = SkipList::new();
        sl.upsert(Bytes::from_static(b"a"), Some(Bytes::from_static(b"1")), 1);
        sl.upsert(Bytes::from_static(b"b"), None, 2); // tombstone
        sl.upsert(Bytes::from_static(b"c"), Some(Bytes::from_static(b"3")), 3);

        // Act
        let results = sl.range(None, None);

        // Assert: only non-tombstones
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, Bytes::from_static(b"a"));
        assert_eq!(results[1].0, Bytes::from_static(b"c"));
    }

    #[test]
    fn should_return_empty_range_for_non_overlapping_bounds() {
        // Arrange
        let sl = SkipList::new();
        sl.upsert(Bytes::from_static(b"a"), Some(Bytes::from_static(b"1")), 1);

        // Act: [z, zz) has no keys
        let results = sl.range(Some(b"z"), Some(b"zz"));

        // Assert
        assert!(results.is_empty());
    }

    // ========================================================================
    // Expiration tests
    // ========================================================================

    #[test]
    fn should_store_expiration_time() {
        // Arrange
        let sl = SkipList::new();

        // Act
        sl.upsert_exp(
            Bytes::from_static(b"k"),
            Some(Bytes::from_static(b"v")),
            1,
            Some(123456),
            OpType::Put,
        );

        // Assert
        let result = sl.get_visible_with_exp(b"k", u64::MAX);
        assert_eq!(result, Some(Some((Bytes::from_static(b"v"), Some(123456)))));
    }

    #[test]
    fn should_support_upsert_without_expiration() {
        // Arrange
        let sl = SkipList::new();

        // Act
        sl.upsert_exp(
            Bytes::from_static(b"k"),
            Some(Bytes::from_static(b"v")),
            1,
            None,
            OpType::Put,
        );

        // Assert
        let result = sl.get_visible_with_exp(b"k", u64::MAX);
        assert_eq!(result, Some(Some((Bytes::from_static(b"v"), None))));
    }

    // ========================================================================
    // OpType tracking tests
    // ========================================================================

    #[test]
    fn should_track_put_operations() {
        // Arrange
        let sl = SkipList::new();

        // Act
        sl.upsert_exp(
            Bytes::from_static(b"k"),
            Some(Bytes::from_static(b"v")),
            1,
            None,
            OpType::Put,
        );

        // Assert
        let versions = sl.get_versions_for_merge(b"k", u64::MAX);
        assert_eq!(versions[0].2, OpType::Put);
    }

    #[test]
    fn should_track_merge_operations() {
        // Arrange
        let sl = SkipList::new();

        // Act
        sl.upsert_exp(
            Bytes::from_static(b"k"),
            Some(Bytes::from_static(b"v")),
            1,
            None,
            OpType::Merge,
        );

        // Assert
        let versions = sl.get_versions_for_merge(b"k", u64::MAX);
        assert_eq!(versions[0].2, OpType::Merge);
    }

    #[test]
    fn should_track_delete_operations() {
        // Arrange
        let sl = SkipList::new();
        sl.upsert_exp(
            Bytes::from_static(b"k"),
            Some(Bytes::from_static(b"v")),
            1,
            None,
            OpType::Put,
        );

        // Act
        sl.upsert_exp(Bytes::from_static(b"k"), None, 2, None, OpType::Delete);

        // Assert
        let versions = sl.get_versions_for_merge(b"k", u64::MAX);
        assert_eq!(versions[0].2, OpType::Delete); // Most recent
        assert_eq!(versions[1].2, OpType::Put); // Older
    }

    // ========================================================================
    // get_all_keys tests
    // ========================================================================

    #[test]
    fn should_return_all_keys_in_insertion_order() {
        // Arrange
        let sl = SkipList::new();
        sl.upsert(Bytes::from_static(b"z"), Some(Bytes::from_static(b"1")), 1);
        sl.upsert(Bytes::from_static(b"a"), Some(Bytes::from_static(b"2")), 1);
        sl.upsert(Bytes::from_static(b"m"), Some(Bytes::from_static(b"3")), 1);

        // Act
        let keys = sl.get_all_keys();

        // Assert: sorted order (skiplist property)
        assert_eq!(keys[0], Bytes::from_static(b"a"));
        assert_eq!(keys[1], Bytes::from_static(b"m"));
        assert_eq!(keys[2], Bytes::from_static(b"z"));
    }

    #[test]
    fn should_return_empty_keys_for_empty_skiplist() {
        // Arrange / Act
        let sl = SkipList::new();
        let keys = sl.get_all_keys();

        // Assert
        assert!(keys.is_empty());
    }

    #[test]
    fn should_return_deleted_keys_in_all_keys() {
        // Arrange
        let sl = SkipList::new();
        sl.upsert(Bytes::from_static(b"a"), Some(Bytes::from_static(b"1")), 1);
        sl.upsert(Bytes::from_static(b"b"), Some(Bytes::from_static(b"2")), 1);
        sl.delete(Bytes::from_static(b"a"), 2); // deleted, but still in skiplist

        // Act
        let keys = sl.get_all_keys();

        // Assert: deleted keys still present in skiplist structure
        assert_eq!(keys.len(), 2);
    }

    // ========================================================================
    // delete_range tests
    // ========================================================================

    #[test]
    fn should_count_changed_keys_in_delete_range() {
        // Arrange
        let sl = SkipList::new();
        sl.upsert(Bytes::from_static(b"a"), Some(Bytes::from_static(b"1")), 1);
        sl.upsert(Bytes::from_static(b"b"), Some(Bytes::from_static(b"2")), 2);
        sl.upsert(Bytes::from_static(b"c"), Some(Bytes::from_static(b"3")), 3);

        // Act: delete range [a, c)
        let changed = sl.delete_range(Some(b"a"), Some(b"c"), 10);

        // Assert: a and b changed
        assert_eq!(changed, 2);
    }

    #[test]
    fn should_handle_delete_range_with_no_changes() {
        // Arrange
        let sl = SkipList::new();
        sl.upsert(Bytes::from_static(b"a"), Some(Bytes::from_static(b"1")), 1);
        sl.delete(Bytes::from_static(b"b"), 2);

        // Act: delete range on already deleted key
        let changed = sl.delete_range(Some(b"b"), Some(b"b"), 10);

        // Assert: no changes (b was already deleted)
        assert_eq!(changed, 0);
    }

    #[test]
    fn should_delete_full_range_when_unbounded() {
        // Arrange
        let sl = SkipList::new();
        sl.upsert(Bytes::from_static(b"a"), Some(Bytes::from_static(b"1")), 1);
        sl.upsert(Bytes::from_static(b"b"), Some(Bytes::from_static(b"2")), 2);
        sl.upsert(Bytes::from_static(b"c"), Some(Bytes::from_static(b"3")), 3);

        // Act: delete [None, None)
        let changed = sl.delete_range(None, None, 10);

        // Assert: all three keys deleted
        assert_eq!(changed, 3);
    }

    #[test]
    fn should_create_multiple_versions_on_delete_range() {
        // Arrange
        let sl = SkipList::new();
        sl.upsert(Bytes::from_static(b"k"), Some(Bytes::from_static(b"v")), 1);

        // Act: delete and then query versions
        sl.delete_range(Some(b"k"), Some(b"l"), 2);
        let versions = sl.get_versions_for_merge(b"k", u64::MAX);

        // Assert: two versions (put, then delete)
        assert_eq!(versions.len(), 2);
        assert!(versions[0].0.is_none()); // Delete is tombstone
        assert_eq!(versions[1].0, Some(Bytes::from_static(b"v"))); // Original put
    }

    // ========================================================================
    // Range visibility tests
    // ========================================================================

    #[test]
    fn should_filter_by_snapshot_in_range() {
        // Arrange
        let sl = SkipList::new();
        sl.upsert(Bytes::from_static(b"a"), Some(Bytes::from_static(b"v1")), 5);
        sl.upsert(
            Bytes::from_static(b"a"),
            Some(Bytes::from_static(b"v2")),
            15,
        );
        sl.upsert(Bytes::from_static(b"b"), Some(Bytes::from_static(b"v")), 10);

        // Act: snapshot at seq 12
        let results = sl.range_visible(None, None, 12);

        // Assert: a shows v1 (seq 5), b shows v (seq 10)
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].1, Bytes::from_static(b"v1"));
        assert_eq!(results[1].1, Bytes::from_static(b"v"));
    }

    #[test]
    fn should_respect_tombstone_visibility() {
        // Arrange
        let sl = SkipList::new();
        sl.upsert(Bytes::from_static(b"a"), Some(Bytes::from_static(b"1")), 1);
        sl.delete(Bytes::from_static(b"a"), 5);
        sl.upsert(Bytes::from_static(b"a"), Some(Bytes::from_static(b"2")), 10);

        // Act: query at different snapshots
        let at_3 = sl.range_visible(None, None, 3); // Before delete
        let at_7 = sl.range_visible(None, None, 7); // After delete
        let at_15 = sl.range_visible(None, None, 15); // After resurrection

        // Assert
        assert_eq!(at_3.len(), 1);
        assert_eq!(at_7.len(), 0);
        assert_eq!(at_15.len(), 1);
    }
}
