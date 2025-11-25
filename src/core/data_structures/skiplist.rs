//! Lock-Free Multi-Version Skiplist
//!
//! - Fully lock-free (writers use CAS, readers are wait-free under epoch guards).
//! - Uses crossbeam-epoch for safe concurrent memory reclamation.
//! - Supports MVCC visibility via sequence numbers.
//! - Designed for LSM memtable use: no physical deletion, tombstones only.

use bytes::Bytes;
use crossbeam_epoch::{self as epoch, Atomic, Guard, Owned, Shared};
use std::cmp::Ordering;
use std::num::Wrapping;
use std::sync::atomic::{AtomicUsize, Ordering as AO};
use std::sync::Arc;

const MAX_LEVEL: usize = 16;

/// Entry metadata tuple: (key, value_opt, sequence, is_tombstone)
pub type SkipListEntry = (Bytes, Option<Bytes>, u64, bool);
/// Extended entry metadata including optional expiration (Unix millis)
pub type SkipListEntryWithExp = (Bytes, Option<Bytes>, u64, bool, Option<u64>, OpType);

/// Operation type for a skiplist version
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

/// A version node in the lock-free version chain
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

/// A node in the lock-free skiplist
#[derive(Debug)]
struct Node {
    key: Bytes,
    /// Head of newest-first version chain
    versions_head: Atomic<VersionNode>,
    /// Forward pointers per level (level 0..level-1 valid)
    forward: [Atomic<Node>; MAX_LEVEL],
    #[allow(dead_code)]
    level: usize,
}

impl Node {
    fn new(key: Bytes, first_version: Owned<VersionNode>, level: usize) -> Self {
        debug_assert!((1..=MAX_LEVEL).contains(&level));

        // Initialize forward array to null
        let forward: [Atomic<Node>; MAX_LEVEL] = Default::default();

        Node {
            key,
            versions_head: Atomic::from(first_version),
            forward,
            level,
        }
    }

    fn sentinel() -> Self {
        let empty_version = Owned::new(VersionNode::new(0, None, None, OpType::Put));
        Node::new(Bytes::new(), empty_version, MAX_LEVEL)
    }
}

/// Splice hint for optimizing sequential/localized insertions
/// Caches the search path from the last insert
struct Splice<'g> {
    /// Cached predecessors at each level
    preds: [Shared<'g, Node>; MAX_LEVEL],
    /// Cached successors at each level
    succs: [Shared<'g, Node>; MAX_LEVEL],
    /// Height of the cached splice
    height: usize,
}

impl<'g> Splice<'g> {
    #[allow(dead_code)]
    fn new() -> Self {
        Splice {
            preds: [Shared::null(); MAX_LEVEL],
            succs: [Shared::null(); MAX_LEVEL],
            height: 0,
        }
    }
}

/// Lock-free skiplist with multi-version concurrency control
///
/// This implementation uses epoch-based memory reclamation (crossbeam-epoch) for safe
/// concurrent access without locks. All operations are lock-free and linearizable.
pub struct SkipList {
    head: Arc<Node>,
    top_level: AtomicUsize,
}

impl SkipList {
    /// Create a new lock-free skiplist
    pub fn new() -> Self {
        let head = Arc::new(Node::sentinel());
        SkipList {
            head,
            top_level: AtomicUsize::new(1),
        }
    }

    /// Generate a random level using thread-local RNG
    #[inline]
    fn random_level() -> usize {
        use std::cell::RefCell;

        thread_local! {
            static RNG: RefCell<Wrapping<u64>> = const { RefCell::new(Wrapping(0x9E37_79B9_7F4A_7C15)) };
        }

        RNG.with(|rng| {
            let mut lvl = 1;
            let mut r = rng.borrow_mut();

            // xorshift64*
            let mut x = r.0;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            *r = Wrapping(x);
            let rand = x.wrapping_mul(0x2545F4914F6CDD1D);

            // p = 1/4: advance level while lowest two bits are zero
            while lvl < MAX_LEVEL && (rand >> (lvl * 2)) & 0b11 == 0 {
                lvl += 1;
            }
            lvl
        })
    }

    #[inline(always)]
    fn cmp_key(a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }

    /// Find predecessors and successors at all levels for the given key
    fn find<'g>(
        &self,
        key: &[u8],
        guard: &'g Guard,
        preds: &mut [Shared<'g, Node>; MAX_LEVEL],
        succs: &mut [Shared<'g, Node>; MAX_LEVEL],
    ) {
        // Start from head sentinel
        let mut pred: Shared<'g, Node> = Shared::from(&*self.head as *const Node);

        // Relaxed ordering is sufficient - we only need approximate top level for search
        let mut level = self.top_level.load(AO::Relaxed);

        while level > 0 {
            let l = level - 1;

            // SAFETY: pred is valid (head or previously loaded node protected by guard)
            let pred_ref = unsafe { pred.deref() };
            let mut curr = pred_ref.forward[l].load(AO::Acquire, guard);

            // Advance while current key < target key
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

    /// Fast-path find for point queries (only computes level 0)
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
                    Ordering::Equal => return curr, // Early exit on exact match
                    Ordering::Greater => break,
                }
            }

            if l == 0 {
                return curr; // Return level 0 result
            }
            level -= 1;
        }
        Shared::null()
    }

    /// Check if the splice is valid for the given key
    /// Returns true if the splice brackets the key at all levels
    fn is_splice_valid<'g>(&self, key: &[u8], splice: &Splice<'g>, guard: &'g Guard) -> bool {
        if splice.height == 0 {
            return false;
        }

        // Check level 0 first (most important)
        let pred0 = unsafe { splice.preds[0].as_ref() };
        let succ0 = unsafe { splice.succs[0].as_ref() };

        // Check predecessor brackets key from below
        if let Some(pred_node) = pred0 {
            if std::ptr::eq(pred_node as *const Node, &*self.head as *const Node) {
                // Head is always valid predecessor
            } else if Self::cmp_key(&pred_node.key, key) >= Ordering::Equal {
                return false; // Predecessor is >= key, not valid
            }
        }

        // Check successor brackets key from above
        if let Some(succ_node) = succ0 {
            if Self::cmp_key(&succ_node.key, key) <= Ordering::Equal {
                return false; // Successor is <= key, not valid
            }
        }

        // Verify splice hasn't been invalidated by concurrent inserts
        if let Some(pred_node) = pred0 {
            let current_next = pred_node.forward[0].load(AO::Acquire, guard);
            if current_next != splice.succs[0] {
                return false; // Splice is stale
            }
        }

        true
    }

    /// Recompute splice levels starting from a valid base level
    #[allow(dead_code)]
    fn recompute_splice<'g>(
        &self,
        key: &[u8],
        guard: &'g Guard,
        splice: &mut Splice<'g>,
        recompute_from_level: usize,
    ) {
        let mut pred = if recompute_from_level == 0 {
            Shared::from(&*self.head as *const Node)
        } else {
            splice.preds[recompute_from_level - 1]
        };

        let mut level = self.top_level.load(AO::Relaxed).max(recompute_from_level);

        while level > recompute_from_level {
            level -= 1;
            let pred_ref = unsafe { pred.deref() };
            let mut curr = pred_ref.forward[level].load(AO::Acquire, guard);

            while let Some(curr_ref) = unsafe { curr.as_ref() } {
                match Self::cmp_key(&curr_ref.key, key) {
                    Ordering::Less => {
                        pred = curr;
                        curr = curr_ref.forward[level].load(AO::Acquire, guard);
                    }
                    _ => break,
                }
            }

            splice.preds[level] = pred;
            splice.succs[level] = curr;
        }
    }

    /// Get the visible value at or before snapshot_seq
    #[inline]
    pub fn get(&self, key: &[u8], snapshot_seq: u64) -> Option<Bytes> {
        let guard = &epoch::pin();
        let node_ptr = self.find_node(key, guard);

        if let Some(node) = unsafe { node_ptr.as_ref() } {
            if node.key.as_ref() == key {
                // Walk version chain to find visible version
                // Snapshot isolation: only see writes with seq < snapshot_seq
                let mut v = node.versions_head.load(AO::Acquire, guard);
                while let Some(vn) = unsafe { v.as_ref() } {
                    if vn.seq < snapshot_seq {
                        return vn.val.clone();
                    }
                    // Relaxed is safe: version nodes are immutable after publishing
                    v = vn.next.load(AO::Relaxed, guard);
                }
            }
        }
        None
    }
    /// Get visible value with expiration at or before snapshot_seq
    pub fn get_visible_with_exp(
        &self,
        key: &[u8],
        snapshot_seq: u64,
    ) -> Option<Option<(Bytes, Option<u64>)>> {
        let guard = &epoch::pin();
        let node_ptr = self.find_node(key, guard);

        if let Some(node) = unsafe { node_ptr.as_ref() } {
            if node.key.as_ref() == key {
                let mut v = node.versions_head.load(AO::Acquire, guard);
                while let Some(vn) = unsafe { v.as_ref() } {
                    // Snapshot isolation: only see writes with seq < snapshot_seq
                    // (strictly less than, not <=)
                    if vn.seq < snapshot_seq {
                        return Some(vn.val.clone().map(|val| (val, vn.exp)));
                    }
                    // Relaxed is safe: version nodes are immutable after publishing
                    v = vn.next.load(AO::Relaxed, guard);
                }
            }
        }
        None
    }

    /// Get all versions for merge resolution
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
                // Collect all visible versions (no unrolling here since we need all versions)
                let mut v = node.versions_head.load(AO::Acquire, guard);
                while let Some(vn) = unsafe { v.as_ref() } {
                    // Snapshot isolation: only see writes with seq < snapshot_seq
                    if vn.seq < snapshot_seq {
                        versions.push((vn.val.clone(), vn.exp, vn.op));
                    }
                    v = vn.next.load(AO::Relaxed, guard);
                }
            }
        }

        versions
    }

    /// Upsert with optional expiration and OpType (lock-free, linearizable)
    pub fn upsert_exp(
        &self,
        key: Bytes,
        value: Option<Bytes>,
        seq: u64,
        exp: Option<u64>,
        op: OpType,
    ) {
        // Use internal method without hint for public API
        let guard = &epoch::pin();
        self.upsert_exp_internal(key, value, seq, exp, op, None, guard);
    }

    /// Internal upsert with optional splice hint
    #[allow(clippy::too_many_arguments)]
    fn upsert_exp_internal<'g>(
        &self,
        key: Bytes,
        value: Option<Bytes>,
        seq: u64,
        exp: Option<u64>,
        op: OpType,
        hint: Option<&mut Splice<'g>>,
        guard: &'g Guard,
    ) {
        let mut preds: [Shared<Node>; MAX_LEVEL] = [Shared::null(); MAX_LEVEL];
        let mut succs: [Shared<Node>; MAX_LEVEL] = [Shared::null(); MAX_LEVEL];

        // Try to use hint if provided and valid
        let use_hint = if let Some(ref splice) = hint {
            if self.is_splice_valid(&key, splice, guard) {
                // Copy splice data to preds/succs
                preds.copy_from_slice(&splice.preds);
                succs.copy_from_slice(&splice.succs);
                true
            } else {
                // Splice invalid, fall back to normal find
                false
            }
        } else {
            false
        };

        if !use_hint {
            self.find(&key, guard, &mut preds, &mut succs);
        }

        // Case 1: Key exists - prepend new version to version chain
        if let Some(curr) = unsafe { succs[0].as_ref() } {
            if curr.key == key {
                // Retry loop for version CAS only (avoid expensive re-find)
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
                        Ok(_) => {
                            // Update hint if provided
                            if let Some(splice) = hint {
                                splice.preds.copy_from_slice(&preds);
                                splice.succs.copy_from_slice(&succs);
                                splice.height = MAX_LEVEL;
                            }
                            return; // Success!
                        }
                        Err(e) => {
                            // CAS failed - version chain changed; drop newly created node (never published)
                            drop(e.new);
                            continue;
                        }
                    }
                }
            }
        }

        // Case 2: Key absent - insert new node at random level
        let node_level = Self::random_level();

        // Raise top_level if needed
        let _ = self.top_level.fetch_max(node_level, AO::AcqRel);

        // Stage 1: Insert at level 0 (the linearization point)
        let new_ptr = loop {
            // Recompute window for level 0 to reduce chances of CAS fail
            self.find(&key, guard, &mut preds, &mut succs);
            let pred0 = unsafe { preds[0].deref() };
            let succ0 = succs[0];

            // Build new node with first version
            let first_ver = Owned::new(VersionNode::new(seq, value.clone(), exp, op));
            let new_node = Owned::new(Node::new(key.clone(), first_ver, node_level));
            let new_ptr = new_node.into_shared(guard);
            // Set level 0 forward pointer to current successor
            unsafe { new_ptr.deref() }.forward[0].store(succ0, AO::Relaxed);

            // Validate window and try to splice at level 0
            let pred_next0 = pred0.forward[0].load(AO::Acquire, guard);
            if pred_next0 != succ0 {
                // Window changed; discard this node and retry
                unsafe { guard.defer_destroy(new_ptr) };
                continue;
            }
            match pred0.forward[0].compare_exchange(succ0, new_ptr, AO::AcqRel, AO::Acquire, guard)
            {
                Ok(_) => break new_ptr, // level 0 inserted
                Err(_) => {
                    // CAS failed due to interference; discard node and retry
                    unsafe { guard.defer_destroy(new_ptr) };
                    continue;
                }
            }
        };

        // Stage 2: Best-effort link higher levels (1..node_level-1)
        for l in 1..node_level {
            loop {
                // Find current window at level l
                self.find(&key, guard, &mut preds, &mut succs);
                let pred = unsafe { preds[l].deref() };
                let succ = succs[l];
                // Set forward pointer for this level to successor
                unsafe { new_ptr.deref() }.forward[l].store(succ, AO::Relaxed);
                let pred_next = pred.forward[l].load(AO::Acquire, guard);
                if pred_next != succ {
                    // Window changed; retry
                    continue;
                }
                if pred.forward[l]
                    .compare_exchange(succ, new_ptr, AO::AcqRel, AO::Acquire, guard)
                    .is_ok()
                {
                    break; // linked this level
                }
                // Retry on CAS failure; it's okay to loop a few times
            }
        }

        // Update hint to point after the newly inserted node
        if let Some(splice) = hint {
            // Set pred to the newly inserted node for the next insert
            for (i, succ) in succs.iter().enumerate().take(node_level) {
                splice.preds[i] = new_ptr;
                splice.succs[i] = *succ;
            }
            splice.height = node_level;
        }

        // Inserted successfully
    }

    /// Insert or update with sequence number
    #[inline]
    pub fn upsert(&self, key: Bytes, value: Option<Bytes>, seq: u64) {
        self.upsert_exp(key, value, seq, None, OpType::Put);
    }

    /// Delete a key (insert tombstone)
    #[inline]
    pub fn delete(&self, key: Bytes, seq: u64) {
        self.upsert_exp(key, None, seq, None, OpType::Delete);
    }

    /// Range scan returning visible entries at snapshot_seq
    pub fn range_visible(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        snapshot_seq: u64,
    ) -> Vec<(Bytes, Bytes)> {
        let guard = &epoch::pin();

        // Find starting point
        let start_key = start.unwrap_or(&[]);
        let mut preds: [Shared<Node>; MAX_LEVEL] = [Shared::null(); MAX_LEVEL];
        let mut succs: [Shared<Node>; MAX_LEVEL] = [Shared::null(); MAX_LEVEL];
        self.find(start_key, guard, &mut preds, &mut succs);

        // Don't pre-allocate - let Vec grow naturally to avoid over-allocation
        let mut out = Vec::new();
        let mut curr = if start.is_none() {
            self.head.forward[0].load(AO::Acquire, guard)
        } else {
            succs[0]
        };

        while let Some(node) = unsafe { curr.as_ref() } {
            // Check end boundary
            if let Some(end_key) = end {
                if node.key.as_ref() >= end_key {
                    break;
                }
            }

            // Find first visible version
            // Snapshot isolation: only see writes with seq < snapshot_seq
            let mut v = node.versions_head.load(AO::Acquire, guard);
            while let Some(vn) = unsafe { v.as_ref() } {
                if vn.seq < snapshot_seq {
                    if let Some(ref val) = vn.val {
                        out.push((node.key.clone(), val.clone()));
                    }
                    break;
                }
                // Relaxed is safe: version nodes are immutable after publishing
                v = vn.next.load(AO::Relaxed, guard);
            }

            curr = node.forward[0].load(AO::Acquire, guard);
        }

        out
    }

    /// Get all tombstoned keys in range visible at snapshot
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

        // Pre-allocate with reasonable capacity
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

            // Snapshot isolation: only see writes with seq < snapshot_seq
            let mut v = node.versions_head.load(AO::Acquire, guard);
            while let Some(vn) = unsafe { v.as_ref() } {
                if vn.seq < snapshot_seq {
                    if vn.val.is_none() {
                        out.push(node.key.clone());
                    }
                    break;
                }
                // Relaxed is safe: version nodes are immutable after publishing
                v = vn.next.load(AO::Relaxed, guard);
            }

            curr = node.forward[0].load(AO::Acquire, guard);
        }

        out
    }

    /// Drain all entries with metadata and expiration (consumes skiplist logically)
    /// Note: In lock-free version, this creates a snapshot rather than destructive drain
    pub fn drain_with_meta_with_exp(&self) -> Vec<SkipListEntryWithExp> {
        let guard = &epoch::pin();
        // Pre-allocate with reasonable capacity for typical memtable sizes
        let mut out = Vec::with_capacity(256);

        let mut curr = self.head.forward[0].load(AO::Acquire, guard);

        while let Some(node) = unsafe { curr.as_ref() } {
            // Get all versions of this key (newest to oldest)
            // This is needed for merge resolution - we need all merge operands,
            // not just the most recent one
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
                vn_ptr = vn.next.load(AO::Acquire, guard);
            }

            curr = node.forward[0].load(AO::Acquire, guard);
        }

        out
    }

    /// Delete range by inserting tombstones for all keys in [start, end)
    pub fn delete_range(&self, start: Option<&[u8]>, end: Option<&[u8]>, seq: u64) -> usize {
        let guard = &epoch::pin();

        // Collect keys in range first
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

        // Insert tombstones for all keys (reuses the guard from collection phase)
        let mut changed = 0;
        for key in keys_to_delete {
            // Check if it had a value before
            // Note: get() will create its own guard, but that's acceptable for correctness
            if self.get(&key, seq).is_some() {
                changed += 1;
            }
            // Insert tombstone using existing guard by calling upsert_exp_with_guard
            self.delete(key, seq);
        }

        changed
    }

    /// Get all keys currently in the skiplist
    pub fn get_all_keys(&self) -> Vec<Bytes> {
        let guard = &epoch::pin();
        // Pre-allocate with reasonable capacity
        let mut keys = Vec::with_capacity(128);

        let mut curr = self.head.forward[0].load(AO::Acquire, guard);
        while let Some(node) = unsafe { curr.as_ref() } {
            keys.push(node.key.clone());
            curr = node.forward[0].load(AO::Acquire, guard);
        }

        keys
    }

    /// Range scan returning latest visible non-tombstone entries (no snapshot filtering)
    /// This is used by MemTable for current state queries
    pub fn range(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Vec<(Bytes, Bytes)> {
        self.range_visible(start, end, u64::MAX)
    }

    /// Get all tombstoned keys in range (no snapshot filtering)
    pub fn tombstones_range(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Vec<Bytes> {
        self.tombstones_range_visible(start, end, u64::MAX)
    }
}

impl Default for SkipList {
    fn default() -> Self {
        Self::new()
    }
}

// Safety: SkipListLf is Send + Sync because all operations use atomic operations
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

        // Assert
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
        assert!(entries[1].3); // is tombstone
        assert_eq!(entries[2].4, Some(12345)); // has expiration
    }
}
