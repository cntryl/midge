Here’s the clean, authoritative, implementation-agnostic summary of how a **world-class LSM merging iterator** must work — exactly what Pebble, RocksDB, and WiredTiger all do, reduced to the core rules.

This is the “gold standard” model you want Midge to follow.

---

# ⭐ **Midge Merging Iterator — Correctness & Performance Model**

A merging iterator provides a **unified, sorted, deduplicated, tombstone-aware stream of keys** from all LSM components:

```
Memtable (newest)
Immutable memtables
Level 0 SSTs (unordered)
Level 1..N SSTs (sorted)
```

It must behave *as if* the entire structure was flattened into a single sorted list.

## The rules come from the LSM tree invariants:

### **1. Newest wins**

Given multiple versions of the same key across sources or seq numbers:

* the one with **highest sequence number** is the visible version
* older versions are shadowed unless snapshot visibility requires them

### **2. Tombstones mask older data**

If the newest version of a key is a tombstone:

* **that key is deleted**
* older versions MUST NOT be returned
* unless needed for snapshot visibility

### **3. Keys must be returned in sorted user-key order**

For forward iteration:

```
user_key: lexicographically ascending  
for equal keys: irrelevant because dedup ensures only newest version is returned
```

For reverse iteration:

```
user_key: descending  
```

### **4. Iteration must interleave all sources**

The iterator merges all sources *without materializing everything*, so:

* at most **one entry per source** is in the heap
* the heap selects the next visible key in sorted order
* once an entry is emitted/retired, that source advances

This is the core of the streaming design.

---

# ⭐ **Internal Key Ordering (Absolute Requirement)**

Every modern LSM uses an **internal key order**:

```
(user_key ASC, seq DESC, value_type ASC)
```

This ordering guarantees that:

* newer versions of the same key are adjacent
* value beats delete for same seq
* the iterator can perform dedup + tombstone skipping with no extra lookups

Even if your memtable and SST iterators don’t expose internal keys directly,
**your merging iterator must behave as if they do.**

---

# ⭐ **Heap Ordering Semantics**

The merging iterator uses a binary heap (or winner tree) with ordering:

### For forward iteration:

1. **user_key ASC**
2. **seq DESC** (newest first)
3. **value before tombstone**
4. **source priority** (memtable > imm > L0 > L1+)

Because Rust's `BinaryHeap` is a max-heap, the comparator is inverted.

### For reverse iteration:

1. **user_key DESC**
2. **seq DESC** (still newest first)
3. **value before tombstone**
4. **same source priority**

Reverse iteration **does not invert seq order**.
Only user_key flips.

This is critical for correctness.

---

# ⭐ **Dedup Semantics**

The iterator must *never* return more than one version of a user key.

Procedure:

```
let last_user_key = Option<Bytes>;

loop:
    entry = heap.pop()
    if entry.user_key == last_user_key → skip
    else:
        last_user_key = entry.user_key
        if entry.value != tombstone:
            yield (user_key, value)
        else
            skip (key is deleted)
```

This ensures:

* newest version wins
* tombstones mask older content
* no HashSet needed
* O(1) key equality check
* highly cache-friendly

---

# ⭐ **Snapshot Visibility**

If you support snapshots:

```
snapshot_seq = highest sequence visible to reader
```

Then:

* when a key’s newest version has seq > snapshot_seq → it’s **invisible**
* the iterator must walk further down to find a visible version
* tombstones behave the same way: snapshot may “see” an older value

This means **dedup is not simply “first version wins”** if snapshot semantics are enabled.

---

# ⭐ **Source Advance Logic**

Whenever an entry is popped:

```
current = heap.pop()

advance only the source that produced 'current'
next_entry = source.next()
if next_entry exists:
    heap.push(next_entry)
```

This ensures minimal state and no prefetching overhead beyond one entry per source.

---

# ⭐ **Reverse Iteration Correctness**

Reverse iteration is **not** implemented by reversing everything.

Reverse rules:

* user_key order is inverted
* sequence number ordering is **not** inverted
* tombstones still mask older values
* dedup still works the same
* source priority stays the same

This produces a correct descending ordering of user keys with correct visibility.

---

# ⭐ **Optional Enhancements (World Class)**

The merging iterator can support:

### **Upper/lower key bounds**

To limit scanning.

### **Prefix iteration**

Used by secondary indexes, range scan optimizations.

### **Key sampling**

Useful for compaction picking and statistics.

### **Bloom filter skip**

Skipping entire SSTs when no keys match bounds/prefix.

### **“Seek” support**

For random access (important for Get(), not just scans).

---

# ⭐ **Putting it all together — One Clean Summary**

> **The Midge merging iterator merges all LSM components by maintaining the smallest (or largest for reverse) internal key in a heap, advancing only the source that produced the entry. It uses internal key ordering `(user_key ASC, seq DESC, type asc)` to ensure newest-wins visibility, masks tombstones, performs single-key dedup by tracking the last emitted user key, and supports both forward and reverse iteration without materializing the result set.**

This is the high-performance, correct, Pebble-class design.


