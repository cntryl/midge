Here is a clean, ready-to-drop `docs/INVARIANTS.md` file — polished, structured, and aligned to how real storage engines (Pebble, RocksDB, FoundationDB) document invariants.

You can paste this directly into your repo.

# **INVARIANTS.md**

# Midge Storage Engine – Correctness Invariants

_Authoritative list of all structural, behavioral, and durability guarantees_

This document defines the invariants that **must always hold true** for the Midge LSM storage engine across all operations: writes, reads, flush, compaction, WAL replay, crash recovery, cloud/hybrid modes, and multi-CF isolation.

These invariants are the foundation for testing, fuzzing, recovery logic, and long-term evolution of the engine.

# **1. LSM Tree Structural Invariants**

### **1.1 Sortedness & Key Range Ordering**

- SST files in each level (L1+) must have **strictly non-overlapping key ranges**.
- Within an SST, keys must be **strictly sorted** by internal key (user key + sequence).
- L0 files may overlap but must be **sorted by creation order**.

### **1.2 SST Metadata Consistency**

- Smallest key ≤ largest key for every SST.
- File metadata (smallest/largest key, seqno bounds, range delete bounds) must match the file contents.
- Every file in the manifest must physically exist on disk.

### **1.3 Orphan File Invariant**

- No SST may exist on disk that is not referenced in the manifest.

# **2. WAL / Manifest / SST Synchronization**

### **2.1 Manifest–WAL Ordering**

- The manifest must never reference data that has not been durably written to the WAL.
- WAL replay must deterministically rebuild the same memtable contents that existed pre-crash.

### **2.2 WAL Integrity**

- WAL tail is either valid or zeroed; partial TLV entries are detected as corruption.
- WAL segments are replayed **at most once**.

### **2.3 Atomicity of Update Sets**

- SST creation + manifest edit must be atomic across crash boundaries.
- No partial manifest edit may become externally visible.

# **3. Memtable and Flush Invariants**

### **3.1 Memtable Ordering**

- Keys in the memtable skiplist are strictly ordered.
- Frozen memtables never accept new writes.

### **3.2 Flush Invariants**

- Flush reads each immutable memtable exactly once.
- Flush produces a complete, valid SST or nothing (atomic visibility).
- Memtable rollover must not lose in-flight writes.

# **4. Iterator & Read-Path Invariants**

- Iterators must respect global sorted order across memtable + SSTs.
- Seek operations must land on the correct block and internal key.
- Range delete and point delete semantics must match RocksDB/Pebble:

  - point deletes shadow earlier puts
  - range deletes shadow all keys in the span

- Iterator behavior must remain consistent during compaction.

# **5. Range Delete (Tombstone) Invariants**

- Range tombstones must overshadow all keys in their span regardless of SST/memtable boundary.
- Compaction must rewrite range tombstones without shrinking their coverage.
- Range deletes must survive crash and replay.
- Snapshots see tombstones correctly according to snapshot sequence number.

# **6. Multi–Column-Family (CF) Invariants**

- CFs never share keyspaces or sequence numbering.
- CF flush ordering must match global WAL commit ordering.
- CF drops affect only that CF and must not corrupt others.
- Compaction in one CF must not leak or eliminate keys in another.

# **7. Snapshot / MVCC Invariants**

### **7.1 Visibility**

- Snapshot view is monotonic and consistent.
- Snapshots maintain visibility across flush and compaction.
- Snapshots pin SSTs they depend on.

### **7.2 Range Deletes with Snapshots**

- Range deletes must not hide keys that are visible to a snapshot taken before the delete.

# **8. Compaction Invariants**

- Compaction must preserve last-write-wins semantics.
- Compaction must not resurrect deleted keys.
- Compaction outputs must be fully written before manifest edits.
- Output SST metadata must reflect actual file contents.
- Compaction can be safely interrupted and retried after a crash.

# **9. Cloud / Hybrid Storage Invariants**

### **9.1 Integrity**

- Local and remote SST hashes must match.
- Partial upload must never form a valid readable SST.
- Cloud listing + manifest reconciliation must converge deterministically.

### **9.2 Atomicity**

- A file must not appear in the manifest until its remote upload is complete.
- Remote index blocks must be validated before use.

### **9.3 Recovery**

- Corrupted or incomplete cloud SSTs must not be treated as valid inputs during rebuild.
- Remote tier must not allow divergent versions of the same file.

# **10. Durability & Crash Consistency**

- Committed writes must never be lost after crash.
- Uncommitted writes must never appear after crash.
- Crash during flush must produce either:

  - a fully visible SST, or
  - no SST at all

- Crash during manifest write must roll back to a previous valid version.
- Crash during compaction must not leak intermediate files.

# **11. Transactions / MVCC**

- Each transaction reads from a consistent snapshot.
- Conflicts are resolved deterministically.
- Transaction commit is atomic with WAL record.
- Transaction abort leaves no partial state.
- Large transactions that spill to disk must recover atomically.

# **12. TTL / Expiration Invariants**

- Expired keys behave as tombstones during compaction.
- Expired keys must not violate snapshot visibility.
- Expiration does not shrink range tombstones beyond correctness.
- Expiration cleanup must not delete SSTs still visible to active snapshots.

# **13. Global Recovery Invariants**

- Rebuilding from SSTs must produce a manifest identical to or compatible with the previous state.
- Manifest rebuild must not introduce overlapping key ranges.
- Hybrid tier must reconcile remote and local files without divergence.
- Recovery must detect and reject irreconcilable corruption states.

# **14. Performance-Safety Invariants**

_(Not functional correctness but protect against runaway behavior)_

- Compaction must eventually make progress (no compaction starvation).
- Memtable rollover must not stall indefinitely due to backpressure.
- Autotuning must not violate configured safety bounds.
- Background work must not block foreground writes indefinitely.

# **Summary**

This invariant set defines the **contract** Midge must maintain across flush, compaction, WAL, multi-CF, snapshots, transactions, cloud storage, and crash recovery.

These invariants guide:

- test design
- fuzz targets
- correctness checks
- recovery logic
- compaction strategy
- cloud consistency rules

When all invariants are enforced by tests, Midge reaches **production-reliable** correctness and long-term maintainability.
