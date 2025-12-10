# Comprehensive Porting Plan: src_old/ → src/

**Status**: Analysis of what needs to be ported from the legacy architecture to make the new actor-based runtime fully functional.

**Goal**: Enable basic KV store functionality (put/get/delete) and pass integration tests.

---

## Executive Summary

The refactoring moved from a monolithic engine architecture (src_old/) to an actor-based runtime (src/runtime/) with a thin engine facade. **Most of the data structure layer (SST, WAL, memtable) has been preserved**, but the **control plane** needs critical infrastructure:

1. **Read path** is 80% stubbed (RuntimeMsg::Read handler not fully implemented)
2. **API surface** is incomplete (missing Iterator, Snapshot, Transaction)
3. **Column family management** exists but is disconnected from the new runtime
4. **Manifest/metadata** layer partially exists but needs integration with runtime actors

---

## CRITICAL PATH (Must implement first for basic put/get/delete)

### 1. **RuntimeMsg::Read Handler in EventLoop**
- **File**: `src/runtime/event_loop.rs` (around line 250+)
- **What's Missing**: The `RuntimeMsg::Read` case is not handled in the main match statement
- **Implementation**: Query local memtable, then immutable memtables, then SST files via manifest
- **Complexity**: **MEDIUM** (~100-150 lines)
- **Why Critical**: Blocks `engine.get_cf()` from working; every test calls this
- **Pseudo-code**:
  ```rust
  RuntimeMsg::Read { cf_id, key, sequence } => {
    // 1. Check local active memtable
    if let Some(value) = state.get_active_memtable(cf_id).get(&key)? {
      return Ok(Some(value));
    }
    // 2. Check immutable memtables (FIFO order)
    for immt in state.get_immutable_memtables(cf_id) {
      if let Some(value) = immt.get(&key)? {
        return Ok(Some(value));
      }
    }
    // 3. Query manifest for SST files at this sequence, open + search
    let ssts = state.manifest.get_ssts_for_cf(cf_id, sequence);
    for sst_name in ssts {
      // Open SST reader, binary search for key, return if found
    }
    return Ok(None);
  }
  ```

### 2. **Engine API Methods: put_cf, get_cf, delete_cf signatures**
- **Files**: `src/engine/mod.rs` (lines 130, 150, 170)
- **What's Wrong**: Current signatures take bare `key/value`, but tests pass `&ColumnFamilyHandle` as first arg
- **Fix**: The methods already exist correctly (accept cf handle), but tests expect older signature
  - OR: Tests need to NOT pass `&cf` to default column family methods
  - **Decision**: Tests should use `.put()` not `.put_cf(&cf, ...)` for default CF
- **Complexity**: **EASY** (signature review, potential test fixes)
- **Why Critical**: Compilation blocker for 90+ test functions

### 3. **MidgeEngine::open_with_options()**
- **File**: `src/engine/mod.rs` (around line 100)
- **What's Missing**: Method exists but signature is `open(PathBuf)`, needs `open_with_options(MidgeOptions)`
- **Current Code**: Already partially implemented (lines 110-120) but not exposed/tested
- **Complexity**: **EASY** (already done, just needs exposure)
- **Why Critical**: Tests use `MidgeOptions` to specify storage mode (Memory vs LocalDisk)

### 4. **Column Family Creation via Runtime**
- **File**: `src/runtime/event_loop.rs` + `src/runtime/actors/manifest.rs`
- **What's Missing**: `RuntimeMsg::ManifestCreateColumnFamily` handler
- **Implementation**: Manifest actor should update `RuntimeState::column_families` map
- **Complexity**: **EASY** (~30 lines in event loop, 20 in manifest actor)
- **Why Critical**: Without this, can't create non-default column families for tests

---

## SUPPORTING (Needed for more tests to pass)

### 5. **Write Batch Support**
- **Files**: `src/engine/api/write_batch.rs` (exists but empty), `src/engine/mod.rs`
- **What's Missing**: `WriteBatch` struct to collect multiple puts/deletes and send atomically
- **From src_old**: `src_old/api/write_batch.rs` has a complete implementation
- **Complexity**: **MEDIUM** (~80-120 lines)
  - WriteBatch struct to accumulate ops
  - Engine::write_batch() to apply them atomically
  - WAL appends all operations together
- **Why Important**: Many tests batch 100s of writes for efficiency

### 6. **Snapshot Support**
- **Files**: `src/engine/api/snapshot.rs` (empty), `src/engine/mod.rs`
- **What's Missing**: `Snapshot` struct that captures a sequence number for MVCC reads
- **From src_old**: `src_old/api/snapshot.rs` + read filtering by sequence
- **Complexity**: **MEDIUM** (~50-100 lines)
  - Capture sequence at snapshot time
  - Pass to all reads within snapshot context
  - Memtable/SST readers filter by sequence
- **Why Important**: Blocks snapshot-based tests and transaction isolation

### 7. **Iterator / Range Scan**
- **File**: `src/engine/api/iterator.rs` (skeleton exists), needs proper impl
- **What's Missing**: Full lazy-loading iterator over key range
- **Implementation**: Merge iterator over memtable(s) + SST iterators
- **Complexity**: **HARD** (~150-250 lines)
  - IteratorBuilder pattern
  - MergeIterator over multiple sources
  - Sequence filtering
  - Lazy buffering
- **Why Important**: All range scan tests fail; depends on #5 (snapshots)
- **Dependencies**: Needs SST reader iterator trait (likely exists in src/sst/fs)

### 8. **Delete Range Optimization**
- **File**: `src/engine/mod.rs` line 236
- **What's Missing**: Efficient delete_range (currently scans + deletes each key)
- **Better Approach**: Write range tombstone to WAL + memtable, merge into SSTs during compaction
- **Complexity**: **MEDIUM** (~80 lines)
  - RangeTombstone type (exists in `src/sst/types.rs`)
  - Encode/decode in WAL
  - Compaction merge respects tombstones (executor might already handle)
- **Why Important**: Performance blocker for large deletions; correctness for deletion tests

### 9. **Manifest Integration with Runtime**
- **Files**: `src/metadata/manifest.rs`, `src/runtime/actors/manifest.rs`
- **What's Missing**: Bidirectional sync between manifest and runtime state
- **Current State**: Manifest exists but mostly unused
- **Complexity**: **MEDIUM** (~100-150 lines)
  - Load manifest on recovery
  - Update manifest when SSTs are added/removed
  - Persist manifest atomically
- **Why Important**: Without this, SST metadata is lost on restart; compaction doesn't see files

---

## DEFERRED (Can come later, needed for advanced features)

### 10. **Transactions & MVCC**
- **Files**: `src/engine/api/transaction.rs` (skeleton), `src/runtime/state.rs`
- **What's Missing**: Transaction object, isolation levels, intent tracking
- **From src_old**: `src_old/api/transaction.rs` + `src_old/core/transaction/` (full MVCC with serializability)
- **Complexity**: **HARD** (300+ lines, complex locking + version tracking)
- **Why Deferred**: Blocks transactional tests but not basic CRUD
- **Dependencies**: Needs full snapshot + isolation level support

### 11. **Merge Operators**
- **Files**: `src/engine/api/` (no merge support yet)
- **What's Missing**: `engine.merge()` API, merge operator callbacks
- **From src_old**: `src_old/api/merge_operator.rs` + write path integration
- **Complexity**: **HARD** (~150-200 lines)
  - Merge operator trait/registry
  - WAL encoding for merges
  - Merge during read path and compaction
- **Why Deferred**: Specialized feature, only a few tests use it

### 12. **Compaction Scheduling & Triggering**
- **Files**: `src/runtime/scheduler.rs` (empty), `src/runtime/actors/compaction.rs`
- **What's Missing**: Background task scheduling for compaction checks
- **Current State**: Skeleton exists, no actual scheduling logic
- **Complexity**: **MEDIUM-HARD** (~120-180 lines)
  - Scheduler picks tasks by priority (flush > compaction > GC)
  - Timer-based compaction checks
  - Backpressure (pause writes if compaction can't keep up)
- **Why Deferred**: Not needed for basic put/get; matters for sustained load

### 13. **Cloud Integration (Full)**
- **Files**: `src/runtime/actors/cloud.rs`, `src/storage/cloud/`
- **What's Missing**: Actual cloud provider backends (S3, GCS, etc.)
- **Current State**: Stub actor that does nothing
- **Complexity**: **HARD** (varies by provider; 200-500+ lines per provider)
- **Why Deferred**: Works fine with local filesystem; cloud is optimization
- **Note**: Mock cloud already exists for tests

### 14. **Eviction Actor (Selective Buddy)**: 
- **Files**: `src/runtime/actors/eviction.rs`, `src/storage/hybrid/`
- **What's Missing**: Full SBA (Selective Buddy Allocation) integration
- **Current State**: Actor created but mostly stubbed
- **Complexity**: **HARD** (50-150 lines in eviction actor)
- **Why Deferred**: Works without SBA (uses all local disk); SBA is memory optimization

### 15. **Compaction Filters & TTL**
- **Files**: `src/sst/` (TTL type exists), compaction executor
- **What's Missing**: TTL enforcement during compaction, custom filters
- **From src_old**: `src_old/core/compaction/` has full TTL + filter support
- **Complexity**: **MEDIUM-HARD** (~100-150 lines)
  - Track key TTL during writes
  - Filter expired keys during merge/compact
  - Custom filter callbacks
- **Why Deferred**: Advanced feature, not needed for basic tests

---

## Priority Ordering Matrix

| Priority | Item | Category | Quick Win? | Blocks # Tests |
|----------|------|----------|-----------|----------------|
| 1 | RuntimeMsg::Read handler | Critical | No | 50+ |
| 2 | put_cf/get_cf signatures | Critical | Yes | 90+ |
| 3 | open_with_options() | Critical | Yes | 80+ |
| 4 | Column family creation | Critical | Yes | 30+ |
| 5 | Write batch | Supporting | No | 20+ |
| 6 | Snapshots | Supporting | No | 25+ |
| 7 | Iterator/Range scan | Supporting | No | 35+ |
| 8 | Delete range opt | Supporting | No | 8+ |
| 9 | Manifest integration | Supporting | No | 15+ |
| 10 | Transactions | Deferred | No | 12+ |
| 11 | Merge operators | Deferred | No | 5+ |
| 12 | Compaction scheduling | Deferred | No | 10+ |
| 13 | Cloud backends | Deferred | No | 2 |
| 14 | Eviction actor | Deferred | No | 3+ |
| 15 | Compaction filters | Deferred | No | 6+ |

---

## Implementation Sequence (Recommended)

### Phase 1: Core CRUD (Est. 3-4 hours)
1. Fix engine method signatures (put_cf/get_cf/delete_cf) → **EASY, unblocks compilation**
2. Expose open_with_options() → **EASY, unblocks test setup**
3. Implement RuntimeMsg::Read handler → **MEDIUM, enables get()**
4. Add column family creation message handler → **EASY, enables CF tests**

**After Phase 1**: `engine_basic.rs` tests for put/get/delete should compile and ~70% pass

### Phase 2: Batch & Snapshot (Est. 2-3 hours)
5. WriteBatch struct & engine method → **MEDIUM, speeds up tests**
6. Snapshot support (sequence capture) → **MEDIUM, enables isolation**

**After Phase 2**: `engine_snapshots.rs` compiles; batch tests pass

### Phase 3: Range Operations (Est. 2-3 hours)
7. Iterator/MergeIterator implementation → **HARD, needed for scans**
8. Delete range optimization → **MEDIUM, correctness + perf**

**After Phase 3**: `engine_iterators.rs` compiles; range tests pass; bench can run

### Phase 4: Metadata & Recovery (Est. 2-3 hours)
9. Manifest integration → **MEDIUM, needed for restart**
10. Recovery path (WAL replay) → **MEDIUM, needed for durability tests**

**After Phase 4**: Restart/recovery tests pass; engine survives shutdown

### Phase 5+: Advanced Features
- Transactions, merge operators, compaction scheduling, cloud, eviction
- Do these as time allows; tests will guide

---

## Key Architecture Notes

### What's Already Ported (Don't Re-implement)
- ✅ SST file format, encoding, readers/writers (src/sst/)
- ✅ WAL format, encoding, recovery basics (src/wal/)
- ✅ Memtable (skiplist) (src/sst/)
- ✅ Compaction logic (merge, executor) (src/compaction/)
- ✅ Error types, common utilities (src/common/)
- ✅ Actor framework skeleton (src/runtime/)
- ✅ Manifest structure (src/metadata/)

### Critical Integration Points
1. **Read path**: Engine → RuntimeHandle.send(Read) → EventLoop → MemtableRead + SSTRead
2. **Write path**: Engine → RuntimeHandle.send(WalAppend) → EventLoop → WALActor → local memtable
3. **Flush path**: Engine → RuntimeHandle.send(FlushMemtable) → EventLoop → FlushActor → SST + Manifest
4. **State**: RuntimeState is source of truth for all CFs, SSTs, WAL segments, sequences

### Testing Strategy
- Start with `tests/engine_basic.rs` (put/get/delete only)
- Expand to `tests/engine_iterators.rs` (ranges)
- Then `tests/engine_snapshots.rs` (MVCC)
- Concurrency tests can wait until core is solid

---

## Risk Areas

1. **Memtable visibility**: Writes go to local engine memtable AND runtime memtable (WAL append)
   - If these get out of sync, reads will fail
   - Solution: WALActor already does this (line 59 in wal.rs)

2. **Column family state**: Runtime owns CF state but engine has references
   - If CF dropped at runtime but engine holds handle, will panic
   - Solution: Make drop explicit, add validation

3. **Sequence numbering**: Multiple sources (engine, runtime, WAL) increment sequence
   - Could cause gaps or duplicates
   - Solution: Only RuntimeState increments sequence; engine reads current

4. **Manifest persistence**: Currently in-memory only
   - Restart loses all SST metadata
   - Solution: Implement manifest.persist() in manifest actor (Phase 4)

---

## Metrics for Success

- [ ] `cargo build --tests` passes
- [ ] `cargo test engine_basic` passes (core CRUD)
- [ ] `cargo test engine_iterators` passes (ranges)
- [ ] `cargo test engine_snapshots` passes (MVCC)
- [ ] No clippy warnings
- [ ] Tests run in <5 seconds
- [ ] Memory-only mode works (no disk I/O)
