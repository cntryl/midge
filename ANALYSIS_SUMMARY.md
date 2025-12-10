# Porting Analysis Summary

**Date**: December 10, 2025  
**Analysis of**: Midge LSM-tree engine refactoring (src_old/ → src/)  
**Goal**: Enable basic KV store (put/get/delete) functionality via actor-based runtime  

---

## Quick Summary

**Status**: The new actor-based runtime framework (src/runtime/) is 60% complete. Most data structure layers (SST, WAL, memtable, compaction) have been ported. **What's missing is the control plane** - the glue that connects engine API calls to runtime actors.

**Blockers for test compilation**:
1. Engine methods have wrong signatures (tests expect old API)
2. RuntimeMsg::Read handler not implemented (no reads work)
3. open_with_options() not exposed (can't set storage mode in tests)
4. No column family creation support (CF tests can't run)

**Estimate to basic functionality**: 6-8 hours of focused work across 4 critical path items

**Tests that will pass after critical path**:
- ✅ `engine_basic::should_get_value_given_existing_key_when_put`
- ✅ `engine_basic::should_return_none_given_nonexistent_key_when_get`
- ✅ `engine_basic::should_overwrite_value_given_existing_key_when_put`
- ✅ 60+ similar basic CRUD tests in `engine_basic.rs`

---

## Architecture Overview

### Old (src_old/)
```
Monolithic Engine
├── api/ (public traits: KvStore, Mutation, WriteOptions)
├── core/ (MidgeEngine impl + state management)
├── compaction/ (picker, executor, levels)
├── wal/ (write-ahead log)
├── sst/ (sorted string tables)
└── manifest/ (file metadata)
```

### New (src/)
```
Actor-Based Runtime
├── engine/ (thin façade over runtime)
│   ├── api/ (KvStore trait reimplemented)
│   └── mod.rs (MidgeEngine → delegates to RuntimeHandle)
├── runtime/ (message-passing based)
│   ├── event_loop.rs (main thread, dispatches messages to actors)
│   ├── actors/ (stateless handlers: flush, compaction, WAL, cloud, GC, manifest)
│   ├── state.rs (centralized mutable state)
│   ├── mod.rs (message types, RuntimeHandle)
│   └── scheduler.rs (prioritizes work)
├── compaction/ (executor + merge logic, reused from old)
├── wal/ (write-ahead log, mostly reused)
├── sst/ (SST format + memtable, mostly reused)
└── metadata/ (manifest, partially reused)
```

### Key Difference
- **Old**: Engine directly calls storage modules in-process
- **New**: Engine sends messages to runtime; runtime processes them via actors in background thread

**Example: Put operation**
```
Old Flow:
  engine.put(key, value) 
    → MemtableWrite
    → WALAppend
    → Direct return to caller

New Flow:
  engine.put(key, value)
    → engine writes to LOCAL memtable
    → engine sends RuntimeMsg::WalAppend to runtime thread
    → runtime thread's WAL actor appends to WAL
    → runtime thread updates runtime's memtable
```

---

## What's Already Ported (Don't Redo)

| Component | Location | Status | Notes |
|-----------|----------|--------|-------|
| SST Format & Encoding | src/sst/ | ✅ Complete | File format unchanged, readers/writers work |
| SST Filesystem Impl | src/sst/fs/ | ✅ Complete | FsSstFactory, SstReader, SstWriter |
| Memtable (SkipList) | src/sst/ | ✅ Complete | Lock-free skiplist, concurrent safe |
| Bloom Filters | src/sst/bloom/ | ✅ Complete | Negative filter for reads |
| Block Caching | src/sst/cache/ | ✅ Complete | LRU cache for decompressed blocks |
| Compression | src/sst/ | ✅ Complete | Block-level compression (snappy, lz4, etc) |
| WAL Format & Encoding | src/wal/ | ✅ Complete | Segment-based WAL, backwards compatible |
| WAL Filesystem Backend | src/wal/fs/ | ✅ Complete | FsWalFactory, readers/writers |
| Compaction Logic | src/compaction/ | ✅ Complete | Merge iterator, executor, strategy |
| Error Types | src/common/error.rs | ✅ Complete | MidgeError, MidgeResult types |
| Metadata/Manifest | src/metadata/ | 🟡 Partial | Structure exists, needs runtime integration |
| Runtime Framework | src/runtime/ | 🟡 Partial | Event loop skeleton done, actors are ~70% done |

---

## What Needs to be Built

### Critical (blocking test compilation)

| Item | What's Missing | LOC | Priority |
|------|---|---|---|
| **RuntimeMsg::Read Handler** | Event loop doesn't match on Read messages | 80-120 | P0 |
| **Engine API Fixes** | Method signatures don't match tests | 10-30 | P0 |
| **open_with_options()** | Storage mode setup not exposed | 5-15 | P0 |
| **Column Family Creation** | No way to create CFs at runtime | 40-60 | P0 |

### Supporting (needed for 80%+ test pass rate)

| Item | What's Missing | LOC | Priority |
|------|---|---|---|
| **WriteBatch** | Struct + engine method for batched writes | 60-80 | P1 |
| **Snapshots** | Sequence capture + filtering by sequence | 60-100 | P1 |
| **Iterator/Range Scan** | MergeIterator + sequence filtering | 150-250 | P1 |
| **Delete Range Opt** | Range tombstones instead of individual deletes | 50-80 | P2 |
| **Manifest Integration** | Populate & query manifest in runtime | 80-120 | P2 |

### Deferred (advanced features, not needed for core tests)

| Item | Why Deferred | Est. Work |
|------|---|---|
| Transactions | Requires full MVCC, isolation levels | 300+ LOC |
| Merge Operators | Specialized feature, few tests | 150-200 LOC |
| Compaction Scheduling | Works without; only matters for sustained load | 120-180 LOC |
| Cloud Backends | Works with local FS; cloud is optimization | 200-500+ LOC per provider |
| Eviction Actor | Works without; SBA is memory optimization | 50-150 LOC |
| TTL/Filters | Advanced feature, compaction already supports it | 100-150 LOC |

---

## Known Issues & Solutions

### Issue 1: Local vs Runtime Memtables Out of Sync
**Problem**: Engine writes to local memtable immediately, but runtime memtable updates async via WAL actor

**Current Status**: ✅ Already handled (WalActor line 56-60 updates runtime memtable)

**Risk**: If WAL append fails, local != runtime

**Mitigation**: Add acknowledgment from WAL actor before returning to caller

---

### Issue 2: Read Path Blocking on Runtime Messages
**Problem**: Engine.get() calls runtime_handle.send_and_wait(), which blocks until response

**Current Status**: 🟡 Works but serial; no parallelism

**Risk**: Slow under high concurrency

**Mitigation**: For now, acceptable. Later: add read thread pool or async/await

---

### Issue 3: Manifest Persistence
**Problem**: Manifest stored in-memory; lost on restart

**Current Status**: 🟡 Not implemented

**Risk**: Can't recover SST list after crash

**Mitigation**: Implement in Phase 4 (manifest actor persists manifest file)

---

### Issue 4: Compaction not Triggered
**Problem**: CompactionActor exists but scheduler never calls it

**Current Status**: ⚠️ Not implemented

**Risk**: Memtable & L0 SST growth unbounded in sustained load tests

**Mitigation**: Deferred; implement scheduler in Phase 5

---

## Test Coverage Summary

### Current Compilation Status

| Test File | Errors | Blockers |
|-----------|--------|----------|
| engine_basic.rs | 74 | RuntimeMsg::Read, API fixes |
| engine_iterators.rs | 90 | API fixes, Iterator not impl, Read |
| engine_snapshots.rs | 88 | API fixes, Snapshot not impl, Read |
| engine_concurrent_* | 60+ | Same + thread safety |
| cloud_* | 40+ | CloudActor, cloud backends |
| compaction_* | 20+ | Compaction scheduling |
| checkpoint.rs | 30+ | Checkpoint/recovery not impl |

### After Critical Path Implementation

**Expected to compile**:
- ✅ engine_basic.rs (100%)
- ✅ config_validation.rs (100%)
- 🟡 engine_iterators.rs (60% - needs Iterator)
- 🟡 engine_snapshots.rs (40% - needs Snapshot)

**Expected to pass**:
- ✅ engine_basic::put/get/delete tests (~50 tests)
- 🟡 engine_basic::insert tests (20 tests, depends on get)
- ⚠️ All others blocked on supporting items

---

## Implementation Dependencies

```
open_with_options()
  ├── (no deps)
  └── unblocks: test setup

Engine API signatures
  ├── (no deps)
  └── unblocks: compilation

RuntimeMsg::Read handler
  ├── depends on: column families exist
  ├── depends on: memtable populated (via WalActor)
  └── unblocks: engine.get(), reads

Column family creation
  ├── depends on: RuntimeMsg::Read works (to verify CF)
  └── unblocks: CF-specific tests

WriteBatch
  ├── depends on: RuntimeMsg::Read works
  └── enables: faster multi-write tests

Snapshots
  ├── depends on: sequence filtering in readers
  ├── depends on: Write Batch (for batch tests)
  └── unblocks: snapshot isolation tests

Iterator
  ├── depends on: MergeIterator skeleton
  ├── depends on: Snapshots (for sequence filtering)
  └── unblocks: range scan tests

Delete Range
  ├── depends on: RangeTombstone type (exists)
  ├── depends on: Compaction respects tombstones (probably done)
  └── unblocks: range deletion tests

Manifest Integration
  ├── depends on: Column family creation
  ├── depends on: SST file creation (flush actor)
  └── unblocks: restart/recovery tests
```

---

## Recommended Implementation Order

### Week 1 (Phase 1-2: Critical Path + Basic Supporting)
```
Day 1-2: 
  - Fix engine API signatures (1h)
  - Expose open_with_options (0.5h)
  - Implement RuntimeMsg::Read handler (3h)
  → engine_basic tests compile & 70% pass

Day 3-4:
  - Add column family creation (1h)
  - Implement WriteBatch (2h)
  → basic put/get/delete fully working, batch tests pass

Day 5:
  - Snapshots support (2h)
  - Initial Iterator skeleton (3h)
  → snapshot isolation tests compile
```

### Week 2 (Phase 3-4: Advanced Supporting + Metadata)
```
Day 6-7:
  - Complete Iterator/MergeIterator (4h)
  - Delete range optimization (1.5h)
  → range scan tests pass

Day 8-9:
  - Manifest integration (3h)
  - WAL recovery path (2h)
  → restart/durability tests pass

Day 10:
  - Cleanup, testing, documentation
```

---

## Success Criteria

| Milestone | Tests Passing | Timeline |
|-----------|---|---|
| Compilation | 0 (build fails) | Today |
| Critical path done | 50 basic CRUD | +4 hours |
| Supporting items 1-3 | 80 tests (CRUD + batch) | +8 hours |
| Full supporting items | 150+ tests (include ranges) | +14 hours |
| Metadata complete | 180+ tests (include recovery) | +18 hours |

---

## Resources

- **Dependency Analysis**: docs/DEPENDENCY_ANALYSIS.md
- **Architecture Docs**: docs/
- **Benchmark Patterns**: benches/criterion_helper.rs
- **Test Conventions**: See copilot-instructions.md

---

## Quick Reference: File Locations

**Engine (API Surface)**:
- `src/engine/mod.rs` - Main MidgeEngine struct
- `src/engine/api/` - KvStore trait, Iterator, Snapshot, Transaction
- `src/engine/open.rs` - open_engine() helper

**Runtime (Message Processing)**:
- `src/runtime/mod.rs` - RuntimeMsg, RuntimeResponse, RuntimeHandle
- `src/runtime/event_loop.rs` - Main loop that handles messages
- `src/runtime/actors/` - Specific actor implementations
- `src/runtime/state.rs` - Centralized mutable state

**Data Layer (Mostly Complete)**:
- `src/sst/` - SST format, memtable, compression, bloom
- `src/wal/` - Write-ahead log
- `src/compaction/` - Merge logic, executor
- `src/metadata/` - Manifest (needs integration)

**Tests (What's Failing)**:
- `tests/engine_basic.rs` - Basic CRUD operations
- `tests/engine_iterators.rs` - Range scans
- `tests/engine_snapshots.rs` - MVCC snapshots
- All others depend on above

