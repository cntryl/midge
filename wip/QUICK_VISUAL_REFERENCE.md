# Quick Visual Reference: Porting Status

## Current State Dashboard

```
┌─────────────────────────────────────────────────────────────┐
│           MIDGE REFACTORING STATUS (Dec 10, 2025)           │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  Overall Porting:         ████░░░░░░  60%                   │
│  Data Layer:              █████████░  95% (SST, WAL, etc)    │
│  Runtime Framework:       ██████░░░░  60% (actors skeleton)  │
│  Control Plane:           ██░░░░░░░░  20% (missing handlers) │
│  API Surface:             ██░░░░░░░░  20% (stubs)            │
│                                                               │
├─────────────────────────────────────────────────────────────┤
│ Test Compilation: FAILING (74+ errors in engine_basic)      │
│ Basic CRUD:       BLOCKED (RuntimeMsg::Read not implemented) │
│ Est. to Core:     6-8 hours (4 critical path items)          │
└─────────────────────────────────────────────────────────────┘
```

---

## Component Dependency Graph

```
┌──────────────────────────────────────────────────────────────────┐
│                         ENGINE API                               │
│        (put, get, delete, scan, snapshot, batch, etc)            │
│                   src/engine/mod.rs                              │
└────────────────────────────┬─────────────────────────────────────┘
                             │
                    RuntimeHandle.send()
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
┌───────▼─────────┐  ┌───────▼─────────┐  ┌─────▼────────────┐
│  RuntimeMsg     │  │ Event Loop      │  │ RuntimeState     │
│  - WalAppend    │  │ - Dispatches    │  │ - CFs            │
│  - Read         │  │ - Calls actors  │  │ - Memtables      │
│  - Flush*       │  │ - Updates state │  │ - Manifest       │
│  - Compact*     │  │                 │  │ - Sequences      │
│  - etc          │  │                 │  │                  │
└─────────────────┘  └─────────────────┘  └──────────────────┘
                             │
        ┌────────────────────┼────────────────────────┬──────────┐
        │                    │                        │          │
    ┌───▼────────┐  ┌────────▼─────────┐  ┌─────────▼────┐  ┌──▼───┐
    │ WALActor   │  │  FlushActor      │  │CompactionActor   │Cloud│
    │ - append   │  │ - freeze memtbl  │  │ - merge SSTs     │Actor│
    │ - sync     │  │ - write SST      │  │ - pick levels    │     │
    └────────────┘  │ - update manifest│  └──────────────────┘  └────┘
                    └──────────────────┘
                             │
        ┌────────────────────┼─────────────────────────┐
        │                    │                         │
    ┌───▼────────┐  ┌────────▼─────────┐  ┌──────────▼───┐
    │ Memtable   │  │ SST (on disk)    │  │  Manifest    │
    │ (skiplist) │  │ (fs-backed)      │  │  (metadata)  │
    │ 95% done   │  │ 95% done         │  │  70% done    │
    └────────────┘  └──────────────────┘  └──────────────┘
```

---

## Implementation Priority Grid

```
         ┌─ QUICK (< 1h) ─────┬─ MEDIUM (1-3h) ──────┬─ HARD (3h+) ──┐
         │                    │                      │               │
CRITICAL │ • Signatures       │ • RuntimeMsg::Read   │               │
(Week 1) │ • open_with_opts   │ • CF creation        │               │
         │                    │                      │               │
────────┼────────────────────┼──────────────────────┼───────────────┤
         │                    │ • WriteBatch         │ • Iterator    │
SUPPORT  │                    │ • Snapshots          │   (MergeIter) │
(Week 2) │                    │ • Delete range       │ • Manifest    │
         │                    │                      │   integration │
────────┼────────────────────┼──────────────────────┼───────────────┤
         │                    │                      │ • Transactions│
DEFERRED │                    │ • Compaction sched   │ • Merge ops   │
(Later)  │                    │   (non-critical)     │ • Cloud       │
         │                    │                      │ • TTL/Filters │
         └────────────────────┴──────────────────────┴───────────────┘
```

---

## Read Path Flow (Currently Broken)

```
┌─────────────────────┐
│  Engine.get(key)    │
└──────────┬──────────┘
           │
           ├─ Check LOCAL memtable ✓
           │  (fast, in-process)
           │
           └─ If not found, send RuntimeMsg::Read
              │
              ├─ RuntimeHandle.send_and_wait()
              │  (blocks caller)
              │
              └─ Event Loop receives Read
                 │
                 ├─ ⚠️ NO HANDLER IMPLEMENTED ⚠️
                 │  (message is ignored!)
                 │
                 └─ Timeout / Wrong response
                    │
                    └─ Engine returns error
```

**After implementing handler**:

```
              Event Loop receives Read { cf_id, key, seq }
                 │
                 ├─ Check CF state active memtable
                 │
                 ├─ Check CF immutable memtables
                 │
                 ├─ Query manifest for SSTs
                 │  (if any found:)
                 │
                 └─ Open SST reader, search key
                    │
                    └─ Return ReadValue(Some/None)
                       │
                       └─ Engine.get() returns result ✓
```

---

## Test Failure Categories (from build output)

```
COMPILATION ERRORS BY CATEGORY:
┌────────────────────────────────────────┐
│ 1. Signature mismatches (35%)          │
│    engine.put(&cf, ...) ← tests expect
│    engine.put(...) ← we provide        │
│                                        │
│ 2. Method not found (25%)              │
│    .scan_streaming() - not implemented │
│    .get_snapshot() - not implemented   │
│    .iterator() - not implemented       │
│                                        │
│ 3. Type mismatches (20%)               │
│    Query vs &Query                     │
│    PathBuf vs MidgeOptions             │
│                                        │
│ 4. Missing variants (15%)              │
│    RuntimeResponse types               │
│    RuntimeMsg types                    │
│                                        │
│ 5. Missing handlers (5%)               │
│    RuntimeMsg::Read not matched        │
└────────────────────────────────────────┘

PRIORITY OF FIXES:
  1. Signatures → unblocks 90+ errors
  2. Methods → unblocks 30+ errors
  3. RuntimeMsg::Read → unblocks 50+ errors
  4. Responses → unblocks 15+ errors
```

---

## Code Map: Where to Edit

```
To enable put/get/delete (Critical Path):
  ├─ src/engine/mod.rs
  │  ├─ Line 110: open_with_options() - expose it
  │  ├─ Line 130: put() - check signature
  │  ├─ Line 150: get() - check signature
  │  └─ Line 170: delete() - check signature
  │
  └─ src/runtime/event_loop.rs
     ├─ Line 250: Add RuntimeMsg::Read handler
     ├─ Line 300: Add ManifestCreateColumnFamily handler
     └─ Line 350: Add other message handlers

To enable batch/snapshot (Supporting):
  ├─ src/engine/api/write_batch.rs
  │  └─ Define WriteBatch struct
  │
  ├─ src/engine/api/snapshot.rs
  │  └─ Define Snapshot struct
  │
  └─ src/engine/mod.rs
     ├─ write_batch() method
     ├─ get_snapshot() method
     └─ get_at_snapshot() method

To enable ranges (Supporting):
  ├─ src/engine/api/iterator.rs
  │  └─ Define Iterator, IteratorBuilder
  │
  ├─ src/iterators/merge.rs
  │  └─ Implement MergeIterator
  │
  └─ src/engine/mod.rs
     └─ range_cf() method

To enable recovery (Supporting):
  └─ src/runtime/actors/manifest.rs
     ├─ handle_add_sst()
     └─ handle_remove_sst()
```

---

## Test Execution Strategy

```
PHASE 1: Get it to compile (Critical Path)
  └─ cd d:\repos\cntryl\midge
     cargo build --tests
     [Should show <50 errors, down from 500+]

PHASE 2: Basic CRUD tests pass (Critical Path)
  └─ cargo test engine_basic \
       should_get_value_given_existing_key_when_put \
       -- --nocapture
     [Should pass]

PHASE 3: All basic tests pass (Critical Path)
  └─ cargo test engine_basic -- --nocapture
     [Should see ~50 tests pass]

PHASE 4: Batch tests pass (Supporting)
  └─ cargo test engine_basic -- --nocapture \
       --skip should_
     [Batch tests should pass]

PHASE 5: Range tests compile (Supporting)
  └─ cargo build --tests
     [engine_iterators.rs should have <10 errors]

PHASE 6: Range tests pass (Supporting)
  └─ cargo test engine_iterators -- --nocapture
     [Range tests should pass]

PHASE 7: Snapshot tests pass (Supporting)
  └─ cargo test engine_snapshots -- --nocapture
     [Snapshot tests should pass]
```

---

## Key Metrics

```
LINES OF CODE TO WRITE:
  Critical Path:  ~200 lines
  Supporting:     ~400 lines
  ────────────────────────
  Total:          ~600 lines

HOURS TO COMPLETE:
  Critical Path:   4-6 hours (easy + straightforward)
  Supporting 1-3:  4-6 hours (medium complexity)
  Supporting 4-5:  3-4 hours (integration)
  ────────────────────────
  Total:           11-16 hours
  
  With testing/debugging: 14-20 hours

FILES TO MODIFY:
  Critical:        5 files (engine, event_loop, actors)
  Supporting:      12 files (api, iterators, metadata)
  ────────────────────────
  Total:           17 files

TESTS AFFECTED:
  Total test functions:    ~700
  Currently failing:       ~600
  Will pass (critical):    ~50
  Will pass (supporting):  ~200
  Deferred (transactions): ~100
```

---

## Success Indicators

✅ **Compilation passes**: `cargo build --tests` returns 0 errors
✅ **Basic CRUD works**: 50+ engine_basic tests pass
✅ **Batch is fast**: 100k write batch takes <100ms
✅ **Snapshots work**: Concurrent reads don't interfere
✅ **Ranges work**: Iterator covers all keys in range
✅ **No warnings**: `cargo clippy` finds no issues
✅ **Tests are fast**: Full suite runs <5 seconds

---

## Risk Assessment

```
HIGH RISK:
  • Sequence number sync between engine & runtime
  • Memtable visibility (local vs runtime)
  • SST reader resource leaks in iterator
  • Manifest getting out of sync with actual files

MEDIUM RISK:
  • Read blocking on runtime thread
  • Concurrent access to RuntimeState
  • Column family deletion race conditions

LOW RISK:
  • Basic message passing (well-tested pattern)
  • Existing SST/WAL code (already proven)
  • Error handling (already comprehensive)
```

---

## Quick Decision Tree

```
"Why aren't tests compiling?"
  ├─ "Too many errors"
  │  └─ Fix engine signatures (put, get, delete)
  │     └─ Unblocks 90+ errors
  │
  ├─ "engine::open_with_options not found"
  │  └─ Make it public in src/engine/mod.rs
  │     └─ Unblocks test setup
  │
  └─ "RuntimeMsg::Read not matched"
     └─ Add handler in event_loop.rs
        └─ Unblocks all read tests

"Why don't basic tests pass?"
  ├─ "get() returns None"
  │  └─ RuntimeMsg::Read handler not working
  │     ├─ Check: are memtables populated after put?
  │     ├─ Check: does manifest have SSTs?
  │     └─ Trace: add logging to handler
  │
  ├─ "put() hangs"
  │  └─ RuntimeHandle blocking
  │     ├─ Check: is runtime thread running?
  │     ├─ Check: is WALActor processing?
  │     └─ Trace: enable tracing in event_loop
  │
  └─ "Random failures"
     └─ Sequence number issue
        ├─ Check: engine.sequence vs state.sequence
        ├─ Check: WALActor updates both?
        └─ Add atomic operations

"Why are range tests failing?"
  ├─ "Iterator empty"
  │  └─ Memtables not being iterated
  │
  ├─ "Sequence filtering wrong"
  │  └─ Snapshot not being respected
  │
  └─ "Performance terrible"
     └─ Opening SST readers repeatedly
        └─ Add reader caching

"What should I work on next?"
  ├─ If compilation is blocking: Fix signatures
  ├─ If put/get failing: Implement RuntimeMsg::Read
  ├─ If batch slow: Implement WriteBatch
  ├─ If snapshots needed: Add Snapshot support
  ├─ If ranges needed: Implement Iterator
  └─ If recovery needed: Integrate Manifest
```

---

## Document Index

| Document | Purpose | Audience |
|----------|---------|----------|
| **ANALYSIS_SUMMARY.md** | High-level overview of what's ported vs missing | Project leads, architects |
| **PORTING_PLAN.md** | Detailed breakdown by module with complexity estimates | Developers, tech leads |
| **IMPLEMENTATION_DETAILS.md** | Code snippets, implementation guides, exact changes | Developers implementing |
| **IMPLEMENTATION_CHECKLIST.md** | Task-level checklist with validation steps | Developers, QA |
| **QUICK_VISUAL_REFERENCE.md** | This file - dashboards, diagrams, quick lookup | Everyone |

