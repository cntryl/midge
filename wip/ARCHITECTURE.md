# Midge Engine Architecture: Actor Model & Deterministic Runtime

## Core Principles

Midge implements a **deterministic, actor-model storage engine** with the following principles:

1. **Single Executor**: All background work flows through EngineRuntime
2. **Determinism**: Same input manifest → same operation sequence → reproducible state
3. **Ownership**: Clear separation between data ownership and worker thread ownership
4. **Atomicity**: All state transitions happen within task execution
5. **Durability**: Cloud-first with optional local cache

---

## Component Overview

### 1. EngineRuntime: Central Coordinator

**Purpose**: Single-threaded executor for all background operations.

**Responsibilities**:
- Route flush operations through flush_coordinator
- Route compaction operations through compaction_controller  
- Route WAL syncs through wal_upload_coordinator
- Route cloud operations through cloud_coordinator
- Execute tasks in submission order (deterministic)

**Task Types** (RuntimeTaskKind enum):
```rust
pub enum RuntimeTaskKind {
    Flush,                    // Memtable flush to SST
    Compaction,              // SST rewriting/merging
    CompactionPlanExecution, // Actual compaction work
    Maintenance,             // Cache eviction, cloud ops, WAL pruning
    WalUpload,              // WAL segment uploads to cloud
}
```

**Key Methods**:
- `submit(task)`: Submit task for execution
- `submit_and_wait(task)`: Submit and block until done
- `shutdown()`: Gracefully stop all workers

---

### 2. Data Ownership Model

#### MidgeEngine Owns

- **Memtable Set**: Active memtables per column family (write path access)
- **Column Family Set**: Column family definitions and schemas
- **Manifest Cache**: Cached copy of current manifest (optimized reads)
- **Snapshot Registry**: References to active snapshots
- **Block Cache**: In-memory block cache (shared across all SSTs)
- **Transaction Manager**: MVCC transaction coordinator
- **WAL Coordinator**: Direct WAL read/write interface

#### EngineRuntime Owns

- **Flush Coordinator**: Manages memtable flush operations
- **Compaction Controller**: Manages compaction planning & execution
- **WAL Upload Coordinator**: Routes WAL syncs through runtime
- **Cloud Coordinator**: Routes cloud ops through runtime
- **Worker Threads**: All background worker thread handles

#### Shared (via Arc)

- **Version Set**: Lock-free manifest access (atomic updates)
- **CloudSstManager**: Cloud storage operations
- **HybridStorage**: Local cache + cloud tier
- **Metrics**: Performance metrics collection

---

### 3. Write Path

```
Application Call: put(cf, key, value)
    ↓
Check Read-Only Mode
    ↓
Check Background Error
    ↓
Acquire sequence number (atomic)
    ↓
Write to active memtable
    ↓
Append to WAL (synchronously via wal_coordinator)
    ↓
Return to application (durable in WAL)
    ↓
(Async in background via runtime tasks)
    ↓
Memtable Size Threshold Reached
    ↓
Call: engine.rollover_and_queue_flush()
    ↓
Submit RuntimeTask(Flush) to EngineRuntime
    ↓
Runtime Executor Runs Task
    ↓
FlushCoordinator::request_flush()
    ↓
Phase 5: Create segments from memtable entries
    ↓
Write SST to local disk
    ↓
Update Manifest Atomically
    ↓
Return from task
    ↓
(Async) spawn_cloud_upload()
    ↓
Upload SST to cloud
```

---

### 4. Read Path

```
Application Call: get(cf, key)
    ↓
Acquire snapshot (sequence number)
    ↓
Check memtables (newest first)
    ↓
  Found? → Return
    ↓
  Not found, check segments (Phase 5 integration)
    ↓
    For each segment:
      - Check bloom filter
      - If maybe_contains, read from segment
      - Segment handles cloud fallback if needed
    ↓
  Found? → Return
    ↓
  Not found, check SST set
    ↓
    For each SST level 0→max:
      - Check bloom filter
      - If maybe_contains, read from SST
      - SST manager handles cloud fallback if needed
      - Use block cache for hot blocks
    ↓
  Found? → Return
    ↓
Not found in any layer → Return None
```

---

### 5. Flush Lifecycle

```
Memtable State: Mutable (receiving writes)
    ↓
Size threshold reached (memtable_size_bytes)
    ↓
Rollover: Memtable → Sealed
    ↓
Submit RuntimeTask(Flush) to engine.rollover_and_queue_flush()
    ↓
Runtime Executor receives task
    ↓
FlushCoordinator::request_flush(job)
    ↓
process_flush_job():
  1. Drain memtable entries + range tombstones
  2. Create SST blob in memory
  3. Phase 5: Create segment from entries
  4. Write SST to local disk
  5. Manifest: Add SST to level 0, record segment metadata
  6. Update manifest cache
    ↓
Manifest updated atomically
    ↓
Task completes (runtime executor continues)
    ↓
(Async background thread via spawn_cloud_upload)
    ↓
  1. Read SST from disk
  2. Upload to cloud
  3. Retry on failure (exponential backoff)
    ↓
Cloud checkpoint updated when all pending SSTs uploaded
    ↓
WAL can be pruned (safe_sequence based on cloud checkpoint or last_persisted)
```

---

### 6. Compaction Lifecycle

```
Background Compaction Trigger:
  - Level 0: Too many files
  - Level N>0: Too much data
  - Manual: compact_level() / compact_range()
    ↓
CompactionController decides which files to compact
    ↓
Determinism: Same manifest → same decision (Phase 2 - deterministic compaction)
    ↓
Submit RuntimeTask(Compaction) to engine.compact_level() or engine.compact_range()
    ↓
Runtime Executor receives task
    ↓
CompactionController::compact_level() or compact_range()
    ↓
Create compaction plan:
  1. Read input SST files
  2. Cloud fallback if file not in local cache
  3. Merge entries (Phase 4 - unified write path)
  4. Write new SST
    ↓
Update manifest:
  1. Add new SST at appropriate level
  2. Mark old SSTs for deletion
  3. Update level boundaries
    ↓
Delete old SSTs:
  1. Remove from local disk
  2. Mark for deletion in cloud (async)
    ↓
(Async) Cloud deletion via HybridStorage
```

---

### 7. Determinism Guarantees

**What's Deterministic**:

1. **Flush Sequence**: Same memtable size → same flush sequence
2. **Compaction Plan**: Same manifest → same compaction decisions (Phase 2)
3. **SST Structure**: Same input → same output SST (same bloom, same trie index)
4. **Manifest Updates**: Atomic per operation, reproducible
5. **Write Order**: WAL guarantees write sequence
6. **Task Execution Order**: Runtime executor processes tasks sequentially

**What's Not Deterministic** (And Why It's OK):

1. **Timing**: Exact wall-clock timing varies (but order preserved)
2. **Cloud Latency**: Network variability doesn't affect consistency
3. **Cache State**: Varies with access patterns (but doesn't affect correctness)
4. **Memory Layout**: Pointer addresses vary between runs (but don't matter)

**Verification**:
- Phase 8 includes determinism test suite
- Same workload on two engines → same manifest state
- Recovery produces identical state to pre-crash

---

### 8. Concurrency Model

**No Shared Mutable State** (Enforced):

- Memtables: Column-family-per-thread via RwLock in memtable_set
- Manifest: Version/AtomicVersionSet with CAS updates
- Block Cache: Arc-shared, internal synchronization
- Version Set: Atomic compare-and-swap for manifest updates
- WAL: Interior mutability (AsyncWalWriter uses channels)

**Thread Safety**:

1. **Write Path**: Single sequence number generator (atomic), WAL serializes
2. **Read Path**: Snapshot captures sequence number, readers get consistent view
3. **Background Operations**: Runtime executor (single thread) prevents races
4. **Manifest Updates**: Version-based atomicity (CAS operations)

---

### 9. Error Handling

**Background Error Container**:

```rust
pub background_error: Arc<RwLock<Option<MidgeError>>>
```

When a background operation fails:
1. Error is stored in `background_error`
2. Write operations check this before proceeding
3. Application can read error status
4. Write operations blocked until error is cleared (manual reset)

**Error Propagation**:

```
Background operation fails
    ↓
Set background_error = Some(error)
    ↓
Write operation:
  check_background_error() → Err
    ↓
Return error to application
    ↓
Application must handle:
  - Retry later
  - Inspect application state for consistency
  - Manual recovery
```

---

### 10. Shutdown Sequence

```
Engine::drop() or explicit shutdown()
    ↓
Set shutdown flag
    ↓
Close WAL (no new writes)
    ↓
Broadcast shutdown signal to all workers via EngineRuntime
    ↓
EngineRuntime::shutdown():
  1. Send shutdown to runtime executor
  2. Wait for executor to finish pending tasks
  3. Stop flush coordinator worker
  4. Stop compaction controller worker
  5. Stop any cloud operation workers
    ↓
Join all background threads
    ↓
Drop all Arc references
    ↓
Deallocate all data structures
```

---

## Performance Characteristics

### Write Latency

- **put()**: O(log M) where M = memtable size, typically <1ms
- **Includes WAL write**, ~1-10ms depending on disk

### Read Latency

- **Cache hit**: O(log B) where B = blocks in block cache, typically <1ms
- **SST hit**: O(log F) where F = files in SST set, typically 1-10ms
- **Cloud fallback**: O(network) + O(log B), typically 50-500ms

### Flush Time

- **Memtable→SST**: O(M log M) for sorting, typically 50-500ms
- **Cloud upload**: O(network), typically 100-1000ms (async, doesn't block)

### Compaction Time

- **Single level**: O(F * log F) where F = files, typically 100-5000ms
- **Parallel compactions**: Multiple levels handled by single executor (sequential)

---

## Alignment with THE_BIG_IDEA

| Requirement | Implementation |
|---|---|
| Central runtime actor | ✅ EngineRuntime executor |
| All background work through runtime | ✅ Flush, Compaction, WAL, Cloud ops |
| Deterministic scheduling | ✅ Single-threaded executor |
| Cloud-first durability | ✅ CloudSstManager + HybridStorage |
| Local cache optional | ✅ Configurable via StorageMode |
| No direct worker thread access | ✅ All via runtime tasks |
| Atomic manifest updates | ✅ Version/AtomicVersionSet |
| Mutable segments | ✅ Phase 5 integration complete |
| Trie index for SSTs | ✅ Phase 4 unified write path |
| No unsafe code | ✅ 100% safe Rust |

---

## Future Improvements

1. **Task Replay**: Save task log for crash recovery
2. **Snapshots**: Periodic checkpoint of runtime state
3. **Multi-threaded Compaction**: Multiple compaction executors (ordered)
4. **Distributed Runtime**: Replicate across nodes
5. **Advanced Caching**: Intelligent prefetch based on workload
