# Gap Analysis: THE_BIG_IDEA vs Current Implementation

## Executive Summary

Midge has made substantial progress toward the actor-based LSM vision outlined in THE_BIG_IDEA.md. The core architecture is largely aligned, with most critical components in place or well-underway. This document identifies remaining gaps and areas where the implementation diverges from or lags behind the vision.

---

## Design Pillars Status

### ✅ 1. Embeddability (In-Process Library)
**Status**: IMPLEMENTED
- Database embeds as a library sharing the application's process
- No RPC layer or separate server
- Direct in-process API calls via `MidgeEngine` and `KvStore` trait
- **Gap**: None identified

### ✅ 2. Actor Core (EngineRuntime)
**Status**: IMPLEMENTED
- Central `EngineRuntime` owns all background worker threads
- Task submission via channel-based message queue
- Deterministic task execution order (unbounded channel preserves order)
- **Implementation**: `src/core/runtime.rs`
- **Gap**: Minor—see details below

### ⚠️ 3. Unified Write Path
**Status**: MOSTLY IMPLEMENTED
- Write pipeline: op → seqno → WAL append → memtable apply → runtime notify
- Sequence numbers are atomic (AtomicU64)
- WAL writes are synchronous (per `WalController`)
- No random worker threads mutating memtables
- **Implementation**: `src/core/engine/core.rs`, `src/wal/`
- **Gap**: See "Gaps & Issues" below

### ⚠️ 4. Cloud-Native WAL
**Status**: PARTIALLY IMPLEMENTED
- Local WAL segments implemented (`src/wal/`)
- WAL upload coordinator routes uploads through runtime (`WalUploadCoordinator`)
- Cloud storage backends available (`src/cloud/`)
- **Issue**: WAL recovery still depends on local filesystem; cloud-first recovery not fully realized
- **Implementation**: `src/wal/`, `src/core/wal_upload_coordinator.rs`
- **Gap**: Recovery path must be cloud-sourced, currently hybrid

### ✅ 5. Cloud-Native SST Layer
**Status**: WELL-UNDERWAY
- SST format supports TLV blocks (`src/sst/encoding.rs`)
- Pluggable metadata: trie index, bloom filters, sparse index, range tombstones
- Block cache for NVMe caching layer
- Cloud SST manager for cloud storage (`src/sst/cloud/`)
- **Implementation**: `src/sst/`, `src/sst/cloud/`
- **Gap**: Direct cloud-to-cloud compaction path not fully realized

### ✅ 6. Deterministic Compaction + Flush
**Status**: IMPLEMENTED
- Deterministic task execution via runtime
- Compaction plans are generated and executed as tasks
- Intent log concept present via `Manifest` versioning
- **Implementation**: `src/core/compaction/`, `src/core/flush_coordinator.rs`
- **Gap**: Intent log scope could be more explicit

### ✅ 7. Modern SST Format
**Status**: IMPLEMENTED
- TLV blocks: ✅ (`src/sst/encoding.rs`)
- Trie index: ✅ (`src/sst/trie_index.rs`)
- Bloom filters: ✅ (`src/sst/bloom.rs`)
- Per-block bloom: ✅ (`src/sst/bloom.rs`)
- Sparse index: ✅ (`src/sst/sparse_index.rs`)
- Range tombstones: ✅ (`src/sst/range_tombstone.rs`)
- **Implementation**: `src/sst/`
- **Gap**: None identified

---

## Detailed Gaps & Issues

### 1. **Recovery Path Not Fully Cloud-Native**
**Severity**: HIGH
**Current State**:
- Recovery logic still relies on local filesystem state ("whatever's on the local FS")
- WAL replay happens from local disk or reconstructed segments
- Manifest sourced from local `manifest.json` file

**Vision**:
- Recovery driven by manifest + WAL + compaction log from cloud
- No local filesystem assumptions

**Action Items**:
- [ ] Refactor recovery to be manifest-first (pull from cloud)
- [ ] Implement cloud WAL replay without local buffer assumptions
- [ ] Move `manifest.json` to be cloud-sourced with local cache only
- **Files to review**: `src/core/persistence/wal_replay.rs`, manifest recovery logic

---

### 2. **Intent Log Scope Not Explicit**
**Severity**: MEDIUM
**Current State**:
- Compaction and flush decisions are deterministic
- Manifest versions provide audit trail
- No formal "intent log" separate from manifest versions

**Vision**:
- Explicit intent log capturing all planned operations
- Enables recovery replay and debugging

**Action Items**:
- [ ] Consider adding explicit `IntentLog` type capturing flush/compaction plans before execution
- [ ] Formalize the intent log as part of recovery mechanism
- [ ] Document intent log format in SST/compaction specifications
- **Files to consider**: New module `src/core/intent_log.rs`

---

### 3. **Write Path May Block on Cloud Operations**
**Severity**: MEDIUM
**Current State**:
- `put()` blocks on local WAL write (good)
- Some configurations may require cloud ACK before return (`wait_for_cloud_wal_uploads_on_sync`)
- This can cause write path latency spikes

**Vision**:
- Write returns after local WAL durability only
- Cloud uploads happen asynchronously in background

**Action Items**:
- [ ] Verify `wait_for_cloud_wal_uploads_on_sync = false` is the default for cloud-backed engines
- [ ] Add telemetry to detect when writes block on cloud operations
- [ ] Document the latency implications of this flag
- **Files to review**: `src/core/engine/core.rs` (EngineBuilder), WAL sync logic

---

### 4. **Compaction May Not Write Directly to Cloud**
**Severity**: MEDIUM
**Current State**:
- Compaction writes SSTs to local disk
- Cloud upload happens as separate async task
- No "direct cloud write" path for compaction output

**Vision**:
- Compaction can write directly to cloud, optionally caching locally
- Reduces local storage pressure

**Action Items**:
- [ ] Add optional direct-cloud compaction output path
- [ ] Implement cloud-write-then-cache-locally pattern
- [ ] Consider "cloud-only" SST files (no local copy)
- **Files to review**: `src/core/compaction/`, `src/sst/cloud/`

---

### 5. **Runtime Task Prioritization Not Explicit**
**Severity**: LOW
**Current State**:
- All runtime tasks use unbounded FIFO channel
- No prioritization between task kinds (Flush vs Compaction vs Maintenance)
- Same executor for all tasks

**Vision**:
- Different priority levels for different task kinds (e.g., Flush > Maintenance > Compaction)
- Ability to schedule urgent tasks ahead of pending work

**Action Items**:
- [ ] Consider priority queue for runtime tasks
- [ ] Document task priority semantics (if needed)
- [ ] Benchmark FIFO vs prioritized scheduler
- **Files to review**: `src/core/runtime.rs`

---

### 6. **Cloud Coordinator Role Still Unclear**
**Severity**: LOW
**Current State**:
- `CloudCoordinator` exists but role is minimal
- Most cloud operations routed through other coordinators

**Vision**:
- `CloudCoordinator` manages deterministic cloud request sequencing
- Prevents race conditions and non-determinism

**Action Items**:
- [ ] Expand `CloudCoordinator` responsibilities
- [ ] Document cloud operation sequencing rules
- [ ] Ensure all cloud I/O goes through runtime (not spawned in background)
- **Files to review**: `src/core/cloud_coordinator.rs`

---

### 7. **Manifest Cache Coherence**
**Severity**: LOW
**Current State**:
- Manifest cache optimization present
- Cache invalidation logic in place
- No explicit coherence guarantees documented

**Vision**:
- Clear cache invalidation semantics
- Deterministic cache behavior under concurrency

**Action Items**:
- [ ] Document manifest cache coherence guarantees
- [ ] Add tests for cache correctness under concurrent updates
- [ ] Consider making cache thread-safe with explicit locking
- **Files to review**: `src/sst/manifest_cache.rs`

---

### 8. **Background Error Handling**
**Severity**: MEDIUM
**Current State**:
- `background_error` Arc<RwLock> field exists
- Background tasks can report errors
- Write path checks for errors before accepting puts

**Vision**:
- Explicit background error propagation model
- Consistent semantics for error recovery

**Action Items**:
- [ ] Document background error semantics and recovery procedure
- [ ] Ensure all background tasks properly report errors
- [ ] Add test coverage for error propagation scenarios
- **Files to review**: `src/core/engine/core.rs`

---

## Implementation Completeness Summary

| Component | Status | Completeness | Notes |
|-----------|--------|--------------|-------|
| EngineRuntime | ✅ | ~95% | Minor prioritization gaps |
| Write Path | ✅ | ~90% | Cloud blocking concerns |
| WAL (Local) | ✅ | 100% | Fully implemented |
| WAL (Cloud) | ⚠️ | ~70% | Upload works, recovery incomplete |
| SST Format | ✅ | 100% | All metadata types present |
| SST Cache | ✅ | ~95% | Block cache, table cache, metadata caches |
| Compaction | ✅ | ~85% | No direct cloud write path |
| Flush | ✅ | ~90% | Coordinate with runtime |
| Manifest | ✅ | ~80% | Cache present, recovery needs work |
| Recovery | ⚠️ | ~60% | Still filesystem-dependent |
| Cloud Backend | ⚠️ | ~75% | Interface present, not fully utilized |
| Concurrency | ✅ | ~85% | Good MVCC, some cache coherence gaps |

---

## Priority Order for Closing Gaps

### P0 (Critical Path)
1. **Recovery must be cloud-native** — currently depends on local FS
2. **Compaction should write to cloud** — needed for storage efficiency
3. **Background error handling** — ensure fault tolerance

### P1 (Important)
4. **Intent log formalization** — improves debuggability
5. **Write path latency** — ensure no cloud blocking on critical path
6. **Task prioritization** — optimize responsiveness

### P2 (Nice-to-have)
7. **Cloud coordinator expansion** — future extensibility
8. **Manifest cache coherence** — correctness assurance
9. **Direct metrics** — observability of design principles

---

## Testing Recommendations

**For Gap Closure Validation:**
1. Add determinism tests for recovery (same manifest → same recovered state)
2. Add latency tests ensuring writes don't block on cloud ops
3. Add intent log audit trail tests
4. Add cloud-write compaction tests
5. Add background error propagation tests

---

## Conclusion

Midge is **~85% aligned** with THE_BIG_IDEA vision. The actor model is implemented, SST format is complete, and the basic infrastructure is solid. The main gaps are:

1. **Recovery path** needs to be cloud-sourced rather than filesystem-sourced
2. **Compaction output** should write directly to cloud
3. **Intent logging** should be more explicit

These are implementable features that don't require architectural changes—they're refinements to the existing foundation.

**Recommendation**: Focus on P0 items (recovery + direct cloud compaction) to fully realize the cloud-native design. Then tackle P1 items (intent log, latency assurances) for robustness.
