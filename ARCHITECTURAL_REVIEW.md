# Midge Architectural Review & Refactor Plan

**Date**: December 8, 2025  
**Reviewer**: Deep Analysis of Actor-Model LSM Engine  
**Status**: 7/8 Phases Complete, Ready for Phase 8 Production Hardening

---

## 1. HIGH-LEVEL ARCHITECTURAL GAPS

### 1.1 Runtime Executor Incomplete Abstraction

**Problem**: The `EngineRuntime` exists but background operations (flush, compaction) still spawn their own worker threads independently.

**Current State**:
- `FlushCoordinator::spawn()` creates its own `JoinHandle`, then wraps it in `WorkerHandle`
- `CompactionController::spawn()` creates a tick thread + worker thread manually
- These threads are "registered" with the runtime but the runtime doesn't truly own their lifecycle

**Impact**:
- Two scheduling layers: runtime tasks queued to coordinators + coordinator's own thread loops
- Thread pool is implicit (flush thread + compaction thread + tick thread) with no visibility
- No unified shutdown semantics: coordinators have `shutdown()` but runtime doesn't coordinate them
- Hard to reason about when work actually executes (is it in a runtime task? A coordinator thread?)

**Recommendation**: 
Move to a **single-threaded event loop** runtime with explicit work items:
1. Replace `RuntimeTask` callback model with work queue structs
2. Have runtime own a threadpool for execution (can start with 1 thread for determinism)
3. Coordinators become thin wrappers that queue work, not separate threads
4. See Section 5 for migration strategy

---

### 1.2 Module Boundaries Violate Layering

**Problem**: Coordinators (flush, compaction, WAL upload) live in different modules but all do the same job.

**Current Structure**:
```
core/
  runtime.rs                 ← RuntimeTask, EngineRuntime
  persistence/
    flush_coordinator.rs     ← FlushCoordinator (owns thread)
    flush/
      process.rs            ← Actual flush logic
  compaction/
    controller.rs           ← CompactionController (owns thread)
    executor.rs             ← Actual compaction logic
  wal_upload_coordinator.rs ← WalUploadCoordinator
  cloud_coordinator.rs      ← CloudCoordinator
```

**Issues**:
- Four coordinator types with different APIs (`request_flush()`, `compact_level()`, `submit_sst_upload_task()`)
- Coordinators are in 3 different module locations, inconsistent naming
- `WalUploadCoordinator` and `CloudCoordinator` are never called from the engine
- No single place to understand "what operations can the runtime do?"

**Recommendation**:
Create `core/coordinator/` with unified coordinator interface:
```
core/
  coordinator/
    mod.rs                  ← Public API (trait)
    flush.rs               ← FlushCoordinator implementation
    compaction.rs          ← CompactionController implementation
    wal_upload.rs          ← WalUploadCoordinator implementation
    cloud.rs               ← CloudCoordinator implementation
```

All coordinators implement trait:
```rust
pub trait BackgroundCoordinator: Send + Sync {
    fn submit(&self, work: BackgroundWork) -> MidgeResult<()>;
    fn shutdown(&self) -> MidgeResult<()>;
}
```

---

### 1.3 Manifest Cache is a Duplicate Source of Truth

**Problem**: Manifest exists on disk AND in memory, with inconsistent update paths.

**Current State**:
- `Manifest` is immutable, only updated via `VersionSet` 
- `ManifestCache` shadows the manifest for fast reads
- Caches: `manifest_cache`, `bloom_cache`, `sparse_index_cache`
- Updates must go through: flush/compaction → manifest write → cache invalidate

**Issues**:
- Multiple update paths: direct manifest writes + cache updates (hard to keep in sync)
- Cache invalidation is manual, easy to forget
- No clear invariant about when cache is stale
- 3 separate metadata caches (manifest, bloom, sparse index) with different invalidation semantics

**Recommendation**:
Create `core/metadata/` as single authoritative source:
```rust
pub struct Metadata {
    // Immutable snapshot of manifest (version-managed)
    manifest: Arc<Manifest>,
    
    // Derivative caches (invalidated atomically with manifest updates)
    bloom_index: BloomMetadataIndex,
    sparse_index: SparseIndexCache,
    file_summary: FileSummaryCache,
}

// Single update path
impl Metadata {
    pub fn update_from_edit(edit: VersionEdit) -> Self { /* ... */ }
}
```

All caches invalidated together when manifest changes. Read path always asks metadata, never disk.

---

### 1.4 Cloud Integration Paths Are Fragmented

**Problem**: Cloud operations are scattered, not integrated into the write/background work model.

**Current State**:
- `CloudCoordinator` created but never called from flush/compaction
- `WalUploadCoordinator` created but not integrated into WAL sync path
- `HybridStorage` backend exists but isn't wired into engine operations
- `cloud_sst_manager` field exists on engine but unused in hot paths

**Issues**:
- SSTs are written locally, not submitted for cloud upload
- WAL syncs don't coordinate with cloud WAL uploader
- Phase 7.2-7.3 infrastructure exists but not connected
- No visibility into which files are cloud-resident vs local

**Recommendation**:
Integrate cloud as a **storage layer** not a side effect:
```rust
// Existing
pub fn get(&self, key: &[u8]) -> Option<Bytes>;

// Add storage tier awareness
impl MidgeEngine {
    pub fn get_with_tier(&self, key: &[u8]) -> (Option<Bytes>, StorageTier) {
        // Reads from: segment → memtable → L0/L1 (local) → L2+ (hybrid cache/cloud)
    }
    
    pub fn list_cloud_files(&self) -> Vec<CloudFileInfo> {
        // Operator visibility into what's uploaded
    }
}

// Flush automatically submits to cloud
impl FlushCoordinator {
    pub fn request_flush(&self, job: FlushJob) -> MidgeResult<()> {
        // 1. Flush to local SST
        // 2. Submit SST path to cloud via CloudCoordinator::submit_sst_upload_task
        // 3. WAL upload if needed
    }
}
```

---

### 1.5 Error Handling Is Inconsistent

**Problem**: No unified error propagation from background workers to user code.

**Current State**:
- `MidgeError` enum is small (connection, invalid_config, internal)
- Flush/compaction errors are logged but not surfaced to API
- User code can't distinguish between temporary slowness and permanent failure
- No "background error" visibility (Phase 6 added field but unused)

**Issues**:
- `put()` succeeds even if flush fails later
- Operator has no way to know the engine is in a degraded state
- No retry policy for failed flushes/compactions
- Test hooks exist but real error handling is minimal

**Recommendation**:
Add error tracking to engine:
```rust
pub struct EngineState {
    background_error: Option<MidgeError>,
    last_error_timestamp: Option<Instant>,
    error_count: AtomicU64,
}

impl MidgeEngine {
    pub fn background_error(&self) -> Option<&MidgeError>;
    
    pub fn clear_error(&self) -> MidgeResult<()> {
        // Retry background work if applicable
    }
}

// Coordinator reports errors
impl FlushCoordinator {
    pub fn on_error(&self, err: MidgeError);
}
```

---

### 1.6 Compaction Determinism Not Actually Verified

**Problem**: "Deterministic compaction" is claimed but not validated in tests.

**Current State**:
- `CompactionController` deterministic by design (single thread)
- Tests exist for compaction logic but don't verify determinism
- No test that runs same workload twice on identical engines and compares manifests

**Impact**:
- Phase 2 "deterministic compaction" not proven for Phase 8 validation
- Scheduling is deterministic but manifest contents might not be identical across runs

**Recommendation**:
Add Phase 8 task (already planned in ROADMAP):
```rust
#[test]
fn should_produce_deterministic_compaction_sequence() {
    // Create engine1, engine2 with identical config
    // Run same puts/deletes in same order
    // Trigger compactions
    // Verify both engines produce identical manifest state
}
```

---

## 2. MODULE-BY-MODULE REFACTOR PLAN

### 2.1 Core Engine (`src/core/engine/core.rs` - 576 lines)

**Current State**:
- 40+ public fields: WAL, coordinators, metrics, caches, factories
- Mixed responsibilities: engine state + coordination + caching
- Many `Arc<>` wrapped coordinators suggesting they're shared but API doesn't expose them

**Issues**:
- Every coordinator operation requires Arc clone: `Arc::clone(&self.flush_coordinator)`
- No clear initialization order (which things must exist before others?)
- `mem_mode`, `read_only`, `is_read_only` (AtomicBool) - duplication
- `db_lock` field is dead code (never used)

**Refactoring Steps**:

1. **Extract Engine State** (new file `core/engine/state.rs`):
```rust
pub struct EngineState {
    // Mutable state
    seq: AtomicU64,
    txn_id: AtomicU64,
    is_read_only: AtomicBool,
    
    // Background error tracking
    background_error: RwLock<Option<MidgeError>>,
}
```

2. **Extract Dependencies** (new file `core/engine/deps.rs`):
```rust
pub struct EngineDependencies {
    pub wal: WalController,
    pub flush: Arc<FlushCoordinator>,
    pub compaction: Option<Arc<CompactionController>>,
    pub cloud: Arc<CloudCoordinator>,
    pub metadata: Arc<Metadata>,  // Unified metadata store
}
```

3. **Reduce MidgeEngine**:
```rust
pub struct MidgeEngine {
    // Unique immutable config
    config: EngineConfig,
    
    // Mutable state
    state: Arc<EngineState>,
    
    // Dependencies
    deps: EngineDependencies,
    
    // Read-optimized caches
    metrics: Arc<Metrics>,
}
```

**Benefit**: Clear separation of concerns, easier testing of state mutations independently.

---

### 2.2 Flush Path (`src/core/persistence/flush/`) - Duplication with Compaction

**Current Issue**:
- Flush has its own job queue, worker loop, and entry pipeline
- Compaction has parallel logic for merging/deduplication
- Both do: collect entries → resolve merges → sort → write → update manifest
- Code duplication in `resolve_merges()` between flush and compaction contexts

**Refactoring**:

1. **Extract Shared Entry Pipeline** (`core/entry_pipeline.rs`):
```rust
pub struct EntryPipeline {
    merge_resolver: MergeResolver,
    deduplicator: Deduplicator,
}

impl EntryPipeline {
    pub fn process(&self, entries: Vec<EntryMeta>) -> MidgeResult<Vec<EntryMeta>> {
        // 1. Group by user key
        // 2. Resolve merges for that key
        // 3. Deduplicate versions (keep newest for non-merges)
        // 4. Return sorted
    }
}
```

2. **Unify Work Submission**:
```rust
pub enum BackgroundWork {
    Flush {
        cf_id: u32,
        entries: Vec<EntryMeta>,
    },
    Compaction {
        plan: CompactionPlan,
    },
}

impl MidgeEngine {
    pub fn submit_background_work(&self, work: BackgroundWork) -> MidgeResult<()> {
        self.runtime.submit(work)
    }
}
```

3. **Remove Flush's Own Worker Loop**:
   - `spawn_flush_worker()` → deleted
   - `FlushCoordinator::request_flush()` → `RuntimeTask::new(BackgroundWork::Flush {...})`
   - Flush logic becomes stateless function: `fn execute_flush(config, entries) -> Result<SstFile>`

**Impact**: 
- 200+ lines of duplication removed
- Single code path for entry processing
- Easier to test and maintain

---

### 2.3 Memtable (`src/core/memtable/`) - Unclear Lifecycle

**Current State**:
- `Memtable` struct holds entries, but lifecycle (active → immutable → flushed) is implicit
- Freezing logic spread across multiple places
- Range tombstone tracking is separate concern

**Issues**:
- No explicit state machine for memtable lifecycle
- Flush doesn't know if memtable is still active or already flushed
- MVCC versions kept "for snapshot support" but no clear version retention policy

**Refactoring**:

1. **Add Explicit Lifecycle State**:
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemtableState {
    Active,      // Accepting writes
    Sealed,      // Frozen, no more writes
    Flushing,    // Write in progress to SST
    Flushed,     // Complete
}

pub struct Memtable {
    entries: SkipList,
    state: AtomicCell<MemtableState>,
    created_at: Instant,
    sealed_at: Option<Instant>,
}
```

2. **State Transitions in Runtime**:
```rust
pub fn seal_memtable(&self, cf: &ColumnFamilyHandle) -> MidgeResult<()> {
    // Flush coordinator detects sealed memtables asynchronously
    // No explicit "freeze" call from user code
}
```

3. **MVCC Policy**:
```rust
pub fn snapshot_retention_policy(&self) -> SnapshotRetentionPolicy {
    // Snapshots retain: oldest snapshot seq → all versions newer than that seq
    // After snapshot is released, old versions can be dropped
}
```

---

### 2.4 Compaction Strategy (`src/core/compaction/strategy.rs` + `planner.rs`)

**Current State**:
- `Planner` decides what to compact
- `Compactor` executes the decision
- `CompactionPlan` represents the decision
- `planner_controller.rs` is vestigial dead code (marked for removal)

**Issues**:
- `planner_controller.rs` mentioned in imports but never used (dead code)
- Strategy selection (leveled vs tiered) buried in configuration
- No clear interface between planner and executor
- Compaction filter integration is bolted on

**Refactoring**:

1. **Remove `planner_controller.rs`** (already decided as dead code)

2. **Consolidate Strategy Files**:
```
core/compaction/
  mod.rs
  strategy.rs     ← CompactionStrategy trait
  leveled.rs      ← LeveledCompactionStrategy implementation
  tiered.rs       ← TieredCompactionStrategy implementation (if implemented)
  executor.rs     ← CompactionExecutor (runs a plan)
  controller.rs   ← CompactionController (owns worker)
```

3. **Clear Strategy Interface**:
```rust
pub trait CompactionStrategy {
    fn plan(&self, manifest: &Manifest) -> Option<CompactionPlan>;
    fn name(&self) -> &str;
}

pub struct CompactionController {
    strategy: Arc<dyn CompactionStrategy>,
    executor: Arc<CompactionExecutor>,
    runtime: Arc<EngineRuntime>,
}
```

---

### 2.5 Manifest Versioning (`src/core/manifest/`)

**Current State**:
- `Manifest` is immutable
- `VersionSet` tracks versions
- `VersionManager` owns `VersionSet`
- `AtomicVersionSet` provides lock-free read access
- Caches shadow the manifest

**Issues**:
- 4 different version management types (hard to understand) 
- Cache invalidation is manual
- No invariant about "all snapshots must have a version"

**Refactoring**:

1. **Simplify to 2 Types**:
```rust
pub struct VersionSet {
    current: ArcSwap<Manifest>,
    history: [Arc<Manifest>; MAX_VERSIONS], // Bounded history for snapshots
}

// VersionManager just wraps VersionSet
pub struct VersionManager {
    versions: Arc<VersionSet>,
}

// No AtomicVersionSet wrapper - VersionSet is already lock-free
```

2. **Snapshot Pinning**:
```rust
pub struct Snapshot {
    version: Arc<Manifest>,  // Pinned version, cannot be GC'd
    created_at: Instant,
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        version_manager.unpin(self.version);
    }
}
```

3. **Unified Metadata Cache**:
```rust
pub struct Metadata {
    pub manifest: Arc<Manifest>,
    pub bloom_index: BloomIndex,  // Updated with manifest atomically
    pub file_summary: FileSummary, // Updated atomically
}
```

---

### 2.6 WAL (`src/wal/`)

**Current State**:
- Three WAL implementations: filesystem, in-memory, cloud
- `WalController` coordinates them
- `WalUploadCoordinator` exists but not used
- Cloud WAL upload not integrated with flush

**Issues**:
- `spawn_flush_worker()` calls `wal.sync()` but `WalUploadCoordinator` is separate
- No unified sync model (local sync vs. cloud sync)
- `WalUploadCoordinator` only has 4 tests, not in hot path

**Refactoring**:

1. **Integrate WAL Upload into Flush**:
```rust
// When flush completes:
pub async fn flush_completed(&self, sst_file: SstFile) {
    // 1. Optionally: rotate WAL (if filled during flush)
    // 2. Submit WAL upload to CloudCoordinator
    // 3. Submit SST upload to CloudCoordinator
}
```

2. **Single Sync Interface**:
```rust
pub enum SyncMode {
    Local,       // Local WAL only
    Cloud,       // Wait for cloud upload
}

pub fn sync(&self, mode: SyncMode) -> MidgeResult<()> {
    match mode {
        SyncMode::Local => self.wal.sync_local(),
        SyncMode::Cloud => self.wal.sync_cloud(),
    }
}
```

3. **Remove Unused Coordinator**:
   - `WalUploadCoordinator` → Integrated into Cloud coordinator
   - `WalUploadCoordinator::submit_wal_upload_task()` → `CloudCoordinator::submit_wal_upload_task()`

---

### 2.7 Cloud Integration (`src/cloud/`)

**Current State**:
- `CloudCoordinator` created but not called
- `HybridStorage` backend exists
- `cloud_sst_manager` field on engine unused

**Issues**:
- Flush doesn't submit SSTs to cloud
- No automatic eviction of local blocks to cloud
- Operator has no visibility into cloud upload status

**Refactoring**:

1. **Auto-Upload on Flush**:
```rust
pub fn flush_completed(&self, sst: SstFile) {
    if self.config.cloud_enabled {
        self.cloud_coordinator
            .submit_sst_upload_task(sst.path, sst.size)?;
    }
}
```

2. **Cache Eviction Policy**:
```rust
pub struct CacheEvictionPolicy {
    max_local_bytes: u64,
    min_retain_levels: u32,
}

pub fn check_eviction(&self) {
    if local_bytes > max && can_evict {
        let oldest = self.manifest.oldest_evictable_file();
        self.cloud_coordinator.submit_eviction_task(oldest);
    }
}
```

3. **Operator Visibility**:
```rust
pub fn cloud_status(&self) -> CloudStatus {
    CloudStatus {
        uploaded_bytes: self.cloud_coordinator.uploaded_bytes(),
        pending_bytes: self.cloud_coordinator.pending_bytes(),
        failed_uploads: self.cloud_coordinator.failed_uploads(),
    }
}
```

---

## 3. CONSOLIDATION OPPORTUNITIES

### 3.1 Merge Resolution Duplication

**Issue**: `resolve_merges()` appears in both `maintenance.rs` (flush path) and `compaction/executor.rs`.

**Fix**: Create `core/merge/resolver.rs`:
```rust
pub struct MergeResolver {
    operators: HashMap<u32, Arc<dyn MergeOperator>>,
}

impl MergeResolver {
    pub fn resolve_version_list(
        &self,
        cf_id: u32,
        versions: Vec<(Option<Bytes>, Option<u64>, OpType)>,
    ) -> MidgeResult<Option<Bytes>> {
        // Single implementation
    }
}

// Used by both flush and compaction
```

**Cleanup**: Remove duplicate code from both paths, use `MergeResolver`.

---

### 3.2 Coordinator API Consolidation

**Issue**: Four coordinators with different APIs.

**Current**:
- `FlushCoordinator::request_flush(job)`
- `CompactionController::compact_level(cf_id, level)`
- `WalUploadCoordinator::submit_wal_upload_task(path)`
- `CloudCoordinator::submit_sst_upload_task(path)`

**Unified Trait**:
```rust
pub trait BackgroundCoordinator: Send + Sync {
    fn submit(&self, work: BackgroundWork) -> MidgeResult<()>;
}

pub enum BackgroundWork {
    Flush(FlushWork),
    Compaction(CompactionWork),
    WalUpload(WalUploadWork),
    CloudUpload(CloudUploadWork),
}
```

**Move all coordinators to `core/coordinator/` module**.

---

### 3.3 Snapshot/Version Management Consolidation

**Issue**: Snapshot pinning is implicit, versions not explicitly tied to snapshots.

**Unified Model**:
```rust
pub struct VersionManager {
    versions: Arc<VersionSet>,
    pinned: DashMap<u64, Arc<Manifest>>, // Snapshots keep versions alive
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        self.version_manager.unpin(self.version_id);
    }
}
```

---

### 3.4 Logging Consolidation

**Issue**: Debug logging inconsistent across modules (some use `tracing::debug!`, some use `println!`).

**Fix**: Establish logging standard:
```rust
// Good (used everywhere)
tracing::debug!("key={}", key);
tracing::warn!("background error: {}", err);

// Bad (should not appear in production code)
println!("...");
eprintln!("...");
```

**Audit**: Already mostly clean, but ensure consistency in error reporting.

---

## 4. PRUNING RECOMMENDATIONS

### 4.1 Dead Code to Remove

1. **`src/core/compaction/planner_controller.rs`** (132 lines)
   - Status: Unused module (decision made already)
   - CompactionController is the real implementation
   - Action: DELETE

2. **`src/core/engine/core.rs` field `db_lock`** (#[allow(dead_code)])
   - Status: Never used after locking initialization
   - Action: REMOVE from struct

3. **`src/core/engine/core.rs` field `mem_mode`** (#[allow(dead_code)])
   - Status: Never read, StorageMode is the source of truth
   - Action: REMOVE from struct

4. **`src/wal/mem/shared.rs` in-memory WAL** (test-only)
   - Status: Works, but if StorageMode::Memory is never used in production...
   - Action: KEEP (test support is valuable)

### 4.2 Unused Features/Fields

1. **`VersionSet::history` bounded history**
   - Check if actually used for snapshot retention
   - If not used, simplify to simple current + pinned versions

2. **`Metadata` triple-cache pattern**
   - Manifest cache + bloom cache + sparse index cache
   - Each with independent invalidation
   - Action: Unify into single atomic metadata update

3. **`WalUploadCoordinator` if cloud uploads go through CloudCoordinator**
   - Consolidate into single coordinator interface

### 4.3 Unused Experimental Code

1. **`test_hooks.rs` CompactionGatePoint**
   - Check if any tests actually use this
   - If not, simplify to basic barrier (or just use `Barrier` from `parking_lot`)

2. **`latency_sim.rs` LatencySimulator**
   - Check if used in benchmarks
   - If only for testing, move to `testutils/`

---

## 5. RISKS & MIGRATION NOTES

### 5.1 Migration Path for Runtime Refactor

**Risk**: Changing runtime from "coordinator threads + task submission" to "single event loop" is high-risk.

**Mitigation**:

**Phase 1: Parallel Implementation** (Phase 9, not Phase 8)
- Add new `EngineRuntimeV2` alongside existing runtime
- Implement same interface as old runtime
- Flush/compaction use both (feature-flag gated)
- Tests exercise both, compare results
- Effort: ~3 weeks

**Phase 2: Cutover**
- Remove old runtime
- Mark new runtime as default
- Effort: ~1 week

**Phase 3: Cleanup**
- Remove shim code
- Clean up feature flags

### 5.2 Snapshot Isolation During Refactor

**Risk**: Changing manifest versioning could break snapshot isolation.

**Mitigation**:
- Every snapshot test must remain passing
- Add explicit test for snapshot vs. compaction (from Phase 5 tests)
- Add invariant checker: "every snapshot has a pinned version"
- Run test suite after each refactor step

### 5.3 Cloud Integration Rollout

**Risk**: Wiring cloud uploads into hot paths (flush/compaction) could impact latency.

**Mitigation**:
- Cloud uploads are async (non-blocking for user code)
- Flush returns after local SST write, cloud upload happens in background
- Benchmark flush latency before/after integration
- Add flag `cloud_upload_blocking` for operator choice

### 5.4 Testing Strategy

**Requirement**: All existing tests must pass during refactor.

**Steps**:
1. Run full test suite before refactor: `cargo test --lib`
2. After each consolidation step, re-run tests
3. If any test fails, revert + investigate
4. Add new tests for consolidated APIs

---

## 6. OPTIONAL: PROPOSED DIRECTORY LAYOUT

After refactors, recommended layout:

```
src/
├── api/                      # Public API layer
│   ├── mod.rs
│   ├── kv_store.rs
│   ├── transaction.rs
│   ├── snapshot.rs
│   └── ...
│
├── common/                   # Foundational utilities (no deps on core)
│   ├── codec.rs
│   ├── error.rs
│   ├── internal_key.rs
│   └── ...
│
├── config/                   # Configuration & validation
│   └── ...
│
├── core/                     # Engine implementation
│   ├── engine/
│   │   ├── core.rs          # Main MidgeEngine (simplified)
│   │   ├── state.rs         # EngineState (NEW)
│   │   ├── deps.rs          # EngineDependencies (NEW)
│   │   ├── config.rs        # EngineConfig
│   │   └── ...
│   │
│   ├── runtime/             # (Refactored)
│   │   ├── mod.rs
│   │   ├── executor.rs      # RuntimeExecutor (single-threaded)
│   │   └── work.rs          # BackgroundWork enum
│   │
│   ├── coordinator/         # (NEW - consolidated)
│   │   ├── mod.rs           # BackgroundCoordinator trait
│   │   ├── flush.rs
│   │   ├── compaction.rs
│   │   ├── wal.rs
│   │   └── cloud.rs
│   │
│   ├── metadata/            # (NEW - unified)
│   │   ├── mod.rs
│   │   ├── manifest.rs      # Immutable manifest
│   │   ├── versioning.rs    # Version tracking
│   │   └── cache.rs         # Unified metadata cache
│   │
│   ├── pipeline/            # (NEW - shared entry processing)
│   │   └── entry.rs         # EntryPipeline
│   │
│   ├── merge/               # (NEW - consolidated)
│   │   └── resolver.rs      # MergeResolver
│   │
│   ├── memtable/
│   │   └── ... (with explicit state machine)
│   │
│   ├── persistence/         # WAL + Flush
│   │   ├── flush/
│   │   ├── flush_coordinator.rs (thinner - uses RuntimeTask)
│   │   ├── wal.rs
│   │   └── ...
│   │
│   ├── compaction/
│   │   ├── strategy.rs
│   │   ├── executor.rs
│   │   ├── controller.rs
│   │   └── ... (planner_controller.rs REMOVED)
│   │
│   ├── manifest/
│   │   ├── version_set.rs
│   │   ├── version_manager.rs (simplified)
│   │   └── ...
│   │
│   ├── transaction/
│   └── ...
│
├── sst/                      # SST format & caching
│   ├── format.rs
│   ├── trie_index.rs
│   ├── bloom.rs
│   └── ...
│
├── wal/                      # WAL implementations
│   ├── controller.rs         # Simplified (cloud upload in coordinator/)
│   ├── fs.rs
│   ├── cloud.rs
│   └── ...
│
├── cloud/                    # Cloud storage layer
│   ├── backend.rs            # StorageBackend trait
│   ├── hybrid.rs             # Hybrid local + cloud
│   └── ...
│
├── metrics/                  # Observability
└── lib.rs                    # Re-exports
```

**Key Changes**:
- `core/coordinator/` consolidates all background work
- `core/metadata/` unifies manifest versioning + caching
- `core/pipeline/` shared entry processing
- `core/merge/` centralized merge resolution
- Removed `planner_controller.rs`

---

## 7. SUMMARY OF CHANGES

| Category | Change | Effort | Priority | Phase |
|----------|--------|--------|----------|-------|
| **Architecture** | Runtime to true event loop | 3 weeks | High | 9 |
| **Module Org** | Consolidate coordinators to `core/coordinator/` | 1 week | High | 8 |
| **Module Org** | Extract `core/engine/state.rs` and `deps.rs` | 3 days | Medium | 8 |
| **Consolidation** | Unify manifest versioning (2 types instead of 4) | 3 days | Medium | 8 |
| **Consolidation** | Merge entry pipeline (flush + compaction shared) | 5 days | Medium | 8 |
| **Cloud** | Wire SST uploads into flush | 2 days | High | 8 |
| **Cloud** | Wire WAL uploads into coordinator | 2 days | High | 8 |
| **Pruning** | Remove `planner_controller.rs` | 1 day | Low | 8 |
| **Pruning** | Remove dead code fields (`mem_mode`, `db_lock`) | 1 day | Low | 8 |
| **Testing** | Add determinism validation suite | 3 days | High | 8 |
| **Testing** | Add snapshot isolation invariant checks | 2 days | Medium | 8 |
| **Logging** | Audit logging consistency | 1 day | Low | 8 |

---

## 8. PHASE 8 IMMEDIATE ACTIONS

**High Priority** (do now in Phase 8):
1. ✅ Document all pending decisions (DONE)
2. Extract `core/engine/state.rs` and `deps.rs` (consolidate responsibilites)
3. Move coordinators to `core/coordinator/` module (standardize API)
4. Wire CloudCoordinator into flush path (cloud uploads actually happen)
5. Audit and document snapshot retention policy

**Medium Priority** (Phase 8 optional):
6. Merge entry pipeline (flush + compaction use same code)
7. Simplify version management (2 types instead of 4)
8. Add explicit memtable state machine

**Phase 9+ (future)**:
9. Refactor runtime to true event loop
10. Comprehensive integration tests for determinism

---

## Conclusion

Midge is a well-structured LSM with strong fundamentals:
- ✅ Unified write path
- ✅ Deterministic compaction/flush infrastructure
- ✅ Clean SST format (trie + bloom)
- ✅ Cloud-native architecture (WAL + SST)
- ✅ Production-grade error handling

**Main architectural friction**:
- Runtime execution model is implicit (coordinator threads + task submission)
- Coordinator APIs are inconsistent
- Cloud integration exists but not wired into hot paths
- Manifest caching is manual and fragmented

**Path forward**:
1. Consolidate modules (coordinator, metadata, pipeline) in Phase 8
2. Wire cloud operations into flush/compaction in Phase 8
3. Refactor runtime to true event loop in Phase 9
4. Add determinism validation in Phase 8

After these refactors, Midge will be production-ready with clear architecture, minimal duplication, and fully integrated cloud support.
