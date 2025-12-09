# Codebase Cleanup & Reorganization Plan

## Overview
This plan addresses legacy code, obsolete patterns, and reorganization needed to align the codebase with the actor-model design and make it ready for future development.

---

## Part 1: Legacy Code & Obsolete Patterns to Remove

### 1.1 Redundant Flush Manager Layer
**Current State**: 
- `src/core/engine/flush_manager.rs` exists but may be redundant
- `src/core/persistence/flush_coordinator.rs` is the active coordinator
- Mixed ownership between engine and coordinator

**Action**:
- [ ] Review `flush_manager.rs` usage in `core.rs`
- [ ] Consolidate into single `FlushCoordinator` if not already done
- [ ] Remove duplicate/wrapper code
- **Files**: `src/core/engine/flush_manager.rs`, `src/core/persistence/flush_coordinator.rs`

### 1.2 Old Parallel Worker Thread Patterns
**Current State**:
- Some code uses raw `thread::spawn()` instead of going through `EngineRuntime`
- `FlushWorker` still spawns its own thread directly via `spawn_flush_worker()`
- Should route all through centralized runtime

**Action**:
- [ ] Audit all `thread::spawn()` calls in `src/core/`
- [ ] Convert worker threads to use `RuntimeTask` submission
- [ ] Remove `FlushWorker::spawn_flush_worker()` and integrate into runtime
- **Files**: `src/core/persistence/flush/worker.rs`

### 1.3 Legacy Recovery Filesystem Dependency
**Current State**:
- `src/core/persistence/wal_replay.rs` assumes local filesystem is source of truth
- Recovery doesn't prioritize cloud manifest/WAL
- Comments reference "whatever's on the local FS"

**Action**:
- [ ] Refactor recovery to be manifest-first (cloud-sourced)
- [ ] Make local FS optional cache layer for recovery
- [ ] Add tests for cloud-only recovery scenarios
- **Files**: `src/core/persistence/wal_replay.rs`, recovery initialization in `engine/state.rs`

### 1.4 Dead Code Annotations
**Current State**:
- Many `#[allow(dead_code)]` annotations in tests and helpers
- Indicates code written for future use but not integrated
- Clutters codebase

**Action**:
- [ ] Remove unused test helpers marked with `#[allow(dead_code)]`
- [ ] Either integrate remaining helpers or consolidate into single utility module
- **Files**: `tests/common/mod.rs`, `tests/common/helpers.rs`, `testutils/validate_tests.rs`

---

## Part 2: Module Organization & Right-Sizing

### 2.1 Core Engine Module Consolidation

**Current Structure (Fragmented)**:
```
src/core/engine/
  ├── core.rs              (Main MidgeEngine struct - 576 lines!)
  ├── factory.rs           (Construction helpers)
  ├── state.rs             (Initialization logic)
  ├── cf_manager.rs        (Column family management)
  ├── column_family.rs     (Column family types)
  ├── flush_manager.rs     (Flush coordination wrapper?)
  ├── kv_store_adapter.rs  (API adapter)
  ├── mod.rs               (Exports)
  └── operations/          (Focused operation modules)
```

**Issue**: `core.rs` is 576 lines with too many responsibilities
- Engine state management
- Initialization
- WAL coordination
- Manifest access
- Multiple coordinator references

**Reorganization**:
```
src/core/engine/
  ├── mod.rs               (Exports, public API)
  ├── kv_store.rs          (Main MidgeEngine + KvStore trait impl)
  ├── initialization.rs    (Engine::new() and setup)
  ├── column_families.rs   (ColumnFamily + ColumnFamilySet + cf_manager)
  ├── operations/          (Query, Write, Transaction ops - no change needed)
  └── types.rs             (Result types, InsertResult, CasResult)
```

**Action Items**:
- [ ] Extract initialization logic from `core.rs` → `initialization.rs`
- [ ] Merge `cf_manager.rs` + `column_family.rs` → `column_families.rs`
- [ ] Remove `flush_manager.rs` (if it's just a wrapper)
- [ ] Move non-public types to appropriate modules
- [ ] Update imports across codebase

### 2.2 Persistence Layer Simplification

**Current Structure**:
```
src/core/persistence/
  ├── flush/
  │   ├── mod.rs
  │   ├── worker.rs        (spawn_flush_worker - still uses raw thread::spawn)
  │   └── process.rs       (Actual flush logic)
  ├── flush_coordinator.rs (Wrapper around flush worker)
  ├── wal_replay.rs        (Recovery logic - NEEDS REFACTOR)
  └── mod.rs
```

**Issue**: Flush worker still outside runtime; recovery is filesystem-dependent

**Reorganization**:
```
src/core/persistence/
  ├── flush.rs             (Merge flush_coordinator + process; keep simple)
  ├── recovery.rs          (Cloud-sourced recovery)
  └── mod.rs
```

**Action Items**:
- [ ] Remove `flush/` subdirectory
- [ ] Merge `flush_coordinator.rs` + `flush/process.rs` → `flush.rs`
- [ ] Delete `flush/worker.rs` (move worker into runtime task executor)
- [ ] Rename + refactor `wal_replay.rs` → `recovery.rs` with cloud-first semantics

### 2.3 Write Path Consolidation

**Current Structure**:
```
src/core/write_path/
  ├── coordinator.rs       (WritePathCoordinator - minimal)
  └── mod.rs
```

**Issue**: Module seems empty relative to what should be there

**Action**:
- [ ] Confirm `write_path/coordinator.rs` is actually used
- [ ] If minimal, consider merging into `engine/operations/` or removing wrapper layer
- **Files**: Check usages with `grep_search`

### 2.4 Manifest & Version Management

**Current Structure**:
```
src/core/manifest/
  ├── manifest.rs          (Core Manifest type)
  ├── version_set.rs       (Lock-free version set - good!)
  ├── version_manager.rs   (Actor for manifest updates - good!)
  └── mod.rs
```

**Status**: Already well-organized ✅
**Action**: None needed, or minor comment cleanups

### 2.5 Cloud Coordinator Expansion

**Current State**:
- `src/core/cloud_coordinator.rs` exists but minimal
- Role not clear from usage

**Action**:
- [ ] Expand CloudCoordinator to explicitly manage all cloud request sequencing
- [ ] Ensure no cloud I/O bypasses runtime
- [ ] Consider renaming if it expands significantly

---

## Part 3: Dead Code Removal Checklist

### Code Marked `#[allow(dead_code)]` to Review

1. **Test Utilities** (`tests/common/`)
   - [ ] Review all dead-code-annotated functions
   - [ ] Consolidate related helpers into single module
   - [ ] Remove truly unused functions
   - [ ] Add `#[cfg(test)]` where appropriate

2. **Testutils** (`testutils/validate_tests.rs`)
   - [ ] Audit dead code annotations
   - [ ] Clean up unused validation helpers

3. **Test Hooks** (`src/common/test_hooks.rs`)
   - [ ] Ensure test hooks are actually used in tests
   - [ ] Remove unused hook types

### Legacy Filesystem Patterns
- [ ] Audit `.local()`, `.local_sst_dir()` for filesystem assumptions
- [ ] Mark deprecated in favor of cloud-sourced equivalents
- [ ] Add migration guide in comments

---

## Part 4: New Module Structure Diagram

```
src/
├── lib.rs                    (No change)
├── api/                      (Public API - no change)
├── cloud/                    (Cloud backends - no change)
├── common/                   (Shared types - minor cleanup)
├── config/                   (Configuration - no change)
├── core/
│   ├── engine/
│   │   ├── mod.rs           (Exports only)
│   │   ├── kv_store.rs      (MidgeEngine impl + KvStore trait - 300 lines)
│   │   ├── initialization.rs (Engine::new(), setup - 200 lines)
│   │   ├── column_families.rs (CF types + management - 200 lines)
│   │   ├── operations/       (Read, Write, Txn, etc - unchanged)
│   │   └── types.rs         (Result types)
│   ├── persistence/
│   │   ├── flush.rs         (Flush coordination - 100 lines)
│   │   ├── recovery.rs      (Cloud-first recovery - 150 lines)
│   │   └── mod.rs
│   ├── compaction/          (Already organized - no change)
│   ├── transaction/         (Already organized - no change)
│   ├── manifest/            (Already organized - no change)
│   ├── memtable/            (Already organized - no change)
│   ├── runtime.rs           (EngineRuntime - no change)
│   ├── cloud_coordinator.rs (Expand as needed)
│   ├── wal_upload_coordinator.rs (Already good)
│   ├── write_path/          (Consolidate or merge)
│   ├── data_structures/     (Already organized)
│   ├── backup/              (Already organized)
│   ├── locking/             (Already organized)
│   └── mod.rs
├── fs/                       (Filesystem operations - review)
├── health/                   (Health checks - no change)
├── metrics/                  (Metrics - no change)
├── sst/                      (SST format - no change)
└── wal/                      (WAL implementation - no change)

tests/
├── common/
│   ├── mod.rs              (Consolidate helpers from old mod.rs)
│   └── (remove old module files)
└── (integration tests - no structural change)
```

---

## Implementation Priority

### Phase 1: Low-Risk Cleanup (Week 1)
1. Remove truly dead code (unused test helpers)
2. Consolidate flush module structure
3. Fix module organization (merge small files)
4. Run full test suite

### Phase 2: Architectural Changes (Week 2-3)
1. Refactor recovery to be cloud-first
2. Integrate flush worker into EngineRuntime
3. Expand CloudCoordinator role
4. Update documentation

### Phase 3: Verification (Week 4)
1. Run full test suite
2. Performance benchmarks
3. Determinism tests
4. Cloud operation tests

---

## Files to Delete / Merge

| File | Action | Rationale |
|------|--------|-----------|
| `src/core/engine/flush_manager.rs` | Merge into flush_coordinator | Wrapper layer |
| `src/core/persistence/flush/worker.rs` | Integrate into runtime | Worker should not spawn own thread |
| `src/core/persistence/flush/mod.rs` | Merge into flush.rs | Single file module |
| `tests/common/test_helpers.rs` | Merge into mod.rs | Small utility file |

---

## Files to Refactor

| File | Changes | LOC Est |
|------|---------|---------|
| `src/core/engine/core.rs` | Split into kv_store + initialization | 300 + 200 |
| `src/core/persistence/wal_replay.rs` | Cloud-first recovery | +50 lines |
| `src/core/engine/column_family.rs` | Merge with cf_manager | -30 lines (consolidation) |
| `src/core/persistence/flush_coordinator.rs` | Simplify after worker integration | -50 lines |

---

## Testing Strategy

After each phase, run:

```bash
# Ensure compilation
cargo build --workspace

# Unit tests
cargo test --lib

# Integration tests
cargo test --test "*"

# Validation tests
cargo run --bin validate_tests -- --summary

# Benchmarks
cargo bench --bench tier1_hotpath
```

---

## Success Criteria

- [ ] All tests pass
- [ ] No `#[allow(dead_code)]` except for intentional future-proofing
- [ ] Core modules <300 LOC each
- [ ] All workers go through EngineRuntime
- [ ] Recovery is manifest-first (cloud-sourced)
- [ ] Module organization matches proposed structure
- [ ] Documentation updated to reflect new structure
