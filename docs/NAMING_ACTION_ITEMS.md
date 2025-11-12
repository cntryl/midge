# Naming Sweep: Prioritized Action Items

## Quick Summary

**Scan Result**: ~20 naming issues where implementation details leak into public/exposed API
**Already Fixed**: `WalSyncMode::GroupCommit` → `WalSyncMode::BatchedSync` ✅
**New Issues Found**: 15-19 additional items across modules, types, and configs

---

## PRIORITY 1: Fix Related Config (1-2 hours)

### `GroupCommitConfig` → `BatchedSyncConfig`
- **Files to update**: 
  - `src/wal/fs/group_commit.rs` (struct definition + docs)
  - `src/wal/fs/mod.rs` (re-export)
  - `benches/hotpath/overhead_analysis.rs` (Layer 7)
  
- **Reason**: Aligns with fixed `WalSyncMode::BatchedSync` enum variant
- **Impact**: Internal-only struct, low risk
- **Status**: READY TO IMPLEMENT

---

## PRIORITY 2: Documentation (1-2 hours, zero breaking changes)

Add behavioral documentation to these vague module names:

### `src/wal/coordinator.rs`
```rust
//! Write-Ahead Log Control - manages durability guarantees and log rotation
//! 
//! Encapsulates WAL writer lifecycle for transactional operations
```

### `src/core/compaction/executor.rs`
```rust
//! Compaction Processing Engine - executes LSM-tree compaction merges
//!
//! Transforms multiple SST versions into compacted output tables
```

### `src/core/compaction/coordinator.rs`
```rust
//! Background Compaction Scheduler - manages when and what to compact
//!
//! Decides compaction strategy and coordinates worker thread lifecycle
```

### `src/health/manager.rs`
```rust
//! Database Health Monitor - checks system health and draining status
//!
//! Performs periodic probes and lifecycle validation
```

### `src/core/transaction/manager.rs`
```rust
//! MVCC Transaction Controller - manages multi-version concurrency control
//!
//! Coordinates snapshot isolation and write conflict detection
```

---

## PRIORITY 3: Type Visibility Review (2-3 hours)

### Candidates for Trait-Based Abstraction (Hide Implementation)

These types expose internal storage/optimization choices:

```
src/sst/block_cache.rs
  ├─ BlockCache (expose only via trait)
  ├─ ShardedBlockCache (hide impl, expose behavior)
  └─ AdaptiveBlockCache (hide impl, expose behavior)

src/core/locking/
  ├─ LocalFileLock (expose as trait Lock)
  └─ CloudLeaseLock (expose as trait Lock)
```

**Action**: Review if types should be public. If yes, create trait abstraction:
```rust
pub trait Lock: Send + Sync {
    fn acquire(&self) -> Result<Guard>;
    fn try_acquire(&self) -> Result<Option<Guard>>;
}
// Hide: LocalFileLock, CloudLeaseLock (impl detail)
```

---

## PRIORITY 4: Public Type Renames (Requires Decision)

### Lower Priority (Nice to Have, Breaking Changes)

| Current Name | Issue | Recommended | Risk |
|---|---|---|---|
| `BlockHandle` | "Block" is impl detail | `SegmentRef` or `TableSegment` | HIGH (public API) |
| `CachedBlock` | "Cached" mixes storage choice with identity | `BlockRef` or `LoadedBlock` | HIGH (public API) |
| `CachedTable` | "Cached" mixes storage choice with identity | `TableRef` or `LoadedTable` | HIGH (public API) |
| `BlockType` enum | "Block" is storage unit (impl) | Keep private or rename | MEDIUM |

---

## PRIORITY 5: Module Reorganization (3-5 hours, minimal breaking)

These generic names could be clarified:

| Module | Current Path | Suggested Organization | Reason |
|--------|--------------|------------------------|--------|
| `group_commit` | `wal/fs/group_commit.rs` | Keep internal, rename to `batched_sync` | Optimization detail |
| `executor` | `core/compaction/executor.rs` | Keep but improve docs | Execution engine pattern |
| `coordinator` (×2) | `wal/`, `compaction/` | Keep but add clear docs | Orchestration pattern |

---

## Implementation Checklist

### [ ] QUICK WIN: Rename `GroupCommitConfig` → `BatchedSyncConfig`
- Update struct definition
- Update re-exports
- Update all usages (estimated 3-4 files)
- Verify compilation
- **Estimated Time**: 30 minutes

### [ ] Documentation Sprint
- [ ] Add behavioral docs to all "coordinator", "executor", "manager" modules
- [ ] Add behavior-focused doc comments
- [ ] Link to internals without exposing impl
- **Estimated Time**: 1-2 hours

### [ ] Type Abstraction Review
- [ ] Audit block_cache.rs exports
- [ ] Audit locking trait boundaries
- [ ] Document decision (public vs private)
- **Estimated Time**: 1-2 hours

### [ ] Breaking Changes (Optional)
- [ ] Decide on public type renames (needs product decision)
- [ ] Plan deprecation/aliasing strategy if needed
- **Estimated Time**: TBD

---

## Decision Tree

```
START: "Should I rename this?"
  ├─ Is it INTERNAL only? 
  │  └─ YES → RENAME IT (config, private modules)
  │  └─ NO → continue...
  ├─ Is it in public API contract?
  │  └─ YES → needs decision/deprecation
  │  └─ NO → continue...
  ├─ Does the name describe behavior or implementation?
  │  └─ BEHAVIOR → KEEP IT
  │  └─ IMPL DETAIL → HIDE BEHIND TRAIT or RENAME
  └─ DONE: Document decision
```

---

## Related Documentation

- `API_NAMING_AUDIT.md` — Comprehensive analysis
- `NAMING_SWEEP_SUMMARY.md` — Full findings
- `docs/DEPENDENCY_ANALYSIS.md` — Architecture reference

---

## Success Metrics

After implementing these actions:

- ✅ All public module docs clearly state **behavior** purpose
- ✅ No implementation-detail names leak into public types  
- ✅ Config types use consistent naming (e.g., `BatchedSyncConfig`)
- ✅ Code review checklist updated with naming guidelines
- ✅ New developers understand "name = behavior, not implementation"
