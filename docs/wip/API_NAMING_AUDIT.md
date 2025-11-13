# API Naming Audit: Behavior vs Implementation

## Executive Summary

This document identifies areas where the public API reveals implementation details rather than communicating observable behavior. The goal is to maintain a user-centric API that communicates "what it does" not "how it works internally."

---

## Categories of Issues

### 1. **Module Names Revealing Implementation** (HIGH PRIORITY)

These module names communicate implementation strategy rather than purpose:

| Module | Current | Issue | Recommended |
|--------|---------|-------|-------------|
| `wal/coordinator` | "Coordinator" | Describes orchestration pattern (impl) | `wal/controller` or `wal/writer_manager` |
| `wal/fs/group_commit` | "GroupCommit" | Describes optimization technique (impl) | `wal/fs/batched_sync` or keep private |
| `core/compaction/coordinator` | "Coordinator" | Describes threading pattern (impl) | `compaction/scheduler` or `compaction/controller` |
| `core/compaction/executor` | "Executor" | Describes execution pattern (impl) | `compaction/worker` or `compaction/engine` |
| `health/manager` | "Manager" | Generic, vague (impl leakage) | `health/monitor` or `health/checker` |
| `core/transaction/manager` | "Manager" | Generic, vague (impl leakage) | `transaction/controller` or `mvcc/coordinator` |

---

### 2. **Public Types Revealing Internal Data Structures** (MEDIUM PRIORITY)

These struct/enum names expose storage choices:

| Type | Module | Issue | Recommendation |
|------|--------|-------|-----------------|
| `BlockCache` | `sst/block_cache` | "Cache" suggests it's optional impl detail | Consider renaming or keeping internal |
| `ShardedBlockCache` | `sst/block_cache` | "Sharded" reveals lock strategy (impl) | Hide behind trait if public |
| `AdaptiveBlockCache` | `sst/block_cache` | "Adaptive" reveals algorithm choice (impl) | Hide behind trait if public |
| `CachedBlock` / `CachedTable` | `sst/` | "Cached" suggests caching is a storage choice | Better: `BlockRef`, `TableRef` |
| `LocalFileLock` | `core/locking/` | "LocalFile" reveals storage backend | Better: just `FileLock` (or keep private) |
| `CloudLeaseLock` | `core/locking/` | "CloudLease" reveals backend strategy | Better: just `Lock` (or keep private) |

---

### 3. **Configuration/State Types with Implementation Names** (MEDIUM PRIORITY)

| Type | Issue | Recommendation |
|------|-------|-----------------|
| `GroupCommitConfig` | "GroupCommit" = impl detail | `BatchedSyncConfig` or `SyncBatchConfig` |
| `BlockType` enum | "Block" is internal storage unit | Consider if this should be public at all |
| `BlockHandle` | "Block" is impl detail; users care about SSTable segments | Rename to `SegmentRef` or keep private |

---

### 4. **Already-Fixed Issues** ✅

| Type | Status | Details |
|------|--------|---------|
| `WalSyncMode::GroupCommit` | ✅ FIXED | Renamed to `BatchedSync` |

---

## Severity Assessment

### ✅ SAFE (Implementation Details, Shouldn't Expose)
- `group_commit.rs` (internal module, fine as-is)
- `BlockCache` internals (use trait abstraction)
- Lock implementations (use trait abstraction)

### ⚠️ NEEDS REVIEW
- Module names like "coordinator", "executor", "manager" (too generic/vague)
- If types are public: consider what behavior they expose vs impl they leak

### 🔴 PRIORITY FIXES
1. `WalSyncMode` enum — DONE ✅
2. Module documentation clarifying purpose (not implementation)
3. Public API audit for trait-based abstractions

---

## Action Plan

### Phase 1: Module Documentation (No Breaking Changes)
- [ ] Add comprehensive module-level docs explaining **behavior purpose** not implementation
- [ ] Example: "WAL Coordinator" → "Write-Ahead Log Control (manages log durability and rotation)"

### Phase 2: Strategic Renames (Review Required)
- [ ] Review which modules/types should be public vs internal
- [ ] If internal: move to private submodules or crate modules
- [ ] If public: ensure names communicate behavior, not impl

### Phase 3: Trait-Based Abstractions
- [ ] Ensure cache implementations are behind traits
- [ ] Ensure locking implementations are behind traits
- [ ] This hides impl details while exposing behavior

---

## Naming Philosophy for Remaining Work

**Rule**: Public API names should answer "What does this do?" not "How is it implemented?"

✅ GOOD:
- `WalSyncMode::NoSync` — user understands: no synchronization
- `WalSyncMode::EveryWrite` — user understands: sync per write
- `WalSyncMode::BatchedSync` — user understands: sync in batches

❌ BAD:
- `WalCoordinator` — user doesn't know: orchestrates what?
- `ShardedBlockCache` — user sees: lock sharding strategy (impl)
- `CompactionExecutor` — user doesn't know: executes what?

---

## Next Steps

1. **Decision**: Which modules/types should be public?
2. **Decision**: Acceptable to rename modules if semantically better?
3. **Review**: Do public names need aliases for backward compat?

This is an active codebase, so these are recommendations for consistency, not blocking issues.
