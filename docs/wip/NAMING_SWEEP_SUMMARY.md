# Codebase Naming Sweep: Findings & Recommendations

## Summary

Comprehensive audit of Midge codebase for areas where **implementation details** leak into public API rather than communicating **observable behavior**.

---

## Key Findings

### 🔴 HIGH PRIORITY: Module Names (Implementation Leakage)

**Problem**: These module names describe *how* something is implemented, not *what* it does.

```
wal/coordinator          → Coordinator pattern (impl detail)
wal/fs/group_commit      → GroupCommit optimization (impl detail)  
core/compaction/coordinator → Threading orchestration (impl detail)
core/compaction/executor → Execution pattern (impl detail)
health/manager           → Generic "manager" (no behavior signal)
core/transaction/manager → Generic "manager" (no behavior signal)
```

**Impact**: Users reading module docs wonder "coordinate what?" or "manage what?"

**Solution**: Rename or add behavior-focused documentation.

---

### 🟡 MEDIUM PRIORITY: Public Types with Implementation Names

**Problem**: Type names expose storage/optimization strategies:

```
BlockCache              → "Cache" implies optional storage layer
ShardedBlockCache       → "Sharded" reveals lock strategy  
AdaptiveBlockCache      → "Adaptive" reveals algorithm variant
CachedBlock/CachedTable → "Cached" mixes behavior with storage choice
LocalFileLock           → "LocalFile" reveals backend (should be abstract)
CloudLeaseLock          → "CloudLease" reveals backend (should be abstract)
GroupCommitConfig       → Related to renamed enum ✅
```

**Impact**: Users understand impl details but don't understand behavior from API.

**Recommendation**: Trait-based abstraction or clearer behavioral names.

---

### ✅ FIXED: WalSyncMode Enum

Successfully renamed `GroupCommit` → `BatchedSync`:
- ✅ Communicates behavior (batched synchronization)
- ✅ Consistent with other modes (NoSync, EveryWrite)
- ✅ No longer exposes optimization technique

---

## Scope Analysis

### Total Issues Found: ~15-20 potential improvements

**By Category**:
- Module names: 6 items
- Type names: 6 items  
- Config/state: 3 items
- Already fixed: 1 item ✅

**By Visibility**:
- Public API: ~8 items (breaking changes if renamed)
- Internal-exposed: ~7 items (can rename freely)
- Private/tests: ~5+ items (no action needed)

---

## Implementation vs Behavior Examples

### ❌ BAD NAMES (Implementation-Focused)
| Name | Leaks | Better |
|------|-------|--------|
| `GroupCommit` | Optimization technique | `BatchedSync` ✅ |
| `ShardedBlockCache` | Lock strategy | `BlockCache` (hide sharding) |
| `CompactionExecutor` | Threading model | `CompactionEngine` or `Compactor` |
| `WalCoordinator` | Orchestration pattern | `WalController` or `WalManager` |
| `LocalFileLock` | Storage backend | `FileLock` (trait-based) |

### ✅ GOOD NAMES (Behavior-Focused)
| Name | Communicates | Benefit |
|------|--------------|---------|
| `WalSyncMode::NoSync` | When syncing happens | User knows: no fsync |
| `WalSyncMode::EveryWrite` | When syncing happens | User knows: sync per write |
| `WalSyncMode::BatchedSync` | When syncing happens | User knows: syncs in batches |
| `HealthMonitor` | What it does | Monitors system health |
| `TransactionController` | What it does | Controls MVCC transactions |

---

## Recommended Action Plan

### Phase 1: Documentation (Zero Breaking Changes)
- [ ] Add behavioral docs to modules with generic names
- [ ] Example for `coordinator`: "Orchestrates write-ahead log durability and rotation"
- [ ] Example for `executor`: "Executes LSM-tree compaction, merging SST versions"

### Phase 2: Internal Refactoring (No Public Impact)
- [ ] Review which types should truly be public
- [ ] Move implementation-detail types to private submodules
- [ ] Create trait abstractions for storage choice types

### Phase 3: Strategic Renames (If Approved)
- [ ] `GroupCommitConfig` → `BatchedSyncConfig` (follows enum rename)
- [ ] Public cache types → trait-based abstractions
- [ ] Lock types → trait-based abstractions

### Phase 4: Public API Stabilization
- [ ] Document naming philosophy in CONTRIBUTING.md
- [ ] Add code review checklist: "Does this name describe behavior or implementation?"

---

## Quick Wins

These can be renamed immediately (low impact):

1. **`GroupCommitConfig`** → `BatchedSyncConfig`
   - Aligns with `WalSyncMode::BatchedSync` ✅
   - No public API change (only used internally with renamed enum)
   - 1-2 files to update

---

## Risk Assessment

### Safe to Rename (Internal/Low Impact):
- `GroupCommitConfig` — tied to internal enum
- Private module names — zero impact
- Comments/docs — zero impact

### Requires Decision (Breaking Changes):
- Module names if types are public
- Type names if part of public API contracts
- Anything in trait bounds

---

## Metrics

**Codebase Health**:
- ✅ 1 major issue identified and fixed: `WalSyncMode::GroupCommit`
- ⚠️ 6-8 modules with implementation-focused names
- ⚠️ 6 types with implementation-detail exposure
- ✅ 0 blocking issues (mostly documentation/internal renames)

---

## Next Steps

**Recommendation**: 
1. Run Phase 1 (Documentation) - Zero risk, immediate benefit
2. Review for Phase 2 (Internal Refactoring) - Low risk, code quality improvement
3. Plan Phase 3 (Public Renames) - Requires decision on API stability vs clarity

---

## Related Files

- `API_NAMING_AUDIT.md` — Detailed analysis with recommendations
- `src/wal/types.rs` — Contains fixed `WalSyncMode` enum
- Test files — Updated with `BatchedSync` variant
