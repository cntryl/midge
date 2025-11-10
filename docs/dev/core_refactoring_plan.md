# Core Module Refactoring Plan

## Current Problems

### 1. **God Object: `engine.rs` (2767 lines, 78 methods)**
- Handles: WAL coordination, memtables, compaction, flushing, snapshots, transactions, metrics, caching, cloud ops, locks, column families
- Violates Single Responsibility Principle
- Hard to test individual concerns
- High cognitive load for contributors

### 2. **Mixed Concerns in Top-Level Files**
- `manifest.rs` (1200 lines): Mixes serialization, file I/O, and business logic
- `lock.rs` (787 lines): Contains both local file locks AND cloud lease locks
- `backup.rs` (857 lines): Combines backup creation and restore in one file
- `skiplist.rs` (864 lines): Data structure implementation mixed with versioning logic

### 3. **Shallow Directory Structure**
- `engine/` has 5 files but only types/factory are properly split
- `compaction/` has good structure but `executor.rs` is 1796 lines
- No clear separation between "data structures", "persistence", "coordination"

### 4. **Poor Encapsulation**
- Many `pub(super)` and `pub(crate)` leaks throughout
- Column family logic split between `engine.rs` and `column_family.rs`
- Flush/compaction logic scattered across engine, coordinator, and worker files

---

## Refactoring Strategy

### Phase 1: Extract Engine Subsystems (Priority: HIGH)

#### 1.1 Create `engine/operations/` directory
Split `engine.rs` into focused operation modules:

```
src/core/engine/operations/
├── mod.rs                  // Re-exports
├── reads.rs                // get(), multi_get(), scan operations
├── writes.rs               // put(), delete(), delete_range()
├── mutations.rs            // insert(), compare_and_swap(), merge()
├── snapshots.rs            // Snapshot creation & management
├── transactions.rs         // Transaction coordination
└── lifecycle.rs            // open(), close(), shutdown logic
```

**Rationale**: Each operation category has distinct error handling, performance characteristics, and testing needs.

#### 1.2 Create `engine/state/` directory
Extract engine state management:

```
src/core/engine/state/
├── mod.rs
├── engine_state.rs         // Core MidgeEngine struct (fields only)
├── initialization.rs       // open(), open_with_config(), recovery
├── cache_management.rs     // Block cache, table cache, bloom cache setup
└── shutdown.rs             // Drop implementation, cleanup logic
```

**Rationale**: State initialization is complex (400+ lines in factory.rs). Needs separate testing.

#### 1.3 Create `engine/coordination/` directory
Extract background task coordination:

```
src/core/engine/coordination/
├── mod.rs
├── flush_manager.rs        // Memtable freeze & flush orchestration
├── compaction_scheduler.rs // When/how to trigger compaction
└── wal_manager.rs          // WAL rotation, sync, recovery integration
```

**Rationale**: These are distinct orchestration concerns that currently pollute the main engine logic.

---

### Phase 2: Split Domain Aggregates (Priority: HIGH)

#### 2.1 Refactor `manifest.rs` → `manifest/` module

```
src/core/manifest/
├── mod.rs                  // Re-exports Manifest struct
├── manifest.rs             // Core Manifest struct & methods (< 400 lines)
├── file_meta.rs            // FileMeta, ColumnFamilyMeta structs
├── checkpoint.rs           // CloudCheckpoint logic
├── io.rs                   // load(), save(), persistence
├── queries.rs              // files_for_scan(), files_at_level()
└── compaction_tracking.rs  // add_file(), delete_file(), level management
```

**Benefits**:
- Clear separation: data model vs I/O vs queries
- Easier to add new manifest features (e.g., compaction history)
- Testable in isolation

#### 2.2 Refactor `lock.rs` → `locking/` module

```
src/core/locking/
├── mod.rs                  // DbLock trait re-export
├── traits.rs               // DbLock trait definition
├── local.rs                // LocalFileLock implementation
├── cloud.rs                // CloudLeaseLock implementation
├── meta.rs                 // LockMeta serialization
└── factory.rs              // acquire_db_lock() logic
```

**Benefits**:
- Clear strategy pattern for different lock backends
- Easy to add new lock types (e.g., Redis-based coordination)
- Local and cloud concerns isolated

#### 2.3 Refactor `backup.rs` → `backup/` module

```
src/core/backup/
├── mod.rs
├── backup_engine.rs        // BackupEngine implementation
├── restore_engine.rs       // RestoreEngine implementation
├── info.rs                 // BackupInfo, SstFileInfo structs
└── options.rs              // BackupOptions, RestoreOptions
```

---

### Phase 3: Extract Data Structures (Priority: MEDIUM)

#### 3.1 Move `skiplist.rs` to `datastructures/skiplist/`

```
src/core/datastructures/
└── skiplist/
    ├── mod.rs              // Public API exports
    ├── skiplist.rs         // SkipList struct & basic operations
    ├── node.rs             // Node, VersionNode structs
    ├── versioning.rs       // MVCC version chain logic
    ├── iterator.rs         // SkipListIterator implementation
    └── splice.rs           // Splice optimization logic
```

**Rationale**: Skiplist is a reusable data structure. Could be extracted to a separate crate eventually.

#### 3.2 Move `memtable.rs` to `memtable/`

```
src/core/memtable/
├── mod.rs
├── memtable.rs             // MemTable struct & operations
├── range_tombstones.rs     // RangeTombstones implementation
└── wal_loading.rs          // load_from_wal() logic
```

---

### Phase 4: Improve Compaction Structure (Priority: MEDIUM)

#### 4.1 Split `compaction/executor.rs` (1796 lines!)

```
src/core/compaction/
├── mod.rs
├── coordinator.rs          // High-level scheduling (keep as-is)
├── strategy.rs             // Compaction strategy logic (keep as-is)
├── filter.rs               // CompactionFilter trait (keep as-is)
└── execution/              // NEW: Split executor
    ├── mod.rs
    ├── executor.rs         // Public API (< 300 lines)
    ├── version_collection.rs   // collect_compaction_versions()
    ├── merging.rs          // Merge logic, deduplication
    ├── filtering.rs        // apply_compaction_filter(), tombstone filtering
    └── output_writer.rs    // write_compacted_sst(), output file creation
```

---

### Phase 5: Clarify Flush Architecture (Priority: LOW)

#### 5.1 Refactor `flush.rs` → `flush/` module

```
src/core/flush/
├── mod.rs
├── job.rs                  // FlushJob struct
├── worker.rs               // spawn_flush_worker(), background thread
├── processor.rs            // process_flush_job() logic
└── bounds.rs               // compute_bounds() helper
```

**Rationale**: Flush is simpler than compaction but still has distinct phases (queuing, processing, I/O).

---

## Proposed Directory Structure (After Refactoring)

```
src/core/
├── mod.rs                      // Clean re-exports only

├── engine/
│   ├── mod.rs
│   ├── engine.rs               // NEW: Thin facade delegating to subsystems
│   ├── factory.rs              // Keep: High-level open() logic
│   ├── types.rs                // Keep: InsertResult, CasResult
│   ├── column_family.rs        // Keep: CF management
│   ├── state/                  // NEW
│   │   ├── mod.rs
│   │   ├── engine_state.rs
│   │   ├── initialization.rs
│   │   ├── cache_management.rs
│   │   └── shutdown.rs
│   ├── operations/             // NEW
│   │   ├── mod.rs
│   │   ├── reads.rs
│   │   ├── writes.rs
│   │   ├── mutations.rs
│   │   ├── snapshots.rs
│   │   ├── transactions.rs
│   │   └── lifecycle.rs
│   └── coordination/           // NEW
│       ├── mod.rs
│       ├── flush_manager.rs
│       ├── compaction_scheduler.rs
│       └── wal_manager.rs

├── manifest/                   // NEW: Split from manifest.rs
│   ├── mod.rs
│   ├── manifest.rs
│   ├── file_meta.rs
│   ├── checkpoint.rs
│   ├── io.rs
│   ├── queries.rs
│   └── compaction_tracking.rs

├── locking/                    // NEW: Split from lock.rs
│   ├── mod.rs
│   ├── traits.rs
│   ├── local.rs
│   ├── cloud.rs
│   ├── meta.rs
│   └── factory.rs

├── backup/                     // NEW: Split from backup.rs
│   ├── mod.rs
│   ├── backup_engine.rs
│   ├── restore_engine.rs
│   ├── info.rs
│   └── options.rs

├── memtable/                   // NEW: Split from memtable.rs
│   ├── mod.rs
│   ├── memtable.rs
│   ├── range_tombstones.rs
│   └── wal_loading.rs

├── datastructures/             // NEW: Extract skiplist
│   └── skiplist/
│       ├── mod.rs
│       ├── skiplist.rs
│       ├── node.rs
│       ├── versioning.rs
│       ├── iterator.rs
│       └── splice.rs

├── compaction/
│   ├── mod.rs
│   ├── coordinator.rs          // Keep
│   ├── strategy.rs             // Keep
│   ├── filter.rs               // Keep
│   └── execution/              // NEW: Split executor.rs
│       ├── mod.rs
│       ├── executor.rs
│       ├── version_collection.rs
│       ├── merging.rs
│       ├── filtering.rs
│       └── output_writer.rs

├── flush/                      // NEW: Split from flush.rs
│   ├── mod.rs
│   ├── job.rs
│   ├── worker.rs
│   ├── processor.rs
│   └── bounds.rs

├── metrics/                    // Keep as-is
│   ├── mod.rs
│   ├── engine.rs
│   ├── performance.rs
│   └── timer.rs

├── flush_coordinator.rs        // Keep (only 269 lines, focused)
├── merge_iterator.rs           // Keep (306 lines, cohesive)
├── storage_mode.rs             // Keep (354 lines, well-scoped)
├── transaction_manager.rs      // Keep (426 lines, reasonable)
└── wal_replay.rs               // Keep (227 lines, focused)
```

---

## Implementation Guidelines

### 1. **Incremental Migration**
- Refactor one module at a time
- Maintain backward compatibility via re-exports in `mod.rs`
- Run full test suite after each phase
- Update imports incrementally (can use `pub use` in old locations temporarily)

### 2. **Visibility Rules**
- New submodules should expose minimal `pub` API
- Use `pub(crate)` sparingly - prefer proper module boundaries
- Each module should have a clear "public interface" documented in its `mod.rs`

### 3. **Testing Strategy**
- Add unit tests for newly extracted modules (easier now that they're isolated)
- Integration tests stay in `tests/` directory (no changes needed)
- Use `#[cfg(test)]` mod tests for module-private logic

### 4. **Documentation**
- Each new module gets a module-level doc comment explaining its purpose
- Document the "contracts" between modules (e.g., engine → flush_manager)
- Update `docs/dev/code_guidelines.md` with new structure

---

## Benefits Summary

### Before:
- 5 files > 800 lines (hard to navigate)
- Unclear where to add new features
- High coupling between concerns
- Difficult to test in isolation
- Long compile times when changing engine.rs

### After:
- No file > 500 lines (target < 400)
- Clear separation of concerns (reads/writes/compaction/flush/manifest)
- Easier to parallelize development (work on different modules)
- Better testability (mock interfaces between modules)
- Faster incremental compilation
- Clearer mental model for new contributors

---

## Migration Checklist

### Phase 1: Engine Operations (1-2 days)
- [ ] Create `engine/operations/` directory
- [ ] Extract read operations (get, multi_get, scan)
- [ ] Extract write operations (put, delete, delete_range)
- [ ] Extract mutation operations (insert, CAS, merge)
- [ ] Extract snapshot operations
- [ ] Extract transaction operations
- [ ] Extract lifecycle operations (open, close)
- [ ] Update `engine.rs` to delegate to operation modules
- [ ] Run tests: `cargo test --lib core::engine`

### Phase 2: Engine State (1 day)
- [ ] Create `engine/state/` directory
- [ ] Extract engine state struct definition
- [ ] Extract initialization logic
- [ ] Extract cache management
- [ ] Extract shutdown logic
- [ ] Run tests

### Phase 3: Manifest Split (1 day)
- [ ] Create `manifest/` directory
- [ ] Split manifest.rs into submodules
- [ ] Update imports throughout codebase
- [ ] Run tests: `cargo test --lib core::manifest`

### Phase 4: Lock Split (0.5 days)
- [ ] Create `locking/` directory
- [ ] Split lock.rs into trait/local/cloud
- [ ] Update imports
- [ ] Run tests

### Phase 5: Backup Split (0.5 days)
- [ ] Create `backup/` directory
- [ ] Split backup.rs into backup/restore
- [ ] Update imports
- [ ] Run tests

### Phase 6: Compaction Executor (1 day)
- [ ] Create `compaction/execution/` directory
- [ ] Split executor.rs into focused modules
- [ ] Update compaction coordinator
- [ ] Run tests: `cargo test --lib core::compaction`

### Phase 7: Optional Cleanups (As needed)
- [ ] Extract skiplist to datastructures/
- [ ] Split memtable module
- [ ] Split flush module

---

## Risk Mitigation

1. **Breaking Changes**: Use re-exports to maintain backward compatibility during migration
2. **Test Coverage**: Run full test suite after each phase
3. **Performance**: Profile before/after to ensure no regression
4. **Merge Conflicts**: Communicate refactoring plan with team, coordinate timing
5. **Rollback Plan**: Each phase is independently committable - can pause/revert anytime

---

## Open Questions

1. Should `skiplist` be extracted to a separate crate? (It's generic, reusable)
2. Should `transaction_manager.rs` be split into `transactions/` module?
3. Should `flush_coordinator.rs` and `compaction/coordinator.rs` move to `engine/coordination/`?
4. Do we want to introduce trait boundaries between modules (e.g., `FlushService`, `CompactionService`)?

---

## Success Metrics

- [ ] No core file > 500 lines
- [ ] Each module has < 10 public items in its API
- [ ] 90%+ test coverage on new modules
- [ ] Zero performance regression (run benchmarks)
- [ ] Documentation updated
- [ ] CI passes on all platforms
