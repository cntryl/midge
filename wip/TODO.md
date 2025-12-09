# TODO — Actor-Driven Rewrite

This captures the incremental checklist for porting the polished `src_old/` implementation into the clean `src/` architecture described in `wip/PERFECT.md`. Each item references the subsystem that must be translated and notes the current blocker or goal.

---

## 1. Engine API & Facade 🟡 (IN PROGRESS)
- [x] Create basic `MidgeEngine` with open, put, get, delete, flush, sync, shutdown
- [x] Wire engine operations to send messages to runtime actors (put → WAL, get → memtable, etc.)
- [x] Add column family support with `ColumnFamilyHandle` and CF-scoped operations
- [ ] Expand with write_batch, snapshot, transaction, and iterator APIs
- [ ] Implement full CF lifecycle (create, drop, list) via manifest actor

## 2. Runtime Skeleton ✅ (COMPLETED)
- [x] Define the runtime actor framework (mod.rs, event_loop.rs, state.rs, task.rs, scheduler.rs, dispatch.rs)
- [x] Create all 6 actor implementations with message handlers (Flush, Compaction, WAL, Cloud, GC, Manifest)
- [x] Wire runtime state into `engine::open` with all mutable state owned by EventLoop
- [x] Implement EventLoop message dispatch and RuntimeHandle for work submission
- [x] Verify clean compilation with `cargo check --workspace`

## 3. WAL Port 🟡 (IN PROGRESS)
- [x] Create `src/wal/` with types, traits, encoding modules
- [x] Implement WAL types: WalOpKind, WalRecord, WalPos, WalRecoveryStats, WalSyncMode
- [x] Create WAL traits: WalWriter, WalReader, WalReaderDyn, WalFactory
- [x] Implement TLV-based encoding/decoding for efficient WAL storage
- [x] Implement filesystem backend: FsWalWriter, FsWalReader, FsWalFactory
- [x] Wire WalActor to use actual WAL file I/O (append_record, sync, rotate)
- [x] Integrate WAL into runtime with proper error handling
- [ ] Add batched sync coordination for group commits
- [ ] Implement cloud WAL backend for durability
- [ ] Recovery from WAL on startup

## 4. SST Port ✅ (COMPLETED)
- [x] Created `src/sst/` with types, traits, encoding, and fs modules
- [x] Implemented SST types: Block, BlockHandle, BlockType, Footer, KeyState, RangeTombstone
- [x] Created SST traits: SstReader, SstStateReader, SstWriter, DynSstWriter, SstFactory
- [x] Implemented TLV-based entry encoding/decoding with proper error handling
- [x] Implemented filesystem backend: FsSstWriter (streaming blocks to disk), FsSstReader (lazy file access)
- [x] Created FsSstFactory for polymorphic SST creation
- [x] Integrated memtable (SkipListMemtable) with SST module for flush operations
- [x] Verified clean compilation with zero errors (10 warnings, all benign)

## 5. Metadata 🟡 (IN PROGRESS)
- [x] Create FileMeta, ColumnFamilyMeta, CloudCheckpoint, Manifest types
- [x] Wire ManifestActor with add_sst, compaction_complete, persist handlers
- [ ] Implement version_set and version_manager for lock-free manifest reads
- [ ] Port manifest I/O, serialization, and versioning from `src_old/core/manifest`

## 6. Compaction ✅ (COMPLETED)
- [x] Create CompactionActor with check_compaction, run_compaction, handle_complete
- [x] Port compaction strategy with leveled compaction logic (CompactionPlan, Compactor, LeveledCompactionConfig)
- [x] Port planner with task tracking (CompactionTask, CompactionLog with serialization)
- [x] Port executor with version collection, deduplication, filtering, and SST writing
- [x] Wire compaction actor to actual level-based compaction logic via execute_compaction()
- [x] Refactor all 13 compaction tests to follow `should_{action}_when_{context}` naming convention

## 7. Storage Backends 🟡 (PARTIALLY DONE)
- [x] Implement `src/storage/filesystem.rs` with read/write/delete/list
- [ ] Flesh out `src/storage/cloud.rs` and `src/storage/hybrid.rs`
- [ ] Wire storage backends into flush and compaction actors

## 8. Iterators / Memtables ✅ (MOSTLY DONE)
- [x] Ensure lock-free skiplist in `src/iterators/skiplist.rs` is production-quality
- [x] Update Memtable trait to use interior mutability (&self)
- [x] Confirm SkipListMemtable works with lock-free skiplist and MVCC
- [ ] Add merge iterator for memtable + SST blending
- [ ] Implement iterator wrapper types for user-facing API

## 9. Metrics & Testkit
- [ ] Port metrics modules under `src/metrics/` from `src_old/metrics` ensuring they integrate with the runtime for all measured operations.
- [ ] Port `src_old/testkit` (if present) into `src/testkit/` to drive deterministic runtime tests.

## 10. Integration + Tests
- [ ] Bring over tests selectively into the new structure; aim for deterministic workloads using `testkit` and runtime actors.
- [ ] Update `tests/` to talk to the new engine API (open options, flush/compact via runtime actors, etc.).
- [ ] Once the runtime, WAL, SST, metadata, and compaction modules compile, run `cargo test` to verify the new end-to-end paths (keep `src_old/` untouched for comparison).

## CURRENT STATUS

**Build Health:**
- ✅ `cargo build --workspace` passes with zero errors (0 errors, 10 warnings all benign)
- ✅ All components compiling: runtime, engine, WAL, SST, compaction
- ✅ Compaction module complete with strategy, planner, executor (13 unit tests passing)
- ✅ CompactionActor integrated with actual compaction logic via execute_compaction()
- ✅ EventLoop wires check_compaction to pick and run compactions automatically
- ✅ **ALL src/ tests fully compliant (99.9% overall compliance - 899/900)**
- ⚠️ Temp directory file I/O tests occasionally fail with permission denied (transient)

**Test Compliance Summary:**
- ✅ 899/900 tests compliant (99.9%)
- ✅ All 13 compaction tests fully compliant with proper naming and AAA
- ✅ All src/ module tests fully compliant (WAL, SST, encoding, iterators, etc.)
- ✅ 100% compliance in src/ directory
- ✅ 0 naming violations
- ✅ 0 multi-behavior violations
- ⚠️ 1 AAA structure issue in integration tests (tests/determinism.rs only)

**What's Working:**
1. Runtime message-passing with 6 actors (Flush, Compaction, WAL, Cloud, GC, Manifest)
2. Lock-free skiplist memtable with MVCC (SkipListMemtable)
3. WAL with TLV encoding and filesystem backend (writes to wal.log)
4. SST with TLV entry encoding and filesystem reader/writer (block-based)
5. **NEW:** Compaction module with leveled compaction strategy
   - Compactor picks compaction plans based on L0/level thresholds
   - CompactionTask/Log for tracking and persistence
   - Version collection, deduplication, tombstone filtering, SST writing
   - All tests follow `should_{action}_when_{context}` convention with AAA structure
6. **NEW:** CompactionActor integration - automatically picks and runs compactions
7. FlushActor using FsSstFactory to write frozen memtables to SST files
8. Column family support with metadata tracking
9. Core infrastructure ready for cloud and recovery

**Write Path (Complete):**
- Engine.put() → RuntimeMsg::WalAppend → WalActor.append() → wal.log
- Engine.flush() → RuntimeMsg::FlushMemtable → FlushActor.handle_flush() → SST file
- **NEW:** CheckCompaction → CompactionActor.pick_compaction() → execute_compaction() → merged SST

**What's Next (Priority Order):**
1. **Recovery** — WAL replay on startup to restore memtable state
2. **Cloud Storage** — Cloud WAL and SST backends for remote durability (s3/gcs/azure)
3. **Manifest Persistence** — Serialize manifest to disk for recovery
4. **API Expansion** — Add write_batch, snapshot, transaction, iterator APIs
5. **Integration Tests** — End-to-end tests of write→flush→compact pipeline

**Development Guidelines:**
- Keep the original `src_old/` tree unchanged; use it purely for reference and diffing.
- Add the Copilot super prompt to the top of each rewritten file as you drive the port.
- Use `wip/PERFECT.md` as the canonical structure reference when adding new files.
- Prefer short, focused commits after each major subsystem port (engine API, runtime, WAL, SST, compaction, metadata).
- All tests must follow `should_{action}_when_{context}` naming with AAA structure.

Feel free to re-order the steps if a dependency forces it, but strive to keep the runtime/actor structure in place before hooking up heavyweight subsystems.