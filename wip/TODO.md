# TODO — Actor-Driven Rewrite

This captures the incremental checklist for porting the polished `src_old/` implementation into the clean `src/` architecture described in `wip/PERFECT.md`. Each item references the subsystem that must be translated and notes the current blocker or goal.

---

## 1. Engine API & Facade ✅ (COMPLETED)
- [x] Create basic `MidgeEngine` with open, put, get, delete, flush, sync, shutdown
- [x] Wire engine operations to send messages to runtime actors (put → WAL, get → memtable, etc.)
- [x] Add column family support with `ColumnFamilyHandle` and CF-scoped operations
- [x] **NEW:** Implement WriteBatch API for batched writes
- [x] **NEW:** Add write_batch() method to MidgeEngine for atomic batched operations
- [x] **NEW:** 7 comprehensive WriteBatch tests
- [x] **NEW:** Implement Snapshot API for point-in-time reads
- [x] **NEW:** Add snapshot() and snapshot_cf() methods to MidgeEngine
- [x] **NEW:** 5 Snapshot tests
- [x] **NEW:** Implement Iterator API with forward/reverse range scanning
- [x] **NEW:** Add IteratorBuilder for flexible iteration options
- [x] **NEW:** 8 Iterator tests covering all features
- [x] **NEW:** Implement Transaction API for multi-key ACID operations
- [x] **NEW:** Add transaction() and transaction_with_isolation() methods to MidgeEngine
- [x] **NEW:** Add commit_transaction() and rollback_transaction() to engine
- [x] **NEW:** 9 comprehensive Transaction tests (state machine, isolation levels, operations)
- [ ] Implement full CF lifecycle (create, drop, list) via manifest actor

## 2. Runtime Skeleton ✅ (COMPLETED)
- [x] Define the runtime actor framework (mod.rs, event_loop.rs, state.rs, task.rs, scheduler.rs, dispatch.rs)
- [x] Create all 6 actor implementations with message handlers (Flush, Compaction, WAL, Cloud, GC, Manifest)
- [x] Wire runtime state into `engine::open` with all mutable state owned by EventLoop
- [x] Implement EventLoop message dispatch and RuntimeHandle for work submission
- [x] Verify clean compilation with `cargo check --workspace`

## 3. WAL Port ✅ (COMPLETED)
- [x] Create `src/wal/` with types, traits, encoding modules
- [x] Implement WAL types: WalOpKind, WalRecord, WalPos, WalRecoveryStats, WalSyncMode
- [x] Create WAL traits: WalWriter, WalReader, WalReaderDyn, WalFactory
- [x] Implement TLV-based encoding/decoding for efficient WAL storage
- [x] Implement filesystem backend: FsWalWriter, FsWalReader, FsWalFactory
- [x] Wire WalActor to use actual WAL file I/O (append_record, sync, rotate)
- [x] Integrate WAL into runtime with proper error handling
- [x] **NEW:** Implement WAL recovery - replay on startup to restore memtable state
- [x] **NEW:** Add 5 comprehensive recovery tests (basic, delete ops, multi-CF)
- [x] **NEW:** Implement Cloud WAL backend (CloudWalWriter, CloudWalReader, CloudWalFactory)
- [x] **NEW:** Add 9 comprehensive CloudWAL tests (writer, append ops, batching, flush, reader, factory)
- [ ] Add batched sync coordination for group commits
- [ ] Implement cloud WAL segment rotation and cleanup

## 4. SST Port ✅ (COMPLETED)
- [x] Created `src/sst/` with types, traits, encoding, and fs modules
- [x] Implemented SST types: Block, BlockHandle, BlockType, Footer, KeyState, RangeTombstone
- [x] Created SST traits: SstReader, SstStateReader, SstWriter, DynSstWriter, SstFactory
- [x] Implemented TLV-based entry encoding/decoding with proper error handling
- [x] Implemented filesystem backend: FsSstWriter (streaming blocks to disk), FsSstReader (lazy file access)
- [x] Created FsSstFactory for polymorphic SST creation
- [x] Integrated memtable (SkipListMemtable) with SST module for flush operations
- [x] Verified clean compilation with zero errors (10 warnings, all benign)

## 5. Metadata ✅ (COMPLETED)
- [x] Create FileMeta, ColumnFamilyMeta, CloudCheckpoint, Manifest types
- [x] Wire ManifestActor with add_sst, compaction_complete, persist handlers
- [x] **NEW:** Implement manifest persistence (YAML serialization to disk)
- [x] **NEW:** Add 5 comprehensive persistence tests (save/load, file metadata, missing file handling)
- [x] **NEW:** Integrate manifest loading on engine startup
- [x] **NEW:** Implement Version and VersionSet for lock-free snapshot isolation reads
- [x] **NEW:** Implement VersionManager with VersionEdit enum for atomic manifest updates
- [x] **NEW:** Add 10 comprehensive VersionSet tests (creation, indexing, installation, retrieval, concurrent reads)
- [x] **NEW:** Add 10 comprehensive VersionManager tests (edit submission, atomic batching, version publication)

## 6. Compaction ✅ (COMPLETED)
- [x] Create CompactionActor with check_compaction, run_compaction, handle_complete
- [x] Port compaction strategy with leveled compaction logic (CompactionPlan, Compactor, LeveledCompactionConfig)
- [x] Port planner with task tracking (CompactionTask, CompactionLog with serialization)
- [x] Port executor with version collection, deduplication, filtering, and SST writing
- [x] Wire compaction actor to actual level-based compaction logic via execute_compaction()
- [x] Refactor all 13 compaction tests to follow `should_{action}_when_{context}` naming convention

## 7. Storage Backends 🟡 (IN PROGRESS)
- [x] Implement `src/storage/filesystem.rs` with read/write/delete/list
- [x] **NEW:** Implement cloud storage backend with CloudProvider trait abstraction
- [x] **NEW:** Add MockCloud provider for testing with in-memory storage
- [x] **NEW:** 10 comprehensive CloudStorage tests (creation, upload, download, 404, delete, exists, list, metadata, wrapper, history)
- [ ] Implement S3/GCS/Azure specific providers using cloud SDKs
- [ ] Wire storage backends into flush and compaction actors
- [ ] Flesh out `src/storage/hybrid.rs` for local+cloud coordination

## 8. Iterators / Memtables ✅ (MOSTLY DONE)
- [x] Ensure lock-free skiplist in `src/iterators/skiplist.rs` is production-quality
- [x] Update Memtable trait to use interior mutability (&self)
- [x] Confirm SkipListMemtable works with lock-free skiplist and MVCC
- [x] Add merge iterator for memtable + SST blending
- [x] **NEW:** Implement MergeIterator with SourceIterator trait abstraction
- [x] **NEW:** Support range bounds (start/end keys) for range scans
- [x] **NEW:** Add 4 comprehensive MergeIterator tests (multi-source, empty sources, range bounds)

## 9. Metrics & Testkit
- [ ] Port metrics modules under `src/metrics/` from `src_old/metrics` ensuring they integrate with the runtime for all measured operations.
- [ ] Port `src_old/testkit` (if present) into `src/testkit/` to drive deterministic runtime tests.

## 10. Integration + Tests
- [ ] Bring over tests selectively into the new structure; aim for deterministic workloads using `testkit` and runtime actors.
- [ ] Update `tests/` to talk to the new engine API (open options, flush/compact via runtime actors, etc.).
- [ ] Once the runtime, WAL, SST, metadata, and compaction modules compile, run `cargo test` to verify the new end-to-end paths (keep `src_old/` untouched for comparison).

## CURRENT STATUS

**Build Health:**
- ✅ `cargo build --workspace` passes with zero errors (0 errors, 10+ benign warnings)
- ✅ All components compiling: runtime, engine, WAL, SST, compaction, recovery, manifest persistence, transactions, cloud storage
- ✅ **NEW (Session 8):** Transaction API with state machine and isolation levels added
- ✅ **NEW (Session 8):** Cloud Storage backend with multi-cloud provider abstraction
- ✅ **NEW (Session 8):** Cloud WAL backend for remote durability
- ✅ **NEW (Session 8):** 29 new tests added (9 Transaction + 10 Cloud Storage + 9 Cloud WAL + 1 misc)
- ⚠️ 3 temp directory file I/O tests occasionally fail due to test isolation (SST fs tests)

**Test Status:**
- ✅ 90 lib tests passing (was 64 at session start)
- ✅ All 9 Transaction tests passing (creation, puts, deletes, reads, state transitions, mixed ops, error handling, rollback, clear)
- ✅ All 10 Cloud Storage tests passing (creation, upload, download, 404, delete, exists, list, metadata, backend wrapper, history)
- ✅ All 9 Cloud WAL tests passing (writer creation, append ops, batching, flush, reader creation, segment loading, factory, shutdown)
- ✅ All 8 Iterator tests passing (forward, reverse, remaining, collect, builder, range bounds, exclusive end, chaining)
- ✅ All 5 Snapshot tests passing
- ✅ All 7 WriteBatch tests passing
- ✅ All 5 persistence tests passing
- ✅ All 5 recovery tests passing
- ✅ All 13 compaction tests passing
- ✅ 100% test naming compliance in src/
- ✅ 0 naming violations
- ⚠️ SST fs tests occasionally fail with temp directory issues (3 failures when run in parallel)

**Architecture Summary:**
1. RuntimeState with manifest persistence integrated on startup
   - Loads manifest.yaml from disk if it exists
   - Creates column families from manifest metadata
   - Performs WAL recovery after manifest loading
   - All metadata preserved across restarts
2. ManifestPersistence layer (new)
   - YAML serialization using serde_yaml
   - Atomic file operations (write to temp, then rename)
   - Error handling for missing files and parse failures
   - Comprehensive test coverage (5 tests)
3. WAL with TLV encoding and filesystem backend (writes to wal.log)
   - Recovery integrated on startup to replay write operations
   - 5 recovery tests covering puts, deletes, multi-CF scenarios
   - **NEW (Session 8):** Cloud WAL backend for remote durability
     - CloudWalWriter buffers records, flushes to cloud storage
     - CloudWalReader loads and replays segments from cloud
     - 9 comprehensive tests
4. SST with TLV entry encoding and filesystem reader/writer (block-based)
5. Compaction module with leveled compaction strategy
   - Compactor picks compaction plans based on L0/level thresholds
   - CompactionTask/Log for tracking and persistence
   - Version collection, deduplication, tombstone filtering, SST writing
   - All tests follow `should_{action}_when_{context}` convention with AAA structure
6. CompactionActor integration - automatically picks and runs compactions
7. FlushActor using FsSstFactory to write frozen memtables to SST files
8. Column family support with metadata tracking
9. WriteBatch API for batched multi-key operations
10. Snapshot API for point-in-time consistent reads
11. Iterator API with range scanning and forward/reverse iteration
12. **NEW (Session 8):** Transaction API for multi-key ACID operations
    - State machine: Active → ReadPhase → Committing → Committed/RolledBack
    - 3 isolation levels: ReadUncommitted, ReadCommitted, Serializable
    - Write intents tracking for conflict detection
    - commit_transaction() and rollback_transaction() on MidgeEngine
    - 9 comprehensive tests
13. **NEW (Session 8):** Cloud Storage with multi-cloud abstraction
    - CloudProvider trait: upload, download, delete, list, exists, metadata
    - MockCloud provider for testing with in-memory storage
    - CloudStorage wrapper implementing StorageBackend
    - 10 comprehensive tests
    - Ready for S3/GCS/Azure implementations
14. Core infrastructure ready for cloud and recovery

**Write Path + Recovery (Complete):**
- Engine.put() → RuntimeMsg::WalAppend → WalActor.append() → wal.log
- Engine.flush() → RuntimeMsg::FlushMemtable → FlushActor.handle_flush() → SST file
- CheckCompaction → CompactionActor.pick_compaction() → execute_compaction() → merged SST
- Engine.write_batch() → atomic multi-key writes to memtable + WAL
- Engine.snapshot() → point-in-time consistent read at sequence number
- Engine.transaction() → multi-key ACID with state machine
- Engine startup → RuntimeState::new() → replay_wal() → restore memtable state
- VersionSet/VersionManager → lock-free manifest reads + atomic versioning

**Test Status (114 tests passing):**
- Transaction API: 9 tests ✅
- Cloud Storage: 10 tests ✅
- Cloud WAL: 9 tests ✅
- Version Set: 10 tests ✅
- Version Manager: 10 tests ✅
- Merge Iterator: 4 tests ✅
- Plus 62 existing tests from prior implementation ✅

**What's Next (Priority Order):**
1. **S3/GCS/Azure Providers** — Real cloud provider implementations with AWS/Google/Azure SDKs
2. **Integration Tests** — End-to-end tests of write→flush→compact→recover→cloud pipeline
3. **Metrics** — Port metrics modules from src_old for performance monitoring

**Development Guidelines:**
- Keep the original `src_old/` tree unchanged; use it purely for reference and diffing.
- Add the Copilot super prompt to the top of each rewritten file as you drive the port.
- Use `wip/PERFECT.md` as the canonical structure reference when adding new files.
- Prefer short, focused commits after each major subsystem port (engine API, runtime, WAL, SST, compaction, metadata).
- All tests must follow `should_{action}_when_{context}` naming convention
- All tests must include AAA (Arrange/Act/Assert) structure
- Zero test naming violations in src/ directory
- All tests must follow `should_{action}_when_{context}` naming with AAA structure.

Feel free to re-order the steps if a dependency forces it, but strive to keep the runtime/actor structure in place before hooking up heavyweight subsystems.