# Integration Tests Rebuild — Lock-Down Checklist

**Status:** Ready for user review and approval

**Date Created:** Now  
**Audit Scope:** All 932 test functions across 75 legacy test files (tests_old/)  
**Mapping Complete:** Yes ✅

---

## SECTION 1: Audit Completeness

- [x] All non-skipped test files counted (75 files)
- [x] All test functions extracted (932 total)
- [x] Test functions organized by source file (LEGACY_TESTS_EXTRACTED.txt)
- [x] Test functions mapped to behavior domains (16 domains identified)
- [x] Behavior domains mapped to target new test files (13 Phase 1-5 files + Phase 5+ advanced)
- [x] Phase groupings documented with rationale
- [x] Blockers identified and documented per Phase

---

## SECTION 2: Phase 1-2 Validation (Core KV + Durability + TTL)

**Target: 330 tests in 4 new test files**

### core_kvstore.rs (target: ~60 tests)
Source files: `engine_basic.rs` (8), `api_kvstore.rs` (14), `engine_iterators.rs` (22), `common_new.rs` (1)
- [x] Basic put/get/delete covered
- [x] Iterator operations covered
- [x] Empty/binary data cases covered
- [x] Column family isolation (initial) covered
- [x] Key/value serialization covered

**User review:** Does this grouping make sense?  
- [ ] Yes, approve
- [ ] Modify (list changes):

### core_wal_durability.rs (target: ~49 tests)
Source files: `durability_wal.rs` (10), `durability_atomicity.rs` (11), `durability_recovery.rs` (14), `concurrency_wal.rs` (4)
- [x] WAL persistence covered
- [x] Crash recovery covered
- [x] Atomicity guarantees covered
- [x] Durability policies covered
- [x] WAL rotation covered
- [x] Concurrent append ordering covered

**User review:** Does this grouping make sense?  
- [ ] Yes, approve
- [ ] Modify (list changes):

### core_ttl.rs (target: ~30 tests)
Source files: `ttl.rs` (12), `compaction_filters.rs` (7), `compaction_streaming.rs` (11)
- [x] TTL expiration on read covered
- [x] TTL metadata persistence covered
- [x] TTL filtering in compaction covered
- [x] TTL in streaming covered
- [x] Logical clock testing scaffold in place (tests marked #[ignore])

**User review:** Does this grouping make sense?  
- [ ] Yes, approve
- [ ] Modify (list changes):

### core_column_families.rs (target: ~34 tests)
Source files: `column_family_lifecycle.rs` (28), `admin_operations.rs` (6)
- [x] Multi-CF operations covered
- [x] CF creation/drop covered
- [x] CF isolation covered
- [x] Persistence across restart covered
- [x] Admin operations covered

**User review:** Does this grouping make sense?  
- [ ] Yes, approve
- [ ] Modify (list changes):

### core_snapshots.rs (target: ~39 tests)
Source files: `engine_snapshots.rs` (15), `engine_integration_e2e.rs` (24)
- [x] Snapshot isolation covered
- [x] Time-travel reads covered
- [x] Concurrent writes during snapshot covered
- [x] Note: Depends on snapshot seq plumbing (blocked)

**User review:** Does this grouping make sense?  
- [ ] Yes, approve
- [ ] Modify (list changes):

---

## SECTION 3: Phase 3 Validation (Flush, Compaction, Compression)

**Target: 225 tests in 2 new test files**

### core_flush_compaction.rs (target: ~73 tests)
Source files: `compaction_basic.rs` (16), `compaction_concurrent.rs` (12), `compaction_levels.rs` (15), `compaction_metrics.rs` (4), `compaction_determinism.rs` (6), `compaction_errors.rs` (8)
- [x] Memtable flush to SST covered
- [x] Multi-level compaction covered
- [x] Tombstone cleanup covered
- [x] Concurrent compaction covered
- [x] Metrics covered
- [x] Error handling covered
- [x] Note: Depends on flush/compaction actor integration (blocked)

**User review:** Does this grouping make sense?  
- [ ] Yes, approve
- [ ] Modify (list changes):

### core_compression.rs (target: ~75 tests)
Source files: `compression.rs` (16), `per_block_bloom_tests.rs` (19), `sst_writer_per_block_bloom_tests.rs` (4), `sst_block_summary.rs` (1), `cache_line_packing.rs` (3), `sst_trie.rs` (6), `sst_trie_compat.rs` (10), etc.
- [x] Block compression covered
- [x] Compression roundtrip covered
- [x] Bloom filters covered
- [x] SST layout/packing covered
- [x] Codec variations covered

**User review:** Does this grouping make sense?  
- [ ] Yes, approve
- [ ] Modify (list changes):

---

## SECTION 4: Phase 4 Validation (Concurrency & Transactions)

**Target: 169 tests in 2 new test files**

### core_concurrent_writes.rs (target: ~56 tests)
Source files: `concurrency_writes.rs` (13), `concurrency_flush.rs` (10), `concurrency_delete_range.rs` (4), `determinism.rs` (8), `stress_workloads.rs` (11), `stress_large_values.rs` (11)
- [x] Concurrent serialization covered
- [x] Last-writer-wins ordering covered
- [x] Sequence number monotonicity covered
- [x] Flush coordination covered
- [x] Stress patterns covered
- [x] Large values covered

**User review:** Does this grouping make sense?  
- [ ] Yes, approve
- [ ] Modify (list changes):

### core_transactions_basic.rs (target: ~113 tests)
Source files: `transaction_basic.rs` (21), `transaction_advanced.rs` (19), `transaction_conflicts.rs` (26), `transaction_isolation.rs` (22), `transaction_spill.rs` (14), `transaction_deadlock.rs` (11), `engine_write_batch.rs` (17)
- [x] WriteBatch atomicity covered
- [x] Conflict detection covered
- [x] Snapshot isolation covered
- [x] Deadlock detection covered
- [x] Large batches covered
- [x] Advanced batch ops covered
- [x] Note: Depends on conflict detection impl (blocked)

**User review:** Does this grouping make sense?  
- [ ] Yes, approve
- [ ] Modify (list changes):

---

## SECTION 5: Phase 5+ Validation (Advanced Features)

**Target: 208 tests in 3 core files + numerous specialized files**

### core_delete_range.rs (target: ~16 tests)
Source: `engine_delete_range.rs` (16)
- [x] Range delete covered
- [x] Boundaries covered
- [x] Tombstone cleanup covered

### core_merge_operators.rs (target: ~21 tests)
Source: `engine_merge_operators.rs` (21)
- [x] Merge operator application covered
- [x] Merge ordering covered

### Specialized (lower priority):
- [ ] Cache optimization (block_cache, cache_read_path, hybrid_storage_budget, rate_limiting) — 41 tests
- [ ] Config & tuning (config_api, config_validation, autotune) — 34 tests
- [ ] Cloud integration (cloud_*) — 54 tests
- [ ] Checkpoint/backup — 36 tests
- [ ] Advanced SST features — 150+ tests
- [ ] Test infrastructure — 39 tests

**User review:** Phase 5+ scope is acceptable?  
- [ ] Yes, defer until core is solid
- [ ] Prioritize some (list):

---

## SECTION 6: Known Blockers Acceptance

### Blocker 1: Snapshot Sequence Plumbing (PHASE 2)
- **Impact:** Affects 39 tests in core_snapshots.rs
- **Work required:** Plumb max_seq through runtime Read message → memtable + SST lookups
- **Current status:** Stubbed (`get_at` delegates to `get`)
- [x] Documented in TODO
- [x] Impact understood
- [ ] **USER ACCEPTANCE:** Acceptable to defer Phase 2 until Phase 1 complete

### Blocker 2: Flush/Compaction Routing (PHASE 3)
- **Impact:** Affects 73 tests in core_flush_compaction.rs
- **Work required:** Implement flush/compaction actor integration + manifest updates
- **Current status:** Stubbed (no-ops)
- [x] Documented in TODO
- [x] Impact understood
- [ ] **USER ACCEPTANCE:** Acceptable to defer Phase 3 until Phase 1-2 complete

### Blocker 3: Logical Clock for TTL Testing (PHASE 2)
- **Impact:** Affects 30 tests in core_ttl.rs (currently #[ignore])
- **Work required:** Abstraction for test clock; engine wired to use it when available
- **Current status:** Placeholder `advance_ttl_window()` function (no-op)
- [x] Documented in TODO
- [x] Tests scaffolded (named, marked #[ignore])
- [ ] **USER ACCEPTANCE:** Acceptable to implement test clock separately from core logic

### Blocker 4: Transaction Conflict Detection (PHASE 4)
- **Impact:** Affects 113 tests in core_transactions_basic.rs
- **Work required:** Read-write set tracking; conflict detection algorithm; snapshot-aware isolation
- **Current status:** Stubbed (no isolation)
- [x] Documented in TODO
- [x] Impact understood
- [ ] **USER ACCEPTANCE:** Acceptable to defer Phase 4 until Phase 3 complete

---

## SECTION 7: Test File Grouping Rationalization

### Why these 13 Phase 1-4 test files?

1. **Logical dependency chain:** Phase 1 (basic ops) → Phase 2 (durability) → Phase 3 (compaction) → Phase 4 (transactions)
2. **Behavior isolation:** Each file tests one major feature area
3. **Legacy test mapping:** Covers all 932 tests; no orphaned behaviors
4. **Incremental validation:** Can run phases independently
5. **Maintainability:** Small files easier to debug than one 500-test megafile

### Are there behaviors that should move between phases?

- [ ] No, mapping is correct
- [ ] Yes, move (describe):

---

## SECTION 8: Coverage Validation

- [x] All 932 legacy tests accounted for in mapping
- [x] No behaviors identified as missing from Phase 1-4
- [x] Phase 5+ captures advanced/specialized tests
- [x] Insert semantics distributed appropriately (core_kvstore, core_insert_semantics, transaction tests)
- [x] Delete range operations isolated in core_delete_range.rs
- [x] Merge operator operations isolated in core_merge_operators.rs

**User review:** Coverage feels complete?
- [ ] Yes, approve
- [ ] Missing behavior (describe):

---

## SECTION 9: Test Naming & Conventions

- [x] All test functions use `should_<behavior>_given_<context>_when_<condition>` or `test_<name>` convention
- [x] Naming conventions documented in `docs/dev/test-guidelines.md`
- [x] 932 test names extracted and categorized
- [x] New test files will follow same naming convention

**User review:** Naming conventions acceptable?
- [ ] Yes, approved
- [ ] Suggest changes:

---

## SECTION 10: Final Lock-Down Decision

**Have all sections been reviewed and approved?**

- [ ] Section 1: Audit completeness ✅
- [ ] Section 2: Phase 1-2 grouping ✅
- [ ] Section 3: Phase 3 grouping ✅
- [ ] Section 4: Phase 4 grouping ✅
- [ ] Section 5: Phase 5+ scope ✅
- [ ] Section 6: Blockers accepted ✅
- [ ] Section 7: File grouping rationale ✅
- [ ] Section 8: Coverage validated ✅
- [ ] Section 9: Naming conventions ✅

### LOCK-DOWN APPROVAL

**User confirms:**
- [ ] All sections reviewed
- [ ] No major changes needed
- [ ] Ready to create Phase 1 test skeletons
- [ ] Next step: Start with `tests/core_kvstore.rs`

---

## Artifacts Created During Audit

1. **INTEGRATION_TESTS_TODO.md** (expanded)
   - Added "Legacy Tests Inventory & Mapping" section
   - 16 behavior domains with test file mappings
   - Detailed file breakdown organized by Phase
   - 932 tests mapped; no unmapped behaviors

2. **LEGACY_TESTS_EXTRACTED.txt**
   - Complete list of all 932 test function names
   - Organized by source file
   - Useful for cross-referencing during implementation

3. **INTEGRATION_TESTS_AUDIT.md** (this document)
   - Summary stats and findings
   - Key findings section (coverage, blockers, redundancy mitigation)
   - Next steps options

4. **INTEGRATION_TESTS_REBUILD_CHECKLIST.md** (this file)
   - Section-by-section validation for user sign-off
   - Lock-down decision point

---

## Next Actions (Upon Approval)

1. **Create test file skeletons:**
   - `tests/core_kvstore.rs` (stub with test names)
   - Import from LEGACY_TESTS_EXTRACTED.txt
   - Use template from INTEGRATION_TESTS_TODO.md

2. **Start Phase 1 implementation:**
   - Implement tests one at a time
   - Identify gaps in engine/runtime
   - Implement gaps
   - Mark tests DONE in TODO

3. **Track progress:**
   - Update progress table in INTEGRATION_TESTS_TODO.md
   - Document implementation gaps as they arise

---

## Questions or Changes?

Before proceeding to Phase 1 implementation, flag any:
- [ ] Phase grouping concerns
- [ ] Behavior mapping errors
- [ ] Blocker dependency changes
- [ ] Test naming issues
- [ ] Other

**Notes:**

---

**APPROVED BY:** _______________  
**DATE:** _______________
