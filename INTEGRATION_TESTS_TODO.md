# Integration Tests Rebuild Plan

## Overview

This document outlines the **incremental rebuild** of the integration test suite following the actor model refactor. Each test file targets a specific behavior area. As we discover missing functionality, we implement it and circle back to green tests.

**Key principle:** One test file at a time. Implement, test, verify, then move to the next.

---

## Phase 1: Core Engine Functionality (Foundation)

### 1.1 `core_kvstore.rs` — Basic Put/Get/Delete (NEXT)

**Status:** Not started  
**Implementation gaps:** None known (likely working)

**Test cases (from `engine_basic.rs`):**
- `should_get_value_given_existing_key_when_put` — put then get returns value
- `should_return_none_given_nonexistent_key_when_get` — get on missing key returns None
- `should_return_none_given_deleted_key_when_get` — delete then get returns None
- `should_overwrite_value_given_existing_key_when_put` — second put overwrites
- `should_handle_empty_value_when_put` — empty byte arrays work
- `should_handle_binary_data_when_put` — arbitrary binary data round-trips
- `should_succeed_given_nonexistent_key_when_delete` — delete non-existent key is ok

**Expected to find:**
- Engine open/close works
- Default column family accessible
- Basic key serialization/deserialization

---

### 1.2 `core_wal_durability.rs` — Write-Ahead Log & Restart (NEXT+1)

**Status:** Not started  
**Implementation gaps:** Likely none for basic recovery

**Test cases (from `durability_wal.rs`):**
- `should_persist_data_across_restart` — write, close, reopen, data exists
- `should_recover_unflushed_writes_given_crash` — WAL recovery on startup
- `should_sync_on_flush_given_wal_durability_policy` — fsync behaves per policy
- `should_track_wal_segment_rotation` — segments rotate on threshold
- `should_prevent_stale_wal_reads_given_recovery` — newer writes visible after restart

**Expected to find:**
- WAL append during put works
- Recovery plumbing connects WAL replay to memtable restore
- Sequence numbers assigned correctly

**Known issues:**
- CloudFirst durability may not queue writes correctly (lower priority)

---

### 1.3 `core_column_families.rs` — Multi-CF Operations (NEXT+2)

**Status:** Not started  
**Implementation gaps:** Likely working but check CF isolation

**Test cases (from `column_family_lifecycle.rs`):**
- `should_isolate_data_given_separate_column_families` — CF A data != CF B data
- `should_create_cf_given_name_when_opening` — explicit CF creation works
- `should_persist_cf_list_across_restart` — CFs remembered after reopen
- `should_read_write_to_default_cf` — default CF is usable out of the box

**Expected to find:**
- CF routing in engine/runtime works
- Manifest tracks CF list persistently
- Column family state isolation correct

---

## Phase 2: Structured Durability (Build on Phase 1)

### 2.1 `core_snapshots.rs` — Snapshot Isolation (NEXT+3)

**Status:** Stubbed (`get_at` delegates to `get`)  
**Implementation gaps:** **MAJOR** — snapshot sequence not plumbed to reads

**Test cases:**
- `should_see_consistent_view_given_snapshot_when_concurrent_writes` — snapshot freezes view
- `should_hide_later_writes_given_snapshot_before_update_when_reading` — time-travel reads
- `should_return_value_at_snapshot_sequence_given_write_after_snapshot` — causality preserved

**Missing work:**
1. Plumb snapshot sequence into `RuntimeMsg::Read`
2. Memtable + SST lookups honor max_seq parameter
3. Update `get_at` to pass snapshot seq to runtime
4. Test helpers for snapshot creation

---

### 2.2 `core_ttl.rs` — Time-To-Live Semantics (NEXT+4)

**Status:** Partially impl (reads honor TTL, no logical clock for tests)  
**Implementation gaps:** Logical clock hook for deterministic tests; compaction cleanup

**Test cases (adapted from `ttl.rs` to use logical clock):**
- `should_return_value_given_ttl_not_elapsed_when_reading` — non-expired visible
- `should_return_none_given_ttl_elapsed_when_reading` — expired hidden (via logical clock advance)
- `should_persist_ttl_metadata_given_restart` — TTL survives recovery
- `should_expire_after_restart_given_ttl_elapsed_during_shutdown` — clock-based expiry works across restarts

**Missing work:**
1. Logical/test clock abstraction in testkit (e.g., `advance_test_time()`)
2. Engine wired to use test clock when available (feature flag or hook)
3. Compaction cleanup of expired keys
4. SST metadata carries TTL; readers skip expired blocks
5. Batch TTL propagation to WAL

---

### 2.3 `core_insert_semantics.rs` — Insert-If-Not-Exists (NEXT+5)

**Status:** Partially impl (WAL append rejects on exists; engine respects response)  
**Implementation gaps:** Batch support; atomicity verification

**Test cases:**
- `should_return_true_given_insert_on_new_key` — insert succeeds, returns true
- `should_return_false_given_insert_on_existing_key` — insert fails gracefully, returns false
- `should_return_existing_value_given_insert_with_value_on_collision` — `insert_with_value` returns existing
- `should_fail_atomically_given_insert_if_not_exists_when_concurrent` — no race condition on collision

**Missing work:**
1. Test/verify WAL insert-only enforcement is atomic w.r.t. reads
2. Batch `insert` operations
3. Snapshot-aware inserts (don't collide with in-flight transactions)

---

## Phase 3: Structured Updates (Build on Phase 2)

### 3.1 `core_flush_compaction.rs` — Manual Flush & Compaction Triggers (NEXT+6)

**Status:** Stubbed (`flush_cf`, `compact_all` are no-ops)  
**Implementation gaps:** **MAJOR** — implement flush/compaction routing

**Test cases:**
- `should_move_memtable_to_sst_given_flush_when_memtable_has_data` — flush writes SST
- `should_trigger_memtable_rotation_given_flush` — flush creates new active memtable
- `should_compact_l0_ssts_given_compact_all_when_multiple_l0_files` — compaction merges levels
- `should_preserve_key_order_given_compaction_across_levels` — LSM invariant maintained
- `should_remove_tombstones_given_compaction_when_older_than_all_snapshots` — tombstone cleanup

**Missing work:**
1. Plumb `flush_cf` → runtime flush actor → memtable rotation
2. Plumb `compact_all` → compaction actor → level merging
3. SST factory and write path integration
4. Manifest updates post-flush/compaction
5. Level assignment (L0 from flush, compact up levels)

---

### 3.2 `core_compression.rs` — Block Compression (NEXT+7)

**Status:** Likely working (compression policy plumbed)  
**Implementation gaps:** Test SST readers decode correctly

**Test cases:**
- `should_compress_blocks_given_compression_enabled_when_flushing` — SST uses compression
- `should_decompress_on_read_given_compressed_blocks` — reader auto-decompresses
- `should_roundtrip_given_various_compression_algorithms` — zstd, lz4, none all work
- `should_handle_incompressible_data_gracefully` — large or random data ok

**Expected to find:**
- Compression policy respected in flush
- SST reader handles compressed blocks
- No data loss under compression

---

## Phase 4: Concurrency & Transactions (Build on Phase 3)

### 4.1 `core_concurrent_writes.rs` — Concurrent Put/Delete (NEXT+8)

**Status:** Some impl (lock-free skiplist)  
**Implementation gaps:** Race condition testing; WAL append ordering

**Test cases:**
- `should_serialize_concurrent_puts_given_same_key` — last writer wins (or serialized)
- `should_preserve_put_delete_order_given_interleaved_operations` — happens-before respected
- `should_not_lose_writes_given_concurrent_puts_to_different_keys` — all writes visible
- `should_recover_all_concurrent_writes_given_crash_during_concurrent_load` — WAL orders all

**Missing work:**
1. Test harness for concurrent ops (crossbeam or tokio)
2. Verify memtable sequence numbers are monotonic
3. Verify WAL ordering matches engine ordering

---

### 4.2 `core_transactions_basic.rs` — Transaction Basics (NEXT+9)

**Status:** Stubbed (transaction types exist but no isolation)  
**Implementation gaps:** **MAJOR** — transaction isolation, conflict detection

**Test cases:**
- `should_atomically_apply_write_batch_given_batch_operations` — batch is all-or-nothing
- `should_fail_batch_on_first_error_given_batch_with_error` — error stops batch
- `should_isolate_batch_read_from_concurrent_write_given_snapshot_isolation` — reads frozen
- `should_conflict_on_write_write_collision_given_optimistic_concurrency` — conflict detection

**Missing work:**
1. WriteBatch → RuntimeMsg → WAL ordering
2. Snapshot isolation for batches
3. Conflict detection (read set vs. write set)
4. Rollback/abort semantics

---

## Phase 5: Advanced Features (Lower Priority)

### 5.1 `core_delete_range.rs` — Range Tombstones

**Test cases:**
- `should_hide_all_keys_in_range_given_delete_range` — range delete works
- `should_preserve_keys_outside_range_given_delete_range` — boundaries respected
- `should_cleanup_tombstones_given_compaction_on_delete_range` — compaction efficiency

---

### 5.2 `core_merge_operators.rs` — Merge Operations

**Test cases:**
- `should_apply_merge_operator_given_merge_on_existing_key` — merge combines values
- `should_treat_merge_as_put_given_merge_on_missing_key` — merge behaves on missing
- `should_preserve_merge_order_given_multiple_merges` — operator associativity matters

---

### 5.3 `core_stress_basic.rs` — Basic Stress (Phase 5+)

**Test cases:**
- `should_handle_large_keys_and_values` — 1MB+ keys/values work
- `should_survive_high_write_throughput_given_sequential_keys` — perf baseline
- `should_not_corrupt_under_mixed_load_given_put_delete_interleaved` — LSM invariants hold

---

## Implementation Workflow

For each test file:

1. **Create skeleton** in `tests/` with test names and doc comments
2. **Run `cargo test --test <name>`** → watch failures
3. **Identify missing impl** (check BEHAVIORS_GAP, look at error messages)
4. **Implement missing pieces** in `src/` (engine, runtime, actors)
5. **Debug test failures** one by one
6. **Verify WAL/memtable state** with debug output if needed
7. **Mark DONE** in `INTEGRATION_TESTS_TODO.md`; move to next file

---

## Test File Template

```rust
//! Core <Feature> Integration Tests
//!
//! Tests the <feature> behavior end-to-end using the public MidgeEngine API.
//! Follows naming convention: should_<behavior>_given_<context>_when_<condition>

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use cntryl_midge::testkit::{test_temp_dir, with_engine_restart};

mod common;
use common::*;

#[test]
fn should_<behavior>_given_<context>_when_<condition>() {
    // Arrange
    let dir = test_temp_dir();
    let opts = default_opts(dir.path().to_path_buf());
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Act
    engine.put(&cf, b"key", b"value").expect("put");

    // Assert
    let result = engine.get(&cf, b"key").expect("get");
    assert_eq!(result, Some(Bytes::from_static(b"value")));
}
```

---

## Progress Tracking

| Phase | File | Status | Impl Gaps Found | Notes |
|-------|------|--------|-----------------|-------|
| 1 | `core_kvstore.rs` | ⏳ Not started | TBD | Basic API test |
| 1 | `core_wal_durability.rs` | ⏳ Not started | TBD | Recovery critical |
| 1 | `core_column_families.rs` | ⏳ Not started | TBD | CF isolation |
| 2 | `core_snapshots.rs` | ⏳ Blocked | MAJOR: snapshot seq plumbing | Depends on Phase 1 |
| 2 | `core_ttl.rs` | ⏳ Partial | Logical clock, compaction | Depends on Phase 1 |
| 2 | `core_insert_semantics.rs` | ⏳ Partial | Batch support | Depends on Phase 1 |
| 3 | `core_flush_compaction.rs` | ⏳ Blocked | MAJOR: flush/compaction routing | Depends on Phase 2 |
| 3 | `core_compression.rs` | ⏳ Likely ready | TBD | Depends on Phase 3 |
| 4 | `core_concurrent_writes.rs` | ⏳ Partial | Test harness | Depends on Phase 1 |
| 4 | `core_transactions_basic.rs` | ⏳ Blocked | MAJOR: isolation & conflicts | Depends on Phase 2 |
| 5 | `core_delete_range.rs` | ⏳ Not started | TBD | Depends on Phase 3 |
| 5 | `core_merge_operators.rs` | ⏳ Not started | TBD | Depends on Phase 3 |
| 5 | `core_stress_basic.rs` | ⏳ Not started | TBD | Depends on Phase 4 |

---

## Known Blockers

- **Snapshot isolation:** Requires plumbing snapshot sequence through runtime read path.
- **Flush/Compaction:** Requires actor integration + manifest updates.
- **Logical clock:** Required for deterministic TTL tests (clock-driven expiry).
- **Transaction isolation:** Requires conflict detection + snapshot-aware memtable/SST reads.

---

---

## Legacy Tests Inventory & Mapping

**Total legacy tests in tests_old/:** 932 across 75 test files (excluding *.skip)

### Grouping by Behavior Domain

| Domain | Legacy Test Files | Test Count | Target New File(s) | Status |
|--------|-------------------|------------|-------------------|--------|
| **KV Basics** | api_kvstore, engine_basic, engine_delete_range, engine_iterators | 60 | core_kvstore, core_delete_range | ⏳ |
| **Durability & WAL** | durability_wal, durability_atomicity, durability_recovery, concurrency_wal | 49 | core_wal_durability | ⏳ |
| **TTL & Expiration** | ttl, compaction_filters, compaction_streaming | 30 | core_ttl | ⏳ |
| **Column Families** | column_family_lifecycle, admin_operations | 34 | core_column_families | ⏳ |
| **Snapshots** | engine_snapshots, engine_integration_e2e | 39 | core_snapshots | ⏳ |
| **Flush & Compaction** | compaction_basic, compaction_concurrent, compaction_levels, compaction_metrics, compaction_determinism, compaction_errors | 73 | core_flush_compaction | ⏳ |
| **Compression & Encoding** | compression, sst_block_summary, sst_writer_bloom_tests, per_block_bloom_tests, sst_trie | 75 | core_compression | ⏳ |
| **Concurrency** | concurrency_writes, concurrency_flush, concurrency_delete_range, determinism, stress_workloads, stress_large_values | 56 | core_concurrent_writes | ⏳ |
| **Transactions** | transaction_basic, transaction_advanced, transaction_conflicts, transaction_isolation, transaction_spill, transaction_deadlock | 113 | core_transactions_basic | ⏳ |
| **Insert Semantics** | *(covered in api_kvstore, engine_basic, transaction tests)* | ~20 | core_insert_semantics | ⏳ |
| **Cache & Performance** | block_cache, cache_read_path, cache_line_packing, hybrid_storage_budget, rate_limiting | 41 | *(Phase 5+)* | ⏳ |
| **Config & Tuning** | config_api, config_validation, autotune | 34 | *(Phase 5+)* | ⏳ |
| **Cloud & Backup** | cloud_consistency, cloud_durability, cloud_hybrid, cloud_real_providers, backup_restore | 54 | *(Phase 5+)* | ⏳ |
| **Merge Operators** | engine_merge_operators | 21 | core_merge_operators | ⏳ |
| **Advanced** | checkpoint, readonly_mode, memory_mode, paranoid_mode, error_handling, sst_invariants, fence_pointers, streaming_*, phase3_*, phase4_*, segment_*, sba_actor_*, eviction_*, invariants_* | 259 | *(Phase 5+ / advanced)* | ⏳ |
| **Test Infrastructure** | test_infrastructure, deadlock_detector_demo, proptest_parsers, fault_injection | 39 | *(Support)* | ⏳ |

**Summary by Phase:**
- **Phase 1-2 (KV + Durability + TTL):** ~330 tests mapped
- **Phase 3 (Flush, Compaction, Compression):** ~225 tests mapped
- **Phase 4 (Concurrency + Transactions):** ~169 tests mapped
- **Phase 5+ (Cache, Config, Cloud, Advanced):** ~208 tests mapped
- **Total mapped:** 932 tests

### Detailed File Breakdown

**PHASE 1: Core KV (Tests 1-9 map to legacy)**
- `engine_basic.rs` (8) → core_kvstore
- `api_kvstore.rs` (14) → core_kvstore, core_insert_semantics
- `common_new.rs` (1) → core_kvstore (helper)

**PHASE 2: Durability & Snapshots**
- `durability_wal.rs` (10) → core_wal_durability
- `durability_atomicity.rs` (11) → core_wal_durability
- `durability_recovery.rs` (14) → core_wal_durability
- `concurrency_wal.rs` (4) → core_wal_durability
- `engine_snapshots.rs` (15) → core_snapshots
- `engine_integration_e2e.rs` (24) → core_snapshots, core_kvstore
- `ttl.rs` (12) → core_ttl (already refactored to use logical clock)
- `compaction_filters.rs` (7) → core_ttl (TTL filtering during compaction)
- `compaction_streaming.rs` (11) → core_ttl (TTL in streaming)

**PHASE 3: Column Families & Flush/Compaction**
- `column_family_lifecycle.rs` (28) → core_column_families
- `admin_operations.rs` (6) → core_column_families
- `compaction_basic.rs` (16) → core_flush_compaction
- `compaction_concurrent.rs` (12) → core_flush_compaction
- `compaction_levels.rs` (15) → core_flush_compaction
- `compaction_metrics.rs` (4) → core_flush_compaction
- `compaction_determinism.rs` (6) → core_flush_compaction
- `compaction_errors.rs` (8) → core_flush_compaction
- `compression.rs` (16) → core_compression
- `per_block_bloom_tests.rs` (19) → core_compression
- `sst_writer_per_block_bloom_tests.rs` (4) → core_compression
- `cache_line_packing.rs` (3) → core_compression (SST layout)

**PHASE 4: Concurrency & Transactions**
- `concurrency_writes.rs` (13) → core_concurrent_writes
- `concurrency_flush.rs` (10) → core_concurrent_writes (flush ordering)
- `concurrency_delete_range.rs` (4) → core_concurrent_writes (delete range ordering)
- `determinism.rs` (8) → core_concurrent_writes (deterministic ordering)
- `engine_write_batch.rs` (17) → core_transactions_basic
- `transaction_basic.rs` (21) → core_transactions_basic
- `transaction_advanced.rs` (19) → core_transactions_basic (advanced batch ops)
- `transaction_conflicts.rs` (26) → core_transactions_basic (conflict detection)
- `transaction_isolation.rs` (22) → core_transactions_basic (snapshot isolation)
- `transaction_spill.rs` (14) → core_transactions_basic (large batches)
- `transaction_deadlock.rs` (11) → core_transactions_basic (deadlock detection)
- `stress_workloads.rs` (11) → core_concurrent_writes (stress patterns)
- `stress_large_values.rs` (11) → core_concurrent_writes (large value handling)

**PHASE 5 & ADVANCED (Lower priority / specialized)**
- `engine_delete_range.rs` (16) → core_delete_range
- `engine_merge_operators.rs` (21) → core_merge_operators
- `engine_iterators.rs` (22) → core_kvstore (iteration patterns)
- `block_cache.rs` (12) → cache optimization phase
- `cache_read_path.rs` (6) → cache optimization phase
- `hybrid_storage_budget.rs` (11) → memory management phase
- `rate_limiting.rs` (19) → performance control phase
- `config_api.rs` (20) → config & tuning phase
- `config_validation.rs` (6) → config & tuning phase
- `autotune.rs` (8) → config & tuning phase
- `checkpoint.rs` (26) → checkpoint/backup phase
- `backup_restore.rs` (10) → checkpoint/backup phase
- `cloud_consistency.rs` (6) → cloud integration phase
- `cloud_durability.rs` (12) → cloud integration phase
- `cloud_hybrid.rs` (6) → cloud integration phase
- `cloud_real_providers.rs` (8) → cloud integration phase
- `readonly_mode.rs` (7) → operational modes
- `memory_mode.rs` (2) → operational modes
- `paranoid_mode.rs` (4) → validation modes
- `error_handling.rs` (17) → error injection
- `fault_injection.rs` (4) → error injection
- `sst_invariants.rs` (10) → LSM invariants
- `fence_pointers.rs` (12) → advanced SST features
- `sst_trie.rs` (6) → SST indexing
- `sst_trie_compat.rs` (10) → SST compatibility
- `sst_block_summary.rs` (1) → SST metadata
- `streaming_bloom_tuning.rs` (16) → SST bloom filters
- `streaming_fence_pointer_skipping.rs` (15) → SST optimization
- `streaming_sequential_optimization.rs` (14) → SST optimization
- `sst_reader_per_block_bloom.rs` (4) → SST reads
- `sst_writer_per_block_bloom_integration.rs` (4) → SST writes
- `invariants_flush.rs` (15) → LSM guarantees
- `invariants_lsm.rs` (4) → LSM guarantees
- `phase3_index_table.rs` (20) → phased SST development
- `phase4_tombstone_index.rs` (20) → tombstone handling
- `phase7_cloud_coordination.rs` (2) → cloud coordination
- `segment_flush_coordination.rs` (9) → segment management
- `segment_reads.rs` (9) → segment management
- `eviction_actor_integration.rs` (4) → memory eviction
- `sba_actor_integration.rs` (9) → storage budget actor
- `runtime_actors_cloud_gc.rs` (9) → cloud GC
- `test_infrastructure.rs` (9) → test helpers
- `deadlock_detector_demo.rs` (4) → diagnostics
- `proptest_parsers.rs` (17) → fuzzing

---

## References

- `docs/dev/test-guidelines.md` — Test conventions and naming
- `BEHAVIORS.md` — Intended LSM behavior
- `BEHAVIORS_GAP.md` — Known implementation gaps
- `tests_old/` — Legacy tests (932 tests across 75 files; mapped above)

