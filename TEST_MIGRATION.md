# Test Migration Tracker

This document tracks the migration and reorganization of integration tests.

## Goals

1. **Eliminate duplication** - Consolidate overlapping tests
2. **Improve organization** - Group tests by feature, not implementation
3. **Ensure correctness** - Each test should test exactly what it claims
4. **Use deterministic patterns** - Test hooks for async/concurrent behavior
5. **Storage mode coverage** - Test all storage modes where relevant (see below)

## Migration Process

1. Review the **Target Structure** below to understand where tests should go
2. Pick a legacy test file and review each test:
   - Does it test user-facing behavior (not implementation details)?
   - Does it duplicate tests in other files?
   - Does it follow naming: `should_{action}_given_{context}_when_{condition}`?
   - Does it use test hooks for deterministic synchronization?
3. Either migrate the test to the appropriate target file or mark for deletion
4. Run `cargo test --test {filename}` to verify
5. Update the migration checklist

## Storage Mode Coverage

Midge supports three storage modes. Tests should cover all relevant modes:

| Mode | Description | When to Test |
|------|-------------|--------------|
| `Memory` | In-memory only, no persistence | All CRUD tests (fast path) |
| `LocalDisk` | Local filesystem with WAL/SST | All tests (primary path) |
| `CloudBacked` | Cloud storage with local cache | Tests involving SST reads, recovery, cloud sync |

**Pattern**: Use `for mode in all_storage_modes()` loop with `create_storage_mode(mode)` helper.

**When to use which helper**:
- `all_storage_modes()` → `["Memory", "LocalDisk", "CloudBacked"]` - Most CRUD tests
- `disk_storage_modes()` → `["LocalDisk", "CloudBacked"]` - Tests requiring SST files or WAL persistence
- Single mode - Tests specific to one mode (e.g., Memory mode has no filesystem artifacts)

**Example**:
```rust
#[test]
fn should_get_value_given_existing_key_when_put() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions { storage_mode, ..Default::default() };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        // Act
        engine.put(&cf, b"key", b"value").expect("put");
        let result = engine.get(&cf, b"key").expect("get");

        // Assert
        assert_eq!(result, Some(Bytes::from_static(b"value")), "Failed for {}", name);
    }
}
```

## Status Legend

- ⬜ Not started
- 🔄 In progress  
- ✅ Migrated and passing
- 🗑️ Deleted (duplicate/invalid)

---

# Migration Order

Prioritized by dependency chain and bug-catching value:

| # | Target File | Rationale | Status |
|---|-------------|-----------|--------|
| 1 | `engine_basic.rs` | Foundation for everything | ✅ |
| 2 | `durability_wal.rs` | Critical path - WAL correctness prevents data loss | ✅ |
| 3 | `durability_recovery.rs` | Crash recovery, manifest persistence | ✅ |
| 4 | `engine_write_batch.rs` | Atomic batches use WAL; common user operation | ✅ |
| 5 | `engine_snapshots.rs` | Point-in-time reads; needed before transactions | ✅ |
| 6 | `transaction_basic.rs` | Depends on snapshots; high user-facing value | ✅ |
| 7 | `column_family_lifecycle.rs` | CF create/drop/persist; isolated subsystem | ✅ |
| 8 | `column_family_isolation.rs` | Data isolation between CFs | ✅ (merged into #7) |
| 9 | `compaction_basic.rs` | Space reclamation; can run after data written | ✅ |
| 10 | `compaction_levels.rs` | Multi-level compaction behavior | ✅ |
| 11 | `engine_iterators.rs` | Advanced iteration patterns | ✅ |
| 12 | `engine_delete_range.rs` | Range tombstones are complex | ✅ |
| 13 | `engine_merge_operators.rs` | Advanced feature, fewer users | ✅ (exposes bug) |
| 14 | `concurrency_*.rs` | Stress tests; need solid base first | ✅ |
| 15 | `stress_*.rs` | Soak/capacity tests last | ✅ |

**Logic:**
1. **Durability first** - If recovery is broken, nothing else matters
2. **Core write path** - Batches, snapshots, transactions
3. **Multi-tenancy** - Column families
4. **Background ops** - Compaction
5. **Advanced features** - Iterators, merge ops, delete ranges
6. **Stress/edge cases** - After happy path is solid

---

# Current Progress

| Target File | Status | Tests | Storage Modes | Notes |
|-------------|--------|-------|---------------|-------|
| `engine_basic.rs` | ✅ | 25 | All 3 | Put/Get/Delete, Scans, Insert, CAS, Delete Range |
| `durability_wal.rs` | ✅ | 10 | LocalDisk, CloudBacked | WAL persistence, fsync, rotation, crash recovery |
| `durability_recovery.rs` | ✅ | 14 | LocalDisk | Clean shutdown, crash during flush, manifest failures |
| `engine_write_batch.rs` | ✅ | 14 | LocalDisk | Atomic batches, ordering, durability, multi-CF |
| `engine_snapshots.rs` | ✅ | 15 | All 3 | Snapshot reads, MVCC, flush/compaction isolation |
| `transaction_basic.rs` | ✅ | 21 | LocalDisk, CloudBacked | Commit, rollback, isolation, insert, delete_range, durability, timeouts, concurrent |
| `column_family_lifecycle.rs` | ✅ | 28 | LocalDisk | Create, drop, list, isolation, persistence, lookup |
| `compaction_basic.rs` | ✅ | 16 | LocalDisk, CloudBacked | Manual compaction, data correctness, tombstones, snapshots, background |
| `compaction_levels.rs` | ✅ | 15 | LocalDisk, CloudBacked | L0 sublevels, level size enforcement, cascading, statistics |
| `engine_iterators.rs` | ✅ | 22 | LocalDisk, CloudBacked | Forward/reverse scans, seek, tombstones, streaming, pagination |
| `engine_delete_range.rs` | ✅ | 16 | LocalDisk, CloudBacked | Range deletion, recovery, compaction, snapshots, overlapping |
| `engine_merge_operators.rs` | ✅ | 23 | All 3 (2 fail) | **BUG**: CloudBacked doesn't persist merge operands correctly - returns last operand instead of resolved sum after recovery |
| `concurrency_writes.rs` | ✅ | 13 | All 3 | Concurrent puts, sequence allocation, memtable race conditions |
| `concurrency_flush.rs` | ✅ | 10 | All 3 (4 disk-only) | Flush contention, backpressure, iterator correctness, deadlock prevention |
| `concurrency_wal.rs` | ✅ | 4 | LocalDisk, CloudBacked | WAL serialization, ordering, rotation during concurrent writes |
| `concurrency_delete_range.rs` | ✅ | 4 | All 3 | Concurrent delete ranges, overlapping ranges, interleaved operations |
| `stress_large_values.rs` | ✅ | 11 | All 3 (4 disk-only) | Large value storage, mixed sizes, backpressure, crash recovery, snapshots |
| `stress_workloads.rs` | ✅ | 11 | All 3 (5 disk-only) | Hot partition, high throughput, TTL patterns, append-only, mixed workloads |
| `checkpoint.rs` | ✅ | 26 | LocalDisk | Creation, consistency, isolation, multi-CF, concurrent, recovery, error handling |
| `cloud_durability.rs` | ✅ | 12 | CloudBacked (4 configs) | SST upload, manifest persistence, crash recovery, concurrent writes, restart |
| `cloud_consistency.rs` | ✅ | 6 | CloudBacked | Listing lag, eventual consistency, checksums, corrupted blobs, sync |
| `cloud_hybrid.rs` | ✅ | 6 | CloudBacked (hybrid) | Cache eviction, concurrent access, churn, async uploads, recovery, metrics |
| `compaction_concurrent.rs` | ✅ | 12 | LocalDisk, CloudBacked | Reads/writes during compaction, snapshot isolation, iterator stability, tombstones |
| `transaction_isolation.rs` | ✅ | 22 | LocalDisk, CloudBacked | Dirty read/write prevention, snapshot isolation, conflict detection, phantom read prevention |
| `transaction_conflicts.rs` | ✅ | 26 | LocalDisk, CloudBacked | LWW semantics, INSERT conflicts, CAS conflicts, delete range LWW, high contention |
| `transaction_deadlock.rs` | ✅ | 11 | LocalDisk, CloudBacked | INSERT conflict detection, PUT LWW semantics, concurrent transactions, recovery |
| `transaction_spill.rs` | ✅ | 14 | LocalDisk, CloudBacked + Memory | Large transaction spill-to-disk, data integrity, rollback cleanup, recovery, memory pressure, memory mode verification |
| `transaction_advanced.rs` | ✅ | 19 | LocalDisk, CloudBacked | Edge cases, atomicity, delete range integration, durability, sequential transactions |
| `error_handling.rs` | ✅ | 17 | LocalDisk | WAL/manifest corruption, disk full, SST corruption, background errors, crash during flush |
| `config_validation.rs` | ✅ | 6 | LocalDisk | Config bounds validation, concurrent validation, stress stability, persistence |
| `metrics.rs` | ✅ | 9 | All 3 (2 disk-only) | Sequence tracking, memory usage, SST stats, amplification, performance metrics |
| `readonly_mode.rs` | ✅ | 7 | LocalDisk, CloudBacked, Memory | Write rejection, read operations, delete/insert rejection in read-only mode |
| `memory_mode.rs` | ✅ | 2 | Memory | No filesystem artifacts, column family isolation in memory |
| (remaining ~6 files) | ⬜ | ~18 | TBD | Not started |

---

# Target Test Structure (Ideal)

The reorganized test suite consolidates 97 legacy files into ~35 focused test files.

## 1. Core Engine Operations (`engine_*.rs`)

### `engine_basic.rs` ✅ (25 tests) — All storage modes
Core CRUD operations that every user needs. Tests all 3 storage modes via loop.
```
PUT/GET:
- should_get_value_given_existing_key_when_put
- should_return_none_given_nonexistent_key_when_get
- should_overwrite_value_given_existing_key_when_put
- should_handle_empty_value_when_put
- should_handle_binary_data_when_put

DELETE:
- should_return_none_given_deleted_key_when_get
- should_succeed_given_nonexistent_key_when_delete

SCAN:
- should_return_ordered_pairs_given_range_when_scan
- should_return_matching_keys_given_prefix_when_scan
- should_respect_limit_given_limit_when_scan
- should_return_reverse_order_given_reverse_when_scan
- should_exclude_deleted_keys_when_scan
- should_return_empty_given_no_data_when_scan

INSERT (Insert-if-not-exists):
- should_insert_value_given_nonexistent_key_when_insert
- should_not_insert_given_existing_key_when_insert
- should_return_existing_value_given_existing_key_when_insert_with_value
- should_insert_given_deleted_key_when_insert

CAS (Compare-and-Swap):
- should_swap_value_given_matching_expected_when_cas
- should_return_mismatch_given_unexpected_value_when_cas
- should_insert_given_none_expected_and_missing_key_when_cas
- should_return_mismatch_given_none_expected_and_existing_key_when_cas

DELETE_RANGE:
- should_delete_keys_in_range_when_delete_range
- should_be_noop_given_empty_range_when_delete_range
- should_be_noop_given_inverted_range_when_delete_range

MEMORY MODE:
- should_not_create_filesystem_artifacts_when_memory_mode
```
**Source files**: `engine_basic_ops.rs`, `engine_scans.rs`, `engine_multi_get.rs`, `engine_atomics.rs`

### `engine_write_batch.rs` (~12 tests)
Atomic batch operations.
```
- should_commit_batch_atomically
- should_preserve_operation_order_in_batch
- should_handle_empty_batch
- should_apply_delete_after_put_in_same_batch
- should_support_batch_with_ttl
- should_batch_across_column_families
- should_persist_batch_after_crash
- should_recover_batch_from_wal
- should_handle_large_batch
- should_propagate_disk_full_error
- should_rollback_batch_on_error
- should_support_batch_clear_and_reuse
```
**Source files**: `engine_write_batch_atomicity.rs`, `engine_write_batch_edge.rs`

### `engine_snapshots.rs` (~10 tests)
Point-in-time consistent views.
```
- should_create_snapshot_at_current_state
- should_hide_writes_after_snapshot
- should_show_writes_before_snapshot
- should_support_multiple_concurrent_snapshots
- should_preserve_snapshot_during_compaction
- should_preserve_snapshot_during_flush
- should_release_snapshot_resources_on_drop
- should_handle_snapshot_with_delete_range
- should_iterate_snapshot_consistently
- should_not_block_writes_when_snapshot_held
```
**Source files**: `engine_snapshots.rs`, `snapshot_lifecycle.rs`, `snapshot_lifecycle_compaction.rs`

### `engine_iterators.rs` ✅ (22 tests)
Iterator behavior, scanning, and streaming operations.
```
BASIC ITERATION:
- should_iterate_all_keys_in_order_given_populated_db_when_scanning
- should_iterate_in_reverse_given_reverse_query_when_scanning
- should_limit_results_given_limit_query_when_scanning
- should_return_empty_given_empty_db_when_scanning

SEEK OPERATIONS:
- should_return_next_key_given_seek_to_missing_key_when_scanning
- should_return_empty_given_seek_past_end_when_scanning
- should_return_empty_given_invalid_range_when_start_greater_than_end

ITERATOR STABILITY:
- should_continue_safely_given_compaction_when_iterating_with_snapshot
- should_handle_gracefully_given_sst_removed_when_iterating_with_snapshot
- should_iterate_consistently_given_data_spans_sst_boundaries_when_scanning
- should_yield_stable_results_given_flush_in_progress_when_scanning

TOMBSTONE HANDLING:
- should_skip_deleted_keys_given_tombstones_when_scanning
- should_respect_range_tombstones_given_delete_range_when_scanning
- should_return_latest_value_given_interleaved_puts_deletes_when_scanning

STREAMING SCANS:
- should_match_regular_scan_given_streaming_scan_when_comparing
- should_respect_limit_given_streaming_scan_when_limited
- should_apply_tombstones_given_streaming_scan_when_keys_deleted
- should_handle_concurrent_streaming_scans_when_multiple_threads

PAGINATION:
- should_paginate_results_given_chunked_queries_when_iterating
- should_produce_identical_results_given_repeated_scans_when_rewinding

LARGE DATASETS:
- should_handle_large_scan_given_many_keys_when_iterating
- should_handle_large_streaming_scan_given_multiple_ssts_when_spanning
```
**Source files**: `engine_iterator_edge.rs`, `iterator_lifecycle.rs`, `iterator_stability_under_pressure.rs`, `engine_streaming.rs`

### `engine_delete_range.rs` ✅ (16 tests)
Range deletion (range tombstone) functionality.
```
BASIC RANGE DELETION:
- should_delete_keys_in_range_given_delete_range_when_querying
- should_delete_keys_across_levels_given_flushed_data_when_delete_range
- should_handle_empty_range_given_start_equals_end_when_delete_range

SCAN/GET BEHAVIOR:
- should_hide_deleted_range_in_scan_given_delete_range_when_scanning
- should_handle_large_range_deletion_given_many_keys_when_deleting

RECOVERY:
- should_persist_delete_range_given_wal_when_recovering
- should_recover_range_tombstone_given_no_flush_when_restarting
- should_apply_delete_range_after_crash_given_flushed_tombstone_when_recovering

COMPACTION:
- should_apply_range_tombstone_during_compaction_given_flushed_data_when_compacting
- should_not_resurrect_deleted_keys_given_compaction_when_range_delete_applied

SNAPSHOT ISOLATION:
- should_preserve_snapshot_view_given_delete_range_after_snapshot_when_reading
- should_include_deleted_range_in_snapshot_scan_given_delete_after_snapshot_when_scanning

OVERLAPPING/INTERLEAVED:
- should_merge_overlapping_ranges_given_multiple_delete_ranges_when_deleting
- should_allow_put_after_delete_range_given_interleaved_ops_when_writing
- should_apply_memtable_and_sst_tombstones_given_mixed_sources_when_reading

READ-ONLY MODE:
- should_reject_delete_range_given_read_only_mode_when_attempting
```
**Source files**: `engine_delete_range.rs`, `engine_delete_range_core.rs`, `range_delete_edge_cases.rs`

### `engine_merge_operators.rs` (~12 tests)
Merge operator functionality.
```
- should_apply_merge_operator_on_read
- should_chain_multiple_merge_operands
- should_apply_merge_to_base_value
- should_apply_merge_to_tombstone
- should_persist_merge_operands_in_wal
- should_compact_merge_operands
- should_handle_missing_merge_operator_on_reopen
- should_handle_failing_merge_operator
- should_support_per_cf_merge_operators
- should_preserve_merge_operand_order
- should_handle_binary_merge_operands
- should_handle_concurrent_merges
```
**Source files**: `engine_merge_operator_correctness.rs`, `engine_merge_operator_errors.rs`, `merge_operator_failure_modes.rs`, `engine_cf_merge_operators.rs`

---

## 2. Column Families (`column_family_*.rs`)

### `column_family_lifecycle.rs` (~8 tests)
CF creation, deletion, and persistence.
```
- should_create_column_family
- should_drop_column_family
- should_list_column_families
- should_persist_cf_across_restart
- should_prevent_drop_of_default_cf
- should_require_flush_before_drop
- should_handle_cf_with_custom_config
- should_reopen_with_existing_cfs
```
**Source files**: `column_family_lifecycle.rs`

### `column_family_isolation.rs` (~6 tests)
Data isolation between CFs.
```
- should_isolate_keys_between_cfs
- should_isolate_scans_between_cfs
- should_isolate_delete_ranges_between_cfs
- should_compact_cfs_independently
- should_flush_cfs_independently
- should_handle_same_key_in_multiple_cfs
```
**Source files**: `column_family_isolation.rs`, `multi_cf_compaction_fairness.rs`, `multicf_compaction_recovery.rs`

---

## 3. Transactions (`transaction_*.rs`)

### `transaction_basic.rs` (~10 tests)
Core transaction operations.
```
- should_begin_transaction
- should_commit_transaction
- should_rollback_transaction
- should_read_own_writes
- should_hide_uncommitted_writes_from_others
- should_persist_committed_transaction
- should_not_persist_rolled_back_transaction
- should_handle_empty_transaction
- should_support_transaction_timeout
- should_cleanup_on_transaction_drop
```
**Source files**: `engine_transactions.rs`, `txn_transaction_lifecycle.rs`, `txn_durability.rs`

### `transaction_isolation.rs` (~10 tests)
Isolation level guarantees.
```
- should_prevent_dirty_reads
- should_prevent_dirty_writes
- should_provide_snapshot_isolation
- should_prevent_phantom_reads
- should_see_consistent_snapshot
- should_track_read_set
- should_handle_concurrent_reads
- should_isolate_transactions_in_different_cfs
- should_enforce_read_committed_level
- should_enforce_serializable_level
```
**Source files**: `txn_isolation_levels.rs`, `transaction_isolation.rs`, `txn_snapshot_isolation_enforcement.rs`

### `transaction_conflicts.rs` ✅ (21 tests) — LocalDisk, CloudBacked
Conflict detection and resolution.
```
WRITE-WRITE CONFLICTS:
- should_detect_write_write_conflict_given_concurrent_updates_to_same_key
- should_preserve_first_commit_given_write_conflict_when_second_aborts
- should_reject_second_committer_on_write_write_conflict
- should_allow_concurrent_writes_to_different_keys
- should_preserve_both_updates_given_non_overlapping_keys_when_concurrent_commits

READ-WRITE CONFLICTS:
- should_detect_lost_update_given_cas_pattern_when_value_changed
- should_abort_second_transaction_given_write_conflict_when_both_commit
- should_prevent_lost_update_given_read_modify_write_when_concurrent
- should_commit_transaction_given_no_conflicts
- should_commit_transaction_given_concurrent_modifications_to_different_keys

OCC/OPTIMISTIC LOCKING:
- should_commit_new_key_given_clean_transaction
- should_read_values_within_transaction
- should_handle_high_concurrency_optimistic_locking

CONCURRENT/STRESS:
- should_handle_concurrent_read_modify_writes_without_panic
- should_handle_high_contention_writes_without_panic
- should_maintain_transaction_isolation_under_stress
- should_detect_conflict_on_delete_range_given_overlapping_keys
- should_handle_write_conflict_on_delete_given_concurrent_delete_and_put

DURABILITY:
- should_persist_lost_update_prevention_after_restart
- should_recover_conflict_state_after_engine_restart
- should_maintain_optimistic_locking_under_recovery
```
**Source files**: `txn_write_write_conflicts.rs`, `txn_occ_conflict.rs`, `txn_lost_updates.rs`, `txn_optimistic_locking.rs`

### `transaction_deadlock.rs` ✅ (11 tests) — LocalDisk, CloudBacked
Deadlock detection, victim selection, and read-write conflict handling.
```
CIRCULAR WAIT DETECTION:
- should_detect_deadlock_given_circular_wait_when_two_transactions
- should_detect_deadlock_given_three_way_circular_dependency

VICTIM SELECTION:
- should_abort_victim_transaction_given_deadlock_when_detected
- should_allow_retry_given_deadlock_victim_when_aborted

LIVELOCK PREVENTION:
- should_handle_high_concurrency_without_livelock

READ-WRITE CONFLICTS (SSI behavior):
- should_detect_read_write_conflict_given_concurrent_modification_to_read_key
- should_allow_read_only_transaction_given_no_conflict_on_read_keys

EDGE CASES:
- should_handle_self_conflict_given_same_key_multiple_writes
- should_handle_many_concurrent_transactions_on_disjoint_keys

DURABILITY:
- should_handle_recovery_after_complex_deadlock_scenario
- should_persist_winning_transaction_value_after_conflict_and_restart
```
**Source files**: `txn_deadlock_detection.rs`

### `transaction_spill.rs` ✅ (14 tests) — LocalDisk, CloudBacked + Memory
Large transaction memory management through spill-to-disk. Memory mode included to verify no disk artifacts.
```
LARGE TRANSACTION COMMIT:
- should_commit_large_transaction_given_many_writes_exceeding_memory_limit
- should_handle_very_large_transaction_given_multiple_spills

DATA INTEGRITY:
- should_preserve_data_integrity_given_large_transaction_with_specific_values
- should_preserve_key_order_given_large_transaction_when_iterating

ROLLBACK:
- should_rollback_spilled_transaction_given_drop_without_commit
- should_cleanup_spill_files_given_transaction_rollback

RECOVERY:
- should_rollback_uncommitted_spill_given_restart_before_commit
- should_recover_committed_spill_given_restart_after_commit

MEMORY PRESSURE:
- should_not_starve_foreground_writes_given_background_spill_activity
- should_handle_concurrent_large_transactions_given_memory_pressure

EDGE CASES:
- should_handle_transaction_with_tiny_memory_limit
- should_handle_mixed_small_and_large_values_in_spilled_transaction

MEMORY MODE (no disk):
- should_not_create_disk_artifacts_given_large_transaction_when_memory_mode
- should_handle_large_transaction_in_memory_mode_without_spill_files
```
**Source files**: `txn_transaction_spill_to_disk.rs`, `transaction_spill_pressure.rs`

### `transaction_advanced.rs` ✅ (16 tests) — LocalDisk, CloudBacked
Advanced transaction scenarios including edge cases, atomicity, and delete range integration.
```
EDGE CASES:
- should_commit_empty_transaction_given_no_operations
- should_commit_read_only_transaction_given_no_writes
- should_read_own_writes_given_nested_gets_within_transaction
- should_handle_rapid_transaction_creation_and_commit

ATOMICITY:
- should_commit_all_or_nothing_given_multi_key_transaction
- should_be_atomic_given_transaction_with_100_operations
- should_rollback_all_writes_given_transaction_dropped
- should_not_expose_partial_writes_given_concurrent_reader
- should_maintain_atomicity_under_concurrent_commits

DELETE RANGE INTEGRATION:
- should_preserve_snapshot_view_given_range_delete_after_snapshot
- should_abort_transaction_safely_given_delete_range_in_transaction
- should_recover_after_abort_given_transaction_with_delete_range

DURABILITY:
- should_persist_atomic_transactions_after_restart
- should_not_persist_uncommitted_transaction_after_restart

SEQUENTIAL TRANSACTIONS:
- should_handle_multiple_sequential_transactions_on_different_keys
- should_detect_write_conflict_given_sequential_updates_to_same_key
```
**Source files**: `txn_edge_cases.rs`, `txn_atomicity.rs`, `transaction_range_delete_integration.rs`

---

## 4. Durability & Recovery (`durability_*.rs`)

### `durability_wal.rs` ✅ (10 tests)
WAL durability guarantees.
```
BASIC PERSISTENCE:
- should_recover_writes_given_unflushed_memtable_when_reopening
- should_persist_write_given_fsync_enabled_when_crash_occurs
- should_call_fsync_given_wal_sync_enabled_when_put

WAL ROTATION & SEGMENTS:
- should_rotate_wal_given_small_buffer_when_writes_exceed_buffer
- should_replay_all_records_given_multiple_wal_segments_when_recovering

CONCURRENT WRITES:
- should_recover_all_writes_given_concurrent_puts_when_crash_occurs

CRASH & TRUNCATION:
- should_handle_gracefully_given_truncated_wal_tail_when_recovering
- should_not_recover_data_given_truncated_wal_append_when_reopening
- should_allow_data_loss_given_skipped_fsync_when_crash_occurs

RECOVERY MODE:
- should_tolerate_corrupted_tail_given_recovery_mode_set_when_reopening
```
**Source files**: `durability_wal.rs`, `durability_skip_fsync_recovery.rs`, `engine_wal_recovery.rs`, `durability_wal_truncate_sim.rs`

### `durability_recovery.rs` (~10 tests)
Crash recovery scenarios.
```
- should_recover_from_clean_shutdown
- should_recover_from_crash_after_flush
- should_recover_from_crash_during_flush
- should_recover_from_crash_during_compaction
- should_recover_manifest_after_crash
- should_handle_orphaned_sst_files
- should_handle_wal_manifest_divergence
- should_rebuild_state_from_wal
- should_handle_duplicate_wal_entries
- should_skip_records_before_checkpoint
```
**Source files**: `durability_recovery.rs`, `durability_recovery_edge.rs`, `wal_manifest_divergence.rs`, `durability_manifest.rs`

### `durability_atomicity.rs` (~6 tests)
Atomic persistence guarantees.
```
- should_atomically_commit_flush
- should_atomically_commit_compaction
- should_rollback_partial_sst_write
- should_handle_crash_between_sst_and_manifest
- should_handle_concurrent_cf_flush_atomicity
- should_handle_wal_truncation
```
**Source files**: `atomicity_wal_manifest_sst.rs`, `durability_compaction.rs`, `durability_engine_truncate_fallback.rs`, `durability_wal_truncate_sim.rs`

---

## 5. Compaction (`compaction_*.rs`)

### `compaction_basic.rs` (~8 tests)
Core compaction behavior.
```
- should_trigger_compaction_on_l0_threshold
- should_compact_l0_to_l1
- should_cascade_compaction_to_lower_levels
- should_remove_tombstones_during_compaction
- should_deduplicate_keys_during_compaction
- should_preserve_snapshot_data
- should_update_manifest_after_compaction
- should_handle_empty_level_compaction
```
**Source files**: `compaction_correctness.rs`, `engine_compaction.rs`

### `compaction_concurrent.rs` (~8 tests)
Compaction with concurrent operations.
```
- should_allow_reads_during_compaction
- should_allow_writes_during_compaction
- should_handle_flush_during_compaction
- should_serialize_concurrent_compactions
- should_handle_compaction_with_active_iterators
- should_handle_compaction_with_active_snapshots
- should_not_lose_data_during_concurrent_ops
- should_maintain_consistency_during_compaction
```
**Source files**: `compact_reads_during_compaction.rs`, `compact_writes_during_compaction.rs`, `concurrent_concurrent_compaction_and_writes.rs`

### `compaction_levels.rs` ✅ (15 tests)
Level management and organization.
```
L0 SUBLEVELS:
- should_organize_l0_into_sublevels_given_overlapping_files_when_flushing
- should_compact_oldest_sublevel_first_given_incremental_strategy_when_compacting
- should_compact_all_sublevels_given_high_file_count_when_aggressive_compaction
- should_maintain_sublevel_ordering_given_sequential_flushes_when_reading

LEVEL SIZE ENFORCEMENT:
- should_trigger_compaction_given_level_exceeds_target_size_when_sst_threshold_reached
- should_compact_largest_file_given_varying_sizes_when_level_too_large
- should_respect_level_multiplier_given_cascading_compaction_when_levels_fill
- should_not_exceed_target_size_given_completed_compaction_when_data_consolidated

CASCADING COMPACTION:
- should_trigger_l2_compaction_given_l1_exceeds_capacity_when_compacting
- should_propagate_compaction_to_deeper_levels_given_overflow_when_incremental_compaction
- should_handle_cascading_compaction_to_max_level_given_deep_structure_when_compacting
- should_not_trigger_cascade_given_sufficient_capacity_when_modest_data

LEVEL STATISTICS:
- should_report_sst_count_given_multiple_flushes_when_querying_stats
- should_reduce_sst_count_given_compaction_when_merging_files
- should_report_total_sst_size_given_data_written_when_querying_stats
```
**Source files**: `compact_l0_sublevel_compaction.rs`, `compact_level_target_size_enforcement.rs`, `compact_multi_level_compaction_cascades.rs`

### `compaction_filters.rs` (~6 tests)
Compaction filter functionality.
```
- should_apply_ttl_filter
- should_apply_custom_filter
- should_handle_prefix_drop_filter
- should_invoke_filter_for_each_key
- should_allow_filter_to_modify_value
- should_handle_filter_errors
```
**Source files**: `compact_ttl_compaction_filter.rs`, `compact_custom_compaction_filter.rs`

### `compaction_errors.rs` (~6 tests)
Error handling during compaction.
```
- should_handle_disk_full_during_compaction
- should_cleanup_partial_output_on_error
- should_restore_manifest_on_failure
- should_cancel_compaction_on_shutdown
- should_handle_sst_corruption
- should_retry_transient_errors
```
**Source files**: `compact_compaction_cancellation.rs`, `compact_compaction_error_recovery.rs`

### `compaction_metrics.rs` (~4 tests)
Compaction metrics and amplification.
```
- should_track_read_amplification
- should_track_write_amplification
- should_track_space_amplification
- should_report_compaction_statistics
```
**Source files**: `compact_amplification_measurement.rs`

---

## 6. Concurrency (`concurrency_*.rs`)

### `concurrency_writes.rs` (~8 tests)
Concurrent write handling.
```
- should_handle_concurrent_puts
- should_allocate_unique_sequence_numbers
- should_maintain_sequence_monotonicity
- should_serialize_writes_correctly
- should_handle_write_contention
- should_handle_memtable_freeze_during_writes
- should_route_writes_during_freeze
- should_not_lose_writes_during_high_concurrency
```
**Source files**: `concurrent_multi_threaded_write_stress.rs`, `concurrent_sequence_number_allocation.rs`, `concurrent_memtable_race_conditions.rs`

### `concurrency_flush.rs` (~5 tests)
Flush coordination under concurrency.
```
- should_handle_concurrent_flushes
- should_apply_backpressure_on_l0_buildup
- should_not_deadlock_flush_coordinator
- should_maintain_write_throughput_during_flush
- should_handle_flush_stalls_gracefully
```
**Source files**: `concurrent_flush_vs_write_contention.rs`, `concurrency_internal_invariants.rs`

### `concurrency_wal.rs` (~4 tests)
WAL concurrency handling.
```
- should_serialize_wal_writes
- should_maintain_wal_ordering
- should_handle_wal_rotation_during_writes
- should_handle_concurrent_sync_requests
```
**Source files**: `concurrent_wal_concurrency.rs`

### `concurrency_delete_range.rs` (~4 tests)
Concurrent delete range operations.
```
- should_handle_concurrent_delete_ranges
- should_handle_overlapping_concurrent_ranges
- should_handle_point_writes_during_delete_range
- should_handle_reads_during_delete_range
```
**Source files**: `concurrent_delete_range_concurrency.rs`

---

## 7. Cloud Storage (`cloud_*.rs`)

### `cloud_durability.rs` (~8 tests)
Cloud persistence guarantees.
```
- should_upload_sst_to_cloud
- should_persist_manifest_to_cloud
- should_handle_upload_failure
- should_retry_failed_uploads
- should_recover_from_cloud_after_local_loss
- should_handle_concurrent_uploads
- should_track_cloud_checkpoint
- should_handle_partial_cloud_state
```
**Source files**: `cloud_durability.rs`, `cloud_hybrid_faults.rs`

### `cloud_consistency.rs` (~6 tests)
Cloud consistency scenarios.
```
- should_handle_manifest_lag
- should_handle_eventual_consistency
- should_validate_cloud_checksums
- should_handle_corrupted_cloud_blob
- should_handle_stale_cloud_data
- should_synchronize_local_and_cloud
```
**Source files**: `cloud_consistency_edge_cases.rs`, `cloud_hybrid_faults.rs`

### `cloud_hybrid.rs` (~5 tests)
Hybrid local/cloud storage.
```
- should_cache_hot_data_locally
- should_evict_cold_data_to_cloud
- should_prefetch_from_cloud
- should_handle_cache_miss
- should_balance_local_and_cloud_storage
```
**Source files**: `cloud_hybrid_stress.rs`

---

## 8. Checkpoints (`checkpoint_*.rs`)

### `checkpoint.rs` (~10 tests)
Checkpoint creation and usage.
```
- should_create_checkpoint
- should_create_consistent_checkpoint
- should_create_checkpoint_with_multiple_cfs
- should_create_checkpoint_during_writes
- should_isolate_checkpoint_from_source
- should_open_checkpoint_as_readonly
- should_create_multiple_sequential_checkpoints
- should_handle_checkpoint_during_compaction
- should_recover_checkpoint_after_crash
- should_handle_disk_full_during_checkpoint
```
**Source files**: `checkpoint_lifecycle.rs`, `engine_checkpoint.rs`, `engine_checkpoint_stress.rs`, `checkpoint_compaction_recovery_triple.rs`

---

## 9. Configuration & Admin (`config_*.rs`, `admin_*.rs`)

### `config_validation.rs` (~5 tests)
Configuration validation.
```
- should_reject_invalid_memory_budget
- should_apply_config_on_startup
- should_preserve_config_across_restart
- should_validate_concurrent_config
- should_enforce_config_bounds
```
**Source files**: `config_validation.rs`

### `admin_operations.rs` (~6 tests)
Administrative operations.
```
- should_handle_backup_during_compaction
- should_handle_cf_drop_with_unflushed_data
- should_list_column_families
- should_handle_concurrent_admin_ops
- should_compact_range_manually
- should_flush_manually
```
**Source files**: `admin_concurrency.rs`

### `autotune.rs` (~5 tests)
Auto-tuning behavior.
```
- should_adjust_memtable_size
- should_prevent_oscillation
- should_respect_configured_limits
- should_apply_safe_defaults_on_restart
- should_track_autotune_metrics
```
**Source files**: `autotune_stability.rs`, `autotune_unit.rs`

---

## 10. Metrics & Observability (`metrics_*.rs`)

### `metrics.rs` (~8 tests)
Metrics and observability.
```
- should_report_current_sequence
- should_report_memory_usage
- should_report_sst_count
- should_report_read_amplification
- should_report_write_amplification
- should_report_cache_hit_rate
- should_report_bloom_filter_effectiveness
- should_report_compaction_statistics
```
**Source files**: `metrics_accessors.rs`

---

## 11. Error Handling (`error_*.rs`)

### `error_handling.rs` (~10 tests)
Error handling and recovery.
```
- should_handle_wal_corruption_tolerant_mode
- should_handle_wal_corruption_strict_mode
- should_handle_manifest_corruption
- should_handle_sst_corruption
- should_handle_disk_full_on_write
- should_handle_disk_full_on_flush
- should_handle_io_errors
- should_propagate_errors_correctly
- should_track_fsync_errors
- should_recover_from_transient_errors
```
**Source files**: `error_handling_core.rs`, `error_handling_flush.rs`

---

## 12. Edge Cases & Stress (`stress_*.rs`)

### `stress_large_values.rs` (~4 tests)
Large value handling.
```
- should_handle_large_values
- should_handle_mixed_value_sizes
- should_apply_backpressure_for_large_writes
- should_recover_large_values_from_wal
```
**Source files**: `large_value_stress.rs`

### `stress_workloads.rs` (~4 tests)
Realistic workload simulation.
```
- should_handle_hot_partition_appends
- should_maintain_low_tail_latencies
- should_handle_ttl_and_delete_mix
- should_track_write_amplification_under_load
```
**Source files**: `fitz_style_workloads.rs`

---

## 13. Special Modes

### `memory_mode.rs` (~3 tests)
In-memory only mode.
```
- should_not_write_to_disk_in_memory_mode
- should_support_cfs_in_memory_mode
- should_lose_data_on_restart_in_memory_mode
```
**Source files**: `memory_mode_no_disk_writes.rs`

### `readonly_mode.rs` (~3 tests)
Read-only mode.
```
- should_reject_writes_in_readonly_mode
- should_allow_reads_in_readonly_mode
- should_allow_delete_range_queries_in_readonly_mode
```
**Source files**: `engine_readonly_mode.rs`

### `paranoid_mode.rs` (~4 tests)
Paranoid checksum verification.
```
- should_verify_checksums_on_read
- should_verify_compressed_blocks
- should_be_disabled_by_default
- should_detect_corruption_with_paranoid_mode
```
**Source files**: `paranoid_checksum_mode.rs`

---

## 14. Caching (`cache_*.rs`)

### `cache_read_path.rs` (~5 tests)
Read path caching.
```
- should_cache_hot_blocks
- should_evict_cold_blocks
- should_use_bloom_filter_effectively
- should_handle_concurrent_cache_access
- should_track_cache_statistics
```
**Source files**: `read_path_caching.rs`

---

## 15. API Surface (`api_*.rs`)

### `api_kvstore.rs` (~10 tests)
KvStore trait implementation.
```
- should_put_via_kvstore_trait
- should_get_via_kvstore_trait
- should_delete_via_kvstore_trait
- should_scan_via_kvstore_trait
- should_delete_range_via_kvstore_trait
- should_cas_via_kvstore_trait
- should_batch_via_kvstore_trait
- should_handle_transactions_via_kvstore_trait
- should_handle_concurrent_inserts_via_kvstore_trait
- should_handle_cf_operations_via_kvstore_trait
```
**Source files**: `api_kvstore_adapter.rs`

---

## 16. LSM Invariants (`invariants_*.rs`)

### `invariants_lsm.rs` (~4 tests)
LSM-tree invariants.
```
- should_maintain_non_overlapping_sst_ranges_in_l1_plus
- should_sync_manifest_with_sst_directory
- should_prevent_orphan_sst_files
- should_recover_delete_range_correctly
```
**Source files**: `lsm_global_invariants.rs`

---

## 17. Test Infrastructure (`test_*.rs`)

### `test_infrastructure.rs` (~6 tests)
Test framework validation.
```
- should_enforce_test_naming_convention
- should_enforce_aaa_structure
- should_enforce_single_behavior_per_test
- should_skip_fsync_with_test_hook
- should_count_wal_appends_with_test_hook
- should_gate_compaction_with_test_hook
```
**Source files**: `test_guidelines_compliance.rs`, `test_hooks_integration.rs`

---

# Legacy File Mapping

Maps each legacy file to its target location(s) in the new structure.

| Legacy File | Target File(s) | Status |
|-------------|----------------|--------|
| `admin_concurrency.rs` | `admin_operations.rs` | ⬜ |
| `api_kvstore_adapter.rs` | `api_kvstore.rs` | ⬜ |
| `atomicity_wal_manifest_sst.rs` | `durability_atomicity.rs` | ⬜ |
| `autotune_stability.rs` | `autotune.rs` | ⬜ |
| `autotune_unit.rs` | `autotune.rs` | ⬜ |
| `checkpoint_compaction_recovery_triple.rs` | `checkpoint.rs` | ✅ |
| `checkpoint_lifecycle.rs` | `checkpoint.rs` | ✅ |
| `cloud_consistency_edge_cases.rs` | `cloud_consistency.rs` | ✅ |
| `cloud_durability.rs` | `cloud_durability.rs` | ✅ |
| `cloud_hybrid_faults.rs` | `cloud_durability.rs`, `cloud_consistency.rs` | ✅ |
| `cloud_hybrid_stress.rs` | `cloud_hybrid.rs` | ✅ |
| `cloud_real_providers.rs` | 🗑️ (requires real credentials) | ⬜ |
| `column_family_isolation.rs` | `column_family_isolation.rs` | ⬜ |
| `column_family_lifecycle.rs` | `column_family_lifecycle.rs` | ⬜ |
| `compact_amplification_measurement.rs` | `compaction_metrics.rs` | ⬜ |
| `compact_compaction_cancellation.rs` | `compaction_errors.rs` | ⬜ |
| `compact_compaction_error_recovery.rs` | `compaction_errors.rs` | ⬜ |
| `compact_custom_compaction_filter.rs` | `compaction_filters.rs` | ⬜ |
| `compact_l0_sublevel_compaction.rs` | `compaction_levels.rs` | ✅ |
| `compact_level_target_size_enforcement.rs` | `compaction_levels.rs` | ✅ |
| `compact_multi_level_compaction_cascades.rs` | `compaction_levels.rs` | ✅ |
| `compact_reads_during_compaction.rs` | `compaction_concurrent.rs` | ✅ |
| `compact_ttl_compaction_filter.rs` | `compaction_filters.rs` | ⬜ |
| `compact_writes_during_compaction.rs` | `compaction_concurrent.rs` | ✅ |
| `compaction_correctness.rs` | `compaction_basic.rs` | ⬜ |
| `concurrency_internal_invariants.rs` | `concurrency_flush.rs` | ✅ |
| `concurrent_concurrent_compaction_and_writes.rs` | `compaction_concurrent.rs` | ✅ |
| `concurrent_delete_range_concurrency.rs` | `concurrency_delete_range.rs` | ✅ |
| `concurrent_flush_vs_write_contention.rs` | `concurrency_flush.rs` | ✅ |
| `concurrent_memtable_race_conditions.rs` | `concurrency_writes.rs` | ✅ |
| `concurrent_multi_threaded_write_stress.rs` | `concurrency_writes.rs` | ✅ |
| `concurrent_sequence_number_allocation.rs` | `concurrency_writes.rs` | ✅ |
| `concurrent_wal_concurrency.rs` | `concurrency_wal.rs` | ✅ |
| `config_validation.rs` | `config_validation.rs` | ⬜ |
| `durability_compaction.rs` | `durability_atomicity.rs` | ⬜ |
| `durability_engine_truncate_fallback.rs` | `durability_atomicity.rs` | ⬜ |
| `durability_manifest.rs` | `durability_recovery.rs` | ✅ |
| `durability_recovery.rs` | `durability_recovery.rs` | ✅ |
| `durability_recovery_edge.rs` | `durability_recovery.rs` | ✅ |
| `durability_skip_fsync_recovery.rs` | `durability_wal.rs` | ✅ |
| `durability_wal.rs` | `durability_wal.rs` | ✅ |
| `durability_wal_truncate_sim.rs` | `durability_wal.rs` | ✅ |
| `engine_atomics.rs` | `engine_basic.rs` | ✅ |
| `engine_basic_ops.rs` | `engine_basic.rs` | ✅ |
| `engine_cf_merge_operators.rs` | `engine_merge_operators.rs` | ✅ |
| `engine_checkpoint.rs` | `checkpoint.rs` | ✅ |
| `engine_checkpoint_stress.rs` | `checkpoint.rs` | ✅ |
| `engine_compaction.rs` | `compaction_basic.rs` | ⬜ |
| `engine_delete_range.rs` | `engine_delete_range.rs` | ✅ |
| `engine_delete_range_core.rs` | `engine_delete_range.rs` | ✅ |
| `engine_iterator_edge.rs` | `engine_iterators.rs` | ✅ |
| `engine_merge_operator_correctness.rs` | `engine_merge_operators.rs` | ✅ |
| `engine_merge_operator_errors.rs` | `engine_merge_operators.rs` | ✅ |
| `engine_multi_get.rs` | `engine_basic.rs` | ✅ |
| `engine_readonly_mode.rs` | `readonly_mode.rs` | ⬜ |
| `engine_scans.rs` | `engine_basic.rs` | ✅ |
| `engine_snapshots.rs` | `engine_snapshots.rs` | ⬜ |
| `engine_sst_operations.rs` | `engine_basic.rs` | ⬜ |
| `engine_streaming.rs` | `engine_iterators.rs` | ✅ |
| `engine_transactions.rs` | `transaction_basic.rs` | ✅ |
| `engine_wal_recovery.rs` | `durability_wal.rs` | ✅ |
| `engine_write_batch_atomicity.rs` | `engine_write_batch.rs` | ⬜ |
| `engine_write_batch_edge.rs` | `engine_write_batch.rs` | ⬜ |
| `engine_write_options.rs` | `engine_basic.rs` | ⬜ |
| `error_handling_core.rs` | `error_handling.rs` | ✅ |
| `error_handling_flush.rs` | `error_handling.rs` | ✅ |
| `fitz_style_workloads.rs` | `stress_workloads.rs` | ⬜ |
| `iterator_lifecycle.rs` | `engine_iterators.rs` | ✅ |
| `iterator_stability_under_pressure.rs` | `engine_iterators.rs` | ✅ |
| `large_value_stress.rs` | `stress_large_values.rs` | ⬜ |
| `lsm_global_invariants.rs` | `invariants_lsm.rs` | ⬜ |
| `memory_mode_no_disk_writes.rs` | `memory_mode.rs` | ✅ |
| `memtable_concurrency.rs` | `concurrency_writes.rs` | ⬜ |
| `memtable_freeze_edge_cases.rs` | `concurrency_writes.rs` | ⬜ |
| `merge_operator_failure_modes.rs` | `engine_merge_operators.rs` | ✅ |
| `metrics_accessors.rs` | `metrics.rs` | ⬜ |
| `multi_cf_compaction_fairness.rs` | `column_family_isolation.rs` | ⬜ |
| `multicf_compaction_recovery.rs` | `column_family_isolation.rs` | ⬜ |
| `paranoid_checksum_mode.rs` | `paranoid_mode.rs` | ⬜ |
| `range_delete_edge_cases.rs` | `engine_delete_range.rs` | ✅ |
| `range_tombstone_stress.rs` | `engine_delete_range.rs` | ✅ |
| `read_path_caching.rs` | `cache_read_path.rs` | ⬜ |
| `shutdown_semantics.rs` | `durability_recovery.rs` | ⬜ |
| `snapshot_lifecycle.rs` | `engine_snapshots.rs` | ⬜ |
| `snapshot_lifecycle_compaction.rs` | `engine_snapshots.rs` | ⬜ |
| `sst_key_encoding_bug.rs` | 🗑️ (regression test, consider keeping) | ⬜ |
| `test_guidelines_compliance.rs` | `test_infrastructure.rs` | ⬜ |
| `test_hooks_integration.rs` | `test_infrastructure.rs` | ⬜ |
| `test_timeout_demo.rs` | 🗑️ (demo only) | ⬜ |
| `transaction_isolation.rs` | `transaction_isolation.rs` | ✅ |
| `transaction_range_delete_integration.rs` | `transaction_advanced.rs` | ✅ |
| `transaction_spill_pressure.rs` | `transaction_spill.rs` | ✅ |
| `txn_atomicity.rs` | `transaction_advanced.rs` | ✅ |
| `txn_deadlock_detection.rs` | `transaction_deadlock.rs` | ✅ |
| `txn_durability.rs` | `transaction_basic.rs` | ✅ |
| `txn_edge_cases.rs` | `transaction_basic.rs` | ✅ |
| `txn_edge_cases.rs` | `transaction_advanced.rs` | ✅ |
| `txn_isolation_levels.rs` | `transaction_isolation.rs` | ✅ |
| `txn_lost_updates.rs` | `transaction_conflicts.rs` | ✅ |
| `txn_occ_conflict.rs` | `transaction_conflicts.rs` | ✅ |
| `txn_optimistic_locking.rs` | `transaction_conflicts.rs` | ✅ |
| `txn_snapshot_isolation_enforcement.rs` | `transaction_isolation.rs` | ✅ |
| `txn_transaction_lifecycle.rs` | `transaction_basic.rs` | ✅ |
| `txn_transaction_spill_to_disk.rs` | `transaction_spill.rs` | ✅ |
| `txn_write_write_conflicts.rs` | `transaction_conflicts.rs` | ✅ |
| `wal_manifest_divergence.rs` | `durability_recovery.rs` | ⬜ |

---

# Migration Statistics

| Category | Target Files | Target Tests | Legacy Files | Status |
|----------|--------------|--------------|--------------|--------|
| Engine Core | 6 | ~69 | 25 | ⬜ |
| Column Families | 2 | ~14 | 4 | ⬜ |
| Transactions | 6 | ~47 | 17 | ⬜ |
| Durability | 3 | ~24 | 12 | ⬜ |
| Compaction | 6 | ~38 | 13 | ⬜ |
| Concurrency | 4 | ~21 | 8 | ⬜ |
| Cloud | 3 | ~19 | 5 | ⬜ |
| Checkpoint | 1 | ~10 | 4 | ⬜ |
| Config/Admin | 3 | ~16 | 4 | ⬜ |
| Metrics | 1 | ~8 | 1 | ⬜ |
| Error Handling | 1 | ~10 | 2 | ⬜ |
| Stress/Edge | 2 | ~8 | 2 | ⬜ |
| Special Modes | 3 | ~10 | 3 | ⬜ |
| Caching | 1 | ~5 | 1 | ⬜ |
| API Surface | 1 | ~10 | 1 | ⬜ |
| LSM Invariants | 1 | ~4 | 1 | ⬜ |
| Test Infrastructure | 1 | ~6 | 3 | ⬜ |
| **TOTAL** | **~35** | **~319** | **97** | ⬜ |

**Reduction**: 97 legacy files → ~35 organized files (64% reduction)

---

## Notes

- `tests/common/` - Shared test utilities (kept in place)
- `tests/cloud/` - Cloud-specific test utilities (kept in place)  
- `testutils/` - Additional test utilities (kept in place)
- Files marked 🗑️ are candidates for deletion after review
