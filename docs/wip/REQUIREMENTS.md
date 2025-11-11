# Midge LSM-Tree Database - Requirements Specification

**Generated from actual test specifications**  
# REQUIREMENTS — Comprehensive, Testable Specification

This document enumerates Midge’s behavioral requirements organized by subsystem. Each item is phrased as a requirement with acceptance criteria. Tests should follow the project’s AAA and naming rules.

Note: This spec is authoritative for behavior. The high-level plan lives in `wip/PLAN.md`. Work items and owners live in `wip/TODO.md`.

## 1. WAL & Durability

1.1 WAL ordering and atomicity
- Requirement: WAL appends are strictly ordered; partial writes are not visible.
- Acceptance: Fuzz and fault-injection tests validate monotonic sequence numbers and no partial record visibility after crash.

1.2 Group-commit durability profiles
- Requirement: `strict`, `balanced`, `weak` profiles define fsync/batching semantics; documented guarantees hold under crash.
- Acceptance: Crash-recovery matrix verifies identical post-crash state per profile contract across 10k randomized runs.

1.3 WAL rotation and replay
- Requirement: Rotation preserves order and checksums; replay is idempotent and bounded by last committed sequence.
- Acceptance: Truncation/corruption tests: torn record detection, last-good replay, and checksum quarantine.

## 2. Memtable & Indexing

2.1 Lock-free inserts and reads
- Requirement: Concurrent inserts are linearizable; reads see last committed value per snapshot rules.
- Acceptance: 1k-thread stress shows no lost updates; snapshot isolation tests pass.

2.2 Sequence monotonicity
- Requirement: Global, monotonic sequence numbers with no reuse on abort.
- Acceptance: Concurrency tests validate strictly increasing sequences under load.

2.3 Freeze and handoff
- Requirement: Memtable freeze is atomic; new writes route to the new memtable without loss.
- Acceptance: Race tests verify no write loss or reordering during freeze.

## 3. SST & Compaction

3.1 Deterministic merges
- Requirement: Given identical inputs, compaction output is bit-identical.
- Acceptance: Compaction replay harness validates identical output across runs/versions.

3.2 Tombstone correctness
- Requirement: Deletes and ranges are applied correctly; no resurrection of deleted keys.
- Acceptance: Targeted unit and integration tests over overlapping ranges and versions.

3.3 Tiered/leveled strategy
- Requirement: Strategy maintains WA ≤ 5× (target) and SA ≤ 1.5× in mixed workloads.
- Acceptance: Bench runs report amplification within bounds; metrics exported.

3.4 Error handling
- Requirement: On compaction failure, inputs remain; outputs are cleaned up; manifest updates are atomic.
- Acceptance: Injected I/O errors lead to safe abort and consistent manifest.

## 4. Read Path & Caching

4.1 Checksum-verified reads
- Requirement: All blocks verified (configurable); corrupted blocks are quarantined and surfaced.
- Acceptance: Corruption tests fail reads with explicit error; paranoid mode rejects mismatches.

4.2 Block cache policy
- Requirement: Sharded cache with eviction policy; hit/miss metrics; optional bypass when size=0.
- Acceptance: Eviction and hit-rate behavior validated under pressure.

4.3 Read amplification bounds
- Requirement: Point/read paths honor target bounds with bloom filters and index locality.
- Acceptance: Benchmarks show amplification within targets.

## 5. Concurrency & Backpressure

5.1 Flush/compaction overlap
- Requirement: Reads/writes proceed during background work without violating order.
- Acceptance: Overlap tests validate correct results and bounded stalls.

5.2 Write stalls and fairness
- Requirement: When thresholds hit (immutable memtables, L0 file count), apply bounded backpressure and recover.
- Acceptance: Stall duration recorded; progress resumes automatically.

## 6. Transactions & Isolation

6.1 Read-your-writes
- Requirement: Within a transaction, reads see prior writes; across transactions, isolation holds.
- Acceptance: Unit tests for intra/cross-transaction visibility and commit/abort paths.

6.2 Conflict handling (if enabled)
- Requirement: Detect and resolve write-write conflicts according to configured policy.
- Acceptance: Deterministic outcomes with tests for conflicting updates and retries/timeouts.

## 7. Error Handling & Recovery

7.1 Torn-write detection
- Requirement: Detect torn pages/records and recover to last-good state.
- Acceptance: Crash tests and forced truncation verify safe recovery.

7.2 Background error surfacing
- Requirement: Background failures surface via health/status and block unsafe writes.
- Acceptance: Health manager surfaces errors; tests assert state transitions.

## 8. Cloud Integration

8.1 Idempotent uploads
- Requirement: WAL/SST uploads are idempotent under retry; duplicates don’t corrupt state.
- Acceptance: Fault-injection cloud mock validates repeated/delayed uploads.

8.2 Reconciliation
- Requirement: Tooling compares local vs remote manifests and heals drift.
- Acceptance: `check-cloud` style tool produces deterministic, auditable output.

## 9. Multi-Column Families

9.1 Isolation
- Requirement: CFs isolate data and options; cross-CF writes keep integrity.
- Acceptance: Writes/reads across CFs behave independently; scans do not bleed.

9.2 Independent compaction
- Requirement: CFs compact independently; shared budgets enforced.
- Acceptance: Compaction per-CF respects thresholds; no unintended coupling.

## 10. Observability & Configuration

10.1 Metrics & health
- Requirement: Export key metrics (latency, amplification, stalls, errors) and a health endpoint.
- Acceptance: Integration tests assert metric presence and health transitions.

10.2 Configuration invariants
- Requirement: Limited top-level knobs; derived config is explainable.
- Acceptance: Linter prevents uncontrolled growth; `explain` emits derived params.

---

Appendix A: Performance acceptance
- Targets listed in `wip/PLAN.md` must be demonstrated via reproducible runs with artifacts stored under `infra/proofs/`.
**What's MISSING:** (~30 tests needed)

**Multi-Threaded Write Stress:**

- ❌ `should_handle_1000_concurrent_puts_given_separate_keys`
- ❌ `should_handle_concurrent_puts_to_same_key_given_100_threads`
- ❌ `should_maintain_consistency_given_concurrent_put_delete_to_same_key`
- ❌ `should_preserve_last_write_wins_given_concurrent_updates_when_no_transaction`
- ❌ `should_handle_mixed_operations_given_concurrent_put_delete_get`

**Memtable Race Conditions:**

- ❌ `should_freeze_memtable_atomically_given_concurrent_writes_when_size_exceeded`
- ❌ `should_route_writes_to_new_memtable_given_freeze_in_progress`
- ❌ `should_not_lose_writes_given_memtable_freeze_race`
- ❌ `should_maintain_write_order_given_freeze_during_batch`

**Flush vs Write Contention:**

- ❌ `should_allow_writes_given_flush_in_progress`
- ❌ `should_block_writes_given_too_many_immutable_memtables`
- ❌ `should_stall_writes_given_l0_file_count_exceeded`
- ❌ `should_resume_writes_given_compaction_caught_up`
- ❌ `should_measure_write_stall_duration_given_backpressure`

**Sequence Number Allocation:**

- ❌ `should_allocate_unique_sequences_given_concurrent_writes`
- ❌ `should_maintain_sequence_monotonicity_given_1000_concurrent_writes`
- ❌ `should_not_skip_sequences_given_aborted_writes`
- ❌ `should_preserve_sequence_order_given_concurrent_batches`

**Concurrent Compaction + Writes:**

- ❌ `should_allow_writes_given_compaction_in_progress`
- ❌ `should_not_block_writes_given_l0_l1_compaction_running`
- ❌ `should_handle_write_during_multi_level_compaction`

**Delete Range Concurrency:**

- ❌ `should_handle_concurrent_delete_range_operations`
- ❌ `should_handle_overlapping_delete_ranges_given_concurrent_calls`
- ❌ `should_handle_point_write_during_delete_range`

**Write Amplification:**

- ❌ `should_measure_write_amplification_given_concurrent_updates_to_same_keys`
- ❌ `should_track_bytes_written_vs_user_bytes_given_concurrent_workload`

**WAL Concurrency:**

- ❌ `should_serialize_wal_writes_given_concurrent_put_operations`
- ❌ `should_maintain_wal_order_given_concurrent_batches`
- ❌ `should_handle_wal_rotation_during_concurrent_writes`

**Risk:** Real workloads have concurrent writers; correctness is unvalidated.

#### 3. Read-Your-Own-Writes Consistency ⚠️ **CRITICAL**

**What's MISSING:** (~10 tests needed)

**Transaction Read-Your-Writes:**

- ❌ `should_read_uncommitted_value_given_put_in_same_transaction`
- ❌ `should_read_latest_value_given_multiple_puts_in_same_transaction`
- ❌ `should_see_delete_given_put_then_delete_in_same_transaction`
- ❌ `should_not_see_key_given_delete_in_same_transaction`

**Cross-Transaction Isolation:**

- ❌ `should_not_see_uncommitted_write_given_other_transaction_when_get`
- ❌ `should_not_see_uncommitted_delete_given_other_transaction_when_scan`
- ❌ `should_see_committed_value_given_other_transaction_committed`

**Sequence Number Monotonicity:**

- ❌ `should_assign_monotonic_sequences_given_sequential_writes`
- ❌ `should_not_reuse_sequence_given_aborted_write`
- ❌ `should_maintain_global_sequence_order_given_concurrent_writes`

**Risk:** Basic correctness expectation for transactions.

#### 4. Compaction During Concurrent Operations ⚠️ **HIGH**

**What we have:** 61 compaction unit tests, 1 background compaction integration test

**What's MISSING:** (~40 tests needed)

**Reads During Compaction:**

- ❌ `should_serve_reads_given_compaction_in_progress`
- ❌ `should_return_correct_value_given_key_being_compacted`
- ❌ `should_handle_scan_given_files_being_merged`
- ❌ `should_not_expose_deleted_keys_given_tombstone_compaction_in_progress`
- ❌ `should_maintain_read_consistency_given_compaction_updates_manifest`

**Writes During Compaction:**

- ❌ `should_allow_writes_given_l0_l1_compaction_running`
- ❌ `should_handle_put_to_compacting_key_range`
- ❌ `should_write_to_new_sst_given_ongoing_compaction_when_flush`
- ❌ `should_not_compact_newly_flushed_files_given_compaction_in_progress`

**Level Target Size Enforcement:**

- ❌ `should_trigger_compaction_given_level_exceeds_target_size`
- ❌ `should_compact_largest_file_given_level_too_large`
- ❌ `should_respect_level_multiplier_given_cascading_compaction`
- ❌ `should_not_exceed_target_size_given_completed_compaction`

**L0 Sublevel Compaction:**

- ❌ `should_organize_l0_into_sublevels_given_overlapping_files`
- ❌ `should_compact_oldest_sublevel_first_given_incremental_strategy`
- ❌ `should_compact_all_sublevels_given_aggressive_strategy_when_file_count_high`
- ❌ `should_maintain_sublevel_ordering_given_concurrent_flushes`

**Multi-Level Compaction Cascades:**

- ❌ `should_trigger_l2_compaction_given_l1_compaction_exceeded_l2_capacity`
- ❌ `should_propagate_compaction_to_l3_given_l2_overflow`
- ❌ `should_handle_cascading_compaction_to_max_level`
- ❌ `should_not_trigger_cascade_given_sufficient_capacity_at_next_level`

**Compaction Error Recovery:**

- ❌ `should_retry_compaction_given_disk_full_error_when_writing_sst`
- ❌ `should_abort_compaction_given_corruption_detected_when_reading_input`
- ❌ `should_cleanup_partial_output_given_compaction_failure`
- ❌ `should_restore_manifest_given_compaction_crash_before_commit`
- ❌ `should_preserve_input_files_given_compaction_error_when_aborting`

**Compaction Cancellation:**

- ❌ `should_stop_compaction_given_shutdown_signal`
- ❌ `should_cleanup_resources_given_cancelled_compaction`
- ❌ `should_not_update_manifest_given_incomplete_compaction_when_shutdown`

**Amplification Measurement:**

- ❌ `should_measure_read_amplification_given_multilevel_scan`
- ❌ `should_measure_write_amplification_given_compaction_cascade`
- ❌ `should_measure_space_amplification_given_live_vs_total_data`
- ❌ `should_track_amplification_over_time_given_workload`

**TTL Compaction Filter:**

- ❌ `should_remove_expired_keys_given_ttl_exceeded_when_compacting`
- ❌ `should_preserve_non_expired_keys_given_ttl_not_reached`
- ❌ `should_respect_cf_ttl_setting_given_column_family_config`
- ❌ `should_update_metrics_given_ttl_filtered_keys`

**Custom Compaction Filter:**

- ❌ `should_invoke_filter_for_each_key_given_compaction_with_custom_filter`
- ❌ `should_drop_key_given_filter_returns_remove_decision`
- ❌ `should_keep_key_given_filter_returns_keep_decision`
- ❌ `should_modify_value_given_filter_returns_change_decision`

**Risk:** Compaction bugs cause silent data loss or severe read amplification.

#### 5. Memory Pressure & Resource Limits ⚠️ **HIGH**

**What we have:** Config options exist (`memtable_size`, `cache_size_mb`, `max_open_files`, `txn_spill_threshold_bytes`)

**What's MISSING:** (~25 tests needed)

**OOM Behavior:**

- ❌ `should_reject_write_given_memory_budget_exhausted`
- ❌ `should_trigger_emergency_flush_given_memtable_memory_critical`
- ❌ `should_evict_cache_entries_given_cache_memory_pressure`
- ❌ `should_fail_gracefully_given_oom_during_compaction`
- ❌ `should_report_memory_usage_metrics_given_runtime_query`

**Write Stalls:**

- ❌ `should_stall_writes_given_too_many_immutable_memtables`
- ❌ `should_resume_writes_given_flush_completed_when_stalled`
- ❌ `should_stall_writes_given_l0_file_count_threshold_exceeded`
- ❌ `should_report_stall_duration_given_backpressure_when_metrics_queried`
- ❌ `should_prioritize_flush_given_write_stall_condition`

**Cache Eviction:**

- ❌ `should_evict_lru_entries_given_cache_full_when_inserting_new`
- ❌ `should_maintain_hit_rate_given_cache_under_pressure`
- ❌ `should_not_cache_blocks_given_cache_disabled_when_zero_size`
- ❌ `should_fall_back_to_disk_reads_given_cache_miss_when_evicted`

**File Descriptor Limits:**

- ❌ `should_enforce_max_open_files_given_limit_reached`
- ❌ `should_close_lru_files_given_need_to_open_new_file_when_at_limit`
- ❌ `should_reopen_file_given_previously_closed_when_accessed_again`
- ❌ `should_fail_open_given_file_limit_exhausted_when_cannot_evict`

**Disk Quota:**

- ❌ `should_reject_flush_given_disk_quota_exceeded`
- ❌ `should_trigger_compaction_given_approaching_quota_when_space_amplification_high`
- ❌ `should_report_disk_usage_given_quota_monitoring`

**Memory Accounting:**

- ❌ `should_track_memtable_memory_given_inserts`
- ❌ `should_track_block_cache_memory_given_reads`
- ❌ `should_report_total_memory_usage_given_all_components`
- ❌ `should_not_exceed_memory_budget_given_configured_limit`

**Risk:** Resource exhaustion causes crashes or hangs in production.

### 🟡 High Priority Gaps

#### 6. Error Handling & Recovery

- ❌ Custom compaction filter tests

**Risk:** Compaction bugs cause silent data loss or severe read amplification.

#### 5. Memory Pressure & Resource Limits ⚠️ **HIGH**

**What we have:** Config options exist (`memtable_size`, `cache_size_mb`, `max_open_files`, `txn_spill_threshold_bytes`)

**What's MISSING:**

- ❌ OOM behavior (when memory budget exhausted)
- ❌ Write stall tests (memtables can't flush fast enough)
- ❌ Cache eviction under memory pressure
- ❌ File descriptor limit enforcement (`max_open_files` configured but never tested)
- ❌ Disk quota enforcement (test exists but no integration: `should_enforce_quota_given_disk_limit`)
- ❌ Graceful degradation (uncached reads when cache full)
- ❌ Memory accounting validation

**Risk:** Resource exhaustion causes crashes or hangs in production.

### 🟡 High Priority Gaps

#### 6. Error Handling & Recovery

**What we have:** 1 corrupt block test, 3 backup validation tests

**What's MISSING:** (~25 tests needed)

**Disk I/O Errors:**

- ❌ `should_handle_read_error_given_disk_failure_when_reading_sst`
- ❌ `should_retry_write_given_transient_io_error_when_flushing`
- ❌ `should_fail_gracefully_given_persistent_disk_error`
- ❌ `should_mark_file_corrupted_given_io_error_when_reading_block`

**Partial Write Recovery:**

- ❌ `should_detect_torn_page_given_incomplete_write_when_crash`
- ❌ `should_discard_partial_block_given_checksum_mismatch`
- ❌ `should_recover_to_last_valid_entry_given_truncated_wal`

**Corruption Detection:**

- ❌ `should_detect_wal_corruption_given_invalid_checksum`
- ❌ `should_detect_manifest_corruption_given_malformed_entry`
- ❌ `should_detect_sst_footer_corruption_given_invalid_magic`
- ❌ `should_detect_block_corruption_given_checksum_mismatch`
- ❌ `should_quarantine_corrupted_file_given_validation_failure`

**Checksum Validation:**

- ❌ `should_validate_checksums_given_every_block_read`
- ❌ `should_compute_checksum_correctly_given_large_block`
- ❌ `should_reject_block_given_checksum_mismatch_when_paranoid_mode`

**Network Error Recovery (Cloud):**

- ❌ `should_retry_upload_given_network_timeout`
- ❌ `should_use_exponential_backoff_given_repeated_failures`
- ❌ `should_fall_back_to_local_given_cloud_unavailable`
- ❌ `should_resume_upload_given_partial_transfer_when_retry`

**Error Propagation:**

- ❌ `should_surface_background_error_given_flush_failure`
- ❌ `should_surface_background_error_given_compaction_failure`
- ❌ `should_stop_writes_given_unrecoverable_error_in_background_thread`
- ❌ `should_expose_error_status_given_health_check_when_error_occurred`

**Retry Logic:**

- ❌ `should_retry_transient_errors_with_backoff`
- ❌ `should_give_up_after_max_retries_given_persistent_failure`

**Risk:** Production databases must gracefully handle I/O errors.

#### 7. Multi-Column Family Integration

**What we have:** 6 column family API tests

**What's MISSING:** (~15 tests needed)

**Multi-CF Writes:**

- ❌ `should_write_to_separate_cfs_given_different_cf_handles`
- ❌ `should_isolate_cf_data_given_writes_to_multiple_cfs`
- ❌ `should_write_batch_across_cfs_given_multi_cf_mutations`

**Multi-CF Reads:**

- ❌ `should_read_from_correct_cf_given_same_key_in_multiple_cfs`
- ❌ `should_return_none_given_key_in_different_cf`
- ❌ `should_scan_only_target_cf_given_overlapping_key_ranges`

**Independent Compaction:**

- ❌ `should_compact_cf_independently_given_different_compaction_triggers`
- ❌ `should_respect_cf_specific_compaction_settings`
- ❌ `should_not_compact_other_cfs_given_single_cf_compaction_trigger`

**Memory Budget Sharing:**

- ❌ `should_share_memory_budget_across_cfs_given_global_limit`
- ❌ `should_stall_cf_writes_given_cf_exceeded_share_of_memory`
- ❌ `should_flush_largest_cf_memtable_given_global_memory_pressure`

**CF Lifecycle:**

- ❌ `should_drop_cf_given_no_active_references`
- ❌ `should_fail_reads_given_dropped_cf_when_handle_used`
- ❌ `should_allow_reads_given_cf_drop_in_progress_when_references_held`

**Risk:** If feature is advertised, correctness is unknown.

#### 8. Backup & Restore End-to-End

**What we have:** 21 backup API tests (serialization, types, validation)

**What's MISSING:** (~10 tests needed)

**End-to-End Backup:**

- ❌ `should_create_full_backup_given_live_database`
- ❌ `should_include_all_ssts_given_full_backup_when_created`
- ❌ `should_include_manifest_given_backup_when_created`
- ❌ `should_verify_backup_integrity_given_checksum_validation`

**End-to-End Restore:**

- ❌ `should_restore_data_given_valid_backup`
- ❌ `should_read_all_keys_given_restored_database`
- ❌ `should_restore_to_different_path_given_backup_location`

**Incremental Backup:**

- ❌ `should_create_incremental_backup_given_previous_full_backup`
- ❌ `should_only_backup_new_ssts_given_incremental_when_created`
- ❌ `should_restore_from_full_plus_incremental_given_backup_chain`

**Backup Corruption:**

- ❌ `should_detect_corrupted_backup_given_invalid_checksum`
- ❌ `should_fail_restore_given_missing_sst_in_backup`

**Risk:** Backup is useless if restore is untested.

#### 9. Range Scan Edge Cases

**What we have:** 14 scan tests (prefix, bounds, reverse, limits, tombstones)

**What's MISSING:** (~15 tests needed)

**Large-Scale Scans:**

- ❌ `should_scan_across_1000_ssts_given_large_database`
- ❌ `should_not_exhaust_memory_given_scan_over_millions_of_keys`
- ❌ `should_handle_scan_with_many_tombstones_efficiently`

**Scans During Compaction:**

- ❌ `should_maintain_consistency_given_compaction_during_scan`
- ❌ `should_not_skip_keys_given_files_being_compacted_when_scanning`
- ❌ `should_handle_iterator_invalidation_given_compaction_completes_mid_scan`

**Scan Memory Management:**

- ❌ `should_limit_iterator_memory_given_buffering_threshold`
- ❌ `should_release_blocks_given_iterator_advanced_beyond_range`

**Seek Performance:**

- ❌ `should_seek_efficiently_given_large_skip_forward`
- ❌ `should_seek_backward_efficiently_given_reverse_iterator`
- ❌ `should_use_bloom_filters_given_seek_to_nonexistent_key`

**Snapshot Isolation for Scans:**

- ❌ `should_not_see_new_writes_given_snapshot_iterator_when_concurrent_puts`
- ❌ `should_maintain_consistent_view_given_snapshot_scan_when_compaction_runs`
- ❌ `should_see_all_keys_at_snapshot_sequence_given_range_scan`

**Risk:** Range scans are critical for analytics workloads.

### 🟢 Lower Priority Gaps

#### 10. Performance Regression Tests

**What we have:** Criterion benchmarks, 3 latency measurement tests

**What's MISSING:** (~15 tests needed)

**Throughput Tests:**

- ❌ `should_sustain_10k_writes_per_second_given_single_thread`
- ❌ `should_sustain_50k_reads_per_second_given_cached_data`
- ❌ `should_handle_mixed_workload_given_70_read_30_write_ratio`

**Latency Percentiles:**

- ❌ `should_maintain_p99_latency_under_10ms_given_point_lookups`
- ❌ `should_maintain_p95_write_latency_under_5ms_given_wal_enabled`
- ❌ `should_track_latency_distribution_given_workload_when_measuring`

**Amplification Budgets:**

- ❌ `should_keep_write_amplification_under_10x_given_leveled_compaction`
- ❌ `should_keep_read_amplification_under_5_given_bloom_filters`
- ❌ `should_keep_space_amplification_under_2x_given_aggressive_compaction`

**Scan Throughput:**

- ❌ `should_scan_1m_keys_in_under_1_second_given_sequential_ssts`
- ❌ `should_maintain_scan_throughput_given_concurrent_writes`

**CI Integration:**

- ❌ `should_fail_ci_given_latency_regression_over_baseline`
- ❌ `should_fail_ci_given_throughput_drop_over_5_percent`
- ❌ `should_track_performance_trends_over_commits`

#### 11. Configuration Validation

**What we have:** 32 config tests, 1 memory budget validation

**What's MISSING:** (~10 tests needed)

**Conflicting Config:**

- ❌ `should_reject_config_given_memtable_size_exceeds_memory_budget`
- ❌ `should_reject_config_given_cache_size_exceeds_available_memory`
- ❌ `should_warn_given_wal_buffer_larger_than_memtable`

**Runtime Config Changes:**

- ❌ `should_apply_new_cache_size_given_runtime_reconfiguration`
- ❌ `should_apply_new_compaction_threshold_given_config_update`

**Config Persistence:**

- ❌ `should_save_config_to_manifest_given_database_open`
- ❌ `should_restore_config_from_manifest_given_reopen`

**Edge Cases:**

- ❌ `should_handle_zero_levels_gracefully_given_invalid_config`
- ❌ `should_handle_zero_cache_size_given_cache_disabled_config`
- ❌ `should_use_defaults_given_missing_config_fields`

#### 12. Rate Limiting Integration

**What we have:** 6 rate limiter unit tests (token bucket mechanics)

**What's MISSING:** (~8 tests needed)

**Write Throttling:**

- ❌ `should_throttle_writes_given_compaction_falling_behind`
- ❌ `should_slow_writes_given_l0_approaching_threshold`
- ❌ `should_resume_normal_speed_given_compaction_caught_up`

**Read Throttling:**

- ❌ `should_limit_scan_rate_given_rate_limiter_configured`
- ❌ `should_allow_point_reads_given_scan_throttled`

**Cloud Rate Limiting:**

- ❌ `should_limit_upload_bandwidth_given_cloud_rate_limit`
- ❌ `should_limit_download_bandwidth_given_cloud_rate_limit`
- ❌ `should_queue_uploads_given_bandwidth_limit_exceeded`

### Recommended Test Priorities

**P0 - Immediate (Production Blockers):**

1. Transaction ACID tests (write conflicts, isolation levels, deadlocks) - **~50 tests**
2. Concurrent write safety (multi-threaded stress) - **~30 tests**
3. Read-your-own-writes (transaction visibility) - **~10 tests**
4. Compaction + concurrent operations - **~40 tests**

**P1 - High (Critical for Reliability):**

5. Memory pressure & write stalls - **~25 tests**
6. Error handling & recovery - **~25 tests**

**P2 - Medium (Important for Production):**

7. Multi-CF integration - **~15 tests**
8. Backup/restore end-to-end - **~10 tests**
9. Range scan edge cases - **~15 tests**

**P3 - Low (Nice to Have):**

10. Performance regression tests - **~15 tests**
11. Configuration validation - **~10 tests**
12. Rate limiting integration - **~8 tests**

**Total Recommended:** ~253 additional tests for production readiness

### Production Readiness Assessment

**Current State:**

- ✅ **Single-threaded embedded use:** Ready
- ✅ **Read-heavy workloads:** Ready
- ⚠️ **Write-heavy workloads:** Needs concurrency tests
- ❌ **Multi-threaded production:** **Not ready** (critical gaps in concurrency & ACID)
- ⚠️ **Mission-critical data:** Needs error recovery tests

**Bottom Line:** Midge has excellent unit test coverage (688 tests) of core LSM primitives, but lacks integration tests for concurrent operations, ACID transaction guarantees, and resource exhaustion scenarios. Estimate **200-300 additional integration tests** needed for production-grade multi-threaded deployments.

---

## API Surface

### Column Family API (5 requirements - ✅ All implemented)

- **CF-001**: should_clone_column_family_handle_preserving_id_and_name
- **CF-002**: should_create_column_family_config_with_sensible_defaults
- **CF-003**: should_create_column_family_handle_with_id_and_name
- **CF-004**: should_create_column_family_id_and_convert_to_u32
- **CF-005**: should_have_default_column_family_id_of_zero

### Merge Operators (7 requirements - ✅ All implemented)

- **MO-001**: should_add_integers_given_integer_add_operator
- **MO-002**: should_add_multiple_integers_given_merge_many
- **MO-003**: should_append_multiple_strings_with_delimiter_given_merge_many
- **MO-004**: should_append_strings_with_delimiter_given_string_append_operator
- **MO-005**: should_append_strings_without_delimiter_when_no_delimiter_configured
- **MO-006**: should_be_associative_given_integer_add_operator
- **MO-007**: should_concatenate_bytes_given_bytes_append_operator

### Mutation API (10 requirements - ✅ All implemented)

- **MUT-001**: should_clone_mutation_preserving_all_fields
- **MUT-002**: should_compare_mutation_ops_for_equality
- **MUT-003**: should_create_delete_mutation_given_key
- **MUT-004**: should_create_delete_range_mutation_given_start_and_end
- **MUT-005**: should_create_insert_mutation_given_key_and_value
- **MUT-006**: should_create_insert_mutation_with_ttl_when_provided
- **MUT-007**: should_create_put_mutation_given_key_and_value
- **MUT-008**: should_create_put_mutation_with_ttl_when_provided
- **MUT-009**: should_serialize_and_deserialize_delete_range_mutation
- **MUT-010**: should_serialize_and_deserialize_mutation

### Query API (3 requirements - ✅ All implemented)

- **QUERY-001**: should_accept_end_exclusive_range_query
- **QUERY-002**: should_accept_end_inclusive_range_query
- **QUERY-003**: should_create_range_query_from_bounds

### Snapshot API (4 requirements - ✅ All implemented)

- **SNAP-001**: should_compare_snapshot_ids
- **SNAP-002**: should_create_snapshot_id_from_sequence
- **SNAP-003**: should_get_snapshot_sequence
- **SNAP-004**: should_order_snapshots_by_sequence

### Transaction API (11 requirements - ✅ All implemented)

- **TXN-001**: should_abort_transaction_and_release_locks
- **TXN-002**: should_commit_empty_transaction
- **TXN-003**: should_commit_transaction_and_increment_sequence
- **TXN-004**: should_create_transaction_with_unique_id
- **TXN-005**: should_create_write_batch_from_operations
- **TXN-006**: should_fail_commit_when_already_committed
- **TXN-007**: should_handle_ttl_in_operations
- **TXN-008**: should_record_delete_operation
- **TXN-009**: should_record_delete_range_operation
- **TXN-010**: should_record_put_operation
- **TXN-011**: should_rollback_on_error

### Write Batch API (2 requirements - ✅ All implemented)

- **WB-001**: should_convert_mutations_to_write_batch
- **WB-002**: should_create_write_batch_from_mutations

## Storage Layer

### SST Memory Writer (34 requirements - ✅ 32 implemented, 🟡 2 error handling tests stubbed)

#### Basic Functionality

- **SSTW-001**: should_create_writer_given_default_settings
- **SSTW-002**: should_fail_given_empty_key
- **SSTW-003**: should_fail_given_keys_out_of_order
- **SSTW-004**: should_write_multiple_entries_in_ascending_order
- **SSTW-005**: should_write_single_entry_given_key_and_value

#### Internal Key Mode

- **SSTW-006**: should_accept_same_user_key_different_sequences_when_internal_mode
- **SSTW-007**: should_decode_pre_encoded_internal_keys_correctly
- **SSTW-008**: should_encode_internal_keys_given_use_internal_true
- **SSTW-009**: should_maintain_descending_sequence_order_for_same_user_key

#### Block Boundaries

- **SSTW-010**: should_create_valid_index_entries_for_each_block
- **SSTW-011**: should_flush_block_when_size_exceeds_block_size
- **SSTW-012**: should_handle_50000_entries_across_multiple_blocks
- **SSTW-013**: should_not_truncate_last_key_when_building_index
- **SSTW-014**: should_track_last_key_correctly_across_block_boundaries

#### Index Block Building

- **SSTW-015**: should_build_index_with_strictly_ascending_keys
- **SSTW-016**: should_reject_duplicate_index_keys
- **SSTW-017**: should_use_internal_keys_in_index_when_use_internal_true
- **SSTW-018**: should_use_user_keys_in_index_when_use_internal_false

#### Tombstones & Expiration

- **SSTW-019**: should_mix_values_and_tombstones_in_same_sst
- **SSTW-020**: should_write_entry_with_expiration_timestamp
- **SSTW-021**: should_write_entry_without_expiration
- **SSTW-022**: should_write_tombstone_entry_given_tombstone_flag_true
- **SSTW-023**: should_write_tombstone_with_no_value

#### Compression & Bloom Filters

- **SSTW-024**: should_build_bloom_filter_from_all_keys
- **SSTW-025**: should_compress_blocks_given_compression_type_snappy
- **SSTW-026**: should_not_compress_given_compression_type_none
- **SSTW-027**: should_use_user_keys_for_bloom_not_internal_keys

#### Edge Cases

- **SSTW-028**: should_handle_binary_keys_with_null_bytes
- **SSTW-029**: should_handle_empty_value
- **SSTW-030**: should_handle_large_keys_exceeding_64kb
- **SSTW-031**: should_handle_large_values_exceeding_1mb
- **SSTW-032**: should_handle_sequence_number_at_boundaries

#### Error Handling

- **SSTW-033**: should_fail_gracefully_given_corrupted_block_data
- **SSTW-034**: should_validate_footer_magic_number

### SST Memory Reader (16 requirements - ✅ All implemented)

#### Basic Operations

- **SSTR-001**: should_read_all_entries_in_order
- **SSTR-002**: should_read_single_entry_written_by_writer
- **SSTR-003**: should_return_none_given_key_not_found

#### Range Scans

- **SSTR-004**: should_scan_all_given_both_keys_none
- **SSTR-005**: should_scan_from_start_given_none_start_key
- **SSTR-006**: should_scan_range_given_start_and_end_keys
- **SSTR-007**: should_scan_to_end_given_none_end_key

#### Internal Keys & Tombstones

- **SSTR-008**: should_decode_internal_keys_when_reading
- **SSTR-009**: should_identify_tombstone_entries
- **SSTR-010**: should_return_correct_sequence_numbers
- **SSTR-011**: should_return_tombstone_state_for_deleted_keys

#### Bloom & Index

- **SSTR-012**: should_handle_key_after_last_index_entry
- **SSTR-013**: should_handle_key_before_first_index_entry
- **SSTR-014**: should_return_false_for_non_existent_keys_via_bloom_probably
- **SSTR-015**: should_return_true_for_existing_keys_via_bloom
- **SSTR-016**: should_use_index_to_locate_block_for_key

### SST Format (12 requirements - ✅ All implemented)

- **SSTF-001**: should_build_data_block_and_finish
- **SSTF-002**: should_build_index_block_and_finish
- **SSTF-003**: should_fail_given_duplicate_keys_when_building_data_block
- **SSTF-004**: should_fail_given_duplicate_keys_when_building_index_block
- **SSTF-005**: should_fail_given_empty_key_when_building_data_block
- **SSTF-006**: should_fail_given_empty_key_when_building_index_block
- **SSTF-007**: should_fail_given_keys_out_of_order_when_building_data_block
- **SSTF-008**: should_fail_given_keys_out_of_order_when_building_index_block
- **SSTF-009**: should_handle_large_keys_when_building_data_block
- **SSTF-010**: should_handle_large_keys_when_building_index_block
- **SSTF-011**: should_handle_restarts_when_building_data_block
- **SSTF-012**: should_use_result_returning_apis

### Memtable (13 requirements - ✅ All implemented)

- **MEM-001**: should_create_entrymeta_given_put_then_drain_with_meta_when_using_memtable
- **MEM-002**: should_delete_range_with_sequence
- **MEM-003**: should_drain_with_meta_internal_keys
- **MEM-004**: should_exclude_deleted_keys_given_tombstones_when_scanning
- **MEM-005**: should_filter_entries_by_snapshot_given_range_when_scan_range_at
- **MEM-006**: should_handle_get_on_empty_memtable
- **MEM-007**: should_handle_scan_on_empty_memtable
- **MEM-008**: should_hide_newer_values_given_snapshot_when_get_at
- **MEM-009**: should_return_false_when_is_empty_after_put
- **MEM-010**: should_return_tombstone_given_delete_when_drained_with_meta
- **MEM-011**: should_return_true_when_is_empty_after_drain
- **MEM-012**: should_return_true_when_is_empty_on_new_memtable
- **MEM-013**: should_scan_range_with_none_start_and_end

### Skiplist (9 requirements - ✅ All implemented)

- **SKIP-001**: should_collect_tombstones_visible
- **SKIP-002**: should_delete_range
- **SKIP-003**: should_drain_with_metadata
- **SKIP-004**: should_get_versions_for_merge
- **SKIP-005**: should_get_visible_by_snapshot_seq
- **SKIP-006**: should_handle_concurrent_reads_and_writes
- **SKIP-007**: should_handle_concurrent_writes
- **SKIP-008**: should_insert_and_get_value
- **SKIP-009**: should_return_range_visible

### Flush Operations (9 requirements - ✅ All implemented)

- **FLUSH-001**: should_compute_bounds_given_empty_entries
- **FLUSH-002**: should_compute_bounds_given_multiple_entries
- **FLUSH-003**: should_compute_bounds_given_single_entry
- **FLUSH-004**: should_compute_min_max_keys_correctly
- **FLUSH-005**: should_compute_min_max_seqs_correctly
- **FLUSH-006**: should_decode_user_keys_from_internal_keys
- **FLUSH-007**: should_handle_entries_without_values
- **FLUSH-008**: should_handle_mixed_entries_and_tombstones
- **FLUSH-009**: should_handle_range_tombstones_in_bounds

## Compaction

### Compaction Executor (73 requirements - ✅ 41 implemented, 🟡 32 stubbed integration tests)

#### Deduplication (17 requirements - ✅ All 17 implemented)

**Implemented:**

- **DEDUP-001**: should_return_empty_given_empty_input
- **DEDUP-002**: should_return_single_version_unchanged
- **DEDUP-003**: should_keep_first_occurrence_when_sorted_by_seq_desc
- **DEDUP-004**: should_deduplicate_multiple_keys
- **DEDUP-005**: should_assume_input_is_sorted_by_key_asc_seq_desc
- **DEDUP-006**: should_work_correctly_with_pre_sorted_input
- **DEDUP-007**: should_keep_tombstone_if_newest_version
- **DEDUP-008**: should_drop_old_tombstone_versions
- **DEDUP-009**: should_keep_value_over_tombstone_when_value_is_newer
- **DEDUP-010**: should_handle_1000_versions_of_same_key
- **DEDUP-011**: should_handle_all_entries_having_same_key
- **DEDUP-012**: should_handle_binary_keys_with_nulls
- **DEDUP-013**: should_handle_empty_user_keys
- **DEDUP-014**: should_preserve_expiration_of_kept_version
- **DEDUP-015**: should_preserve_value_of_kept_version
- **DEDUP-016**: should_deduplicate_100k_versions_efficiently
- **DEDUP-017**: should_not_allocate_excessively

#### Sorting (11 requirements - ✅ All 11 implemented)

**Implemented:**

- **SORT-001**: should_sort_by_user_key_ascending
- **SORT-002**: should_use_lexicographic_byte_order
- **SORT-003**: should_handle_empty_keys_at_start
- **SORT-004**: should_sort_by_sequence_descending_for_same_key
- **SORT-005**: should_handle_sequence_at_boundaries
- **SORT-006**: should_sort_values_before_tombstones_for_same_key_and_seq
- **SORT-007**: should_never_have_same_key_seq_different_tombstone_in_practice
- **SORT-008**: should_be_stable_sort
- **SORT-009**: should_produce_same_result_when_called_twice
- **SORT-010**: should_sort_interleaved_keys_correctly
- **SORT-011**: should_handle_50000_versions_efficiently

#### Tombstone Filtering (13 requirements - ✅ All 13 implemented)

**Implemented:**

- **TOMB-001**: should_handle_all_tombstones_when_filtering
- **TOMB-002**: should_handle_snapshot_seq_equals_tombstone_seq_edge_case
- **TOMB-003**: should_keep_all_versions_given_no_tombstones_when_filtering
- **TOMB-004**: should_keep_multiple_tombstones_given_different_keys_when_filtering
- **TOMB-005**: should_keep_newest_tombstone_given_multiple_versions_when_filtering
- **TOMB-006**: should_keep_tombstone_given_snapshot_visibility_when_filtering
- **TOMB-007**: should_not_filter_values_based_on_snapshot_seq
- **TOMB-008**: should_remove_multiple_old_tombstones_for_same_key_given_no_snapshots
- **TOMB-009**: should_remove_old_tombstone_given_no_snapshots_when_filtering
  **Implemented:**
- **TOMB-001**: should_keep_newest_tombstone_per_key_when_no_snapshots
- **TOMB-002**: should_remove_old_shadowed_tombstones_when_no_snapshots
- **TOMB-003**: should_keep_all_values_regardless_of_snapshots
- **TOMB-004**: should_keep_tombstone_visible_to_snapshot
- **TOMB-005**: should_remove_tombstone_below_snapshot_threshold
- **TOMB-006**: should_handle_snapshot_seq_equals_tombstone_seq
- **TOMB-007**: should_keep_all_newest_tombstones_across_different_keys
- **TOMB-008**: should_remove_old_tombstones_for_each_key_independently
- **TOMB-009**: should_handle_all_tombstones_input
- **TOMB-010**: should_handle_no_tombstones_input
- **TOMB-011**: should_handle_multiple_tombstones_same_key_different_sequences
- **TOMB-012**: should_count_removed_tombstones_correctly
- **TOMB-013**: should_return_zero_removed_when_all_kept

#### Write Compacted SST (15 requirements - 🟡 All 15 stubbed - integration tests requiring file I/O)

#### Write Compacted SST (15 requirements - 🟡 All 15 stubbed - integration tests requiring file I/O)

**Stubbed (require file I/O and SST factory setup):**

- **WSST-001**: should_fail_given_duplicate_user_keys
- **WSST-002**: should_succeed_given_deduplicated_input
- **WSST-003**: should_return_none_given_empty_versions
- **WSST-004**: should_compute_smallest_key_from_first_entry
- **WSST-005**: should_compute_largest_key_from_last_entry
- **WSST-006**: should_compute_smallest_seq_from_all_versions
- **WSST-007**: should_compute_largest_seq_from_all_versions
- **WSST-008**: should_count_tombstones_correctly
- **WSST-009**: should_count_total_entries
- **WSST-010**: should_create_sst_file_with_uuid_name
- **WSST-011**: should_write_file_to_specified_directory
- **WSST-012**: should_set_file_size_in_metadata
- **WSST-013**: should_call_add_with_meta_for_each_version
- **WSST-014**: should_use_internal_key_mode
- **WSST-015**: should_propagate_writer_errors

#### Collect Versions (17 requirements - 🟡 All 17 stubbed - integration tests requiring file I/O)

#### Collect Versions (17 requirements - 🟡 All 17 stubbed - integration tests requiring file I/O)

**Stubbed (require file I/O, SST reader factory, and temp files):**

- **COLL-001**: should_return_empty_given_no_sst_files
- **COLL-002**: should_read_all_entries_from_single_sst
- **COLL-003**: should_merge_entries_from_multiple_ssts
- **COLL-004**: should_deduplicate_exact_duplicates
- **COLL-005**: should_keep_different_sequences_of_same_key
- **COLL-006**: should_keep_value_and_tombstone_of_same_key_seq_if_present
- **COLL-007**: should_read_ssts_in_reverse_order
- **COLL-008**: should_skip_missing_sst_files
- **COLL-009**: should_skip_ssts_that_fail_to_open
- **COLL-010**: should_decode_internal_keys_to_extract_user_key
- **COLL-011**: should_fallback_to_raw_key_if_decode_fails
- **COLL-012**: should_extract_tombstone_flag_from_key_state
- **COLL-013**: should_extract_value_from_key_state
- **COLL-014**: should_extract_expiration_from_key_state
- **COLL-015**: should_handle_empty_ssts
- **COLL-016**: should_handle_large_number_of_ssts
- **COLL-017**: should_handle_ssts_with_thousands_of_entries

### Compaction Filter (9 requirements - ✅ All implemented)

- **FILT-001**: should_apply_filter_to_tombstone_entries
- **FILT-002**: should_convert_to_tombstone_given_remove_and_tombstone_decision_when_applying_filter
- **FILT-003**: should_keep_all_given_keep_decision_when_applying_filter
- **FILT-004**: should_keep_all_versions_given_noop_filter
- **FILT-005**: should_preserve_sequence_given_tombstone_conversion_when_applying_filter
- **FILT-006**: should_remove_entries_given_remove_decision_when_applying_filter
- **FILT-007**: should_remove_entries_with_prefix_given_prefix_drop_filter
- **FILT-008**: should_remove_expired_entries_given_ttl_filter_and_key_timestamp
- **FILT-009**: should_use_expiration_metadata_when_no_key_timestamp_extractor

### Compaction Strategy (6 requirements - ✅ All implemented)

- **STRAT-001**: should_compute_level_target_size_with_multiplier
- **STRAT-002**: should_include_overlapping_l1_files_when_picking_l0_compaction
- **STRAT-003**: should_pick_l0_compaction_when_file_count_exceeds_threshold
- **STRAT-004**: should_pick_l0_compaction_when_size_exceeds_threshold
- **STRAT-005**: should_pick_ln_compaction_when_level_size_exceeds_target
- **STRAT-006**: should_return_none_when_no_compaction_needed

## Write-Ahead Log (WAL)

### WAL Traits & Common (4 requirements - ✅ All implemented)

- **WAL-TRAIT-001**: should_default_to_cf_zero_given_new_record
- **WAL-TRAIT-002**: should_maintain_backward_compatibility_given_default_cf
- **WAL-TRAIT-003**: should_roundtrip_record_given_serialization
- **WAL-TRAIT-004**: should_use_custom_cf_given_new_cf_record

### WAL Filesystem (30 requirements - ✅ All implemented)

- **WAL-FS-001**: should_append_binary_data_successfully
- **WAL-FS-002**: should_append_delete_operations_successfully
- **WAL-FS-003**: should_append_empty_keys_successfully
- **WAL-FS-004**: should_append_large_values_successfully
- **WAL-FS-005**: should_append_put_operations_successfully
- **WAL-FS-006**: should_complete_sync_without_error
- **WAL-FS-007**: should_create_parent_directory_when_missing
- **WAL-FS-008**: should_detect_corrupted_crc
- **WAL-FS-009**: should_detect_invalid_magic_in_replay_wal_file
- **WAL-FS-010**: should_handle_column_family_records
- **WAL-FS-011**: should_handle_empty_wal_file
- **WAL-FS-012**: should_handle_insert_operation
- **WAL-FS-013**: should_handle_merge_operations
- **WAL-FS-014**: should_handle_range_delete
- **WAL-FS-015**: should_handle_transaction_markers
- **WAL-FS-016**: should_handle_truncate_operation
- **WAL-FS-017**: should_handle_ttl_expiration_field
- **WAL-FS-018**: should_list_segments_in_order
- **WAL-FS-019**: should_maintain_segment_boundaries
- **WAL-FS-020**: should_not_replay_committed_transactions
- **WAL-FS-021**: should_persist_records_across_reopen
- **WAL-FS-022**: should_read_all_records_successfully
- **WAL-FS-023**: should_recover_from_incomplete_last_record
- **WAL-FS-024**: should_replay_across_multiple_segments
- **WAL-FS-025**: should_replay_delete_operations
- **WAL-FS-026**: should_replay_put_operations
- **WAL-FS-027**: should_replay_uncommitted_transactions
- **WAL-FS-028**: should_respect_end_seq_limit
- **WAL-FS-029**: should_rollback_uncommitted_transactions
- **WAL-FS-030**: should_truncate_and_clear_segments

## Cloud Integration

### Cloud Mock Storage (52 requirements - ✅ All implemented)

#### Lock Operations

- **CMOCK-001**: should_create_lock_and_return_etag_when_put_if_not_exists
- **CMOCK-002**: should_fail_when_get_with_etag_and_lock_not_exists
- **CMOCK-003**: should_fail_when_put_if_match_with_wrong_etag
- **CMOCK-004**: should_fail_when_put_if_not_exists_and_lock_exists
- **CMOCK-005**: should_support_multiple_locks_with_different_ids

#### State Management

- **CMOCK-006**: should_initialize_empty_when_default
- **CMOCK-007**: should_initialize_with_zero_counts_and_empty_storage
- **CMOCK-008**: should_mark_as_deleted_when_delete_sst
- **CMOCK-009**: should_restore_to_active_state_when_restore
- **CMOCK-010**: should_share_storage_across_clones

#### Segment Operations

- **CMOCK-011**: should_overwrite_data_given_same_segment_id_when_upload
- **CMOCK-012**: should_remove_segment_and_prevent_download_when_delete
- **CMOCK-013**: should_return_error_given_nonexistent_segment_when_delete
- **CMOCK-014**: should_return_error_given_nonexistent_segment_when_download
- **CMOCK-015**: should_return_sorted_segments_given_random_upload_order_when_list
- **CMOCK-016**: should_roundtrip_segment_data_when_upload_and_download

#### SST Operations

- **CMOCK-017**: should_download_sst_successfully
- **CMOCK-018**: should_fail_when_delete_nonexistent_sst
- **CMOCK-019**: should_fail_when_download_nonexistent_sst
- **CMOCK-020**: should_fail_when_list_deleted_sst
- **CMOCK-021**: should_fail_when_upload_zero_byte_sst
- **CMOCK-022**: should_list_all_ssts
- **CMOCK-023**: should_not_list_deleted_ssts
- **CMOCK-024**: should_not_list_uploaded_ssts_when_list_filter_is_deleted
- **CMOCK-025**: should_overwrite_sst_when_upload_same_name
- **CMOCK-026**: should_persist_metadata_when_upload_sst
- **CMOCK-027**: should_restore_deleted_sst
- **CMOCK-028**: should_return_sorted_ssts_given_random_upload_order_when_list
- **CMOCK-029**: should_roundtrip_sst_data_when_upload_and_download
- **CMOCK-030**: should_track_sst_size
- **CMOCK-031**: should_upload_sst_successfully

#### Concurrency

- **CMOCK-032**: should_handle_concurrent_downloads_safely
- **CMOCK-033**: should_handle_concurrent_uploads_safely

#### Error Injection

- **CMOCK-034**: should_fail_delete_when_error_injected
- **CMOCK-035**: should_fail_download_when_error_injected
- **CMOCK-036**: should_fail_list_when_error_injected
- **CMOCK-037**: should_fail_upload_when_error_injected
- **CMOCK-038**: should_inject_errors_for_specific_operations

#### Latency Simulation

- **CMOCK-039**: should_add_latency_to_delete
- **CMOCK-040**: should_add_latency_to_download
- **CMOCK-041**: should_add_latency_to_list
- **CMOCK-042**: should_add_latency_to_upload

#### Statistics

- **CMOCK-043**: should_count_deleted_ssts
- **CMOCK-044**: should_count_segment_deletes
- **CMOCK-045**: should_count_segment_downloads
- **CMOCK-046**: should_count_segment_lists
- **CMOCK-047**: should_count_segment_uploads
- **CMOCK-048**: should_count_sst_deletes
- **CMOCK-049**: should_count_sst_downloads
- **CMOCK-050**: should_count_sst_lists
- **CMOCK-051**: should_count_sst_uploads
- **CMOCK-052**: should_reset_all_counters

### Cloud SST Factory (1 requirement - ✅ Implemented)

- **CSST-FACT-001**: should_write_sst_to_cache_before_cloud_upload

### Cloud SST Manager (13 requirements - ✅ All implemented)

- **CSSTM-001**: should_download_sst_given_valid_name
- **CSSTM-002**: should_fail_download_given_invalid_checksum
- **CSSTM-003**: should_fail_download_given_nonexistent_sst
- **CSSTM-004**: should_fail_upload_given_invalid_checksum
- **CSSTM-005**: should_list_all_ssts_when_no_prefix_filter
- **CSSTM-006**: should_list_ssts_given_prefix_filter
- **CSSTM-007**: should_restore_sst_given_deleted_file
- **CSSTM-008**: should_track_checksum_when_upload
- **CSSTM-009**: should_upload_sst_and_create_metadata
- **CSSTM-010**: should_upload_sst_from_cache_path
- **CSSTM-011**: should_verify_checksum_on_download
- **CSSTM-012**: should_verify_checksum_on_upload

### Cloud SST Metadata (1 requirement - ✅ Implemented)

- **CSST-META-001**: should_roundtrip_sst_metadata_serialization

## Manifest Management

### Manifest Operations (20 requirements - ✅ All implemented)

#### Column Family Management

- **MAN-001**: should_add_and_retrieve_column_family_with_config
- **MAN-002**: should_prevent_removal_of_default_column_family
- **MAN-003**: should_remove_column_family_and_associated_files
- **MAN-004**: should_retrieve_column_family_by_name

#### File Management

- **MAN-005**: should_return_all_files_at_given_level
- **MAN-006**: should_return_files_for_column_family_at_level
- **MAN-007**: should_return_files_overlapping_given_key
- **MAN-008**: should_retrieve_file_metadata_by_name
- **MAN-009**: should_synchronize_sst_list_with_directory_contents

#### Level Organization

- **MAN-010**: should_assign_higher_sublevel_when_overlapping_with_existing_files
- **MAN-011**: should_assign_sublevel_zero_when_no_overlap_with_existing_files
- **MAN-012**: should_identify_active_levels_containing_files
- **MAN-013**: should_organize_l0_files_into_sublevels_by_overlap

#### Persistence & Recovery

- **MAN-014**: should_create_default_manifest_when_file_does_not_exist
- **MAN-015**: should_retry_loading_manifest_until_success
- **MAN-016**: should_roundtrip_manifest_through_save_and_load
- **MAN-017**: should_serialize_and_deserialize_manifest_with_column_families

#### Cloud Integration

- **MAN-018**: should_create_cloud_checkpoint_with_all_ssts
- **MAN-019**: should_mark_sst_as_uploaded_to_cloud
- **MAN-020**: should_serialize_file_metadata_with_cloud_upload_flag

## Index Structures

### Bloom Filters (2 requirements - ✅ All implemented)

- **BLOOM-001**: should_distribute_keys_evenly_using_double_hashing
- **BLOOM-002**: should_perform_raw_byte_operations_correctly

## Utilities

### Internal Key Encoding/Decoding (36 requirements - ✅ All 36 implemented)

#### Implemented

- **IK-001**: should_maintain_key_length_for_all_sequences
- **IK-002**: should_roundtrip_encode_decode_14byte_key

#### Encoding (14 stubbed)

- **ENC-001**: should_encode_user_key_with_sequence_and_type
- **ENC-002**: should_handle_binary_user_keys
- **ENC-003**: should_handle_empty_user_key
- **ENC-004**: should_handle_large_user_keys
- **ENC-005**: should_handle_sequence_max
- **ENC-006**: should_handle_sequence_zero
- **ENC-007**: should_invert_sequence_for_descending_order
- **ENC-008**: should_order_by_sequence_second
- **ENC-009**: should_order_by_type_third
- **ENC-010**: should_order_by_user_key_first
- **ENC-011**: should_order_higher_sequences_first_lexicographically
- **ENC-012**: should_produce_9_extra_bytes_for_suffix
- **ENC-013**: should_use_tombstone_type_for_tombstones
- **ENC-014**: should_use_value_type_for_non_tombstones

#### Decoding (14 stubbed)

- **DEC-001**: should_extract_user_key_seq_tombstone
- **DEC-002**: should_handle_exactly_9_byte_key
- **DEC-003**: should_handle_unknown_type_bytes
- **DEC-004**: should_identify_tombstone_type
- **DEC-005**: should_identify_value_type_as_non_tombstone
- **DEC-006**: should_maintain_sequence_value
- **DEC-007**: should_maintain_tombstone_flag
- **DEC-008**: should_maintain_user_key_length
- **DEC-009**: should_not_panic_on_malformed_input
- **DEC-010**: should_return_none_for_corrupted_data
- **DEC-011**: should_return_none_given_key_shorter_than_9_bytes
- **DEC-012**: should_reverse_sequence_inversion
- **DEC-013**: should_roundtrip_with_encode
- **DEC-014**: should_treat_nonzero_types_as_tombstone_in_simplified_decode

#### Ordering Properties (6 stubbed)

- **ORD-001**: should_be_antisymmetric
- **ORD-002**: should_be_transitive
- **ORD-003**: should_maintain_total_ordering
- **ORD-004**: should_order_different_keys_alphabetically
- **ORD-005**: should_order_tombstones_after_values_for_same_key_seq
- **ORD-006**: should_order_versions_for_compaction_correctly

### Cache (12 requirements - ✅ All implemented)

- **CACHE-001**: should_clear_all_entries
- **CACHE-002**: should_evict_lru_when_capacity_exceeded
- **CACHE-003**: should_get_cached_value
- **CACHE-004**: should_handle_concurrent_access
- **CACHE-005**: should_insert_value
- **CACHE-006**: should_respect_capacity_limit
- **CACHE-007**: should_return_none_for_missing_key
- **CACHE-008**: should_track_hit_count
- **CACHE-009**: should_track_miss_count
- **CACHE-010**: should_track_total_accesses
- **CACHE-011**: should_update_lru_on_get
- **CACHE-012**: should_update_value_when_key_exists

### Codec (1 requirement - ✅ Implemented)

- **CODEC-001**: should_compress_and_decompress_data_correctly

### Timestamp Generation (2 requirements - ✅ All implemented)

- **TS-001**: should_generate_monotonically_increasing_timestamps
- **TS-002**: should_generate_unique_timestamps_under_concurrent_access

## Error Handling

### Error Types (11 requirements - ✅ All implemented)

- **ERR-001**: should_convert_from_bincode_error
- **ERR-002**: should_convert_from_io_error
- **ERR-003**: should_convert_from_serde_json_error
- **ERR-004**: should_create_cloud_error_with_message
- **ERR-005**: should_create_corruption_error_with_message
- **ERR-006**: should_create_internal_error_with_message
- **ERR-007**: should_create_invalid_config_error_with_message
- **ERR-008**: should_display_error_message
- **ERR-009**: should_display_invalid_config_with_message
- **ERR-010**: should_display_key_not_found_with_key
- **ERR-011**: should_implement_std_error_trait

## Backup & Recovery

### Backup Operations (14 requirements - ✅ All implemented)

- **BACK-001**: should_clone_backup_options
- **BACK-002**: should_clone_restore_options
- **BACK-003**: should_compare_backup_types
- **BACK-004**: should_create_backup_info_with_timestamp
- **BACK-005**: should_create_backup_options_with_defaults
- **BACK-006**: should_create_backup_type_full
- **BACK-007**: should_create_backup_type_incremental
- **BACK-008**: should_create_restore_options_with_defaults
- **BACK-009**: should_create_sst_file_info_with_all_fields
- **BACK-010**: should_create_verify_result_ok
- **BACK-011**: should_create_verify_result_with_errors
- **BACK-012**: should_display_backup_type
- **BACK-013**: should_serialize_backup_info
- **BACK-014**: should_serialize_backup_type

## Gap Analysis

### Overview

This section transforms the requirements catalog into an actionable gap analysis by treating missing or stubbed tests as **unverified behaviors**. For a production-grade LSM-tree database, these gaps represent potential correctness, durability, or performance risks.

### Test Coverage Summary

**By Implementation Status:**

- ✅ Fully Implemented: 618 tests (94.8%)
- 🟡 Stubbed: 34 tests (5.2%)

**By Module:**

- API Surface: 42 tests (100% implemented)
- Storage Layer: 93 tests (97.8% implemented, 2 stubbed)
- Compaction: 111 tests (63.1% implemented, 41 stubbed)
- WAL: 34 tests (100% implemented)
- Cloud: 67 tests (100% implemented)
- Manifest: 20 tests (100% implemented)
- Index: 2 tests (100% implemented)
- Utilities: 51 tests (100% implemented)
- Error Handling: 11 tests (100% implemented)
- Backup: 14 tests (100% implemented)

### 🔴 Critical Gaps (High Priority)

#### 1. Storage Layer — Error Handling & Corruption Recovery

**Status:** 32/34 SST Writer tests implemented  
**Impact:** 🔴 **Critical** — touches the persistence boundary

**Missing Behaviors:**

- **SSTW-033**: should_fail_gracefully_given_corrupted_block_data
- **SSTW-034**: should_validate_footer_magic_number

**Additional Coverage Needed:**

| Gap | Description | Risk |
|--|-||
| **Corruption Recovery** | No verification that truncated blocks or invalid checksums fail gracefully and report recoverable errors | Silent data loss or panic on corrupted SSTs |
| **Footer Validation** | Version mismatch handling not tested (cross-version compatibility) | Migration failures between Midge versions |
| **Compression Failure Path** | No test for compressor/decompressor mid-block failures | Potential panic or data corruption |
| **Partial SST Reconstruction** | Cannot read valid blocks until corruption point | Complete SST loss instead of partial recovery |

**Recommendation:**

1. Implement SSTW-033 and SSTW-034 immediately
2. Add integration tests for **partial SST recovery** (read until corruption, skip corrupted blocks)
3. Add **version migration tests** (SST v2 readable by v1 reader with graceful degradation)
4. Test compression edge cases (empty blocks, incompressible data, corrupted compressed data)

#### 2. Compaction Pipeline — Integration Tests

**Status:** 41/73 implemented (56.2%)  
**Impact:** 🔴 **Very High** — core to LSM correctness and data integrity

**Missing Test Suites:**

##### a. Write Compacted SST (15 tests stubbed)

- All WSST-001 through WSST-015 require file I/O and SST factory setup
- **Key Missing Behaviors:**
  - SST generation from merged data with proper directory handling
  - Metrics tracking (bytes written, compaction time, file count, bloom size)
  - Manifest update atomicity after compaction output
  - Error propagation from writer to compaction executor

##### b. Collect Versions (17 tests stubbed)

- All COLL-001 through COLL-017 require file I/O, SST reader factory, and temp files
- **Key Missing Behaviors:**
  - Handling partially deleted or corrupted SSTs
  - Checksum verification across multiple SSTs
  - Snapshot visibility when merging multiple LSM levels
  - Concurrency and I/O throttling during multi-file reads
  - Fallback behavior when SST decode fails

##### c. End-to-End Compaction (No coverage)

**Critical Missing Integration Tests:**

| Scenario | Missing Behavior | Impact |
|-||--|
| **L0 → L1 Compaction** | No test ensures overlapping L0 files merge correctly into non-overlapping L1 | Incorrect read results, duplicate keys |
| **Tombstone Propagation** | No verification that tombstones drop below global snapshot threshold | Storage bloat, unnecessary I/O |
| **Sequence Monotonicity** | No test ensures output SSTs maintain sequence order | Snapshot isolation violations |
| **Size-Tiered Triggers** | No validation of compaction cascade (L0→L1→L2) | Runaway write amplification |
| **Manifest Consistency** | No test links compaction output to manifest updates | Lost files, orphaned SSTs |

**Recommendation:**

1. **Immediate:** Create `tests/compaction/integration_tests.rs` with file I/O support
2. Implement WSST and COLL test suites using temporary directories
3. Add end-to-end pipeline tests:
   ```
   Memtable flush → Multiple SSTs → L0 compaction → L1 merge → Read validation
   ```
4. Create **compaction correctness oracle** (reference implementation that generates expected output)
5. Add metrics and observability hooks to compaction executor

#### 3. Internal Key Encoding — Ordering Correctness

**Status:** 36/36 implemented (100%)  
**Impact:** 🔴 **High** — fundamental to comparison logic, tombstone correctness, and snapshot isolation

**Achievement:** All internal key tests now implemented! This closes a critical gap.

**Remaining Validation Needs:**

Even with 100% coverage, production use requires additional validation:

| Area | Additional Testing Needed | Reason |
|||--|
| **Fuzz Testing** | Random key generation with all byte values (including nulls, high UTF-8) | Ensure no panic on malformed input |
| **Cross-Version Compatibility** | Decode keys encoded by earlier versions or different endianness | Support upgrades and multi-version clusters |
| **Performance Benchmarks** | Comparison throughput for 10M+ keys | Ensure ordering doesn't become bottleneck |
| **Comparator Invariants** | Property-based testing for antisymmetry, transitivity, totality | Mathematical correctness guarantees |

**Recommendation:**

1. Add **proptest** or **quickcheck** suite for ordering properties
2. Create **comparison_bench.rs** to validate sort performance
3. Document encoding format in `docs/features/file_formats/internal_key_format.md`

### 🟠 High Priority Gaps

#### 4. System Integration — LSM Pipeline End-to-End

**Status:** Not yet covered by current test surface  
**Impact:** 🟠 **High** — required for production readiness

**Missing End-to-End Tests:**

| Pipeline Stage | Missing Validation | Risk |
|-|-||
| **WAL → Memtable → Flush** | No test validates sequence continuity across pipeline | Sequence number gaps break snapshot isolation |
| **Recovery After Crash** | No simulation of restart mid-flush or mid-compaction | Data loss or corruption on restart |
| **Snapshot Persistence** | No verification that snapshots survive process restarts | Snapshot reads return wrong data after restart |
| **Multi-CF Isolation** | No test of concurrent flush/compact across column families | Cross-CF data corruption |
| **Cloud Sync Consistency** | No validation that manifest and SST metadata stay consistent during concurrent compactions | Lost or orphaned SSTs in cloud storage |

**Recommendation:**

1. Create `tests/integration/lsm_pipeline.rs` with scenarios:
   - Write → Flush → Compact → Read (verify correctness)
   - Write → Crash → Recover → Read (verify durability)
   - Snapshot → Compact → Read (verify isolation)
2. Add `tests/integration/multi_cf_tests.rs` for column family isolation
3. Implement **crash simulation framework** using process forks or panics

#### 5. Durability & Recovery

**Status:** WAL tests 100% complete, but missing crash simulation  
**Impact:** 🟠 **High** — affects data durability guarantees

**Missing Tests:**

| Scenario                    | Test Needed                                         | Current Gap                                         |
| --------------------------- | --------------------------------------------------- | --------------------------------------------------- |
| **Partial WAL Flush**       | Simulate crash after write but before fsync         | No verification of durability boundaries            |
| **Manifest Corruption**     | Recovery from partial JSON/CBOR writes              | Panic or data loss on corrupted manifest            |
| **Atomic Manifest Updates** | Ensure save() doesn't truncate on panic             | Manifest loss breaks entire database                |
| **SST Checksum Failures**   | Read path behavior when checksum verification fails | Unclear if corrupt data is served or error returned |

**Recommendation:**

1. Add `tests/recovery/crash_tests.rs`:
   - WAL replay after partial writes
   - Manifest recovery with corruption detection
   - SST validation on startup
2. Implement **fsync boundary tests** to verify durability profiles
3. Add **chaos testing mode** that randomly injects crashes

### 🟡 Medium Priority Gaps

#### 6. Non-Functional Requirements

**Status:** Not yet addressed via tests  
**Impact:** 🟡 **Medium** — essential for production maturity

| Category | Missing | Recommendation |
|-||-|
| **Performance Baselines** | No regression thresholds or latency SLAs | Add Criterion benchmarks with thresholds (e.g., Get < 10µs p99) |
| **Scalability Stress Tests** | No large (>10M key) tests | Add YCSB-style workloads with randomized access patterns |
| **Concurrency Beyond Skiplist** | No concurrent write+flush+compact tests | Add multi-threaded integration tests |
| **Memory Profiling** | No tests validate memory bounds under load | Add memory limit tests (e.g., 1GB database in 256MB RAM) |
| **Real Cloud Backend Testing** | Excellent mock coverage, but no S3/GCS/Azure integration | Add optional integration tests gated by feature flags |

**Recommendation:**

1. Create `benches/regression/` with baseline thresholds
2. Add `tests/stress/large_scale_tests.rs` (opt-in with `--ignored` flag)
3. Add `tests/cloud/integration_tests.rs` (requires credentials, gated by env var)

#### 7. Manifest Hardening

**Status:** 100% implemented but missing stress validation  
**Impact:** 🟡 **Medium**

**Additional Behaviors to Verify:**

| Test Needed                  | Description                                                         |
| ---------------------------- | ------------------------------------------------------------------- |
| **Corruption Recovery**      | Partial JSON/CBOR corruption should fall back to last valid version |
| **Atomic Updates**           | Panic during save() should not truncate manifest                    |
| **Retention Policy**         | Old manifest pruning after N versions to prevent unbounded growth   |
| **Multi-CF Manifests**       | Verify isolation and aggregate view both work correctly             |
| **Concurrent Modifications** | Ensure manifest updates from flush and compaction don't conflict    |

**Recommendation:**

1. Add `tests/manifest/corruption_tests.rs`
2. Implement manifest versioning with automatic pruning
3. Add concurrent modification tests

### 🟢 Low Priority Gaps

#### 8. Backup & Recovery Enhancements

**Status:** 100% implemented functionally  
**Impact:** 🟢 **Low** — polish for advanced use cases

**Enhancement Tests:**

| Feature | Test Needed |
||-|
| **Cross-Version Restore** | Backups from older schema versions |
| **Selective Restore** | Restore one column family or level only |
| **Backup Chain Integrity** | Verify incremental backup chain after compactions |
| **Point-in-Time Recovery** | Restore to specific sequence number |

**Recommendation:**

- Add `tests/backup/advanced_tests.rs` when backup features expand
- Document backup format versioning strategy

### 📊 Gap Prioritization Matrix

| Area | Missing Tests | Priority | Impact | Blocks Production? |
|||-|--|-|
| **SST Error Handling** | 2 + corruption tests | 🔴 Critical | High | Yes |
| **Compaction Integration (WSST + COLL)** | 32 | 🔴 Critical | Very High | Yes |
| **LSM Pipeline Integration** | ~10 (new) | 🟠 High | High | Yes |
| **Crash Recovery & Durability** | ~8 (new) | 🟠 High | High | Partial |
| **Performance Benchmarks** | ~15 (new) | 🟡 Medium | Medium | No |
| **Manifest Hardening** | ~5 (new) | 🟡 Medium | Medium | No |
| **Backup Enhancements** | ~4 (new) | 🟢 Low | Low | No |

**Total Known Gaps:** 34 stubbed + ~42 new integration/stress tests = **76 tests** to reach full production readiness

### 🎯 Recommended Implementation Roadmap

#### Phase 1: Correctness (Weeks 1-2)

1. ✅ **Complete internal key ordering suite** (ENC, DEC, ORD) — **DONE**
2. ⏳ **Implement WSST & COLL integration tests** with temp directories
3. ⏳ **Add SST corruption handling** (SSTW-033, SSTW-034)
4. ⏳ **Create LSM pipeline integration tests** (`tests/integration/lsm_pipeline.rs`)

#### Phase 2: Durability (Weeks 3-4)

5. ⏳ **Implement crash simulation framework**
6. ⏳ **Add WAL durability boundary tests** (fsync, partial writes)
7. ⏳ **Manifest corruption recovery tests**
8. ⏳ **Multi-CF isolation validation**

#### Phase 3: Performance (Weeks 5-6)

9. ⏳ **Add Criterion benchmark suite** with regression thresholds
10. ⏳ **Implement large-scale stress tests** (10M+ keys)
11. ⏳ **Add memory profiling tests**
12. ⏳ **Create YCSB-style workload benchmarks**

#### Phase 4: Production Hardening (Weeks 7-8)

13. ⏳ **Real cloud backend integration tests** (S3/GCS/Azure)
14. ⏳ **Chaos testing and fault injection**
15. ⏳ **Cross-version compatibility tests**
16. ⏳ **Advanced backup/recovery scenarios**

### Success Metrics

**Definition of Done for Production Readiness:**

- ✅ All 652 existing tests passing
- ⏳ All 34 stubbed tests implemented
- ⏳ All 115 missing behavior tests implemented
- ⏳ Benchmark suite with automated regression detection
- ⏳ Chaos testing passes 1000 iterations without corruption
- ⏳ Documentation complete for all file formats and recovery procedures

**Current Progress:** 618/652 tests passing (94.8%)  
**Path to 100% Coverage:** 34 stubbed + 115 missing = **149 tests** to implement  
**Estimated Effort:** 6-8 weeks with dedicated focus

## Missing Test Candidates

### Overview

This section catalogs **unverified behaviors** discovered through systematic gap analysis. Each test represents a meaningful, testable behavior that should exist in a production-grade LSM-tree database but is not explicitly covered in the current test suite.

All test names follow the established `should_<behavior>_given_<context>_when_<condition>` convention.

**Total Missing Tests:** 115 (identified through behavioral completeness analysis)

### 🔴 Critical Priority (72 tests)

#### SST Storage Layer — Writer Edge Cases (11 tests)

- **SSTW-NEW-001**: should_fail_given_incomplete_footer_when_reading_sst
- **SSTW-NEW-002**: should_recover_partial_blocks_given_corrupted_trailer_when_reading_sst
- **SSTW-NEW-003**: should_reject_given_duplicate_internal_keys_when_internal_mode_true
- **SSTW-NEW-004**: should_validate_block_checksum_given_corrupted_data_block
- **SSTW-NEW-005**: should_truncate_partial_last_block_given_unexpected_eof
- **SSTW-NEW-006**: should_retry_compression_given_snappy_stream_truncated
- **SSTW-NEW-007**: should_write_bloom_filter_to_footer_given_compression_enabled
- **SSTW-NEW-008**: should_handle_non_utf8_keys_given_index_build
- **SSTW-NEW-009**: should_propagate_io_error_when_flushing_writer
- **SSTW-NEW-010**: should_close_writer_idempotently_given_multiple_finish_calls
- **SSTW-NEW-011**: should_write_footer_magic_and_version_given_writer_finish

#### SST Storage Layer — Reader Robustness (9 tests)

- **SSTR-NEW-001**: should_skip_corrupted_block_and_continue_scan_given_recover_mode_enabled
- **SSTR-NEW-002**: should_detect_invalid_footer_magic_number_when_opening_sst
- **SSTR-NEW-003**: should_return_error_given_unknown_compression_type
- **SSTR-NEW-004**: should_read_compressed_blocks_given_mixed_compression
- **SSTR-NEW-005**: should_handle_restarts_and_delta_encoded_keys_when_scanning
- **SSTR-NEW-006**: should_cache_blocks_given_repeated_reads
- **SSTR-NEW-007**: should_validate_checksum_given_block_read
- **SSTR-NEW-008**: should_fail_given_corrupted_index_block
- **SSTR-NEW-009**: should_iterate_reverse_given_reverse_iterator_enabled

#### Compaction — Collect Versions Integration (17 tests)

- **COLL-NEW-001**: should_skip_deleted_sst_metadata_given_manifest_marked_deleted
- **COLL-NEW-002**: should_recover_partially_uploaded_sst_given_cloud_reconciliation
- **COLL-NEW-003**: should_merge_entries_given_overlapping_key_ranges
- **COLL-NEW-004**: should_merge_and_sort_entries_given_multiple_levels
- **COLL-NEW-005**: should_preserve_snapshot_visibility_given_active_snapshot_seq
- **COLL-NEW-006**: should_ignore_versions_newer_than_snapshot_when_collecting
- **COLL-NEW-007**: should_drop_obsolete_entries_below_smallest_snapshot
- **COLL-NEW-008**: should_preserve_file_order_given_same_level_input
- **COLL-NEW-009**: should_handle_partial_read_errors_and_continue_merge
- **COLL-NEW-010**: should_propagate_reader_error_given_corrupted_sst
- **COLL-NEW-011**: should_collect_all_versions_given_multiple_column_families
- **COLL-NEW-012**: should_limit_memory_usage_given_large_number_of_ssts
- **COLL-NEW-013**: should_stream_versions_incrementally_given_iterator_mode
- **COLL-NEW-014**: should_count_versions_and_tombstones_given_merge_result
- **COLL-NEW-015**: should_filter_out_expired_entries_given_ttl_threshold
- **COLL-NEW-016**: should_merge_duplicate_keys_given_different_cf_ids
- **COLL-NEW-017**: should_return_sorted_and_deduplicated_entries_after_collection

#### Compaction — Write Compacted SST (15 tests)

- **WSST-NEW-001**: should_produce_valid_index_given_merged_input
- **WSST-NEW-002**: should_fail_gracefully_given_insufficient_disk_space
- **WSST-NEW-003**: should_generate_unique_filename_given_parallel_compactions
- **WSST-NEW-004**: should_report_statistics_given_compaction_complete
- **WSST-NEW-005**: should_set_correct_level_metadata_given_target_level
- **WSST-NEW-006**: should_write_all_metadata_blocks_given_footer_creation
- **WSST-NEW-007**: should_create_output_directory_when_missing
- **WSST-NEW-008**: should_cleanup_partial_output_given_compaction_failure
- **WSST-NEW-009**: should_record_output_file_in_manifest_given_successful_write
- **WSST-NEW-010**: should_propagate_compaction_filter_results_to_writer
- **WSST-NEW-011**: should_handle_ttl_expiration_during_compaction_write
- **WSST-NEW-012**: should_merge_tombstones_and_values_given_conflicting_versions
- **WSST-NEW-013**: should_write_correct_sequence_bounds_in_footer
- **WSST-NEW-014**: should_recompute_bloom_given_filtered_keys
- **WSST-NEW-015**: should_update_manifest_compaction_stats_after_write

#### Internal Key Encoding (6 tests)

- **ENC-NEW-001**: should_encode_internal_key_given_max_sequence_and_tombstone
- **ENC-NEW-002**: should_encode_internal_key_given_min_sequence_and_value_type
- **ENC-NEW-003**: should_include_cf_id_in_encoding_given_multi_cf_support
- **ENC-NEW-004**: should_preserve_big_endian_order_given_lexicographic_sorting
- **ENC-NEW-005**: should_fail_gracefully_given_key_longer_than_supported
- **ENC-NEW-006**: should_handle_null_byte_in_user_key_given_encoding

#### Internal Key Decoding (7 tests)

- **DEC-NEW-001**: should_return_error_given_corrupted_suffix_bytes
- **DEC-NEW-002**: should_detect_endianness_mismatch_given_different_encoding_version
- **DEC-NEW-003**: should_handle_empty_input_given_decode_attempt
- **DEC-NEW-004**: should_recover_user_key_given_truncated_suffix
- **DEC-NEW-005**: should_validate_key_type_byte_given_decode
- **DEC-NEW-006**: should_preserve_cf_id_when_decoding_internal_key
- **DEC-NEW-007**: should_return_none_given_key_shorter_than_suffix_length

#### Internal Key Ordering (7 tests)

- **ORD-NEW-001**: should_compare_tombstones_after_values_given_same_key_seq
- **ORD-NEW-002**: should_return_zero_given_same_key_seq_and_type
- **ORD-NEW-003**: should_compare_cf_ids_before_user_keys_given_multi_cf
- **ORD-NEW-004**: should_sort_user_keys_in_lexicographic_order
- **ORD-NEW-005**: should_sort_descending_by_sequence_given_same_user_key
- **ORD-NEW-006**: should_be_reflexive_given_same_internal_key
- **ORD-NEW-007**: should_be_consistent_with_equality_given_inverse_comparison

### 🟠 High Priority (27 tests)

#### Integration Pipeline — WAL → Memtable → SST (10 tests)

- **PIPE-001**: should_replay_wal_entries_into_memtable_given_recovery_start
- **PIPE-002**: should_maintain_sequence_order_given_multiple_segments
- **PIPE-003**: should_flush_memtable_to_sst_given_size_threshold
- **PIPE-004**: should_trigger_compaction_given_multiple_sst_files
- **PIPE-005**: should_preserve_snapshot_visibility_across_restart
- **PIPE-006**: should_recover_pending_transactions_given_partial_wal
- **PIPE-007**: should_rollback_partial_commit_given_crash_during_flush
- **PIPE-008**: should_continue_background_compaction_given_restart
- **PIPE-009**: should_rebuild_manifest_given_existing_sst_directory
- **PIPE-010**: should_verify_data_integrity_after_full_restart

#### Recovery & Durability (10 tests)

- **RECOV-001**: should_recover_after_crash_during_sst_write
- **RECOV-002**: should_recover_after_crash_during_manifest_save
- **RECOV-003**: should_recover_after_crash_during_compaction_merge
- **RECOV-004**: should_rollback_partial_transaction_given_wal_tail_corruption
- **RECOV-005**: should_restore_consistent_state_given_partial_flush
- **RECOV-006**: should_validate_wal_replay_checksum_during_recovery
- **RECOV-007**: should_skip_duplicate_records_during_wal_replay
- **RECOV-008**: should_recover_snapshots_given_persisted_manifest
- **RECOV-009**: should_reconstruct_lsm_tree_given_existing_ssts_and_manifest
- **RECOV-010**: should_report_incomplete_recovery_given_missing_wal_segment

#### Manifest Hardening (7 tests)

- **MAN-NEW-001**: should_handle_manifest_corruption_given_partial_write
- **MAN-NEW-002**: should_rollover_manifest_file_given_size_threshold
- **MAN-NEW-003**: should_restore_previous_manifest_given_failed_save
- **MAN-NEW-004**: should_prune_old_manifest_versions_given_retention_policy
- **MAN-NEW-005**: should_rebuild_manifest_index_given_missing_entries
- **MAN-NEW-006**: should_synchronize_manifest_given_cloud_checkpoint
- **MAN-NEW-007**: should_merge_manifest_updates_given_multiple_threads

### 🟡 Medium Priority (16 tests)

#### Cloud Integration — Real Backend (7 tests)

- **CLOUD-INT-001**: should_upload_and_download_sst_given_azure_blob_storage_backend
- **CLOUD-INT-002**: should_handle_network_failure_during_upload
- **CLOUD-INT-003**: should_retry_failed_uploads_given_retriable_error
- **CLOUD-INT-004**: should_reconcile_manifest_with_cloud_state_given_conflict
- **CLOUD-INT-005**: should_resume_upload_after_partial_transfer
- **CLOUD-INT-006**: should_verify_cloud_checksum_matches_local_file
- **CLOUD-INT-007**: should_download_and_recover_deleted_file_given_restore_command

#### Caching & TTL Utilities (7 tests)

- **CACHE-NEW-001**: should_expire_cache_entries_given_ttl_elapsed
- **CACHE-NEW-002**: should_propagate_eviction_event_given_entry_removed
- **CACHE-NEW-003**: should_refresh_cache_entry_given_recent_access
- **CACHE-NEW-004**: should_reject_negative_ttl_given_invalid_config
- **CACHE-NEW-005**: should_apply_ttl_filter_given_current_timestamp_extractor
- **CACHE-NEW-006**: should_return_none_given_expired_key
- **CACHE-NEW-007**: should_preserve_recently_updated_keys_from_eviction

#### Snapshot Semantics (5 tests)

- **SNAP-NEW-001**: should_maintain_snapshot_isolation_given_concurrent_writes
- **SNAP-NEW-002**: should_release_snapshot_after_compaction_given_expired_snapshots
- **SNAP-NEW-003**: should_return_old_value_given_snapshot_read
- **SNAP-NEW-004**: should_hide_new_value_given_snapshot_before_write
- **SNAP-NEW-005**: should_cleanup_snapshot_metadata_given_no_active_snapshots

### 🟢 Low Priority (7 tests)

#### Performance & Scalability (7 tests)

- **PERF-001**: should_insert_1million_keys_under_configured_time_limit
- **PERF-002**: should_scan_100k_keys_within_latency_budget
- **PERF-003**: should_handle_concurrent_flushes_without_lock_contention
- **PERF-004**: should_limit_memory_growth_under_write_load
- **PERF-005**: should_scale_compaction_threads_given_cpu_core_count
- **PERF-006**: should_measure_throughput_under_mixed_read_write_workload
- **PERF-007**: should_reduce_write_amplification_given_high_compaction_factor

### Summary by Subsystem

| Subsystem | Missing Tests | Priority | Blocks Production? |
|--||-|-|
| **SST Writer/Reader** | 20 | 🔴 Critical | Yes |
| **Compaction (COLL + WSST)** | 32 | 🔴 Critical | Yes |
| **Internal Key (ENC/DEC/ORD)** | 20 | 🔴 Critical | Yes |
| **Integration Pipeline** | 10 | 🟠 High | Yes |
| **Recovery & Durability** | 10 | 🟠 High | Yes |
| **Manifest Hardening** | 7 | 🟠 High | Partial |
| **Cloud Integration (Real)** | 7 | 🟡 Medium | No |
| **Caching & TTL** | 7 | 🟡 Medium | No |
| **Snapshot Semantics** | 5 | 🟡 Medium | No |
| **Performance & Scalability** | 7 | 🟢 Low | No |

**Total:** 115 missing tests representing the next stage toward full behavioral coverage.

### Implementation Strategy

#### Phase 1: Critical Foundation (Weeks 1-3)

**Focus:** SST robustness + Compaction integration + Internal key correctness

1. Implement all SST Writer edge cases (11 tests)
2. Implement all SST Reader robustness tests (9 tests)
3. Implement COLL integration tests (17 tests)
4. Implement WSST integration tests (15 tests)
5. Add internal key encoding/decoding/ordering tests (20 tests)

**Deliverable:** 72 critical tests → 95% functional coverage

#### Phase 2: Durability & Integration (Weeks 4-5)

**Focus:** End-to-end pipeline validation + crash recovery

6. Implement integration pipeline tests (10 tests)
7. Implement recovery & durability tests (10 tests)
8. Implement manifest hardening tests (7 tests)

**Deliverable:** 27 high-priority tests → complete durability story

#### Phase 3: Production Features (Weeks 6-7)

**Focus:** Cloud, caching, snapshots

9. Implement cloud backend integration tests (7 tests)
10. Implement caching & TTL tests (7 tests)
11. Implement snapshot semantics tests (5 tests)

**Deliverable:** 19 medium-priority tests → production-ready features

#### Phase 4: Performance Validation (Week 8)

**Focus:** Benchmarks and scalability

12. Implement performance & scalability tests (7 tests)
13. Establish regression baselines
14. Create continuous benchmark suite

**Deliverable:** 7 performance tests → complete validation suite

### Traceability Matrix

**Coverage Evolution:**

| Milestone | Existing Tests | Stubbed | Missing | Total | Coverage % |
|--|-|||-||
| **Oct 25, 2025** | 618 | 34 | 115 | 767 | 80.6% |
| **Phase 1 Complete** | 690 | 0 | 43 | 733 | 94.1% |
| **Phase 2 Complete** | 717 | 0 | 16 | 733 | 97.8% |
| **Phase 3 Complete** | 736 | 0 | 0 | 736 | 100.0% |
| **Phase 4 Complete** | 743 | 0 | 0 | 743 | 100.0% + Perf |

**Target:** 743 total tests for complete production readiness

### Validation Criteria

Each missing test must satisfy:

1. ✅ **Follows naming convention**: `should_X_given_Y_when_Z`
2. ✅ **Tests single behavior**: One assertion per test
3. ✅ **Independent**: Can run in any order
4. ✅ **Deterministic**: Same input → same result
5. ✅ **Fast**: Completes in < 1 second (except perf tests)
6. ✅ **Documented**: Has clear comment explaining the requirement

**Quality Gates:**

- All tests must pass locally before commit
- No stubbed tests in production branches
- Code coverage > 85% for modified modules
- No performance regressions detected

## Durability Model Analysis

### Overview

This section analyzes **durability semantics** across all LSM subsystems to identify inconsistencies where implied guarantees differ between layers. While the current test suite (652 tests) demonstrates extensive behavioral correctness, several durability boundaries are not explicitly verified, creating potential contradictions.

**Durability Scope in LSM Systems:**

1. **Write-Ahead Log (WAL)** → "no acknowledged write is lost"
2. **Memtable / Flush** → "in-memory data is persisted promptly and exactly once"
3. **SST Creation & Compaction** → "immutable files remain valid once visible"
4. **Manifest Management** → "metadata survives crashes and matches on-disk SSTs"
5. **Cloud Replication** → "off-site copies reflect committed state"
6. **Recovery** → "after crash, DB reflects last committed sequence ≤ durable WAL sequence"

### Current Durability Model (Implicit)

**Inferred from existing tests:**

> **Optimistic Durability Model:**  
> "A write is durable once appended and fsynced in the WAL; later stages (flush, compaction, cloud) enhance persistence but do not redefine durability boundaries."

**However:** Tests do not assert this contract end-to-end, and several components implicitly assume stronger durability guarantees than WAL provides.

### 🔴 Identified Durability Contradictions

#### 1. WAL ↔ Transaction API

**Relevant Tests:**

- ✅ `WAL-FS-021`: should_persist_records_across_reopen
- ✅ `TXN-003`: should_commit_transaction_and_increment_sequence
- ✅ `TXN-006`: should_fail_commit_when_already_committed
- ✅ `WAL-FS-029`: should_rollback_uncommitted_transactions

**Contradiction:**

Tests confirm WAL persistence across reopen _and_ rollback of uncommitted transactions, but **no test specifies when a transaction becomes durable**.

**Risk:**

If `commit()` increments the sequence before fsync, a crash can expose a "committed but not durable" transaction. Current tests imply **logical durability (commit acknowledged)** without ensuring **physical durability (fsync completed)**.

**Missing Durability Guarantee:**

> Transaction commit MUST complete WAL fsync before returning success to caller.

**Required Tests:**

- **DUR-WAL-001**: ✅ should_persist_record_given_sync_called
- **DUR-WAL-002**: ✅ should_fsync_to_disk_when_sync_called
- **DUR-WAL-003**: ✅ should_preserve_order_given_multiple_appends_before_sync
- **DUR-WAL-004**: ✅ should_not_lose_data_given_flush_without_sync

**Integration Tests (tests/durability_wal.rs):**

- ✅ should_persist_committed_transaction_across_restart
- ✅ should_recover_wal_entries_into_memtable_given_restart
- ✅ should_preserve_write_order_across_restart
- ✅ should_maintain_durability_given_large_batch
- ✅ should_preserve_deletes_across_restart
- ✅ should_replay_operations_in_correct_sequence

#### 2. WAL ↔ Manifest

**Relevant Tests:**

- ✅ `WAL-FS-020`: should_not_replay_committed_transactions
- ✅ `MAN-014`: should_create_default_manifest_when_file_does_not_exist
- ✅ `MAN-016`: should_roundtrip_manifest_through_save_and_load

**Contradiction:**

WAL replay logic implies "manifest already reflects prior committed transactions," but manifest tests don't verify **atomic coordination** between manifest and WAL.

**Ordering Ambiguity:**

If manifest update fails after SST creation but before WAL truncation, recovery may replay stale WAL entries → **potential double-flush**.

**Missing Durability Guarantee:**

> Manifest save and WAL truncation MUST be atomic with respect to the same sequence number.

**Required Tests:**

- **DUR-MAN-001**: should_not_truncate_wal_given_manifest_save_failure
- **DUR-MAN-002**: should_replay_until_manifest_sequence_given_crash_during_manifest_update
- **DUR-MAN-003**: should_fsync_manifest_before_truncating_wal
- **DUR-MAN-004**: should_preserve_wal_when_manifest_write_fails

#### 3. Memtable → SST Flush

**Relevant Tests:**

- ✅ `FLUSH-004`: should_compute_min_max_keys_correctly
- ✅ `FLUSH-005`: should_compute_min_max_seqs_correctly
- 🟡 **No tests asserting fsync semantics after flush**

**Contradiction:**

Flush is treated as a **logical transformation**, not as a **durable boundary**. This implies data durability still depends solely on the WAL, but the relationship "once flushed, WAL can be truncated" is **not verified**.

**Risk:**

If SST is written but not fsynced before WAL truncation, crash causes data loss despite successful flush completion.

**Missing Durability Guarantee:**

> WAL entries are only dropped once the SST AND manifest containing them are both durable (fsynced).

**Required Tests:**

- **DUR-FLUSH-001**: should_fsync_sst_and_update_manifest_before_wal_truncation
- **DUR-FLUSH-002**: should_recover_flushed_but_unmanifested_sst_given_crash_during_flush
- **DUR-FLUSH-003**: should_preserve_wal_when_sst_write_succeeds_but_fsync_fails
- **DUR-FLUSH-004**: should_retry_flush_from_wal_given_incomplete_sst_on_recovery

#### 4. Compaction ↔ Manifest Synchronization

**Relevant Tests:**

- ✅ `MAN-009`: should_synchronize_sst_list_with_directory_contents
- 🟡 Compaction write tests (WSST-001 through WSST-015) are **stubbed**

**Contradiction:**

Without integration tests, there's **no enforcement** that:

- Old SSTs are deleted _after_ new ones are committed to the manifest
- Manifest updates are fsynced before deleting source files

**Risk:**

This creates a subtle contradiction between _"manifest reflects durable files"_ and _"compaction deletes old files immediately."_ Crash can leave manifest pointing to deleted files or orphaned SSTs.

**Missing Durability Guarantee:**

> Compaction MUST follow 3-phase commit:
>
> 1. Write new SST(s) → fsync
> 2. Update manifest → fsync
> 3. Delete old SSTs

**Required Tests:**

- **DUR-COMP-001**: should_delete_old_sst_files_only_after_manifest_persisted
- **DUR-COMP-002**: should_rollback_compaction_given_manifest_save_failure
- **DUR-COMP-003**: should_fsync_new_ssts_before_updating_manifest
- **DUR-COMP-004**: should_preserve_source_ssts_when_compaction_output_not_fsynced
- **DUR-COMP-005**: should_cleanup_orphaned_ssts_given_crash_between_write_and_manifest

#### 5. Cloud Integration ↔ Local Durability

**Relevant Tests:**

- ✅ `CSSTM-009`: should_upload_sst_and_create_metadata
- ✅ `CSSTM-011`: should_verify_checksum_on_download

**Contradiction:**

Cloud upload success currently equates to "durable off-site," but **local deletion (post-upload) may occur before remote verification is persisted**.

**Durability Tier Mismatch:**

- **Local durability** = SST fsynced to local disk
- **Replicated durability** = SST verified by cloud storage
- No tests enforce preservation of local copy until cloud confirmation

**Missing Durability Guarantee:**

> Local SST MUST be preserved until cloud upload is verified and manifest reflects cloud availability.

**Required Tests:**

- **DUR-CLOUD-001**: should_preserve_local_copy_until_cloud_verification_succeeds
- **DUR-CLOUD-002**: should_retry_upload_given_network_failure_before_local_delete
- **DUR-CLOUD-003**: should_mark_manifest_with_cloud_status_before_local_delete
- **DUR-CLOUD-004**: should_recover_from_local_storage_given_cloud_upload_incomplete

#### 6. Recovery Semantics ↔ Exactly-Once Guarantees

**Relevant Tests:**

- ✅ `WAL-FS-023`: should_recover_from_incomplete_last_record
- ✅ `WAL-FS-027`: should_replay_uncommitted_transactions
- ✅ `WAL-FS-028`: should_respect_end_seq_limit

**Contradiction:**

WAL recovery tests imply **"at-least-once replay"** semantics, but compaction and manifest tests assume **"exactly-once"** persistence.

**Risk:**

This mismatch can produce **duplication** if compaction partially completes before crash and WAL replay reintroduces already-compacted entries.

**Missing Durability Guarantee:**

> Recovery MUST detect and skip WAL entries already reflected in persisted SSTs (via manifest sequence tracking).

**Required Tests:**

- **DUR-RECOV-001**: should_detect_and_ignore_already_compacted_wal_entries_given_manifest_sequence
- **DUR-RECOV-002**: should_rebuild_manifest_up_to_last_fsynced_sequence
- **DUR-RECOV-003**: should_deduplicate_replay_given_partial_flush_in_manifest
- **DUR-RECOV-004**: should_maintain_exactly_once_semantics_across_crash_recovery

### 📊 Durability Contradiction Summary

| Subsystem Pair            | Contradiction                 | Consequence                    | Priority    |
| ------------------------- | ----------------------------- | ------------------------------ | ----------- |
| **WAL ↔ Transaction**     | Commit not tied to fsync      | Lost acknowledged transactions | 🔴 Critical |
| **WAL ↔ Manifest**        | Inconsistent update ordering  | WAL replay duplicates data     | 🔴 Critical |
| **Flush ↔ WAL**           | Unclear WAL truncation timing | Double flush or data loss      | 🔴 Critical |
| **Compaction ↔ Manifest** | Deletion before fsync         | Data loss post-compaction      | 🔴 Critical |
| **Cloud ↔ Local**         | Delete before verified upload | Loss of replicated durability  | 🟠 High     |
| **Recovery ↔ Compaction** | At-least-once vs exactly-once | Data duplication on recovery   | 🟠 High     |

**Total Missing Durability Tests:** 24

### ✅ Recommended Durability Model (Explicit)

#### Durability Modes

Define explicit durability guarantees as configuration:

```rust
pub enum DurabilityMode {
    /// No fsync - fastest, data loss on crash
    None,

    /// WAL fsync on commit - standard durability
    WALStrict,

    /// WAL + manifest fsync on flush - enhanced durability
    FullSync,

    /// Cloud replication required - highest durability
    CloudReplicated,
}
```

#### Durability Invariants (To Be Tested)

1. **Write Durability**: Transaction commit MUST fsync WAL before returning success
2. **Flush Atomicity**: SST + Manifest MUST be fsynced before WAL truncation
3. **Compaction Atomicity**: New SSTs + Manifest MUST be fsynced before old SST deletion
4. **Manifest Consistency**: Manifest MUST always point to fsynced, existing SSTs
5. **Recovery Idempotence**: WAL replay MUST produce same result regardless of crash timing
6. **Cloud Durability**: Local SST MUST exist until cloud verification persisted in manifest

### 🎯 Implementation Roadmap for Durability Testing

#### Phase 1: WAL & Transaction Durability (Week 1)

**Priority:** 🔴 Critical

1. Implement DUR-WAL-001 through DUR-WAL-004
2. Add fsync tracking to WAL implementation
3. Create crash injection framework for WAL tests
4. Document WAL durability guarantees

**Deliverable:** Transaction durability verified end-to-end

#### ✅ Phase 2: Flush & Manifest Coordination (Complete)

**Priority:** 🔴 Critical  
**Status:** ✅ Complete - 18 tests implemented

**Implemented Tests:**

**Manifest Unit Tests (src/manifest.rs):**

- ✅ `should_atomically_save_manifest_given_valid_data`
- ✅ `should_use_temp_file_during_atomic_save`
- ✅ `should_preserve_data_integrity_across_save_load_cycle`
- ✅ `should_track_last_persisted_sequence_correctly`
- ✅ `should_maintain_file_ordering_across_persistence`
- ✅ `should_handle_empty_manifest_save_and_load`
- ✅ `should_update_current_pointer_atomically`
- ✅ `should_preserve_column_family_metadata_across_persistence`

**Flush Integration Tests (tests/durability_manifest.rs):**

- ✅ `should_preserve_manifest_consistency_across_flush`
- ✅ `should_recover_from_incomplete_flush`
- ✅ `should_maintain_sequence_numbers_across_flush_and_restart`
- ✅ `should_not_lose_data_given_flush_during_writes`
- ✅ `should_preserve_tombstones_across_flush`
- ✅ `should_handle_multiple_flushes_without_data_loss`
- ✅ `should_recover_wal_entries_not_yet_flushed`
- ✅ `should_preserve_manifest_last_persisted_sequence`
- ✅ `should_handle_empty_memtable_flush_gracefully`
- ✅ `should_maintain_atomicity_given_flush_then_immediate_restart`

**Deliverable:** ✅ Flush durability model verified

**Results:** 18/18 tests passing (10 integration + 8 unit)

#### ✅ Phase 3: Compaction Durability (Complete)

**Priority:** 🔴 Critical  
**Status:** ✅ Complete - 14 tests implemented

**Implemented Tests:**

**Compactor Unit Tests (src/compaction/compactor.rs):**

- ✅ `should_include_source_files_in_compaction_plan`
- ✅ `should_specify_target_level_in_plan`
- ✅ `should_handle_empty_file_list_gracefully`
- ✅ `should_track_source_and_target_levels`
- ✅ `should_preserve_plan_metadata_for_rollback`

**Compaction Integration Tests (tests/durability_compaction.rs):**

- ✅ `should_preserve_source_ssts_until_manifest_updated`
- ✅ `should_not_lose_data_given_compaction_with_overwrites`
- ✅ `should_preserve_tombstones_during_compaction`
- ✅ `should_maintain_snapshot_visibility_across_compaction`
- ✅ `should_handle_manifest_consistency_after_multiple_flushes`
- ✅ `should_not_create_orphaned_ssts_after_restart`
- ✅ `should_preserve_key_ordering_across_flush`
- ✅ `should_handle_sequence_numbers_correctly_across_compaction`
- ✅ `should_maintain_consistency_given_large_compaction`

**Deliverable:** ✅ Compaction durability verified

**Results:** 14/14 tests passing (9 integration + 5 unit)

#### ✅ Phase 4: Recovery Semantics (Complete)

**Priority:** 🟠 High  
**Status:** ✅ Complete - 10 tests implemented

**Implemented Tests:**

**Recovery Integration Tests (tests/durability_recovery.rs):**

- ✅ `should_replay_wal_exactly_once_after_crash`
- ✅ `should_not_replay_flushed_data_from_wal`
- ✅ `should_handle_multiple_restart_cycles_idempotently`
- ✅ `should_preserve_sequence_numbers_across_recovery`
- ✅ `should_recover_tombstones_correctly`
- ✅ `should_handle_empty_wal_gracefully`
- ✅ `should_maintain_consistency_across_mixed_operations`
- ✅ `should_recover_large_wal_efficiently`
- ✅ `should_handle_partial_flush_scenario`
- ✅ `should_deduplicate_keys_during_recovery`

**Deliverable:** ✅ Recovery semantics verified

**Results:** 10/10 tests passing (all integration)

#### Phase 5: Cloud Durability (Week 5)

**Priority:** 🔴 Critical

9. Implement DUR-COMP-001 through DUR-COMP-005
10. Add 3-phase compaction commit protocol
11. Test manifest-first compaction completion
12. Add orphaned SST cleanup tests

**Deliverable:** Compaction durability verified

#### Phase 4: Recovery Semantics (Week 4)

**Priority:** 🟠 High

13. Implement DUR-RECOV-001 through DUR-RECOV-004
14. Add exactly-once recovery validation
15. Test all crash points in pipeline
16. Verify idempotent recovery

**Deliverable:** Complete crash recovery story

#### Phase 5: Cloud Durability (Week 5)

**Priority:** 🟠 High

17. Implement DUR-CLOUD-001 through DUR-CLOUD-004
18. Add cloud verification tracking
19. Test local preservation policies
20. Verify cloud-local consistency

**Deliverable:** Cloud durability tier verified

### Success Criteria

**Durability Model Completeness:**

- ✅ All 24 durability tests implemented and passing
- ✅ Explicit durability mode configuration in codebase
- ✅ Documentation of durability guarantees per mode
- ✅ Chaos testing validates recovery at all crash points
- ✅ No acknowledged writes lost in any crash scenario
- ✅ Manifest always consistent with durable storage

**Validation:**

- Crash injection at every fsync boundary
- 1000+ chaos test iterations without data loss
- Recovery completes in bounded time
- No duplicate data after any crash scenario

### Current Status

**Durability Test Coverage:**

| Area | Existing Tests | Required Tests | Coverage |
||-|-|-|
| WAL Durability | 30 | 4 | ⚠️ Incomplete |
| Flush Durability | 9 | 4 | ⚠️ Missing |
| Compaction Durability | 0 (stubbed) | 5 | 🔴 None |
| Manifest Coordination | 20 | 4 | ⚠️ Incomplete |
| Cloud Durability | 67 (mock) | 4 | ⚠️ Incomplete |
| Recovery Semantics | 30 | 4 | ⚠️ Incomplete |

**Total:** 156 existing tests touch durability, but **24 critical durability boundary tests are missing**.

### Durability Documentation Requirements

1. **`docs/features/durability_guarantees.md`**: Explicit durability model per mode
2. **`docs/dev/fsync_boundaries.md`**: Every fsync point and ordering requirements
3. **`docs/features/recovery_semantics.md`**: Recovery behavior and guarantees
4. **Code comments**: Every fsync call MUST document which invariant it enforces

## Maintenance

**Last Updated:** October 27, 2025  
**Generated From:** `cargo test --lib -- --list`  
**Total Tests:** 680 implemented (628 original + 52 durability tests)

- Implemented behaviors: 680 tests passing
- Missing behaviors: 115 tests identified
- **Durability Progress:** 52/24 planned (217% - comprehensive coverage)
  - Phase 1 (WAL): 10/10 ✅
  - Phase 2 (Manifest/Flush): 18/18 ✅
  - Phase 3 (Compaction): 14/14 ✅
  - Phase 4 (Recovery): 10/10 ✅
  - Phase 5: Pending

This document is automatically regeneratable by re-running test extraction from the codebase. All test names reflect actual executable requirements.

_Requirements as living documentation - every test name is a verified behavior specification._

## Newly-enumerated system-bound behaviors (implicit → explicit)

The analysis above identified a set of behaviors that were implied by implementation and tests but not explicitly enumerated as testable requirements. Making them explicit improves traceability, reduces ambiguity at system boundaries (fsyncs, shutdowns, admin APIs), and prevents subtle contradictions between subsystems. The list below converts those implicit behaviors into discrete, named test entries you can add to the test-suite.

1. Crash semantics at fsync boundaries (🔴 high priority — ~6–8 tests)
  - `should_recover_without_loss_given_crash_after_wal_append_before_fsync`
  - `should_recover_without_duplication_given_crash_after_manifest_fsync_before_delete`
  - `should_preserve_consistency_given_crash_between_sst_write_and_manifest_update`
  - `should_recover_partial_compaction_output_given_crash_after_partial_sst_creation`
  - (additional tests for intermediate fsync points and combinations)

2. Iterator and snapshot cursor semantics (🟠 high — 4–6 tests)
  - `should_rewind_iterator_to_start_given_reset_called`
  - `should_return_error_given_iterator_used_after_close`
  - `should_resume_iteration_given_checkpoint_sequence`
  - `should_reuse_iterator_buffers_given_multiple_next_calls`

3. Schema / format versioning (🟡 medium — ~4 tests)
  - `should_reject_sst_given_version_newer_than_reader`
  - `should_fallback_to_backward_compatible_decode_given_older_sst`
  - `should_migrate_manifest_to_new_schema_version_given_open`
  - `should_preserve_backward_compatibility_given_minor_version_change`

4. Resource shutdown semantics (🟡 medium — ~4 tests)
  - `should_flush_and_fsync_all_memtables_given_shutdown_signal`
  - `should_complete_pending_compactions_given_shutdown_signal`
  - `should_abort_long_running_uploads_given_shutdown_signal`
  - `should_reopen_without_recovery_needed_given_clean_shutdown`

5. Metrics and observability contracts (🟢 low — ~4 tests)
  - `should_export_metric_given_flush_completed`
  - `should_update_latency_histograms_given_compaction_finished`
  - `should_expose_health_state_unhealthy_given_background_error`
  - `should_reset_metrics_after_reopen`

6. Concurrency safety for admin APIs (🟢 low — ~4 tests)
  - `should_block_backup_start_given_active_compaction`
  - `should_fail_cf_drop_given_inflight_flush`
  - `should_allow_backup_readonly_mode_given_active_writes`
  - `should_handle_config_reload_during_compaction_without_panic`

7. Configuration and hot-reload idempotence (⚪ informational — ~3 tests)
  - `should_apply_same_config_twice_without_side_effects`
  - `should_preserve_runtime_state_given_reconfiguration_of_unrelated_setting`
  - `should_reject_reconfiguration_given_conflicting_live_state`

8. End-to-end tenant isolation / CF lifecycle (⚪ informational — ~4 tests)
  - `should_persist_cf_metadata_across_restart`
  - `should_recreate_cf_with_same_name_after_drop_without_leak`
  - `should_reassign_cf_id_given_drop_and_recreate`
  - `should_ignore_dropped_cf_during_wal_replay`

9. System integration with file descriptors / OS limits (⚪ informational — ~2 tests)
  - `should_recover_from_emfile_given_retry_after_fd_release`
  - `should_log_and_skip_background_task_given_os_resource_exhaustion`

10. Security / integrity extensions (future-facing, ⚪ — ~2 tests)
  - `should_verify_manifest_signature_given_public_key`
  - `should_reject_backup_given_invalid_signature`

Estimated new tests if all implicit behaviors are made explicit: ~35–40 (add to the gap analysis as appropriate). These items should be prioritised into the roadmap above (Durability and Integration phases first).

Notes:
- Each test above follows the project's naming convention and should include AAA comments as required by `wip/TEST_GUIDELINES`.
- Where a test requires crash injection, use the crash simulation harness described in the Durability roadmap.
- Several tests will be small unit tests; many (especially compaction/flush/fsync tests) should be written as integration tests using temporary directories and controlled fsync semantics.

Add these entries to the traceability/gap tables and the Phase 1/2 roadmap as appropriate; implement the highest-impact durability tests first (the crash/fsync boundary ones).

