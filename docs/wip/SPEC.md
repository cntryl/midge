Perfect.
Here is the complete **Midge LSM-Tree Database — Full Behavioral Requirements Specification**, written as a pure behavioral contract with numbered sections and test-style acceptance lines.

---

# Midge LSM-Tree Database — Full Behavioral Requirements Specification

*All behaviors defined here are authoritative and testable. Each acceptance line is phrased as a behavioral test name describing the expected outcome.*

---

## Scope

Midge is an embedded, single-node database intended to be used as a local library or process. Distributed multi-node replication, leader election, and consensus protocols are out of scope for this specification and are not required for compliance.


## 1. Write-Ahead Log (WAL) & Durability

### 1.1 Ordering and Atomicity

Requirement: WAL appends must be strictly ordered and atomic; partial writes are never visible.
Acceptance: should_maintain_strict_wal_order_given_concurrent_appends_when_crash_occurs

### 1.2 Group Commit Profiles

Requirement: Durability profiles (`strict`, `balanced`, `weak`) define fsync frequency and batching guarantees.
Acceptance: should_persist_committed_records_given_durability_profile_strict_when_crash

### 1.3 Rotation and Replay

Requirement: WAL rotation preserves order and checksums; replay is idempotent.
Acceptance: should_replay_all_valid_records_given_multiple_segments_when_recovering

### 1.4 Partial Write Detection

Requirement: Torn or truncated WAL records are detected and truncated safely.
Acceptance: should_discard_partial_record_given_truncated_wal_segment_when_recovering

### 1.5 Commit Durability

Requirement: A transaction is acknowledged only after fsync completes for its WAL records.
Acceptance: should_not_acknowledge_commit_given_wal_unsynced_when_crash_occurs

---

## 2. Memtable & In-Memory Indexing

### 2.1 Lock-Free Inserts and Reads

Requirement: Concurrent inserts are linearizable; reads observe the latest committed value.
Acceptance: should_return_latest_value_given_concurrent_puts_to_same_key_when_read

### 2.2 Sequence Monotonicity

Requirement: Sequence numbers increase globally (shared across column families) and are never reused. Column families do not have independent sequence number spaces; CFs isolate data and compaction behavior but share the global sequence space.
Acceptance: should_generate_strictly_increasing_global_sequence_numbers_given_parallel_writes

### 2.3 Freeze and Handoff

Requirement: Memtable freeze is atomic; new writes route to the next memtable immediately.
Acceptance: should_route_new_writes_to_new_memtable_given_freeze_in_progress_when_full

### 2.4 Flush Trigger

Requirement: Memtables flush when exceeding configured size threshold.
Acceptance: should_trigger_flush_given_memtable_exceeds_threshold_when_background_thread_runs

---

## 3. SST Files & Compaction

### 3.1 Deterministic Merges

Requirement: Compaction with identical input produces bit-identical SST output.
Acceptance: should_produce_identical_output_given_same_input_runs_when_compacting

### 3.2 Tombstone Correctness

Requirement: Delete markers suppress older versions; no resurrection of deleted keys.
Acceptance: should_remove_deleted_keys_given_tombstones_when_compaction_runs

### 3.3 Leveling Strategy

Requirement: Tiered/leveled compaction maintains target write and space amplification bounds.
Acceptance: should_keep_write_amplification_under_target_given_mixed_workload

### 3.4 Atomic Output

Requirement: Compaction writes new SSTs and updates the manifest atomically.
Acceptance: should_commit_new_ssts_and_manifest_together_given_compaction_successful

### 3.5 Compaction Cancellation

Requirement: In-progress compactions terminate safely on shutdown or cancellation.
Acceptance: should_cleanup_partial_output_given_cancelled_compaction_when_shutdown_signal_received

---

## 4. Read Path & Caching

### 4.1 Checksum Verification

Requirement: Every block read verifies its checksum before returning data.
Acceptance: should_reject_block_given_checksum_mismatch_when_paranoid_mode_enabled

### 4.2 Block Cache Policy

Requirement: Cached blocks obey LRU or shard-aware eviction; metrics report hits and misses.
Acceptance: should_evict_least_recently_used_entry_given_cache_full_when_insert_new_block

### 4.3 Read Amplification Bound

Requirement: Point and range reads remain within configured amplification targets.
Acceptance: should_limit_read_amplification_given_bloom_filters_and_index_locality

---

## 5. Concurrency & Backpressure

### 5.1 Flush/Compaction Overlap

Requirement: Reads and writes proceed correctly while background flush or compaction runs.
Acceptance: should_return_correct_value_given_concurrent_read_and_compaction

### 5.2 Write Stalls and Recovery

Requirement: When thresholds are exceeded, writes stall briefly and resume automatically.
Acceptance: should_resume_writes_given_flush_completed_after_stall_trigger

---

## 6. Transactions & Isolation

### 6.1 Read-Your-Writes

Requirement: A transaction sees its own uncommitted writes.
Acceptance: should_read_uncommitted_value_given_put_in_same_transaction_when_read

### 6.2 Cross-Transaction Isolation

Requirement: Uncommitted data from one transaction is invisible to others.
Acceptance: should_not_see_uncommitted_write_given_other_transaction_when_read

### 6.3 Atomic Commit and Abort

Requirement: A transaction either fully commits or fully aborts.
Acceptance: should_rollback_all_operations_given_transaction_abort_called

### 6.4 Conflict Detection

Requirement: Write-write conflicts resolve per configured policy (last-write-wins or error).
Acceptance: should_detect_conflict_given_concurrent_updates_to_same_key_when_commit

---

## 7. Error Handling & Recovery

### 7.1 Torn-Write Detection

Requirement: Partial pages are detected and database recovers to last valid point.
Acceptance: should_recover_to_last_valid_state_given_incomplete_write_when_crash

### 7.2 Background Error Propagation

Requirement: Background failures surface through health status and prevent unsafe writes.
Acceptance: should_expose_error_status_given_background_thread_failure_when_health_checked

### 7.3 Retry and Backoff

Requirement: Transient I/O failures are retried with exponential backoff.
Acceptance: should_retry_write_given_transient_io_error_when_flush_fails

---

## 8. Cloud Integration

### 8.1 Idempotent Uploads

Requirement: WAL and SST uploads can be safely retried.
Acceptance: should_upload_sst_idempotently_given_duplicate_upload_attempt_when_network_flaky

### 8.2 Manifest Reconciliation

Requirement: Local and cloud manifests can be compared and healed deterministically.
Acceptance: should_reconcile_cloud_manifest_given_remote_drift_when_check_cloud_command_runs

### 8.3 Local Preservation Until Verified

Requirement: Local SST remains until upload verified.
Acceptance: should_preserve_local_file_given_upload_in_progress_when_crash

---

## 9. Multi-Column Families

### 9.1 Isolation

Requirement: Column families isolate data and compaction; they do not have independent sequence spaces (sequence numbers are global). Column families provide logical isolation but operate within the same global ordering for transactions and snapshots.
Acceptance: should_not_return_key_from_different_cf_given_same_user_key_when_read

### 9.2 Independent Compaction

Requirement: Each CF compacts independently within shared resource budgets.
Acceptance: should_compact_cf_independently_given_multiple_cfs_when_threshold_exceeded

### 9.3 Lifecycle

Requirement: CFs can be created, dropped, and reopened safely.
Acceptance: should_recreate_cf_with_same_name_given_previous_drop_when_reopen

---

## 10. Manifest Management

### 10.1 Atomic Persistence

Requirement: Manifest saves use atomic write-then-rename.
Acceptance: should_write_manifest_to_temp_and_rename_given_persist_called

### 10.2 Consistency

Requirement: Manifest always reflects only durable SSTs.
Acceptance: should_list_only_existing_ssts_given_manifest_load_when_restart

### 10.3 Versioning

Requirement: Each manifest record includes version and last durable sequence.
Acceptance: should_record_last_persisted_sequence_given_manifest_save

---

## 11. Backup & Restore

### 11.1 Full Backup

Requirement: Backup captures all live SSTs and manifest.
Acceptance: should_create_full_backup_given_live_database_when_backup_invoked

### 11.2 Incremental Backup

Requirement: Incremental backups include only new SSTs since last backup.
Acceptance: should_include_only_new_ssts_given_incremental_backup_after_full

### 11.3 Restore

Requirement: Restore reconstructs identical logical state.
Acceptance: should_restore_all_keys_given_valid_backup_when_restore_executed

---

## 12. Snapshot Semantics

### 12.1 Snapshot Isolation

Requirement: Reads through a snapshot observe consistent historical view.
Acceptance: should_return_old_value_given_snapshot_created_before_write

### 12.2 Snapshot Lifecycle

Requirement: Snapshots can be created, iterated, and released safely.
Acceptance: should_release_snapshot_resources_given_snapshot_closed_when_no_active_readers

---

## 13. Iterator & Range Scan Behavior

### 13.1 Iterator Lifecycle

Requirement: Iterators can be reset, rewound, and closed deterministically.
Acceptance: should_reset_iterator_to_start_given_rewind_called_when_end_reached

### 13.2 Stability During Compaction

Requirement: Active iterators remain valid during background compaction.
Acceptance: should_continue_iteration_given_compaction_in_progress_when_scan

### 13.3 Reverse Iteration

Requirement: Reverse iterators produce keys in descending order.
Acceptance: should_iterate_in_reverse_given_reverse_iterator_enabled_when_scan

---

## 14. Crash & Shutdown Semantics

### 14.1 Clean Shutdown

Requirement: Shutdown flushes and fsyncs all pending writes before exit.
Acceptance: should_persist_all_memtables_given_shutdown_signal_when_clean_exit

### 14.2 Crash Resilience

Requirement: Any crash yields recovery to the last committed sequence without duplication.
Acceptance: should_recover_last_committed_state_given_crash_during_write

### 14.3 Compaction Safety on Crash

Requirement: Compaction partially completed never corrupts database.
Acceptance: should_recover_consistent_state_given_crash_mid_compaction_when_restart

---

## 15. Durability Model

### 15.1 WAL Strict Durability

Requirement: In `WALStrict` mode, acknowledged writes are never lost.
Acceptance: should_not_lose_acknowledged_write_given_strict_durability_mode_when_crash

### 15.2 FullSync Durability

Requirement: WAL, manifest, and SST fsynced before write visible.
Acceptance: should_replay_to_last_synced_sequence_given_fullsync_mode_when_recover

### 15.3 CloudReplicated Durability

Requirement: Data considered durable only after verified cloud copy.
Acceptance: should_wait_for_cloud_ack_given_cloud_replicated_mode_when_commit

---

## 16. Observability & Metrics

### 16.1 Metrics Export

Requirement: Core metrics (latency, amplification, stalls, errors) are exported.
Acceptance: should_expose_metric_endpoint_given_metrics_enabled_when_server_running

### 16.2 Health States

Requirement: Database exposes Healthy, Degraded, and Failed states.
Acceptance: should_report_degraded_state_given_background_error_detected_when_health_checked

### 16.3 Event Logging

Requirement: Structured logs record lifecycle events and errors.
Acceptance: should_log_event_given_memtable_flush_completed_when_logging_enabled

---

## 17. Configuration & Runtime Behavior

### 17.1 Validation

Requirement: Invalid or conflicting configuration is rejected at startup.
Acceptance: should_reject_config_given_memtable_size_exceeds_memory_budget_when_open_called

### 17.2 Hot Reload

Requirement: Supported settings can be updated live without restart.
Acceptance: should_apply_new_cache_size_given_runtime_config_reload_when_requested

### 17.3 Idempotence

Requirement: Reapplying identical configuration has no effect.
Acceptance: should_not_restart_components_given_same_config_reapplied_when_reload

---

## 18. Resource Management

### 18.1 Memory Budget Enforcement

Requirement: Total memory usage respects configured limit.
Acceptance: should_reject_write_given_memory_budget_exhausted_when_insert

### 18.2 File Descriptor Limits

Requirement: The system reuses or evicts open files to stay under OS limits.
Acceptance: should_close_lru_file_given_fd_limit_reached_when_open_new_file

### 18.3 Disk Quota Enforcement

Requirement: Writes and flushes respect configured disk quota.
Acceptance: should_fail_flush_given_disk_quota_exceeded_when_triggered

---

## 19. Compatibility & Versioning

### 19.1 SST Format Versioning

Requirement: Older readers can open SSTs from compatible minor versions.
Acceptance: should_open_sst_given_previous_minor_version_when_read

### 19.2 Manifest Schema Evolution

Requirement: Manifests evolve forward-compatibly without breaking load.
Acceptance: should_load_manifest_given_older_schema_version_when_open

### 19.3 Endianness and Platform Independence

Requirement: Data files read identically across architectures.
Acceptance: should_produce_same_sorted_order_given_different_endianness_when_read

---

## 20. Performance & Scalability

### 20.1 Write Throughput

Requirement: Database sustains target write rate under benchmark load.
Acceptance: should_sustain_configured_write_throughput_given_continuous_load_when_measured

### 20.2 Latency Distribution

Requirement: p99 latency remains under configured bound for point reads and writes.
Acceptance: should_maintain_p99_latency_under_target_given_benchmark_workload

### 20.3 Compaction Scaling

Requirement: Compaction parallelism scales with available CPU cores.
Acceptance: should_spawn_multiple_compaction_threads_given_high_cpu_core_count_when_enabled

---

## 21. Security & Integrity

### 21.1 Checksums

Requirement: All persisted files include checksums verified on load.
Acceptance: should_fail_open_given_corrupted_checksum_when_sst_loaded

### 21.2 Manifest Integrity

Requirement: Manifest optionally signed or hashed for tamper detection.
Acceptance: should_verify_manifest_hash_given_integrity_check_enabled_when_load

---

## 22. System Integration & Administration

### 22.1 Backup Safety During Load

Requirement: Backups can run concurrently with reads but not with active compactions.
Acceptance: should_block_backup_start_given_active_compaction_when_requested

### 22.2 Maintenance Mode

Requirement: Maintenance mode allows read-only operations.
Acceptance: should_allow_reads_given_database_in_readonly_mode_when_backup_running

### 22.3 Admin API Consistency

Requirement: Administrative operations reflect current consistent state.
Acceptance: should_return_current_cf_list_given_admin_query_when_changes_in_progress

---

# End of Specification

Every line above defines a discrete, testable behavioral contract for Midge.
Together, these requirements describe the complete functional and operational semantics of the system — independent of implementation status or test coverage.
