# ✅ **ENGINE-LAYER TESTS**

### **engine_basic.rs**

```
should_get_value_given_existing_key_when_put
should_return_none_given_nonexistent_key_when_get
should_overwrite_value_given_existing_key_when_put
should_handle_empty_value_when_put
should_handle_binary_data_when_put
should_return_none_given_deleted_key_when_get
should_succeed_given_nonexistent_key_when_delete
should_not_create_filesystem_artifacts_when_memory_mode
```

### **engine_write_batch.rs**

```
should_commit_all_operations_given_batch_when_write_batch
should_apply_last_value_given_duplicate_keys_when_write_batch
should_succeed_given_empty_batch_when_write_batch
should_delete_key_given_delete_after_put_when_write_batch
should_delete_existing_key_given_delete_in_batch_when_write_batch
should_overwrite_existing_value_given_put_in_batch_when_write_batch
should_apply_mixed_operations_in_order_when_write_batch
should_handle_large_batch_given_many_operations_when_write_batch
should_persist_batch_given_flush_when_reopening
should_write_to_multiple_cfs_given_multi_cf_batch_when_write_batch
should_isolate_keys_given_same_key_in_different_cfs_when_write_batch
should_not_interleave_given_concurrent_batches_when_write_batch   ← rewritten for actor model
should_be_atomic_given_crash_during_wal_write_when_recovering
should_be_atomic_given_large_batch_crash_when_recovering
should_support_batch_with_ttl_when_write_batch
should_maintain_atomicity_during_concurrent_reads_when_write_batch  ← rewritten
should_increment_sequence_numbers_given_batch_operations_when_write_batch
```

### **engine_delete_range.rs**

```
should_delete_keys_in_range_given_delete_range_when_querying
should_delete_keys_across_levels_given_flushed_data_when_delete_range
should_handle_empty_range_given_start_equals_end_when_delete_range
should_hide_deleted_range_in_scan_given_delete_range_when_scanning
should_handle_large_range_deletion_given_many_keys_when_deleting
should_persist_delete_range_given_wal_when_recovering
should_recover_range_tombstone_given_no_flush_when_restarting
should_apply_delete_range_after_crash_given_flushed_tombstone_when_recovering
should_apply_range_tombstone_during_compaction_given_flushed_data_when_compacting
should_not_resurrect_deleted_keys_given_compaction_when_range_delete_applied
should_preserve_snapshot_view_given_delete_range_after_snapshot_when_reading
should_include_deleted_range_in_snapshot_scan_given_delete_after_snapshot_when_scanning
should_merge_overlapping_ranges_given_multiple_delete_ranges_when_deleting
should_allow_put_after_delete_range_given_interleaved_ops_when_writing
should_apply_memtable_and_sst_tombstones_given_mixed_sources_when_reading
should_reject_delete_range_given_read_only_mode_when_attempting
```

### **engine_iterators.rs**

```
should_iterate_all_keys_in_order_given_populated_db_when_scanning
should_iterate_in_reverse_given_reverse_query_when_scanning
should_limit_results_given_limit_query_when_scanning
should_return_empty_given_empty_db_when_scanning
should_return_next_key_given_seek_to_missing_key_when_scanning
should_return_empty_given_seek_past_end_when_scanning
should_return_empty_given_invalid_range_when_start_greater_than_end
should_skip_deleted_keys_given_tombstones_when_scanning
should_respect_range_tombstones_given_delete_range_when_scanning
should_return_latest_value_given_interleaved_puts_deletes_when_scanning
should_match_regular_scan_given_streaming_scan_when_comparing
should_respect_limit_given_streaming_scan_when_limited
should_apply_tombstones_given_streaming_scan_when_keys_deleted
should_handle_large_scan_given_many_keys_when_iterating
should_handle_large_streaming_scan_given_multiple_ssts_when_spanning
should_handle_concurrent_streaming_scans_when_multiple_threads  ← rewritten to actor semantics
should_produce_identical_results_given_repeated_scans_when_rewinding
```

### **engine_snapshots.rs**

```
should_hide_writes_given_snapshot_created_before_write_when_get_at
should_return_none_given_snapshot_before_key_exists_when_get_at
should_see_value_given_snapshot_after_write_when_get_at
should_see_deleted_key_given_snapshot_before_delete_when_get_at
should_hide_newer_writes_given_snapshot_when_scan_at
should_exclude_keys_written_after_snapshot_when_scan_at
should_include_deleted_keys_given_snapshot_before_delete_when_scan_at
should_maintain_separate_views_given_multiple_snapshots_when_reading
should_work_correctly_given_empty_database_when_snapshot_created
should_not_block_writes_given_snapshot_held_when_writing
should_allow_writes_given_snapshot_dropped_when_continuing
should_recover_data_given_crash_with_active_snapshot_when_reopening
should_preserve_snapshot_view_given_flush_when_reading_at_snapshot
should_preserve_snapshot_view_given_compaction_when_reading_at_snapshot
should_preserve_deleted_range_given_snapshot_before_delete_range_when_scan_at
```

### **engine_merge.rs**

```
should_merge_without_base_value_given_no_existing_key_when_merging
should_merge_with_existing_base_value_given_put_when_merging
should_apply_multiple_merges_sequentially_given_repeated_operations_when_reading
should_merge_after_delete_given_tombstone_when_treating_as_missing
should_handle_merge_with_put_interleaved_given_mixed_ops_when_reading
should_use_string_append_operator_given_delimiter_when_merging
should_string_append_with_base_value_given_initial_put_when_merging
should_handle_empty_merge_operand_given_empty_bytes_when_appending
should_isolate_merge_operators_across_cfs_given_different_operators_when_merging
should_handle_default_cf_merge_independently_given_custom_cf_when_merging
should_preserve_merge_semantics_across_restart_given_flush_when_recovering
should_persist_merge_resolutions_given_cf_restart_when_reopening
should_error_when_merging_without_registered_operator_when_merging
should_surface_error_given_failing_merge_operator_when_getting
should_keep_data_readable_given_merge_operator_changed_across_restart_when_reopening
should_not_lose_merge_operands_under_concurrency_given_same_key_when_merging   ← rewritten for actor model
should_handle_concurrent_merges_to_same_key_given_integer_add_operator_when_merging  ← rewritten
should_handle_merge_with_binary_data_given_binary_key_when_merging
should_not_merge_across_delete_range_given_range_tombstone_when_merging
```

### **engine_ttl.rs**

```
should_return_value_given_ttl_not_elapsed_when_reading
should_return_none_given_ttl_elapsed_when_reading
should_expire_key_given_zero_ttl_means_no_expiration_when_reading
should_persist_ttl_metadata_given_restart_when_reopening
should_expire_after_restart_given_ttl_elapsed_during_shutdown_when_reopening
should_remove_expired_entries_given_compaction_when_ttl_exceeded
should_preserve_non_expired_entries_given_compaction_when_ttl_not_exceeded
should_hide_expired_key_given_snapshot_after_expiry_when_reading_at_snapshot
should_show_key_given_snapshot_before_expiry_when_reading_at_snapshot
should_apply_ttl_given_write_batch_with_ttl_when_committed
should_handle_mixed_ttl_keys_given_some_expire_when_reading
should_update_ttl_given_overwrite_with_new_ttl_when_writing
```

---

# ✅ **TRANSACTION TESTS**

> **Transaction Classification Rule:**
> - **Logical behavior (semantics, isolation, conflicts)** → ALL modes (Memory, FS, Cloud)
> - **Persistence/recovery/restart** → FS + Cloud only
> - **Spill files** → FS + Cloud only
> - **No spill files** → Memory only

### **transaction_basic.rs**

**Runs on ALL modes (with exceptions noted):**

```
should_commit_transaction_given_multiple_operations_when_committed            [ALL]
should_succeed_given_empty_transaction_when_committed                         [ALL]
should_succeed_given_read_only_transaction_when_committed                     [ALL]
should_rollback_transaction_given_uncommitted_when_dropped                    [ALL]
should_rollback_all_writes_given_multiple_operations_when_dropped             [ALL]
should_release_locks_given_aborted_transaction_when_cleanup                   [ALL]
should_provide_snapshot_isolation_given_concurrent_writes_when_transaction_active [ALL]
should_read_own_writes_given_transaction_when_reading                         [ALL]
should_insert_value_given_nonexistent_key_when_insert_in_transaction          [ALL]
should_delete_range_given_committed_transaction_when_delete_range             [ALL]
should_hide_deleted_range_given_transaction_scan_when_delete_range            [ALL]
should_see_uncommitted_writes_given_transaction_scan_when_scanning            [ALL]
should_allow_operations_given_previous_commit_failed_when_disk_full           [ALL]
should_persist_transaction_given_commit_when_crash_after                      [FS, CLOUD]
should_not_persist_transaction_given_abort_when_crash_after                   [FS, CLOUD]
should_recover_committed_transactions_given_wal_replay_when_restart           [FS, CLOUD]
```

**Reason:** Tests validate transaction *rules* logically (commit, rollback, isolation) across all modes. Recovery tests require durable persistence.

---

### **transaction_conflicts.rs**

**Runs on ALL modes (with exceptions noted):**

```
should_allow_concurrent_puts_to_same_key_given_lww_semantics                  [ALL]
should_allow_both_puts_to_succeed_given_concurrent_writes_when_lww            [ALL]
should_accept_both_committers_given_concurrent_puts_when_lww                  [ALL]
should_preserve_first_commit_given_write_conflict_when_second_aborts          [ALL]
should_allow_concurrent_delete_put_operations_given_lww_semantics             [ALL]
should_allow_overlapping_put_after_delete_range_given_lww_semantics           [ALL]
should_allow_put_then_delete_range_given_lww_semantics                        [ALL]
should_allow_concurrent_delete_ranges_given_lww_semantics                     [ALL]
should_allow_delete_range_delete_operations_given_lww_semantics               [ALL]
should_conflict_on_concurrent_inserts_given_same_key_when_one_commits_first   [ALL]
should_conflict_on_insert_given_key_already_exists_when_committed             [ALL]
should_allow_lost_update_given_put_read_modify_write_when_concurrent          [ALL]
should_detect_lost_update_given_cas_pattern_when_value_changed                [ALL]
should_preserve_both_updates_given_non_overlapping_keys_when_concurrent_commits [ALL]
should_commit_transaction_given_no_conflicts                                  [ALL]
should_commit_transaction_given_concurrent_modifications_to_different_keys     [ALL]
should_read_values_within_transaction                                         [ALL]
should_commit_new_key_given_clean_transaction                                 [ALL]
should_allow_concurrent_writes_to_different_keys                              [ALL]
should_handle_high_contention_writes_without_panic                            [ALL]
should_handle_concurrent_read_modify_writes_without_panic                     [ALL]
should_handle_high_concurrency_optimistic_locking                             [ALL]
should_maintain_transaction_isolation_under_stress                            [ALL]
should_recover_conflict_state_after_engine_restart                            [FS, CLOUD]
should_persist_lost_update_prevention_after_restart                           [FS, CLOUD]
```

**Reason:** LWW semantics and conflict detection are in-memory guarantees. Recovery tests require durable state.

---

### **transaction_isolation.rs**

**Runs on ALL modes (with exceptions noted):**

```
should_prevent_dirty_read_given_uncommitted_write_when_reading                [ALL]
should_not_see_uncommitted_write_given_concurrent_transaction_when_reading    [ALL]
should_allow_dirty_write_given_uncommitted_update_when_serialized             [ALL]
should_read_uncommitted_value_given_put_in_same_transaction_when_reading      [ALL]
should_see_own_writes_given_transaction_when_reading                          [ALL]
should_read_at_begin_sequence_given_snapshot_when_reading                     [ALL]
should_not_see_concurrent_writes_given_snapshot_isolation_when_reading        [ALL]
should_return_old_value_given_snapshot_before_write_when_reading              [ALL]
should_provide_consistent_view_given_transaction_when_scanning                [ALL]
should_allow_commit_given_read_key_modified_when_concurrent_write             [ALL]
should_allow_put_commit_given_read_key_modified_when_concurrent_write         [ALL]
should_allow_concurrent_puts_given_different_keys_when_multiple_transactions  [ALL]
should_allow_commit_under_read_committed_isolation_when_serializable_not_needed [ALL]
should_prevent_phantom_read_given_range_query_when_concurrent_insert          [ALL]
should_rollback_all_operations_given_transaction_when_aborted                 [ALL]
should_preserve_isolation_across_transaction_lifecycle_when_reading           [ALL]
should_maintain_isolation_under_concurrent_transaction_pressure_when_stressed [ALL]
should_handle_high_concurrency_readers_given_many_transactions_when_active    [ALL]
should_maintain_consistency_with_mixed_reader_writer_load_when_concurrent     [ALL]
should_recover_snapshot_view_after_engine_restart                             [FS, CLOUD]
```

**Reason:** Isolation is a logical guarantee (no dirty reads, phantom reads, etc.). Recovery requires persistence.

---

### **transaction_advanced.rs**

**Runs on FS + CLOUD ONLY** (WAL durability required):

```
should_persist_atomic_transactions_after_restart                              [FS, CLOUD]
should_not_persist_uncommitted_transaction_after_restart                      [FS, CLOUD]
should_recover_after_abort_given_transaction_with_delete_range_when_restart   [FS, CLOUD]
should_recover_committed_spill_given_restart_after_commit                     [FS, CLOUD]
should_rollback_uncommitted_spill_given_restart_before_commit                 [FS, CLOUD]
should_handle_transaction_abort_idempotency_given_multiple_restart_cycles     [FS, CLOUD]
should_maintain_exactly_once_semantics_given_transaction_with_crash           [FS, CLOUD]
should_recover_large_transaction_given_crash_during_spill                     [FS, CLOUD]
should_not_lose_transaction_writes_given_incomplete_wal_sync                  [FS, CLOUD]
should_survive_mid_spill_crash_given_transaction_recovery                     [FS, CLOUD]
```

**Reason:** These tests require crash recovery, WAL replay, and/or spill file durability. Memory-mode drops all state on shutdown.

---

### **transaction_spill.rs**

**Runs on FS + CLOUD ONLY** (except one):

```
should_commit_large_transaction_given_many_writes_exceeding_memory_limit      [FS, CLOUD]
should_handle_very_large_transaction_given_multiple_spills_when_persisted     [FS, CLOUD]
should_preserve_data_integrity_given_large_transaction_with_specific_values   [FS, CLOUD]
should_preserve_key_order_given_large_transaction_when_iterating              [FS, CLOUD]
should_rollback_spilled_transaction_given_drop_without_commit                 [FS, CLOUD]
should_cleanup_spill_files_given_transaction_rollback_when_finalizing         [FS, CLOUD]
should_rollback_uncommitted_spill_given_restart_before_commit                 [FS, CLOUD]
should_recover_committed_spill_given_restart_after_commit                     [FS, CLOUD]
should_not_starve_foreground_writes_given_background_spill_activity           [FS, CLOUD]
should_handle_concurrent_large_transactions_given_memory_pressure             [FS, CLOUD]
should_handle_transaction_with_tiny_memory_limit_given_forced_spill           [FS, CLOUD]
should_handle_mixed_value_sizes_in_spilled_transaction_when_committed         [FS, CLOUD]
should_not_create_disk_artifacts_given_large_transaction_when_memory_mode     [MEMORY ONLY]
```

**Reason:** Spill tests require on-disk spill files. The memory-mode test verifies that spill files are *not* created under memory-mode.

---

# ✅ **COLUMN FAMILIES**

### **column_families.rs**

```
should_create_column_family_given_valid_name_when_engine_open
should_create_multiple_column_families_given_unique_names_when_engine_open
should_fail_create_column_family_given_duplicate_name_when_cf_exists
should_create_column_family_with_custom_config_given_config_when_creating
should_drop_column_family_given_empty_cf_when_requested
should_drop_column_family_given_flushed_data_when_requested
should_fail_drop_column_family_given_unflushed_data_when_memtable_not_empty
should_fail_drop_default_column_family_given_drop_request_when_default_cf
should_invalidate_handle_given_cf_dropped_when_accessing
should_delete_cf_data_given_cf_dropped_when_persisted
should_allow_recreate_cf_with_same_name_given_cf_dropped_when_creating
should_list_default_cf_only_given_no_custom_cfs_when_listing
should_list_all_column_families_given_multiple_cfs_when_listing
should_not_list_dropped_cf_given_cf_dropped_when_listing
should_isolate_keys_given_same_key_in_different_cfs_when_reading
should_isolate_deletes_given_delete_in_one_cf_when_other_cf_has_same_key
should_isolate_data_given_different_data_volumes_when_reading
should_isolate_compaction_given_per_cf_data_when_compacting
should_persist_cf_metadata_given_restart_when_cf_created
should_persist_cf_data_given_restart_when_data_flushed
should_persist_multiple_cfs_given_restart_when_all_flushed
should_persist_cf_drop_given_restart_when_cf_was_dropped
should_get_column_family_by_name_given_existing_cf_when_querying
should_fail_get_column_family_given_nonexistent_name_when_querying
should_get_default_column_family_given_fresh_engine_when_querying
should_isolate_cf_after_flush_given_same_key_when_reading
should_handle_operations_on_default_cf_given_custom_cfs_exist_when_operating
should_maintain_cf_isolation_given_many_cfs_when_operating
```

---

# ✅ **CONFIG**

### **config_api.rs**

```
should_build_config_given_minimal_defaults_when_only_path_provided
should_set_goal_given_latency_when_optimizing_for_p99
should_set_goal_given_throughput_when_optimizing_for_bulk_writes
should_set_goal_given_cost_when_minimizing_resources
should_set_durability_given_strict_when_fsync_per_write_required
should_set_durability_given_steady_when_balanced_sync_needed
should_respect_memory_budget_given_explicit_bytes_when_configured
should_use_auto_memory_given_no_explicit_budget_when_default
should_optimize_params_given_write_heavy_profile_when_configured
should_optimize_params_given_read_mostly_profile_when_configured
should_optimize_params_given_range_scan_profile_when_configured
should_require_cloud_config_given_cloud_mode_when_not_off
should_allow_cloud_off_given_no_cloud_config_when_local_only
should_enable_autotune_given_flag_set_when_requested
should_disable_autotune_given_default_when_not_requested
should_convert_to_options_given_config_when_bridging_to_engine
should_derive_consistent_params_given_all_knobs_set_when_building
should_derive_different_params_given_latency_vs_throughput_when_comparing
should_store_path_given_relative_path_when_building
should_store_path_given_absolute_path_when_building
```

---

# ✅ **DURABILITY + RECOVERY**

### **durability_wal.rs**

```
should_recover_writes_given_unflushed_memtable_when_reopening
should_persist_write_given_fsync_enabled_when_crash_occurs
should_call_fsync_given_wal_sync_enabled_when_put
should_rotate_wal_given_small_buffer_when_writes_exceed_buffer
should_replay_all_records_given_multiple_wal_segments_when_recovering
should_recover_all_writes_given_concurrent_puts_when_crash_occurs
should_handle_gracefully_given_truncated_wal_tail_when_recovering
should_not_recover_data_given_truncated_wal_append_when_reopening
should_allow_data_loss_given_skipped_fsync_when_crash_occurs
should_tolerate_corrupted_tail_given_recovery_mode_set_when_reopening
```

### **durability_recovery.rs**

```
should_recover_from_clean_shutdown_when_reopening
should_recover_from_crash_after_flush_when_reopening
should_recover_unflushed_data_given_crash_during_flush_when_reopening
should_prefer_wal_given_wal_newer_than_sst_when_recovering
should_skip_wal_entries_given_already_in_sst_when_recovering
should_replay_wal_in_order_given_multiple_writes_when_recovering
should_recover_deletes_given_crash_after_delete_when_reopening
should_recover_write_batch_atomically_given_crash_when_reopening
should_recover_from_wal_given_manifest_save_failure_when_reopening
should_preserve_consistency_given_crash_before_manifest_update_when_reopening
should_be_idempotent_given_multiple_recovery_cycles_when_reopening
should_maintain_exactly_once_given_multiple_crash_cycles_when_reopening
should_continue_sequence_numbers_given_recovery_when_new_writes
should_skip_corrupted_tail_given_partial_record_when_tolerant_mode
```

### **durability_atomicity.rs**

```
should_not_expose_sst_without_manifest_entry_given_orphan_file_when_recovering
should_replay_wal_until_manifest_sequence_given_manifest_fsynced_when_recovering
should_preserve_manifest_authority_given_wal_newer_when_sst_missing
should_not_auto_claim_orphan_sst_given_sst_exists_when_manifest_behind
should_not_publish_sst_given_manifest_not_persisted_when_adding_sst
should_maintain_atomicity_given_concurrent_flush_manifest_fsync_when_updating
should_maintain_order_given_multiple_cfs_flush_concurrently_when_updating_manifest
should_commit_ssts_manifest_together_given_compaction_success_when_completing
should_cleanup_partial_output_given_compaction_failure_when_recovering
should_delete_old_ssts_only_after_manifest_persisted_when_compacting
should_not_recover_truncated_wal_append_given_truncate_fallback_when_reopening
```

---

# ✅ **SST LAYER**

### **sst_reader.rs**

```
should_verify_checksums_on_read_given_paranoid_mode_enabled_when_reading
should_limit_read_amplification_given_bloom_filters_when_point_reading
should_hit_cache_for_frequently_accessed_keys_given_working_set_fits_when_reading
should_maintain_efficiency_given_concurrent_range_scans_when_accessing  ← actor-safe
should_balance_cache_given_concurrent_readers_when_working_sets_overlap ← actor-safe
should_rehydrate_partial_cloud_object_given_short_file_when_reading
should_fail_fast_given_corrupted_cloud_sst_index_block_when_reading_data
```

### **sst_writer.rs**

```
should_roundtrip_data_given_no_compression_when_using_noop_codec
should_roundtrip_data_given_lz4_compression_when_compressing_text
should_roundtrip_data_given_zstd_level1_when_compressing_text
should_roundtrip_data_given_zstd_level3_when_compressing_text
should_roundtrip_data_given_zstd_level5_when_compressing_text
should_achieve_better_ratio_given_higher_zstd_level_when_comparing_levels
should_handle_large_data_given_multi_mb_input_when_using_lz4
should_handle_random_data_given_incompressible_input_when_using_lz4
should_persist_compression_setting_given_reopen_when_using_same_options
should_handle_all_zeros_given_maximally_compressible_data_when_using_lz4
should_handle_all_0xff_given_uniform_data_when_compressing
should_write_per_block_blooms_to_sst_during_finalization
should_support_per_block_blooms_in_meta_index
should_include_per_block_bloom_offsets_in_index
```

### **sst_index_table.rs**

```
should_create_index_table_from_block_metas
should_find_block_with_exact_min_key
should_find_block_with_key_within_range
should_find_block_for_key_between_blocks
should_find_last_block_for_key_within_last_block
should_return_none_for_empty_table_find_block
should_find_blocks_in_range
should_return_empty_for_invalid_range
should_return_empty_for_reverse_range
should_iterate_all_blocks
should_access_block_by_index
should_return_none_for_out_of_bounds_index
should_calculate_memory_usage
should_get_all_blocks_slice
should_preserve_block_metadata_through_index_table
should_handle_adjacent_key_ranges
should_find_block_with_single_key_range
should_find_blocks_in_range_with_exact_boundaries
should_correctly_handle_large_key_spaces
should_maintain_order_invariant
```

### **sst_tombstone_index.rs**

```
should_create_empty_tombstone_index_when_no_tombstones
should_build_tombstone_index_from_single_block
should_build_tombstone_index_from_multiple_blocks
should_find_tombstone_blocks_for_key_in_range
should_return_no_blocks_when_key_not_in_any_range
should_find_blocks_intersecting_range
should_return_no_blocks_when_range_disjoint
should_detect_potential_deletion_when_key_in_range
should_detect_no_deletion_when_key_outside_range
should_handle_overlapping_tombstones_in_same_block
should_handle_tombstones_with_identical_ranges
should_skip_empty_tombstone_blocks
should_handle_tombstones_with_empty_start_key
should_handle_tombstones_spanning_entire_keyspace
should_maintain_block_order_for_sequential_ranges
should_check_entry_coverage_with_boundary_keys
should_check_entry_range_intersection_with_boundaries
should_handle_large_number_of_tombstone_blocks
should_find_multiple_overlapping_blocks_for_key
should_iterate_through_all_entries
```

### **sst_trie.rs**

```
should_roundtrip_simple_trie
should_handle_hierarchical_keys
should_handle_shared_prefixes
should_handle_large_trie
should_handle_empty_trie
should_handle_single_char_keys
```

### **sst_fence_pointers.rs**

```
should_track_fence_pointers_in_block_meta
should_track_tombstone_ranges_in_block_meta
should_detect_block_fully_covered_by_tombstones
should_detect_partial_tombstone_coverage
should_check_key_containment_in_block
should_check_range_intersection
should_use_fence_pointers_to_skip_blocks_in_range_scan
should_skip_blocks_fully_covered_by_tombstones_in_compaction
should_use_fence_pointers_in_iterator_next_block
should_track_multiple_tombstone_ranges
should_handle_block_meta_without_tombstones
should_maintain_fence_pointer_ordering
```

### **sst_block_cache.rs**

```
should_cache_block_given_basic_cache_when_inserting
should_return_none_given_nonexistent_key_when_getting
should_distinguish_block_types_given_same_file_and_offset_when_caching
should_track_stats_given_cache_operations_when_querying_stats
should_cache_block_given_sharded_cache_when_inserting
should_distribute_entries_given_many_keys_when_using_sharded_cache
should_handle_concurrent_access_given_multiple_threads_when_using_sharded_cache  ← actor-safe
should_evict_entries_given_capacity_exceeded_when_inserting
should_update_lru_order_given_recent_access_when_getting
should_distinguish_keys_given_different_files_when_same_offset
should_distinguish_keys_given_different_offsets_when_same_file
should_respect_size_limit_given_large_blocks_when_inserting
```

### **sst_per_block_bloom.rs**

```
should_create_block_bloom_with_capacity
should_add_keys_to_block_bloom
should_not_have_false_negatives
should_encode_block_bloom_to_bytes
should_decode_block_bloom_from_bytes
should_store_block_bloom_offset_in_block_meta
should_query_block_bloom_from_block_meta
should_create_block_index_entry_with_bloom_offset
should_create_block_index_entry_without_bloom
should_detect_per_block_bloom_format
should_read_old_sst_format_without_per_block_blooms
should_maintain_acceptable_false_positive_rate
should_add_batch_of_keys_to_bloom
should_check_multiple_keys_efficiently
should_handle_empty_block_bloom
should_handle_large_keys_in_bloom
should_handle_small_bloom_size
should_survive_encode_decode_round_trip
should_encode_bloom_efficiently
```

---

# ✅ **STREAMING / READ OPTIMIZATION**

### **streaming_bloom.rs**

```
should_create_bloom_filter_with_8_bits_per_key
should_create_bloom_filter_with_12_bits_per_key
should_show_improved_fpr_with_higher_bits_per_key
should_maintain_no_false_negatives_with_increased_bits_per_key
should_have_lower_false_positive_rate_for_negative_lookups_at_12_bits_per_key
should_handle_wide_range_negative_lookups_efficiently
should_construct_fast_negative_filter_for_sst_blocks
should_encode_decode_fast_negative_filter
should_use_fast_negative_filter_in_negative_lookup_path
should_handle_index_table_without_fast_negative_filter
should_fit_fast_negative_filter_in_l1_cache
should_efficiently_skip_empty_blocks_with_fast_filter
should_support_maximum_256_blocks_per_sst
should_identify_blocks_via_fast_negative_filter
should_measure_negative_lookup_improvement
should_support_bloom_filter_roundtrip_with_configurable_bits_per_key
```

### **streaming_fence_pointer.rs**

```
should_skip_block_entirely_before_range_start
should_skip_block_entirely_after_range_end
should_not_skip_block_that_partially_overlaps_range
should_handle_range_exactly_at_block_boundaries
should_skip_blocks_in_sequential_read
should_measure_block_skip_ratio_for_narrow_range
should_measure_block_skip_ratio_for_wide_range
should_skip_all_blocks_for_range_before_all_blocks
should_skip_all_blocks_for_range_after_all_blocks
should_not_lose_keys_at_range_boundaries
should_include_block_containing_only_range_start_key
should_handle_single_key_range
should_handle_streaming_window_scan_pattern
should_handle_overlapping_queries
should_correctly_order_results_when_using_fence_pointers
```

### **streaming_sequential.rs**

```
should_create_optimizer_with_clean_state
should_predict_sequential_blocks
should_break_prediction_on_non_sequential_access
should_cache_frequently_accessed_blocks
should_predict_mixed_sequential_patterns
should_handle_range_scan_pattern
should_handle_repeated_lookups
should_reset_metrics_but_keep_predictor
should_handle_empty_cache_lookups
should_report_metrics_accurately
should_handle_large_block_indices
should_handle_non_sequential_forward_jumps
should_have_consistent_efficiency_ratio
should_optimize_repeated_range_scan
```

## Run against storage modes (with transaction-aware matrix)

| File                    | Memory | FS | Cloud | Notes |
| ----------------------- | ------ | -- | ----- | ----- |
| engine_basic            | ✔️     | ✔️ | ✔️    | |
| engine_write_batch      | ✔️     | ✔️ | ✔️    | |
| engine_delete_range     | ✔️     | ✔️ | ✔️    | |
| engine_iterators        | ✔️     | ✔️ | ✔️    | |
| engine_snapshots        | ✔️     | ✔️ | ✔️    | |
| engine_merge            | ✔️     | ✔️ | ✔️    | |
| engine_ttl              | ✔️     | ✔️ | ✔️    | |
| column_families         | ✔️     | ✔️ | ✔️    | |
| config_api              | ✔️     | ✔️ | ✔️    | |
| durability_atomicity    | ✔️*    | ✔️ | ✔️    | *Some tests FS+Cloud only |
| durability_recovery     | ⚠️*    | ✔️ | ✔️    | *Most FS+Cloud only |
| durability_wal          | ⚠️*    | ✔️ | ✔️    | *Most FS+Cloud only |
| sst_reader              | ✔️     | ✔️ | ✔️    | |
| sst_writer              | ✔️     | ✔️ | ✔️    | |
| sst_index_table         | ✔️     | ✔️ | ✔️    | |
| sst_tombstone_index     | ✔️     | ✔️ | ✔️    | |
| sst_trie                | ✔️     | ✔️ | ✔️    | |
| sst_fence_pointers      | ✔️     | ✔️ | ✔️    | |
| sst_block_cache         | ✔️     | ✔️ | ✔️    | |
| sst_per_block_bloom     | ✔️     | ✔️ | ✔️    | |
| streaming_bloom         | ✔️     | ✔️ | ✔️    | Phase 5+ |
| streaming_fence_pointer | ✔️     | ✔️ | ✔️    | Phase 5+ |
| streaming_sequential    | ✔️     | ✔️ | ✔️    | Phase 5+ |
| transaction_basic       | ✔️**   | ✔️ | ✔️    | **Except crash/restart tests (FS+Cloud) |
| transaction_conflicts   | ✔️**   | ✔️ | ✔️    | **Except restart tests (FS+Cloud) |
| transaction_isolation   | ✔️**   | ✔️ | ✔️    | **Except one restart test (FS+Cloud) |
| transaction_advanced    | ❌     | ✔️ | ✔️    | Requires WAL durability |
| transaction_spill       | ⚠️     | ✔️ | ✔️    | ⚠️ One test MEMORY ONLY |
