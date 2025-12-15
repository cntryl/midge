# Test & Bench Inventory

Complete inventory of all test and benchmark functions across midge.

**Src Tests**

- `src/lib.rs`
  - tests: (none)

- `src/common/error.rs`
  - tests: (none)

- `src/common/singleflight.rs`
  - tests:
    - `should_fan_out_one_flush_result_to_many_waiters`
    - `should_flush_only_when_policy_triggers`
    - `should_run_flush_once_for_many_submitters`
    - `should_group_waiters_by_key_and_drain_on_complete`

- `src/compaction/mod.rs`
  - tests:
    - `should_format_output_filename_with_zero_padded_sequence`
    - `should_preserve_cf_directory_path_when_generating_filename`
    - `should_handle_large_sequence_numbers_when_formatting`
    - `should_produce_lexicographically_sortable_filenames`
    - `should_use_sst_extension_when_generating_filename`
    - `should_handle_zero_sequence_number_when_formatting`
    - `should_create_compaction_plan_with_constructor`
    - `should_set_output_sequence_when_using_with_output_seq`
    - `should_allow_chaining_builder_methods_when_using_with_output_seq`
    - `should_calculate_l0_target_size_correctly`
    - `should_calculate_level_multiplier_correctly`
    - `should_create_leveled_compaction_config_with_default_values`
    - `should_initialize_empty_file_vectors_when_creating_plan`
    - `should_preserve_level_information_in_plan`
    - `should_handle_maximum_column_family_id`

- `src/compaction/strategy.rs`
  - tests: (many — compaction planning, thresholds, stability, deduplication)

- `src/compaction/planner.rs`
  - tests: (planner creation, task ids, serialization and roundtrips)

- `src/compaction/merge.rs`
  - tests: (merge iterator behavior — ordering, duplicates, bounds)

- `src/compaction/executor.rs`
  - tests:
    - `should_keep_highest_sequence_when_deduplicating_versions`
    - `should_remove_tombstones_when_filtering_versions`
    - `should_skip_expired_entries_when_deduplicating_with_ttl`
    - `should_deduplicate_multiple_keys_independently`
    - `should_detect_expired_version_when_past_expiration`
    - `should_not_expire_version_when_future_or_none`
    - `should_stream_deduplicate_multiple_versions_when_using_iterator`
    - `should_handle_empty_streams_in_merge`
    - `should_deduplicate_correctly_across_streams_with_overlapping_keys`

- `src/engine/mod.rs`
  - tests: (many small API/handle/ID tests)

- `src/engine/context.rs`
  - tests: (none)

- `src/engine/open.rs`
  - tests: (none)

- `src/engine/api/write_options.rs`
  - tests: (builder and clone tests)

- `src/engine/api/write_batch.rs`
  - tests: (write-batch behavior)

- `src/engine/api/transaction.rs`
  - tests: (transaction lifecycle and operations)

- `src/engine/api/snapshot.rs`
  - tests: (snapshot construction & equality)

- `src/engine/api/query.rs`
  - tests: (query builder, bounds, chaining)

- `src/iterators/skiplist.rs`
  - tests: (skiplist insert/get/scan/tombstone visibility and concurrency)

- `src/iterators/merge.rs`
  - tests: (merge iterator tests — ordering, bounds, duplicates)

- `src/metadata/version_set.rs`
  - tests: (version creation, indexing, lookups)

- `src/metadata/version_manager.rs`
  - tests: (edit application, atomicity, roundtrips)

- `src/metadata/manifest.rs`
  - tests: (manifest create/add/remove, next ids)

- `src/metadata/persistence.rs`
  - tests: (manifest persistence and file operations)

- `src/runtime/mod.rs`
  - tests: (runtime ids, request routing, router behavior)

- `src/runtime/task.rs`
  - tests: (task id generation, priorities, cloning)

- `src/runtime/state.rs`
  - tests: (runtime state invariants, wal/frontier, compaction tracking)

- `src/runtime/scheduler.rs`
  - tests: (scheduling, priority ordering, concurrency limits)

- `src/runtime/event_loop.rs`
  - tests: (actor initialization, routing, runtime creation/shutdown)

- `src/runtime/dispatch.rs`
  - tests: (message routing to actors)

- `src/runtime/actors/*.rs`
  - tests: (actor unit tests across manifest, gc, flush, compaction, cloud, eviction)

- `src/sst/*`
  - tests: (encoding, footer, trie, sparse index, traits, readers/writers — many)

- `src/storage/*`
  - tests: (providers and hybrid/filesystem/cloud behaviors)

- `src/wal/*`
  - tests: (encoding, fs reader/writer, recovery, factory — many)

- `src/telemetry/*`
  - tests: (spans, metrics, config)

- `src/metrics/*`
  - tests: (metrics collection invariants)

**Integration Tests (tests/)**

- `tests/transaction_basic.rs`
  - tests:
    - `should_commit_transaction_given_multiple_operations_when_committed`
    - `should_succeed_given_empty_transaction_when_committed`
    - `should_succeed_given_read_only_transaction_when_committed`
    - `should_rollback_transaction_given_uncommitted_when_dropped`
    - `should_rollback_all_writes_given_multiple_operations_when_dropped`
    - `should_release_locks_given_aborted_transaction_when_cleanup`
    - `should_provide_snapshot_isolation_given_concurrent_writes_when_transaction_active`
    - `should_read_own_writes_given_transaction_when_reading`
    - `should_insert_value_given_nonexistent_key_when_insert_in_transaction`
    - `should_delete_range_given_committed_transaction_when_delete_range`
    - `should_hide_deleted_range_given_transaction_scan_when_delete_range`
    - `should_see_uncommitted_writes_given_transaction_scan_when_scanning`
    - `should_allow_operations_given_previous_commit_failed_when_disk_full`
    - `should_persist_transaction_given_commit_when_crash_after`
    - `should_not_persist_transaction_given_abort_when_crash_after`
    - `should_recover_committed_transactions_given_wal_replay_when_restart`

- `tests/transaction_advanced.rs`
  - tests:
    - `should_persist_atomic_transactions_after_restart`
    - `should_not_persist_uncommitted_transaction_after_restart`
    - `should_recover_after_abort_given_transaction_with_delete_range_when_restart`
    - `should_recover_committed_spill_given_restart_after_commit`
    - `should_rollback_uncommitted_spill_given_restart_before_commit`
    - `should_handle_transaction_abort_idempotency_given_multiple_restart_cycles`
    - `should_maintain_exactly_once_semantics_given_transaction_with_crash`
    - `should_recover_large_transaction_given_crash_during_spill`
    - `should_not_lose_transaction_writes_given_incomplete_wal_sync`
    - `should_survive_mid_spill_crash_given_transaction_recovery`

- `tests/transaction_isolation.rs`
  - tests:
    - `should_prevent_dirty_read_given_uncommitted_write_when_reading`
    - `should_not_see_uncommitted_write_given_concurrent_transaction_when_reading`
    - `should_allow_dirty_write_given_uncommitted_update_when_serialized`
    - `should_read_uncommitted_value_given_put_in_same_transaction_when_reading`
    - `should_see_own_writes_given_transaction_when_reading`
    - `should_read_at_begin_sequence_given_snapshot_when_reading`
    - `should_not_see_concurrent_writes_given_snapshot_isolation_when_reading`
    - `should_return_old_value_given_snapshot_before_write_when_reading`
    - `should_provide_consistent_view_given_transaction_when_scanning`
    - `should_allow_commit_given_read_key_modified_when_concurrent_write`
    - `should_allow_put_commit_given_read_key_modified_when_concurrent_write`
    - `should_allow_concurrent_puts_given_different_keys_when_multiple_transactions`
    - `should_allow_commit_under_read_committed_isolation_when_serializable_not_needed`
    - `should_prevent_phantom_read_given_range_query_when_concurrent_insert`
    - `should_rollback_all_operations_given_transaction_when_aborted`
    - `should_preserve_isolation_across_transaction_lifecycle_when_reading`
    - `should_maintain_isolation_under_concurrent_transaction_pressure_when_stressed`
    - `should_handle_high_concurrency_readers_given_many_transactions_when_active`
    - `should_maintain_consistency_with_mixed_reader_writer_load_when_concurrent`
    - `should_recover_snapshot_view_after_engine_restart`

- `tests/transaction_conflicts.rs`
  - tests:
    - `should_allow_concurrent_puts_to_same_key_given_lww_semantics`
    - `should_allow_both_puts_to_succeed_given_concurrent_writes_when_lww`
    - `should_accept_both_committers_given_concurrent_puts_when_lww`
    - `should_preserve_first_commit_given_write_conflict_when_second_aborts`
    - `should_allow_concurrent_delete_put_operations_given_lww_semantics`
    - `should_allow_overlapping_put_after_delete_range_given_lww_semantics`
    - `should_allow_put_then_delete_range_given_lww_semantics`
    - `should_allow_concurrent_delete_ranges_given_lww_semantics`
    - `should_allow_delete_range_delete_operations_given_lww_semantics`
    - `should_conflict_on_concurrent_inserts_given_same_key_when_one_commits_first`
    - `should_conflict_on_insert_given_key_already_exists_when_committed`
    - `should_allow_lost_update_given_put_read_modify_write_when_concurrent`
    - `should_detect_lost_update_given_cas_pattern_when_value_changed`
    - `should_preserve_both_updates_given_non_overlapping_keys_when_concurrent_commits`
    - `should_commit_transaction_given_no_conflicts`
    - `should_commit_transaction_given_concurrent_modifications_to_different_keys`
    - `should_read_values_within_transaction`
    - `should_commit_new_key_given_clean_transaction`
    - `should_allow_concurrent_writes_to_different_keys`
    - `should_handle_high_contention_writes_without_panic`
    - `should_handle_concurrent_read_modify_writes_without_panic`
    - `should_handle_high_concurrency_optimistic_locking`
    - `should_maintain_transaction_isolation_under_stress`
    - `should_recover_conflict_state_after_engine_restart`
    - `should_persist_lost_update_prevention_after_restart`

- `tests/transaction_spill.rs`
  - tests:
    - `should_commit_large_transaction_given_many_writes_exceeding_memory_limit`
    - `should_handle_very_large_transaction_given_multiple_spills_when_persisted`
    - `should_preserve_data_integrity_given_large_transaction_with_specific_values`
    - `should_preserve_key_order_given_large_transaction_when_iterating`
    - `should_rollback_spilled_transaction_given_drop_without_commit`
    - `should_cleanup_spill_files_given_transaction_rollback_when_finalizing`
    - `should_rollback_uncommitted_spill_given_restart_before_commit`
    - `should_recover_committed_spill_given_restart_after_commit`
    - `should_not_starve_foreground_writes_given_background_spill_activity`
    - `should_handle_concurrent_large_transactions_given_memory_pressure`
    - `should_handle_transaction_with_tiny_memory_limit_given_forced_spill`
    - `should_handle_mixed_value_sizes_in_spilled_transaction_when_committed`
    - `should_not_create_disk_artifacts_given_large_transaction_when_memory_mode`

- `tests/engine_write_batch.rs`
  - tests:
    - `should_commit_all_operations_given_batch_when_write_batch`
    - `should_apply_last_value_given_duplicate_keys_when_write_batch`
    - `should_succeed_given_empty_batch_when_write_batch`
    - `should_delete_key_given_delete_after_put_when_write_batch`
    - `should_delete_existing_key_given_delete_in_batch_when_write_batch`
    - `should_overwrite_existing_value_given_put_in_batch_when_write_batch`
    - `should_apply_mixed_operations_in_order_when_write_batch`
    - `should_handle_large_batch_given_many_operations_when_write_batch`
    - `should_write_to_multiple_cfs_given_multi_cf_batch_when_write_batch`
    - `should_isolate_keys_given_same_key_in_different_cfs_when_write_batch`
    - `should_not_interleave_given_concurrent_batches_when_write_batch`
    - `should_maintain_atomicity_during_concurrent_reads_when_write_batch`
    - `should_persist_batch_given_flush_when_reopening`
    - `should_be_atomic_given_crash_during_wal_write_when_recovering`
    - `should_be_atomic_given_large_batch_crash_when_recovering`
    - `should_support_batch_with_ttl_when_write_batch`
    - `should_increment_sequence_numbers_given_batch_operations_when_write_batch`

- `tests/engine_ttl.rs`
  - tests:
    - `should_return_value_given_ttl_not_elapsed_when_reading`
    - `should_return_none_given_ttl_elapsed_when_reading`
    - `should_not_expire_key_given_zero_ttl_means_no_expiration_when_reading`
    - `should_persist_ttl_metadata_given_restart_when_reopening`
    - `should_expire_after_restart_given_ttl_elapsed_during_shutdown_when_reopening`
    - `should_remove_expired_entries_given_compaction_when_ttl_exceeded`
    - `should_preserve_non_expired_entries_given_compaction_when_ttl_not_exceeded`
    - `should_hide_expired_key_given_snapshot_after_expiry_when_reading_at_snapshot`
    - `should_check_expiration_at_read_time_given_snapshot_when_ttl_elapses_after_snapshot`
    - `should_apply_ttl_given_write_batch_with_ttl_when_committed`
    - `should_handle_mixed_ttl_keys_given_some_expire_when_reading`
    - `should_update_ttl_given_overwrite_with_new_ttl_when_writing`

- `tests/engine_snapshots.rs`
  - tests:
    - `should_hide_writes_given_snapshot_created_before_write_when_get_at`
    - `should_return_none_given_snapshot_before_key_exists_when_get_at`
    - `should_see_value_given_snapshot_after_write_when_get_at`
    - `should_see_deleted_key_given_snapshot_before_delete_when_get_at`
    - `should_hide_newer_writes_given_snapshot_when_scan_at`
    - `should_exclude_keys_written_after_snapshot_when_scan_at`
    - `should_include_deleted_keys_given_snapshot_before_delete_when_scan_at`
    - `should_maintain_separate_views_given_multiple_snapshots_when_reading`
    - `should_work_correctly_given_empty_database_when_snapshot_created`
    - `should_not_block_writes_given_snapshot_held_when_writing`
    - `should_allow_writes_given_snapshot_dropped_when_continuing`
    - `should_preserve_snapshot_view_given_flush_when_reading_at_snapshot`
    - `should_preserve_snapshot_view_given_compaction_when_reading_at_snapshot`
    - `should_preserve_deleted_range_given_snapshot_before_delete_range_when_scan_at`

- `tests/engine_merge.rs`
  - tests:
    - `should_merge_without_base_value_given_no_existing_key_when_merging`
    - `should_merge_with_existing_base_value_given_put_when_merging`
    - `should_apply_multiple_merges_sequentially_given_repeated_operations_when_reading`
    - `should_merge_after_delete_given_tombstone_when_treating_as_missing`
    - `should_handle_merge_with_put_interleaved_given_mixed_ops_when_reading`
    - `should_use_string_append_operator_given_delimiter_when_merging`
    - `should_string_append_with_base_value_given_initial_put_when_merging`
    - `should_handle_empty_merge_operand_given_empty_bytes_when_appending`
    - `should_isolate_merge_operators_across_cfs_given_different_operators_when_merging`
    - `should_handle_default_cf_merge_independently_given_custom_cf_when_merging`
    - `should_preserve_merge_semantics_across_restart_given_flush_when_recovering`
    - `should_persist_merge_resolutions_given_cf_restart_when_reopening`
    - `should_error_when_merging_without_registered_operator_when_merging`
    - `should_surface_error_given_failing_merge_operator_when_getting`
    - `should_keep_data_readable_given_merge_operator_changed_across_restart_when_reopening`
    - `should_not_lose_merge_operands_under_concurrency_given_same_key_when_merging`
    - `should_handle_concurrent_merges_to_same_key_given_integer_add_operator_when_merging`
    - `should_handle_merge_with_binary_data_given_binary_key_when_merging`
    - `should_not_merge_across_delete_range_given_range_tombstone_when_merging`

- `tests/engine_iterators.rs`
  - tests:
    - `should_iterate_all_keys_in_order_given_populated_db_when_scanning`
    - `should_iterate_in_reverse_given_reverse_query_when_scanning`
    - `should_limit_results_given_limit_query_when_scanning`
    - `should_return_empty_given_empty_db_when_scanning`
    - `should_return_next_key_given_seek_to_missing_key_when_scanning`
    - `should_return_empty_given_seek_past_end_when_scanning`
    - `should_return_empty_given_invalid_range_when_start_greater_than_end`
    - `should_skip_deleted_keys_given_tombstones_when_scanning`
    - `should_respect_range_tombstones_given_delete_range_when_scanning`
    - `should_return_latest_value_given_interleaved_puts_deletes_when_scanning`
    - `should_match_regular_scan_given_streaming_scan_when_comparing`
    - `should_respect_limit_given_streaming_scan_when_limited`
    - `should_apply_tombstones_given_streaming_scan_when_keys_deleted`
    - `should_handle_large_scan_given_many_keys_when_iterating`
    - `should_handle_large_streaming_scan_given_multiple_ssts_when_spanning`

- `tests/snapshots_advanced.rs`
  - tests:
    - `should_not_block_compaction_given_held_snapshot_when_compaction_triggered`
    - `should_not_block_flush_given_held_snapshot_when_flush_triggered`
    - `should_handle_many_concurrent_snapshots_given_100_snapshots_when_creating`
    - `should_maintain_isolation_given_concurrent_delete_range_when_snapshot_active`
    - `should_see_consistent_state_given_snapshot_across_write_batch_when_committed`
    - `should_maintain_snapshots_at_different_sequence_numbers_when_multiple`
    - `should_cleanup_resources_given_snapshot_drop_when_no_longer_needed`
    - `should_preserve_snapshot_across_multiple_column_families_when_created`

- `tests/sst_reads_integration.rs`
  - tests:
    - `should_read_from_sst_after_flush`
    - `should_track_l0_sst_reads`
    - `should_use_key_ranges_for_higher_levels`
    - `should_handle_memtable_and_sst_reads`

- `tests/merge_advanced.rs`
  - tests:
    - `should_apply_merge_given_delete_then_merge_when_tombstone_base`
    - `should_delete_after_merge_given_merge_then_delete_when_sequence`
    - `should_handle_merge_on_many_tombstones_given_delete_merge_cycles_when_repeated`
    - `should_apply_multiple_merges_in_batch_given_write_batch_when_committed`
    - `should_accumulate_values_given_10_sequential_merges_when_applying`
    - `should_preserve_merge_with_empty_operand_given_empty_bytes_when_merging`
    - `should_handle_binary_data_in_merge_given_non_utf8_when_merging`
    - `should_handle_special_characters_in_string_merge_given_delimiters_when_appending`
    - `should_accumulate_multiple_merges_on_different_keys_when_batch`

- `tests/memory_mode_isolation.rs`
  - tests:
    - `should_not_create_filesystem_artifacts_when_memory_mode`
    - `should_not_persist_data_across_restart_given_memory_mode_when_reopening`
    - `should_isolate_data_given_multiple_memory_engines_when_separate_instances`
    - `should_handle_many_writes_efficiently_when_writing_100_keys`
    - `should_handle_many_deletes_efficiently_when_deleting_50_keys`
    - `should_handle_mixed_operations_efficiently_when_put_delete_overwrite`

- `tests/read_amp_api.rs`
  - tests:
    - `should_expose_read_amp_metrics_through_api`
    - `should_track_l0_overlap_in_metrics`
    - `should_show_zero_metrics_for_new_engine`
    - `should_accumulate_metrics_over_multiple_reads`
    - `should_report_budget_violations_when_exceeded`

- `tests/read_amp_metrics_integration.rs`
  - tests:
    - `should_track_blocks_read_per_operation`
    - `should_track_bloom_rejections_correctly`
    - `should_calculate_averages_correctly`
    - `should_handle_zero_reads_without_panic`

- `tests/hot_sst_tracking.rs`
  - tests:
    - `should_track_read_counts_per_sst_when_accessed`
    - `should_track_l0_reads_separately`
    - `should_skip_cold_ssts_using_key_ranges`
    - `should_accumulate_reads_over_time`

**Benches (benches/)**

- `benches/tier1_hotpath_api.rs`
  - benches:
    - `bench_batch_put`
    - `bench_single_get`
    - `bench_single_put`

- `benches/tier1_hotpath_block_cache.rs`
  - benches:
    - `bench_get_hot_single`
    - `bench_insert_single`
    - `bench_get_batch_hit`
    - `bench_get_batch_miss`
    - `bench_insert_batch`
    - `bench_eviction`

- `benches/tier1_hotpath_bloom.rs`
  - benches:
    - `bench_bloom_maybe_contains`
    - `bench_bloom_batch_lookups`
    - `bench_bloom_compute_hashes`

- `benches/tier1_hotpath_iterator.rs`
  - benches:
    - `bench_iter_sequential`
    - `bench_range_bounded`
    - `bench_iter_single_step`
    - `bench_range_position`
    - `bench_range_bounds_vs_unbounded`

- `benches/tier1_hotpath_memtable.rs`
  - benches:
    - `bench_put_single`
    - `bench_put_batch`
    - `bench_get_point`
    - `bench_delete`
    - `bench_size_bytes`

- `benches/tier1_hotpath_singleflight.rs`
  - benches:
    - `bench_singleflight_flush_fanout`

- `benches/tier1_hotpath_sparse_index.rs`
  - benches:
    - `bench_sparse_index_find_block`
    - `bench_sparse_index_sizes`

- `benches/tier1_hotpath_sst.rs`
  - benches:
    - `bench_encode`
    - `bench_decode`
    - `bench_roundtrip`

- `benches/tier1_hotpath_trie.rs`
  - benches:
    - `bench_trie_find_block`
    - `bench_trie_prefix_range`
    - `bench_trie_key_patterns`

- `benches/tier1_hotpath_wal.rs`
  - benches:
    - `bench_wal_encode_record`
    - `bench_wal_decode_record`
    - `bench_wal_roundtrip`
    - `bench_wal_encode_sizes`

- `benches/tier2_subsystem_block_cache.rs`
  - benches:
    - `bench_eviction_scan`
    - `bench_fill_then_hit`
    - `bench_hotset_rotation`
    - `bench_lru_eviction_1k`
    - `bench_lru_eviction_10k`

- `benches/tier2_subsystem_bloom_build.rs`
  - benches:
    - `bench_bloom_build_10k_keys`
    - `bench_bloom_build_100k_keys`
    - `bench_bloom_build_1m_keys`

- `benches/tier2_subsystem_iterator_multi_sst.rs`
  - benches:
    - `bench_iterator_disjoint_ssts`
    - `bench_iterator_overlapping_ssts`
    - `bench_iterator_partial_overlap_ssts`
    - `bench_iterator_multi_sst_comparison`

- `benches/tier2_subsystem_memtable_rotate.rs`
  - benches:
    - `bench_memtable_rotate_small`
    - `bench_memtable_rotate_large`

- `benches/tier2_subsystem_range_scan_cache.rs`
  - benches:
    - `bench_range_scan_warm_cache`
    - `bench_range_scan_cold_cache`
    - `bench_range_scan_partial_cache`
    - `bench_range_scan_cache_comparison`
    - `bench_range_scan_strided_access`

- `benches/tier2_subsystem_read_amplification.rs`
  - benches:
    - `bench_read_amp_point_lookups_zipfian`
    - `bench_read_amp_mixed_get_scan`
    - `bench_read_amp_uniform_distribution`
    - `bench_read_amp_cache_effectiveness`

- `benches/tier2_subsystem_sst_point_read_bloom.rs`
  - benches:
    - `bench_point_read_bloom_enabled`
    - `bench_point_read_bloom_disabled`
    - `bench_point_read_bloom_comparison`

- `benches/tier3_system_compaction.rs`
  - benches:
    - `bench_flush`
    - `bench_compact_all`
    - `bench_flush_throughput`
    - `bench_incremental_compact`
    - `bench_flush_concurrent`

- `benches/tier3_system_concurrency_stress.rs`
  - benches:
    - `bench_concurrent_puts`
    - `bench_mixed_read_write`
    - `bench_compaction_pressure`
    - `bench_concurrent_deletes`
    - `bench_concurrent_multi_cf`

- `benches/tier3_system_contention_heavy.rs`
  - benches:
    - `bench_engine_heavy_write_contention`
    - `bench_engine_heavy_read_contention`
    - `bench_engine_mixed_contention`

- `benches/tier3_system_durability_modes.rs`
  - benches:
    - `bench_durability_async_wal`
    - `bench_durability_wal_sync_every`
    - `bench_durability_concurrent`
    - `bench_durability_write_heavy`

- `benches/tier3_system_engine_advanced.rs`
  - benches:
    - `bench_ttl`
    - `bench_column_family_scaling`
    - `bench_large_values`
    - `bench_delete_heavy`

- `benches/tier3_system_engine_basic.rs`
  - benches:
    - `bench_single_put`
    - `bench_single_get`
    - `bench_single_delete`
    - `bench_batch_put`
    - `bench_mixed_crud`
    - `bench_concurrent_reads`
    - `bench_concurrent_writes`
    - `bench_point_lookup_miss`

- `benches/tier3_system_isolation_mvcc.rs`
  - benches:
    - `bench_single_thread_baseline`
    - `bench_contention_breakdown`
    - `bench_snapshot_stress`
    - `bench_transaction_isolation`

- `benches/tier3_system_lsm.rs`
  - benches:
    - `bench_system_wal_write`
    - `bench_system_flush_reopen_read`
    - `bench_system_l0_compaction`
    - `bench_system_mixed_workload`

- `benches/tier3_system_read_latency_during_flush.rs`
  - benches:
    - `bench_read_latency_during_flush`

- `benches/tier3_system_recovery.rs`
  - benches:
    - `bench_recovery_throughput`
    - `bench_recovery_with_wal_sync`
    - `bench_recovery_with_l0_data`
    - `bench_recovery_speed_comparison`

- `benches/tier3_system_scan_l0_only.rs`
  - benches:
    - `bench_scan_l0_direct`
    - `bench_scan_l0_prefix`

- `benches/tier3_system_scan_multi_level.rs`
  - benches:
    - `bench_scan_multi_level_range`

- `benches/tier3_system_snapshot_consistency.rs`
  - benches:
    - `bench_snapshot_consistency_concurrent_writes`

- `benches/tier3_system_sst_trie_index.rs`
  - benches:
    - `bench_point_lookups`
    - `bench_full_scans`
    - `bench_prefix_scans`

- `benches/tier3_system_startup_large.rs`
  - benches:
    - `bench_engine_startup_100k_sst_files`

- `benches/tier3_system_startup_wal.rs`
  - benches:
    - `bench_engine_startup_from_wal`

- `benches/tier3_system_sustained_mixed_workload.rs`
  - benches:
    - `bench_sustained_mixed_workload_with_compaction`

- `benches/tier4_integration_ycsb_workload_a.rs`
  - benches:
    - `bench_workload_a`

- `benches/tier4_integration_ycsb_workload_b.rs`
  - benches:
    - `bench_workload_b`

- `benches/tier4_integration_ycsb_workload_c.rs`
  - benches:
    - `bench_workload_c`

- `benches/tier4_integration_ycsb_workload_d.rs`
  - benches:
    - `bench_workload_d`

- `benches/tier4_integration_ycsb_workload_e.rs`
  - benches:
    - `bench_workload_e`

- `benches/tier4_integration_ycsb_workload_f.rs`
  - benches:
    - `bench_workload_f`

- `benches/tier5_soak_compaction_backlog_growth.rs`
  - benches:
    - `bench_compaction_backlog_growth`

- `benches/tier5_soak_level_drift.rs`
  - benches:
    - `bench_level_drift`

- `benches/tier5_soak_space_amplification.rs`
  - benches:
    - `bench_space_amplification`

- `benches/tier6_capacity_cold_start_large.rs`
  - benches:
    - `bench_cold_start_large`

- `benches/tier6_capacity_large_dataset_compaction.rs`
  - benches:
    - `bench_large_dataset_compaction`

- `benches/tier6_capacity_large_dataset_insert.rs`
  - benches:
    - `bench_large_dataset_insert`

- `benches/tier6_capacity_wal_growth_large.rs`
  - benches:
    - `bench_wal_growth_large`
