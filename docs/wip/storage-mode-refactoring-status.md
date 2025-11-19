# Storage Mode Refactoring Status

## Goal
Refactor all compaction tests to validate behavior across multiple storage modes (LocalDisk and CloudBacked), using common helpers from `tests/common/mod.rs`.

## Common Helpers Added ✅

### In `tests/common/mod.rs`:
- `create_storage_mode(mode: &str)` - Creates storage configurations
- `all_storage_modes()` - Returns ["Memory", "LocalDisk", "CloudBacked"]
- `disk_storage_modes()` - Returns ["LocalDisk", "CloudBacked"] (for tests requiring SST files)
- `compaction_test_opts(storage_mode)` - Updated to accept storage_mode parameter (was hardcoded to Memory)
- `populate_multi_level_data(engine, cf)` - Already existed, unchanged

## Completed Files ✅

### ✅ `compact_multi_level_compaction_cascades.rs` (4 tests)
- All tests iterate over `disk_storage_modes()`
- Tests: 8 scenarios (4 tests × 2 storage modes)
- Status: **PASSING**

### ✅ `compact_reads_during_compaction.rs` (5 tests)
- All tests iterate over `disk_storage_modes()`
- Tests: 10 scenarios (5 tests × 2 storage modes)
- Status: **PASSING** (minor unused variable warning fixed)

### ✅ `compact_ttl_compaction_filter.rs` (4 tests)
- All tests iterate over `disk_storage_modes()`
- Tests: 8 scenarios (4 tests × 2 storage modes)
- Status: **PASSING**

### ✅ `compact_level_target_size_enforcement.rs` (4 tests)
- All tests iterate over `disk_storage_modes()`
- Tests: 8 scenarios (4 tests × 2 storage modes)
- Status: **PASSING**

## Remaining Files 🔄

### `compact_writes_during_compaction.rs` (4 tests)
- Already uses `compaction_test_opts()` from common
- Needs: Wrap tests in storage mode loops, pass storage_mode parameter
- Tests: `should_allow_writes_given_l0_l1_compaction_running`, `should_handle_put_to_compacting_key_range`, `should_write_to_new_sst_given_ongoing_compaction_when_flush`, `should_not_compact_newly_flushed_files_given_compaction_in_progress`

### `compact_l0_sublevel_compaction.rs` (4 tests)  
- Already uses `compaction_test_opts()` from common
- Needs: Wrap tests in storage mode loops, pass storage_mode parameter
- Tests: `should_organize_l0_into_sublevels_given_overlapping_files`, `should_compact_oldest_sublevel_first_given_incremental_strategy`, `should_compact_all_sublevels_given_aggressive_strategy_when_file_count_high`, `should_maintain_sublevel_ordering_given_concurrent_flushes`

### `compact_custom_compaction_filter.rs` (3 tests)
- Uses LocalDisk with manual TempDir
- Needs: Replace manual temp_dir logic with `create_storage_mode()`, wrap in loops
- Tests: `should_invoke_filter_for_each_key_given_compaction_with_custom_filter`, `should_drop_key_given_filter_returns_remove_decision`, `should_keep_key_given_filter_returns_keep_decision`

### `compact_compaction_error_recovery.rs` (5 tests)
- Check storage mode usage
- May need special handling for error scenarios
- Tests: `should_retry_compaction_given_disk_full_error_when_writing_sst`, `should_abort_compaction_given_corruption_detected_when_reading_input`, `should_cleanup_partial_output_given_compaction_failure`, `should_restore_manifest_given_compaction_crash_before_commit`, `should_preserve_input_files_given_compaction_error_when_aborting`

### `compact_compaction_cancellation.rs` (3 tests)
- Check storage mode usage
- Tests: `should_stop_compaction_given_shutdown_signal`, `should_cleanup_resources_given_cancelled_compaction`, `should_not_update_manifest_given_incomplete_compaction_when_shutdown`

### `compact_amplification_measurement.rs` (4 tests)
- Check storage mode usage
- Tests: `should_measure_read_amplification_given_multilevel_scan`, `should_measure_write_amplification_given_compaction_cascade`, `should_measure_space_amplification_given_live_vs_total_data`, `should_track_amplification_over_time_given_workload`

## Summary
- **Completed**: 17 tests (34 scenarios with 2 storage modes each)
- **Remaining**: ~23 tests to update
- **All completed tests**: PASSING ✅
