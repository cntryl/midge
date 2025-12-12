# config_api.rs - Spec Card

## Philosophy

Tests define the **correct future behavior**, not document current limitations. Always implement tests fully; they may fail until features exist.

- ✅ Write ALL tests (never `#[ignore]`)
- ✅ Tests **MAY FAIL** if features aren't implemented yet
- ✅ Once features are built, failing tests become passing tests
- ✅ Tests act as a specification for what code needs to do
- ❌ Never stub behavior; always assert desired semantics
- ❌ Never skip tests on certain storage modes; use conditional logic instead

---

## PROMPT (Self-Driving Implementation Guide)

**Create a test file that validates the OpenOptions configuration builder API and parameter derivation for different optimization goals and workload profiles.**

**Key Requirements**:
- All 18 tests parametrized across all storage modes (Memory, LocalDisk, CloudBacked)
- Pattern: `for_each_storage_mode(&all_storage_modes_new(), |mode, opts| { ... })`
- Focus on API structure, not persistence (use all-modes, not durable-only)
- Each test validates ONE specific configuration goal or setting
- Verify that optimization goals (Latency/Throughput/Cost) derive correct parameters
- Verify workload profiles (WriteHeavy/ReadMostly/RangeScan) adjust tuning accordingly
- Verify durability settings (Strict/Steady) configure fsync behavior
- Verify memory budget settings are respected or auto-calculated
- Verify cloud config requirements and CloudMode behavior

**Testing Approach**:
1. Create empty engine with minimal config → verify defaults
2. Create engine with specific goal → verify parameters are derived correctly
3. Create engine with workload profile → verify tuning reflects profile
4. Verify that conflicting settings are validated or resolved
5. Compare two configs with different goals → confirm they produce different parameters

**Critical Details**:
- ✅ Use all_storage_modes_new() (this is config logic, not persistence)
- ✅ Tests should NOT involve WAL/recovery/crashes
- ✅ Focus on OpenOptions builder API validation
- ✅ Verify derived values through getter methods on MidgeOptions
- ✅ No Phase 1/Phase 2 structure needed

---

**File Location**: `tests/config_api.rs`
**Test Count**: 18 tests
**Storage Modes**: ALL (Memory, LocalDisk, CloudBacked)
**Pattern**: `for_each_storage_mode(&all_storage_modes_new(), |mode, opts| { ... })`
**Status**: ✅ 18/18 passing

---

## Purpose
Test the OpenOptions configuration builder API and parameter derivation for different optimization goals (latency, throughput, cost) and workload profiles.

---

## Tests

1. **should_build_config_given_minimal_defaults_when_only_path_provided**
   - Creates engine with only path, verifies default config applies

2. **should_set_goal_given_latency_when_optimizing_for_p99**
   - Goal: Latency → verifies smaller blocks, bigger cache, aggressive flushing

3. **should_set_goal_given_throughput_when_optimizing_for_bulk_writes**
   - Goal: Throughput → verifies larger blocks, batching, delayed flushing

4. **should_set_goal_given_cost_when_minimizing_resources**
   - Goal: Cost → verifies smaller memory footprint, compact layout

5. **should_set_durability_given_strict_when_fsync_per_write_required**
   - Durability: Strict → fsync_enabled = true per write

6. **should_set_durability_given_steady_when_balanced_sync_needed**
   - Durability: Steady → balanced fsync (periodic, not per-write)

7. **should_respect_memory_budget_given_explicit_bytes_when_configured**
   - Sets explicit memory budget, verifies it's applied

8. **should_use_auto_memory_given_no_explicit_budget_when_default**
   - No memory budget set → auto-calculate based on available RAM

9. **should_optimize_params_given_write_heavy_profile_when_configured**
   - Profile: WriteHeavy → larger memtable, delayed compaction

10. **should_optimize_params_given_read_mostly_profile_when_configured**
    - Profile: ReadMostly → bigger block cache, pre-compute bloom filters

11. **should_optimize_params_given_range_scan_profile_when_configured**
    - Profile: RangeScan → fence pointers, trie index for range queries

12. **should_derive_consistent_params_given_all_knobs_set_when_building**
    - All config options set (path, goal, durability, budget, workload) → verifies consistency and no conflicts

13. **should_derive_different_params_given_latency_vs_throughput_when_comparing**
    - Compare two configs (latency goal vs throughput goal) → confirms different parameters (block size, memtable, SST)

14. **should_provide_getter_access_given_derived_params_when_querying**
    - Verifies all getter methods work and return sensible positive values (block_size, memtable_size_limit, target_sst_size, block_cache_size, wal_buffer_size, l0_compaction_trigger)

15. **should_store_path_given_relative_path_when_building**
    - Sets relative path → verifies it's stored as-is

16. **should_store_path_given_absolute_path_when_building**
    - Sets absolute path → verifies it's stored as-is

17. **should_clone_options_preserving_all_settings_given_configured_opts_when_cloning**
    - Clones fully configured OpenOptions → verifies all fields copied (path, goal, durability, workload, derived values)

18. **should_use_sensible_defaults_given_no_configuration_when_using_default**
    - Uses Default::default() on OpenOptions → verifies defaults (Goal::Latency, Durability::Steady, MemoryBudget::Auto, WorkloadProfile::Mixed)

---

## Key APIs
- `OpenOptions::new(path)`
- `.goal(Goal::Latency | Throughput | Cost)`
- `.durability(Durability::Strict | Steady)`
- `.memory_budget(bytes)`
- `.workload_profile(Profile::WriteHeavy | ReadMostly | RangeScan)`
- `.cloud_config(CloudConfig)`
- `.autotune_limits(bool)`
- `.build()` → `MidgeOptions`

---

## Implementation Notes

✅ All tests use `all_storage_modes_new()` (logic tests, not persistence)
✅ Tests verify derived parameters via getter methods on returned MidgeOptions
✅ Use assertion helpers to check block sizes, cache allocation, etc.
✅ No crash/recovery phases needed (pure config validation)

---

## Test Pattern Example

```rust
#[test]
fn should_set_goal_given_latency_when_optimizing_for_p99() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange: opts already has Goal::Latency set

        // Act: Create engine with latency-optimized opts
        let engine = open_with_mode(opts, mode);

        // Assert: Verify configuration parameters
        assert_eq!(engine.block_size(), LATENCY_OPTIMIZED_BLOCK_SIZE, "mode: {}", mode);
        assert!(engine.cache_size() > normal_cache, "latency profile should have bigger cache");
    });
}
```

---

## Status

**Current**: ✅ 18/18 passing
**Notes**: Config builder fully working, all optimization goals implemented

---

## References
- See INTEGRATION_TESTS_FINAL.md line ~660 for full config spec
- Builder pattern in `src/engine/api.rs`
