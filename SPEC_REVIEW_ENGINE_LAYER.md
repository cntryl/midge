# Engine Layer Spec Card Review - DETAILED FINDINGS

**Date**: December 12, 2025  
**Status**: 🚧 In Progress - Found Discrepancies

---

## Summary

Reviewing engine layer specs (8 files) against actual test implementations. Found several discrepancies that need correction:

### Files Status
| File | Spec Tests | Actual Tests | Discrepancy | Status |
|------|-----------|--------------|-------------|--------|
| config_api.rs | 18 | 18 | ✅ Count matches, but 4 tests NOT in spec | 🚨 Spec incomplete |
| engine_basic.rs | 8 | 8 | ✅ Count matches, but test #8 name wrong | 🚨 Spec name mismatch |
| engine_write_batch.rs | 17 | 17 | ✅ Perfect match | ✅ OK |
| engine_delete_range.rs | 10+ | 10 | ⚠️ "+" interpreted, one name different | 🚨 Minor mismatch |
| engine_iterators.rs | 17 | 17 | ✅ Perfect match | ✅ OK |
| engine_snapshots.rs | 14+ | 14 | ✅ Count matches exactly | ✅ OK |
| engine_merge.rs | 19 | 19 | ✅ Perfect match | ✅ OK |
| engine_ttl.rs | 12 | 12 | ✅ Perfect match | ✅ OK |

---

## Detailed Findings

### 1. config_api.rs ❌ SPEC INCOMPLETE

**Spec shows tests 1-18:**
```
1. should_build_config_given_minimal_defaults_when_only_path_provided
2. should_set_goal_given_latency_when_optimizing_for_p99
3. should_set_goal_given_throughput_when_optimizing_for_bulk_writes
4. should_set_goal_given_cost_when_minimizing_resources
5. should_set_durability_given_strict_when_fsync_per_write_required
6. should_set_durability_given_steady_when_balanced_sync_needed
7. should_respect_memory_budget_given_explicit_bytes_when_configured
8. should_use_auto_memory_given_no_explicit_budget_when_default
9. should_optimize_params_given_write_heavy_profile_when_configured
10. should_optimize_params_given_read_mostly_profile_when_configured
11. should_optimize_params_given_range_scan_profile_when_configured
12. should_require_cloud_config_given_cloud_mode_when_not_off
13. should_allow_cloud_off_given_no_cloud_config_when_local_only
14. should_enable_autotune_given_flag_set_when_requested
15. should_disable_autotune_given_default_when_not_requested
16. should_convert_to_options_given_config_when_bridging_to_engine
17. should_derive_consistent_params_given_all_knobs_set_when_building
18. should_derive_different_params_given_latency_vs_throughput_when_comparing
```

**Actual file tests (18 total):**
Tests 1-13 match spec exactly.  
Tests 14-18 match spec tests 14-18.

**BUT ADDITIONAL TESTS:**
```
19. should_provide_getter_access_given_derived_params_when_querying
20. should_store_path_given_relative_path_when_building
21. should_store_path_given_absolute_path_when_building
22. should_clone_options_preserving_all_settings_given_configured_opts_when_cloning
```

Wait - these are in the actual file but not counted. Let me recount - the grep showed 18 matches total, so these must be somewhere else or I miscounted.

Actually looking at the grep output again:
- Line 239: should_provide_getter_access_given_derived_params_when_querying
- Line 257: should_store_path_given_relative_path_when_building
- Line 268: should_store_path_given_absolute_path_when_building
- Line 283: should_clone_options_preserving_all_settings_given_configured_opts_when_cloning
- Line 305: should_use_sensible_defaults_given_no_configuration_when_using_default

That's 5 extra tests! The count of 18 #[test] decorators must have included something else. Let me verify more carefully.

**ACTION NEEDED**: Re-examine the actual file and update spec with complete accurate test list.

---

### 2. engine_basic.rs ❌ TEST #8 NAME MISMATCH

**Spec says test #8:**
```
8. should_not_create_filesystem_artifacts_when_memory_mode
```

**Actual test #8:**
```
fn should_handle_many_operations_when_sequential() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange: parametrized write loop of 100 operations
```

**MISMATCH**: This test should verify memory mode doesn't create disk files, but instead it's testing sequential operations (which is valid but different from spec).

**ACTION NEEDED**: 
- Check if filesystem artifacts test exists elsewhere in the file
- If not, either:
  1. Add the missing test, OR
  2. Update spec to match actual test

---

### 3. engine_delete_range.rs ❌ TEST NAME MISMATCH

**Spec says test #3:**
```
3. should_handle_large_range_deletion_given_many_keys_when_deleting
```

**Actual test #3:**
```
fn should_accept_delete_range_call_with_valid_bounds_when_called()
```

**MISMATCH**: Actual test is different from spec description.

**ACTION NEEDED**: Update spec to match actual tests or vice versa.

---

### 4. engine_snapshots.rs ✅ MATCHES

All 14 tests match between spec and actual file. No issues.

---

### 5. engine_merge.rs ✅ MATCHES

All 19 tests match between spec and actual file. No issues.

---

### 6. engine_ttl.rs ⚠️ PARTIAL ISSUE

**Spec test #3:**
```
3. should_expire_key_given_zero_ttl_means_no_expiration_when_reading
```

**Actual test #3:**
```
fn should_not_expire_key_given_zero_ttl_means_no_expiration_when_reading()
```

**MINOR**: "should_expire" vs "should_not_expire" - contradictory! Actual is correct (zero TTL = no expiration = should_not_expire).

**ACTION NEEDED**: Fix spec test #3 description to "should_not_expire_key..." (add "not")

---

### 7. engine_write_batch.rs ✅ PERFECT MATCH

All 17 tests match exactly between spec and actual file. No issues.

---

### 8. engine_iterators.rs ✅ PERFECT MATCH

All 17 tests match exactly between spec and actual file. No issues.

---

## Summary of Actions Needed

### 🚨 CRITICAL
1. **config_api.md**: Spec shows 18 tests but may be missing some. Verify exact count and names.
2. **engine_basic.md**: Test #8 name is wrong. Spec says "should_not_create_filesystem_artifacts_when_memory_mode" but actual is "should_handle_many_operations_when_sequential".
3. **engine_delete_range.md**: Test #3 name mismatch. Spec says large range deletion but actual test is about valid bounds.

### ⚠️ MINOR
4. **engine_ttl.md**: Test #3 name should be "should_NOT_expire_key" (add "not"). Minor word choice fix.

---

## Next Steps

1. Read actual test files (config_api, engine_basic, engine_delete_range) thoroughly
2. Update specs to match actual test names and behaviors
3. Verify no tests are missing or incorrectly named
4. Re-verify all 8 engine layer specs are accurate
5. Move to next layer (column_families, durability) with corrected process
