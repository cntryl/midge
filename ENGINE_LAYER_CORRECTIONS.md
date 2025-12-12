# ENGINE LAYER SPEC CARD CORRECTIONS

## Overview

After detailed review of actual test files vs spec cards, found the following issues:

---

## CORRECTIONS NEEDED

### 1. config_api.md ✅ SPEC IS COMPLETE (but needs updating)

**Issue**: Spec is missing 4 tests that exist in actual file.

**Actual tests in file**:
1. should_build_config_given_minimal_defaults_when_only_path_provided ✅
2. should_set_goal_given_latency_when_optimizing_for_p99 ✅
3. should_set_goal_given_throughput_when_optimizing_for_bulk_writes ✅
4. should_set_goal_given_cost_when_minimizing_resources ✅
5. should_set_durability_given_strict_when_fsync_per_write_required ✅
6. should_set_durability_given_steady_when_balanced_sync_needed ✅
7. should_respect_memory_budget_given_explicit_bytes_when_configured ✅
8. should_use_auto_memory_given_no_explicit_budget_when_default ✅
9. should_optimize_params_given_write_heavy_profile_when_configured ✅
10. should_optimize_params_given_read_mostly_profile_when_configured ✅
11. should_optimize_params_given_range_scan_profile_when_configured ✅
12. should_derive_consistent_params_given_all_knobs_set_when_building ❌ SPEC MISSING (shown as test 17)
13. should_derive_different_params_given_latency_vs_throughput_when_comparing ❌ SPEC MISSING (shown as test 18)
14. should_provide_getter_access_given_derived_params_when_querying ❌ **NOT IN SPEC**
15. should_store_path_given_relative_path_when_building ❌ **NOT IN SPEC**
16. should_store_path_given_absolute_path_when_building ❌ **NOT IN SPEC**
17. should_clone_options_preserving_all_settings_given_configured_opts_when_cloning ❌ **NOT IN SPEC**
18. should_use_sensible_defaults_given_no_configuration_when_using_default ❌ **NOT IN SPEC**

**Spec location issues**:
- Spec test #12: "should_require_cloud_config_given_cloud_mode_when_not_off" - NOT IN FILE
- Spec test #13: "should_allow_cloud_off_given_no_cloud_config_when_local_only" - NOT IN FILE
- Spec test #14: "should_enable_autotune_given_flag_set_when_requested" - NOT IN FILE
- Spec test #15: "should_disable_autotune_given_default_when_not_requested" - NOT IN FILE

**ACTION**: Rewrite config_api spec tests 12-18 to match actual file.

---

### 2. engine_basic.rs ❌ MISSING TEST

**Issue**: Spec claims test #8 should be "should_not_create_filesystem_artifacts_when_memory_mode" but actual test #8 is "should_handle_many_operations_when_sequential"

**Actual tests in file**:
1. should_get_value_given_existing_key_when_put ✅
2. should_return_none_given_nonexistent_key_when_get ✅
3. should_overwrite_value_given_existing_key_when_put ✅
4. should_handle_empty_value_when_put ✅
5. should_handle_binary_data_when_put ✅
6. should_return_none_given_deleted_key_when_get ✅
7. should_succeed_given_nonexistent_key_when_delete ✅
8. should_handle_many_operations_when_sequential ✅ (but spec expects #8 to be filesystem artifacts test)

**FINDING**: The "filesystem artifacts" test doesn't exist in the file. Spec test #8 is incorrect.

**ACTION**: Choose one:
- Option A: Update spec test #8 description to match actual test
- Option B: Add the missing filesystem artifacts test to the test file

Recommendation: **Option B** - The filesystem artifacts test is important for validating memory mode behavior. Add test #9 to engine_basic.rs.

---

### 3. engine_write_batch.rs ✅ PERFECT MATCH

All 17 tests match exactly. No changes needed.

---

### 4. engine_delete_range.rs ⚠️ MINOR NAME MISMATCH

**Issue**: Test #3 name in actual file differs from spec.

**Spec test #3**: "should_handle_large_range_deletion_given_many_keys_when_deleting"  
**Actual test #3**: "should_accept_delete_range_call_with_valid_bounds_when_called"

**STATUS**: Actual file test is testing bounds validation, not large deletion. Spec description is for a different test concept.

**ACTION**: Update spec test #3 description to match actual test, OR verify intention and update test name/code.

---

### 5. engine_iterators.rs ✅ PERFECT MATCH

All 17 tests match exactly. No changes needed.

---

### 6. engine_snapshots.rs ✅ PERFECT MATCH

All 14 tests match exactly. No changes needed.

---

### 7. engine_merge.rs ✅ PERFECT MATCH

All 19 tests match exactly. No changes needed.

---

### 8. engine_ttl.rs ⚠️ MINOR WORDING

**Issue**: Spec test #3 has contradictory wording.

**Spec test #3**: "should_expire_key_given_zero_ttl_means_no_expiration_when_reading"  
**Actual test #3**: "fn should_not_expire_key_given_zero_ttl_means_no_expiration_when_reading()"

**ISSUE**: Spec says "should_expire" but the logic says "zero_ttl = NO expiration", so test should be "should_NOT_expire"

**ACTION**: Fix spec test #3 wording: Add "not" → "should_not_expire_key_given_zero_ttl_means_no_expiration_when_reading"

---

## Summary Table

| File | Status | Issue | Fix |
|------|--------|-------|-----|
| config_api.md | ❌ | Missing/wrong tests 12-18 | Rewrite tests 12-18 |
| engine_basic.md | ❌ | Test #8 missing (filesystem artifacts) | Add test or update description |
| engine_write_batch.md | ✅ | None | None |
| engine_delete_range.md | ⚠️ | Test #3 description mismatch | Update description or verify test intent |
| engine_iterators.md | ✅ | None | None |
| engine_snapshots.md | ✅ | None | None |
| engine_merge.md | ✅ | None | None |
| engine_ttl.md | ⚠️ | Test #3: "expire" should be "not expire" | Add "not" to test #3 name |

---

## Recommended Workflow

1. **Fix config_api.md** (highest priority - 6 test discrepancies)
2. **Fix engine_basic.md** (missing important test)
3. **Fix engine_delete_range.md** (verify test intent)
4. **Fix engine_ttl.md** (minor wording fix)
5. Move to next layer (column_families) with corrected process

---

## Process Lessons Learned

For future spec cards:
1. Always verify test count by reading actual file, not just counting decorators
2. Extract test names directly from function signatures
3. Match descriptions to actual test implementations
4. Cross-reference with INTEGRATION_TESTS_FINAL.md for authoritative names
5. Flag missing tests that should exist (like filesystem artifacts test)
