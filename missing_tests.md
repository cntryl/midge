Perfect — here are **ready-to-drop-in file stubs** with *only* the test names you need to implement.
Organized by subsystem and aligned with the high-value gaps.

**You can copy/paste this directly into `tests/` or module-local `mod tests {}` blocks.**

---

# ✅ **1. WAL ↔ Manifest ↔ SST Atomicity**

`tests/atomicity_wal_manifest_sst.rs`

```rust
#[test] fn should_not_expose_sst_without_manifest_entry_after_crash() {}
#[test] fn should_delete_orphan_sst_on_recovery_when_manifest_missing() {}
#[test] fn should_replay_wal_until_manifest_fsynced_not_beyond() {}
#[test] fn should_resolve_conflict_when_wal_newer_than_manifest_and_sst_missing() {}
#[test] fn should_resolve_conflict_when_sst_exists_but_manifest_behind() {}
#[test] fn should_not_publish_new_ssts_until_manifest_durable() {}
#[test] fn should_maintain_atomicity_under_concurrent_flush_manifest_fsync() {}
#[test] fn should_maintain_order_when_multiple_cfs_flush_concurrently() {}
```

---

# ✅ **2. Memtable Freeze Edge Cases**

`tests/memtable_freeze_edge_cases.rs`

```rust
#[test] fn should_preserve_snapshot_seq_during_concurrent_freeze() {}
#[test] fn should_not_drop_range_tombstones_during_freeze_rollover() {}
#[test] fn should_not_lose_merge_operands_across_freeze_boundary() {}
#[test] fn should_not_publish_partial_freeze_given_concurrent_writes() {}
#[test] fn should_resolve_freeze_race_during_large_value_insert() {}
#[test] fn should_support_iterator_across_freeze_and_spill() {}
```

---

# ✅ **3. Multi-CF Compaction Interaction**

`tests/multi_cf_compaction_fairness.rs`

```rust
#[test] fn should_not_starve_cf_compaction_under_multi_cf_pressure() {}
#[test] fn should_keep_cf_compaction_independent_under_write_pressure() {}
#[test] fn should_handle_cf_drop_during_other_cf_compaction() {}
#[test] fn should_not_unblock_freeze_for_other_cf_during_unrelated_compaction() {}
```

---

# ✅ **4. Range Tombstones Under Stress**

`tests/range_tombstone_stress.rs`

```rust
#[test] fn should_coalesce_large_tombstone_fanout_during_compaction() {}
#[test] fn should_handle_long_lived_snapshots_with_massive_range_tombstones() {}
#[test] fn should_apply_range_tombstones_across_cf_flush_and_compaction() {}
#[test] fn should_handle_snapshot_then_tombstone_then_compaction_triple_interaction() {}
```

---

# ✅ **5. Merge Operator Failure & Concurrency**

`tests/merge_operator_failure_modes.rs`

```rust
#[test] fn should_recover_consistently_when_merge_operator_changes_mid_run() {}
#[test] fn should_handle_merge_operator_panic_during_flush() {}
#[test] fn should_apply_merge_chain_correctly_during_freeze_plus_wal_rotation() {}
```

---

# ✅ **6. Hybrid Cloud Storage Consistency**

`tests/cloud_consistency_edge_cases.rs`

```rust
#[test] fn should_handle_cloud_listing_lag_when_manifest_references_new_sst() {}
#[test] fn should_retry_cloud_upload_atomically_under_latency_spike() {}
#[test] fn should_rehydrate_partial_cloud_object_without_corruption() {}
#[test] fn should_resolve_mismatched_local_vs_cloud_checksums_during_sync() {}
```

---

# ✅ **7. Transactions × Range Deletes × Compaction**

`tests/transaction_range_delete_integration.rs`

```rust
#[test] fn should_preserve_snapshot_view_across_range_delete_and_compaction() {}
#[test] fn should_abort_transaction_safely_during_range_delete_spill() {}
#[test] fn should_recover_after_crash_during_tx_range_delete_spill_rotation() {}
#[test] fn should_resolve_conflicts_between_tx_write_and_range_tombstone() {}
```

---

# ✅ **8. Iterator Stability Under Chaos**

`tests/iterator_stability_under_pressure.rs`

```rust
#[test] fn should_iterate_consistently_across_sst_boundaries_with_evictions() {}
#[test] fn should_rewind_correctly_with_tombstones_and_merges() {}
#[test] fn should_handle_freeze_then_compaction_then_iterate_sequence() {}
#[test] fn should_yield_stable_results_with_cf_flush_in_progress() {}
```

---

# ✅ **9. Large Value Workload Gaps**

`tests/large_value_stress.rs`

```rust
#[test] fn should_flush_memtable_with_mixed_small_and_large_values() {}
#[test] fn should_apply_backpressure_under_large_value_workload() {}
#[test] fn should_recover_large_value_batches_after_crash() {}
#[test] fn should_respect_snapshot_visibility_for_large_values() {}
```

---

# ✅ **10. Checkpoint × Compaction × Recovery (Triple Interaction)**

`tests/checkpoint_compaction_recovery_triple.rs`

```rust
#[test] fn should_recover_consistently_given_checkpoint_during_compaction_then_crash() {}
#[test] fn should_not_produce_partial_checkpoint_when_manifest_is_stale() {}
#[test] fn should_apply_wal_replay_correctly_when_checkpoint_excludes_pending_tombstones() {}
#[test] fn should_resolve_conflict_between_checkpoint_and_inflight_compaction_on_restart() {}
```

