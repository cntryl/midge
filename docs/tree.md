# Midge Repository File Tree

```
midge/
├── .cargo/
│   └── config.toml
├── .docker/
│   ├── bench/
│   │   ├── compose.yml
│   │   └── Dockerfile
│   └── test/
│       └── Dockerfile
├── .github/
│   ├── copilot-instructions.md
│   ├── dependabot.yml
│   └── workflows/
│       ├── bench.yml
│       └── ci.yml
├── .vscode/
│   └── tasks.json
├── benches/
│   ├── criterion_helper.rs
│   ├── README.md
│   ├── TIER_LADDER.md
│   ├── tier1_hotpath/
│   │   ├── api.rs
│   │   ├── block_cache_hot.rs
│   │   ├── bloom.rs
│   │   ├── cache.rs
│   │   ├── data_structures.rs
│   │   ├── index.rs
│   │   ├── memtable_insert.rs
│   │   ├── memtable_seek.rs
│   │   ├── sst.rs
│   │   ├── tlv.rs
│   │   ├── wal_frame_parse.rs
│   │   └── wal.rs
│   ├── tier2_subsystem/
│   │   ├── block_cache_eviction.rs
│   │   ├── block_cache.rs
│   │   ├── bloom_build.rs
│   │   ├── bloom_false_positive_rate.rs
│   │   ├── core_primitives.rs
│   │   ├── flush.rs
│   │   ├── manifest_apply.rs
│   │   ├── manifest_large_history.rs
│   │   ├── manifest_parse.rs
│   │   ├── memtable_full.rs
│   │   ├── memtable_rotate.rs
│   │   ├── sst.rs
│   │   ├── wal_io.rs
│   │   ├── wal_replay.rs
│   │   └── wal_segment_rollover.rs
│   ├── tier3_system/
│   │   ├── bench_common.rs
│   │   ├── compaction.rs
│   │   ├── concurrency_stress.rs
│   │   ├── contention_heavy.rs
│   │   ├── durability_modes.rs
│   │   ├── engine_advanced.rs
│   │   ├── engine_basic.rs
│   │   ├── isolation_mvcc.rs
│   │   ├── lsm.rs
│   │   ├── recovery.rs
│   │   ├── scan_l0_only.rs
│   │   ├── scan_multi_level.rs
│   │   ├── startup_large.rs
│   │   └── startup_wal.rs
│   ├── tier4_integration/
│   │   ├── ycsb_common.rs
│   │   ├── ycsb_workload_a.rs
│   │   ├── ycsb_workload_b.rs
│   │   ├── ycsb_workload_c.rs
│   │   ├── ycsb_workload_d.rs
│   │   ├── ycsb_workload_e.rs
│   │   └── ycsb_workload_f.rs
│   ├── tier5_soak/
│   │   ├── compaction_backlog_growth.rs
│   │   ├── level_drift.rs
│   │   └── space_amplification.rs
│   └── tier6_capacity/
│       ├── cold_start_large.rs
│       ├── large_dataset_compaction.rs
│       ├── large_dataset_insert.rs
│       └── wal_growth_large.rs
├── cache/
├── docs/
│   ├── dev/
│   │   ├── architecture_principles.md
│   │   ├── bench_guidelines.md
│   │   ├── code_guidelines.md
│   │   └── test_guidelines.md
│   ├── features/
│   │   ├── api_surface.md
│   │   ├── basics.md
│   │   ├── benchmarks_ycsb.md
│   │   ├── bloom_filters.md
│   │   ├── caching.md
│   │   ├── cloud_backend_swap.md
│   │   ├── cloud_configuration_matrix.md
│   │   ├── cloud_integration/
│   │   │   ├── aws_s3.md
│   │   │   ├── azure_blob.md
│   │   │   ├── distributed_locking.md
│   │   │   ├── gcs.md
│   │   │   ├── hybrid_wal.md
│   │   │   ├── overview.md
│   │   │   ├── provider_detection.md
│   │   │   └── remote_tiering.md
│   │   ├── column_families.md
│   │   ├── compaction.md
│   │   ├── durability_modes_explained.md
│   │   ├── durability_profiles.md
│   │   ├── file_formats/
│   │   │   ├── lock_file_format.md
│   │   │   ├── manifest_format.md
│   │   │   ├── sst_format.md
│   │   │   ├── tlv_field_registry.md
│   │   │   └── wal_format.md
│   │   ├── hybrid_storage.md
│   │   ├── lock_file.md
│   │   ├── locking.md
│   │   ├── manifest.md
│   │   ├── memtable_basics.md
│   │   ├── merge_operators.md
│   │   ├── metrics_and_observability.md
│   │   ├── overview.md
│   │   ├── performance.md
│   │   ├── range_tombstones.md
│   │   ├── readonly_mode.md
│   │   ├── recovery_and_durability.md
│   │   ├── roadmap.md
│   │   ├── shale_modes.md
│   │   ├── shale_tlv_format.md
│   │   ├── snapshots.md
│   │   ├── sst_basics.md
│   │   ├── ttl_expiration.md
│   │   └── wal_basics.md
│   ├── wip/
│   │   ├── REQUIREMENTS.md
│   │   └── SPEC.md
│   ├── GLOSSARY.md
│   ├── INVARIANTS.md
│   ├── summary.md
│   ├── TEST_TIMEOUT_GUIDE.md
│   └── tree.md
├── fuzz/
│   ├── fuzz_targets/
│   │   ├── fuzz_block_decode.rs
│   │   ├── fuzz_bloom_filter.rs
│   │   ├── fuzz_internal_key.rs
│   │   ├── fuzz_sst_metadata.rs
│   │   ├── fuzz_tlv_reader.rs
│   │   └── fuzz_wal_decode.rs
│   ├── Cargo.toml
│   └── README.md
├── scripts/
│   └── benchmark_summary.py
├── src/
│   ├── api/
│   │   ├── column_family.rs
│   │   ├── kv_store.rs
│   │   ├── merge_operator.rs
│   │   ├── mod.rs
│   │   ├── mutation.rs
│   │   ├── query.rs
│   │   ├── snapshot.rs
│   │   ├── transaction.rs
│   │   ├── write_batch.rs
│   │   └── write_options.rs
│   ├── cloud/
│   │   ├── aws.rs
│   │   ├── azure.rs
│   │   ├── backend.rs
│   │   ├── gcp.rs
│   │   ├── hybrid.rs
│   │   ├── latency_sim.rs
│   │   ├── mock.rs
│   │   ├── mod.rs
│   │   └── oci.rs
│   ├── common/
│   │   ├── codec.rs
│   │   ├── error.rs
│   │   ├── internal_key.rs
│   │   ├── mod.rs
│   │   ├── range_tombstone.rs
│   │   ├── rate_limiter.rs
│   │   ├── test_hooks.rs
│   │   ├── timestamp.rs
│   │   ├── tlv.rs
│   │   └── worker.rs
│   ├── config/
│   │   ├── autotune.rs
│   │   ├── builder.rs
│   │   ├── cloud_builder.rs
│   │   ├── cloud.rs
│   │   ├── column_family.rs
│   │   ├── derivation.rs
│   │   ├── mod.rs
│   │   ├── options.rs
│   │   ├── profile.rs
│   │   ├── storage_mode.rs
│   │   └── validation.rs
│   ├── core/
│   │   ├── backup/
│   │   │   ├── backup_engine.rs
│   │   │   ├── mod.rs
│   │   │   ├── restore_engine.rs
│   │   │   ├── tests.rs
│   │   │   └── types.rs
│   │   ├── compaction/
│   │   │   ├── controller.rs
│   │   │   ├── executor.rs
│   │   │   ├── filter.rs
│   │   │   ├── mod.rs
│   │   │   └── strategy.rs
│   │   ├── data_structures/
│   │   │   ├── merge_iterator.rs
│   │   │   ├── mod.rs
│   │   │   └── skiplist.rs
│   │   ├── engine/
│   │   │   ├── cf_manager.rs
│   │   │   ├── column_family.rs
│   │   │   ├── core.rs
│   │   │   ├── factory.rs
│   │   │   ├── flush_manager.rs
│   │   │   ├── kv_store_adapter.rs
│   │   │   ├── mod.rs
│   │   │   ├── operations/
│   │   │   │   ├── maintenance.rs
│   │   │   │   ├── mod.rs
│   │   │   │   ├── mutations.rs
│   │   │   │   ├── observability.rs
│   │   │   │   ├── reads.rs
│   │   │   │   ├── snapshots.rs
│   │   │   │   ├── transactions.rs
│   │   │   │   └── writes.rs
│   │   │   ├── state.rs
│   │   │   └── types.rs
│   │   ├── locking/
│   │   │   ├── cloud.rs
│   │   │   ├── local.rs
│   │   │   ├── meta.rs
│   │   │   ├── mod.rs
│   │   │   ├── renewal.rs
│   │   │   └── traits.rs
│   │   ├── manifest/
│   │   │   ├── cloud.rs
│   │   │   ├── column_families.rs
│   │   │   ├── io.rs
│   │   │   ├── mod.rs
│   │   │   ├── queries.rs
│   │   │   ├── types.rs
│   │   │   ├── version_manager.rs
│   │   │   └── version_set.rs
│   │   ├── memtable/
│   │   │   ├── core.rs
│   │   │   ├── mod.rs
│   │   │   ├── range_tombstones.rs
│   │   │   └── wal_loading.rs
│   │   ├── metrics/
│   │   │   ├── engine.rs
│   │   │   ├── mod.rs
│   │   │   ├── performance.rs
│   │   │   └── timer.rs
│   │   ├── persistence/
│   │   │   ├── flush/
│   │   │   │   ├── bounds.rs
│   │   │   │   ├── mod.rs
│   │   │   │   ├── process.rs
│   │   │   │   ├── stats.rs
│   │   │   │   ├── traits.rs
│   │   │   │   └── worker.rs
│   │   │   ├── flush_coordinator.rs
│   │   │   ├── mod.rs
│   │   │   └── wal_replay.rs
│   │   ├── transaction/
│   │   │   ├── conflict_tracking.rs
│   │   │   ├── controller.rs
│   │   │   ├── core.rs
│   │   │   ├── engine_transaction.rs
│   │   │   ├── mod.rs
│   │   │   └── spill.rs
│   │   ├── mod.rs
│   │   ├── naming.rs
│   │   └── runtime.rs
│   ├── fs/
│   │   ├── io.rs
│   │   ├── mod.rs
│   │   ├── numbered_files.rs
│   │   ├── sync.rs
│   │   └── uring.rs
│   ├── health/
│   │   ├── mod.rs
│   │   ├── monitor.rs
│   │   ├── rehydration.rs
│   │   └── state.rs
│   ├── metrics/
│   │   ├── engine.rs
│   │   ├── mod.rs
│   │   ├── performance.rs
│   │   └── timer.rs
│   ├── sst/
│   │   ├── cloud/
│   │   │   ├── factory.rs
│   │   │   ├── lifecycle.rs
│   │   │   ├── mod.rs
│   │   │   ├── reader.rs
│   │   │   └── writer.rs
│   │   ├── fs/
│   │   │   ├── factory.rs
│   │   │   ├── iterator.rs
│   │   │   ├── mod.rs
│   │   │   ├── reader.rs
│   │   │   ├── utils.rs
│   │   │   └── writer.rs
│   │   ├── mem/
│   │   │   ├── factory.rs
│   │   │   ├── mod.rs
│   │   │   ├── reader.rs
│   │   │   └── writer.rs
│   │   ├── block_cache.rs
│   │   ├── bloom_cache.rs
│   │   ├── bloom.rs
│   │   ├── cache.rs
│   │   ├── encoding.rs
│   │   ├── file_manager.rs
│   │   ├── format.rs
│   │   ├── manifest_cache.rs
│   │   ├── meta_index.rs
│   │   ├── metadata_cache.rs
│   │   ├── mod.rs
│   │   ├── range_tombstone.rs
│   │   ├── reader_common.rs
│   │   ├── sparse_index_cache.rs
│   │   ├── sparse_index.rs
│   │   ├── table_cache.rs
│   │   ├── traits.rs
│   │   └── writer_common.rs
│   ├── wal/
│   │   ├── cloud/
│   │   │   ├── mod.rs
│   │   │   ├── reader.rs
│   │   │   ├── shared.rs
│   │   │   └── writer.rs
│   │   ├── fs/
│   │   │   ├── batched_sync.rs
│   │   │   ├── factory.rs
│   │   │   ├── group_commit.rs
│   │   │   ├── mod.rs
│   │   │   ├── reader.rs
│   │   │   └── writer.rs
│   │   ├── mem/
│   │   │   ├── factory.rs
│   │   │   ├── mod.rs
│   │   │   ├── reader.rs
│   │   │   ├── shared.rs
│   │   │   └── writer.rs
│   │   ├── arena.rs
│   │   ├── controller.rs
│   │   ├── encode_pipeline.rs
│   │   ├── encoding.rs
│   │   ├── mod.rs
│   │   ├── traits.rs
│   │   └── types.rs
│   └── lib.rs
├── tests/
│   ├── cloud/
│   │   └── cloud.rs
│   ├── common/
│   │   ├── cloud.rs
│   │   ├── helpers.rs
│   │   ├── mod.rs
│   │   └── test_helpers.rs
│   ├── admin_operations.rs
│   ├── api_kvstore.rs
│   ├── autotune.rs
│   ├── backup_restore.rs
│   ├── block_cache.rs
│   ├── cache_read_path.rs
│   ├── checkpoint.rs
│   ├── cloud_consistency.rs
│   ├── cloud_durability.rs
│   ├── cloud_hybrid.rs
│   ├── cloud_real_providers.rs
│   ├── column_family_lifecycle.rs
│   ├── compaction_basic.rs
│   ├── compaction_concurrent.rs
│   ├── compaction_errors.rs
│   ├── compaction_filters.rs
│   ├── compaction_levels.rs
│   ├── compaction_metrics.rs
│   ├── compression.rs
│   ├── concurrency_delete_range.rs
│   ├── concurrency_flush.rs
│   ├── concurrency_wal.rs
│   ├── concurrency_writes.rs
│   ├── config_api.rs
│   ├── config_validation.rs
│   ├── durability_atomicity.rs
│   ├── durability_recovery.rs
│   ├── durability_wal.rs
│   ├── engine_basic.rs
│   ├── engine_delete_range.rs
│   ├── engine_iterators.rs
│   ├── engine_merge_operators.rs
│   ├── engine_snapshots.rs
│   ├── engine_write_batch.rs
│   ├── error_handling.rs
│   ├── fault_injection.rs
│   ├── invariants_flush.rs
│   ├── invariants_lsm.rs
│   ├── memory_mode.rs
│   ├── metrics.rs
│   ├── paranoid_mode.rs
│   ├── proptest_parsers.proptest-regressions
│   ├── proptest_parsers.rs
│   ├── rate_limiting.rs
│   ├── readonly_mode.rs
│   ├── stress_large_values.rs
│   ├── stress_workloads.rs
│   ├── test_infrastructure.rs
│   ├── transaction_advanced.rs
│   ├── transaction_basic.rs
│   ├── transaction_conflicts.rs
│   ├── transaction_deadlock.rs
│   ├── transaction_isolation.rs
│   ├── transaction_spill.rs
│   └── ttl.rs
├── testutils/
│   ├── README.md
│   └── validate_tests.rs
├── tmp/
├── .dockerignore
├── .gitattributes
├── .gitignore
├── Cargo.lock
├── Cargo.toml
├── LICENSE
├── prompt_optimize.md
└── README.md
```

## Statistics

- **Source files**: 120+ Rust modules in `src/`
- **Test files**: 56 integration test files in `tests/`
- **Benchmark files**: 45+ benchmarks across 6 tiers
- **Fuzz targets**: 6 fuzz targets
- **Documentation**: 40+ markdown files in `docs/`
