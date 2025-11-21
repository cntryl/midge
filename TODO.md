# TODO: Implement Stubbed Benchmark Methods

This document provides a complete audit of all benchmark methods across all tiers.

- benches are correctly registered in Cargo.toml
- benches are clean and follow the project bench guidelines
- benches are benching real code not mocks
- benches contain no cargo clippy errors or warnings

### `benches/tier1_hotpath/api.rs`

- `bench_batch_put`
- `bench_single_get`
- `bench_single_put`
- `bench_batch_delete`
- `bench_batch_mixed`
- `bench_range_scan`

### `benches/tier1_hotpath/block_cache_hot.rs`

- `bench_block_cache_get_hot`
- `bench_block_cache_insert_hot`
- `bench_block_cache_hit_ratio_fast`

### `benches/tier1_hotpath/bloom.rs`

- `bench_bloom_maybe_contains`
- `bench_bloom_compute_hashes`
- `bench_bloom_filter_hot_check`

### `benches/tier1_hotpath/cache.rs`

- `bench_cache_insert`
- `bench_cache_get_hit`
- `bench_cache_get_miss`
- `bench_cache_eviction`
- `bench_cache_concurrent_access`

### `benches/tier1_hotpath/index.rs`

- `bench_bloom_build`
- `bench_bloom_query`
- `bench_bloom_false_positive_rates`

### `benches/tier1_hotpath/memtable_insert.rs`

- `bench_memtable_put_key_small`
- `bench_memtable_put_key_medium`
- `bench_memtable_put_key_large`
- `bench_memtable_seq_insert`

### `benches/tier1_hotpath/memtable_seek.rs`

- `bench_memtable_get_point_lookup`
- `bench_memtable_get_latest_version`
- `bench_memtable_seek_forward_32steps`
- `bench_memtable_seek_reverse_32steps`

### `benches/tier1_hotpath/sst.rs`

- `bench_encode`
- `bench_decode`
- `bench_iterator_step`
- `bench_roundtrip`
- `bench_writer_tiny`

### `benches/tier1_hotpath/storage.rs`

- `bench_skiplist_sequential`
- `bench_skiplist_random`
- `bench_skiplist_concurrent`
- `bench_memtable_sequential`
- `bench_memtable_random`
- `bench_memtable_read`
- `bench_compression_lz4`

### `benches/tier1_hotpath/tlv.rs`

- `bench_varint32_encode`
- `bench_varint64_encode`
- `bench_varint32_decode`
- `bench_varint64_decode`
- `bench_tlv_writer`
- `bench_tlv_reader`
- `bench_tlv_roundtrip`

### `benches/tier1_hotpath/wal.rs`

- `bench_wal_encode_record`
- `bench_wal_decode_record`
- `bench_wal_roundtrip`
- `bench_wal_encode_delete_fast_path`
- `bench_wal_encode_put_fast_path`
- `bench_wal_batch_encode_comparison`
- `bench_wal_mixed_workload`
- `bench_wal_append_individual`
- `bench_wal_append_batch`
- `bench_wal_io_seq_throughput`
- `bench_wal_io_append_sync_latency`
- `bench_wal_io_preencoded`
- `bench_wal_io_uring_compare`

### `benches/tier1_hotpath/wal_frame_parse.rs`

- `bench_wal_frame_parse_small`
- `bench_wal_frame_parse_medium`
- `bench_wal_frame_parse_large`
- `bench_wal_header_scan_only`

## Tier 2 Subsystem Benchmarks

### `benches/tier2_subsystem/block_cache.rs`

- `bench_block_cache_eviction_scan`
- `bench_block_cache_fill_then_hit`
- `bench_block_cache_hotset_rotation`

### `benches/tier2_subsystem/block_cache_eviction.rs`

- `bench_block_cache_lru_eviction_1k`
- `bench_block_cache_lru_eviction_10k`

### `benches/tier2_subsystem/bloom_build.rs`

- `bench_bloom_build_10k_keys`
- `bench_bloom_build_100k_keys`

### `benches/tier2_subsystem/bloom_false_positive_rate.rs`

- `bench_bloom_false_positive_rate_small`
- `bench_bloom_false_positive_rate_large`

### `benches/tier2_subsystem/concurrency_stress.rs`

- `bench_concurrent_puts`
- `bench_mixed_read_write`
- `bench_compaction_pressure`
- `bench_concurrent_deletes`
- `bench_concurrent_multi_cf`

### `benches/tier2_subsystem/engine_advanced.rs`

- `bench_ttl`
- `bench_column_family_scaling`
- `bench_large_values`
- `bench_delete_heavy`

### `benches/tier2_subsystem/engine_basic.rs`

- `bench_put_variants`
- `bench_concurrent_cf_scaling`
- `bench_get_hit_miss`
- `bench_delete`
- `bench_write_modes`
- `bench_memory_mode`
- `bench_full_stack_throughput`

### `benches/tier2_subsystem/flush.rs`

- `bench_flush_small_memtable`
- `bench_flush_large_memtable`
- `bench_flush_sparse_index_build`

### `benches/tier2_subsystem/isolation_mvcc.rs`

- `bench_single_thread_baseline`
- `bench_concurrent_puts_latency`
- `bench_contention_breakdown`
- `bench_compaction_amplification`
- `bench_reads_during_compaction`
- `bench_snapshot_stress`
- `bench_transaction_isolation`
- `bench_snapshots_during_compaction`

### `benches/tier2_subsystem/manifest_apply.rs`

- `bench_manifest_apply_100_ops`
- `bench_manifest_apply_10k_ops`

### `benches/tier2_subsystem/manifest_large_history.rs`

- `bench_manifest_replay_100k_entries`

### `benches/tier2_subsystem/manifest_parse.rs`

- `bench_manifest_parse_small`
- `bench_manifest_parse_large`

### `benches/tier2_subsystem/memtable_full.rs`

- `bench_memtable_full_scan`
- `bench_memtable_full_eviction_trigger`

### `benches/tier2_subsystem/memtable_rotate.rs`

- `bench_memtable_rotate_small`
- `bench_memtable_rotate_large`

### `benches/tier2_subsystem/sst.rs`

- `bench_sst_iterator_full`
- `bench_sst_full_decode`
- `bench_sst_writer_scale`
- `bench_sst_writer_compression`

### `benches/tier2_subsystem/storage.rs`

- `bench_wal_write`
- `bench_block_builder`
- `bench_block_decode`
- `bench_sst_file`
- `bench_writebatch_apply`
- `bench_merge_iterator`

### `benches/tier2_subsystem/wal_replay.rs`

- `bench_wal_replay_small_file`
- `bench_wal_replay_large_file`
- `bench_wal_replay_corrupted_tail`

### `benches/tier2_subsystem/wal_segment_rollover.rs`

- `bench_wal_rollover_small_segments`
- `bench_wal_rollover_large_segments`

## Tier 3 System Benchmarks

### `benches/tier3_system/compaction.rs`

- `bench_flush`
- `bench_compact_all`

### `benches/tier3_system/contention_heavy.rs`

- `bench_engine_heavy_write_contention`
- `bench_engine_heavy_read_contention`
- `bench_engine_mixed_contention`

### `benches/tier3_system/durability_modes.rs`

- `bench_durability_async_wal`
- `bench_durability_wal_sync_every`
- `bench_durability_concurrent`
- `bench_durability_read_heavy`
- `bench_durability_write_heavy`

### `benches/tier3_system/lsm.rs`

- `bench_system_wal_write`
- `bench_system_flush_reopen_read`
- `bench_system_l0_compaction`
- `bench_system_mixed_workload`

### `benches/tier3_system/recovery.rs`

- `bench_recovery_throughput`
- `bench_recovery_with_wal_sync`
- `bench_recovery_with_l0_data`
- `bench_recovery_speed_comparison`

### `benches/tier3_system/scan_l0_only.rs`

- `bench_scan_l0_direct`

### `benches/tier3_system/scan_multi_level.rs`

- `bench_scan_multi_level_range`

### `benches/tier3_system/startup_large.rs`

- `bench_engine_startup_100k_sst_files`

### `benches/tier3_system/startup_wal.rs`

- `bench_engine_startup_from_wal`

## Tier 4 Integration Benchmarks

### `benches/tier4_integration/ycsb_workload_a.rs`

- `bench_workload_a`

### `benches/tier4_integration/ycsb_workload_b.rs`

- `bench_workload_b`

### `benches/tier4_integration/ycsb_workload_c.rs`

- `bench_workload_c`

### `benches/tier4_integration/ycsb_workload_d.rs`

- `bench_workload_d`

### `benches/tier4_integration/ycsb_workload_e.rs`

- `bench_workload_e_single_thread`
- `bench_workload_e_multi_thread`
- `bench_workload_e_scan_lengths`

### `benches/tier4_integration/ycsb_workload_f.rs`

- `bench_workload_f`

## Tier 5 Soak Benchmarks

### `benches/tier5_soak/compaction_backlog_growth.rs`

- `bench_compaction_backlog_growth`

### `benches/tier5_soak/level_drift.rs`

- `bench_level_drift`

### `benches/tier5_soak/space_amplification.rs`

- `bench_space_amplification`

## Tier 6 Capacity Benchmarks

### `benches/tier6_capacity/cold_start_large.rs`

- `bench_cold_start_large`

### `benches/tier6_capacity/large_dataset_compaction.rs`

- `bench_large_dataset_compaction`

### `benches/tier6_capacity/large_dataset_insert.rs`

- `bench_large_dataset_insert`

### `benches/tier6_capacity/wal_growth_large.rs`

- `bench_wal_growth_large`
