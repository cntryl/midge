# NOTE: Work product; keep at repo root per team guidance — DO NOT put in `docs/`.
// STATUS: 1) Per-block bloom upgrade — COMPLETED ✅; 2) Block summary footer — COMPLETED ✅
// Next action: 3) Adaptive Bloom Sizing — IN PROGRESS (see notes below)
# Phase 1 — Guaranteed Wins (Short TODOs)

This document lists the prioritized, surgical, high-ROI Phase 1 tasks and concrete implementation suggestions.

## 1) Per-block bloom upgrade (COMPLETED)

Files:
- `src/sst/block_meta.rs`
- `src/sst/bloom.rs`
- `src/sst/fs/reader.rs`
- `src/sst/writer_common.rs`

Implementation steps:
1. Add `k: u8` to `BlockBloom` and default to `5`.
2. Replace simple multiplicative `hash` with `xxh3_64` with `HASH_SEED`.
3. Implement `double_hash` (Kirsch-Mitzenmacher) to derive `k` probe positions.
4. Keep `encode`/`decode` wire format the same (same capacity bytes layout).
5. Add unit tests for FPR and wire compatibility.

Benchmarks:
- `benches/tier1_hotpath/bloom.rs`
- `tests/per_block_bloom_tests.rs`

Suggested PR validations:
- `cargo test` and `cargo bench` before/after
- Confirm FPR / miss-rate improvements on bench

## 2) Persist Block Summary Footer (COMPLETED)

Files:
- `src/sst/format.rs`
- `src/sst/writer_common.rs`
- `src/sst/fs/reader.rs`
- `src/sst/block_meta.rs`
- `src/sst/meta_index.rs`

Implementation steps:
1. Create `BlockSummary { min_key, max_key, key_count, bloom_offset }` type.
2. Update writer to collect min_key/key_count per block and append a `BlockSummary` block to metaindex.
3. Add optional footer reference in `Footer` for the block summary index.
4. Update SST open path to prefer reading the persisted summary and fallback to reconstruction, maintaining backwards compatibility.
5. Add tests covering legacy_sst (no summary) and new_sst (with summary).

Benchmarks:
- `benches/tier3_system/compaction.rs`
- `tests/streaming_fence_pointer_skipping.rs`

## 3) Adaptive Bloom Sizing (IN PROGRESS)

Files:
- `src/sst/writer_common.rs`
- `src/config.rs` (or similar config module)
- `src/sst/bloom.rs`

Implementation steps:
1. Add `level: u8` param to writer or config path.
2. Provide helper to compute bits_per_key based on `level` and `query_type`.
3. Allow a manual override in `WriterConfig`.
4. Add tests and adjust writer tests to validate different bits/key sizes.

Benchmarks:
- `benches/tier3_system/engine_basic.rs`

Notes & implementation plan (next steps):
- Add `WriterConfig::with_bloom_policy(level: u8)` and helper `WriterConfig::compute_bits_for_level(level)`.
- Default policy: L0/L1 -> 14bpk, L2-L4 -> 10bpk, L5+ -> 8bpk.
- Add a small config flag `writer.adaptive_bloom(true)` to opt in.
- Add unit test cases for level distribution and runtime benches comparing L0 latency.

PR Checklist (Adaptive Bloom Sizing):
- Branch name: `feat/adaptive-bloom-sizing`.
- Add `WriterConfig::bloom_policy` enum and helper methods.
- Update `WriterState` instantiation to compute `bloom_bits_per_key` using policy.
- Update `SstWriter` factory functions that accept a `level` to pass the right policy.
- Add unit tests verifying default policies, override behavior, and config round-trips.
- Add or update benches to measure FPR and L0 latency under adaptive policy.

## 4) In-memory Index Compression (Prefix/Deltas)

Files:
- `src/sst/block_meta.rs` (IndexTable)
- `src/sst/sparse_index.rs` (builder/encoder)
- `src/sst/sparse_index_cache.rs` (cache storage)

Implementation steps:
1. Implement prefix compression for `IndexTable::search_keys`.
2. Add delta/varint length encoding for on-disk representation of index keys (compact encoding).
3. Ensure decode path reconstructs keys and maintains compatibility.
4. Add bench validating memory reduction.

Benchmarks:
- `benches/tier3_system/engine_basic.rs`

## 5) Bloom Cache Warming

Files:
- `src/sst/bloom_cache.rs`
- `src/sst/table_cache.rs`

Implementation steps:
1. Add `warm_bloom_cache(manifest)` to `BloomCache` that preloads L0/L1 blooms.
2. Add heuristics for when to pin blooms in memory (pin hot L0/L1 only).
3. Add tests for `populate_from_manifest` and warm sweeps.

Benchmarks:
- `benches/tier3_system/engine_basic.rs` (latency tail measurements)

## 6) Tighten Tombstone Fast-Path

Files:
- `src/sst/tombstone_index.rs`
- `src/sst/fs/reader.rs`
- `src/sst/block_meta.rs`

Implementation steps:
1. Ensure tombstone index `entries` sorted by `min_key`.
2. Use `partition_point`/binary search to find candidate block(s) for a key.
3. Add early-check using tombstone binary search before per-block reads.
4. Add tests for compaction and range scans.

Benchmarks:
- `benches/tier3_system/compaction.rs`

## 7) SIMD Probe Path (Optional / Batch Only)

Files:
- `src/sst/bloom.rs`
- `benches/**` (create new batch bench for vectorized tests)

Implementation steps:
1. Add `query_blocked_simd` under `#[cfg(target_feature = "avx2")]`.
2. Use `portabled_simd` or intrinsics to perform per-probe vector checks.
3. Guard logic with `is_avx2` detection and only use for batch API paths.
4. Fall back to scalar probe for single key lookups.

Benchmarks:
- `benches/tier1_hotpath/bloom.rs` (batch probe variant)

---

### General PR checklist
- Unit + integration tests included
- Benchmarks updated or new bench included
- CI passes (`cargo test`) and `cargo bench` sanity
- Documentation update (SST_INDEX_BLOOM_ANALYSIS.md)

---

