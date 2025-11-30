# Midge Roadmap

**Vision:** Transform Midge into the fastest, most reliable embedded LSM-tree database engine.

This roadmap is organized into phases, prioritizing correctness and durability first, then performance, then advanced features. Each item includes effort estimates and dependencies.

---

## Phase 1: Correctness & Durability (Critical)

These issues must be fixed before any production use. They affect data integrity.

### 1.1 Fix Merge Operator Persistence ✅ COMPLETED

**Problem:** Merge operands were being converted to Put entries during compaction, losing merge semantics.

**Root Cause:** `CompactionVersion` struct lacked `op_type` field. The compaction executor discarded `op_type` when collecting versions from SSTs and reconstructed it incorrectly as `if tombstone { 2 } else { 0 }`.

**Fix Applied:**
- [x] SST encoding/decoding already supported `entry_type=3` for merge operands
- [x] Flush path already correctly passed `entry.op_type.as_u8()` to SST writer
- [x] Added `op_type: u8` field to `CompactionVersion` struct
- [x] Updated `collect_compaction_versions()` to capture `op_type` from `KeyState`
- [x] Updated SST writing to use `entry.op_type` instead of recomputing from tombstone flag
- [x] All 21 merge operator tests pass, including `should_preserve_merge_semantics_across_restart_given_flush_when_recovering`

**Files Changed:** `src/core/compaction/executor.rs`, `src/core/compaction/filter.rs`

### 1.2 Manifest Durability Audit ✅ PARTIALLY COMPLETED

**Problem:** Each `VersionEdit` triggers a separate manifest write. No batching, no compaction.

**Fix Applied:**
- [x] Batch multiple edits in compaction (AddFile + RemoveFiles + UpdateSequence as single write)
  - Extended `CombinedAddRemove` to include optional `sequence` field
  - Compaction now uses single manifest write instead of two
  - Flush path also batches AddFile + sequence in single write
- [x] Verify fsync ordering: manifest must be durable before SST deletion
  - Already implemented in `save_atomic_with_hooks()` with proper fsync ordering

**Remaining:**
- [ ] Implement manifest snapshotting (full checkpoint + incremental edits) - Future optimization
- [ ] Add manifest size monitoring and auto-compaction trigger - Future optimization

**Files Changed:** `src/core/manifest/version_set.rs`, `src/core/compaction/controller.rs`, `src/core/engine/operations/maintenance.rs`

### 1.3 WAL Recovery Edge Cases ✅ COMPLETED

**Problem:** `TolerateCorruptedTail` mode may silently lose data if corruption is in the middle.

**Fix Applied:**
- [x] Added `SkipAnyCorruptedRecord` mode for maximum recovery
  - New mode continues past any corrupted record, recovering all valid records
  - Useful for disaster recovery scenarios
- [x] Added `WalRecoveryStats` struct for recovery metrics:
  - `files_processed`: Number of WAL files processed
  - `records_recovered`: Successfully recovered records
  - `records_skipped`: Records skipped due to corruption
  - `bytes_recovered` / `bytes_skipped`: Data accounting
  - `corruption_locations`: File offsets of detected corruption
  - `had_corruption`: Boolean flag for quick corruption check
- [x] Added `replay_wal_file_with_stats()` function for detailed recovery information
- [x] Added tests for all recovery modes:
  - `should_skip_corrupted_record_given_skip_mode_when_crc_mismatch`
  - `should_return_stats_given_clean_wal_when_replaying`
  - `should_fail_given_corruption_when_absolute_consistency_mode`

**Remaining (Future):**
- [ ] Fuzz test WAL recovery with random truncation/corruption patterns

**Files Changed:** `src/wal/fs/writer.rs`, `src/wal/types.rs`, `src/config/options.rs`

### 1.4 Crash Consistency Test Suite ✅ COMPLETED

**Status:** Already implemented via comprehensive test infrastructure.

**Existing Implementation:**
- [x] `MockCloudBackend` provides deterministic failure injection (`set_fail_upload_after`)
- [x] `TestHooks` infrastructure supports:
  - `FsyncBehavior::Skip` - simulate crash before fsync
  - `WalBehavior::TruncateAfterWrite` - simulate torn writes
  - `ManifestBehavior::FailSave` - fail manifest updates
  - `CompactionBehavior::FailMidway/CrashBeforeFsync` - crash during compaction
  - `IoBehavior::FailWithEnospc/FailWithEio` - disk errors
  - `CompactionGatePoint` and `FlushGatePoint` for deterministic pause points
- [x] Crash-recovery tests in `tests/durability_recovery.rs`:
  - `should_recover_unflushed_data_given_crash_during_flush_when_reopening`
  - `should_preserve_consistency_given_crash_before_manifest_update_when_reopening`
- [x] Fault injection tests in `tests/fault_injection.rs`:
  - `should_survive_recovery_given_skip_fsync_when_reopening`
  - `should_recover_to_last_valid_record_given_truncated_wal_when_reopening`
  - `should_recover_given_compaction_failure_midway_when_reopening`
  - `should_recover_given_crash_during_compaction_with_pending_wal_when_reopening`
- [x] Cloud durability tests in `tests/cloud_durability.rs`:
  - `should_recover_consistently_given_partial_cloud_sst_upload_when_local_manifest_was_already_updated`
  - `should_not_poison_wal_startup_given_fail_upload_after_is_armed_post_open`

**Files:** `tests/durability_recovery.rs`, `tests/fault_injection.rs`, `tests/cloud_durability.rs`, `src/common/test_hooks.rs`, `src/cloud/mock.rs`

---

## Phase 2: Performance Foundations

With correctness assured, optimize the hot paths.

### 2.1 WAL Write Path Optimization ✅ PARTIALLY COMPLETED

**Current State:** Group commit works well. Parallel encode scales. io_uring feature-gated.

**Completed:**
- [x] io_uring write path for Linux (feature-gated with `--features uring`)
  - Implemented `write_vectored_uring_with_hooks()` with test hook support
  - Uses thread-local IoUring instances for low overhead
  - Proper error handling and I/O failure injection support
- [x] Write buffer pooling (avoid per-batch allocations)
  - `WalInner.scratch` Arena reused across writes (256KB page-aligned buffer)
  - `encode_batch_arena()` uses contiguous buffer for zero-copy vectored I/O
  - Thread-local buffers (`TLS_BUF`) in parallel encode path
- [x] WAL file pre-allocation (reduce metadata updates)
  - Auto-preallocates 64MB on WAL open using `posix_fallocate` (Unix) or `set_len` (Windows)
  - Reduces filesystem metadata updates during sequential writes
  - Added `preallocate()` public API and `needs_repreallocation()` helper

**Remaining:**
- [ ] Benchmark: target 500K+ single-key writes/sec on NVMe
- [ ] Dynamic re-preallocation when approaching preallocated limit

**Files Changed:** `src/wal/fs/writer.rs`, `src/fs/uring.rs`, `src/fs/io.rs`

### 2.2 Memtable Optimization — PARTIALLY COMPLETED

**Current State:** Lock-free skiplist with MVCC. Good concurrency.

**Improvements:**
- [ ] Add arena allocator for skiplist nodes (reduce malloc pressure) — *Deferred: crossbeam-epoch provides adequate memory reclamation*
- [ ] Implement version chain compaction (collapse old versions on read)
- [x] Add bloom filter hint for point lookups (skip scan if key absent) — **8x speedup for negative lookups!**
- [ ] Consider hybrid: skiplist for small memtables, B-tree for large

**Completed Work:**
- Added concurrent `BloomHint` filter in `src/core/memtable/bloom_hint.rs`
- `MemTable::with_bloom_hint(expected_keys)` constructor for opt-in optimization
- Bloom filter uses atomic bit operations for lock-free concurrent add/query
- Benchmark shows **~74ns → ~9ns** per negative lookup (8x improvement)

**Effort:** 3-4 days | **Files:** `src/core/data_structures/skiplist.rs`, `src/core/memtable/core.rs`, `src/core/memtable/bloom_hint.rs`

### 2.3 SST Read Path Optimization — PARTIALLY COMPLETED

**Current State:** Per-block file open. No prefetching.

**Improvements:**
- [x] Cache file handles per SST (avoid repeated open/close)
- [ ] Implement block prefetching for sequential scans
- [ ] Add direct I/O option (bypass page cache for large scans)
- [ ] Optimize bloom filter check before block read

**Completed Work:**
- Added cached file handle to `SstFile` using `parking_lot::Mutex<Option<File>>`
- `get_or_open_file()` lazily opens file on first read, reuses for subsequent reads
- Added cached file handle to `SstRangeIter` for efficient sequential block reads
- Eliminates per-block file open/close overhead during range scans

**Effort:** 3-4 days | **Files:** `src/sst/fs/iterator.rs`, `src/sst/fs/reader.rs`

### 2.4 Compaction I/O Optimization

**Current State:** Rate limiting implemented. No I/O priority support.

**Completed:**
- [x] Compaction rate limiter (bytes/sec cap) via `MidgeOptions::compaction_rate_limiter`
  - Throttles both SST reads (before collection) and writes (after SST write)
  - Uses existing `RateLimiter` (token bucket algorithm)
  - Optional - `None` by default for backward compatibility

**Remaining:**
- [ ] Add compaction I/O priority (lower than foreground)
- [ ] Parallelize compaction input reading (read multiple SSTs concurrently)
- [ ] Add compaction statistics: read amp, write amp, space amp (metrics already exist)

**Effort:** 3-4 days | **Files:** `src/core/compaction/controller.rs`, `src/core/compaction/executor.rs`

---

## Phase 3: Scalability & Advanced Features

### 3.1 Tiered Compaction Strategy

**Current State:** Leveled compaction only.

**Improvements:**
- [ ] Implement tiered (size-tiered) compaction for write-heavy workloads
- [ ] Add FIFO compaction for TTL-heavy use cases
- [ ] Allow per-column-family compaction strategy
- [ ] Add compaction picker scoring and logging

**Effort:** 5-7 days | **Files:** `src/core/compaction/picker.rs`, `src/core/compaction/strategy.rs`

### 3.2 Block Cache Improvements

**Current State:** 128MB default, basic LRU.

**Improvements:**
- [ ] Implement clock-based eviction (lower overhead than LRU)
- [ ] Add cache partitioning (index vs data blocks)
- [ ] Add compressed block cache option
- [ ] Add cache hit/miss metrics per SST level

**Effort:** 3-4 days | **Files:** `src/core/block_cache/mod.rs`

### 3.3 Bloom Filter Enhancements

**Current State:** Per-SST bloom filter, 10 bits/key default.

**Improvements:**
- [ ] Add configurable bits_per_key in MidgeOptions
- [ ] Implement prefix bloom filters for range queries
- [ ] Add bloom filter statistics (FPR monitoring)
- [ ] Consider ribbon filters for better space efficiency

**Effort:** 2-3 days | **Files:** `src/sst/bloom.rs`, `src/config/options.rs`

### 3.4 Column Family Improvements

**Current State:** Basic CF support, shared WAL.

**Improvements:**
- [ ] Add per-CF memtable size configuration
- [ ] Add per-CF compression configuration
- [ ] Implement atomic cross-CF writes (WriteBatch spanning CFs)
- [ ] Add CF-level statistics and metrics

**Effort:** 3-4 days | **Files:** `src/api/column_family.rs`, `src/core/engine.rs`

---

## Phase 4: Cloud & Distribution

### 4.1 Cloud Backend Robustness

**Current State:** S3/Azure/GCS/OCI backends. MockCloud for testing.

**Improvements:**
- [ ] Add retry with exponential backoff for transient failures
- [ ] Implement parallel SST upload (multiple parts)
- [ ] Add cloud operation metrics (latency, errors, retries)
- [ ] Implement local SST cache for cloud-backed mode

**Effort:** 4-5 days | **Files:** `src/cloud/*.rs`

### 4.2 Backup & Restore

**Current State:** Basic checkpoint support.

**Improvements:**
- [ ] Implement incremental backup (only changed SSTs)
- [ ] Add backup verification (checksum validation)
- [ ] Support backup to cloud storage
- [ ] Add point-in-time recovery (PITR) via WAL replay

**Effort:** 4-5 days | **Files:** `src/api/backup.rs`, `src/core/checkpoint.rs`

---

## Phase 5: Observability & Operations

### 5.1 Metrics & Monitoring

**Current State:** Basic metrics infrastructure.

**Improvements:**
- [ ] Add Prometheus exposition format
- [ ] Implement histogram metrics (latency percentiles)
- [ ] Add per-operation breakdown (get/put/delete/scan)
- [ ] Add internal event tracing (spans for compaction, flush)

**Effort:** 3-4 days | **Files:** `src/metrics/*.rs`

### 5.2 Diagnostics & Debugging

**Improvements:**
- [ ] Add `db_stats()` command returning JSON summary
- [ ] Implement SST file inspector tool
- [ ] Add manifest dump utility
- [ ] Add WAL dump utility with corruption detection

**Effort:** 2-3 days | **Files:** `src/api/admin.rs`, `scripts/`

### 5.3 Configuration Validation

**Improvements:**
- [ ] Add configuration linting (warn on suboptimal settings)
- [ ] Implement configuration diff (show changes from defaults)
- [ ] Add runtime configuration updates (where safe)
- [ ] Document all configuration options with tuning guidance

**Effort:** 2 days | **Files:** `src/config/options.rs`, `docs/`

---

## Implementation Priority Matrix

| Item | Impact | Effort | Priority |
|------|--------|--------|----------|
| Merge operator persistence | Critical | Medium | **P0** |
| Manifest durability | High | Medium | **P0** |
| WAL recovery edge cases | High | Low | **P0** |
| Crash consistency tests | High | Medium | **P0** |
| WAL io_uring | High | Medium | P1 |
| SST read optimization | High | Medium | P1 |
| Compaction rate limiter | Medium | Low | P1 |
| Memtable optimization | Medium | Medium | P2 |
| Tiered compaction | Medium | High | P2 |
| Block cache improvements | Medium | Medium | P2 |
| Cloud robustness | Medium | Medium | P2 |
| Metrics & monitoring | Medium | Medium | P3 |
| Backup improvements | Low | Medium | P3 |

---

## Success Metrics

### Correctness
- [ ] Zero data loss in crash-recovery tests (1000+ iterations)
- [ ] Merge operator semantics preserved across all storage modes
- [ ] All integration tests pass with fault injection enabled

### Performance
- [ ] Single-key write: 500K+ ops/sec (NVMe, sync=BatchedSync)
- [ ] Single-key read: 1M+ ops/sec (hot cache)
- [ ] Range scan: 100MB+/sec throughput
- [ ] Write amplification < 15x for leveled compaction

### Reliability
- [ ] 99.9% availability under compaction load
- [ ] Graceful degradation under memory pressure
- [ ] Recovery time < 10 seconds for 10GB database

---

## Contributing

When working on roadmap items:

1. **Create an issue** linking to this roadmap item
2. **Write tests first** — especially for correctness fixes
3. **Follow naming conventions** — `should_{action}_when_{context}`
4. **Run full test suite** — `cargo test` + `cargo run --bin validate_tests`
5. **Update documentation** — especially for new configuration options

---

## Version Milestones

### v0.2.0 — Correctness Release
- All Phase 1 items complete
- Merge operator bug fixed
- Crash consistency test suite passing

### v0.3.0 — Performance Release
- All Phase 2 items complete
- WAL io_uring support (Linux)
- Compaction rate limiting

### v0.4.0 — Feature Release
- Tiered compaction strategy
- Enhanced block cache
- Cloud backend robustness

### v1.0.0 — Production Ready
- All phases complete
- Performance benchmarks published
- Production deployment guide
- Long-term support commitment
