# Midge Roadmap

**Vision:** Focus this roadmap on work that is still outstanding, so it stays a live queue of what is left to build rather than a history of what we already shipped.

This document only lists **remaining** work. Completed items have been pruned and should be discoverable via Git history and issues.

---

## Phase 1: Correctness & Durability (Remaining)

These are non-blocking for basic use but important to harden long-term durability.

### 1.2 Manifest Durability (Remaining)

**Remaining:**
- [ ] Implement manifest snapshotting (full checkpoint + incremental edits)
	- Design: periodically write a full `Manifest` image to a separate, versioned snapshot file, then append only `VersionEdit`s to the main manifest log. On recovery, load the newest valid snapshot, then replay subsequent edits. Keep the on-disk format backward-compatible and reuse existing serialization code where possible.
- [ ] Add manifest size monitoring and auto-compaction trigger
	- Design: track manifest file size and/or number of edits in memory as they are appended. Expose thresholds in config (e.g., max bytes / max edits). When limits are exceeded, atomically rewrite the manifest as a fresh snapshot (compacted manifest) and switch over readers, ensuring the new file is durable before deleting/archiving the old one.

---

## Phase 2: Performance Foundations (Remaining)

With the core hot paths implemented, this phase tracks the remaining performance work.

### 2.1 WAL Write Path

**Remaining:**
- [ ] Benchmark: target 500K+ single-key writes/sec on NVMe
	- Design: add Criterion benches under `benches/` that exercise single-key writes with realistic options (`sync=BatchedSync`, configured WAL preallocation, small value sizes). Report throughput per configuration and use these benches as the canonical place to track the 500K ops/sec goal.
- [ ] Dynamic re-preallocation when approaching preallocated limit
	- Design: extend the WAL writer to monitor how much of the preallocated region is consumed and, when a threshold (e.g., 75–80%) is reached, grow the underlying file in fixed-size chunks. Avoid frequent `set_len` calls, ensure alignment to filesystem expectations, and keep behavior configurable for different environments.

### 2.2 Memtable

**Remaining:**
- [ ] Implement version chain compaction (collapse old versions on read)
	- Design: when lookups encounter long per-key version chains (many overwrites), opportunistically collapse older versions into a shorter representation while preserving MVCC semantics (respecting snapshots and sequence numbers). Start with a simple heuristic (e.g., cap chain length), measure impact via targeted benches, and avoid adding global locks.
- [ ] Consider hybrid structure: skiplist for small memtables, B-tree for large
	- Design: introduce an experimental mode where memtables begin as a skiplist, and once size or key count crosses a threshold, new inserts go into a B-tree-like structure. Reads consult both structures. Keep this behind a config flag and drive the decision with benchmarks rather than default behavior.

> Note: Arena allocator for skiplist nodes is considered **deferred**; crossbeam-epoch currently provides adequate behavior.

### 2.3 SST Read Path

**Remaining:**
- [ ] Implement block prefetching for sequential scans
	- Design: in range iterators, detect forward-sequential access patterns and asynchronously prefetch the next N blocks (configurable window) using the existing I/O abstraction. Coordinate with the block cache so prefetches populate it instead of bypassing it, and ensure memory usage stays bounded.
- [ ] Add direct I/O option (bypass page cache for large scans)
	- Design: add a per-DB/CF option to open SST files with direct I/O on supported platforms. Enforce alignment and buffer size requirements in the I/O layer, and fall back gracefully when direct I/O is unavailable. This should primarily benefit large, sequential scans.
- [ ] Optimize bloom filter check before block read
	- Design: make the bloom filter check a mandatory, cheap gate before issuing a block read for point lookups. Ensure all read paths (including iterators and block cache misses) go through the bloom check, and validate behavior with negative lookup benches.

### 2.4 Compaction I/O

**Remaining:**
- [ ] Add compaction I/O priority (lower than foreground)
	- Design: use the existing rate limiter and/or OS-level hints (where available) to ensure compaction traffic yields to foreground reads/writes. Start with a separate rate limiter or weight for compaction, then consider platform-specific priorities only if needed.
- [ ] Parallelize compaction input reading (read multiple SSTs concurrently)
	- Design: read from multiple input SSTs in parallel with a bounded worker pool, feeding a merge step that preserves key ordering. Keep memory usage and open file handles under control, and validate correctness under concurrent compactions.
- [ ] Add compaction statistics: read amp, write amp, space amp (metrics already exist, but need surfaced in the API/metrics)
	- Design: standardize the existing compaction statistics into a stable metrics API, export them via the metrics subsystem, and ensure they are tagged by column family and level where possible.

---

## Phase 3: Scalability & Advanced Features

### 3.1 Tiered Compaction Strategy

**Current State:** Leveled compaction only.

**Remaining:**
- [ ] Implement tiered (size-tiered) compaction for write-heavy workloads
	- Design: add a size-tiered picker alongside the existing leveled picker. Group SSTs of similar size into tiers and compact within a tier once it exceeds a configured count, favoring lower write amplification over read amplification. Keep behavior configurable per CF.
- [ ] Add FIFO compaction for TTL-heavy use cases
	- Design: implement a simple FIFO strategy that drops oldest files first, primarily for CFs where TTL makes data naturally short-lived. Integrate with existing TTL/index handling to avoid resurrecting expired keys.
- [ ] Allow per-column-family compaction strategy
- [ ] Add compaction picker scoring and logging

### 3.2 Block Cache

**Current State:** 128MB default, basic LRU.

**Remaining:**
- [ ] Implement clock-based eviction (lower overhead than LRU)
	- Design: replace or augment the current LRU implementation with a clock-based policy that uses a circular buffer and reference bits to reduce per-operation overhead, while maintaining similar hit rates.
- [ ] Add cache partitioning (index vs data blocks)
	- Design: split the cache into logical partitions (e.g., index/metadata vs data blocks) with configurable size ratios, so hot metadata is protected from eviction by bulk data reads.
- [ ] Add compressed block cache option
	- Design: optionally store compressed blocks in cache and decompress on hit, trading CPU for memory. Ensure we only compress when it is a net win (based on block size/entropy) and keep this behind a configuration toggle.
- [ ] Add cache hit/miss metrics per SST level
	- Design: instrument cache lookups with level tags so we can observe hit/miss behavior per level, then export these via the metrics subsystem.

### 3.3 Bloom Filters

**Current State:** Per-SST bloom filter, 10 bits/key default.

**Remaining:**
- [ ] Add configurable `bits_per_key` in `MidgeOptions`
	- Design: surface `bits_per_key` as a tuning parameter per CF, plumb it through SST building, and validate expected false-positive rates via targeted tests/benches.
- [ ] Implement prefix bloom filters for range queries
	- Design: add an optional prefix bloom filter that indexes key prefixes (e.g., user key prefix or shard prefix) to speed up range scans that share common prefixes, ensuring it coexists cleanly with existing point-lookups blooms.
- [ ] Add bloom filter statistics (FPR monitoring)
	- Design: periodically sample lookups and track bloom hits that lead to misses at the SST/block level to estimate false-positive rate, exporting this as a metric per CF/level.
- [ ] Consider ribbon filters for better space efficiency
	- Design: prototype a ribbon filter implementation behind a feature flag and compare space and performance characteristics against the current bloom implementation before considering it for default use.

### 3.4 Column Families

**Current State:** Basic CF support, shared WAL.

**Remaining:**
- [ ] Add per-CF memtable size configuration
	- Design: extend configuration to allow overriding memtable size per CF, with sensible defaults and validation to prevent pathological combinations (e.g., too-small per-CF memtables exploding flush frequency).
- [ ] Add per-CF compression configuration
	- Design: allow each CF to choose its own compression algorithm and level, wired through SST building, while keeping a clear default path for users who don’t need per-CF tuning.
- [ ] Implement atomic cross-CF writes (WriteBatch spanning CFs)
	- Design: ensure `WriteBatch` operations that span CFs are applied atomically across WAL, memtables, and recovery. This likely involves encoding CF information into the WAL entries and committing all or none of the batch on crash/recovery.
- [ ] Add CF-level statistics and metrics
	- Design: expose per-CF metrics for size on disk, write/read throughput, compaction activity, and cache behavior to make CF-specific tuning actionable.

---

## Phase 4: Cloud & Distribution

### 4.1 Cloud Backend Robustness

**Current State:** S3/Azure/GCS/OCI backends. MockCloud for testing.

**Remaining:**
- [ ] Add retry with exponential backoff for transient failures
- [ ] Implement parallel SST upload (multiple parts)
- [ ] Add cloud operation metrics (latency, errors, retries)
- [ ] Implement local SST cache for cloud-backed mode

### 4.2 Backup & Restore

**Current State:** Basic checkpoint support.

**Remaining:**
- [ ] Implement incremental backup (only changed SSTs)
- [ ] Add backup verification (checksum validation)
- [ ] Support backup to cloud storage
- [ ] Add point-in-time recovery (PITR) via WAL replay

---

## Phase 5: Observability & Operations

### 5.1 Metrics & Monitoring

**Current State:** Basic metrics infrastructure.

**Remaining:**
- [ ] Add Prometheus exposition format
- [ ] Implement histogram metrics (latency percentiles)
- [ ] Add per-operation breakdown (get/put/delete/scan)
- [ ] Add internal event tracing (spans for compaction, flush)

### 5.2 Diagnostics & Debugging

**Remaining:**
- [ ] Add `db_stats()` command returning JSON summary
- [ ] Implement SST file inspector tool
- [ ] Add manifest dump utility
- [ ] Add WAL dump utility with corruption detection

### 5.3 Configuration Validation

**Remaining:**
- [ ] Add configuration linting (warn on suboptimal settings)
- [ ] Implement configuration diff (show changes from defaults)
- [ ] Add runtime configuration updates (where safe)
- [ ] Document all configuration options with tuning guidance

---

## Implementation Priority Matrix (Remaining Only)

| Item | Impact | Effort | Priority |
|------|--------|--------|----------|
| Manifest durability (snapshotting, compaction) | High | Medium | **P0** |
| WAL benchmarks + dynamic preallocation | High | Medium | **P1** |
| SST read optimization (prefetch, direct I/O, bloom-before-read) | High | Medium | **P1** |
| Compaction I/O improvements (priority, parallelism, stats) | Medium | Medium | **P1** |
| Memtable follow-ups (version chain compaction, hybrid structure) | Medium | Medium | **P2** |
| Tiered compaction | Medium | High | **P2** |
| Block cache improvements | Medium | Medium | **P2** |
| Cloud robustness | Medium | Medium | **P2** |
| Metrics & monitoring | Medium | Medium | **P3** |
| Backup & restore improvements | Low | Medium | **P3** |
| Diagnostics & debugging tools | Medium | Medium | **P3** |
| Configuration validation | Medium | Low | **P3** |

---

## Success Metrics (Still Targeted)

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

## Working From This Roadmap

When picking up a roadmap item:

1. **Create an issue** linking to the specific bullet in this file.
2. **Write tests first** — especially for correctness and durability changes.
3. **Follow naming conventions** — `should_{action}_when_{context}`.
4. **Run the test suite** — `cargo test` + `cargo run --bin validate_tests`.
5. **Update documentation** where behavior or tuning guidance changes.

Version milestones (v0.2.0, v0.3.0, etc.) are now tracked in the changelog and release notes rather than this file, to keep this document focused on **what is left to do**.
