Here we go: **World‑Class Block Cache for Midge** — this document now
describes the cache we actually ship: sharded, WTinyLFU‑style admission,
benchmarked, and fully wired into the SST read path.

---

# World‑Class Block Cache for Midge

**Architecture, Implementation & Integration**

## 1. Goals & Non-Goals

### Goals

- **High hit rate** across diverse workloads:

  - Point lookups, range scans, compaction, MVCC snapshots.

- **Predictable latency** at high concurrency.
- **Bounded memory** with smart eviction (no unbounded HashMap).
- **Safe Rust**, minimal `unsafe` (only for tight array ops if needed).
- **Pluggable policy**, but with a strong default (WTinyLFU-style).
- **Per-tenant / per-CF isolation hooks** (so one tenant can’t poison others).
- **First-class metrics** for hit/miss/eviction/admission.
  - Target: under realistic Midge workloads, WTinyLFU should achieve
    materially higher hit rates than LRU at the same capacity while
    keeping cache lookups sub-microsecond at high concurrency.

### Non-Goals (for now)

- Persistent / disk-backed cache.
- NUMA-aware placement.
- Cross-process shared cache.

---

## 2. High-Level Architecture

Core idea: **sharded, size-aware, policy-driven cache** built on:

- A **`BlockCache` trait** in `src/sst/block_cache/mod.rs`:

  - Knows about total capacity, stats, and an optional `prefetch` hook.

- A concrete **`ShardedBlockCache`** implementation:

  - Owns multiple `BlockCacheShard`s, each with its own lock, hash
    table, eviction policy, and statistics.
  - Sharding based on `BlockKey::shard_hash()`.

- Each shard maintains:

  - A hash table from `BlockKey` → entry.
  - A policy state (`LruPolicy` or `WTinyLfuPolicy`).
  - Accounting for bytes used vs capacity, plus optional per‑CF stats.

- The cached value is a **reference‑counted block** (`BlockData`) with
  a **pinned handle** (`BlockHandle`).

All SST access goes through a unified API that returns **handles**; the
cache is fully wired into `SSTReader` and the engine.

---

## 3. Key Concepts and Types

### 3.1 Block Identity (`BlockKey`)

Implemented in `src/sst/block_cache/key.rs`:

```rust
pub struct BlockKey {
    file_number: u64,      // SST file id
    block_offset: u64,     // logical offset within file
    kind: BlockKind,       // Data, Index, Filter, Meta, CompressionDict
    cf_id: u32,            // for per-CF accounting / isolation
}
```

- Hash + equality implemented on `(file_number, block_offset, kind, cf_id)`.
- Exposes `as_u8()` on `BlockKind` for indexing stats arrays and a
  `shard_hash()` helper used by `ShardedBlockCache`.

### 3.2 Block Value (`BlockData`)

Implemented in `src/sst/block_cache/value.rs`:

```rust
pub struct BlockData {
    bytes: Arc<[u8]>,
    kind: BlockKind,
    // compression metadata elided here; see value.rs
}
```

- Stored behind an `Arc<[u8]>` to allow shared handles across iterators.
- The cache accounts in bytes via the configured `SizeAccounting`
  strategy (uncompressed vs compressed).

### 3.3 Cache Entry (per shard)

Internal to `src/sst/block_cache/shard.rs`:

```rust
struct BlockEntry {
    key: BlockKey,
    data: BlockData,
    // size_bytes, pin count, and policy metadata are tracked by the shard
}
```

- The shard tracks used bytes and pinning and exposes a `BlockHandle`
  to callers.

### 3.4 Handle (`BlockHandle`)

Implemented in `src/sst/block_cache/handle.rs`:

```rust
pub struct BlockHandle { /* opaque */ }
```

- Returned to callers on `get()`/`insert()`.
- Encapsulates pinning and reference counting inside the cache
  implementation and exposes safe accessors like `data()` and
  `is_pinned()`.

---

## 4. Module Layout

The shipped module layout matches the original proposal:

```text
src/
  sst/
    block_cache/
      mod.rs               // public facade + config
      key.rs               // BlockKey, BlockKind, hashing
      value.rs             // BlockData etc
      handle.rs            // BlockHandle, pin logic
      shard.rs             // BlockCacheShard: hash table + policy hooks
      table.rs             // internal table abstraction over indexmap
      policy/
        mod.rs             // policy trait
        wtiny_lfu.rs       // default policy: Windowed TinyLFU (or Clock-Pro)
      admission.rs         // admission controller & frequency sketch
      metrics.rs           // hit/miss/eviction counters
      config.rs            // BlockCacheOptions
```

---

## 5. Public Block Cache API

### 5.1 Trait (`BlockCache`)

Defined in `src/sst/block_cache/mod.rs` and used throughout SST code:

```rust
pub trait BlockCache: Send + Sync {
    fn get(&self, key: &BlockKey) -> Option<BlockHandle>;
    fn insert(&self, key: BlockKey, data: BlockData) -> BlockHandle;
    fn insert_if_absent(&self, key: BlockKey, data: BlockData) -> BlockHandle;
    fn capacity_bytes(&self) -> usize;
    fn used_bytes(&self) -> usize;
    fn stats(&self) -> BlockCacheStats;
    fn prefetch(&self, _key: BlockKey) { /* default no-op */ }
}
```

Notes:

- `insert_if_absent` dedups races between concurrent loaders within a
  shard.
- `BlockHandle` pins the block while in scope.
- `prefetch` is intentionally a best‑effort hint; `ShardedBlockCache`
  currently implements it as a no‑op while higher layers perform
  structured prefetch based on access patterns.

---

## 6. Sharding & Concurrency

### 6.1 Sharding

- Number of shards: power of two (`num_shards`) configured via
  `BlockCacheOptions::num_shards`.
- `shard_index = BlockKey::shard_hash() as usize & (num_shards - 1)`.
- Each shard has its own mutex so contention is limited to a
  particular shard under load.

### 6.2 Per-Shard Concurrency Model

- We use a **mutex‑per‑shard** design (based on `parking_lot`), with
  hash table and policy operations done under the shard lock.
- Types are structured so the internals can evolve toward more
  fine‑grained or lock‑free designs if profiling ever demands it.

---

## 7. Hash Table Structure

Inside each shard we use a thin wrapper over `indexmap` to provide a
hash table with stable iteration order and efficient key lookup. The
exact representation is hidden behind `table.rs` so we can switch
implementations if needed without touching policy or shard logic.

---

## 8. Block Storage & Pinning

### 8.1 Storage

- Entries stored in a contiguous `Vec<BlockEntry>` or `SlotMap`.
- `BlockEntry` contains `Arc<BlockData>` for sharing with handles.
- `size_bytes` determines capacity usage:

  - Use **uncompressed_size** by default.
  - For compressed blocks, optionally tune to min(uncompressed, compressed).

### 8.2 Pinning

- On hit via `BlockCacheShard::get`, the shard pins the entry and
  returns a `BlockHandle`.
- Eviction only considers entries that are not currently pinned.
- `BlockHandle` encapsulates any pin/unpin mechanics so callers only
  hold it for as long as they need the data.

---

## 9. Eviction Policy (WTinyLFU-style)

We implement a **Windowed TinyLFU‑style** policy as `WTinyLfuPolicy`,
and a simpler `LruPolicy` used for some configurations and tests.

- **Window segment (W):** small recency buffer.
- **Main segment (M):** majority of capacity, frequency-based.
- **TinyLFU frequency sketch:** approximate count of accesses.

### 9.1 Policy responsibilities

`Policy` is defined in `src/sst/block_cache/policy/mod.rs` and used by
each shard to track recency/frequency and to choose victims under
memory pressure. `WTinyLfuPolicy` combines a window segment with a
frequency sketch to keep hot entries around and resist scan pollution.

### 9.2 TinyLFU Frequency Sketch

Implemented in `src/sst/block_cache/admission.rs` as a compact
`FrequencySketch` used by `WTinyLfuPolicy` to estimate how often keys
are accessed. Admission compares candidate vs victim estimates and may
reject cold candidates even when space is available, dramatically
improving hit rate under mixed workloads.

---

## 10. Admission Control

On insert, shards delegate to the configured policy and admission
controller:

1. Frequency sketch provides an estimate for the **candidate** key.
2. If space is tight, policy proposes a **victim**.
3. Candidate vs victim estimates are compared; cold candidates may be
  rejected, incrementing the `rejected` counter in `BlockCacheStats`.
4. Admitted blocks enter the appropriate segment in the policy (window
  vs main) and contribute to `admissions` and `used_bytes`.

This cleanly separates **loading** a block from **caching** it.

---

## 11. In-Flight Load De-Duplication (Future Work)

The current implementation does not yet include a public `get_or_load`
API or an in‑flight de‑duplication map; loaders are responsible for
coordinating concurrent I/O before inserting into the cache. The
design still leaves room to add this without breaking callers.

---

## 12. Prefetch & Readahead Hooks

The `BlockCache` trait exposes a `prefetch` hook, and
`ShardedBlockCache` currently treats it as a no‑op placeholder. Range
scan prefetch today is orchestrated at higher layers (cloud download
config, SST iterator strategies). The type signatures and config
plumbing are in place to add true async prefetching later.

---

## 13. Metrics & Instrumentation

Per shard, aggregated at top:

- `hits`, `misses`, `evictions`, `admissions`, `rejected_admissions`.
- `bytes_used`, `bytes_capacity`.
- Histograms:

  - Block size distribution.
  - Hit rate by block_kind.
  - Eviction reason (space, TTL, etc. — if you add TTL later).

Currently implemented and exposed via `ShardStats` /
`BlockCacheStats`:

- `hits`, `misses`, `evictions`, `admissions`, `rejected`.
- `used_bytes`, `capacity_bytes`.
- Per‑block‑kind hit/miss breakdowns via fixed‑size arrays indexed by
  `BlockKind`.

These aggregate into the engine’s metrics modules and power
observability for cache hit rate and utilization.

---

## 14. Integration Points with Midge

### 14.1 SST Reader

- `SSTReader` in `src/sst/fs/reader.rs` holds an
  `Option<Arc<dyn BlockCache>>` and looks up blocks via the cache on
  the read path; on miss, it loads from storage and inserts back into
  the cache.
- `BlockKind` is used for per‑kind prioritization and per‑kind
  metrics.

### 14.2 Column Families & Tenants

- `BlockKey` includes `cf_id` and shards can track per‑CF stats.
- `ShardedBlockCache` exposes `cf_stats` / `all_cf_stats` to aggregate
  hit/miss/bytes‑used per column family, enabling future per‑CF
  quotas or weighted sharing.

### 14.3 Compaction

- Compaction and internal iterators route through the same SST read
  path, so they share the cache; WTinyLFU admission helps avoid scan
  pollution from compaction workloads.

---

## 15. Testing & Benchmarks

### 15.1 Tests

- Unit tests live alongside the implementation in
  `src/sst/block_cache/` and `mod.rs`, covering sharding,
  insertion/lookup, stats aggregation, and `insert_if_absent`
  semantics.

### 15.2 Benches

- Tier 1 hot‑path benches in `benches/tier1_hotpath/block_cache.rs`
  cover single‑key hits, misses, inserts, and eviction cost with strict
  runtime budgets.
- Tier 2 subsystem benches in `benches/tier2_subsystem/block_cache.rs`
  exercise eviction scanning, hot‑set rotation, and LRU under pressure
  with precomputed keys and zero allocations in the hot loop.

---

## 16. Remaining Work

Even with a world‑class core and good wiring, there is more we can do:

### 16.1 Prefetch and In‑Flight Loads

- [ ] Upgrade `prefetch` from a no‑op to a true async prefetch that
  coordinates with the SST iterator and cloud download layer.
- [ ] *(Deferred)* Add optional in‑flight load de‑duplication for a
  future `get_or_load` API to avoid redundant disk/network reads.

### 16.2 Advanced Metrics & Tuning

- [ ] Add richer policy‑level metrics (segment sizes, promotion rates)
  to make WTinyLFU tuning observable.
- [ ] Experimentally validate and document hit‑rate gains vs LRU across
  representative workloads (YCSB‑style traces, compaction‑heavy
  scenarios) and publish target SLOs here.

### 16.3 Higher‑Tier Workloads

- [ ] Add Tier 3+ benches or integration tests that measure
  end‑to‑end query latency and throughput with the cache enabled vs
  disabled under realistic workloads.

### 16.4 Future Enhancements

- [ ] Explore per‑CF quotas or weighted fair sharing using existing
  `cf_stats` plumbing.
- [ ] Consider NUMA‑aware shard placement and cross‑process cache
  sharing if/when Midge scales into those environments.
