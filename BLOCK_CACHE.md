Here we go: “World Class Block Cache for Midge” as an actual design doc, not just vibes.

---

# World Class Block Cache for Midge

**Architecture & Data Structures**

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

- A **BlockCache** facade:

  - Knows about total capacity, sharding, metrics, policy configuration.

- Multiple **BlockCacheShard**s:

  - Each shard owns its own hash table, eviction policy, and statistics.
  - Sharding based on `BlockKey` hash.

- Each shard maintains:

  - A **hash table** from `BlockKey` → `BlockEntry`.
  - A **policy state** (WTinyLFU / Clock-Pro / Segmented LRU hybrid).
  - Accounting for bytes used vs capacity.

- The cached value is a **reference-counted block** with metadata and a **pin count**.

All access goes through a unified API that returns **handles** which keep blocks pinned while in use.

---

## 3. Key Concepts and Types

### 3.1 Block Identity

```rust
// Conceptual, not exact code
struct BlockKey {
    file_number: u64,         // SST file id
    block_offset: u32,        // logical offset within file
    block_kind: BlockKind,    // Data, Index, Filter, Meta, CompressionDict
    cf_id: ColumnFamilyId,    // for per-CF accounting / isolation
}
```

- Hash + equality implemented on `(file_number, block_offset, block_kind, cf_id)`.
- Used as the canonical key for the cache.

### 3.2 Block Value

```rust
struct BlockData {
    bytes: Arc<[u8]>,         // block payload
    compressed: bool,
    uncompressed_size: u32,
    compressed_size: u32,
    block_kind: BlockKind,
}
```

- Stored behind an `Arc` to allow shared handles across iterators.
- The cache accounts in **bytes** (choose uncompressed_size for memory, but keep compressed_size for policy).

### 3.3 Cache Entry

```rust
struct BlockEntry {
    key: BlockKey,
    value: Arc<BlockData>,
    size_bytes: usize,        // charge against capacity
    pins: u32,                // current pin count
    policy_meta: PolicyMeta,  // eviction metadata (LRU pos, clock ref, etc.)
}
```

- `pins` > 0 ⇒ entry is **not evictable**.
- `PolicyMeta` is opaque to core, used by the policy module.

### 3.4 Handle

```rust
struct BlockHandle {
    value: Arc<BlockData>,
    // drop impl decrements pin in owning shard
}
```

- Returned to callers on `get()`/`insert()`.
- While any handle exists, the entry is considered in use (or at least pinned until handle drop triggers pin release).

---

## 4. Module Layout

Suggested multi-file module:

```text
src/
  sst/
    block_cache/
      mod.rs               // public facade + config
      key.rs               // BlockKey, BlockKind, hashing
      value.rs             // BlockData etc
      handle.rs            // BlockHandle, pin logic
      shard.rs             // BlockCacheShard: hash table + policy hooks
      table.rs             // internal hash table implementation
      policy/
        mod.rs             // policy trait
        wtiny_lfu.rs       // default policy: Windowed TinyLFU (or Clock-Pro)
      admission.rs         // admission controller & frequency sketch
      metrics.rs           // hit/miss/eviction counters
      config.rs            // BlockCacheOptions
```

---

## 5. Public Block Cache API

### 5.1 Trait

```rust
pub trait BlockCache {
    fn get(&self, key: &BlockKey) -> Option<BlockHandle>;
    fn insert(&self, key: BlockKey, block: BlockData) -> BlockHandle;
    fn insert_if_absent(&self, key: BlockKey, block: BlockData) -> BlockHandle;
    fn capacity_bytes(&self) -> usize;
    fn used_bytes(&self) -> usize;
    fn stats(&self) -> BlockCacheStats;
}
```

Notes:

- `insert_if_absent` dedups races between concurrent loaders.
- `BlockHandle` pins the block until dropped.
- Optional future extension: async `get_or_load` that takes a loader closure.

---

## 6. Sharding & Concurrency

### 6.1 Sharding

- Number of shards: power of two (`shards = 16, 32, 64`).
- `shard_index = hash(BlockKey) & (shards - 1)`.
- Each shard has its own lock (or lock-free structure) so independent contention.

### 6.2 Per-Shard Concurrency Model

Two realistic options:

1. **Mutex-per-shard** (simpler, still high-performance in practice):

   - A `parking_lot::Mutex` around `BlockCacheShardInner`.
   - Hash table + policy ops are done under this lock.
   - Good enough for 16–32 shards.

2. **Fine-grained** (more complex, later iteration):

   - Per-bucket locks or lock-free table.
   - Policy metadata updated atomically.

Spec: Start with **Mutex-per-shard**, but design types so we can swap implementation later.

---

## 7. Hash Table Structure

Inside each shard:

```rust
struct BlockCacheShardInner {
    table: HashTable<BlockKey, BlockEntryRef>,
    policy: Box<dyn Policy>,
    capacity_bytes: usize,
    used_bytes: usize,
    in_flight: HashMap<BlockKey, Arc<InFlightLoad>>, // optional (for get_or_load)
}
```

### 7.1 HashTable

- Fixed-capacity vector of buckets (power of two length).
- Each bucket: `(u64 key_hash, BlockEntryRef)` or empty.
- Collision resolution: **Robin Hood** or **linear probing**.
- `BlockEntryRef` is a `usize` index into a `Vec<BlockEntry>` (or direct pointer).

Goal: avoid heap allocations per entry and minimize pointer chasing.

---

## 8. Block Storage & Pinning

### 8.1 Storage

- Entries stored in a contiguous `Vec<BlockEntry>` or `SlotMap`.
- `BlockEntry` contains `Arc<BlockData>` for sharing with handles.
- `size_bytes` determines capacity usage:

  - Use **uncompressed_size** by default.
  - For compressed blocks, optionally tune to min(uncompressed, compressed).

### 8.2 Pinning

Current implementation:

- On hit via `BlockCacheShard::get`:

  - Acquire shard lock → increment `pins` on the entry → return a
    `BlockHandle` that shares the underlying `Arc<BlockData>`.

- On explicit unpin:

  - `BlockCacheShard::unpin` decrements the pin count. Eviction only
    considers entries with `pins == 0`.

- `BlockHandle` exposes `is_pinned` and shares `Arc<BlockData>`, but
  cloning a handle does not currently change shard pin counts, and
  `Drop` does not yet automatically unpin.

Target design:

- Make `BlockHandle` a true RAII pin guard that adjusts shard pin
  counts on clone and `Drop` via an internal reference back to the
  owning shard (no global registries). This is tracked in the
  remaining work section.

---

## 9. Eviction Policy (WTinyLFU-style)

We want something like **Windowed TinyLFU**:

- **Window segment (W):** small recency buffer.
- **Main segment (M):** majority of capacity, frequency-based.
- **TinyLFU frequency sketch:** approximate count of accesses.

### 9.1 Policy responsibilities

`Policy` trait (concept):

```rust
pub trait Policy {
    fn on_hit(&mut self, entry_id: EntryId);
    fn on_insert(&mut self, entry_id: EntryId, size: usize);
    fn on_erase(&mut self, entry_id: EntryId, size: usize);
    fn choose_victim(&mut self, needed_bytes: usize) -> Option<EntryId>;
}
```

- `EntryId` ties into `BlockEntry` index.
- Policy maintains a few deques / ring buffers for segments:

  - `window`: recent entries (LRU list).
  - `probation`: new entries promoted from window.
  - `protected`: frequently used entries.

### 9.2 TinyLFU Frequency Sketch

- Power-of-two array of 4-bit or 8-bit counters.
- Hash key to 4 positions and increment counters, saturating.
- For admission decisions:

  - Compare frequency estimate of candidate vs victim.
  - Admit only if candidate is “hotter”.

Frequency sketch important for:

- Preventing scan pollution.
- Avoiding caching one-off blocks from compaction or full table scans.

---

## 10. Admission Control

When inserting a block:

1. Look up frequency estimate for **candidate** key.
2. If cache full:

   - Ask policy for **victim**.
   - Compare candidate freq vs victim freq.
   - If candidate is colder, **reject admission**, just return handle without caching.

3. If admitted:

   - Insert into `window` segment, account bytes, update tables.

This separates **loading** a block from **caching** it.

---

## 11. In-Flight Load De-Duplication (Optional but Nice)

For a future async API:

```rust
fn get_or_load<F>(&self, key: BlockKey, loader: F) -> Result<BlockHandle>
where
    F: FnOnce(&BlockKey) -> Result<BlockData> + Send + 'static;
```

Inside shard:

- Check cache:

  - If hit: return handle.

- Else check `in_flight` map:

  - If present: clone `Arc<InFlightLoad>` and await completion.

- Else:

  - Create new `InFlightLoad`, insert into map.
  - Run loader (sync or async).
  - Insert into cache, complete promise, remove from `in_flight`.

This avoids 8 threads racing to read the same block from disk.

---

## 12. Prefetch & Readahead Hooks

Expose hooks for SST iterator:

```rust
pub trait BlockCache {
    fn prefetch(&self, key: BlockKey);
}
```

- Prefetch:

  - Runs a low-priority load (possibly async) and puts block into window segment if admitted.

- SST iterators:

  - On reading block N, can prefetch block N+1, N+2 based on layout.

- This is where range scans get big wins.

---

## 13. Metrics & Instrumentation

Per shard, aggregated at top:

- `hits`, `misses`, `evictions`, `admissions`, `rejected_admissions`.
- `bytes_used`, `bytes_capacity`.
- Histograms:

  - Block size distribution.
  - Hit rate by block_kind.
  - Eviction reason (space, TTL, etc. — if you add TTL later).

Currently implemented and exposed via `ShardStats` / `BlockCacheStats`:

- `hits`, `misses`, `evictions`, `admissions`.
- `bytes_used`, `bytes_capacity`.

Planned extensions:

- Track and expose `rejected_admissions` (candidate colder than
  victim) as `BlockCacheStats.rejected`.
- Per-block-kind and per-column-family breakdowns.

Expose via:

```rust
pub struct BlockCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub admissions: u64,
    pub rejected: u64,
    pub used_bytes: usize,
    pub capacity_bytes: usize,
    // maybe rates precomputed or left to metrics layer
}
```

These wire into your existing `metrics/` module.

---

## 14. Integration Points with Midge

### 14.1 SST Reader

- The new block cache implementation is designed to sit in front of
  all SST reads, but full wiring is still in progress.

- Target state:

  - All block loads (user reads, iterators, compaction, backups) go
    through `BlockCache`.
  - For each `BlockHandle` in SST index/iterator:

    - Get block from cache or load and insert via `BlockCache`.

  - `BlockKind` is used for per-kind prioritization and metrics.

### 14.2 Column Families & Tenants

- `BlockKey` already includes `cf_id`:

  - Keep per-CF stats (hit/miss per CF).
  - In the future: per-CF quota or weighted fair sharing.

### 14.3 Compaction

- Compaction iterators use block cache:

  - But admission control + TinyLFU prevents compaction from blowing away working set.
  - You can tune a lower priority for compaction reads by:

    - Marking `BlockKind::CompactionData` vs `BlockKind::UserData`.

---

## 15. Testing & Benchmarks

### 15.1 Tests

- Deterministic policy tests:

  - Insert known patterns, assert victim choices.

- Concurrency tests:

  - Multiple threads doing `get/insert`.
  - Ensure no memory leaks, no panics, correct pinning behavior.

- FPR tests:

  - For random key sets, verify hit rates and eviction patterns.

### 15.2 Benches (tie into your Tier 1/Tier 3)

- **Tier 1 (Hotpath)**

  - `cache_hit` — lookup time with 100% hit.
  - `cache_miss` — lookup time with cold keys.
  - `cache_insert` — cost per insert.

- **Tier 2/3**

  - YCSB A/B/C style with controlled working set vs cache size.
  - Vary read/write ratios, CFs, and block sizes.

---

## 16. Remaining Work

### 16.1 Pinning, In‑Flight Loads, Prefetch

- [ ] Wire `BlockHandle` drop‑based unpin into shards so inserts can return truly pinned handles without manual `unpin` calls.
- [ ] *(Deferred)* Add optional `in_flight` load de‑duplication map to shards for a future `get_or_load` API.

### 16.2 Metrics & Integration

- [ ] Add basic integration points with SST readers / iterators so all block loads flow through `BlockCache`.
- [ ] Add hooks for per-CF accounting using `cf_id` in `BlockKey` (per‑CF hit/miss/eviction stats, capacity hints).
- [ ] Extend stats/metrics to break down by `BlockKind` (data vs index vs filter, etc.).

### 16.3 Testing & Benchmarks

- [ ] Add Tier 2 subsystem benches that exercise mixed read/write workloads, varying cache sizes, and scan pollution scenarios (e.g., YCSB‑style A/B/C traces).
- [ ] Add Tier 3 system benches or integration tests that measure end‑to‑end SST read throughput and hit rate with the cache enabled vs disabled.

### 16.4 Cleanup, Adapters & Migration

- [ ] Add configuration / adapter plumbing so engines can explicitly select and tune the new block cache (capacity, shards, policy, accounting mode).
- [ ] Update docs and diagrams in `BLOCK_CACHE.md` and `docs/` once the cache is fully wired into SST readers, compaction, and higher‑tier benches.
