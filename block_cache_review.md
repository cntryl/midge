# Block Cache Implementation Review

**Review Date**: December 13, 2025  
**Branch**: actor-model  
**Reviewer**: Architecture Analysis  

## Executive Summary

The current block cache implementation has solid fundamentals (sharding, pluggable policies, admission control) but **fails critical production requirements** for a high-performance LSM engine. The cache treats all blocks identically and uses blocking locks on the read path, which will cause severe contention under load.

**Critical Issues**:
- ❌ No block type differentiation (index/data/filter treated equally)
- ❌ Read path blocks on every cache hit/miss (Mutex contention)
- ❌ Cache inserts block reader threads during eviction
- ⚠️ No soft partitioning to protect hot index blocks

**Recommendation**: Implement the 4 priority fixes outlined below before tier4/tier5 production benchmarks.

---

## Ideal Cache Strategy (Reference Checklist)

### Purpose
Keep **index blocks** hotter than data blocks. Index blocks are:
- Small (few KB)
- Reused heavily (every point read touches them)
- Critical path for latency

Data blocks are:
- Larger (64KB+ typical)
- May be touched once in a scan, never again
- Should not evict index blocks

### What is Cached
1. **Index blocks** (sparse index, block index)
2. **Data blocks** (KV pairs)
3. **Filter blocks** (bloom filters)

These should be **distinct classes** with different admission/eviction policies.

### Admission Policy
- **Index blocks**: Always admit (no check)
- **Bloom filter blocks**: Always admit
- **Data blocks**: Admit on **second access** (probabilistic counter)

### Eviction Policy
Must be **scan-resistant** (not plain LRU):
- TinyLFU (frequency + recency)
- CLOCK-Pro (multi-tier)
- ARC (adaptive)

### Cache Partitioning
Logical soft guarantees:
- Index/bloom: 20% of cache (protected)
- Data blocks: 80% of cache

### Threading & Contention
- **Lock-free on read path** (get must never block)
- Sharded design (16+ shards)
- Async admission (channel-based puts)

### Lifecycle Rules
- Blocks are **immutable** (Arc<Bytes>)
- Refcounted, zero-copy
- No in-place modifications

### Read Path Flow
```
1. Check bloom (SST-level) → skip entire SST if negative
2. Check sparse index → narrow block range
3. Check block bloom → skip block if negative
4. CHECK CACHE → return immediately if hit (NO LOCK WAIT)
5. Read from disk → insert into cache (async, non-blocking)
```

### Observability
Per-block-class metrics:
- Index: hits, misses, bytes
- Data: hits, misses, bytes
- Filter: hits, misses, bytes

### Alignment Checklist
- [ ] Index blocks are first-class (always admitted, protected from eviction)
- [ ] Scan protection via TinyLFU/CLOCK-Pro
- [ ] Non-LRU eviction policy
- [ ] Admission control for data blocks
- [ ] No locks on read path
- [ ] No reader stalls during cache inserts

---

## Current Implementation Analysis

### Architecture Overview

**Location**: `src/sst/cache/`

**Components**:
- `mod.rs`: Top-level `BlockCache` with 16 shards
- `shard.rs`: Individual `CacheShard` with lock-based storage
- `key.rs`: `CacheKey` struct (sst_id, block_offset)
- `value.rs`: `CacheValue` with Arc<Bytes> data
- `admission.rs`: Probabilistic counter for second-access policy
- `metrics.rs`: Atomic counters (hits, misses, evictions, memory)
- `policy/`: Pluggable eviction (LRU, TinyLFU, CLOCK-Pro)

**Integration**:
- SST reader calls `cache.get()` before disk read ([reader.rs:323](src/sst/fs/reader.rs#L323))
- SST reader calls `cache.put()` after disk read ([reader.rs:331](src/sst/fs/reader.rs#L331))

---

## ✅ Strengths (Already Implemented)

### 1. Sharding & Low Contention
**Implementation**: 16 shards with hash-based distribution
```rust
// src/sst/cache/mod.rs
pub struct BlockCache {
    shards: Vec<Arc<CacheShard>>,
    num_shards: usize,
}
```

**Key Distribution**:
```rust
// src/sst/cache/key.rs
impl CacheKey {
    pub fn shard_index(&self, num_shards: usize) -> usize {
        let h = self.hash();
        (h as usize) % num_shards
    }
}
```

**Status**: ✅ Well-designed, evenly distributes load

---

### 2. Scan-Resistant Eviction Policies
**Implementation**: Pluggable policies including TinyLFU and CLOCK-Pro

```rust
// src/sst/cache/policy/mod.rs
pub enum CachePolicyType {
    Lru,        // Baseline (not scan-resistant)
    TinyLfu,    // Frequency + recency (W-TinyLFU)
    ClockPro,   // Multi-tier with scan resistance
}
```

**TinyLFU**: Uses frequency estimation to prevent one-time scans from evicting hot blocks  
**CLOCK-Pro**: Three-tier structure (hot, cold, test) with explicit scan protection

**Status**: ✅ Already world-class, just needs type-aware tuning

---

### 3. Admission Control
**Implementation**: Probabilistic counter tracks key frequencies

```rust
// src/sst/cache/admission.rs
pub struct AdmissionCounter {
    cells: Arc<Vec<AtomicU64>>,
    reset_interval: u64,
    access_count: AtomicU64,
}

impl AdmissionCounter {
    pub fn estimate(&self, key: &[u8]) -> bool {
        let cell_idx = (Self::hash_key(key) as usize) % self.cells.len();
        let counter = self.cells[cell_idx].load(Ordering::Relaxed);
        counter > 0  // Admit if seen before
    }
}
```

**Current Usage**: Applied in `CacheShard::put()` to track SST access
```rust
// src/sst/cache/shard.rs:71
self.admission.record_access(key.sst_id.to_le_bytes().as_ref());
```

**Status**: ✅ Correct "second access" implementation, BUT applies uniformly to all block types (needs fix)

---

### 4. Immutable Blocks
**Implementation**: CacheValue wraps Arc<Bytes>

```rust
// src/sst/cache/value.rs
pub struct CacheValue {
    pub data: Arc<Bytes>,
    pub inserted_at: u64,
    pub access_count: Arc<AtomicU64>,
}
```

**Benefits**:
- Zero-copy reads via `Arc::clone()`
- Safe concurrent access
- Automatic reference counting

**Status**: ✅ Perfect design

---

### 5. Observability
**Implementation**: Per-shard atomic metrics

```rust
// src/sst/cache/metrics.rs
pub struct CacheMetrics {
    pub(crate) hits: Arc<AtomicU64>,
    pub(crate) misses: Arc<AtomicU64>,
    pub(crate) evictions: Arc<AtomicU64>,
    pub(crate) memory_bytes: Arc<AtomicU64>,
}

impl CacheMetrics {
    pub fn hit_rate(&self) -> f64 {
        let hits = self.hit_count() as f64;
        let total = (self.hit_count() + self.miss_count()) as f64;
        if total == 0.0 { 0.0 } else { (hits / total) * 100.0 }
    }
}
```

**Status**: ✅ Good foundation, needs per-block-type breakdown (see recommendations)

---

## ❌ Critical Gaps (Must Fix)

### 1. No Block Type Differentiation ⚠️ **CRITICAL**

**Problem**: Cache treats all blocks identically—no distinction between index, data, and filter blocks.

**Current CacheKey**:
```rust
// src/sst/cache/key.rs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CacheKey {
    pub sst_id: u64,
    pub block_offset: u64,
    // ❌ NO BLOCK TYPE!
}
```

**What's Missing**:
- Index blocks should **always** be admitted
- Index blocks should be **protected from eviction** (only evict if cache completely full)
- Data blocks should use admission control (second access)
- Filter blocks should be treated like index blocks

**Impact**: 
- Large scans can evict tiny index blocks
- Every subsequent point read must re-fetch index blocks from disk
- **Direct latency impact** on tier4/tier5 benchmarks

**Evidence from Ideal Strategy**:
> "Index blocks: Always admit, never evict unless cache is empty"
> "Data blocks: Admit on second access"

---

### 2. Read Path Has Blocking Locks ⚠️ **CRITICAL**

**Problem**: Every cache hit/miss takes a mutex lock.

**Current Code**:
```rust
// src/sst/cache/shard.rs:48-58
pub fn get(&self, key: &CacheKey) -> Option<CacheValue> {
    let entries = self.entries.lock().expect("cache shard lock");  // ❌ BLOCKS EVERY READ
    if let Some(value) = entries.get(key) {
        value.increment_access();
        self.policy.on_access(*key);
        self.metrics.record_hit();
        Some(value.clone())
    } else {
        self.metrics.record_miss();
        None
    }
}
```

**What This Means**:
- High read concurrency → lock contention
- Reader threads wait for each other even on cache hits
- Cache becomes a **bottleneck** instead of an accelerator

**Evidence from Ideal Strategy**:
> "Get must never block. Never. If you need to lock the data structure, you have already lost."

**Tier4 Impact**: 
- YCSB Workload A: 50% reads, 50% writes → heavy read contention
- YCSB Workload B: 95% reads, 5% writes → cache lock is the hottest lock in the system
- YCSB Workload C: 100% reads → complete serialization on cache access

---

### 3. Put() Blocks Reader Threads ⚠️ **CRITICAL**

**Problem**: Cache insertion holds the shard lock during eviction loop.

**Current Code**:
```rust
// src/sst/cache/shard.rs:91-103
pub fn put(&self, key: CacheKey, value: Bytes) -> bool {
    // ...
    let mut entries = self.entries.lock().expect("cache shard lock");  // ❌ HOLDS LOCK
    
    entries.insert(key, cache_value);
    self.metrics.add_memory(value_size);
    self.policy.on_access(key);

    // ❌ EVICTION LOOP WHILE HOLDING LOCK
    while self.metrics.memory_bytes() > self.max_bytes {
        if let Some(victim) = self.policy.pick_victim() {
            if let Some(evicted) = entries.remove(&victim) {
                self.metrics.remove_memory(evicted.size_bytes() as u64);
                self.metrics.record_eviction();
            }
        } else {
            break;
        }
    }

    true
}
```

**What This Means**:
- During heavy flush (memtable → L0), reader threads stall
- During compaction (writing new SSTs), readers wait for eviction
- Eviction can take milliseconds if policy needs to scan many candidates

**Evidence from Ideal Strategy**:
> "Async admission (via channel), so puts don't block gets"

---

### 4. No Soft Partitioning 📊 **IMPORTANT**

**Problem**: No memory reservations for index vs data blocks.

**Current Allocation**:
```rust
// src/sst/cache/mod.rs
pub fn new(capacity: u64, num_shards: usize, policy_type: CachePolicyType) -> Self {
    let per_shard_capacity = capacity / num_shards as u64;
    // ❌ NO PARTITIONING BY BLOCK TYPE
}
```

**What's Missing**:
- Soft guarantee: "Index/bloom get 20% of cache"
- Soft guarantee: "Data blocks get 80% of cache"
- Hard limit: "Data blocks cannot push index blocks below 10%"

**Impact**: 
- Worst case: Large scan fills cache with data blocks, evicts all index
- Next point read: Cache miss on index block (should be 100% hit rate)
- Cascading misses until index blocks re-populate cache

**Evidence from Ideal Strategy**:
> "Index/bloom: 20% of cache (soft guaranteed)"
> "Data blocks: 80% of cache (remainder)"

---

## 🔧 Recommended Fixes (Priority Order)

### Priority 1: Add Block Type to CacheKey 🔥

**Goal**: Enable type-aware admission and eviction policies

**Changes Required**:

#### 1a. Extend CacheKey with BlockType
```rust
// src/sst/cache/key.rs

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BlockType {
    Index,   // Block index, sparse index
    Data,    // KV data blocks
    Filter,  // Bloom filter blocks
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CacheKey {
    pub sst_id: u64,
    pub block_offset: u64,
    pub block_type: BlockType,  // ← NEW
}

impl CacheKey {
    pub fn new(sst_id: u64, block_offset: u64, block_type: BlockType) -> Self {
        Self { sst_id, block_offset, block_type }
    }
    
    pub fn for_data(sst_id: u64, block_offset: u64) -> Self {
        Self::new(sst_id, block_offset, BlockType::Data)
    }
    
    pub fn for_index(sst_id: u64, block_offset: u64) -> Self {
        Self::new(sst_id, block_offset, BlockType::Index)
    }
    
    pub fn for_filter(sst_id: u64, block_offset: u64) -> Self {
        Self::new(sst_id, block_offset, BlockType::Filter)
    }
}
```

#### 1b. Update Admission Policy
```rust
// src/sst/cache/admission.rs

impl AdmissionCounter {
    /// Check if a key should be admitted based on block type
    pub fn should_admit(&self, key: &CacheKey) -> bool {
        match key.block_type {
            BlockType::Index | BlockType::Filter => {
                // Always admit index and filter blocks
                true
            }
            BlockType::Data => {
                // Data blocks: admit on second access
                self.estimate(&key.sst_id.to_le_bytes())
            }
        }
    }
}
```

#### 1c. Update Eviction Policy
```rust
// src/sst/cache/policy/mod.rs

pub trait CachePolicy: Send + Sync {
    fn on_access(&self, key: CacheKey);
    
    /// Pick victim, excluding protected block types
    fn pick_victim(&self, exclude_types: &[BlockType]) -> Option<CacheKey>;
    
    fn remove(&self, key: CacheKey);
    fn clear(&self);
}

// src/sst/cache/shard.rs (eviction loop)
while self.metrics.memory_bytes() > self.max_bytes {
    // Never evict index/filter unless we have no choice
    if let Some(victim) = self.policy.pick_victim(&[BlockType::Index, BlockType::Filter]) {
        // ... evict victim
    } else if self.metrics.memory_bytes() > self.max_bytes * 2 {
        // Emergency: evict anything (cache completely full)
        if let Some(victim) = self.policy.pick_victim(&[]) {
            // ... evict victim
        } else {
            break;
        }
    } else {
        break;
    }
}
```

#### 1d. Update SST Reader Call Sites
```rust
// src/sst/fs/reader.rs

// When caching data blocks (current usage)
let cache_key = CacheKey::for_data(self.sst_id, handle.offset);

// When caching index blocks (TODO: add this)
let cache_key = CacheKey::for_index(self.sst_id, index_handle.offset);

// When caching filter blocks (TODO: add this)
let cache_key = CacheKey::for_filter(self.sst_id, bloom_handle.offset);
```

**Estimated Impact**:
- Index block hit rate: 60% → 99%+ (always admitted, rarely evicted)
- Point read P99 latency: -20% to -40% (no index re-fetches)
- Scan throughput: Unchanged (data blocks still cached normally)

---

### Priority 2: Make Get() Lock-Free 🔥

**Goal**: Eliminate mutex contention on read path

**Solution**: Replace `Mutex<HashMap>` with concurrent hashmap (`dashmap`)

#### 2a. Add Dependency
```toml
# Cargo.toml
[dependencies]
dashmap = "5.5"
```

#### 2b. Replace Mutex with DashMap
```rust
// src/sst/cache/shard.rs

use dashmap::DashMap;  // ← LOCK-FREE CONCURRENT HASHMAP

pub struct CacheShard {
    entries: DashMap<CacheKey, CacheValue>,  // ← NO MORE MUTEX
    policy: Box<dyn CachePolicy>,
    admission: AdmissionCounter,
    metrics: CacheMetrics,
    max_bytes: u64,
}

impl CacheShard {
    pub fn new(max_bytes: u64, policy_type: CachePolicyType) -> Self {
        Self {
            entries: DashMap::new(),  // ← NO LOCK
            policy: policy_type.create(),
            admission: AdmissionCounter::new(64, 1000),
            metrics: CacheMetrics::new(),
            max_bytes,
        }
    }

    pub fn get(&self, key: &CacheKey) -> Option<CacheValue> {
        // ✅ NO LOCK - DashMap uses fine-grained sharding internally
        if let Some(value_ref) = self.entries.get(key) {
            let value = value_ref.value().clone();
            value.increment_access();
            self.policy.on_access(*key);
            self.metrics.record_hit();
            Some(value)
        } else {
            self.metrics.record_miss();
            None
        }
    }

    pub fn remove(&self, key: &CacheKey) -> Option<CacheValue> {
        if let Some((_, value)) = self.entries.remove(key) {
            self.metrics.remove_memory(value.size_bytes() as u64);
            self.policy.remove(*key);
            Some(value)
        } else {
            None
        }
    }

    pub fn clear(&self) {
        self.entries.clear();
        self.metrics.set_memory_bytes(0);
        self.policy.clear();
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
```

**DashMap Benefits**:
- Lock-free reads (uses internal sharding)
- Scales to 100+ concurrent readers
- Drop-in replacement for `HashMap` API
- Battle-tested in production Rust systems

**Estimated Impact**:
- Cache contention: -90%+ (no blocking on reads)
- Read throughput: +30% to +50% under high concurrency
- P99 latency: -15% to -25% (no lock wait spikes)

---

### Priority 3: Async Cache Admission 🔥

**Goal**: Decouple `put()` from eviction, prevent reader stalls

**Solution**: Use channel-based admission queue with background worker

#### 3a. Add Async Channel to CacheShard
```rust
// src/sst/cache/shard.rs

use tokio::sync::mpsc;
use std::sync::Arc;

pub struct CacheShard {
    entries: DashMap<CacheKey, CacheValue>,
    policy: Box<dyn CachePolicy>,
    admission: AdmissionCounter,
    metrics: CacheMetrics,
    max_bytes: u64,
    admission_tx: mpsc::UnboundedSender<(CacheKey, Bytes)>,  // ← ASYNC QUEUE
}

impl CacheShard {
    pub fn new(max_bytes: u64, policy_type: CachePolicyType) -> Arc<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        
        let shard = Arc::new(Self {
            entries: DashMap::new(),
            policy: policy_type.create(),
            admission: AdmissionCounter::new(64, 1000),
            metrics: CacheMetrics::new(),
            max_bytes,
            admission_tx: tx,
        });

        // Spawn background admission worker
        let shard_clone = Arc::clone(&shard);
        tokio::spawn(async move {
            shard_clone.admission_worker(rx).await;
        });

        shard
    }

    pub fn put(&self, key: CacheKey, value: Bytes) -> bool {
        // ✅ FAST PATH: Just send to queue, return immediately
        self.admission_tx.send((key, value)).is_ok()
        // Reader threads never block here
    }

    async fn admission_worker(
        self: Arc<Self>,
        mut rx: mpsc::UnboundedReceiver<(CacheKey, Bytes)>,
    ) {
        while let Some((key, value)) = rx.recv().await {
            // Check admission control
            if !self.admission.should_admit(&key) {
                continue;
            }

            let cache_value = CacheValue::new(value);
            let value_size = cache_value.size_bytes() as u64;

            // Insert into cache (DashMap is thread-safe)
            if let Some(existing) = self.entries.insert(key, cache_value) {
                // Updated existing entry
                let old_size = existing.size_bytes() as u64;
                self.metrics.add_memory(value_size);
                self.metrics.remove_memory(old_size);
            } else {
                // New entry
                self.metrics.add_memory(value_size);
            }

            self.policy.on_access(key);

            // Evict if over capacity (runs in background)
            self.evict_if_needed();
        }
    }

    fn evict_if_needed(&self) {
        while self.metrics.memory_bytes() > self.max_bytes {
            // Try to evict data blocks first
            if let Some(victim) = self.policy.pick_victim(&[BlockType::Index, BlockType::Filter]) {
                if let Some((_, evicted)) = self.entries.remove(&victim) {
                    self.metrics.remove_memory(evicted.size_bytes() as u64);
                    self.metrics.record_eviction();
                    self.policy.remove(victim);
                }
            } else {
                break;
            }
        }
    }
}
```

**Benefits**:
- `put()` returns in <1μs (just queue send)
- Eviction runs in background (never blocks readers)
- Admission policy runs in background
- Batching opportunity (future optimization)

**Estimated Impact**:
- Write flush latency: -40% to -60% (no cache insert stalls)
- Read P99 during flush: -30% to -50% (readers never wait for eviction)
- Compaction throughput: +10% to +20% (faster SST insertion)

---

### Priority 4: Soft Partitioning by Block Type 📊

**Goal**: Reserve cache capacity for index/filter blocks

**Solution**: Track memory per block type, enforce soft limits

#### 4a. Extend CacheMetrics
```rust
// src/sst/cache/metrics.rs

pub struct CacheMetrics {
    // Aggregate counters (existing)
    hits: Arc<AtomicU64>,
    misses: Arc<AtomicU64>,
    evictions: Arc<AtomicU64>,
    
    // Per-block-type counters (NEW)
    index_hits: Arc<AtomicU64>,
    index_misses: Arc<AtomicU64>,
    data_hits: Arc<AtomicU64>,
    data_misses: Arc<AtomicU64>,
    filter_hits: Arc<AtomicU64>,
    filter_misses: Arc<AtomicU64>,
    
    // Per-block-type memory (NEW)
    index_bytes: Arc<AtomicU64>,
    data_bytes: Arc<AtomicU64>,
    filter_bytes: Arc<AtomicU64>,
}

impl CacheMetrics {
    pub fn record_hit(&self, block_type: BlockType) {
        self.hits.fetch_add(1, Ordering::Relaxed);
        match block_type {
            BlockType::Index => self.index_hits.fetch_add(1, Ordering::Relaxed),
            BlockType::Data => self.data_hits.fetch_add(1, Ordering::Relaxed),
            BlockType::Filter => self.filter_hits.fetch_add(1, Ordering::Relaxed),
        };
    }
    
    pub fn add_memory(&self, bytes: u64, block_type: BlockType) {
        match block_type {
            BlockType::Index => self.index_bytes.fetch_add(bytes, Ordering::Relaxed),
            BlockType::Data => self.data_bytes.fetch_add(bytes, Ordering::Relaxed),
            BlockType::Filter => self.filter_bytes.fetch_add(bytes, Ordering::Relaxed),
        };
    }
    
    pub fn index_memory(&self) -> u64 {
        self.index_bytes.load(Ordering::Relaxed)
    }
    
    pub fn data_memory(&self) -> u64 {
        self.data_bytes.load(Ordering::Relaxed)
    }
    
    pub fn filter_memory(&self) -> u64 {
        self.filter_bytes.load(Ordering::Relaxed)
    }
}
```

#### 4b. Add Partitioning Logic
```rust
// src/sst/cache/shard.rs

pub struct CacheShard {
    // ... existing fields ...
    index_reserved_bytes: u64,   // 20% soft guarantee
    data_max_bytes: u64,         // 80% soft limit
}

impl CacheShard {
    pub fn new(max_bytes: u64, policy_type: CachePolicyType) -> Arc<Self> {
        let index_reserved = (max_bytes as f64 * 0.20) as u64;
        let data_max = (max_bytes as f64 * 0.80) as u64;
        
        // ... rest of initialization ...
        
        let shard = Arc::new(Self {
            // ... existing fields ...
            index_reserved_bytes: index_reserved,
            data_max_bytes: data_max,
        });
        
        shard
    }

    fn evict_if_needed(&self) {
        let total_bytes = self.metrics.memory_bytes();
        
        // Strategy 1: If data blocks exceed 80%, evict data only
        if self.metrics.data_memory() > self.data_max_bytes {
            while self.metrics.data_memory() > self.data_max_bytes {
                if let Some(victim) = self.policy.pick_victim(&[BlockType::Index, BlockType::Filter]) {
                    // Evict data block
                    if let Some((_, evicted)) = self.entries.remove(&victim) {
                        self.metrics.remove_memory(evicted.size_bytes() as u64, victim.block_type);
                        self.metrics.record_eviction();
                        self.policy.remove(victim);
                    }
                } else {
                    break;
                }
            }
        }
        
        // Strategy 2: If total exceeds max, evict anything (but prefer data)
        if total_bytes > self.max_bytes {
            while self.metrics.memory_bytes() > self.max_bytes {
                // Try data blocks first
                if let Some(victim) = self.policy.pick_victim(&[BlockType::Index, BlockType::Filter]) {
                    // ... evict data block
                } else {
                    // Emergency: evict index/filter if absolutely necessary
                    if let Some(victim) = self.policy.pick_victim(&[]) {
                        // ... evict any block
                    } else {
                        break;
                    }
                }
            }
        }
    }
}
```

**Estimated Impact**:
- Index block hit rate during scans: 85% → 98%+
- Point read latency during scan: -25% to -35%
- Mixed workload performance: More stable (less variance)

---

## 📊 Per-Block-Class Metrics Dashboard

With Priority 4 implemented, you can track:

```rust
// Example metrics output
pub struct CacheStats {
    // Index blocks
    pub index_hit_rate: f64,     // Should be 95-99%
    pub index_memory_pct: f64,   // Should be 15-25%
    pub index_avg_size: u64,     // Typically 4-8 KB
    
    // Data blocks
    pub data_hit_rate: f64,      // 40-70% (depends on workload)
    pub data_memory_pct: f64,    // Should be 70-80%
    pub data_avg_size: u64,      // Typically 32-64 KB
    
    // Filter blocks
    pub filter_hit_rate: f64,    // Should be 99%+
    pub filter_memory_pct: f64,  // Should be 5-10%
    pub filter_avg_size: u64,    // Typically 1-4 KB
}
```

**Key Indicators**:
- ⚠️ Index hit rate < 95%: Cache too small or partitioning broken
- ⚠️ Data memory > 85%: Data blocks evicting index blocks (bad)
- ✅ Filter hit rate > 99%: Bloom blocks staying resident (good)

---

## ✅ Final Checklist Alignment

| Criterion | Current Status | After Priority 1 | After Priority 2 | After Priority 3 | After Priority 4 |
|-----------|----------------|------------------|------------------|------------------|------------------|
| **Index blocks first-class** | ❌ No differentiation | ✅ Type-aware admission/eviction | ✅ | ✅ | ✅ |
| **Scan protection** | ✅ TinyLFU/CLOCK-Pro | ✅ | ✅ | ✅ | ✅ |
| **Non-LRU eviction** | ✅ TinyLFU/CLOCK-Pro | ✅ | ✅ | ✅ | ✅ |
| **Admission control** | ⚠️ Uniform for all types | ✅ Index always, data second-access | ✅ | ✅ | ✅ |
| **No read-path locks** | ❌ Mutex on every get() | ❌ | ✅ DashMap | ✅ | ✅ |
| **No reader stalls** | ❌ put() blocks during eviction | ❌ | ❌ | ✅ Async admission | ✅ |
| **Soft partitioning** | ❌ No memory reserves | ❌ | ❌ | ❌ | ✅ 20% index reserve |

**After All Priorities**: 6/6 criteria met (100% aligned with ideal strategy)

---

## 🚀 Implementation Roadmap

### Phase 1: Foundation (Priority 1) - 2-3 hours
- [ ] Add `BlockType` enum to `CacheKey`
- [ ] Update `AdmissionCounter::should_admit()` with type logic
- [ ] Update `CachePolicy::pick_victim()` with type exclusion
- [ ] Update SST reader call sites (mark block types correctly)
- [ ] Run `cargo test --lib sst::cache` (verify no regressions)

### Phase 2: Lock-Free Reads (Priority 2) - 1-2 hours
- [ ] Add `dashmap` dependency
- [ ] Replace `Mutex<HashMap>` with `DashMap` in `CacheShard`
- [ ] Update `get()`, `put()`, `remove()`, `clear()` methods
- [ ] Run `cargo test --lib sst::cache`
- [ ] Run `cargo bench --bench tier2_subsystem_block_cache` (verify speedup)

### Phase 3: Async Admission (Priority 3) - 3-4 hours
- [ ] Add `tokio::sync::mpsc` channel to `CacheShard`
- [ ] Convert `put()` to send to queue (non-blocking)
- [ ] Implement `admission_worker()` background task
- [ ] Move eviction logic to `evict_if_needed()`
- [ ] Update `CacheShard::new()` to spawn worker
- [ ] Run `cargo test --lib sst::cache`
- [ ] Run `cargo bench --bench tier3_system_engine_basic` (verify no write stalls)

### Phase 4: Soft Partitioning (Priority 4) - 2-3 hours
- [ ] Extend `CacheMetrics` with per-type counters
- [ ] Add `index_reserved_bytes` and `data_max_bytes` to `CacheShard`
- [ ] Update `evict_if_needed()` with partitioning logic
- [ ] Update `record_hit()`, `add_memory()` to track by type
- [ ] Add metrics dashboard in `BlockCache::stats()`
- [ ] Run full test suite: `cargo test`
- [ ] Run tier4 benchmarks: `cargo bench --bench tier4*`

**Total Estimated Time**: 8-12 hours (one solid work day)

---

## 📈 Expected Performance Gains

### Tier4 YCSB Benchmarks (Post-Fix)

| Workload | Current Bottleneck | Expected Improvement |
|----------|-------------------|---------------------|
| **Workload A** (50% R, 50% W) | Cache lock contention | **+30% to +50% throughput** |
| **Workload B** (95% R, 5% W) | Cache lock contention on reads | **+40% to +60% throughput** |
| **Workload C** (100% read) | Cache lock serializes reads | **+50% to +70% throughput** |
| **Workload D** (95% read latest) | Index blocks evicted by scans | **-20% to -30% P99 latency** |
| **Workload E** (95% scan) | Cache pollution from scans | **+10% to +20% point read throughput** |
| **Workload F** (50% R, 50% RMW) | Write stalls during cache eviction | **+25% to +35% throughput** |

### System-Level Improvements

- **Compaction throughput**: +15% to +25% (no cache insert stalls)
- **Flush latency**: -30% to -50% (async admission)
- **Point read P99**: -25% to -40% (index blocks always cached)
- **Range scan throughput**: +5% to +15% (less cache pollution)

---

## 🔍 Validation Strategy

### Unit Tests (Per Priority)
```bash
# After each priority, run:
cargo test --lib sst::cache::

# Expected: All existing tests pass, new tests added
```

### Tier2 Subsystem Benchmarks
```bash
# Measure cache hit rate and lock contention
cargo bench --bench tier2_subsystem_block_cache
cargo bench --bench tier2_subsystem_read_amplification

# Expected:
# - Block cache: +30% to +50% ops/sec
# - Read amp: -10% to -20% avg reads per key
```

### Tier3 System Benchmarks
```bash
# Measure mixed workload performance
cargo bench --bench tier3_system_engine_basic
cargo bench --bench tier3_system_concurrency_stress

# Expected:
# - Engine basic: +20% to +40% throughput
# - Concurrency: +40% to +60% throughput (less contention)
```

### Tier4 Integration Benchmarks
```bash
# Final validation with YCSB workloads
cargo bench --bench tier4* > tier4_after_cache_fix.txt

# Compare to baseline (tier4_results.txt):
# - Workload B (95% read): +40% to +60% ops/sec
# - Workload C (100% read): +50% to +70% ops/sec
```

---

## 📝 Notes & Caveats

### DashMap Considerations
- **Memory overhead**: ~10-15% more memory than `HashMap` (worth it for lock-free)
- **CPU usage**: Slightly higher due to internal sharding (marginal)
- **Dependency**: Well-maintained, 10M+ downloads on crates.io

### Async Admission Considerations
- **Latency**: Admitted blocks appear in cache after ~10-100μs (queue processing)
- **Memory spikes**: Queue can grow during burst writes (use bounded channel if needed)
- **Graceful shutdown**: Need to flush admission queue on engine shutdown

### Soft Partitioning Considerations
- **Not a hard guarantee**: Can be violated under extreme memory pressure
- **Tuning needed**: 20%/80% split is a starting point (may need workload-specific tuning)
- **Overhead**: Extra memory tracking adds ~5% CPU (negligible compared to gains)

---

## 🎯 Expert Review & Validation

**Review Status**: ✅ Validated by Staff/Principal Engineer  
**Verdict**: Analysis is directionally correct. Cache is the next big performance limiter.

### Key Insight

> **"Your actor model is preventing *stalls*, but your cache is still causing *contention*."**

This explains why Tier-4 performance degrades smoothly (good) but doesn't jump yet (cache not pulling its weight).

**Fix the cache and you'll see step-function improvements, not incremental ones.**

---

### What Was Confirmed 100% Correct

1. **No block-type differentiation is fatal**
   - Index + Bloom blocks *must* be first-class
   - Treating them like data blocks guarantees tail regressions
   - This alone explains a lot of Tier-4 variance

2. **Blocking on `get()` is unacceptable**
   - Any mutex on the read path becomes the hottest lock in YCSB B/C
   - This caps scalability regardless of actor model quality

3. **Synchronous eviction during `put()` is poisoning reads**
   - Flush/compaction shouldn't ever stall foreground reads
   - Actor model *assumes* this separation — the cache violates it

4. **TinyLFU/CLOCK-Pro is world-class**
   - The issue is *integration*, not policy choice
   - Don't tune policy parameters before fixing architectural issues

---

### Critical Refinements

#### 1️⃣ DashMap is a Means, Not the Goal

The real invariant is:
> **Cache hits must never wait on unrelated cache activity**

Achievable via:
- DashMap (pragmatic choice)
- Sharded lock-free maps
- RCU-style swap maps

**Adopt DashMap pragmatically, but the *semantic guarantee* is the win.**

---

#### 2️⃣ Async Admission Aligns Perfectly with Actor Model

This is the most important architectural alignment:
- Reads are latency-sensitive
- Writes/flushes are throughput-oriented
- Cache admission is *background work*

Therefore:
- **Admission worker = background actor**
- **Cache hits = synchronous, wait-free**
- **Eviction = cooperative, never blocking reads**

**This is exactly consistent with engine philosophy.**

---

#### 3️⃣ Soft Partitioning is About Correctness, Not Tuning

The 20%/80% split isn't about tuning — it's about **correctness**:

Required invariant:
> "Once an index block is hot, it stays hot unless memory pressure is extreme."

**If a scan can evict index blocks, the cache is *logically broken*.**

Current cache does not guarantee this.

---

### Refined Execution Order

**Do NOT tune TinyLFU/CLOCK-Pro knobs before completing steps 1-4 — it's wasted effort.**

1. **Add BlockType to CacheKey** (non-negotiable)
2. **Make `get()` wait-free** (DashMap or equivalent)
3. **Protect index + bloom blocks from eviction**
4. **Move eviction/admission fully off the read path**
5. **Only then tune policy parameters**

---

## 🎯 Conclusion

The current block cache has **fundamentally sound design** but **violates three invariants that matter more than clever eviction**:

1. ❌ **No block type awareness** → index blocks get evicted by scans
2. ❌ **Blocking locks on reads** → cache becomes a contention bottleneck
3. ❌ **Synchronous eviction** → write flushes stall readers

**Impact on Tier4 Benchmarks**:
- Read-heavy workloads (B/C) are currently **serialized by cache lock**
- Mixed workloads (A/F) suffer from **write-induced read stalls**
- Scan workloads (E) pollute cache and **evict hot index blocks**

**Status**: **One refactor away, not a rewrite away**

**Recommendation**: Fix architectural issues first, then benchmark. Each fix provides incremental value:
- **Priority 1**: Foundation for intelligent caching (+10-20% improvement)
- **Priority 2**: Eliminates read bottleneck (+30-50% on read-heavy loads)
- **Priority 3**: Eliminates write-induced stalls (+20-30% on mixed loads)
- **Priority 4**: Stabilizes performance under diverse workloads (+10-15%)

**Combined Impact**: **+60% to +100% throughput** on tier4 benchmarks (conservative estimate).

**Estimated Engineering Time**: 8-12 hours (one work day for experienced Rust engineer).

**Risk**: Low (DashMap is battle-tested, async admission is well-understood pattern).

**Next Steps**:
- Design cache-only microbench to prove wins fast
- Stage refactor safely without breaking CI
- Validate against Tier-2/Tier-3 benches before Tier-4

---

## 📚 References

### Internal Documentation
- [DEPENDENCY_ANALYSIS.md](docs/DEPENDENCY_ANALYSIS.md) - Layer architecture
- [TODO.md](TODO.md) - Bench checklist
- [.github/copilot-instructions.md](.github/copilot-instructions.md) - Project conventions

### External References
- [Caffeine Cache](https://github.com/ben-manes/caffeine/wiki/Efficiency) - TinyLFU implementation
- [CLOCK-Pro Paper](https://www.usenix.org/legacy/events/usenix05/tech/general/full_papers/jiang/jiang.pdf) - Scan-resistant eviction
- [DashMap Documentation](https://docs.rs/dashmap/) - Concurrent hashmap
- [RocksDB Block Cache](https://github.com/facebook/rocksdb/wiki/Block-Cache) - Production LSM cache design

---

**Document Version**: 1.0  
**Last Updated**: December 13, 2025  
**Status**: Recommendations pending implementation
