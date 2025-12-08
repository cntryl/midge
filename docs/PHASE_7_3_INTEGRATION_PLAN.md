# Phase 7.3 Integration Plan: Cache Eviction Coordination

## Overview

Phase 7.3 integrates cache eviction decisions into `EngineRuntime`, ensuring that when the local cache reaches capacity, eviction is submitted as a runtime task rather than happening in a background thread. This provides:

1. **Deterministic eviction**: Same cache state → same eviction decision
2. **Ordered eviction**: Eviction sequenced with flushes/compactions
3. **Metrics**: Clear visibility into eviction rate and performance impact
4. **Controllability**: Can pause/resume eviction via runtime backpressure

## Current State (Pre-Phase 7.3)

### Cache Eviction Flow

```
HybridStorage::get_or_fetch()
    ↓
Cache hit → return cached block
    ↓
If cache miss AND cache_size > max_capacity:
    ↓
evict_lru_entry()  // Background thread eviction
    ↓
Eviction runs independently of EngineRuntime
    ↓
Cache updated asynchronously
    ↓
Non-deterministic eviction order relative to other operations
```

**Problem**: Cache eviction is not coordinated with runtime:
- Eviction timing varies with background thread scheduling
- Eviction order not reproducible
- No way to control eviction rate relative to flush/compaction
- Metrics collection non-deterministic

## Phase 7.3 Target State

### New Cache Eviction Flow

```
HybridStorage::get_or_fetch()
    ↓
Cache hit → return cached block
    ↓
If cache miss AND cache_size > max_capacity:
    ↓
cloud_coordinator.submit_eviction_task()  // Submit to runtime
    ↓
RuntimeTask(Maintenance, "cache_eviction")
    ↓
EngineRuntime executor (single-threaded)
    ↓
Execute task: HybridStorage::evict_lru_entry()
    ↓
Eviction runs as sequential runtime task
    ↓
Cache updated deterministically
    ↓
Metrics recorded with deterministic timing
```

**Benefits**:
- Eviction sequenced with flush/compaction/cloud ops
- Eviction order reproducible from manifest state
- Can measure eviction impact on read latency
- Runtime can backpressure if eviction falls behind

## Integration Points

### 1. File: `src/cloud/hybrid.rs`

**Current Code Structure**:

```rust
pub struct HybridStorage {
    local_cache: Arc<RwLock<HashMap<String, CachedBlock>>>,
    cloud_manager: Arc<CloudSstManager>,
    metrics: Arc<CacheMetrics>,
    // ...other fields...
}

pub fn get_or_fetch(&self, sst_id: &str, block_id: u32) -> Result<Block> {
    // Check cache
    if let Some(block) = self.local_cache.read().get(&key) {
        return Ok(block.clone());
    }
    
    // Fetch from cloud
    let block = self.cloud_manager.fetch_block(sst_id, block_id)?;
    
    // Store in cache
    let mut cache = self.local_cache.write();
    if cache.len() >= self.max_capacity {
        // PROBLEM: Eviction here happens synchronously or in background
        self.evict_lru_entry(&mut cache);
    }
    cache.insert(key, block.clone());
    
    Ok(block)
}
```

**Changes Required**:

1. Add reference to `cloud_coordinator` in HybridStorage
2. When eviction needed, call `cloud_coordinator.submit_eviction_task()`
3. Move actual eviction logic into a closure passed to coordinator
4. Maintain metrics atomically

**New Code Pattern**:

```rust
pub struct HybridStorage {
    local_cache: Arc<RwLock<HashMap<String, CachedBlock>>>,
    cloud_manager: Arc<CloudSstManager>,
    cloud_coordinator: Option<Arc<CloudCoordinator>>,  // NEW
    runtime: Option<Arc<EngineRuntime>>,              // NEW
    metrics: Arc<CacheMetrics>,
    // ...
}

pub fn get_or_fetch(&self, sst_id: &str, block_id: u32) -> Result<Block> {
    // Check cache
    if let Some(block) = self.local_cache.read().get(&key) {
        return Ok(block.clone());
    }
    
    // Fetch from cloud
    let block = self.cloud_manager.fetch_block(sst_id, block_id)?;
    
    // Store in cache
    {
        let mut cache = self.local_cache.write();
        let need_eviction = cache.len() >= self.max_capacity;
        
        if need_eviction {
            // NEW: Submit eviction as runtime task
            if let (Some(coordinator), Some(runtime)) = (&self.cloud_coordinator, &self.runtime) {
                let cache_inner = Arc::clone(&self.local_cache);
                let max_cap = self.max_capacity;
                let metrics = Arc::clone(&self.metrics);
                
                coordinator.submit_eviction_task(runtime, move || {
                    let mut cache = cache_inner.write();
                    let evicted = Self::evict_lru_entry_impl(&mut cache, max_cap);
                    metrics.record_eviction(evicted);
                })?;
            }
        }
        
        cache.insert(key, block.clone());
    }
    
    Ok(block)
}

// New internal method for eviction logic (called from runtime task)
fn evict_lru_entry_impl(cache: &mut HashMap<String, CachedBlock>, max_capacity: usize) -> usize {
    // Find LRU entry and remove it
    // Return size of evicted entry
    // (existing eviction logic, now called from runtime task)
}
```

### 2. File: `src/core/engine/core.rs`

**Changes Required**:

1. Store `cloud_coordinator` reference in HybridStorage
2. Store `runtime` reference in HybridStorage

**Implementation**:

```rust
pub fn initialize_hybrid_storage(
    engine: &Arc<MidgeEngine>,
) -> Result<Arc<HybridStorage>> {
    let config = &engine.config;
    let storage = Arc::new(HybridStorage::new(config)?);
    
    // NEW: Set coordinator and runtime references
    storage.set_coordinator(
        Some(Arc::clone(&engine.cloud_coordinator)),
        Some(Arc::clone(&engine.runtime)),
    );
    
    Ok(storage)
}
```

**Alternative: Constructor Parameter**:

```rust
pub fn new_with_coordinator(
    config: &StorageConfig,
    cloud_coordinator: Arc<CloudCoordinator>,
    runtime: Arc<EngineRuntime>,
) -> Result<Self> {
    Ok(HybridStorage {
        local_cache: Arc::new(RwLock::new(HashMap::new())),
        cloud_manager: Arc::new(CloudSstManager::new(config)?),
        cloud_coordinator: Some(cloud_coordinator),
        runtime: Some(runtime),
        metrics: Arc::new(CacheMetrics::new()),
        max_capacity: config.cache_size_bytes,
    })
}
```

### 3. File: `src/core/cloud_coordinator.rs`

**Current Implementation** (Already exists):

```rust
pub fn submit_eviction_task<F>(
    &self,
    runtime: &Arc<EngineRuntime>,
    eviction_fn: F,
) -> MidgeResult<()>
where
    F: Fn() + Send + 'static,
{
    let task = RuntimeTask::new(
        RuntimeTaskKind::Maintenance,
        "cache_eviction".to_string(),
        Box::new(eviction_fn),
    );
    runtime.submit(task)
}
```

**Status**: ✅ Already implemented and tested

## Cache Metrics Enhancement

### New Metrics to Track

```rust
pub struct CacheMetrics {
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    evictions: AtomicU64,
    evicted_bytes: AtomicU64,
    eviction_latency_us: AtomicU64,  // Time for eviction task to execute
}

impl CacheMetrics {
    pub fn hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 { 0.0 } else { hits as f64 / total as f64 }
    }
    
    pub fn avg_eviction_latency_us(&self) -> u64 {
        // Averaged across evictions
    }
}
```

### Observability Points

1. **Cache hit rate**: Percentage of successful cache lookups
2. **Eviction rate**: Evictions per second
3. **Eviction latency**: Time between eviction task submission and execution
4. **Cache utilization**: Current cache size / max capacity

## Testing Strategy for Phase 7.3

### Unit Tests

1. **Test: Eviction submission when cache full**
   - Arrange: Create HybridStorage with small max_capacity
   - Act: Get/fetch multiple blocks beyond capacity
   - Assert: Eviction task submitted to runtime

2. **Test: Eviction task execution**
   - Arrange: Create HybridStorage with runtime coordination
   - Act: Execute eviction task
   - Assert: LRU block removed from cache

3. **Test: Cache hit rate metrics**
   - Arrange: Create HybridStorage with metrics
   - Act: Perform cache hits and misses
   - Assert: Hit rate calculated correctly

### Integration Tests

1. **Test: Deterministic eviction order**
   - Arrange: Create two engines with same workload
   - Act: Run read workload triggering evictions
   - Assert: Both engines evict blocks in same order

2. **Test: Eviction sequencing with flushes**
   - Arrange: Create engine, populate cache
   - Act: Trigger flush and read beyond capacity
   - Assert: Eviction sequenced after flush in runtime task log

3. **Test: Cloud fallback after eviction**
   - Arrange: Create engine, flush SST, reach eviction threshold
   - Act: Evict cached blocks, then read them
   - Assert: Keys read successfully from cloud fallback

## Success Criteria

- [ ] `HybridStorage::get_or_fetch()` submits eviction as runtime task when needed
- [ ] Eviction logic moved into closure executed by runtime
- [ ] Cache metrics track hit rate and eviction rate
- [ ] All Phase 5/6/7.1-7.2 tests still pass
- [ ] New cache eviction tests pass (5-6 tests)
- [ ] Determinism validation: two engines produce same eviction sequence
- [ ] 2329+ total tests at 100% compliance
- [ ] Zero clippy warnings

## Implementation Checklist

Phase 7.3 implementation order:

1. **Cache metrics enhancement** (~20 mins)
   - [ ] Add cache_hits, cache_misses, evictions counters to CacheMetrics
   - [ ] Implement hit_rate() and eviction metrics
   - [ ] Add eviction_latency_us counter

2. **HybridStorage integration** (~40 mins)
   - [ ] Add cloud_coordinator and runtime fields to HybridStorage
   - [ ] Modify constructor to accept coordinator references
   - [ ] Update get_or_fetch() to submit eviction tasks
   - [ ] Extract eviction logic into evict_lru_entry_impl()
   - [ ] Record eviction metrics atomically

3. **Engine initialization** (~15 mins)
   - [ ] Pass cloud_coordinator and runtime to HybridStorage
   - [ ] Ensure proper initialization order in state.rs

4. **Validation** (continuous, ~20 mins)
   - [ ] Run all tests: `cargo test --lib`
   - [ ] Check compliance: `cargo run --bin validate_tests -- --summary`
   - [ ] Add 5-6 new cache eviction tests following AAA pattern
   - [ ] Run clippy: `cargo clippy --all-targets`

5. **Commit** (~5 mins)
   - [ ] Stage changes: `git add -A`
   - [ ] Commit: `git commit -m "phase-7.3: integrate cache eviction through EngineRuntime"`
   - [ ] Update ROADMAP.md with completion status

## Expected Outcome

After Phase 7.3:
- Cache eviction coordinated through EngineRuntime
- Eviction sequencing deterministic
- Cache metrics observable (hit rate, eviction rate, latency)
- All background work (flush, compaction, cloud ops, eviction) through runtime
- Ready for Phase 8 (production hardening)
- 2329+ tests at 100% compliance

## Phase 7 Completion Criteria (7.1 ✅ + 7.2 + 7.3)

After all Phase 7 tasks complete:
- ✅ 7.1: CloudCoordinator infrastructure created and documented
- ⏳ 7.2: Cloud SST uploads wired into runtime coordination
- ⏳ 7.3: Cache eviction coordinated through runtime
- Result: **Hybrid storage fully integrated with deterministic runtime**

## Notes

- HybridStorage may need interior mutability adjustment for cloud_coordinator/runtime refs
- Consider using `Option<Arc<>>` to allow construction without coordinator (testing scenarios)
- Eviction task name should include bytes evicted: `"cache_eviction(1024kb)"`
- Metrics should be recorded BEFORE eviction task returns
- No allocations during runtime task execution (pre-allocate block info)
