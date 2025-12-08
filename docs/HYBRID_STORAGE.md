# Hybrid Storage Architecture & Cloud Integration

## Overview

Midge implements a two-tier storage system:
1. **Primary Durability**: Cloud object store (S3, Azure Blob, GCS, OCI)
2. **Performance Cache**: Local NVMe (optional, configurable LRU eviction)

This document describes how these tiers integrate with the deterministic runtime model and flush/compaction pipelines.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    Write & Read Operations                  │
│              (memtable, block cache, reads)                 │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                  EngineRuntime Executor                     │
│          (Flush, Compaction, WAL Sync, Cloud Ops)          │
└─────────────────────────────────────────────────────────────┘
        ↙          ↙           ↙              ↙
    Flush     Compaction    WAL Sync      Cloud Ops
      ↓            ↓            ↓              ↓
┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐
│Memtable→ │  │Compaction│  │WAL Write │  │CloudCoordinator
│Segment→  │  │Pipeline  │  │& Upload  │  │+ SST Upload/
│SST Write │  │          │  │          │  │  Download
└──────────┘  └──────────┘  └──────────┘  └──────────────┘
      ↓            ↓            ↓              ↓
    LOCAL SST    LOCAL SST    LOCAL WAL    CLOUD STORAGE
  (cached)       (cached)     (ephemeral)   (durable)
      ↓            ↓            ↓
┌──────────────────────────────────────────────────────────────┐
│                  HybridStorage Layer                         │
│         (Local cache + Cloud tier with fallback)            │
└──────────────────────────────────────────────────────────────┘
```

## Phase 7 Tasks & Integration Points

### Phase 7.1: Cloud Storage Coordination Foundation ✅ COMPLETE

**What**: Created CloudCoordinator module as interface for runtime-coordinated cloud operations.

**Integration**:
```rust
// CloudCoordinator provides three task submission methods:
coordinator.submit_sst_upload_task(&runtime, sst_id, move || { /* upload */ })
coordinator.submit_sst_download_task(&runtime, sst_id, move || { /* download */ })
coordinator.submit_eviction_task(&runtime, move || { /* evict */ })
```

**Current State**: Infrastructure ready, integrated into MidgeEngine.

---

### Phase 7.2: Cloud SST as Primary Storage

**Current Implementation**: SST cloud uploads already implemented via `spawn_cloud_upload()` in flush pipeline.

**Integration Path**:
1. Flush creates local SST
2. Manifest tracked with SST metadata
3. `spawn_cloud_upload()` spawns background thread for async upload
4. CloudSstManager handles upload throttling and retry logic

**CloudCoordinator Integration Points** (Available for use):
- Line 224-245 in `src/core/persistence/flush/process.rs`: `spawn_cloud_upload()`
- Could be wrapped with `CloudCoordinator::submit_sst_upload_task()` for runtime coordination
- Currently uses guarded spawn for test safety

**Next Steps for Full Integration**:
1. Route SST uploads through CloudCoordinator when runtime available
2. Track upload progress in manifest metadata
3. Coordinate SST downloads during recovery from cloud

---

### Phase 7.3: Cache Eviction as Runtime Task

**Current Implementation**: HybridStorage already has eviction logic.

**Location**: `src/cloud/hybrid.rs` - HybridStorage implements LRU eviction.

**Integration Path**:
1. Cache reaches threshold (max_local_bytes)
2. Eviction decision made by HybridStorage
3. CloudCoordinator::submit_eviction_task() submits to runtime
4. Runtime executor runs eviction as Maintenance task
5. Manifest updated with cache state

**Determinism Guarantee**:
- Eviction policy is deterministic (LRU based on access timestamps)
- Runtime ensures sequential execution with other operations
- Cache hits/misses tracked in metrics

---

## Read Path: Cloud Fallback

When reading an SST:

1. **Check Local Cache**: Fast path, no network
2. **Cloud Fallback**: If not in local cache
   - Fetch from cloud via CloudSstManager
   - Cache locally (LRU eviction on threshold)
   - Return to caller
3. **Block Cache**: Operates on fetched blocks
   - Transparent whether blocks came from local cache or cloud

```rust
// Read path flow (simplified)
fn get(key) -> Result {
    if let Some(value) = memtable.get(key) {
        return Ok(value);
    }
    
    for segment in segments.iter() {
        if segment.bloom.may_contain(key) {
            // Segment manages its own cloud fallback
            if let Some(value) = segment.get(key) {
                return Ok(value);
            }
        }
    }
    
    for sst in sst_set.iter() {
        // HybridStorage handles cloud fallback transparently
        if let Some(reader) = sst.reader() {
            if let Some(value) = reader.get(key) {
                return Ok(value);
            }
        }
    }
    
    Ok(None)
}
```

---

## Write Path: Local→Cloud Pipeline

### Flush Pipeline

```
Memtable
    ↓
Segment Creation (Phase 5 integration)
    ↓
SST Write (Local)
    ↓
Manifest Update (atomic)
    ↓
async: Cloud Upload (CloudCoordinator + spawn_cloud_upload)
    ↓
Cloud Checkpoint (when all SSTs uploaded)
    ↓
WAL Pruning (safe after cloud checkpoint)
```

### Compaction Pipeline

```
Read from SSTs (cloud fallback if needed)
    ↓
Merge & Rewrite
    ↓
New SST Write (Local)
    ↓
Manifest Update (atomic)
    ↓
async: Cloud Upload (CloudCoordinator + spawn_cloud_upload)
    ↓
Delete Old SST (local and cloud)
    ↓
Cache Eviction (if needed, CloudCoordinator)
```

---

## Determinism Through Runtime

### What's Deterministic

1. **SST Creation Order**: Same memtable state → same SST sequence
2. **Compaction Decisions**: Same manifest → same compaction plan
3. **Cloud Upload Ordering**: Runtime executor processes uploads in submission order
4. **Cache Eviction Order**: LRU policy is deterministic

### What's Not (By Design)

1. **Cloud Upload Latency**: Network variability, but ordering preserved
2. **Cloud Download Timing**: On-demand based on read access
3. **Local Cache State**: Varies with access patterns, but correctness unaffected

---

## Configuration: Local Cache Size

```rust
MidgeOptions {
    // Cloud storage configuration
    storage_mode: CloudBacked {
        provider: "s3",
        region: "us-west-2",
        bucket: "my-bucket",
        local_wal_sync: false,  // Don't wait for cloud WAL uploads on sync()
    },
    
    // Cache configuration
    cache_size_mb: 1024,        // Block cache (in-memory)
    table_cache_size: 1000,     // SST reader cache
    
    // Hybrid storage configuration (in cloud backend)
    max_local_cache_bytes: 10 * 1024 * 1024 * 1024,  // 10GB local NVMe cache
    
    // Eviction policy
    cache_eviction_policy: LRU,
    cache_eviction_threshold: 0.9,  // Evict when 90% full
}
```

---

## Metrics & Observability

CloudMetrics tracked in HybridStorage:
- `cache_hits`: SSTs read from local cache
- `cache_misses`: SSTs fetched from cloud
- `uploads_completed`: SSTs successfully uploaded to cloud
- `uploads_failed`: SST upload failures
- `files_evicted`: Local cache evictions
- `upload_latencies_ms`: Recent upload latency samples

---

## Failure Scenarios

### Network Failure During Upload

1. CloudSstManager implements retry logic (exponential backoff)
2. Failed upload doesn't block flush (async operation)
3. Manifest marks SST as "pending_upload"
4. Recovery reads from cloud on restart (eventual consistency)

### Network Failure During Download

1. Block cache miss → Cloud fallback
2. Download fails → Return to application with error
3. Application retries or fails read operation
4. No data loss (cloud is source of truth)

### Local Cache Eviction

1. LRU eviction doesn't affect correctness
2. Next read of evicted SST fetches from cloud
3. No data loss, only performance impact

---

## Performance Characteristics

### Latencies

- **Local SST Read**: ~1-10ms (SSD)
- **Cloud SST Download**: ~50-500ms (network dependent)
- **Block Cache Hit**: <1ms
- **Flush (including cloud upload)**: ~100-1000ms (depends on SST size + network)

### Throughput

- **Write Throughput**: Limited by memtable size + flush frequency (not cloud)
- **Read Throughput**: Unlimited for cache hits, ~10-100 Mbps for cloud fetches
- **Cloud Upload**: Rate-limited via global rate limiter (configurable)

---

## Specification Alignment

This implementation aligns with THE_BIG_IDEA.md:
- ✅ Central runtime owns all background operations
- ✅ Deterministic flush & compaction sequences
- ✅ Cloud as primary durability
- ✅ Local cache as optional performance layer
- ✅ Transparent cloud fallback on cache miss

---

## Future Enhancements

1. **Intelligent Prefetching**: Predict next SSTs based on read patterns
2. **Adaptive Cache Sizing**: Adjust local cache based on hit rates
3. **Regional Replication**: Multi-region cloud redundancy
4. **Compression in Cloud**: Store compressed, decompress on download
5. **Incremental SST Upload**: Upload blocks as they're written
