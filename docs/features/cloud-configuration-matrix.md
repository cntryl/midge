# Cloud Storage Configuration Matrix

## Durability × Storage Mode Matrix

| Durability Mode | CloudMode | Cache Size | WAL Sync | Upload Mode | RPO | Use Case |
|----------------|-----------|------------|----------|-------------|-----|----------|
| **Strict** | Cache | 1024 MB | ✅ Yes | Sync (blocking) | **0** (no loss) | Financial transactions, critical metadata |
| **Steady** | Cache | 1024 MB | ✅ Yes | Async (20ms intervals) | ~20ms | High-throughput OLTP, general purpose |
| **CloudReplicated** | Tiered | 256 MB | ❌ No | Async (100ms intervals) | ~100ms | Distributed systems, containerized apps |

## Detailed Configuration Breakdown

### Strict Durability (Zero Data Loss)

```rust
CloudConfigBuilder::strict_durability(backend, "./cache")
```

| Setting | Value | Rationale |
|---------|-------|-----------|
| CloudMode | `Cache` | All SSTs cached locally for fast reads |
| Cache Size | 1024 MB (default) | Large cache to hold working set |
| Local WAL Sync | ✅ Enabled | fsync() ensures local durability |
| Cloud Upload | **Synchronous** | Blocks until cloud confirms write |
| WAL Batch Size | 256 KB | Small batches (frequent uploads) |
| Sync Interval | N/A | Every write synced immediately |
| Background Workers | ✅ Enabled | Process uploads, evict cache |
| **RPO** | **0** | No data loss on node failure |
| **Latency** | High (cloud RTT) | Blocks on cloud round-trip |

**Example:**
```rust
let backend = Arc::new(AwsS3Backend::new("us-east-1", "bucket", None)?);
let storage = CloudConfigBuilder::strict_durability(backend, "./cache")
    .with_max_cache_size_mb(2048)  // Optional: increase cache
    .with_path("financial-db")      // Optional: hierarchical path
    .build();
```

---

### Steady Durability (Balanced Performance)

```rust
CloudConfigBuilder::balanced_durability(backend, "./cache")
```

| Setting | Value | Rationale |
|---------|-------|-----------|
| CloudMode | `Cache` | All SSTs cached locally for fast reads |
| Cache Size | 1024 MB (default) | Large cache to hold working set |
| Local WAL Sync | ✅ Enabled | fsync() ensures local durability |
| Cloud Upload | **Asynchronous** | Background thread uploads |
| WAL Batch Size | 2 MB | Larger batches (efficient uploads) |
| Sync Interval | 20 ms | Upload every 20ms |
| Background Workers | ✅ Enabled | Process uploads, evict cache |
| **RPO** | ~20ms | Lose up to 20ms of writes on crash |
| **Latency** | Low (local ack) | Returns immediately after local write |

**Example:**
```rust
let backend = Arc::new(AzureBlobBackend::new("account", "container", None)?);
let storage = CloudConfigBuilder::balanced_durability(backend, "./cache")
    .with_sync_interval_ms(50)     // Optional: less frequent syncs
    .with_max_cache_size_mb(4096)  // Optional: larger cache
    .build();
```

---

### Cloud-Replicated Durability (Cloud-First)

```rust
CloudConfigBuilder::replicated_durability(backend, "./cache")
```

| Setting | Value | Rationale |
|---------|-------|-----------|
| CloudMode | `Tiered` | **Only hot SSTs cached locally** |
| Cache Size | 256 MB (default) | Small cache (ephemeral, cloud-first) |
| Local WAL Sync | ❌ Disabled | Cloud is source of truth |
| Cloud Upload | **Asynchronous** | Background thread uploads |
| WAL Batch Size | 4 MB | Largest batches (minimize API calls) |
| Sync Interval | 100 ms | Less frequent syncs |
| Background Workers | ✅ Enabled | Process uploads, evict cache |
| **RPO** | ~100ms | Lose up to 100ms of writes on crash |
| **Latency** | Lowest (no fsync) | No local disk sync overhead |

**Example:**
```rust
let backend = Arc::new(GcpStorageBackend::new("bucket", None)?);
let storage = CloudConfigBuilder::replicated_durability(backend, "./cache")
    .with_local_cache_enabled(true)   // Optional: enable cache
    .with_max_cache_size_mb(128)      // Optional: tiny cache
    .build();
```

---

## Cache Behavior Comparison

### Cache Mode (Strict & Steady)

```
Max Cache: 1024 MB (large)
Eviction: LRU when cache exceeds limit
Strategy: Cache everything, evict oldest on pressure

┌─────────────────────────────────────┐
│   Local Cache (1024 MB)             │
│   ┌─────────────────────────────┐   │
│   │ SST-001 (hot)    ✅ cached  │   │
│   │ SST-002 (warm)   ✅ cached  │   │
│   │ SST-003 (warm)   ✅ cached  │   │
│   │ SST-004 (cold)   ✅ cached  │   │
│   │ SST-005 (cold)   ✅ cached  │   │
│   └─────────────────────────────┘   │
│   All SSTs fit in large cache       │
└─────────────────────────────────────┘
```

### Tiered Mode (Cloud-Replicated)

```
Max Cache: 256 MB (small)
Eviction: LRU when cache exceeds limit
Strategy: Cache hot data only, cold data cloud-only

┌─────────────────────────────────────┐
│   Local Cache (256 MB)              │
│   ┌─────────────────────────────┐   │
│   │ SST-001 (hot)    ✅ cached  │   │
│   │ SST-002 (warm)   ✅ cached  │   │
│   └─────────────────────────────┘   │
│                                     │
│   Cloud Only (not cached):          │
│   │ SST-003 (cold)   ☁️  cloud   │   │
│   │ SST-004 (cold)   ☁️  cloud   │   │
│   │ SST-005 (cold)   ☁️  cloud   │   │
└─────────────────────────────────────┘
```

---

## Performance Characteristics

| Metric | Strict | Steady | Cloud-Replicated |
|--------|--------|--------|------------------|
| **Write Latency** | 🔴 High (50-200ms) | 🟢 Low (<1ms) | 🟢 Lowest (<0.5ms) |
| **Read Latency (cached)** | 🟢 Low | 🟢 Low | 🟢 Low |
| **Read Latency (uncached)** | 🟡 Medium (cloud fetch) | 🟡 Medium (cloud fetch) | 🔴 High (cloud fetch) |
| **Durability** | 🟢 Zero loss | 🟡 ~20ms loss | 🟡 ~100ms loss |
| **Disk Usage** | 🔴 High (1GB+) | 🔴 High (1GB+) | 🟢 Low (256MB) |
| **Cloud API Calls** | 🔴 High (every write) | 🟢 Low (batched) | 🟢 Lowest (largest batches) |
| **Best For** | Mission-critical | General purpose | Ephemeral/Cloud-native |

---

## Customization Examples

### Custom Cache Size

```rust
// Strict mode with 4GB cache
CloudConfigBuilder::strict_durability(backend, "./cache")
    .with_max_cache_size_mb(4096)
    .build()

// Tiered mode with 64MB cache (aggressive tiering)
CloudConfigBuilder::replicated_durability(backend, "./cache")
    .with_max_cache_size_mb(64)
    .build()
```

### Custom Sync Intervals

```rust
// Steady mode with 50ms intervals (lower throughput, better durability)
CloudConfigBuilder::balanced_durability(backend, "./cache")
    .with_sync_interval_ms(50)
    .build()

// Cloud-replicated with 500ms intervals (maximize batching)
CloudConfigBuilder::replicated_durability(backend, "./cache")
    .with_sync_interval_ms(500)
    .build()
```

### Hierarchical Organization

```rust
// Multi-tenant deployment
CloudConfigBuilder::strict_durability(backend, "./cache")
    .with_path("tenant-abc/production")  // midge/tenant-abc/production/wal/...
    .build()

// Environment separation
CloudConfigBuilder::balanced_durability(backend, "./cache")
    .with_path("staging/us-west")  // midge/staging/us-west/sst/...
    .build()
```

### Disable Local Cache (Cloud-Only)

```rust
// Cloud-only mode (no local cache at all)
CloudConfigBuilder::replicated_durability(backend, "./cache")
    .with_local_cache_enabled(false)
    .build()
```

---

## Decision Tree

```
START: What are your requirements?
│
├─ Need ZERO data loss on crash?
│  └─ ✅ Use: Strict Durability
│     (High latency, max safety)
│
├─ Need low latency + good durability?
│  └─ ✅ Use: Steady Durability
│     (Balanced, general purpose)
│
├─ Running in ephemeral containers?
│  └─ ✅ Use: Cloud-Replicated
│     (Cloud-first, minimal disk)
│
└─ Custom requirements?
   └─ Start with Steady, customize:
      - Increase cache_size for read-heavy
      - Decrease sync_interval for better durability
      - Enable CloudMode::Tiered for tiering
```

---

## Quick Reference Commands

```rust
// STRICT: Zero loss, high latency
let storage = CloudConfigBuilder::strict_durability(backend, "./cache").build();

// STEADY: Balanced (most common choice)
let storage = CloudConfigBuilder::balanced_durability(backend, "./cache").build();

// CLOUD-REPLICATED: Cloud-first, minimal disk
let storage = CloudConfigBuilder::replicated_durability(backend, "./cache").build();
```

All modes support:
- ✅ Any cloud backend (MockCloudBackend, S3, Azure, GCP)
- ✅ HybridStorage with local caching
- ✅ LRU eviction
- ✅ Background upload/eviction workers
- ✅ Hierarchical path organization
- ✅ Crash recovery from cloud

