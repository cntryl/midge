perfect — here’s a polished first draft of your **`docs/features/hybrid_storage.md`**.
it follows the same documentation voice as your other Midge feature docs: crisp, technical, and structured for deep readers.

```markdown
# Hybrid Storage

## Purpose

Midge’s **Hybrid Storage** layer bridges the gap between small local disks and durable cloud blob storage (S3, Azure Blob, GCS).  
It allows the database to operate efficiently in environments with limited local capacity — such as containers or ephemeral VMs — while still achieving full durability and recoverability via remote blob tiers.

The local or NFS-mounted disk acts purely as a **cache and staging area**, never as the source of truth.

## Design Overview
```

┌────────────────────────────────────────────┐
│ Midge Engine │
│ │
│ WAL / SST I/O → HybridStorage abstraction │
└────────────────────────────────────────────┘
│
▼
┌────────────────────────────┐
│ Local Cache / NFS Mount │
│ - Low latency, small size │
│ - WAL + hot SSTs cached │
└────────────────────────────┘
│
▼
┌────────────────────────────┐
│ Blob Storage Backend │
│ (S3 / Azure Blob / GCS) │
│ - Durable, scalable │
│ - Higher latency │
└────────────────────────────┘

````

The **HybridStorage** abstraction automatically chooses the right tier for each operation:
- **WAL & flushes** → local first, durable copy queued for blob upload.
- **Compactions & reads** → prefer local SSTs; fallback to blob on cache miss.
- **Manifest & metadata** → always durable on blob; optionally cached.



## Core Concepts

### Local Tier
- Small, fast, ephemeral storage (2–20 GB typical).
- Used for:
  - WAL segments
  - Recently flushed SSTs
  - Compaction scratch space
- Can be tmpfs, ephemeral SSD, or mounted NFS.

### Blob Tier
- Backed by S3, Azure Blob, or GCS.
- Durable and scalable, but higher latency.
- Source of truth for all SSTs and manifests.

### Cache Semantics
- Reads hit local tier if present.
- On miss, Midge fetches from blob and stores a local copy.
- Writes are acknowledged after blob upload (depending on policy).
- Local tier may be cleared without data loss.



## Lifecycle

### Write Path

1. Write batch appended to WAL on **local disk**.
2. WAL segment periodically uploaded to **blob storage**.
3. Memtable flush creates SST → written locally → uploaded → manifest updated.
4. Local tier may later evict older SSTs when space limits are reached.

### Read Path

1. Attempt to open file from local cache.
2. On miss, download from blob → verify checksum → store locally.
3. Serve read from local file.
4. Background cleaner enforces cache size constraints.

### Eviction

- `max_local_bytes` defines upper bound.
- Eviction policy: Least-Recently Used (LRU) with optional Bloom-based hints.
- Evicted files deleted locally but remain intact in blob tier.



## Configuration

```toml
[storage]
mode = "hybrid"
local_path = "/mnt/midge-cache"
remote_url = "s3://mybucket/midge/"
max_local_bytes = "2GB"
wal_upload_interval = "30s"
````

Midge can also operate in:

- `local` mode — all data on disk only.
- `remote` mode — fully blob-backed, no local cache.

## Consistency & Durability

| Component | Tier | Durability | Notes |
| | | | |
| WAL | Local + Blob | Durable after upload | upload interval defines RPO |
| SST | Local + Blob | Blob copy authoritative | local acts as cache |
| Manifest | Blob only | Always persisted atomically | versioned edits (TLV) |

If the local cache is lost (container restart, NFS failure), Midge:

- Recovers manifest from blob.
- Rehydrates WAL and SSTs as needed.
- Resumes operation automatically.

## Failure Behavior

| Failure | Effect | Recovery |
| - | -- | |
| Local disk full | New SSTs bypass cache | Continue with blob writes |
| Local disk loss | Rehydrate from blob | No data loss |
| Blob unavailable | Local WAL buffered | Retry with exponential backoff |
| Upload failure | Retries; after TTL → read-only mode | Prevents inconsistent manifest |

## Performance Considerations

| Operation | Local Tier | Blob Tier |
| - | | |
| WAL append | µs latency | not used directly |
| SST flush | fast write, async upload | bulk transfer |
| Compaction | mostly cached | large merges uploaded |
| Read | local hit ≈ 100 µs | blob miss ≈ 10–50 ms |

Tuning parameters:

- Increase `max_local_bytes` for higher cache hit rate.
- Adjust `wal_upload_interval` to trade durability vs. write latency.
- Enable prefetch for sequential scan workloads.

## Metrics

| Metric | Description |
| | - |
| `midge_cache_hit_ratio` | % of reads served from local cache |
| `midge_blob_upload_latency_ms` | Time to upload WAL/SST to blob |
| `midge_local_bytes_used` | Current cache usage |
| `midge_upload_failures_total` | Total failed upload attempts |

These metrics surface via Prometheus and are logged through `slog` with context.

## Related Docs

- [WAL Basics](./wal_basics.md)
- [SST Basics](./sst_basics.md)
- [Locking](./locking.md)
- [Cloud Integration Overview](./cloud_integration/overview.md)
- [Performance](./performance.md)

## Summary

Hybrid Storage allows Midge to:

- Run in ephemeral environments with minimal local capacity.
- Maintain full durability via blob storage.
- Gracefully degrade and self-heal after failures.
- Offer near-local performance through opportunistic caching.

It’s the foundation of Midge’s **“cloud-first but locality-aware”** design.

```


```
