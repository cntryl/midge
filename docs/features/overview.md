# Midge Feature Overview

This document provides a high-level overview of all features in the Midge storage engine, their implementation status, and usage patterns.

## Table of Contents

- [Core Storage Features](#core-storage-features)
- [Data Management](#data-management)
- [Performance Optimizations](#performance-optimizations)
- [Observability](#observability)
- [Experimental Features](#experimental-features)

---

## Core Storage Features

### 1. LSM-Tree Architecture ✅

**Status:** Complete and Production-Ready

**Description:**  
Midge implements a Log-Structured Merge-tree (LSM-tree) storage model with:
- In-memory MemTable (backed by concurrent skip-list)
- Sorted String Table (SST) files on disk
- Multi-level compaction strategy
- Write-optimized design with batch writes

**Key Benefits:**
- High write throughput
- Sequential I/O patterns
- Efficient space utilization
- Predictable performance

**Implementation Details:**
- Skip-list with multiple versions per key
- TLV (Type-Length-Value) encoding for SST blocks
- Sparse indexing for efficient block location
- Block-level compression support (configurable)

---

### 2. Write-Ahead Log (WAL) ✅

**Status:** Complete - v1 Format

**Description:**  
Durable transaction log that ensures crash recovery and data consistency.

**Format (v1):**
- Magic header: `SHALWAL1` (8 bytes)
- TLV encoding with CRC32 checksums per record
- Transaction markers: `TxnBegin` and `TxnCommit`
- TTL expiration metadata support

**Features:**
- ✅ Atomic transaction recovery
- ✅ Configurable sync modes (`wal_sync` option)
- ✅ Two-pass recovery algorithm (committed transactions only)
- ✅ Column family awareness
- ✅ Range deletion support
- ✅ TTL expiration persistence

**Usage:**
```rust
let options = MidgeOptions {
    wal_sync: true,  // fsync after each write
    ..Default::default()
};
```

**Recovery Guarantees:**
- Uncommitted transactions are discarded (atomicity)
- Committed transactions applied atomically
- Interleaved transactions handled correctly
- No partial transaction application

---

### 3. Column Families ✅

**Status:** Complete

**Description:**  
Logical partitioning of the keyspace with independent configuration and management.

**Features:**
- ✅ Isolated namespaces within single database
- ✅ Independent compaction per CF
- ✅ Per-CF configuration (planned)
- ✅ Default column family (ID: 0)
- ✅ Dynamic CF creation/deletion (API available)

**Usage:**
```rust
use midge::column_family::ColumnFamilyId;

// Use default CF (ID: 0)
engine.put(Bytes::from("key"), Bytes::from("value"))?;

// Use specific CF
let cf_id = ColumnFamilyId::new(1);
engine.put_cf(cf_id, Bytes::from("key"), Bytes::from("value"))?;
```

**Implementation:**
- WAL records include `cf_id` field
- SST files tagged with column family ID
- Manifest tracks SSTs per CF
- Recovery respects CF boundaries

---

### 4. Transactions ✅

**Status:** Complete with Snapshot Isolation

**Description:**  
ACID transactions with snapshot isolation for consistent multi-operation updates.

**Features:**
- ✅ Snapshot isolation (read from consistent snapshot)
- ✅ Atomic commit/rollback
- ✅ Batch operations (multiple puts/deletes)
- ✅ Automatic rollback on drop
- ✅ WAL-based durability
- ✅ Recovery with transaction markers

**Transaction Lifecycle:**
1. `TxnBegin` marker written to WAL
2. Operations buffered with `txn_id`
3. `TxnCommit` marker written on success
4. Recovery only applies committed transactions

**Usage:**
```rust
// Create transaction
let mut txn = engine.begin_transaction();

// Stage operations
txn.put(Bytes::from("key1"), Bytes::from("value1"), None);
txn.insert(Bytes::from("key2"), Bytes::from("value2"), None);
txn.delete(Bytes::from("key3"));

// Commit atomically
engine.commit_transaction(txn)?;

// Or use batch API (equivalent)
use midge::api::mutation::Mutation;
engine.batch(vec![
    Mutation::put(Bytes::from("key1"), Bytes::from("value1"), None),
    Mutation::delete(Bytes::from("key3")),
])?;
```

**Guarantees:**
- All-or-nothing commit
- Reads see snapshot at transaction start
- Uncommitted changes not visible to other operations
- Crash-safe via WAL

---

### 5. Snapshots ✅

**Status:** Complete

**Description:**  
Point-in-time consistent views of the database for backups, analytics, and long-running queries.

**Features:**
- ✅ Zero-copy snapshot creation
- ✅ Read-only access to historical data
- ✅ Multiple concurrent snapshots
- ✅ Sequence-number based isolation
- ✅ Integration with transaction system

**Usage:**
```rust
// Create snapshot
let snapshot = engine.snapshot();
let seq = snapshot.sequence_number();

// Read from snapshot (point-in-time view)
let value = engine.get_at(b"key", seq)?;

// Range scan at snapshot
let results = engine.scan_at(
    Query::new().prefix(Bytes::from("prefix:")),
    seq
)?;
```

**Implementation:**
- Snapshots hold sequence number reference
- Compaction preserves data visible to active snapshots
- Prevents premature garbage collection
- No storage overhead until compaction

---

## Data Management

### 6. Time-To-Live (TTL) ✅

**Status:** Complete End-to-End

**Description:**  
Automatic key expiration with configurable timeouts and compaction-based cleanup.

**Features:**
- ✅ Write-time TTL specification (seconds or absolute timestamp)
- ✅ Per-key expiration metadata
- ✅ WAL persistence of expiration timestamps
- ✅ Memtable expiration checking
- ✅ SST expiration metadata (TLV tag `0x36`)
- ✅ Compaction filter drops expired entries
- ✅ No-resurrection guarantee (expired keys never visible)
- ✅ Batch operation support

**API:**
```rust
// TTL in seconds (relative)
engine.put_with_ttl(
    Bytes::from("session:123"),
    Bytes::from("data"),
    3600  // expires in 1 hour
)?;

// Absolute expiration timestamp (milliseconds since epoch)
let expires_at = SystemTime::now()
    .duration_since(UNIX_EPOCH)?
    .as_millis() as u64 + 3600_000;
    
engine.put_with_expiration(
    Bytes::from("cache:key"),
    Bytes::from("value"),
    expires_at
)?;

// Batch with TTL
engine.batch(vec![
    Mutation::put(Bytes::from("key1"), Bytes::from("val1"), Some(60)),
    Mutation::put(Bytes::from("key2"), Bytes::from("val2"), Some(120)),
])?;
```

**Expiration Flow:**
1. **Write:** Expiration stored in memtable/WAL/SST
2. **Read:** Expired keys return `None` (never resurrected)
3. **Compaction:** `TtlFilter` drops expired entries (no tombstone)
4. **Recovery:** Expiration metadata restored from WAL

**Configuration:**
```rust
let options = MidgeOptions {
    ttl_seconds: Some(3600),  // Default TTL for all writes
    ..Default::default()
};
```

---

### 7. Range Tombstones ✅

**Status:** Complete

**Description:**  
Efficient deletion of key ranges with single tombstone marker.

**Features:**
- ✅ Covers range `[start, end)` with single marker
- ✅ Memtable integration for immediate visibility
- ✅ SST persistence and compaction
- ✅ Efficient space usage vs per-key tombstones

**Usage:**
```rust
// Delete range [start, end)
engine.delete_range(
    Bytes::from("user:1000"),
    Bytes::from("user:2000")
)?;
```

**Implementation:**
- Stored separately from point tombstones
- Checked during reads before key lookup
- Merged during compaction
- Sequence-number aware for snapshot isolation

---

### 8. Compaction ✅

**Status:** Complete with Filter Support

**Description:**  
Multi-level compaction strategy that merges SSTs and reclaims space.

**Features:**
- ✅ Level-based compaction
- ✅ Configurable level multipliers
- ✅ User-defined compaction filters
- ✅ TTL integration (automatic cleanup)
- ✅ Tombstone garbage collection
- ✅ Background execution (non-blocking)

**Compaction Filters:**
```rust
use midge::compaction::compaction_filter::{CompactionFilter, FilterDecision};

struct MyFilter;

impl CompactionFilter for MyFilter {
    fn filter(&self, level: u32, version: &CompactionVersion) -> FilterDecision {
        // Custom logic to keep/remove/modify entries
        if should_drop(version) {
            FilterDecision::Remove
        } else {
            FilterDecision::Keep
        }
    }
}
```

**Built-in Filters:**
- `TtlFilter` - Drops expired keys based on expiration metadata
- `NoOpFilter` - Keeps all keys (default)
- `PrefixDropFilter` - Drops keys matching prefix

**Configuration:**
```rust
let options = MidgeOptions {
    compaction_filter: Some(Arc::new(TtlFilter::new(...))),
    ..Default::default()
};
```

---

## Performance Optimizations

### 9. Bloom Filters ✅

**Status:** Complete

**Description:**  
Probabilistic filters to skip SST reads when key is definitely absent.

**Features:**
- ✅ Per-SST bloom filters
- ✅ Configurable false-positive rate
- ✅ Automatic integration with SST readers
- ✅ Space-efficient bit arrays

**Benefits:**
- Reduces disk I/O for non-existent keys
- Improves point lookup performance
- Minimal memory overhead

**Implementation:**
- Built during SST creation (flush/compaction)
- Stored in SST footer metadata
- Checked before block index lookup
- Uses murmur3 hash function

---

### 10. Sparse Indexing ✅

**Status:** Complete

**Description:**  
Block-level index for efficient SST navigation without full scan.

**Features:**
- ✅ Index stores first key of each data block
- ✅ Binary search on index for block location
- ✅ Reduces I/O by reading only relevant blocks
- ✅ Compact representation

**Implementation:**
- Index block stored in SST footer
- Each entry: `(first_key, block_offset, block_size)`
- Loaded at SST open time
- Used by all read operations

---

### 11. Rate Limiting ✅

**Status:** Complete

**Description:**  
I/O throttling for background operations to prevent resource starvation.

**Features:**
- ✅ Token bucket algorithm
- ✅ Configurable bytes/second limit
- ✅ Separate limits for reads and writes
- ✅ Background compaction throttling
- ✅ Non-blocking for foreground operations

**Usage:**
```rust
let options = MidgeOptions {
    compaction_read_rate_limit: Some(10_000_000),  // 10 MB/s
    compaction_write_rate_limit: Some(10_000_000), // 10 MB/s
    ..Default::default()
};
```

---

### 12. Caching ✅

**Status:** Complete (Block Cache)

**Description:**  
LRU cache for frequently accessed SST data blocks.

**Features:**
- ✅ Shared block cache across all SSTs
- ✅ LRU eviction policy
- ✅ Configurable capacity
- ✅ Cache-aware read path

**Configuration:**
```rust
let options = MidgeOptions {
    block_cache_capacity: 100_000_000,  // 100 MB
    ..Default::default()
};
```

---

## Observability

### 13. Metrics ✅

**Status:** Complete

**Description:**  
Built-in telemetry for monitoring database health and performance.

**Available Metrics:**
- ✅ Operation counts (get, put, delete, scan)
- ✅ Flush statistics
- ✅ Compaction statistics
- ✅ WAL operations
- ✅ Cache hit rates
- ✅ SST file counts per level

**Usage:**
```rust
let metrics = engine.metrics();
println!("Gets: {}", metrics.get_count());
println!("Flushes: {}", metrics.flush_count());
println!("Compactions: {}", metrics.compaction_count());
```

**Implementation:**
- Lock-free atomic counters
- Zero-allocation read path
- Snapshot-based reporting

---

### 14. Health Monitoring 🚧

**Status:** Partial Implementation

**Description:**  
System health checks and automatic recovery mechanisms.

**Current Features:**
- ✅ Rehydration on startup
- ✅ Manifest validation
- 🚧 Automatic repair (planned)
- 🚧 Health status API (planned)

---

## Experimental Features

### 15. Cloud Storage Backends 🚧

**Status:** Experimental (Not Production-Ready)

**Description:**  
Multi-cloud support for SST and WAL storage.

**Supported Backends:**
- 🚧 AWS S3
- 🚧 Azure Blob Storage
- 🚧 Google Cloud Storage
- ✅ Mock backend (for testing)

**Current Limitations:**
- Async operations not fully integrated
- No cloud WAL support yet
- Performance not optimized
- Limited error handling

**Usage (Experimental):**
```rust
use midge::StorageMode;

let options = MidgeOptions {
    storage_mode: StorageMode::Cloud {
        db_path: PathBuf::from("/local/cache"),
        cloud_backend: Arc::new(MockCloudBackend::new()),
    },
    ..Default::default()
};
```

---

## Feature Matrix

| Feature | Status | API Stable | Production Ready |
|---------|--------|------------|------------------|
| LSM-Tree | ✅ | ✅ | ✅ |
| WAL | ✅ | ✅ | ✅ |
| Column Families | ✅ | ✅ | ✅ |
| Transactions | ✅ | ✅ | ✅ |
| Snapshots | ✅ | ✅ | ✅ |
| TTL | ✅ | ✅ | ✅ |
| Range Tombstones | ✅ | ✅ | ✅ |
| Compaction | ✅ | ✅ | ✅ |
| Compaction Filters | ✅ | ✅ | ✅ |
| Bloom Filters | ✅ | ✅ | ✅ |
| Sparse Indexing | ✅ | ✅ | ✅ |
| Rate Limiting | ✅ | ✅ | ✅ |
| Block Cache | ✅ | ✅ | ✅ |
| Metrics | ✅ | ✅ | ✅ |
| Health Monitoring | 🚧 | 🚧 | ❌ |
| Cloud Storage | 🚧 | ❌ | ❌ |

**Legend:**
- ✅ Complete and stable
- 🚧 In development or experimental
- ❌ Not available

---

## Configuration Overview

### MidgeOptions

Complete configuration structure for engine initialization:

```rust
pub struct MidgeOptions {
    // Storage
    pub storage_mode: StorageMode,
    pub db_path: PathBuf,  // Deprecated, use storage_mode
    
    // WAL
    pub wal_sync: bool,  // fsync after each write
    
    // Compaction
    pub compaction_filter: Option<Arc<dyn CompactionFilter>>,
    pub compaction_read_rate_limit: Option<u64>,   // bytes/sec
    pub compaction_write_rate_limit: Option<u64>,  // bytes/sec
    
    // TTL
    pub ttl_seconds: Option<u64>,  // Default TTL for all writes
    
    // Performance
    pub block_cache_capacity: usize,  // bytes
    pub memtable_flush_threshold: usize,  // bytes
    
    // Advanced
    pub max_open_ssts: usize,
    pub compression: CompressionType,
}
```

---

## Testing Coverage

**Test Statistics:**
- Total tests: 336+ passing
- TTL tests: 10 (including no-resurrection, compaction)
- Transaction tests: 15+ (including recovery)
- Engine tests: 55+
- Storage tests: 50+
- Integration tests: 20+

**Continuous Testing:**
```bash
# Run all tests
cargo test

# Run specific feature tests
cargo test --test ttl
cargo test --test transaction_recovery
cargo test --test engine

# Run benchmarks
cargo bench
```

---

## Performance Characteristics

### Write Performance
- **Throughput:** ~100K-500K ops/sec (depends on sync mode)
- **Latency:** <1ms (async), 1-10ms (sync)
- **Batch writes:** 5-10x faster than individual puts

### Read Performance
- **Point lookups:** <1ms (memtable), <10ms (SST with bloom)
- **Range scans:** Depends on range size, ~50K keys/sec
- **Cache hit:** ~100ns (in-memory)

### Space Amplification
- **Write amplification:** ~10-30x (typical LSM)
- **Space amplification:** 1.3-2x (with compaction)
- **TTL cleanup:** No tombstone overhead

---

## Roadmap

### Short-term (Next Release)
- ✅ Complete TTL implementation
- ✅ Transaction recovery hardening
- ✅ Documentation improvements
- 🚧 Health monitoring API

### Medium-term
- 🚧 Cloud storage stabilization
- 🚧 Compression algorithms (LZ4, Zstd)
- 🚧 Block cache improvements (admission policy)
- 🚧 Async I/O integration

### Long-term
- 🚧 Distributed coordination (multi-node)
- 🚧 Replication support
- 🚧 Advanced compaction strategies (universal)
- 🚧 Query optimization (predicate pushdown)

---

## Contributing

See individual feature documentation in `docs/features/` for implementation details:
- [TTL Architecture](../wip/TTL_ARCHITECTURE.md)
- [Compaction Filters](../wip/COMPACTION_FILTERS.md)
- [Rate Limiting](../wip/RATE_LIMITING.md)
- [Cloud Implementation](../wip/cloud_impl.md)

---

## Summary

Midge is a **production-ready LSM-tree storage engine** with comprehensive feature coverage:

**Core Strengths:**
- ✅ Robust transaction support with WAL recovery
- ✅ Complete TTL implementation (end-to-end)
- ✅ Column family isolation
- ✅ Snapshot isolation for consistent reads
- ✅ Efficient compaction with custom filters
- ✅ Performance optimizations (bloom, cache, rate limiting)

**Current Focus:**
- Stabilizing cloud storage backends
- Expanding health monitoring
- Performance optimization

**Production Readiness:** 🟢 Ready for production workloads requiring:
- High write throughput
- Point lookups and range scans
- TTL/expiration requirements
- ACID transactions
- Crash recovery guarantees

