# Midge Operations Guide

## Overview

This guide provides operators with detailed instructions for running, debugging, tuning, and monitoring Midge in production. It complements the architecture documentation (see `docs/ARCHITECTURE.md`) with practical operational procedures.

---

## Table of Contents

1. [Getting Started](#getting-started)
2. [Configuration](#configuration)
3. [Monitoring & Observability](#monitoring--observability)
4. [Performance Tuning](#performance-tuning)
5. [Debugging](#debugging)
6. [Troubleshooting](#troubleshooting)
7. [Recovery & Safety](#recovery--safety)
8. [Best Practices](#best-practices)

---

## Getting Started

### Installation & Initialization

```rust
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use std::path::PathBuf;

// Create engine with local disk storage
let opts = MidgeOptions {
    storage_mode: StorageMode::LocalDisk {
        db_path: PathBuf::from("/data/midge"),
    },
    ..Default::default()
};

let engine = MidgeEngine::open(opts)
    .expect("Failed to open Midge engine");
```

### Column Families

```rust
// Get default column family
let cf_default = engine.default_column_family();

// Create custom column family
let cf_orders = engine.create_column_family(
    "orders",
    Default::default()
).expect("Failed to create CF");

// Operations are CF-specific
engine.put(&cf_orders, b"order_123", b"data")
    .expect("Failed to write");
```

### Basic Operations

```rust
let cf = engine.default_column_family();

// Write
engine.put(&cf, b"key", b"value")
    .expect("Write failed");

// Read
let value = engine.get(&cf, b"key")
    .expect("Read failed");
assert_eq!(value, Some(b"value".into()));

// Delete
engine.delete(&cf, b"key")
    .expect("Delete failed");

// Range operations
let iter = engine.scan(&cf, None, None)
    .expect("Scan failed");
for (k, v) in iter {
    println!("Key: {:?}, Value: {:?}", k, v);
}
```

---

## Configuration

### MidgeOptions Reference

```rust
pub struct MidgeOptions {
    /// Storage backend: Memory, LocalDisk, or CloudBacked
    pub storage_mode: StorageMode,
    
    /// Maximum memtable size before flush (bytes)
    pub memtable_size_bytes: u64,
    
    /// Number of memtable copies before blocking writes
    pub max_memtables: usize,
    
    /// Level multiplier for LSM structure
    pub level_multiplier: u32,
    
    /// Compression codec for SST blocks
    pub compression: CompressionType,
    
    /// Block cache size (bytes)
    pub block_cache_size_bytes: u64,
    
    /// Enable paranoid mode (extra validation)
    pub paranoid_mode: bool,
    
    /// Enable engine runtime coordination
    pub single_executor_runtime: bool,
}
```

### Recommended Configurations

#### Development/Testing

```rust
MidgeOptions {
    storage_mode: StorageMode::Memory,
    memtable_size_bytes: 4 * 1024 * 1024,      // 4 MB
    max_memtables: 2,
    block_cache_size_bytes: 16 * 1024 * 1024,  // 16 MB
    ..Default::default()
}
```

#### Production (OLTP)

```rust
MidgeOptions {
    storage_mode: StorageMode::LocalDisk { db_path },
    memtable_size_bytes: 64 * 1024 * 1024,     // 64 MB
    max_memtables: 4,
    block_cache_size_bytes: 512 * 1024 * 1024, // 512 MB
    compression: CompressionType::Snappy,
    paranoid_mode: true,
    ..Default::default()
}
```

#### Cloud-Backed (Hybrid)

```rust
MidgeOptions {
    storage_mode: StorageMode::CloudBacked {
        local_cache_path: PathBuf::from("/tmp/midge_cache"),
        cloud_backend: CloudBackend::S3 { /* ... */ },
    },
    memtable_size_bytes: 32 * 1024 * 1024,
    block_cache_size_bytes: 256 * 1024 * 1024,
    // Cloud coordination ready (Phase 7.1+)
    ..Default::default()
}
```

---

## Monitoring & Observability

### Metrics Collection

Midge exposes metrics through the `Metrics` interface:

```rust
let metrics = engine.metrics();

// Memtable metrics
println!("Active memtables: {}", metrics.active_memtables);
println!("Immutable memtables: {}", metrics.immutable_memtables);
println!("Total memtable size: {} bytes", metrics.total_memtable_bytes);

// LSM metrics
println!("Total files: {}", metrics.total_sst_files);
println!("L0 files: {}", metrics.l0_files);
println!("Total SST size: {} bytes", metrics.total_sst_bytes);

// Cache metrics
println!("Block cache hits: {}", metrics.block_cache_hits);
println!("Block cache misses: {}", metrics.block_cache_misses);
println!("Cache hit ratio: {:.2}%", metrics.cache_hit_ratio() * 100.0);

// Operation counters
println!("Puts: {}", metrics.total_puts);
println!("Gets: {}", metrics.total_gets);
println!("Deletes: {}", metrics.total_deletes);
```

### Log Levels

Configure logging to monitor engine activity:

```bash
# Set log level via environment variable
RUST_LOG=cntryl_midge=debug cargo run

# Component-specific logging
RUST_LOG=cntryl_midge::core::runtime=trace,cntryl_midge::core::persistence=debug
```

### Manifest Inspection

Check engine state via manifest snapshot:

```rust
let manifest = engine.get_manifest();

// View LSM structure
for (cf_id, cf_name) in &manifest.column_families {
    println!("Column Family: {} (ID={})", cf_name, cf_id);
    
    // Files by level
    let mut by_level = std::collections::BTreeMap::new();
    for file in &manifest.files {
        if file.cf_id == *cf_id {
            by_level.entry(file.level)
                .or_insert_with(Vec::new)
                .push(&file.name);
        }
    }
    
    for (level, files) in by_level {
        println!("  L{}: {} files", level, files.len());
    }
}
```

---

## Performance Tuning

### Write Performance Optimization

#### 1. Batch Operations

```rust
// ❌ SLOW: Individual puts
for item in items.iter() {
    engine.put(&cf, &item.key, &item.value)?;
}

// ✅ FAST: Write batch
let mut batch = WriteBatch::new();
for item in items.iter() {
    batch.put(item.key.clone(), item.value.clone());
}
engine.write(&cf, &batch)?;
```

#### 2. Memtable Size

Larger memtables reduce flush frequency:

```rust
// Larger memtables = more compaction work, fewer flushes
MidgeOptions {
    memtable_size_bytes: 128 * 1024 * 1024,  // 128 MB
    ..Default::default()
}
```

#### 3. Flush Behavior

Control flush aggressiveness:

```rust
// More memtables = allows more write parallelism before stall
MidgeOptions {
    max_memtables: 4,  // Default is typically 2-3
    ..Default::default()
}
```

### Read Performance Optimization

#### 1. Block Cache

```rust
// Larger block cache = more hits for repeated reads
MidgeOptions {
    block_cache_size_bytes: 1024 * 1024 * 1024,  // 1 GB
    ..Default::default()
}

// Monitor effectiveness
let metrics = engine.metrics();
if metrics.cache_hit_ratio() < 0.8 {
    println!("WARNING: Cache hit ratio below 80%, consider increasing block_cache_size_bytes");
}
```

#### 2. Prefix Trie Index

The prefix-trie index (Phase 3) is automatically used for new SSTs:

```rust
// Trie index advantages:
// - O(prefix_length) key lookups vs O(log N) for block index
// - Fast range scans on clustered keys
// - Auto-detected by readers, no configuration needed
```

#### 3. Range Scans

```rust
// Efficient range scans use trie index
let start = Some(b"user_001");
let end = Some(b"user_999");

for (key, value) in engine.scan(&cf, start, end)? {
    // Trie index skips irrelevant blocks
    process(&key, &value);
}
```

### Compaction Tuning

#### 1. Level Multiplier

Controls LSM structure growth:

```rust
// Default: 10x per level (L1=10MB, L2=100MB, L3=1GB)
MidgeOptions {
    level_multiplier: 10,
    ..Default::default()
}

// More aggressive: 5x per level (more files, smaller compactions)
MidgeOptions {
    level_multiplier: 5,
    ..Default::default()
}
```

#### 2. Compression

```rust
// Compression reduces space but adds CPU overhead
MidgeOptions {
    compression: CompressionType::Snappy,  // Good balance
    ..Default::default()
}

// Options: None, Snappy, LZ4, Zstd
// Snappy: Low CPU, moderate compression (recommended default)
// LZ4: Very low CPU, lower compression
// Zstd: High compression, high CPU
```

#### 3. Manual Compaction

Trigger compaction when needed:

```rust
// Compact a specific level
engine.compact_level(&cf, 0)?;  // Compact L0

// Compact a key range
engine.compact_range(&cf, Some(b"key_start"), Some(b"key_end"))?;

// Full compaction (expensive, avoid in production)
engine.compact_range(&cf, None, None)?;
```

---

## Debugging

### Enable Debug Logging

```bash
# Full debug output
RUST_LOG=cntryl_midge=debug cargo run

# Specific subsystems
RUST_LOG=cntryl_midge::core::runtime=trace \
         cntryl_midge::core::persistence=debug \
         cntryl_midge::core::compaction=debug
```

### Manifest Analysis

```rust
// Examine current LSM state
let manifest = engine.get_manifest();

// Check for problematic patterns
println!("=== LSM Structure ===");
for (level, files) in group_by_level(&manifest) {
    let total_size: u64 = files.iter().map(|f| f.size_bytes).sum();
    println!("L{}: {} files, {} MB",
        level,
        files.len(),
        total_size / (1024 * 1024)
    );
}

// Identify hot spots (high overlap)
println!("\n=== Potential Compaction Hotspots ===");
if manifest.files.iter().filter(|f| f.level == 0).count() > 10 {
    println!("WARNING: L0 has {} files (many flushes)", 
        manifest.files.iter().filter(|f| f.level == 0).count());
}
```

### Tracing Operations

```rust
// Enable trace-level logging for specific operations
RUST_LOG=cntryl_midge::core::runtime=trace \
    cntryl_midge::core::persistence::flush=trace

// Engine will emit detailed task scheduling information
// Example output:
// [INFO] Submitting flush task for memtable 3
// [INFO] Flush task: frozen=1000 entries, size=4MB
// [INFO] Flush completed: sst_seq=42, duration=250ms
```

### Performance Profiling

```rust
// Use criterion benchmarks
cargo bench --bench tier1_hotpath

// Analyze with flame graphs
cargo flamegraph --bin myapp
```

---

## Troubleshooting

### Issue: High Write Latency

**Symptom**: Write operations taking longer than expected.

**Diagnosis**:
```rust
// Check if memtable is full (flush pending)
let metrics = engine.metrics();
if metrics.immutable_memtables > 2 {
    println!("Memtable backlog detected");
}

// Check compaction backlog
if metrics.l0_files > 10 {
    println!("L0 compaction backlog");
}
```

**Solutions**:
1. Increase `memtable_size_bytes` to reduce flush frequency
2. Increase `block_cache_size_bytes` to speed up compaction
3. Reduce compression (use LZ4 or None) to reduce CPU
4. Trigger manual compaction: `engine.compact_level(&cf, 0)?`

### Issue: High Read Latency

**Symptom**: Get/scan operations slower than expected.

**Diagnosis**:
```rust
let metrics = engine.metrics();
let hit_ratio = metrics.cache_hit_ratio();

if hit_ratio < 0.7 {
    println!("Low cache hit ratio: {:.1}%", hit_ratio * 100.0);
}

// Check SST count (more files = slower reads)
println!("Total SST files: {}", metrics.total_sst_files);
```

**Solutions**:
1. Increase `block_cache_size_bytes` to improve cache hit ratio
2. Trigger compaction to reduce SST file count
3. Check for lock contention (verify single-threaded reads)
4. Profile with flame graphs to identify hotspots

### Issue: High Disk Usage

**Symptom**: Storage growing faster than expected.

**Diagnosis**:
```rust
let manifest = engine.get_manifest();
let total_size: u64 = manifest.files.iter()
    .map(|f| f.size_bytes).sum();

println!("Total data size: {} MB", total_size / (1024 * 1024));

// Check compression effectiveness
let compressed_size = total_size;
let estimated_uncompressed = total_size * 2;  // Rough estimate
println!("Compression ratio: {:.1}%",
    (1.0 - (compressed_size as f64 / estimated_uncompressed as f64)) * 100.0
);
```

**Solutions**:
1. Enable/increase compression: `CompressionType::Zstd`
2. Reduce `memtable_size_bytes` (smaller flushes = better granularity)
3. Enable paranoid mode to detect bloated blocks
4. Trigger full compaction (production use only)

### Issue: "Database is locked" Error

**Symptom**: Cannot open database after crash.

**Cause**: Stale lock file from previous process.

**Resolution**:
```bash
# 1. Verify no Midge process is running
ps aux | grep midge

# 2. Remove stale lock file
rm /data/midge/LOCK

# 3. Reopen database (recovery will occur automatically)
```

### Issue: Corruption Detected

**Symptom**: "SST corruption" or "Manifest corruption" error.

**Actions**:
1. Enable `paranoid_mode` to catch issues early
2. Check disk health: `smartctl -a /dev/sda`
3. Verify file permissions and disk space
4. Consider rebuilding from backup if persistent

---

## Recovery & Safety

### Backup Strategy

```rust
// Periodic snapshot backups
fn backup_interval() {
    loop {
        std::thread::sleep(Duration::from_secs(3600)); // 1 hour
        
        let manifest = engine.get_manifest();
        let backup_path = format!("/backups/midge_{}", 
            chrono::Local::now().format("%Y%m%d_%H%M%S"));
        
        // Copy all SST files and manifest
        // (Midge will emit backup-friendly manifest format in Phase 8+)
        
        println!("Backup completed to {}", backup_path);
    }
}
```

### Point-in-Time Recovery

```rust
// Open from specific point
// (Phase 8+ will support manifest snapshots)

// For now, use WAL with proper durable configuration
let opts = MidgeOptions {
    storage_mode: StorageMode::LocalDisk {
        db_path: PathBuf::from("/data/midge"),
    },
    // Ensure WAL durability
    // (Automatic in cloud-backed mode)
    ..Default::default()
};
```

### Crash Recovery

Midge performs automatic recovery on startup:

```rust
// 1. Open manifest
// 2. Validate SST files referenced in manifest
// 3. Recover WAL if needed
// 4. Rebuild memtables from in-flight operations
// 5. Resume normal operation

let engine = MidgeEngine::open(opts)?;  // Recovery happens here automatically
```

---

## Best Practices

### 1. Configuration Versioning

```rust
// Document your configuration
let config = r#"
# Midge Production Configuration
memtable_size_bytes: 64MB  # Tuned for workload X
block_cache_size_bytes: 512MB
compression: Snappy
version: "1.0"
last_updated: 2025-01-15
"#;
```

### 2. Health Checks

```rust
fn health_check(engine: &MidgeEngine) -> Result<(), String> {
    let metrics = engine.metrics();
    
    // Check memtable backlog
    if metrics.immutable_memtables > 5 {
        return Err("Memtable backlog too high".to_string());
    }
    
    // Check L0 overflow
    if metrics.l0_files > 20 {
        return Err("L0 compaction backlog".to_string());
    }
    
    // Check cache effectiveness
    if metrics.cache_hit_ratio() < 0.5 {
        return Err("Cache hit ratio dangerously low".to_string());
    }
    
    Ok(())
}
```

### 3. Gradual Rollout

```bash
# 1. Deploy to staging environment
cargo build --release

# 2. Run performance benchmarks
cargo bench --bench tier1_hotpath

# 3. Load test in staging
# (Custom load test harness)

# 4. Blue/green deploy in production
# Use container orchestration (K8s) for safe rollout
```

### 4. Monitoring Checklist

Set up alerts for:
- ❌ `memtable_backlog > 3`
- ❌ `l0_files > 15`
- ❌ `cache_hit_ratio < 0.7`
- ❌ `write_latency_p99 > 100ms`
- ❌ `read_latency_p99 > 50ms`
- ❌ `compaction_duration > 10s`

### 5. Capacity Planning

```
Storage: Account for LSM amplification
- Typical 10-20x write amplification
- Monitor total_sst_bytes and plan accordingly

Memory: Cache sizing
- Memtables: max_memtables * memtable_size_bytes
- Block cache: block_cache_size_bytes
- Overhead: ~5-10% for indexes, metadata

CPU: Compression overhead
- Snappy: ~5-15% CPU overhead
- Zstd: ~20-40% CPU overhead for high compression
- LZ4: ~1-5% CPU overhead
```

---

## Support & Escalation

### Self-Service Diagnostics

```bash
# Generate diagnostic bundle
midge-diagnostics \
  --engine-path /data/midge \
  --output diagnostics.tar.gz
```

### When to Contact Support

- Manifest corruption errors
- Persistent "database locked" issues
- Unexplained performance degradation
- Data loss (backup from last known good state)

---

## Further Reading

- **Architecture**: See `docs/ARCHITECTURE.md` for design details
- **Performance**: See `benches/` for tuning methodology
- **Tests**: See `tests/` for operational patterns
- **Cloud Integration**: See `docs/PHASE_7_2_INTEGRATION_PLAN.md`, `docs/PHASE_7_3_INTEGRATION_PLAN.md`

---

*Last Updated: December 2025 | Midge Phase 8 Operations Guide*
