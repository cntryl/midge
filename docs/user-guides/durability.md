# Durability Guarantees

**Understanding Midge's durability levels and recovery behavior**

## Overview

Midge provides **explicit durability guarantees** based on your choice of `WriteOptions`. All writes require you to specify the durability level - there are no defaults.

**Core principle:**
> If a write is acknowledged (commit returns Ok), it survives according to its WriteOptions policy. No write disappears silently.

This guide helps you understand:
- What each durability level guarantees
- Which durability level to choose for your use case
- What happens during crashes
- How to test and verify durability

For technical details about recovery implementation, see [../development/recovery-internals.md](../development/recovery-internals.md).

## Durability Levels

### sync() — Full Local Durability

```rust
engine.commit(tx, WriteOptions::sync())?;
// When this returns Ok(_), write is guaranteed durable on local disk
```

**Guarantee:**
- Write has been fsynced to local disk
- Survives process crash, OS crash, power loss
- Kernel has confirmed persistence to stable storage

**Recovery:**
- Write is present in local WAL on restart
- WAL replay restores write to memtable
- No data loss

**Performance:**
- Highest latency (~5-20ms, depends on disk)
- Throughput limited by fsync rate (~200-1000 ops/sec)

**Use when:**
- Absolutely cannot lose this write
- Financial transactions, critical metadata
- Single-node deployments without replication

**Failure modes:**
- Disk corruption: Write may be lost (use replication/backups)
- Disk physically destroyed: Write is lost (use cloud or replication)

---

### buffered() — Group Commit Durability

```rust
engine.commit(tx, WriteOptions::buffered())?;
// When this returns Ok(_), write is visible but not yet durable
```

**Guarantee:**
- Write is visible immediately to all readers
- Write is in local WAL (not yet fsynced)
- Durability achieved via background group commit (batched fsync)

**Background fsync triggers:**
- 1024 operations accumulated, OR
- 4MB of data written, OR
- 500μs elapsed since first buffered write

**Recovery scenarios:**

| Crash timing | Result |
|--------------|--------|
| Before group commit fsync | **Data lost** (not yet durable) |
| After group commit fsync | **Data recovered** (in WAL) |

**Performance:**
- Low latency (~1-5ms, local write only)
- High throughput (~10k-100k ops/sec)
- ~100x faster than sync() due to batching

**Use when:**
- General production workloads
- High throughput requirements
- Acceptable to lose <1 second of writes on crash

**Window of vulnerability:**
- Typically <500ms (max batch delay)
- At most 1024 ops or 4MB (max batch size)
- If crash during this window: those buffered writes are lost

---

### best_effort() — No Durability

```rust
engine.commit(tx, WriteOptions::best_effort())?;
// When this returns Ok(_), write is visible but NOT durable
```

**Guarantee:**
- Write is visible immediately to all readers
- Write is in memtable ONLY (not in WAL)
- No durability until explicit `engine.flush_cf()`

**Recovery:**
- Crash before flush: **All best_effort writes are lost**
- Crash after flush: **Writes are recovered** (in SST)

**Performance:**
- Lowest latency (~0.1-1ms, memory write only)
- Highest throughput (100k+ ops/sec)
- No WAL overhead, no fsync

**Use ONLY when:**
- Bulk data loads (can be reloaded from source)
- Test data / benchmark initialization
- Setup phase before measured workload
- Data is reproducible or non-critical

**NEVER use when:**
- Data cannot be reloaded
- Production writes that matter
- Measured benchmark workloads (defeats purpose of benchmarking durability)

**Safe pattern:**
```rust
// Phase 1: Fast load with best_effort
for i in 0..1_000_000 {
    let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    tx.put(format!("key:{}", i).into_bytes(), b"value".to_vec(), None)?;
    engine.commit(tx, WriteOptions::best_effort())?;
}

// Phase 2: Make durable via flush
engine.flush_cf(&cf)?;  // NOW all writes are durable in SST

// Phase 3: Production workload with durability
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
tx.put(b"important".to_vec(), b"data".to_vec(), None)?;
engine.commit(tx, WriteOptions::buffered())?;  // Durable via WAL
```

---

### cloud_strict() — Immediate Cloud Durability

```rust
engine.commit(tx, WriteOptions::cloud_strict())?;
// When this returns Ok(_), write is guaranteed durable in cloud storage
```

**Guarantee:**
- Write has been uploaded to cloud object storage
- Survives local disk loss, instance termination
- Cloud provider has acknowledged write

**Recovery:**
- Write is present in cloud WAL on restart
- Cloud-based recovery reads from object storage
- Local disk can be empty/deleted without data loss

**Performance:**
- Highest latency (~50-200ms, network round-trip)
- Throughput limited by cloud API rate limits
- Use sparingly for critical checkpoints only

**Use when:**
- Cloud is primary durability target
- Local disk is known to be ephemeral
- Explicit cloud persistence required before proceeding
- Compliance requires cloud-level durability proof

**Note:** Regular Cloud mode uses background uploads. Most applications should use `buffered()` in Cloud mode, not `cloud_strict()`.

---

## Choosing the Right Durability Level

### Decision Tree

```
Do you need to survive local disk loss?
├─ YES → Use cloud_strict() or Cloud storage mode + buffered()
└─ NO → Continue below

Is data reloadable from source?
├─ YES → Use best_effort() + flush() after load
└─ NO → Continue below

Can you tolerate <1 second of data loss?
├─ YES → Use buffered() (recommended for production)
└─ NO → Use sync() (strict durability)
```

### By Use Case

| Use Case | Recommended Mode | Rationale |
|----------|------------------|-----------|
| Financial transactions | `sync()` | Zero tolerance for data loss |
| User-generated content | `buffered()` | Balance durability and performance |
| Session state | `buffered()` | Acceptable to lose recent activity |
| Metrics/analytics | `buffered()` or `best_effort()` | Approximate data acceptable |
| Bulk data import | `best_effort()` + `flush()` | Reloadable from source |
| Cloud-native app | `buffered()` (Cloud mode) | Cloud backup handles durability |
| Critical checkpoint | `cloud_strict()` | Explicit cloud durability |
| Cache | `InMemory` storage | No persistence needed |

### Performance vs Durability Tradeoff

```
Durability ↑                         Performance ↑
sync()  >  cloud_strict()  >  buffered()  >  best_effort()
```

Choose based on your specific requirements. Most production workloads use `buffered()`.

## Crash Scenarios

### Process Crash (SIGKILL, panic, etc.)

**Behavior:**
- OS keeps unfsynced data in page cache
- May persist buffered writes (if OS flushes before reboot)

**Recovery:**

| WriteOptions | Result |
|--------------|--------|
| `sync()` | ✅ Recovered |
| `buffered()` | ⚠️ May be lost (if not yet fsynced) |
| `best_effort()` | ❌ Lost |
| `cloud_strict()` | ✅ Recovered (from cloud) |

### OS Crash (kernel panic, BSOD)

**Behavior:**
- Page cache is lost (all unfsynced data gone)
- Only fsynced data survives

**Recovery:**

| WriteOptions | Result |
|--------------|--------|
| `sync()` | ✅ Recovered |
| `buffered()` | ⚠️ Lost if in batch window (<500ms) |
| `best_effort()` | ❌ Lost |
| `cloud_strict()` | ✅ Recovered (from cloud) |

### Power Loss

**Behavior:**
- All volatile data lost
- Disk write cache may lose data (unless battery-backed or disabled)

**Recovery:**

| WriteOptions | Result |
|--------------|--------|
| `sync()` | ✅ Recovered* |
| `buffered()` | ❌ Lost |
| `best_effort()` | ❌ Lost |
| `cloud_strict()` | ✅ Recovered (from cloud) |

\* If disk write cache disabled or battery-backed

**Recommendation:** Use `hdparm -W 0 /dev/sdX` to disable disk write cache, or use battery-backed RAID controller.

### Disk Failure

**Behavior:**
- Local data lost or corrupted

**Recovery:**

| Storage Mode | Result |
|--------------|--------|
| Local mode | ❌ Lost (restore from backup) |
| Cloud mode | ✅ Recovered (from cloud) |

**Protection:**
- Use Cloud storage mode for critical data
- Regular backups for Local mode
- RAID for hardware redundancy

## Best Practices

### 1. Match Durability to Data Criticality

```rust
// Critical data: full durability
engine.commit(tx, WriteOptions::sync())?;

// General data: group commit
engine.commit(tx, WriteOptions::buffered())?;

// Reloadable data: no durability
engine.commit(tx, WriteOptions::best_effort())?;
engine.flush_cf(&cf)?;  // Flush when load completes
```

### 2. Flush Before Shutdown

Always flush before clean shutdown:

```rust
// Before shutdown
for cf_name in engine.list_column_families() {
    if let Some(cf) = engine.get_column_family(&cf_name) {
        engine.flush_cf(&cf)?;
    }
}

drop(engine);  // Clean shutdown
```

**Why:**
- Reduces WAL size (nothing to replay)
- Faster recovery (no replay needed)
- SSTs are durable, WAL can be deleted

### 3. Use Cloud for Multi-AZ Durability

If single-node failure is unacceptable:

```rust
let opts = OpenOptions::cloud(
    "./cache",
    "my-bucket",
    "db/prod/"
).build();

let engine = MidgeEngine::open(opts)?;
```

**Benefit:**
- Survives entire instance loss
- Multi-AZ/multi-region durability
- Serverless-friendly

### 4. Test Your Recovery Path

Include recovery tests in your application:

```rust
#[test]
fn should_recover_after_crash() {
    // 1. Write data
    let engine = MidgeEngine::open(opts)?;
    let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    tx.put(b"key".to_vec(), b"value".to_vec(), None)?;
    engine.commit(tx, WriteOptions::sync())?;
    
    // 2. Simulate crash (drop without flush)
    drop(engine);
    
    // 3. Reopen and verify
    let engine = MidgeEngine::open(opts)?;
    let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
    assert_eq!(tx.get(b"key")?, Some(b"value".to_vec()));
}
```

### 5. Monitor Recovery Time

Track recovery time in production:

```rust
let start = std::time::Instant::now();
let engine = MidgeEngine::open(opts)?;
let recovery_time = start.elapsed();

if recovery_time > Duration::from_secs(30) {
    log::warn!("Slow recovery: {:?}", recovery_time);
    // Consider: smaller memtables, more frequent flushes
}
```

### 6. Backup Regularly

Even with cloud storage, maintain backups:

```bash
# Backup local database
tar -czf backup-$(date +%Y%m%d).tar.gz ./db/

# Backup cloud database
aws s3 sync s3://my-bucket/prod/db1/ ./backups/$(date +%Y%m%d)/
```

**Backup strategy:**
- Daily: Full backup
- Hourly: Incremental (new SSTs only)
- Retention: 7-30 days

## Recovery Guarantees Summary

| WriteOptions | Process Crash | OS Crash | Power Loss | Disk Loss |
|--------------|---------------|----------|------------|-----------|
| `sync()` | ✅ Recovered | ✅ Recovered | ⚠️ Recovered* | ❌ Lost** |
| `buffered()` | ⚠️ <500ms loss | ⚠️ <500ms loss | ❌ Lost | ❌ Lost** |
| `best_effort()` | ❌ Lost | ❌ Lost | ❌ Lost | ❌ Lost** |
| `cloud_strict()` | ✅ Recovered | ✅ Recovered | ✅ Recovered | ✅ Recovered |

\* If disk write cache disabled or battery-backed  
\*\* Unless using Cloud mode (cloud is source of truth)

## Next Steps

- **API reference**: [api-guide.md](api-guide.md) — Complete API documentation
- **Quick start**: [quick-start.md](quick-start.md) — 5-minute hello-world
- **Cloud setup**: [../operations/cloud-setup.md](../operations/cloud-setup.md) — Cloud provider configuration
- **Recovery internals**: [../development/recovery-internals.md](../development/recovery-internals.md) — Technical implementation details
- **FAQ**: [faq.md](faq.md) — Common questions
