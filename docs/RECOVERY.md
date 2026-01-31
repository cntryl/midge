# Recovery and Durability Guarantees

Comprehensive guide to Midge's recovery behavior and durability guarantees.

## Table of Contents

- [Overview](#overview)
- [Durability Guarantees by WriteOptions](#durability-guarantees-by-writeoptions)
- [Recovery Process](#recovery-process)
- [Crash Scenarios](#crash-scenarios)
- [Storage Mode Recovery](#storage-mode-recovery)
- [WAL Replay](#wal-replay)
- [Manifest-Driven Recovery](#manifest-driven-recovery)
- [Partial Failure Handling](#partial-failure-handling)
- [Data Integrity](#data-integrity)
- [Best Practices](#best-practices)

## Overview

Midge provides **explicit durability guarantees** based on your choice of `WriteOptions`. Understanding these guarantees is critical for production deployments.

**Core principle:**
> If a write is acknowledged (commit returns Ok), it survives according to its WriteOptions policy. No write disappears silently.

**What this document covers:**
- Exactly what "durable" means for each WriteOptions
- What happens on crash (process, OS, hardware)
- How recovery works (WAL replay, manifest reconciliation)
- Data integrity guarantees (checksums, consistency)

## Durability Guarantees by WriteOptions

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

## Recovery Process

### Startup Sequence

When `Engine::open()` is called, recovery happens automatically:

```
1. Acquire exclusive lease (prevents concurrent access)
2. Load manifest (discover SST files and sequence ranges)
3. Replay WAL (restore uncommitted writes from log)
4. Reconcile state (merge WAL writes with SST data)
5. Resume operations (start accepting new writes)
```

**Key invariants:**
- Recovery is deterministic (same input → same output)
- Sequence numbers are monotonic (never reused)
- All committed writes are restored (per their WriteOptions)

### Recovery Time

**Factors affecting recovery time:**
- WAL size (more uncommitted writes = longer replay)
- Manifest size (more SSTs = longer load time)
- Cloud latency (if recovering from cloud storage)

**Typical recovery times:**

| Scenario | Recovery Time |
|----------|---------------|
| Clean shutdown (no WAL) | <100ms |
| Small WAL (<10MB) | <1 second |
| Large WAL (100MB+) | 5-10 seconds |
| Cloud recovery (manifest + WAL download) | 10-30 seconds |

**Optimization tips:**
- Flush before shutdown (`engine.flush_cf()`)
- Use smaller memtables (more frequent flushes = smaller WAL)
- Avoid crashes during high write throughput

## Crash Scenarios

### Process Crash (SIGKILL, panic, etc.)

**Behavior:**
- OS keeps unfsynced data in page cache
- May persist buffered writes (if OS flushes before reboot)
- sync() writes are always safe

**Recovery:**
- Reopen engine: `Engine::open(opts)?`
- WAL replay restores committed writes
- sync() writes: ✅ Recovered
- buffered() writes: ⚠️ May be lost (if not yet fsynced)
- best_effort() writes: ❌ Lost

### OS Crash (kernel panic, BSOD)

**Behavior:**
- Page cache is lost (all unfsynced data gone)
- Only fsynced data survives

**Recovery:**
- WAL replay after reboot
- sync() writes: ✅ Recovered
- buffered() writes: ⚠️ Lost if in batch window (<500ms)
- best_effort() writes: ❌ Lost

### Power Loss

**Behavior:**
- All volatile data lost
- Only durable storage survives
- Disk write cache may lose data (unless battery-backed or disabled)

**Recovery:**
- WAL replay after power restore
- sync() writes: ✅ Recovered (if disk cache disabled or battery-backed)
- sync() writes: ⚠️ May be lost (if disk write cache enabled without battery)
- buffered() writes: ❌ Lost
- best_effort() writes: ❌ Lost

**Recommendation:** Use `hdparm -W 0 /dev/sdX` to disable disk write cache, or use battery-backed RAID controller.

### Disk Corruption

**Behavior:**
- Filesystem may return corrupted data
- Checksums detect corruption (see [Data Integrity](#data-integrity))

**Recovery:**
- Restore from backup
- Or use cloud recovery (if Cloud mode)

**Protection:**
- Use checksums (enabled by default)
- Use replicated storage (RAID, cloud)
- Regular backups

## Storage Mode Recovery

### InMemory Recovery

**Behavior:**
- No persistence, no recovery
- Data lost when engine drops

**Use only for:**
- Testing
- Ephemeral caches
- Throwaway workloads

### Local Recovery

**Recovery from local disk:**

```
1. Read manifest from disk (./db/MANIFEST)
2. Discover SST files and ranges
3. Replay WAL from disk (./db/wal/*.log)
4. Resume operations
```

**Requirements:**
- Local disk intact
- Filesystem accessible
- WAL and manifest not corrupted

**Failure modes:**
- Disk lost: Data lost (restore from backup)
- Filesystem corrupted: May need repair (fsck)

### Cloud Recovery

**Recovery from cloud object storage:**

```
1. Download manifest from cloud (s3://bucket/prefix/MANIFEST)
2. Discover SST files in cloud (no local disk needed)
3. Download and replay WAL from cloud (s3://bucket/prefix/wal/*.log)
4. Resume operations (local cache is empty, downloads on demand)
```

**Key characteristics:**
- **Local disk is ignored** (may be empty or deleted)
- Cloud is source of truth
- Recovery works even on new instance with empty disk

**Requirements:**
- Cloud bucket accessible
- Credentials valid
- Network connectivity

**Failure modes:**
- Bucket deleted: Data lost (unrecoverable)
- Credentials invalid: Cannot recover (fix credentials)
- Network down: Cannot recover (wait for network)

## WAL Replay

### What Gets Replayed

WAL contains uncommitted writes that were not yet flushed to SST.

**Replay logic:**

```
For each WAL segment:
    For each WAL record:
        If sequence > last_flushed_sequence:
            Apply to memtable
```

**Result:**
- Memtable restored to pre-crash state
- Writes become visible again
- Sequence numbers resume from last assigned

### WAL Durability by Policy

| WAL Policy | Replay Source |
|------------|---------------|
| Strict | Local WAL (fsynced per write) |
| Batched | Local WAL (fsynced per batch) |
| CloudMirrored | Local WAL (fsynced) + cloud backup |
| CloudFirst | Cloud WAL (local is cache) |
| BestEffort | No WAL (nothing to replay) |

**WAL Policies** (configured at engine level, not per-write):
- Control when WAL is fsynced
- Separate from WriteOptions (which control API acknowledgment)

See [API_GUIDE.md](API_GUIDE.md) for WriteOptions vs WAL Policy distinction.

## Manifest-Driven Recovery

### Manifest Purpose

Manifest is the **authoritative source of truth** for:
- Which SST files exist
- What sequence ranges each SST covers
- Compaction history and level assignments

**Manifest format:**
- JSON or binary (versioned)
- Immutable log of state transitions
- Each entry is a compaction or flush event

### Recovery from Manifest

```
1. Read latest manifest (MANIFEST-000001)
2. Build version set (active SSTs)
3. Determine last_flushed_sequence
4. Replay WAL for sequences > last_flushed_sequence
5. Reconcile compaction intents (did in-flight compaction finish?)
```

**Key insight:**
- Manifest + WAL = complete database state
- SSTs are immutable (safe to reuse or redownload)
- Compaction may be retried (idempotent)

### Manifest in Cloud Mode

**Cloud manifest:**
- Stored in cloud (s3://bucket/prefix/MANIFEST)
- Downloaded on startup
- Local manifest is ignored (may be stale or missing)

**Recovery advantages:**
- New instance can recover from scratch
- No local state needed
- Serverless-friendly (stateless compute)

## Partial Failure Handling

### In-Flight Compactions

**Scenario:** Compaction started but crashed before committing to manifest.

**Recovery:**
- Detect incomplete compaction (intent logged, no manifest commit)
- Retry compaction OR
- Abandon compaction and continue with old SSTs

**Guarantee:** No data loss. SSTs are immutable, compaction is idempotent.

### Partial WAL Segments

**Scenario:** WAL write interrupted mid-record.

**Recovery:**
- Detect incomplete record (checksum mismatch)
- Discard incomplete record
- Resume from last complete record

**Guarantee:** Writes are atomic at record granularity. Partial records are never applied.

### Cloud Upload Failures

**Scenario:** WAL or SST upload to cloud failed mid-transfer.

**Recovery:**
- Detect missing or incomplete cloud object
- Retry upload from local cache OR
- Regenerate SST (if local cache lost)

**Guarantee:** Manifest only references successfully-uploaded objects. No dangling references.

## Data Integrity

### Checksums

All on-disk data is protected by checksums:
- WAL records: CRC32
- SST blocks: CRC32
- Manifest entries: CRC32

**On read:**
- Checksum validated before returning data
- Corruption detected immediately
- Error returned to caller (no silent corruption)

### Corruption Detection

**WAL corruption:**
- Detected during replay
- Corrupted record is skipped (logged as warning)
- Recovery continues with valid records

**SST corruption:**
- Detected during read
- Error returned to caller
- Requires restore from backup or cloud

**Manifest corruption:**
- Detected during load
- Recovery fails (cannot determine state)
- Requires restore from backup or cloud

### Write Atomicity

**Single write:**
- Write is atomic at record level
- Entire record is written or not written
- No partial writes visible

**Transaction:**
- All writes in transaction are atomic
- Single sequence number for entire batch
- All visible or none visible (no partial transactions)

## Best Practices

### 1. Flush Before Shutdown

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

### 2. Choose Appropriate WriteOptions

Match durability to data criticality:

```rust
// Critical data: full durability
engine.commit(tx, WriteOptions::sync())?;

// General data: group commit
engine.commit(tx, WriteOptions::buffered())?;

// Reloadable data: no durability
engine.commit(tx, WriteOptions::best_effort())?;
engine.flush_cf(&cf)?;  // Flush when load completes
```

### 3. Use Cloud for Multi-AZ Durability

If single-node failure is unacceptable:

```rust
let opts = OpenOptions::new()
    .storage(Storage::Cloud {
        bucket: "my-db".to_string(),
        // ... cloud config
    })
    .build();
```

**Benefit:**
- Survives entire instance loss
- Multi-AZ/multi-region durability
- Serverless-friendly

### 4. Test Recovery

Include recovery tests in your application:

```rust
#[test]
fn should_recover_after_crash() {
    // 1. Write data
    let engine = Engine::open(opts)?;
    let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    tx.put(b"key".to_vec(), b"value".to_vec(), None)?;
    engine.commit(tx, WriteOptions::sync())?;
    
    // 2. Simulate crash (drop without flush)
    drop(engine);
    
    // 3. Reopen and verify
    let engine = Engine::open(opts)?;
    let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
    assert_eq!(tx.get(b"key")?, Some(b"value"));
}
```

### 5. Monitor Recovery Time

Track recovery time in production:

```rust
let start = std::time::Instant::now();
let engine = Engine::open(opts)?;
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

### 7. Use WAL Policies Appropriately

Match WAL policy to deployment:

```rust
// Local mode: Batched (default)
// - Good balance of durability and performance

// Cloud mode: CloudFirst
// - Automatically configured for Cloud storage mode
// - Background uploads, cloud is source of truth
```

### 8. Handle Backpressure

On recovery, memtable queue may be full:

```rust
loop {
    match engine.commit(tx, opts) {
        Ok(_) => break,
        Err(MidgeError::WriteStall) => {
            // Recovery in progress, retry after delay
            std::thread::sleep(Duration::from_millis(100));
        }
        Err(e) => return Err(e),
    }
}
```

## Recovery Guarantees Summary

| WriteOptions | Process Crash | OS Crash | Power Loss | Disk Loss |
|--------------|---------------|----------|------------|-----------|
| `sync()` | ✅ Recovered | ✅ Recovered | ⚠️ Recovered* | ❌ Lost** |
| `buffered()` | ⚠️ <500ms loss | ⚠️ <500ms loss | ❌ Lost | ❌ Lost** |
| `best_effort()` | ❌ Lost | ❌ Lost | ❌ Lost | ❌ Lost** |
| `cloud_strict()` | ✅ Recovered | ✅ Recovered | ✅ Recovered | ✅ Recovered |

\* If disk write cache disabled or battery-backed  
\** Unless using Cloud mode (cloud is source of truth)

## Next Steps

- **Cloud setup**: [CLOUD_SETUP.md](CLOUD_SETUP.md)
- **API reference**: [API_GUIDE.md](API_GUIDE.md)
- **Architecture**: [THE_BIG_IDEA.md](THE_BIG_IDEA.md)
