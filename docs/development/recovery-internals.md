# Recovery Internals

**Technical details of Midge's recovery algorithm and implementation**

> For user-facing durability guarantees and decision guidance, see [../user-guides/durability.md](../user-guides/durability.md)

## Overview

This document describes how Midge implements crash recovery, WAL replay, and manifest reconciliation at a technical level. Understanding these internals is essential for contributors working on recovery logic, durability features, or storage mode implementations.

**Core recovery invariants:**
- Recovery is deterministic (same input → same output)
- Sequence numbers are monotonic (never reused)
- All committed writes are restored (per their WriteOptions)

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

Each step is atomic and logged. If recovery fails mid-process, it can be retried from the beginning.

### Recovery Time

**Factors affecting recovery time:**
- WAL size (more uncommitted writes = longer replay)
- Manifest size (more SSTs = longer load time)
- Cloud latency (if recovering from cloud storage)

**Typical recovery times** (on modern SSD, local storage):

| Scenario | Recovery Time |
|----------|---------------|
| Clean shutdown (no WAL) | <100ms |
| Small WAL (<10MB) | <1 second |
| Large WAL (100MB+) | 5-10 seconds |
| Cloud recovery (manifest + WAL download) | 10-30 seconds* |

_*Cloud recovery times vary significantly based on network latency and cloud provider performance._

**Optimization opportunities:**
- Flush before shutdown reduces WAL size
- Smaller memtables = more frequent flushes = smaller WAL
- Manifest compaction reduces load time (not yet implemented)

## Storage Mode Recovery

### InMemory Recovery

**Behavior:**
- No persistence, no recovery
- Data lost when engine drops
- Lease is in-memory only (no filesystem state)

**Implementation:**
- Skip manifest load
- Skip WAL replay
- Initialize empty state

**Use only for:**
- Testing
- Ephemeral caches
- Throwaway workloads

### Local Recovery

**Recovery from local disk:**

```
1. Acquire exclusive lease (./db/LOCK)
2. Read manifest from disk (./db/MANIFEST)
3. Discover SST files and sequence ranges
4. Replay WAL from disk (./db/wal/*.log)
5. Build memtable from WAL entries
6. Resume operations
```

**Requirements:**
- Local disk intact and accessible
- Filesystem readable
- WAL and manifest not corrupted (checksums valid)

**Failure modes:**
- Disk lost: Data lost (restore from backup)
- Filesystem corrupted: May need repair (fsck)
- Manifest corrupted: Recovery fails (cannot determine state)

**Lease implementation:**
- PID-based lock file (./db/LOCK)
- Prevents concurrent access from same or different process
- Stale lock detection (PID no longer exists)

### Cloud Recovery

**Recovery from cloud object storage:**

```
1. Acquire exclusive lease (cloud-based lease with TTL)
2. Download manifest from cloud (s3://bucket/prefix/MANIFEST)
3. Discover SST files in cloud (list objects under prefix)
4. Download and replay WAL from cloud (s3://bucket/prefix/wal/*.log)
5. Build memtable from WAL entries
6. Resume operations (local cache is empty, downloads SSTs on demand)
```

**Key characteristics:**
- **Local disk is ignored** (may be empty or deleted)
- Cloud is source of truth for all state
- Recovery works even on new instance with empty disk
- Local cache populated lazily as data accessed

**Requirements:**
- Cloud bucket accessible
- Credentials valid
- Network connectivity
- Lease service available (DynamoDB, etc.)

**Failure modes:**
- Bucket deleted: Data lost (unrecoverable)
- Credentials invalid: Cannot recover (fix credentials, retry)
- Network down: Cannot recover (wait for network)
- Lease service down: Cannot acquire lease (retry or fail-fast)

**Lease implementation:**
- Cloud-based distributed lease (DynamoDB item with TTL)
- Heartbeat mechanism (renew lease every 30s)
- Fencing tokens prevent split-brain
- Lease timeout (60s) allows recovery after crash

## WAL Replay

### WAL Structure

Each WAL segment contains a sequence of records:

```
Record: [Checksum(4B) | Length(4B) | Type(1B) | Payload(N bytes)]
```

**Record types:**
- Put: Key-value insert
- Delete: Key deletion
- DeleteRange: Range tombstone
- BeginTransaction: Transaction start marker
- CommitTransaction: Transaction commit marker

**WAL durability:**
- Checksums (CRC32) on every record
- Incomplete records detected and skipped
- Atomic record writes (no partial records)

### Replay Logic

```
For each WAL segment (ordered by sequence):
    For each WAL record:
        Validate checksum
        If valid:
            If sequence > last_flushed_sequence:
                Apply to memtable
            Else:
                Skip (already in SST)
        Else:
            Log warning (corrupted record)
            Skip record
            Continue with next record
```

**Key invariants:**
- Replay is idempotent (same input → same output)
- Sequence numbers determine what to replay
- Corrupted records are skipped (logged but not fatal)
- Transactions are atomic (all-or-nothing)

### Sequence Number Management

**Last flushed sequence:**
- Recorded in manifest
- Determines what to replay from WAL
- Updated on every flush

**WAL replay starts from:**
- `last_flushed_sequence + 1`
- Ensures no duplicate applications
- Ensures all committed writes are restored

**Sequence number assignment during replay:**
- Preserve original sequence numbers from WAL
- Do NOT assign new sequence numbers
- Resume sequence number counter from highest seen + 1

### Transaction Replay

**Transaction boundaries:**
- BeginTransaction record marks start
- CommitTransaction record marks end
- All writes between are part of transaction

**Replay behavior:**
- If CommitTransaction found: Apply entire transaction
- If only BeginTransaction found: Transaction incomplete, discard
- Transactions are atomic during replay

## Manifest-Driven Recovery

### Manifest Purpose

Manifest is the **authoritative source of truth** for:
- Which SST files exist and their locations
- What sequence ranges each SST covers
- Compaction history level assignments
- Last flushed sequence number

**Manifest format:**
- Append-only log of state transitions
- Each entry is versioned and checksummed
- Entries: AddSST, RemoveSST, CompactionComplete, etc.

### Recovery from Manifest

```
1. Read latest manifest entry
2. Apply all entries in order to build version set
3. Determine last_flushed_sequence (from latest flush entry)
4. Build list of active SST files
5. Register SST files with metadata (bloom, sparse index, etc.)
6. Prepare for WAL replay (start from last_flushed_sequence + 1)
```

**Manifest replay:**
- Deterministic (same entries → same state)
- Idempotent (can replay multiple times)
- Validates SST files exist (checksums, file size)

### Manifest in Cloud Mode

**Cloud manifest:**
- Stored in cloud (s3://bucket/prefix/MANIFEST)
- Downloaded on startup (cached locally)
- Local manifest is ignored (may be stale or missing)
- Atomic updates via versioned object writes

**Recovery advantages:**
- New instance can recover from scratch (no local state needed)
- Manifest lists cloud SST locations (not local paths)
- SST files downloaded on demand (lazy loading)
- Serverless-friendly (stateless compute)

**Manifest versioning:**
- Each manifest update creates new version
- Old versions retained for concurrent readers
- Garbage collection removes old versions after grace period

## Partial Failure Handling

### In-Flight Compactions

**Scenario:** Compaction started but crashed before committing to manifest.

**Detection:**
- Intent logged in manifest: "Begin compaction of SSTs [A, B] → C"
- No corresponding completion entry: "Compaction complete, SST C committed"

**Recovery:**
- Detect incomplete compaction in manifest
- Check if output SST exists and is valid
  - Valid: Commit compaction (idempotent retry)
  - Invalid or missing: Abandon compaction, keep input SSTs

**Guarantee:** No data loss. SSTs are immutable, compaction is idempotent.

**Implementation:**
- Compaction intents include input/output checksums
- Output SST validated before commit
- Input SSTs retained until commit

### Partial WAL Segments

**Scenario:** WAL write interrupted mid-record (crash during write).

**Detection:**
- Record length field says N bytes
- Only M bytes available (M < N)
- OR checksum mismatch

**Recovery:**
- Detect incomplete record
- Log warning: "Incomplete WAL record at offset X"
- Discard incomplete record
- Resume from last complete record

**Guarantee:** Writes are atomic at record granularity. Partial records are never applied.

**Implementation:**
- WAL writer uses atomic append (O_APPEND)
- Checksum validates integrity
- Length field validates completeness

### Cloud Upload Failures

**Scenario:** WAL or SST upload to cloud failed mid-transfer (network timeout, credentials expired, etc.).

**Detection:**
- Upload task reports failure
- Object missing or incomplete in cloud
- Manifest does not reference object

**Recovery:**
- Retry upload from local cache (if available)
- OR regenerate SST from memtable (if memtable still in memory)
- OR skip and continue (if data already flushed to other SST)

**Guarantee:** Manifest only references successfully-uploaded objects. No dangling references.

**Implementation:**
- Upload tasks are idempotent (same input → same output)
- Manifest commit waits for upload acknowledgment
- Local cache retained until upload confirmed

### Manifest Corruption

**Scenario:** Manifest file corrupted (disk error, partial write, etc.).

**Detection:**
- Checksum mismatch on manifest entry
- JSON parse error (if JSON format)
- Invalid structure (missing required fields)

**Recovery:**
- Recovery fails (cannot determine authoritative state)
- Requires restore from backup or cloud backup

**Mitigation:**
- Use checksums on every manifest entry
- Use cloud storage for manifest (durability)
- Regular manifest backups

**Future improvement:**
- Manifest compaction (reduce size, improve load time)
- Manifest snapshots (full state at version N)

## Data Integrity

### Checksums

All on-disk data is protected by checksums:

- **WAL records**: CRC32 checksum per record
- **SST blocks**: CRC32 checksum per block
- **Manifest entries**: CRC32 checksum per entry

**Checksum validation:**
- On read: Validate before returning data
- On replay: Validate before applying
- On corruption: Return error, log warning

### Corruption Detection

**WAL corruption:**
- Detected during replay
- Corrupted record is skipped (logged as warning)
- Recovery continues with valid records
- Result: Data loss for corrupted records only

**SST corruption:**
- Detected during read
- Error returned to caller
- Requires restore from backup or cloud redownload

**Manifest corruption:**
- Detected during load
- Recovery fails (cannot determine state)
- Requires restore from backup

### Write Atomicity

**Single write:**
- Write is atomic at record level
- Entire record is written or not written
- No partial writes visible
- Guaranteed by O_APPEND and atomic fsync

**Transaction:**
- All writes in transaction are atomic
- Single sequence number for entire batch
- All visible or none visible (no partial transactions)
- Implemented via CommitTransaction marker in WAL

## Concurrency and Lease Management

### Lease Acquisition

**Purpose:**
- Prevent concurrent access to same database
- Prevent split-brain scenarios
- Enable safe recovery after crash

**Local lease (Local mode):**
- PID-based lock file (./db/LOCK)
- Contains PID of owning process
- Stale lock detection (check if PID exists)
- Non-blocking acquisition (fail-fast)

**Cloud lease (Cloud mode):**
- Distributed lease (DynamoDB, etcd, etc.)
- TTL-based expiration (60s default)
- Heartbeat renewal (every 30s)
- Fencing tokens prevent split-brain

### Lease Heartbeat

**Heartbeat mechanism:**
- Background thread sends lease renewal every 30s
- Cloud service updates TTL on renewal
- If heartbeat fails: Lease expires after TTL
- Other instances can acquire lease after expiration

**Failure scenarios:**
- Process crash: Lease expires after TTL (60s)
- Network partition: Lease expires, fencing token prevents writes
- Heartbeat thread stuck: Lease expires, process detects and shuts down

### Fencing Tokens

**Purpose:**
- Prevent writes after lease loss
- Prevent split-brain scenarios

**Implementation:**
- Every lease has monotonic token (version)
- All writes include fencing token
- Cloud storage validates token before accepting write
- Stale token rejected (write fails)

**Example:**
- Instance A acquires lease with token 42
- Instance A loses network (heartbeat fails)
- Lease expires after 60s
- Instance B acquires lease with token 43
- Instance A regains network, tries to write with token 42
- Cloud storage rejects (token 42 < current token 43)

## Testing Recovery

### Unit Tests

**Test recovery scenarios:**

```rust
#[test]
fn should_recover_committed_writes_after_crash() {
    let path = tempdir()?;
    
    // 1. Write and commit
    let engine = Engine::open(OpenOptions::local(&path).build())?;
    let mut tx = engine.begin_tx(cf, TransactionMode::ReadWrite)?;
    tx.put(b"key".to_vec(), b"value".to_vec(), None)?;
    engine.commit(tx, WriteOptions::sync())?;
    drop(engine);  // Simulate crash
    
    // 2. Reopen and verify
    let engine = Engine::open(OpenOptions::local(&path).build())?;
    let tx = engine.begin_tx(cf, TransactionMode::ReadOnly)?;
    assert_eq!(tx.get(b"key")?, Some(b"value".to_vec()));
}
```

**Test WAL replay edge cases:**

```rust
#[test]
fn should_skip_corrupted_wal_records() {
    // 1. Write valid records
    // 2. Manually corrupt WAL file (flip bits)
    // 3. Reopen and verify valid records recovered
    // 4. Verify corrupted records skipped (logged)
}

#[test]
fn should_replay_transactions_atomically() {
    // 1. Start transaction with multiple writes
    // 2. Crash before commit
    // 3. Reopen and verify transaction not visible
    
    // 1. Start transaction with multiple writes
    // 2. Commit transaction
    // 3. Crash after commit
    // 4. Reopen and verify all writes visible
}
```

### Integration Tests

**End-to-end recovery:**

```rust
#[test]
fn should_recover_from_cloud_storage() {
    let bucket = mock_s3_bucket();
    
    // 1. Write to cloud mode engine
    let engine = Engine::open(OpenOptions::cloud(cache, bucket, prefix).build())?;
    // ... write data ...
    engine.flush_cf(&cf)?;
    drop(engine);
    
    // 2. Delete local cache (simulate new instance)
    std::fs::remove_dir_all(cache)?;
    
    // 3. Reopen and verify recovery from cloud
    let engine = Engine::open(OpenOptions::cloud(cache, bucket, prefix).build())?;
    // ... verify data ...
}
```

### Fault Injection

**Simulate failures:**

```rust
#[test]
fn should_handle_partial_wal_write() {
    // 1. Inject fault: truncate WAL mid-record
    // 2. Reopen and verify partial record skipped
}

#[test]
fn should_retry_failed_cloud_uploads() {
    // 1. Inject fault: cloud upload fails
    // 2. Verify retry logic
    // 3. Verify eventual success
}
```

## Performance Optimization

### Parallel WAL Replay

**Opportunity:**
- WAL records can be replayed in parallel (if independent keys)
- Partition WAL by key range
- Replay partitions concurrently

**Implementation complexity:**
- Requires key-range partitioning
- Transaction atomicity must be preserved
- Sequence number ordering must be preserved

**Status:** Not yet implemented (future optimization)

### Incremental Manifest Load

**Opportunity:**
- Load only recent manifest entries
- Skip old compaction history (not needed for recovery)

**Implementation:**
- Manifest snapshots (full state at version N)
- Delta entries (changes since snapshot)

**Status:** Not yet implemented (future optimization)

### Background WAL Prefetch

**Opportunity:**
- Download cloud WAL in background during lease acquisition
- Overlap network I/O with manifest load

**Implementation:**
- Start WAL download immediately after lease acquisition
- Manifest load completes while WAL downloads
- WAL replay starts when download completes

**Status:** Partially implemented (cloud WAL download is sequential)

## Related Documentation

- **User guide**: [../user-guides/durability.md](../user-guides/durability.md) — Durability guarantees and decision guide
- **Architecture**: [architecture.md](architecture.md) — Module structure, threading model
- **WAL implementation**: See `src/wal/` for WAL writer/reader code
- **Manifest implementation**: See `src/metadata/manifest.rs` for manifest format
- **Testing**: [testing.md](testing.md) — Test structure and naming conventions
