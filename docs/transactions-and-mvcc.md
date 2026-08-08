# Transactions and MVCC in Midge

## Design goals

**Current MVCC summary:**

- Reads use a fixed snapshot captured at transaction start.
- Writes are buffered client-side until commit.
- Commit is atomic through runtime sequencing and WAL bracketing.
- Concurrent write conflicts default to last-writer-wins.
- Optional strict mode can abort on write conflicts.
- Active snapshots are registered so compaction and GC can preserve reader-visible versions.

---

Midge uses MVCC so that reads do not block writes and writes do not block reads. The goals are:

- **Non-blocking reads.** A `ReadOnly` transaction captures a snapshot at start time and executes against it without acquiring any lock or coordinating with the write path.
- **Atomic multi-key writes.** A `ReadWrite` transaction buffers writes client-side until commit, after which all writes are applied atomically via a single WAL append and a single sequence-number range.
- **Idempotent crash recovery.** Each committed transaction is encoded as one atomic `TxnBatch` WAL frame, so torn or partial commits are discarded on replay.
- **TTL and range-delete support.** MVCC sequence numbers propagate into SST files, enabling correct TTL expiry and range tombstone application across the read path.

---

## Transaction model

### Starting a transaction

```rust
let tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite)?;
let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly)?;
```

`begin_tx` starts with a fast-path read from a lock-free `ArcSwap`-backed `SnapshotCache` (published by the event loop after every write), then registers the snapshot with the runtime so compaction and GC can respect it. The returned `Transaction` owns a snapshot-pin guard that unregisters on commit, rollback, or drop, and stores:

- `start_sequence: u64` — the global sequence at load time. Used as the read horizon.
- `read_snapshot: Arc<ReadSnapshot>` — `Arc` references to the current active memtable, immutable memtables, and a `Vec<FileMeta>` cloned from the manifest.
- `write_set: Vec<WriteIntent>` — initially empty.

A transaction is scoped to a single column family. There is no cross-CF atomic transaction.

### How reads work

`Transaction::get(key)`:

1. Checks `write_set` first (read-your-own-writes). Returns the buffered value if found.
2. Delegates to `ReadSnapshot::get(key, start_sequence)`.

`ReadSnapshot::get(key, seq)` merges `KeyState` across all memtable layers and SST files using highest-sequence-wins. Only entries with `entry_seq <= seq` are considered. TTL-expired entries are treated as tombstones. Range tombstones are applied: if `tombstone.seq <= seq` and the key falls within the tombstone range, the entry is suppressed.

### How writes are buffered

`tx.put(key, value, ttl)`, `tx.delete(key)`, `tx.delete_range(start, end)` append a `WriteIntent` to `Transaction.write_set`. Nothing is written to the WAL or memtable at this point. No other transaction sees the buffered writes.

`tx.insert(key, value, ttl)` additionally checks, at commit time, that no existing entry for the key exists; the commit returns `MidgeError::InvalidArgument` if the key is already present.

### When writes become visible

Writes become visible only after a successful `tx.commit(opts)`. The commit:

1. Submits the transaction's runtime operations through the ingest coordinator for the target column family.
2. The runtime allocates a contiguous sequence-number range for the transaction (one per op, plus begin and commit), appends the full batch to the WAL unless the caller chose `best_effort()`, applies all ops to the active memtable, and advances the global sequence.
3. After applying, a new `PublishedSnapshot` is written to the `SnapshotCache`; subsequent `begin_tx` calls see the committed writes.

All ops in a commit become visible atomically. There is no partial visibility.

### Commit behavior

`commit` is synchronous. The caller blocks until the runtime has accepted, sequenced, and applied the transaction according to the selected durability policy. Durability depends on `WriteOptions`:

| `WriteOptions` | WAL behavior |
|---|---|
| `sync()` | A write commit appends an independent `TxnBatch` WAL frame and establishes a covering local fsync before returning. A fully empty or assertion-only commit appends no frame but still establishes an explicit local fsync barrier, which also covers prior buffered commits. Concurrent non-overlapping strict write commits may share the physical fsync. |
| `buffered()` | WAL write, no fsync. Crash between write and fsync loses data. |
| `best_effort()` | WAL skipped entirely. Data lives only in the memtable until flush. |
| `cloud_strict()` | Uses the cloud durability path and waits for the cloud durability frontier before returning. A fully empty commit with no assertions validates cloud-mode compatibility without sealing. An assertion-only commit performs its server-side validation and requests coverage of the current runtime sequence without allocating a new one. It returns immediately when that sequence is already cloud-durable; otherwise it seals a pending covering WAL segment when needed or joins its in-flight upload and waits. |

### Rollback behavior

```rust
tx.rollback()?;  // discards the write_set and unregisters the snapshot
```

Because writes are never sent to the engine until `commit`, dropping a `Transaction` without committing it is sufficient to discard all pending writes. There is no WAL abort record, no undo log, no two-phase rollback. `Transaction::rollback()` exists as an explicit API for clarity and unregisters the transaction snapshot.

### Copy-on-write

Midge does not use copy-on-write for write buffering. `write_set` is a plain `Vec<WriteIntent>` on the `Transaction` struct. There is no in-place divergence tracking.

---

## MVCC model

### Multiple versions per key

Midge uses sequence-versioned visibility for reads, but does not retain long-lived historical version chains as a first-class user-visible feature. The memtable stores one entry per key per sequence number; SST files store entries with embedded sequence numbers. Older versions of a given key are compacted away during merges — only the highest-sequence entry per key survives (subject to the snapshot horizon — see the compaction section for a known limitation).

### Version identifiers

All versions are identified by a single global monotonic `u64` sequence number. There are no timestamps or hybrid clocks. Each record in the WAL and each entry in memtable and SST files carries the sequence number at which it was written.

For a committed transaction of N ops:

```
begin_seq      = state.sequence + 1
op[0]          = begin_seq + 1
op[1]          = begin_seq + 2
...
op[N-1]        = begin_seq + N
commit_seq     = begin_seq + N + 1
state.sequence = commit_seq   (after commit)
```

### How readers choose visible versions

Reads use `start_sequence` (captured at `begin_tx` time) as the visibility cutoff. An entry at sequence `s` is visible to a transaction with `start_sequence = h` if and only if `s <= h`. The read path picks the entry with the highest sequence number that satisfies this constraint.

For unconstrained reads (e.g., event loop's own lookups), `seq = u64::MAX` makes all entries visible.

### Write conflicts

`ReadWrite` transactions support two commit policies:

- `ConflictPolicy::LastWriteWins` (default): concurrent overlapping writers both commit; the higher sequence wins.
- `ConflictPolicy::AbortOnWriteConflict`: commit fails with `MidgeError::WriteConflict` if a write-set key or delete-range target has been modified after `start_sequence`.

In strict mode, conflict checks happen in the runtime apply path before WAL append, so aborted conflicting commits do not publish partial writes.

The only exception is `tx.insert()`: at commit time (inside the event loop), the engine checks whether the key already exists. If it does, the commit returns an error. This is a server-side existence check, not a conflict check against other in-flight transactions.

### Value assertions (`assert_value`)

`tx.assert_value(key, expected)` registers a precondition against the transaction's frozen start snapshot: the key must hold `expected` (or be absent, for `expected = None`) in that snapshot, and no subsequently sequenced point mutation or covering range deletion may occur before runtime commit serialization. This is a two-part check:

1. **Client-side, at `commit()`**: the expected value is compared against the transaction's frozen start snapshot. This is a fast local fail and is where value equality is actually checked.
2. **Server-side, at the runtime's apply path**: only the *key* crosses into the runtime (never the expected value). The runtime checks whether the key received a point mutation or a covering range deletion with a sequence greater than the transaction's `start_sequence` — the same check `AbortOnWriteConflict` uses for write-set keys, applied to asserted keys instead of written ones.

The server-side check runs unconditionally, **regardless of `ConflictPolicy`**: an explicit assertion is a stronger, opt-in guarantee than the ambient conflict policy, so it is enforced even under `LastWriteWins`. It is a sequence comparison, not a value re-read, which makes it ABA-safe — a concurrent writer that changes the value and then restores it still advances the sequence and is still correctly rejected.

Assertions always compare with the frozen start snapshot, even though `get()`
provides read-your-own-writes behavior. A pending `put` or `delete` in the same
transaction therefore does not change what `assert_value` validates. The guard
is installed only when `assert_value` returns `Ok(())`; callers must not ignore
its result. Assertion storage and resident write intents share the bounded
engine-wide transaction memory pool, and either consumer can receive
`ResourceLimit` when concurrent transactions exhaust that pool.

A transaction with only assertions and no writes commits without allocating a sequence, appending to the WAL, or touching a memtable. It validates at the runtime's serialization point, then applies the selected durability barrier: local `sync()` performs a covering WAL fsync, while `cloud_strict()` requests cloud coverage at the current runtime sequence. An already-covered cloud sequence returns immediately; a pending sequence seals when necessary or joins its covering in-flight upload and waits.

### Snapshot isolation and isolation levels

| Mode | Isolation characterization |
|---|---|
| `ReadOnly` | **Consistent point-in-time snapshot reads** — all reads see the committed state at `begin_tx` time. Concurrent commits are invisible for the lifetime of the transaction. |
| `ReadWrite` | **Snapshot reads + read-your-own-writes + configurable commit conflict policy** — reads are fixed to the snapshot at `begin_tx` time (not re-read on later commits). In default mode, writes are last-write-wins. In strict mode, overlapping writes abort with `WriteConflict`. This is not read committed: later committed data remains invisible to the transaction regardless of when it reads. |

---

## Snapshot behavior

### How snapshots are created

Every `begin_tx` call does a single wait-free `ArcSwap::load()` on the `SnapshotCache`. The cache holds the most recently published `PublishedSnapshot`, which contains:

- `sequence: u64` — the sequence at the time of the last publish.
- A `HashMap<cf_id, CfSnapshotData>` mapping each column family to its `ReadSnapshot`.

A `ReadSnapshot` holds:
- `Arc<SkipListMemtable>` — reference-counted pointer to the active memtable.
- `Vec<Arc<SkipListMemtable>>` — reference-counted pointers to immutable memtables awaiting flush.
- `Vec<FileMeta>` — a clone of the manifest's file list at publish time.

The `Arc` references keep memtables alive as long as any `Transaction` holds a `ReadSnapshot`. `FileMeta` records are plain metadata (file name, level, key range, sequence range); they do not pin SST files on disk.

### How long snapshots live

A snapshot lives as long as the `Transaction` struct that holds it. When the `Transaction` is dropped (after commit, rollback, or explicit drop), the `Arc` counts on the memtables decrement. Memtables are freed when all reference counts reach zero.

Transactions are registered in `RuntimeState.active_snapshots` during `begin_tx` and unregistered on commit, rollback, or drop. Each registration includes sequence and pinned SST names used by compaction/GC retention logic.

### Consistency guarantee

A `ReadOnly` transaction guarantees a **consistent point-in-time snapshot**: all reads within the transaction reflect the committed state of the engine at the moment `begin_tx` was called. No write committed after `begin_tx` is visible, regardless of how long the transaction remains open.

A `ReadWrite` transaction provides the same read guarantee but offers no protection against concurrent writes to the same keys.

### Long snapshots and compaction

Transactions are registered as active snapshots on `begin_tx` and unregistered on commit, rollback, or drop. Compaction reads `oldest_active_snapshot_sequence()` and keeps tombstones newer than that horizon. GC also skips SSTs pinned by active snapshots. A long-lived `ReadOnly` transaction can therefore increase retention pressure while it remains open.

The snapshot lifetime threshold is an observability and pressure signal, not an automatic eviction boundary. Timed-out snapshots continue to pin their SST files until the owning `Transaction` unregisters, because retaining old files is safer than invalidating a live point-in-time read.

---

## Compaction and version GC

### When old versions are removed

Compaction merges SST files across levels. During a merge, each key's entries are sorted by sequence number. All but the highest-sequence entry for a given key are dropped. Tombstones are also dropped (see below). Expired entries (TTL) are dropped based on wall-clock time at compaction time.

### Tombstone handling

The compaction executor calls `filter_tombstones_with_horizon(versions, snapshot_horizon)`:

```rust
match snapshot_horizon {
    Some(h) => retain tombstones with seq > h,   // keep tombstones newer than active readers
    None    => drop all point-key tombstones,     // current behavior
}
```

Because active snapshots are registered, `oldest_active_snapshot_sequence()` reflects the oldest live reader. Point-key tombstones older than that horizon can be dropped; newer tombstones are retained.

Range tombstones are handled separately and are written into the output SST. They are not subject to this filter.

### How Midge prevents removing versions still needed by readers

Midge uses two mechanisms:

- Snapshot horizon retention in compaction (`oldest_active_snapshot_sequence`) to keep tombstones needed by active readers.
- SST pinning in GC via snapshot registrations so files referenced by active snapshots are not deleted.

Long-lived snapshots can increase retention pressure, but the model is wired for correctness.

### Interaction between compaction and active transactions

Compaction reads `state.oldest_active_snapshot_sequence()` and applies that horizon to tombstone filtering. This coordinates compaction retention with active transaction snapshots.

### SST GC pinning

`get_pinned_sst_names()` returns the union of SST names pinned by active snapshots. GC skips deleting those files until snapshots unregister.

---

## Isolation guarantees

**`ReadOnly` transactions:**

- Dirty reads: **not possible.** Writes are not visible until commit.
- Repeatable reads: **guaranteed.** `start_sequence` is fixed at `begin_tx`.
- Phantom protection: **not guaranteed.** Range scans see only the snapshot at `begin_tx`; new keys inserted after that are invisible. This is snapshot-consistent behavior, not predicate-locking-based phantom prevention. New keys inserted between two sequential scans within the same transaction are invisible — which may be interpreted as phantom protection, but there is no locking mechanism enforcing it; it is a consequence of the fixed snapshot.
- Write conflicts: **not applicable** — `ReadOnly` transactions do not write.

**`ReadWrite` transactions:**

- Dirty reads: **not possible.** Same snapshot read mechanism.
- Repeatable reads: **provided for the fixed transaction snapshot**, plus read-your-own-writes for buffered keys. Concurrent commits remain invisible, but conflicting writes are not detected at commit.
- Phantom protection: **not provided.**
- Write conflicts: **policy-dependent.** Default mode is last-write-wins; strict mode aborts overlapping writes with `WriteConflict`.
- Lost updates: **allowed in default mode**, prevented for overlapping write-sets in strict conflict-abort mode.
- Value assertions (`assert_value`): **enforced regardless of `ConflictPolicy`.** An asserted key is checked against the same commit-time serialization point as write-set keys, independent of whether the transaction is in last-write-wins or strict mode.

**Non-guarantees (all modes):**

- Serializable isolation is not implemented.
- Serializable Snapshot Isolation (SSI) is not implemented.
- No predicate locking or range locking.
- No multi-CF atomic transactions.

---

## Recovery interaction

### WAL replay and MVCC state

WAL records carry four fields relevant to MVCC: `seq`, `txn_id`, `writer_epoch`, and `op_kind` (which includes `TxnBatch` for committed transactions).

Recovery (`src/wal/recovery.rs`) reconstructs state as follows:

1. Scan WAL records in order.
2. On `TxnBatch`: validate the nested payload, then apply every enclosed op atomically with its original sequence numbers.
3. Legacy split-marker transactions are still understood by recovery for internal coverage, but released format v2 writes `TxnBatch` by default.

### Partially committed transactions

A transaction is either fully committed (the `TxnBatch` frame and nested payload are intact) or fully absent (a truncated or malformed transaction frame is discarded). There is no state in which a subset of a transaction's ops are visible after recovery.

The WAL actor includes fail-point annotations at:
- `midge::wal::txn_after_ops_append_before_commit` — crash here before the transaction frame append → transaction absent on replay.
- `midge::wal::txn_after_commit_append_before_sync` — crash here after the `TxnBatch` frame append but before sync → strict durability depends on whether the frame reached stable storage.

### Atomicity preservation

Atomicity is preserved by the WAL's single-frame `TxnBatch` encoding combined with recovery's all-or-nothing application. The sequence number range for a transaction is allocated atomically by the single-threaded event loop; no partial allocation is possible.

The `writer_epoch` field on each WAL record prevents zombie writes from stale leaders: recovery skips records from any epoch lower than the maximum epoch seen in the log.

After recovery, `RuntimeState.sequence` is set to the maximum sequence number found in the WAL. There is no separate MVCC metadata file; the sequence number embedded in every WAL record and SST entry is the only version state.

---

## Known limitations

- **Long-lived snapshots increase storage pressure**: while a snapshot is active, compaction retains newer tombstones and GC skips overlapping SSTs. This preserves correctness but can temporarily increase disk usage and compaction debt.

- **Strict mode covers write-write conflicts, not serializability**: `AbortOnWriteConflict` detects overlapping writes (including delete-range overlap checks), but does not provide SSI/serializable guarantees.

- **Single column family per transaction**: `begin_tx` accepts one `cf_id`. There is no mechanism for a single atomic transaction to span multiple column families.

- **No serializable isolation**: Read-write transactions use snapshot reads captured at `begin_tx`, plus read-your-own-writes, with last-writer-wins commit semantics. Serializable isolation, SSI, and write-write conflict detection are not implemented.

- **No explicit abort record in WAL**: Rollback leaves no WAL trace. This is safe (the write_set is discarded), but there is no audit trail for rolled-back transactions.

- **`best_effort` mode loses data on crash**: When `WriteOptions::best_effort()` is used, WAL is skipped. Data in the memtable is lost if the process crashes before flush. This is a documented trade-off.

---

## Future improvements (optional)

- Add richer operator metrics for snapshot retention pressure (for example, pinned SST count and oldest snapshot age alerts).
- Consider optional conflict detection for `ReadWrite` transactions to prevent lost updates in workloads that need stricter semantics.

---

## Simple example

```rust
use cntryl_midge::{MidgeEngine, MidgeResult, OpenOptions, TransactionMode, WriteOptions};

fn example(engine: &MidgeEngine, cf_id: u32) -> MidgeResult<()> {
    // Start a read-write transaction.
    // Captures the current snapshot sequence synchronously (no event loop round-trip).
    let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite)?;

    // Read — checks write_set first, then the snapshot at begin_tx time.
    let existing = tx.get(b"counter")?;
    let count: u64 = existing
        .as_deref()
        .and_then(|b| b.try_into().ok())
        .map(u64::from_be_bytes)
        .unwrap_or(0);

    // Write — appended to write_set, not yet visible to other readers.
    tx.put(b"counter".to_vec(), (count + 1).to_be_bytes().to_vec(), None)?;

    // Commit — sends all writes atomically to the event loop.
    // WAL write + memtable apply happen before this call returns.
    tx.commit(WriteOptions::buffered())?;

    // Read-only snapshot transaction — sees the committed state.
    let ro = engine.begin_tx(cf_id, TransactionMode::ReadOnly)?;
    let val = ro.get(b"counter")?;
    assert!(val.is_some());
    // ro is dropped here; memtable Arc refcounts decrement.

    Ok(())
}
```

**Note on concurrent `ReadWrite` transactions:** default mode (`LastWriteWins`) allows lost updates under overlap. If that is unacceptable, set `tx.set_conflict_policy(ConflictPolicy::AbortOnWriteConflict)` before commit.
