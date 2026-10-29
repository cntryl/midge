# Transaction and Durability Contract

This document is the canonical external contract for Midge transaction semantics and write acknowledgment behavior.

It is intentionally narrower than an implementation walkthrough. The goal is to let an adopter answer four questions from one page:

- what reads and writes a transaction can observe
- what `commit()` guarantees
- what behavior is intentionally last-writer-wins
- what survives crash under each durability mode

This contract describes the current intended semantics for the supported local-first production target:

- single-process embedded deployment
- local-disk storage mode
- cloud-backed storage mode with explicit cloud durability options
- `RecoveryPolicy::Strict`

## Core Model

Midge uses explicit transactions. There is no auto-commit mode.

- `TransactionMode::ReadOnly` provides snapshot-based reads
- `TransactionMode::ReadWrite` provides snapshot-based reads plus buffered writes
- `commit()` and `commit_with_sequence()` are the only operations that publish a read-write transaction; the latter also returns the published sequence
- dropping an uncommitted read-write transaction rolls its writes back

Within a committed transaction:

- all writes publish atomically
- write operations occupy one contiguous transaction sequence range
- readers either observe the full committed batch or none of it

Successful writes publish in their admitted sequence order. Preparing or
queuing a later write does not make it visible ahead of an earlier unapplied
transaction. A later `best_effort()` commit can wait behind a Strict group;
that wait does not give the BestEffort write a WAL durability guarantee.

## Shutdown and outliving transactions

Use `Engine::shutdown(timeout)` for explicit graceful shutdown. It returns
`Busy` while transactions remain live, and cleanup can be retried after those
transactions finish. A caller timeout does not cancel admitted storage work.

Dropping an Engine delegates cleanup to an owned reaper. Its transaction wait
defaults to 30 seconds and is configured with
`OpenOptionsBuilder::drop_transaction_wait_timeout`. When that window expires,
outliving transactions return `Fenced` from reads, iterator rows and commits,
including cached reads. This policy applies to memory, local and cloud modes.

The reaper drains admitted WAL/runtime/storage effects before releasing writer
authority. A physical effect that cannot be storage-fenced must finish first,
so blocked device or provider I/O can retain the lease beyond the transaction
wait window. The window bounds retention caused by a leaked transaction; it
does not promise a hard takeover deadline for unfenceable I/O.

## Read Semantics

### Read-only transactions

`ReadOnly` transactions read from a stable snapshot captured at `begin_tx()`.

- repeated reads within the same transaction observe the same committed view
- uncommitted writes from other transactions are never visible
- later commits from other transactions do not change the snapshot already held by the reader

### Read-write transactions

`ReadWrite` transactions also read from a snapshot captured at `begin_tx()`, plus their own uncommitted writes.

- read-your-own-writes is supported
- scans reflect the transaction's own writes and deletes
- uncommitted writes remain invisible to other transactions until commit

This is the practical transaction model an external caller should assume:

- snapshot-based reads
- atomic commit visibility
- no dirty reads from other transactions
- no promise of full serializable isolation

## Write-Write Semantics

Midge's default `ConflictPolicy::LastWriteWins` intentionally permits
last-writer-wins behavior for overlapping writes. Callers can instead select
`ConflictPolicy::AbortOnWriteConflict` for overlapping write-set detection or
attach explicit value preconditions with `Transaction::assert_value`.

For conflicting writes to the same key:

- both transactions may commit successfully
- the later committed value becomes the visible value for subsequent readers
- the engine does not promise automatic prevention of all lost-update patterns

For non-overlapping writes:

- independent transactions are expected to commit without conflicting with each other

For delete and delete-range interactions:

- `put`, `delete`, and `delete_range` operations may be freely mixed within a single transaction
- all operations in a transaction commit atomically
- range tombstones and point operations follow the same sequence-based ordering rules
- later committed effects win over earlier overlapping effects

The externally visible rule is simple:

- Midge provides atomic commits and snapshot reads
- Midge does not promise serializable conflict detection
- applications that must reject lost updates must explicitly select a conflict
  policy or assert the values on which an update depends

## Lost Update Posture

Callers using the default conflict policy must assume that read-modify-write
races can lose updates unless they add an explicit guard.

Examples of patterns that are not guaranteed to fail automatically:

- two transactions read the same counter, increment it, and both commit
- two transactions read the same value, derive new values, and commit in sequence

`ConflictPolicy::AbortOnWriteConflict` rejects mutations that overlap the
transaction's write set after its snapshot. It does not make the transaction
serializable and does not protect values that were read but not written.

For compare-and-set behavior, call `assert_value(key, expected)` before staging
the replacement. Pass `None` to require absence. The assertion:

- compares `expected` with the transaction's frozen start snapshot, not with a
  pending write in the same transaction
- rejects any later point mutation or covering range deletion before runtime
  commit serialization, even under `LastWriteWins`
- uses sequence movement as its runtime guard, so changing a value and restoring
  it does not evade the assertion
- reserves from the shared transaction memory pool and can return
  `MidgeError::ResourceLimit`; callers must not ignore that result

Assertions protect only the named keys. They do not provide predicate locking,
phantom protection, or general serializable isolation.

An assertion-only commit allocates no new sequence and writes no transaction
frame. It still validates at runtime serialization and honors the selected
durability barrier: local `sync()` establishes a covering fsync, while
`cloud_strict()` establishes cloud coverage for the current runtime sequence.

## Atomicity and Crash Semantics

`commit()` publishes the transaction atomically, but durability depends on the selected `WriteOptions`.

Atomicity guarantees:

- committed transactions recover as all-or-nothing units
- incomplete transactional WAL state is not made visible during replay
- restart recovery may restore a committed transaction from WAL or from already-published SST state
- uncommitted transactions must not become visible after restart

This means:

- visibility atomicity and crash atomicity are part of the contract
- durability timing is a separate choice controlled by `WriteOptions`

## Durability Modes

A write can cross several boundaries:

1. visible: the commit has been applied to the memtable and new readers can observe it
2. locally durable: the write survives restart from local storage
3. cloud durable: the cloud-backed WAL segment upload has been acknowledged and published in the WAL catalog
4. SST-published: the data is represented by manifest-visible SST state instead of WAL replay

`commit()` does not always wait for all of them. The selected `WriteOptions`
define the acknowledgment boundary on a successful return.

### Three frontiers

Every engine tracks three monotonic sequence frontiers. A write at sequence
`s` has crossed a frontier once the frontier is at or above `s`.

| Frontier | Meaning | How to observe |
|---|---|---|
| visible | new readers see the write | `begin_tx` snapshots |
| local durable | the write survives a process crash while the local disk or cache survives | `RuntimeMetricsSnapshot::wal_local_durable_seq` |
| cloud durable | the write survives loss of the whole local cache and is recoverable by a successor opening from the cloud | `Engine::cloud_durable_sequence()`, `RuntimeMetricsSnapshot::wal_cloud_durable_seq` |

What each write option guarantees at `commit()` return, and what survives
each failure:

| Write option | Frontier crossed at return | Process crash | Local disk or cache loss | Writer takeover by another process |
|---|---|---|---|---|
| `sync()` | local durable | kept | lost (local-only storage) | not applicable |
| `buffered()` | visible | kept only if the later fsync ran | lost | not applicable |
| `best_effort()` | visible | lost unless a later flush published it | lost unless a later flush published it | lost unless a later flush published it; in cloud mode the successor recovers only published state |
| `cloud_async()` | visible (local durable follows; cloud durable follows asynchronously) | kept on same-cache restart from intact local WAL | kept only if the covering upload was acknowledged | not promised: a successor recovers the published cloud state; unpublished writes stay with the surviving cache and cannot override the shared catalog |
| `cloud_strict()` | cloud durable | kept | kept | kept: the successor recovers it from the published WAL or SST state |

To take local-latency commits and still get the cloud guarantee where it
matters, commit with `cloud_async()` through
`Transaction::commit_with_sequence`, which returns the commit's sequence, and
call `Engine::wait_cloud_durable(sequence, timeout)` only where the caller
must know the write survives cache loss. The wait returns `Ok(true)` once the
cloud frontier covers the sequence and `Ok(false)` when it does not yet: the
timeout elapsed, or the runtime reported backpressure or a transient failure
while it keeps retrying the covering upload. Neither is final; call again
later. A wait may seal the open WAL segment early so the covering upload
starts, as `cloud_strict()` does.
Both calls return `MidgeError::InvalidArgument` on storage that is not
cloud-backed. A `MidgeError::Fenced` from the wait means this writer lost
authority and can no longer prove cloud durability for anything above the
frontier it last published.

Reads do not take a durability level: a reader sees every visible write,
including `cloud_async()` writes that are not yet cloud durable. A read
pinned at the cloud-durable frontier is tracked as a separate feature (#958).

Every synchronous runtime response also has a deadline. When the response to a
commit with writes misses it, the runtime may already have accepted the
transaction and may complete it later: the timeout removes the caller's response
route but does not cancel the mutation. `commit()` therefore reports that case
as `MidgeError::OutcomeUnknown`, not as `MidgeError::Timeout`, so
`severity()` never suggests a retry for a write that may have landed. Use an
application-level idempotency check before writing it again. A plain
`MidgeError::Timeout` from `commit()` comes only from commits that cannot have
changed state.

A writer timeout that carries proof of no physical effects, such as rejection
before enqueue or a durably completed rollback, therefore remains a typed
`Timeout` through the runtime response and transaction API. The caller-side
wait alone has no such proof. WAL append failures retain their existing
fencing behavior regardless of the error shown to the caller.

When a commit with writes has crossed the WAL or visibility boundary but Midge
cannot confirm its requested durability, `commit()` reports
`MidgeError::OutcomeUnknown`. This includes a failed sync after the WAL append,
or a `cloud_strict()` seal and upload acknowledgment that cannot finish because
of shutdown, upload backpressure or repeated upload failures. The write may be
visible now or replay whole after restart, so check its outcome before retrying.
A failed WAL upload keeps retrying in the background, so such a write may still
become cloud-durable.

`MidgeError::OutcomeUnknown` also reports a write whose WAL record was written
but which did not reach the memtable before the writer was fenced, for example
because its writer lease lapsed, its leader store could not confirm authority,
or a WAL write failed partway and could not be rolled back. The write is not
visible now, but restart replays its WAL record and may apply it, whole. Neither
a lapsed lease nor a failed sync after the WAL append makes the write "not
applied".

When writes share one WAL append and the writer is fenced partway through
applying them, a write that already reached the memtable also reports
`OutcomeUnknown`: it is visible, but its WAL record may not be synced and may
not survive a crash. A `sync()` write in the same position is acknowledged,
because its record was synced before it was applied.

`MidgeError::Fenced` from `commit()` means the write was not applied and has
no WAL record restart could replay. That includes a write that waited
behind a failed batch and was never logged. Midge never retries a write after
`OutcomeUnknown`, so a caller that retries must first check whether the write
applied, as described above.

`abandoned_runtime_requests_total` and `late_runtime_responses_total` are global
diagnostic counters. Their deltas include unrelated requests and both successful
and failed late responses, so they cannot establish whether one specific timed-out
commit applied.

`WriteOptions::sync()` and `WriteOptions::buffered()` are local-only. `WriteOptions::cloud_async()` and `WriteOptions::cloud_strict()` are cloud-only. Non-cloud storage rejects the cloud-only modes, and cloud-backed storage rejects the local-only modes.

| Write option | `commit()` returns after | Local durability at return | Crash outcome | Recovery source on restart |
|---|---|---|---|---|
| `WriteOptions::sync()` | local WAL append and local fsync complete | Yes in local mode | local-disk write survives restart assuming local storage survives | WAL replay or later SST state |
| `WriteOptions::buffered()` | local WAL append barrier and memtable apply complete | Not yet guaranteed in local mode | local-disk write may be lost if the process crashes before the later fsync | WAL replay only if the later fsync completed |
| `WriteOptions::best_effort()` | memtable apply complete; WAL skipped | No | write is lost unless a later `flush_cf()` publishes it | SST state only if flush completed successfully |
| `WriteOptions::cloud_async()` | cloud-backed WAL append barrier and memtable apply complete; seal and upload continue asynchronously | Not yet guaranteed | an intact local WAL is replayed on same-cache restart; the write may be lost if local bytes do not survive and upload was not acknowledged | intact local WAL, uploaded WAL segment, or later SST state |
| `WriteOptions::cloud_strict()` | cloud-backed WAL seal, rotate, upload, and acknowledgment complete | Cloud durable in cloud mode | write survives local cache loss after cloud acknowledgment | uploaded WAL segment or later SST state |

### `WriteOptions::sync()`

`sync()` is valid only for local or in-memory storage. Cloud-backed storage rejects it with `MidgeError::InvalidArgument`.

`commit()` returns only after the transaction's independent WAL frame has been
appended and a local fsync covering that frame has completed. Independent
concurrent `sync()` transactions may share one physical fsync; every caller is
acknowledged only after that shared durability barrier succeeds.

At return:

- the transaction is visible
- the transaction is locally durable
- restart should recover the transaction assuming local storage survives

Use `sync()` for data that must survive a local crash before the caller continues.

### `WriteOptions::buffered()`

`buffered()` is valid only for local or in-memory storage. Cloud-backed storage rejects it with `MidgeError::InvalidArgument`.

`commit()` returns after WAL append barrier and memtable visibility, but before local fsync is guaranteed.

At return:

- the transaction is visible
- the transaction is not yet guaranteed to survive restart
- a crash before the later fsync may lose the transaction

Use `buffered()` when lower latency is more important than eliminating the bounded crash window.

### `WriteOptions::cloud_async()`

`cloud_async()` is valid only for cloud-backed storage. Non-cloud storage rejects it with `MidgeError::InvalidArgument`.

For a cloud-backed transaction with writes, `commit()` returns after the local WAL append barrier and memtable apply complete. The runtime still must seal the active WAL segment, upload it, and advance the cloud durability frontier later.

At return:

- the transaction is visible
- the transaction is not yet guaranteed to survive local cache loss
- the runtime still owes the cloud `seal` and `upload` work for the covering WAL segment

On a same-cache restart, cloud recovery merges intact local WAL files with the
downloaded remote WAL set before replay. This closes the process-restart window
without treating the local append barrier as an fsync or cloud-durability
guarantee. If the local bytes are lost before upload acknowledgment, the write
can still be lost.

Sealing a segment for upload fsyncs it before the rename, once per segment.
A sealed local segment is therefore complete after power loss, and recovery
does not refuse to open because of a torn sealed file.

Use `cloud_async()` when the engine must stage the write for cloud durability but the caller does not need to wait for cloud acknowledgment.

### `WriteOptions::best_effort()`

`commit()` returns after memtable visibility without requiring WAL durability.

At return:

- the transaction is visible
- the transaction is not crash durable
- recovery can keep the write only if later SST publication completed successfully

Use `best_effort()` only for data that can be rebuilt or safely discarded.

### `WriteOptions::cloud_strict()`

`cloud_strict()` is valid only for cloud-backed storage. Non-cloud storage rejects it with `MidgeError::InvalidArgument`.

For a cloud-backed transaction with writes, `commit()` submits the transaction to the local runtime first, then returns only after the runtime seals and rotates the active WAL segment, uploads that sealed WAL segment, and receives cloud acknowledgment covering the committed sequence.

The transaction may become visible to concurrent readers after local runtime apply and before the committing caller receives the cloud acknowledgment. `cloud_strict()` delays the caller's return until cloud durability is proven; it does not delay local publication until that return point.

At return:

- the transaction has been atomically published locally
- the transaction is covered by the cloud durability frontier
- restart after local cache loss should recover the transaction from uploaded WAL or already-published SST state

Empty cloud-backed `cloud_strict()` transactions are allowed and do not invent a WAL record. Empty non-cloud `cloud_strict()` transactions still reject the option.

## Recovery Policy Contract

### Crash outcomes

- After `sync()`: the write is recovered from the local durable prefix, either by WAL replay or already-published SST state.
- After `buffered()`: the write is recovered only if the later fsync completed before the crash.
- After `best_effort()`: the write is recovered only if a later `flush_cf()` successfully published it to SST state.
- After `cloud_async()`: a same-cache restart replays an intact local WAL even if upload had not completed. If those local bytes did not survive, recovery requires a completed cloud upload or later SST publication.
- After `cloud_strict()`: in cloud-backed mode, local cache loss after cloud acknowledgment should not lose the committed write.
- During flush or compaction: output files may exist, but manifest-visible state remains authoritative until publication completes.

### `RecoveryPolicy::Strict`

`Strict` is the supported production recovery policy for the local-first target.

- open fails on untrustworthy manifest, intent-log, or WAL state
- corruption and compatibility failures are treated as hard-stop conditions
- operators are expected to inspect and repair or restore rather than continue on an uncertain prefix

### `RecoveryPolicy::Salvage`

`Salvage` is a degraded recovery path.

- it may keep only a valid prefix
- it is intended for investigation and operator-controlled recovery
- it is not part of the default production contract unless promoted explicitly

## What Midge Does Not Promise

Midge does not currently promise:

- serializable transaction isolation
- automatic lost-update rejection for arbitrary read-modify-write races whose
  dependencies are not covered by the selected conflict policy or explicit
  value assertions
- multi-process shared-writer coordination
- deployment-specific cloud guarantees such as provider availability, IAM,
  quotas, network policy, lifecycle configuration, or workload capacity
- salvage-mode workflows as part of the standard production contract

## Evidence Surface

The contract on this page is backed by the current live test surface, including:

- transaction basics, conflicts, isolation, and LWW suites
- transaction spill and crash-boundary suites
- WAL, corruption, no-space, and reopen recovery suites
- recovery policy, observability, and adopter smoke suites

See also:

- [durability.md](durability.md)
- [api-guide.md](api-guide.md)
- [../development/stability-policy.md](../development/stability-policy.md)
- [../development/support-matrix.md](../development/support-matrix.md)
- [../development/recovery-internals.md](../development/recovery-internals.md)
