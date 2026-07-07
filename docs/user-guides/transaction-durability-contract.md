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
- `RecoveryPolicy::Strict`

## Core Model

Midge uses explicit transactions. There is no auto-commit mode.

- `TransactionMode::ReadOnly` provides snapshot-based reads
- `TransactionMode::ReadWrite` provides snapshot-based reads plus buffered writes
- `commit()` is the only operation that publishes a read-write transaction
- dropping an uncommitted read-write transaction rolls its writes back

Within a committed transaction:

- all writes publish atomically
- all writes receive one commit sequence number
- readers either observe the full committed batch or none of it

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

Midge intentionally permits last-writer-wins behavior for overlapping writes.

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
- applications that must reject lost updates must enforce that policy themselves

## Lost Update Posture

Callers must assume that read-modify-write races can lose updates unless the application adds its own coordination.

Examples of patterns that are not guaranteed to fail automatically:

- two transactions read the same counter, increment it, and both commit
- two transactions read the same value, derive new values, and commit in sequence

If the application requires compare-and-set behavior or strict lost-update prevention, that logic belongs above the storage engine.

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
3. cloud durable: the cloud-backed WAL segment upload has been acknowledged
4. SST-published: the data is represented by manifest-visible SST state instead of WAL replay

`commit()` does not always wait for all of them. The selected `WriteOptions` define the acknowledgment boundary.

| Write option | `commit()` returns after | Local durability at return | Crash outcome | Recovery source on restart |
|---|---|---|---|---|
| `WriteOptions::sync()` | WAL append and local fsync complete | Yes | write survives local crash assuming local storage survives | WAL replay or later SST state |
| `WriteOptions::buffered()` | WAL append barrier and memtable apply complete | Not yet guaranteed | write may be lost if the process crashes before the batched fsync | WAL replay only if the later fsync completed |
| `WriteOptions::best_effort()` | memtable apply complete; WAL skipped | No | write is lost unless a later `flush_cf()` publishes it | SST state only if flush completed successfully |
| `WriteOptions::cloud_strict()` | cloud-backed WAL seal, rotate, upload, and acknowledgment complete | Cloud durable in cloud mode | write survives local cache loss after cloud acknowledgment | uploaded WAL segment or later SST state |

### `WriteOptions::sync()`

`commit()` returns only after local WAL append and fsync complete.

At return:

- the transaction is visible
- the transaction is locally durable
- restart should recover the transaction assuming local storage survives

Use `sync()` for data that must survive a local crash before the caller continues.

### `WriteOptions::buffered()`

`commit()` returns after WAL append barrier and memtable visibility, but before local fsync is guaranteed.

At return:

- the transaction is visible
- the transaction is not yet guaranteed to survive restart
- a crash before the later fsync may lose the transaction

Use `buffered()` when lower latency is more important than eliminating the bounded crash window.

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
- automatic lost-update rejection for arbitrary read-modify-write races
- multi-process shared-writer coordination
- production cloud guarantees beyond what the support matrix promotes explicitly
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
- [../development/one-dot-zero-contract.md](../development/one-dot-zero-contract.md)
- [../development/support-matrix.md](../development/support-matrix.md)
- [../development/recovery-internals.md](../development/recovery-internals.md)
