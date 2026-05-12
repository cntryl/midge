# Durability Guarantees

This guide explains the exact acknowledgment boundary for each write mode in Midge. It is intentionally narrower than a general feature guide: the goal is to let an evaluator answer “what did `commit()` mean?” after a crash.

## Trust Boundary

A Midge write has three distinct states:

1. visible: the commit has been applied to the memtable and new readers can observe it
2. locally durable: the write survives restart from local storage
3. SST-published: the data is represented by manifest-visible SST state instead of WAL replay

`commit()` does not always wait for all three. The write option determines which boundary it waits for.

## Acknowledgment Table

| Write option | `commit()` returns after | Local durability at return | Crash outcome | Recovery source on restart |
|---|---|---|---|---|
| `WriteOptions::sync()` | WAL append and local fsync complete | Yes | write survives local crash assuming local storage survives | WAL replay or later SST state |
| `WriteOptions::buffered()` | WAL append barrier and memtable apply complete | Not yet guaranteed | write may be lost if crash happens before the batched fsync | WAL replay only if the later fsync completed |
| `WriteOptions::best_effort()` | memtable apply complete; WAL skipped | No | write is lost unless a later `flush_cf()` publishes it | SST state only if flush completed successfully |

## Per-Mode Semantics

### `sync()`

Use `sync()` when the caller wants local durability before `commit()` returns.

- WAL append happens before memtable visibility.
- The runtime waits for local fsync.
- A successful return means the write is in the local durable WAL prefix.

This is the strongest local durability mode and the easiest one to reason about during incident review.

### `buffered()`

Use `buffered()` when you want lower latency and accept a bounded crash window.

- `commit()` returns after the write crossed the local WAL append barrier and became visible in the memtable.
- The write is not yet guaranteed to survive restart.
- Local durability advances later through group fsync.

If the process crashes before that later fsync, the write may disappear on restart even though it was visible before the crash.

### `best_effort()`

Use `best_effort()` only for data that can be regenerated.

- No WAL record is required.
- `commit()` returns after the memtable update.
- Recovery cannot restore the write from WAL.

The write becomes durable only after a successful `flush_cf()` publishes new SST state.

## Fsync and Ordering

Midge relies on ordered WAL replay and explicit durability frontiers.

### WAL ordering guarantees

- Sequence numbers define recovery order.
- WAL files replay in segment order followed by the active WAL file.
- Partial tail records are dropped, never partially applied.
- Transaction markers prevent incomplete transactional WAL state from becoming visible during replay.

### Fsync behavior

- `sync()` waits for the local fsync boundary.
- `buffered()` does not wait for fsync; later batched sync makes the write durable.
- `best_effort()` does not use the WAL durability path.

## Crash Outcomes

### Crash after `sync()`

Expected result:

- the write is recovered from the local durable prefix
- recovery may rebuild it from WAL or from already-published SST state

### Crash after `buffered()`

Expected result:

- if the later fsync happened, the write is recovered
- if the crash wins the race, the write is lost

### Crash after `best_effort()`

Expected result:

- if no successful flush published the data, the write is lost
- if `flush_cf()` returned successfully before the crash, the SST-published state is recovered

### Crash during flush or compaction

Expected result:

- output files may exist
- manifest-visible state remains authoritative until publication completes
- recovery uses the intent log to publish or discard output idempotently

## What To Verify Before Evaluating Midge

Read these in order:

1. [../development/storage-invariants.md](../development/storage-invariants.md)
2. [../development/architecture.md](../development/architecture.md)
3. [../development/recovery-internals.md](../development/recovery-internals.md)
4. [../development/testing.md](../development/testing.md)

The tests mapped in the trust matrix are the executable form of the guarantees on this page.
