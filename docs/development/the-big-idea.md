# The Big Idea

Midge is an embedded LSM storage engine built around explicit state transitions.
It favors predictable recovery, inspectable durability, and deterministic
orchestration over hidden concurrency or opaque background behavior.

For user-facing use cases, see [../user-guides/overview.md](../user-guides/overview.md).
For module boundaries and recovery order, see [architecture.md](architecture.md).

## Core Principle

All mutable engine state is sequenced through the runtime actor. Background work
may build SSTs, compress data, or upload objects, but it reports immutable
results back to the actor before authoritative state changes.

The design goal is simple: if state changed, there should be a message,
sequence number, WAL record, manifest update, or intent-log entry that explains
why.

## Architectural Pillars

### Actor-Sequenced Core

The event loop owns the mutable runtime state:

- sequence allocation
- memtable visibility
- snapshot registration
- WAL lifecycle
- flush and compaction publication
- backpressure and durability frontier updates

Workers receive immutable inputs and return results. They do not directly mutate
manifest-visible state or runtime bookkeeping.

This gives Midge a single ordering surface for correctness audits. The tradeoff
is that throughput is bounded by a serialization point; the project accepts that
cost for a simpler recovery model.

### Explicit Storage Modes

Midge supports three storage modes:

- **Memory**: ephemeral state for tests and disposable workloads
- **Local**: local filesystem is authoritative
- **Cloud**: cloud storage is the durability target and local disk acts as a cache

Cloud mode is not a storage plugin hidden behind the local path. It changes WAL
durability, SST placement, startup materialization, and cleanup proof rules.
Local cache loss must be recoverable from authoritative cloud WAL/SST state once
the relevant cloud durability frontier has advanced.

### Embedded API

Midge runs in-process as a library. Public APIs are synchronous and explicit:

- callers choose the storage mode
- callers choose `WriteOptions`
- callers control lifecycle and shutdown
- observability APIs expose recovery, runtime, and storage layout state

The API avoids a hidden async runtime or service boundary. Applications that
want concurrency can place Midge behind their own worker threads.

## Write Path

Writes move through the same ordered path:

```text
transaction operations
  -> sequence planning
  -> WAL append when durability mode requires it
  -> memtable apply
  -> visibility update
  -> explicit background work when needed
```

`WriteOptions` decide the acknowledgment boundary:

- `sync()` waits for local WAL fsync
- `buffered()` waits for local WAL append and visibility, with fsync later
- `best_effort()` skips WAL and depends on later SST publication for recovery
- `cloud_strict()` waits for cloud WAL acknowledgment in cloud-backed mode

See [../user-guides/transaction-durability-contract.md](../user-guides/transaction-durability-contract.md)
for the external contract.

## Flush and Compaction Publication

Flush and compaction are publication workflows, not direct state mutation:

1. the actor records intent
2. a worker builds immutable output
3. the actor validates the result
4. manifest state publishes the new SST set
5. obsolete files become cleanup candidates only after publication is durable

If a process crashes between those steps, recovery uses the manifest and intent
log to decide which outputs are authoritative. Orphan files can be removed; live
state must not be inferred from filesystem listings alone.

## SST Format

Midge uses a custom SST format with:

- sparse index data for block lookup
- metadata for key and sequence ranges
- Bloom filters for point lookup reduction
- optional compression
- block sizing that can be tuned for local or object-storage patterns

The format is not RocksDB-compatible. The benefit is a smaller design surface
for Midge's recovery, metadata, and cloud-read assumptions.

## Recovery Model

Startup reconstructs trusted state in a fixed order:

1. acquire the primary lease
2. load manifest and intent-log state
3. replay the durable WAL prefix
4. replay interrupted publication intents
5. resume runtime operation with recovery metrics populated

Recovery does not search the filesystem for truth. Manifest state, WAL frames,
and intent-log records define which data is authoritative.

See [recovery-internals.md](recovery-internals.md) for failure-mode details.

## Observability

Midge exposes runtime and recovery state so failures can be audited:

- recovery metrics for WAL and intent-log replay
- runtime metrics for sequence, flush, compaction, backpressure, and health
- storage layout snapshots for SST and WAL state
- verification APIs for storage consistency checks

Tests should validate behavior through those surfaces where possible, not by
assuming internal timing or hidden background work.

## Tradeoffs

Midge deliberately chooses:

- synchronous APIs over a hidden async public surface
- actor sequencing over sharded mutable state
- explicit durability modes over implicit background promises
- deterministic publication over opportunistic cleanup
- a custom SST format over compatibility with existing table files

Those choices make the system easier to reason about, but they do not make it a
raw-throughput benchmark target. Performance work should preserve the recovery
and publication contracts first.

## Related Documentation

- [architecture.md](architecture.md)
- [architecture-diagrams.md](architecture-diagrams.md)
- [storage-invariants.md](storage-invariants.md)
- [recovery-internals.md](recovery-internals.md)
- [testing.md](testing.md)
