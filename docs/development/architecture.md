# Storage Architecture Overview

This document is the storage-engine map for Midge as an experimental crate that is safe enough to evaluate. It focuses on the parts that control durability, crash recovery, and read correctness rather than the full runtime feature set.

For visual diagrams of the module boundaries and data flows, see [architecture-diagrams.md](architecture-diagrams.md).

## Purpose

Midge is an embedded LSM engine with four storage-critical subsystems:

- WAL: accepts committed writes before they are flushed
- Memtable: holds the newest ordered state in memory
- SST files: immutable durable files used for reads and compaction
- Manifest and intent log: publish durable file-set changes and recover interrupted flush or compaction work

The core trust model is:

1. A write is acknowledged at a defined durability boundary.
2. Recovery rebuilds in-memory state from the durable prefix.
3. Flush and compaction publish new SST state atomically or leave the previous state authoritative.

## Write Path

```text
write
  -> WAL
  -> memtable
flush
  -> SST
compaction
  -> new SST levels
```

### Commit flow

1. `Transaction::commit` sends the transaction into the runtime.
2. The WAL actor allocates sequence numbers and appends WAL records unless the caller chose `WriteOptions::best_effort()`.
3. After the local visibility barrier succeeds, the same operations are applied to the active memtable.
4. The runtime either:
   - waits for local fsync for `sync()`
   - returns after WAL append for `buffered()`
   - skips the WAL and returns after memtable apply for `best_effort()`

### What the caller sees

- New reads observe the committed sequence once the runtime updates the memtable.
- Durability depends on the chosen write option, not just visibility.
- Flush and compaction are background publication steps; they do not redefine the meaning of an earlier commit acknowledgment.

## Storage Layout

For local storage, Midge persists a database directory containing:

- `wal/`: active WAL plus rotated segments used for replay
- `sst/`: immutable SST files produced by flush and compaction
- manifest files and journal: authoritative published SST set and durable sequence tracking
- intent log: interrupted publication state for flush and compaction replay
- lease files: single-writer protection for local mode

The exact filenames can change over time, but the recovery contract is stable:

- WAL durability protects writes that are not yet published into SST state.
- Manifest state identifies which SST files are authoritative for reads.
- The intent log bridges the gap between “output files exist” and “manifest state is authoritative.”

## WAL

The WAL is the first durable landing zone for writes in local durable modes.

### Responsibilities

- preserve operation order with sequence numbers
- detect torn or corrupted frames during replay
- provide a durable prefix that recovery can trust
- support salvage of a valid tail-truncated prefix when policy allows

### Replay rules

- WAL files are replayed in segment order, then the active file.
- Partial tail records are never applied.
- Strict recovery fails open on corruption at byte 0 or invalid corrupted frames.
- Salvage mode keeps the valid prefix and reports degraded recovery.

### Dependency boundary

- WAL replay depends on the base `io::Fs` and `io::File` abstractions, not `storage`.
- Hybrid storage owns cloud orchestration for WAL segment upload, readback proof, and pruning.
- Byte-level WAL segment interpretation, including cloud WAL object-key formatting and transaction-batch expansion, lives in `src/wal/cloud_segment.rs`.
- Storage policy code consumes WAL-owned coverage records and combines them with manifest/SST proof before pruning remote WAL.

## Memtable

The memtable is the newest readable state.

### Responsibilities

- apply committed puts, deletes, and range tombstones in sequence order
- serve reads before data reaches SST files
- freeze into an immutable memtable before flush

### Lifecycle

- `Active`: receives new committed writes
- `Immutable`: frozen and waiting to flush
- `Published`: removed once its SST output is durably published

## SST Files

SST files are immutable sorted files used for durable reads and compaction.

### Responsibilities

- store flushed or compacted key ranges durably
- preserve sequence metadata for read resolution
- remain immutable once published into the manifest

### Read resolution

Reads combine:

1. active memtable
2. immutable memtables
3. manifest-visible SST files

Sequence order, tombstones, and range tombstones determine the visible value.

## Flush Publication

Flush is a two-step operation:

1. write a new SST file from an immutable memtable
2. publish that SST into manifest state

If Midge crashes between those steps, recovery uses the intent log to decide whether the new SST should be published or discarded. The old authoritative state remains valid until manifest publication completes.

That is why a failed flush must not expose an orphan SST as committed durable state.

## Compaction Publication

Compaction also separates output creation from publication:

1. read manifest-visible input SSTs
2. write replacement SST outputs
3. publish the replacement file set to the manifest
4. delete obsolete input SSTs only after the replacement state is durable

If a crash happens after output creation but before manifest publication, the input SSTs stay authoritative. If the crash happens after manifest publication, recovery finalizes cleanup idempotently.

Compaction workers are transient executors. They must receive plans that are already safe to publish: the event loop/`RuntimeState` scheduling boundary assigns compaction output identity and the current snapshot horizon before a worker starts. Raw plans returned by the strategy layer use `output_seq == 0` as an unpublishable placeholder and must not reach actor execution directly.

## Recovery Sequence

At open, Midge reconstructs trusted state in this order:

1. acquire the lease for single-writer access
2. load manifest state and last durable publish sequence
3. replay the WAL durable prefix into memtables
4. replay intent-log publication state for interrupted flushes or compactions
5. resume normal operation with updated recovery metrics

See [recovery-internals.md](recovery-internals.md) for the failure-mode details.

## Storage-Critical Code Map

Use this reading order if you are auditing correctness:

1. `src/engine/mod.rs`
   Engine open, public durability surface, verification APIs
2. `src/wal/recovery.rs`
   WAL replay ordering, corruption handling, salvage boundaries over `io::Fs`
3. `src/runtime/actors/wal.rs`
   commit-time WAL append and durability frontier handling
4. `src/wal/cloud_segment.rs`
   cloud WAL segment key formatting, frame validation, and data-coverage extraction
5. `src/runtime/actors/flush.rs`
   memtable freeze, SST creation, flush publication staging
6. `src/runtime/event_loop/mod.rs`
   flush publication, compaction launch identity, and runtime orchestration
7. `src/runtime/actors/compaction.rs`
   transient compaction execution and completion handoff
8. `src/metadata/manifest.rs` and `src/runtime/intent_persistence.rs`
   authoritative file-set publication and interrupted publication replay

## Audit Checklist

When evaluating Midge for early adoption, verify that the following questions are easy to answer from code and tests:

- When does `commit()` return for each write mode?
- What state is authoritative if the process crashes mid-flush?
- What state is authoritative if the process crashes mid-compaction?
- Which errors mean corruption, recovery failure, or operator-actionable space pressure?
- Which tests prove the guarantees you care about?

For the invariant list that defines “correct enough to try,” see [storage-invariants.md](storage-invariants.md).
