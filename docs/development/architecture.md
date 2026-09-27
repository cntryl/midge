# Storage Architecture Overview

This document is the storage-engine map for Midge as a supported pre-1.0 crate.
It focuses on the parts that control durability, crash recovery, and read
correctness rather than the full runtime feature set.

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
  -> partitioned replacement SSTs (or same-level overlap repair)
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
- `.midge_leader`: persistent epoch/holder/timestamp record for the filesystem lease, acquired with CAS-via-rename

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
- Strict recovery fails closed on corruption at byte 0 or invalid corrupted frames.
- Salvage mode keeps the valid prefix and reports degraded recovery.

### Dependency boundary

- WAL replay depends on the base `io::Fs` and `io::File` abstractions, not `storage`.
- Byte-level WAL segment interpretation, including cloud WAL object-key formatting and transaction-batch expansion, lives in `src/wal/cloud_segment.rs`.
- `runtime::hybrid_persistence` owns cloud WAL/SST interpretation, manifest coverage decisions, and guarded-prune orchestration.
- `HybridStorage` exposes only bounded keyed object I/O, byte-identity readback, provider identities, immutable publication, and conditional deletion. It does not import WAL, SST, or manifest formats.
- `runtime::hybrid_persistence::CloudPersistence` orchestrates cloud WAL/SST proof, manifest coverage, and guarded pruning. WAL byte interpretation also lives in `wal/cloud_segment.rs`, and startup recovery verifies cloud authority. `CloudPersistence` wraps an `Arc<HybridStorage>` and derefs to it so raw object I/O remains available. Its tests live next to it in `runtime/hybrid_persistence/tests/`; `src/storage` holds format-neutral tests.
- A guarded prune is authorized in the runtime, then rechecks format-neutral object identities in the storage worker immediately before the provider conditional delete.

For cloud WAL, the storage worker emits `CloudAck` only after an immutable
upload and exact byte readback. The runtime then validates and publishes the
lease-fenced WAL catalog before advancing cloud durability waiters. An uploaded
object without that catalog publication remains an orphan, not an acknowledged
commit. SST publication likewise requires output identity proofs before the
manifest switches from old inputs to the complete replacement set.

The provider configuration exposes explicit S3 credentials, environment credentials,
shared profiles, and an AWS-only default chain. Azure supports shared key, SAS, and
identity credentials; GCS supports HMAC, service-account, authorized-user, and
bearer credentials. The storage provider consumes the selected source; no
credential material belongs in a manifest or SST.

## Memtable

The memtable is the newest readable state.

The ordered skiplist implementation lives in the lower `memtable` module. Public scan contracts are the `ScanIterator` types re-exported from `engine`; `iterators` is only an internal bench shim over the memtable skiplist. SST construction consumes the memtable directly; SST and iterator modules do not depend on each other.

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
In hybrid mode, normal SST readers fetch authoritative remote object ranges;
startup-verified salvage files and internal, non-authoritative repair scratch
use local reads. The runtime shares
reader metadata through its cache and quarantines any complete-bound L1+
overlap from indexed selection until repair publishes a replacement.

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

An underfull L1+ level can still need maintenance when complete key bounds
overlap or three files share an inclusive endpoint. The picker selects the full
connected component for same-level repair before work depending on that level's
ordering. Large components are merged through bounded local scratch runs that
remain outside manifest authority. Ephemeral-cache mode uses its reserved
staging allowance; local-only mode admits at most one eighth of the current
free space on the SST filesystem. This is a finite snapshot admission, while
write failures still retain authoritative inputs. Repair preserves versions
and range tombstones without tombstone GC, then uses the same intent and manifest
publication sequence as ordinary compaction. A failed worker or a publication
failure before an intent becomes ambiguous retains the old authoritative inputs
and keeps reads conservative; the next repair check is deferred by the existing
30-second maintenance interval. If an intent may have reached durable storage,
compaction is fenced until recovery reconciles it. Once the manifest switches
to the replacement set, recovery completes cleanup instead of retrying the
repair.

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
