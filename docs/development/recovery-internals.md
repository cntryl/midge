# Recovery Process

This document describes how Midge restores a trustworthy state after restart and how it handles interrupted publication work. It is written for contributors and early adopters performing incident review.

## Recovery Goals

Recovery must satisfy three requirements:

1. restore every write that crossed the relevant durability boundary
2. never apply a partial WAL frame or partial transaction
3. treat manifest-visible SST state as authoritative unless publication replay proves a newer durable state

## Startup Sequence

At `Engine::open()`, local recovery proceeds in this order:

1. acquire the single-writer lease
2. load manifest state and published SST metadata
3. determine the last durable publish sequence
4. replay WAL files into memtables
5. replay intent-log entries for interrupted flush or compaction publication
6. finalize health state and expose recovery metrics

This order matters:

- manifest state defines the currently published SST set
- WAL replay restores newer committed state that has not been SST-published
- intent replay resolves interrupted publication transitions without guessing

## WAL Replay Rules

Midge replays WAL segments in order and then replays the active WAL file.

### Valid records

- applied in sequence order
- written into recovered memtables by column family
- incomplete transactions are dropped unless a matching commit marker exists

### Truncated tail

If the active WAL ends with a typed incomplete-tail condition beyond byte 0:

- the valid prefix is kept
- the truncated tail is discarded
- recovery continues

This includes a partial final header or payload with no verified frame after it,
and an all-zero final region left by file preallocation. These are the expected
shapes of a torn or unwritten final append. Recovery does not infer this state
from error-message text.

If a declared payload length overruns EOF but the bytes it would otherwise hide
contain a CRC-valid, decodable WAL frame, the length field is corrupt rather
than a final torn append. Strict recovery fails; salvage recovery retains only
the verified prefix and reports degraded health.

### Partial WAL entry

If a frame header or record body is incomplete:

- the partial record is never applied
- strict mode accepts only the salvageable trailing-tail case
- salvage mode can keep the valid prefix and continue in degraded state

### Corrupted first frame or non-tail corruption

If corruption is detected at byte 0 or inside the non-tail durable prefix:

- strict recovery fails open
- salvage mode may keep the valid prefix and mark the engine degraded

## Strict vs Salvage Recovery

### Strict

Use strict recovery when the engine should refuse to open unless it can establish a trustworthy state.

Strict mode:

- rejects non-salvageable WAL corruption
- rejects unrecoverable manifest or intent-log publication ambiguity
- surfaces a recovery failure instead of silently continuing

### Salvage

Use salvage recovery when the operator would rather keep the valid prefix than fail open.

Salvage mode:

- keeps the valid WAL prefix when possible
- preserves authoritative pre-publication SST state if interrupted output cannot be safely published
- marks the engine degraded or in salvage mode for diagnostics

## Flush Recovery

Flush is a staged publish workflow:

1. freeze memtable
2. write SST output
3. record output durability in intent state
4. publish the new SST to manifest state
5. clear the intent

### Interruption cases

#### Crash before SST finalize

- no new SST becomes authoritative
- WAL-backed state remains authoritative
- restart recovers the data from the WAL durable prefix

#### Crash after SST durable but before manifest publish

- the new SST may exist on disk
- it is not authoritative yet
- recovery uses the flush intent to either publish it safely or remove it
- reads must continue to reflect the old authoritative state until publication completes

## Compaction Recovery

Compaction follows the same publication pattern with a different state transition:

1. read manifest-visible input SSTs
2. write replacement SST outputs
3. record compaction output durability in the intent log
4. publish the manifest batch that removes inputs and adds outputs
5. delete obsolete input SSTs

### Interruption cases

#### Crash after output durable but before manifest publish

- input SSTs remain authoritative
- replacement SSTs must not be used for reads yet
- an `OutputDurable` intent is rolled back and its non-authoritative output is removed
- a `ManifestPublished` intent may complete the replacement batch
- if the manifest journal already contains the whole batch, that durable authority wins even if the intent phase update was interrupted

#### Crash after manifest publish but before input cleanup

- replacement SSTs are already authoritative
- old input SSTs are obsolete but may still exist on disk
- recovery or later GC deletes the obsolete files idempotently

## Incomplete SST Handling

Midge treats manifest visibility, not raw file presence, as the authority boundary.

- orphan SST files are not automatically trusted
- flush or compaction outputs require matching publish state before they become authoritative
- interrupted output files may be deleted during recovery if publication never completed

## Incident Review Checklist

When investigating a crash or restart, answer these questions in order:

1. Which write mode acknowledged the missing or present write?
2. What was the last published manifest sequence?
3. Did WAL replay recover the write sequence?
4. Was there an outstanding flush or compaction publication intent?
5. Did recovery open in strict or salvage mode?

The recovery counters exposed by `Engine::get_recovery_metrics()` should confirm which replay paths ran.

## Related References

- [architecture.md](architecture.md)
- [storage-invariants.md](storage-invariants.md)
- [testing.md](testing.md)
