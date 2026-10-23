# Midge 0.5.0 Roadmap

This roadmap records the 0.5.0 performance release plan. The authoritative
tracker is [GitHub milestone 0.5.0](https://github.com/cntryl/midge/milestone/2)
and its plan issue [#935](https://github.com/cntryl/midge/issues/935). This
page is updated at each phase exit; the issues carry requirements and
acceptance criteria and are not repeated here.

The plan comes from a repository-wide architecture review of `develop` at
`3daa1ac` (2026-10-23). The review found correctness contracts ahead of
comparable engines (CAS-fenced writer authority, `OutcomeUnknown`, intent-log
publication, deterministic crash simulation) and a performance architecture
behind them: one event-loop thread performs every write and blocks on fsync,
every fsync also writes `wal/LEDGER`, point reads probe every level because
L0 compaction breaks sequence order, one compaction runs at a time, and cloud
metadata is mirrored JSON with full readback and no garbage collection.

## Goal

1. **Lead on correctness contracts.** Keep the fencing, outcome, and
   publication contracts and raise assurance volume to match: differential
   fuzzing of the public API and loom/miri coverage of the lock-free skiplist.
2. **Local parity.** Within a stated tolerance of RocksDB `db_bench` on the
   baseline machine for fillrandom, readrandom, seekrandom, and YCSB A–F.
3. **Lead on hybrid cloud.** Strict local commit latency with cloud-durable,
   lease-fenced semantics, measured side by side with cloud-durable lag,
   provider requests per commit, and bytes uploaded per committed byte.

## Rules

- **No implementation PR without a number.** Phase 0 publishes the baseline
  (#929). Before any Phase 1–3 issue starts it receives a numeric target
  relative to that baseline, written into the issue; the PR records
  before/after on the named `cntryl-stress` row.
- **One purpose per issue, one PR per issue.** Issues that bundled several
  changes were split; each has one failure scenario and one proof.
- **Formats are unstable on `develop` between releases.** Each
  format-changing PR lands independently, bumps its own rejection test, and
  updates [format-compatibility.md](format-compatibility.md). No coordination
  branch. The release notes list the cumulative rejections: SST V4, manifest
  format 8, cloud layout v1, WAL catalog format 3. No migration path, per the
  existing policy and the #808 precedent.
- **#873 decides the WAL design.** #904, #905, #906, #916, #940, #941, and
  #947 are hypotheses that #873's transition table confirms, reorders, or
  replaces. #873 is the only Phase 2 gate.
- **Assurance precedes concurrency.** #931 and #951 land before #906.
- **Hard-prune.** Whatever a PR obsoletes is deleted in that PR.

## Baseline

The Phase 0 baseline runs on an Apple Silicon macOS machine with APFS. Its
numbers are for relative deltas within this release; the evidence file
records the hardware. A Linux/NVMe run can be added later without changing
the procedure. RocksDB `db_bench` results with matched parameters on the same
machine are recorded as context only, per
[performance-targets.md](performance-targets.md).

## Phases

One PR at a time within a phase, in the listed order. Phases 1–3 are
independent and may interleave if one is blocked; Phase 4 follows the design
decisions of Phases 2 and 3; Phase 5 may start any time after Phase 0.

| Phase | Issues in order | Exit |
| --- | --- | --- |
| 0 Measure and make safe | this document → #901 → #950 → #930 → #936 → #937 → #929 | evidence file committed; every Phase 1–3 issue can receive a target |
| 1 Read path | #902 → #903 → #917 → #921 → #919 → #920 → #944 → #945 → #938 | memtable-hit and L0-hit rows touch 0 and 1 SST; readrandom, seekrandom, YCSB C, hybrid YCSB C within target |
| 2 Commit path | #931 → #951 → #873 → #904 → #946 → #905 → #924 → #940 → #941 → #947 → #906 → #907 → #939 | one fsync per strict group; strict throughput scales to the fsync ceiling; read p99 decoupled from fsync and provider latency; sharding decision recorded |
| 3 Background work | #909 → #910 → #908 → #912 → #942 → #911 → #948 → #949 | zero hard stalls in `tier4_system_compaction_backpressure`; write and space amplification within target |
| 4 Format changes | #926 → #918 → #953 → #913 → #943 → #914 → #916 → #915 | rejection tests for every superseded format; fuzz targets for every new decoder; cold cloud start and metadata requests per commit within target |
| 5 Hardening and polish | #952 → #923 → #927 → #933 → #932 | — |

### Phase 0: measure and make safe

- #901 Map commit `Timeout` to `OutcomeUnknown` severity (bug fix toward the documented contract).
- #950 Tuned release and bench profiles; optional allocator; `hdrhistogram` to dev-dependencies.
- #930 Per-operation latency histograms and tracing spans.
- #936 Hybrid durability contract and a cloud-durable wait API.
- #937 Benchmark strict local commit latency against cloud-durable lag.
- #929 Published baseline matrix with `db_bench` context.

### Phase 1: read path

- #902 Compact L0 in sequence order (prerequisite for #903).
- #903 Return at the first hit in `ReadSnapshot::get_bytes`.
- #917 Key the block cache by file number, not a hash of file metadata.
- #921 Index range tombstones instead of scanning them per point read.
- #919 Keep SST file handles open; shard the table cache.
- #920 Lazy memtable scans.
- #944 Scans do not insert into the block cache.
- #945 Heap-based scan source merge and sequential readahead.
- #938 Bounded local disk cache for remote SST blocks.

### Phase 2: commit path

- #931 loom and miri coverage for the lock-free skiplist.
- #951 Differential API fuzzer against a `BTreeMap` model.
- #873 WAL pipeline design (existing issue; gates the rest of this phase).
- #904 Stop committing a LEDGER edit on every WAL fsync.
- #946 Conflict checks must not read SSTs on the event loop.
- #905 Do not break strict group commit on key overlap.
- #924 Publish read snapshots per column family.
- #940 Complete WAL fsync as an event instead of blocking the event loop.
- #941 Encode and compress WAL batches off the event loop.
- #947 Build WAL batches once and write them vectored.
- #906 Concurrent memtable apply with a visibility watermark.
- #907 Cloud metadata mirror, seal, and WAL-ack publication off the event loop.
- #939 Design spike: shard the event loop by column family (decision only, gated on Phase 2 numbers).

### Phase 3: background work

- #909 Concurrent compactions with immediate rescheduling.
- #910 Flush publication independent of the compaction publication gate.
- #908 Write slowdown stage and pending-compaction limits (opt-in by default).
- #912 Reclaim expired TTL values during compaction.
- #942 Drop range tombstones with the bottommost-overlap proof.
- #911 Manifest snapshots on a size or edit-count policy.
- #948 Cut compaction outputs at grandparent overlap limits.
- #949 Compute output identity while writing; replace-top merge heap.

### Phase 4: format changes

- #926 Record level in `FileMeta`; trivial moves.
- #918 SST V5: restart points and compact entry header.
- #953 Manifest format 9: binary encoding and append-only intent entries.
- #913 Collect old cloud metadata generations.
- #943 Durable delete intents and a remote orphan sweeper.
- #914 Cloud layout v2: versioned manifest committed by put-if-absent (absorbs #934).
- #916 Parallel cloud WAL uploads; WAL catalog format 4.
- #915 Server-side checksums and multipart upload for SST publication.

### Phase 5: hardening and polish

- #952 Deeper SST decoder fuzz targets; lease `unsafe impl` review.
- #923 Remove per-transaction global locks and the per-row lease mutex.
- #927 Parallel cold-start SST inventory; retry jitter; idempotent-mutation retries.
- #933 Bounded shutdown reaper.
- #932 Documentation drift.

## Merge hazards

Where two issues touch the same function, land them in this order:

- `src/runtime/actors/wal/durability.rs` `begin_sync_io`: #904, then #940.
- `src/runtime/actors/wal/transaction_state.rs` `latest_key_sequence`: #946, then #905.
- `src/runtime/snapshot_cache.rs` and `publish_snapshot`: #924, then #906.
- `src/wal/encoding.rs` and `src/wal/fs/writer_io.rs` frame build: #941, then #947.
- `src/runtime/read_snapshot/scan.rs` sources: #920, then #945.
- `src/runtime/actors/compaction.rs`: #909 restructures it first; #949, #911, and #926 rebase onto it.
- `src/compaction/executor/partition.rs`: #912, #942, #948, #949 in that order.
- `src/runtime/event_loop/coordination.rs` and `actors/compaction/publication.rs`: #909, then #910.
- `src/runtime/event_loop/cloud_integration/publication.rs`: #907, then #916.
- Manifest codec: #953 before #914.
- `src/telemetry/metrics.rs` and `RuntimeMetricsSnapshot`: #950, #930, #937.

## Decision recorded for #873

#904 turns the ledger's `durable_bytes` into a low-frequency floor written at
rotate, seal, shutdown, and salvage cut. Today `valid_bytes < durable_bytes`
proves acknowledged bytes vanished and fails strict replay; with a floor, loss
between the floor and the WAL tail is tolerated as a torn tail. This is an
outcome-boundary move and is recorded in #873 before #904 opens a PR. If #873
rejects it, #904 becomes "amortize the ledger edit to one per N syncs" with
the same regression test.

## Deferred from 0.5.0

- #928 `object_store` transport evaluation: after the cloud layout settles.
- #922 Table-level filter and partitioned index: gated on a measurement after #918 showing index memory or filter probes binding.
- #925 Score-based picking and dynamic level sizing: gated on the Phase 3 write and space amplification numbers.
- #934 closed as absorbed by #914.

## Release

- Cargo version `0.5.0`; follow the [release checklist](../operations/release-checklist.md).
- Release notes list every rejected prior format and the caller-visible behavior changes: #901 severity mapping, #908 opt-in slowdown, #933 bounded shutdown wait, #936 new cloud-durable API.
- The baseline matrix is re-run on the release SHA and committed next to the
  0.4.1 matrix, with the hybrid rows from #937.
