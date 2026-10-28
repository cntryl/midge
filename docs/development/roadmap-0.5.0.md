# Midge 0.5.0 Roadmap

This roadmap records the 0.5.0 performance release plan. The authoritative
tracker is [GitHub milestone 0.5.0](https://github.com/cntryl/midge/milestone/2)
and its plan issue [#935](https://github.com/cntryl/midge/issues/935). This
page is updated when phase decisions change; the issues carry requirements and
acceptance criteria and are not repeated here.

## Release scope frozen on 2026-10-28

Ship the existing FORMAT 9/SST V5 engine and cloud metadata layout v2 after
the remaining qualification gates. The current owning issues are #914
(metadata-publication acceptance), #929 (absolute baseline summary), #937
(provider hybrid durability measurements), and #935 (final release checklist,
including the four-machine-hour differential-fuzz budget). Source-pinned local
evidence is accepted; keep raw reports outside commits, use Sqrzl `latest`
with its resolved digest recorded, and keep workflows unchanged. Failed,
inconclusive and pending results remain explicit.

The phase history below does not add speculative implementation to this release.
The SST redesign is the 0.5.1 program tracked by #922 and #1010–#1016. That
release requires a crash-safe in-place upgrade from released 0.5.0 databases,
including mixed V5/new SST reads; it supersedes the exploratory design's
no-upgrade assumption. No 0.5.1 capability is claimed in 0.5.0. Destructive TTL
GC #912 and weaker WAL detection floors #904 remain excluded.

The original plan came from a repository-wide architecture review of `develop` at
`3daa1ac` (2026-10-23). The review found correctness contracts ahead of
comparable engines (CAS-fenced writer authority, `OutcomeUnknown`, intent-log
publication, deterministic crash simulation) and a performance architecture
behind them: one event-loop thread performs every write and blocks on fsync,
every fsync also writes `wal/LEDGER`, point reads probe every level because
L0 compaction breaks sequence order, one compaction runs at a time, and cloud
metadata is mirrored JSON with full readback and no generation garbage collection.
That paragraph describes the review baseline, not current completion status.
The read-path work has since merged; the status below is recorded at
`e9a0d86b7be0c1b298ef8f546326c5893aedba04` (2026-10-24).

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

- **Performance evidence is source-pinned and absolute.** Record each named
  row, metric, workload, configuration, environment and source SHA. Repeat
  measurements to characterize noise, preserve all artifacts and mark unstable
  rows inconclusive. Acceptance does not require base/candidate comparisons or
  relative deltas. See [the baseline procedure](baseline-procedure.md).
- **Correctness and assurance use behavioral gates.** Correctness fixes need
  a failing invariant test and a no-regression check on the relevant rows;
  they do not wait for an absolute baseline. Assurance targets specify
  detection power and compute budget. #929 still owes a concise baseline
  summary; raw reports and uptime remain attached to GitHub Actions runs.
  Absolute results supply performance evidence for the recorded source.
- **One purpose per issue, one milestone delivery PR.** Issues that bundled
  several changes were split; each has one failure scenario and one proof.
  The 0.5.0 issue work is tracked as issue-scoped commits on one integration
  branch and will be submitted in one PR after the milestone issues are ready.
- **Formats are unstable on `develop` between releases.** Each
  format-changing PR lands independently, bumps its own rejection test, and
  updates [format-compatibility.md](format-compatibility.md). No coordination
  branch. Release notes list the versions actually superseded by landed
  changes. Planned candidates are SST V4, manifest format 8 and cloud layout
  v1; an upload-concurrency change alone does not require a WAL catalog bump.
  No migration path, per the
  existing policy and the #808 precedent.
- **#873 decides the WAL design.** #904, #905, #906, #916, #940, #941, and
  #947 are hypotheses that #873's transition table confirms, reorders, or
  replaces. PR #973 merged the design and slice order below; #979 tracks its
  remaining preservation debt. The #904 durability decision still requires
  explicit sign-off at implementation; a design merge does not approve that
  behavior-changing slice.
- **Assurance precedes concurrency.** #931 and #951 have merged, in #971 and
  #972. Re-run that coverage on #906's changed path. #972's 90-second fuzz
  smoke is distinct from the four machine-hours per release budget.
- **Hard-prune.** Whatever a PR obsoletes is deleted in that PR.

## Baseline

The Phase 0 baseline runs on an Apple Silicon macOS machine with APFS. Its
evidence records the hardware and load around every run. The checkpoint at
`f64d6e60` lists every absolute row and marks them unmeasured; the qualified
evidence matrix is still outstanding. The six Tier 2 baseline rows now have
three acceptable runs each at `64389f99`; their medians and variability are
recorded, and the other planned rows remain due. See
[the current matrix status](https://github.com/cntryl/midge/issues/929)
and [the earlier matrix discussion](https://github.com/cntryl/midge/issues/929).
Source-pinned absolute measurements govern performance reporting; noise remains
grounds for an inconclusive result. The first matrix is Midge-only. RocksDB
`db_bench` context is deferred and cannot support a parity claim until it has
been measured with matched parameters, per
[performance-targets.md](performance-targets.md).

## Phases

One PR at a time within a phase, in the listed order. Phases 1–3 are
independent and may interleave if one is blocked; Phase 4 follows the design
decisions of Phases 2 and 3; Phase 5 may start any time after Phase 0.

| Phase | Issues in order | Exit |
| --- | --- | --- |
| 0 Measure and make safe | #901, #950, #930, #936 merged; #937 Tier 2 and #929 rows/procedure merged | evidence matrix and #937 Tier 5 row remain; correctness, assurance and absolute measurement gates govern later work |
| 1 Read path | #902 → #903 → #917 → #921 → #919 → #920 → #944 → #945 → #938 | memtable-hit and L0-hit rows touch 0 and 1 SST; readrandom, seekrandom, YCSB C, hybrid YCSB C within target |
| 2 Commit path | #931/#951 merged; #873 proposed order: #974 → #975 → #904 → #946 → #976 → #977 → #924 → #978 → #940 → #941/#947 → #905 → #906 → #979 → #907 → #916 → #939 | source-pinned commit-pipeline measurements first; #939 then records the sharding decision; amortized ledger fsyncs counted separately |
| 3 Background work | #909 → #910 → #908 → #912 → #942 → #911 → #948 → #949 | zero hard stalls in `tier4_system_compaction_backpressure`; record absolute write and space amplification for the defined workloads |
| 4 Format and cloud authority | #926 → #918 → #953 → #913 → #943 → #914 → #915 | rejection tests for actual format changes; #952 decoder coverage with each codec; cold cloud start and metadata requests per commit within target |
| 5 Hardening and polish | #952 → #923 → #927 → #933 → #932 | — |

### Phase 0: measure and make safe

- #901 Map commit `Timeout` to `OutcomeUnknown` severity (bug fix toward the documented contract).
- #950 Tuned release and bench profiles; optional allocator; `hdrhistogram` to dev-dependencies.
- #930 Per-operation latency histograms and tracing spans.
- #936 Hybrid durability contract and a cloud-durable wait API.
- #937 Tier 2 hybrid lag rows merged in #960; Tier 5 Sqrzl row remains open.
- #929 Rows and procedure merged in #961; baseline summary remains open.

### Phase 1: read path

The implementations below merged in #962–#970. Their merge status does not
establish all release performance exits. #945's PR #969 deliberately omitted
the proposed heap; the delivered scan behavior is described in that PR.

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

- #931/#951 Assurance implementations merged in #971/#972; retain the release
  compute budget and extend coverage for the changed pipeline.
- #873 Design merged in PR #973; #979 tracks known preservation/evidence debt.
- #974 One local/cloud durable-floor rule before #904.
- #975 Writer contract; #976 admission parity; #977 commit-outcome
  classification; #979 test/evidence debt. These prerequisite slices can
  move earlier where their own dependencies permit.
- #904 Amortized floor proposal, subject to explicit sign-off on lost-write
  detection and tests at both sides of the floor.
- #946 Conflict validation handoff plus a staged-write overlay.
- #924 Per-family snapshots and per-group apply bookkeeping.
- #978 Applied-sequence publication, parked writer-effect groups, and drains
  before rotation, sealing, lease transition, spilled writes and shutdown.
- #940 Pipelined append/sync completion with receipts for the exact barrier.
- #941/#947 Writer-thread encoding and frame copies. Sequences are inside the
  compressed transaction payload; no post-compression sequence patch or
  unconditional vectored-write requirement. Coordinate their proof boundaries
  in the implementation PRs.
- #905 Overlap after the staged overlay and pipelining; validate write-write
  conflicts, assertions and `insert_only`, not only blind writes.
- #906 Concurrent apply with whole-transaction visibility and fencing.
- #907 then #916 Cloud request/completion and bounded upload concurrency.
  #943 follows their authority decisions in Phase 4.
- #939 Decision-only sharding spike consumes the commit-pipeline measurements
  before the final Phase 2 exit; its own decision is not an input gate.

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
- #915 Server-side checksums and multipart upload for SST publication.

#913 must either justify collecting legacy generations before #914 or rebase
its retention policy onto manifest versions. #943 must protect live snapshot
sources, intents and admitted work as well as committed manifest/catalog
references. WAL entries already retired below the catalog anchor may be
rediscovered without a new intent format; broader sweeps need their own proof.

### Phase 5: hardening and polish

- #952 Deeper decoder fuzzing accompanies each format change; remaining
  coverage and lease `unsafe impl` review finish here.
- #923 Optimize locks while preserving before/after read checks, per-row
  fencing, pin acquisition and lifecycle wakeup guarantees.
- #927 Parallel cold-start SST inventory; retry jitter; idempotent-mutation retries.
- #933 Bounded leaked-transaction wait; drain or storage-fence outstanding
  effects before releasing resources that permit successor takeover.
- #932 Documentation drift.

## Merge hazards

Where two issues touch the same function, land them in this order:

- Durable-floor recovery: #974 before #904; #904 before #940.
- `src/runtime/actors/wal/transaction_state.rs` `latest_key_sequence`: #946, then #905.
- `src/runtime/snapshot_cache.rs` and `publish_snapshot`: #924, then #906.
- Pipeline state: #946/#976/#924 before #978, then #940; #941/#947 follow
  pipelining, #905 follows the overlay and pipeline, then #906.
- `src/runtime/read_snapshot/scan.rs` sources: #920, then #945.
- `src/runtime/actors/compaction.rs`: #909 restructures it first; #949, #911, and #926 rebase onto it.
- `src/compaction/executor/partition.rs`: #912, #942, #948, #949 in that order.
- `src/runtime/event_loop/coordination.rs` and `actors/compaction/publication.rs`: #909, then #910.
- `src/runtime/event_loop/cloud_integration/publication.rs`: #907, then #916.
- Manifest codec: #953 before #914.
- `src/telemetry/metrics.rs` and `RuntimeMetricsSnapshot`: #950, #930, #937.

## Proposed #904 decision, pending sign-off

PR #973 proposes an amortized `SyncOpen` floor, with exact edits at rotation,
clean shutdown and salvage. Intact acknowledged frames past the floor still
replay. Damage above the floor may be classified as a torn tail, discarding
the damaged frame and later valid acknowledged frames; damage below the floor
still fails strict recovery. An N-sync interval also has this detection window
when N is greater than one, so it is not an exact-detection fallback.

The proposed byte/time limits are provisional. Record their accepted values
and the explicit durability decision in #904's PR. Under pipelining, each floor
must use the fsynced group's end offset, not a later `current_pos`. Count WAL
and ledger fsyncs separately: amortization reduces the ledger contribution
rather than promising exactly one total physical fsync for every group.
Keeping the existing encoded edit shape alone needs no format bump; this is
a proposed recovery-behavior change, not a landed guarantee.

## Deferred from 0.5.0

- #928 `object_store` transport evaluation: after the cloud layout settles.
- #922 Table-level filter and partitioned index: gated on a measurement after #918 showing index memory or filter probes binding.
- #925 Score-based picking and dynamic level sizing: gated on the Phase 3 write and space amplification numbers.
- #958 Cloud-durable reads: requires actual historical-version or source-view
  retention and safe enablement after prior compaction; a horizon alone is
  insufficient.
- #934 closed as absorbed by #914.

## Release

- Cargo version `0.5.0`; follow the [release checklist](../operations/release-checklist.md).
- Release notes list every actual format rejection and landed caller-visible
  behavior change, including #904's detection window if approved, #901
  severity mapping, #908 opt-in slowdown, #933 shutdown policy and #936's API.
- The baseline matrix is re-run on the release SHA and committed next to the
  0.4.1 matrix, with the hybrid rows from #937.
