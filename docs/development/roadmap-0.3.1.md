# Midge 0.3.1 Bughunt and Roadmap

Historical status record: superseded by the [final release qualification](#final-release-qualification-2026-10-02).

This roadmap records a repository-wide correctness and performance review of Midge 0.3.0 at `c484ea3d`. The review used Jev to prioritize bounded questions and challenge conclusions. Jev results are triage signals; every committed item below is supported by source analysis plus a focused failing probe or measured scaling evidence.

## Release decision

Ship all eight confirmed fixes #662–#669, every additional confirmed P1/P2 defect found during expanded discovery, and #670's checkpoint optimization if its measurement gate qualifies. The [0.3.1 GitHub milestone](https://github.com/cntryl/midge/milestone/1) initially contained three P1 defects, five P2 items, and one P3 qualification item. All eight fixes are required release scope.

Completion requires a qualified `main` revision, immutable `v0.3.1` tag, published crates.io artifact, successful registry consumer test, GitHub release, and closed milestone. Preserve public Rust signatures, error variants, CLI JSON schema, and persisted formats. Document the approved transaction tradeoff: reserving read-index scratch space upfront reduces maximum spill capacity near the disk limit.

Land this roadmap and the Jev guidance in `AGENTS.md` through a documentation PR targeting `develop` before adding discovery scaffolding. The full logical discovery campaign passed on `0e0a6f7c` (1,024 fixture histories, 117,380 actions). Subsequent user direction prioritizes repairing confirmed bugs immediately; the backup/restore batch addresses #664/#665 while remaining physical discovery continues. The complete coverage map and consolidated findings remain release qualification requirements. Evidence below describes the original review unless explicitly updated.

| Order | Work item | Priority | Labels | Release role |
| ---: | --- | --- | --- | --- |
| 1 | [#662 Reject writes synchronously when lease validity expires](https://github.com/cntryl/midge/issues/662) | P1 | `area:lease`, `flea` | Release blocker |
| 2 | [#663 Treat epoch-regressed cloud WAL as a salvage replay hole](https://github.com/cntryl/midge/issues/663) | P1 | `area:wal`, `flea` | Release blocker |
| 3 | [#664 Make restore retries recover from crash-left staging directories](https://github.com/cntryl/midge/issues/664) | P1 | `area:engine`, `flea` | Release blocker |
| 4 | [#665 Reject overlapping backup, restore, database, and artifact paths](https://github.com/cntryl/midge/issues/665) | P2 | `area:engine`, `flea` | Planned |
| 5 | [#666 Preserve CloudAck memory-contention identity across concurrent catalog work](https://github.com/cntryl/midge/issues/666) | P2 | `area:runtime`, `flea` | Planned |
| 6 | [#667 Eliminate per-result whole-write-set probes in transaction reads](https://github.com/cntryl/midge/issues/667) | P2 | `area:runtime`, `flea` | Planned |
| 7 | [#668 Replace `SnapshotScan`'s per-key full range-tombstone scan](https://github.com/cntryl/midge/issues/668) | P2 | `area:runtime`, `flea` | Planned |
| 8 | [#669 Retain bounded SST block locality during cloud WAL replay coverage](https://github.com/cntryl/midge/issues/669) | P2 | `area:wal`, `solid` | Planned |
| 9 | [#670 Benchmark and bound full-manifest checkpoint cadence after local flushes](https://github.com/cntryl/midge/issues/670) | P3 | `area:metadata`, `solid` | Measurement-gated stretch |

## Architecture map

| Boundary | Main implementation | Contracts reviewed |
| --- | --- | --- |
| Public engine API | `src/engine/` | open/close, transactions, snapshots, flush, ingest, backup/restore, configuration |
| Runtime coordination | `src/runtime/event_loop/`, actors, scheduler | serialized state transitions, write admission, durability waiters, cancellation, DDL publication |
| Writer authority | `src/lease/`, runtime fencing | exclusive ownership, monotonic expiry, renewal, epoch validation |
| WAL and recovery | `src/wal/`, `src/runtime/cloud_startup/` | framing, replay prefix, salvage, sequence identity, cloud publication and pruning |
| Metadata | `src/metadata/`, runtime manifest state | edit journaling, checksums, checkpoints, tail repair, atomic publication |
| SST and reads | `src/sst/`, `src/runtime/read_snapshot.rs` | bounds, checksums, MVCC visibility, range tombstones, forward/reverse iteration |
| Mutable state and compaction | memtable, flush, compaction actors | publication atomicity, rollback, overlap, tombstone fragmentation, authority loss |
| Storage | `src/storage/`, `src/io/` | filesystem durability, object identity, pagination, timeouts, conditional mutation, partial I/O |
| Resource control | budgets, transaction spill, cloud maintenance | bounded memory, backpressure, ownership, retry classification |
| Operations and validation | configuration, diagnostics, metrics, `midge verify`, tests, benches, fuzz | invalid-input rejection, observability, deterministic qualification |

## Coverage record

| Area | Highest-value failure modes tested or traced | Result |
| --- | --- | --- |
| Architecture and ownership | dependency direction, high fan-in state, public boundaries | Reviewed; no separate architecture finding |
| Lease and runtime fencing | delayed watchdog, successor takeover, drained writes | P1 finding |
| Cloud WAL recovery | replay holes, epoch order, sequence floor, active WAL quarantine | P1 finding |
| Backup and restore | consistent cut, crash retry, path overlap, artifact reuse | P1 and P2 findings |
| Transactions and spill | read-own-writes, resident/spilled lookup scaling, ordinal semantics | P2 finding |
| Read snapshots | MVCC, range-tombstone coverage, lazy SST opening, reverse scan | P2 finding |
| Cloud durability admission | memory backpressure, waiter retention, catalog concurrency | P2 finding |
| Cloud replay coverage | exact-value proof, reader/cache budget, remote range I/O | P2 finding |
| Manifest and journal | edit authority, CRC/tail recovery, checkpoint publication and cadence | Stretch finding; recovery contracts held |
| Flush and compaction | crash publication, stale authority, WAL coverage, range fragmentation | Focused probes passed |
| SST, WAL codec, and I/O | malformed frames, footer/index bounds, partial I/O, object identity | No additional release finding |
| Storage providers | pagination, callback bounds, timeout and conditional mutation | No additional release finding |
| DDL coordination | prepare/reconcile, delayed CAS, publication gating | Focused tests passed |
| Configuration and tooling | validation, deadlines, `midge verify`, metrics and diagnostics | No additional release finding |

## P1 release blockers

### 1. [#662 Reject writes synchronously when lease validity expires](https://github.com/cntryl/midge/issues/662)

- **Location:** `src/runtime/event_loop/fencing.rs`, `src/runtime/event_loop/wal.rs`, and `src/lease/heartbeat.rs`.
- **Observed behavior:** `RuntimeFence::check_health` trusts an asynchronously updated `AtomicBool`. After the monotonic lease deadline expires and a successor acquires the same lease, the old runtime can still admit a write before its watchdog updates the bit. BestEffort has no later authority check; CloudAsync publishes and acknowledges before its validating seal.
- **Expected behavior:** write admission rejects an expired lease synchronously, regardless of watchdog scheduling.
- **Evidence:** a lease probe acquired a successor and then observed the old fence return `Ok(())`. An Engine probe stopped expiry observation, verified an injected 200 ms validity had expired, and committed a real BestEffort transaction; expected `Fenced`, observed `Ok(())`.
- **Root cause:** the runtime owns a cached health signal but does not consult the `LeaseValidity` object carrying the authoritative monotonic deadline.
- **Impact and scope:** an old process can acknowledge or expose a write after another process owns the writer lease. The demonstrated paths are BestEffort and CloudAsync before seal.
- **Pattern:** safety admission based on asynchronously refreshed authority after the source can expire independently.
- **Confidence:** high.
- **Acceptance:** add deterministic successor-takeover fence and Engine regressions. BestEffort and CloudAsync must reject after validity expiry even when the watchdog has not run. Preserve transient provider-error classification and existing epoch validation.
- **Related work:** closed #481 fixed drained and coalesced writes after the cached bit was already false; it does not cover an expired source with a stale `true` bit.

### 2. [#663 Treat epoch-regressed cloud WAL as a salvage replay hole](https://github.com/cntryl/midge/issues/663)

- **Location:** `src/runtime/cloud_startup/streaming_wal_plan.rs`, especially `build`, `enforce_epoch_order`, and `stop_at_first_hole`.
- **Observed behavior:** salvage removes an epoch-regressed sealed segment after the `skipped` set is built, so a valid later segment remains replayable. It immediately quarantines a stale active WAL without lifting `max_unreplayed_sequence` or staging the rename behind floor persistence.
- **Expected behavior:** an epoch-regressed source is a replay hole. Recovery stops before it, sets later sources aside, raises the sequence floor over every unreplayed verified record, persists the floor, and only then quarantines local files.
- **Evidence:** for catalog entries `(id, sequence, epoch) = (1,1,8), (2,2,7), (3,3,9)`, a focused probe expected replay IDs `[1]` and observed `[1,3]`. With sealed `(1,1,8)` and active `(sequence 2, epoch 7)`, a second probe expected floor `2` and observed `0`.
- **Root cause:** epoch validation mutates the source plan outside the shared hole and floor planner.
- **Impact and scope:** salvage can construct a non-prefix state, including retaining a later write after losing an intermediate delete, and can reuse a sequence present in a quarantined active WAL.
- **Pattern:** recovery input removed or quarantined without recording its replay hole and durable identity floor.
- **Confidence:** high.
- **Acceptance:** cover sealed epoch regression followed by valid history and a stale active WAL. Add a crash boundary between floor persistence and rename. Strict recovery must continue to fail rather than salvage.
- **Related work:** narrow follow-up to closed #486 and #543; their verified-prefix and mixed-epoch coverage did not route this epoch filter through the hole planner.

### 3. [#664 Make restore retries recover from crash-left staging directories](https://github.com/cntryl/midge/issues/664)

- **Location:** `src/engine/backup.rs` around restore stage creation, cleanup, and publication.
- **Observed behavior:** restore always uses `.midge-restore-{backup_id}.tmp` and rejects an existing stage before inspecting or cleaning it. Ordinary error paths clean their own stage, but process or host interruption leaves residue that blocks every later retry.
- **Expected behavior:** when the target is absent, retrying a valid artifact after interruption safely removes or replaces stale staging state and completes restore.
- **Evidence:** a public API probe created the deterministic stale stage with no target present. `restore_backup` returned `InvalidArgument("restore staging path ... already exists")`; the valid artifact could not be restored until manual hidden-directory deletion.
- **Root cause:** the deterministic stage is treated as caller-owned conflicting input even though it is engine-owned transactional residue.
- **Impact and scope:** the supported disaster-recovery operation becomes nonretryable after the process failure it is expected to tolerate.
- **Pattern:** transaction residue has no restart reconciliation path.
- **Confidence:** high.
- **Acceptance:** interrupt a child process after staging begins and before publish, confirm the target is absent, then retry the same artifact successfully. Preserve refusal to replace a pre-existing target and prevent symlink/path escape.
- **Related work:** closed #539 introduced the backup/restore contract; #593 was Windows qualification rather than crash retry.

## P2 planned work

### 4. [#665 Reject overlapping backup, restore, database, and artifact paths](https://github.com/cntryl/midge/issues/665)

- **Location:** `src/engine/backup.rs` path validation and artifact inventory.
- **Observed behavior:** `backup_to` accepts a destination below the live database, and `restore_backup` accepts a target below the artifact. The first creates an SST-shaped directory that degrades source health; the second mutates the artifact so its exact inventory fails on reuse.
- **Expected behavior:** canonical, symlink-safe validation rejects ancestor/descendant overlap in either direction while allowing disjoint siblings.
- **Evidence:** backup to `source/sst/accidental.sst` returned `Ok`, after which source health was `Degraded`. Restore to `artifact/objects/restored-db` returned `Ok` and opened, but reusing the artifact returned corruption because object inventory no longer matched.
- **Root cause:** validation checks destination/target existence but not disjointness among database, artifact, stage, and target roots.
- **Impact and scope:** a successful public API call can degrade a live database or silently consume the reusability of a verified backup artifact.
- **Pattern:** destructive path relationship accepted because validation considers each path independently.
- **Confidence:** high.
- **Acceptance:** cover direct, canonicalized, and symlinked ancestor/descendant relationships for both APIs; verify external siblings remain valid and failed validation leaves both roots unchanged.
- **Related work:** one shared path-isolation issue, separate from restore crash residue; closed #539 is the capability predecessor.

### 5. [#666 Preserve CloudAck memory-contention identity across concurrent catalog work](https://github.com/cntryl/midge/issues/666)

- **Location:** `src/common/resource_budget.rs`, `src/runtime/hybrid_persistence/catalog.rs`, and `src/runtime/event_loop/cloud_integration/ack.rs`.
- **Observed behavior:** all clones share one optional contention slot. Every contention-enabled reservation clears it before attempting admission. A sibling successful catalog reservation can therefore erase a CloudAck failure before `defer_cloud_ack_for_memory` consumes its cause.
- **Expected behavior:** retry classification is tied to the reservation attempt that returned the error and cannot be overwritten by unrelated concurrent work.
- **Evidence:** an event-level probe filled the real maintenance pool, produced a retryable catalog admission failure, made a sibling catalog reservation, and followed the actual acknowledgment branch. It observed `deferred=false`, `anomaly=true`, no pending waiter, and the segment requeued. The existing non-racing waiter-retention test passed.
- **Root cause:** out-of-band, pool-global error metadata has no attempt identity or atomic association with its `ResourceLimit` error.
- **Impact and scope:** under catalog memory pressure plus concurrent prune/publication work, an accepted CloudDurability write can return `Internal`, mark degraded health, and later become durable through retry, yielding an ambiguous commit result.
- **Pattern:** concurrent operations communicate error provenance through a mutable shared side channel.
- **Confidence:** high for mechanism and outcome; trigger frequency is unmeasured.
- **Acceptance:** deterministically interleave two real catalog reservations and retain the original waiter without an anomaly. Carry contention provenance in the error/reservation result or otherwise make association attempt-local. Cover nested parent budgets.
- **Related work:** PR #290 introduced the retry metadata. No matching open or closed issue was found.

### 6. [#667 Eliminate per-result whole-write-set probes in transaction reads](https://github.com/cntryl/midge/issues/667)

- **Location:** `src/engine/api/transaction.rs` and `src/runtime/transaction_spill/`.
- **Observed behavior:** `get` and each scan result call `latest_for_key`, which walks all resident operations and every spill run. Spilled scan merging also compares run sources per key.
- **Expected behavior:** read-own-writes lookup uses a bounded latest-intent index/view while preserving operation ordinal, point/range tombstones, and resident/spilled equivalence.
- **Evidence:** release probes for a resident full scan grew from 1.1 ms at 500 writes to 2.56 s at 32,000. With an 8 KiB transaction pool, 2,000 operations across about 210 spill files took 3.46-4.96 s. The implementation establishes the residual `O(K × (W + R))` search pattern; timings show user-visible scale.
- **Root cause:** append-oriented write storage is reused as the lookup structure, and previous spill caching did not create a transaction-wide latest-key index.
- **Impact and scope:** large read-own-writes transactions can stall for seconds while retaining transaction memory, snapshot pins, and lifecycle guards.
- **Pattern:** repeated whole-history search for each output item.
- **Confidence:** high.
- **Acceptance:** add deterministic lookup-work counters and release scaling coverage for resident and heavily spilled transactions. Preserve last-write-wins, range-delete ordering, assertions, and bounded memory; doubling operations must no longer approach fourfold lookup work.
- **Related work:** residual follow-up to closed #394, which cached spill readers/indexes but left resident/run linear search.

### 7. [#668 Replace `SnapshotScan`'s per-key full range-tombstone scan](https://github.com/cntryl/midge/issues/668)

- **Location:** `src/runtime/read_snapshot.rs` tombstone discovery and `range_tombstone_covers_state`.
- **Observed behavior:** a scan accumulates every query-overlapping tombstone in a growing `Vec`. Each candidate key tests `.iter().any(...)` over the entire vector, and expired ranges are not retired.
- **Expected behavior:** a sequence-aware, direction-aware active interval structure advances with the scan and retains work proportional to active overlap.
- **Evidence:** the implementation performs `K × T` checks for `K` candidate keys and `T` noncovering tombstones. Release medians for 10,000 in-memory keys grew from 0.655 ms with no tombstones to 246.331 ms with 8,000; persisted 10,000-key scans with 4,000 tombstones took 126.948 ms.
- **Root cause:** lazy tombstone discovery was added without replacing per-key linear coverage evaluation.
- **Impact and scope:** range-heavy workloads pay quadratic scan CPU in both memory and SST paths, including queries whose tombstones cover no returned key.
- **Pattern:** monotonically accumulated interval history rescanned for every ordered item.
- **Confidence:** high.
- **Acceptance:** use a deterministic work bound, such as a justified `O((K+T) log T)` or tighter counter, as the primary gate. Compare against an oracle for nested, overlapping, equal-sequence, snapshot, bounded, unbounded, forward, reverse, and limited scans. Keep wall-clock benchmarks informational.
- **Related work:** distinct from closed #390's eager SST opening; closed #385 contains an analogous compaction interval tracker.

### 8. [#669 Retain bounded SST block locality during cloud WAL replay coverage](https://github.com/cntryl/midge/issues/669)

- **Location:** `src/runtime/cloud_startup/replay_coverage.rs` and `src/engine/startup/streaming_recovery.rs`.
- **Observed behavior:** exact coverage is evaluated for every WAL value record. The proof scans manifest candidates, retains at most one SST reader, and each reader retains one decoded block. Alternating candidates or blocks repeatedly issue range reads; recovery checkpoints release the reader.
- **Expected behavior:** retain bounded reader/block locality across adjacent probes while preserving exact coverage, immutable-object verification, ordering, TTL, and the global recovery memory budget.
- **Evidence:** the checked-in cold-recovery campaign processed a 134,219,148-byte WAL with 16,212 records. Coverage used 16.107 s of 18.216 s, issued 33,536 HTTP ranges, and received 2.553 GB while verifying about 134 MB of SST data. Focused tests also demonstrate repeated opens with overlapping SSTs.
- **Root cause:** proof caching is bounded at reader identity, but block locality is discarded too aggressively for the replay access pattern.
- **Impact and scope:** cloud cold start can be dominated by repeated remote SST range traffic even after prior value-copy and reader-count improvements.
- **Pattern:** bounded cache policy mismatched to an alternating sequential workload.
- **Confidence:** high for repeated I/O and measured campaign; provider latency benefit must be remeasured after a change.
- **Acceptance:** add deterministic reader/block-hit counters and rerun the same Sqrzl release campaign. Materially reduce range count, response bytes, and coverage time without increasing peak recovery memory beyond the configured budget. Instrument manifest-candidate work separately rather than assuming it is the measured bottleneck.
- **Related work:** closed #644 fixed replay value memory and copying; this is the remaining block-locality cost.

## Measurement-gated stretch

### 9. [#670 Benchmark and bound full-manifest checkpoint cadence after local flushes](https://github.com/cntryl/midge/issues/670)

- **Location:** `src/runtime/state/manifest.rs`, `src/metadata/persistence.rs`, and `src/io/staging.rs`.
- **Observed behavior:** every local flush appends and fsyncs its authoritative journal edit, then unconditionally clones and serializes the full manifest, stages and publishes it durably, and truncates/fsyncs the journal. `require_snapshot = false` only makes checkpoint failure nonfatal.
- **Expected behavior:** normal flushes pay delta-journal cost, while a bounded policy triggers full checkpoints often enough to cap recovery and journal growth. Explicit shutdown or administrative checkpoints retain their stronger semantics.
- **Evidence:** a counting-filesystem probe over 512 real `commit_flush_publication` calls produced a 131,902-byte final snapshot but 33,797,635 cumulative snapshot bytes, 256.23 times the final size. This confirms cumulative quadratic byte growth when the live manifest grows with flush count.
- **Root cause:** checkpoint frequency is coupled to every successful edit rather than a recovery-cost or journal-size policy.
- **Impact and scope:** large live manifests can turn small local flushes into repeated full metadata writes and fsyncs. Real workload severity remains unqualified because compaction limits live-file cardinality.
- **Pattern:** delta log followed by unconditional full-state materialization on every mutation.
- **Confidence:** high in the amplification mechanism; medium in production priority.
- **Promotion gate:** first add a deterministic counting-FS scaling test plus a representative release benchmark across flush rate and steady-state SST cardinality. Promote to P2 implementation only with an explicit latency/write-amplification target. Any new policy needs crash recovery, forced-checkpoint, and bounded-journal tests.
- **Related work:** residual policy question after closed #494/PR #547 removed repeated parsing and a duplicate full write.

## Deferred cleanup and rejected hypotheses

- **Filesystem delete parent sync:** Unix filesystem-backed object deletion acknowledges after `remove_file` without syncing the parent directory. A crash can resurrect obsolete bytes, but WAL catalog and SST manifest authority keep them inactive, and the repository permits storage leaks when safer than deletion. This is residual cleanup from closed #369, classified P3 and excluded from 0.3.1 release scope unless bounded-storage guarantees become a requirement.
- **Rejected after focused evidence:** snapshot publication ordering, durability-frontier regression, DDL prepare/reconcile races, cloud WAL prune authority, manifest-journal tail recovery, provider pagination/timeout handling, simulated-cloud root durability, malformed SST producer paths, backup frontier semantics, Engine sequence regression, shutdown/snapshot-pin handling, and configuration/CLI validation bypasses.
- **Compaction probes passed:** crash publication matrix, lease-loss garbage collection, WAL coverage, range-tombstone fragmentation, and the bounded 10,000-target case.

## Delivery sequence

Work one repair branch and PR at a time, grouping up to ten confirmed related issues per PR as requested on 2026-10-01. Demonstrate each issue's red regression, then complete the batch's implementation, exact-head review, merge, and merged-revision verification before starting the next batch.

1. **Authority:** synchronous lease-validity admission.
2. **Recovery:** epoch-regressed salvage hole/floor planning.
3. **Disaster recovery:** restore crash residue, then backup/restore path isolation as a separate issue.
4. **Durability result classification:** attempt-local CloudAck memory contention.
5. **Read-path scaling:** transaction latest-intent lookup, then snapshot tombstone sweep.
6. **Startup scaling:** cloud replay coverage locality.
7. **Stretch qualification:** manifest checkpoint benchmark; promote only if its gate is met.

Insert newly discovered P1 defects ahead of P2 work. Complete consequential correctness defects before performance work. Do not start a second repair branch or PR before merging and verifying the first.

## Release verification

For each correctness item:

1. add the smallest regression described above and demonstrate the failure;
2. implement the repair without weakening strict recovery, authority, or durability semantics;
3. run the focused regression and adjacent subsystem suite;
4. include deterministic crash/failpoint coverage where ordering matters;
5. record the exact commands and results in the PR.

For each performance item, retain a deterministic work or I/O counter as the regression gate and use release-mode wall-clock measurements as supporting evidence. Preserve configured memory bounds.

Before tagging 0.3.1, run:

```sh
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings -D clippy::pedantic
cargo test
```

Also rerun the Sqrzl cold-recovery campaign if item 8 changes and the representative flush/cardinality benchmark if item 9 is promoted.

## Jev decision log

Broad prompts were split whenever probabilities were weak. After adding exact contracts and probe outcomes, Jev selected lease expiry as the first blocker; classified restore crash residue as P1 with probability 0.86; classified the contention race as P2 with probability 0.83; placed the manifest item in the measurement-gated stretch tier with probability 1.00; and supported deferring delete parent-sync cleanup with probability 0.83. These values set investigation and roadmap priority; they are not correctness evidence.

## Expanded discovery gate

Run three bounded templates in each of seven families. Maintain the architecture map above, and distinguish existing evidence from newly executed coverage.

| Family | Templates and upstream motivation |
| --- | --- |
| Recovery | Durable commits across restart; epoch/hole/torn-tail replay; predecessor writes after successor takeover and WAL GC, extending [SlateDB #1622](https://github.com/slatedb/slatedb/issues/1622). |
| Snapshot/MVCC | Held snapshots versus later commits; resident/spilled read-own-writes; bounded, reverse, limited scans with overlapping tombstones. Probe capture and pin registration against GC, guided by [Pebble #6085](https://github.com/cockroachdb/pebble/issues/6085). |
| Compaction | Logical equivalence; obsolete tombstones yielding zero output, including publication crashes ([RocksDB #14897](https://github.com/facebook/rocksdb/issues/14897)); preservation of pinned versions. |
| File lifecycle | Manifest publication versus SST reclamation; repeated crash/recovery; orphan cleanup preserving reachable or ambiguous objects. |
| Concurrency | Held iterator versus maintenance; scheduled capture/publication/GC races; CloudAck admission versus concurrent catalog work. |
| Failure handling | Actual restore abort and retry; partial provider operations/cancellation; permanent flush errors, retry counts, shutdown ([Pebble #6139](https://github.com/cockroachdb/pebble/issues/6139)). |
| Boundary arithmetic | Binary key/range boundaries; sequence/allocation limits; malformed frame/block lengths. Audit generator exclusions as well as encoders ([SlateDB #2024](https://github.com/slatedb/slatedb/issues/2024)). |

Use a large public transaction followed by flush/compaction as Midge's supported analogue of external ingestion. Test documented snapshot and conflict behavior.

Build a small test-only harness using existing Proptest, fixtures, failpoints, and subprocess crash sentinels. Keep logical histories, recovery prefixes, named race schedules, and retry timing in separate drivers. Merge scaffolding with green model tests and a narrow Local smoke test, then explicitly execute the full discovery target. Every mismatch fails and emits a replay artifact.

The `BTreeMap` oracle evaluates reads from the transaction's starting snapshot plus accepted intents in ordinal order. A successful LastWriteWins commit applies intents to current committed state, preserving intervening unrelated writes. Flush/compaction preserve logical state. Restart ends all live transactions, iterators, and snapshots. Crash assertions distinguish durable acknowledgments from buffered, asynchronous, timed-out, and in-flight operations; ambiguous outcomes permit only legal whole-transaction states. TTL and other conflict policies have separate focused probes.

Use seed `0x4d49444745303331`; generate histories before executing backend variants.

| Profile | Fixed budget |
| --- | --- |
| Normal PR | 32 histories, at most 64 operations each; Local resident/spilled paths and applicable confirmed regressions. |
| Discovery/release | 256 histories, at most 256 operations each; all 21 templates on Local and CloudSimulated. |
| Focused properties | 256 cases for each recovery-plan, provider-failure, numeric-boundary, and malformed-input property. |
| External-pattern crashes | Up to 12 named serial abort scenarios, additional to issue regressions. |
| Sqrzl | 16 histories, at most 64 operations each, through supported provider adapters. |

Bound maintenance, live snapshots, restarts, and outstanding requests. Wall-clock limits are hang watchdogs only. Artifacts record revision, seed, template, backend, durability, failpoint ordinal, counters, and minimized concrete history. Use structural shrinking for logical histories and replayable fixed tuples for subprocess crashes.

For each meaningful investigation: contract → Jev prioritization → focused probe → Jev challenge → confirm/deepen/discard. Rephrase weak judgments with exact code-path and coverage evidence. Complete all 21 templates, resolve every mismatch, search duplicates, and add confirmed P1/P2 findings to the milestone. Numeric exhaustion requires realistic reachability. Publish a consolidated confirmed-findings report and updated coverage map before repairs.

## Repair contracts and acceptance gates

| Issue | Required implementation and decisive coverage |
| --- | --- |
| #662 | Carry `LeaseValidity` into runtime fencing; check monotonic validity synchronously, after blocking preparation, and immediately before WAL/memtable mutation. Preserve provider-error classification. Cover stale cached health, successor takeover, coalesced/spilled preparation, BestEffort/CloudAsync rejection, GC, and restart. |
| #663 | Record epoch-regressed sources as holes before filtering; combine/deduplicate sealed and active quarantine plans; persist the complete floor before renaming or catalog retirement. Cover epochs `8,7,9`, stale active WAL, their combination, crashes before quarantine/after each rename/before retirement, and nonmutating strict rejection. |
| #664 | Uniquely create attempt-owned stages with owned cleanup. Atomically publish backup/restore without replacement using native Linux/macOS/Windows primitives; return `NotSupported` when unavailable. Cover process abort/retry, foreign residue, same-target collision, last-moment directory/symlink creation, and all three OSes. |
| #665 | Resolve roots and prospective paths through canonical existing ancestors; reject component-wise overlap before mutation; recheck actual parent and perform I/O through resolved paths. Cover ancestor/descendant, relative, symlink, dangling link, Windows case aliases, valid siblings, and unchanged rejected roots. |
| #666 | Replace shared contention metadata with attempt-local typed internal failure metadata, including ancestor budgets; carry it through catalog completion to CloudAck. Cover sibling success, waiter completion after release, permanent failure, and deadline expiry. |
| #667 | Charged resident indexes and a lazy full-value sorted spill view; authoritative ordinal streams remain intact. Bound sources/fan-in at eight and stream a static range-boundary index for spilled coverage. Cover point/range ordinal order, both directions, later mutation, generation reuse, scaling, and cleanup bounds. |
| #668 | Directional boundary events and an active sequence-aware interval structure; process late SST discovery before the current key. Cover nested/equal/coincident boundaries, snapshot filters, limits, both directions, and late discovery. For `K` candidates and `T` tombstones, logical event work is at most `K + 3T`. |
| #669 | Retain at most eight identity-pinned readers across checkpoints. Share decoded-block LRU capped at `min(read_budget/2, 16 MiB)`; byte owners retain reservations. Evict under pressure and fall back to WAL replay on failed proof. Cover zero further ranges for fitting warm sets, checkpoint continuity, eviction, identity replacement, corruption, oversized blocks, allocation failure, and exact coverage/budgets. |

Transaction admission reserves original stream plus two conservatively bounded auxiliary generations (`O + 2V`) with checked spill-size accounting. At first spill reserve two 1 KiB buffers and eight fixed cursors, capped at 2,952 bytes. Reads perform no new scratch admission. Construction and option minimums stay unchanged. Insufficient workspace rejects the spill-triggering write and preserves prior accepted intents. Cover concurrent transactions and cleanup failures.

For replay locality, rerun the same cold-recovery campaign: require at least 50% fewer remote ranges and response bytes, and report coverage-time improvement against a 25% target. Enforce the existing read budget and measure combined checkpoint/process peaks.

## Conditional checkpoint policy (#670)

Keep measurement and qualifying implementation in one PR. Run three fresh release runs per compaction-enabled cell: 256 × 1 MiB flushes/one CF; 512 × 256 KiB flushes/sixteen CFs; 1,024 × 64 KiB flushes/one CF. Exclude the first 10% as warmup and attribute ordinary local checkpoints separately from forced calls.

Implement only when two of three runs in a cell show snapshot bytes at least 5% of published SST bytes, or checkpoint time at least 20% of publication time with checkpoint p95 at least 5 ms. Ordinary local flushes checkpoint after 64 journal records, 1 MiB, or a pending retry. Cloud, recovery, administration, DDL, compaction-before-GC, and clean local shutdown remain forced.

Before deferring snapshots, atomically journal allocation, `AddSst`, and `BumpWalSeq`. Matching-existing-SST retries unconditionally journal monotonic frontier repairs. Failure retains journal authority and retries; persistent I/O failure does not imply a hard journal-size bound. Test crashes after WAL pruning and snapshot rename-before-truncation, frontier repair/reopen, and forced-call coverage.

Accept only with at least 90% lower snapshot bytes in fixed-cardinality probes and 50% lower attributable checkpoint bytes/time in qualifying cells, without more than 10% stable throughput or flush-p95 regression. Otherwise record qualification and an explicit deferred-policy follow-up.

## Review and release evidence

For every repair batch, demonstrate each focused failure on unchanged base in an isolated build target; run focused, adjacent, and already-fixed corpus tests after repair. Ask Jev to challenge contract, reproduction, impact, root cause, and related patterns. Obtain two fresh independent reviews covering correctness/test validity and adversarial failure/lifetime/budget behavior. Turn material findings into regressions and repeat review after consequential changes. Require exact-head hosted checks, resolved threads, and relevant Windows/macOS regressions; include omitted decisive Windows tests in CI. Squash into `develop` and verify the actual merged revision before the next batch.

Before promotion, independently review authority/recovery/GC, snapshot retention, transaction accounting, contention classification, and checkpoint/cache interactions. Prepare Cargo metadata/lockfile, changelog, current examples, migration/rollback guidance, support matrix, and known risks while retaining historical evidence.

Qualification requires formatting, pedantic Clippy, default/all-feature tests/docs, serial fault injection, MSRV 1.97, no-default/isolated features, docs/test validation, dependency checks, packaging, sample databases, Docker, and Sqrzl. Execute the complete model/release campaign and all confirmed P1/P2 counterexamples without permanent ignores; run bounded smoke for all four fuzz targets and lifecycle benchmarks.

Use registry `=0.3.0` and the packaged candidate for clean Local/provider-backed Cloud upgrade/downgrade fixtures. Cover Local/CloudSimulated backup format, inspect lockfiles, and exclude salvage-mutated databases from rollback claims.

Promote `develop` to `main` by merge commit after current-head Ubuntu, Windows, macOS, cloud, compatibility, Docker, and managed CodeQL pass. Qualify the actual merged `main`: wait for automatic CI, then dispatch cloud, fuzz, and lifecycle-enabled CI without ref-scoped cancellation; record each tested SHA.

Verify peeled `v0.3.1`, qualified SHA, and current `main` match. Explicitly dispatch Publish on the tag; tag creation does not publish. Download `cntryl-midge@0.3.1` and run a clean consumer pinned to `=0.3.1` using public APIs and registry dependencies. Publish release notes with evidence, compatibility limits, and risks. Close the milestone only after the roadmap records final issues/PRs, qualified revision, publication proof, coverage report, and #670's outcome.


## Backup/restore repair evidence (in progress)

Historical status record: superseded by the [final release qualification](#final-release-qualification-2026-10-02).

The current batch repairs #664 and #665 together. Four public API regressions
failed before runtime changes: direct backup overlap, restore overlap, symlink
alias overlap, and deterministic foreign-stage blockage. Actual restore-process
abort/retry failed on both Local and CloudSimulated. The candidate uses uniquely
created attempt-owned stages, canonical path isolation, and native atomic
publication without replacement. Directory/symlink creation at publication and
same-target concurrent restore are covered. All eight named abort tuples passed
on candidate `3b267126` on macOS. Independent review found two test-only evidence
issues, tracked by #674: the Local crash fixture edited the wrong lease file,
and the restore oracle did not compare keys. Both now have regressions.
Hosted exact-head and post-merge qualification remain pending.

## Delivery updates

- PR #675 merged into `develop` as `a0675657`, closing #664, #665, and #674.
  Candidate and actual merged trees match. Hosted Linux, Windows, and macOS CI
  passed; native provider run `36942486981` passed 128 fixed-seed history replays.
  Focused checks and eight actual aborts also passed on the merged revision.
- The next coherent P1 batch repairs #662 lease authority and #663 salvage
  prefix/floor ordering. Existing public signatures, error variants, and persisted
  formats remain unchanged. Expanded discovery and full release qualification
  remain gates before promotion and publication.

## 0.3.1 release preparation (2026-10-02)

Historical status record: superseded by the [final release qualification](#final-release-qualification-2026-10-02).

All twelve milestone issues are closed. Repairs merged through PRs #675
(backup/restore), #676 (authority and salvage), #679 (spill-fixture validity),
#681 (transaction and tombstone reads), #682 (replay locality), and #683
(CloudAck contention). PR #684 measured #670; checkpoint policy was explicitly
not promoted. Its APFS measurement confirmed quadratic cumulative bytes and
approximately flat 27–29 ms per-flush latency over 64–2,048 live SSTs. It did
not model production flush rate, steady-state compaction, or a deployment SLO.

Development revision `d87da154357c2706c1b177417fefbea7bc6a8c58` passed
[CI](https://github.com/cntryl/midge/actions/runs/37026305792) and managed
CodeQL. These development checks do not establish release completion.

The merged performance implementations differ from the original acceptance
contracts: #681 retains matching-run point probes and does not implement the
specified eight-source sorted spill view or upfront auxiliary scratch admission;
#682 retains four readers with four decoded blocks each, caps retained blocks
at one quarter of the recovery budget, and clears readers/proofs at checkpoints.
The changelog records these limits. On 2026-10-02 the user approved qualifying and shipping these merged designs
instead of requiring the original bounded-source and cross-checkpoint cache
contracts. Those original design prescriptions are superseded for 0.3.1;
correctness, configured-budget, and native qualification checks still apply.
Native cold-recovery measurements remain pending; neither issue closure nor
this version bump establishes qualification completion.

Release preparation adds an explicit `release_recovery_cost` Cloud Integration
workflow input for the documented 128 MiB WAL profile, with durable artifact
upload. Normal provider qualification remains enabled for every dispatch.
Qualification, promotion, tagging, publication, consumer validation, and
milestone closure remain pending until their evidence is recorded.

### Fuzz qualification follow-up #686

Development fuzz run [37028361947](https://github.com/cntryl/midge/actions/runs/37028361947)
failed at the default 2 GiB RSS cap while processing tiny WAL inputs. The log
preserves the four-byte input `04 00 06 00`; the original workflow did not upload
failure artifacts. Independently, the common fuzz helper was confirmed to race
asynchronous Engine drop: all 64 immediate Strict opens returned `LeaseHeld`
after seeding, preventing malformed-input recovery coverage.

Two `tests/fuzz_harness.rs` regressions failed on unchanged helpers, then passed
with explicit bounded shutdown after every successful open. Startup errors
remain accepted input outcomes. Sanitizer settings and RSS limits are unchanged.
Fuzz corpus and crash artifacts are now uploaded even on failure. The subsequent
four-target sanitized rerun is required before treating the RSS failure as
resolved; this evidence establishes the harness defect, not a WAL corruption bug.

### Candidate qualification evidence

- Full logical release profile at `d87da154` passed 1,024 fixture histories and
  117,380 actions across Local/CloudSimulated resident/spilled fixtures. The eight
  named process aborts passed with fourteen validated reopens and scans. Artifacts:
  logical `de01d92c-7140-48b4-a8a3-f8c136609286`, physical
  `c881dfbb-39ed-463a-8ab8-b24cf31da84b`, under the local
  `/tmp/midge-031-release-discovery-d87da154` evidence directory.
- Default `cargo test --locked` with `PROPTEST_CASES=256` passed on the candidate;
  ignored native-provider and fault-injection targets are selected separately
  by hosted qualification. Two fuzz lifecycle regressions were demonstrated red
  before their repair and green afterward. A 256-schedule capture/pin/GC
  regression now checks the real acquisition guard against concurrent GC
  sampling before and after pin registration.
- Clean consumer lockfiles prove registry `=0.3.0` and the packaged `0.3.1`
  candidate. Upgrade/downgrade read/write/scan fixtures passed on Local,
  CloudSimulated, and native Sqrzl S3; provider recovery also passed after complete
  cache loss with both binaries. Local/CloudSimulated backups restored in both
  version directions. These are clean fixtures; salvage mutation is excluded.
- Native cloud [run 37028796716](https://github.com/cntryl/midge/actions/runs/37028796716)
  passed provider contracts, engine qualification, 128 provider histories,
  and the documented release-mode 134,219,148-byte WAL campaign. All 16,212
  source records were verified after interrupted recovery, repeated cache loss,
  the mixed workload, and compaction.

| Release cold-open observation | Historical reader-reuse campaign | 0.3.1 candidate |
| --- | ---: | ---: |
| HTTP ranges | 33,536 | 4,252 |
| Consumed range-response bytes | 2,553,475,173 | 902,222,301 |
| Coverage time | 16.107 s | 2.035 s |
| Cold open | 18.216 s | 4.400 s |
| Coverage probes | 32,618 | 35,402 |
| Verified SST bytes | 133,981,593 | 140,855,958 |

Observed reductions are 87.3% in ranges and 64.7% in response bytes, exceeding
50% targets. Coverage time is 87.4% lower than the historical observation;
this is not a controlled same-host latency comparison or production SLO.
The source WAL and configured 32 MiB local/64 MiB engine profile match;
maintenance produces different SST/probe counts. Candidate open-process RSS
was 82,231,296 bytes, and final whole-campaign RSS reached 98,308,096 bytes.
RSS includes allocations outside configured engine pools and is not a claim
that the process fits within 64 MiB. Tracked local files stayed below 32 MiB;
reader/block budget tests enforce the configured recovery pool separately.
Artifact: `bd4986f4-4bc5-45fc-b570-da5ab14a4ba8`, revision `9457f200`.

MSRV/no-default qualification exposed mechanical test-only blockers: sixteen
one-minute `Duration` spellings and a failpoint-only helper compiled without
`failpoints`. Equivalent spellings and precise feature gating repair these;
no production path changes. Actual Rust 1.97 pedantic no-default Clippy passed.

The first repaired fuzz rerun passed WAL and manifest targets without the prior
RSS failure, then timed out in intent recovery. The preserved eighteen-byte
input is committed as a replay fixture; eight unsanitized replays took under a
second each. A sanitizer replay precedes the next four-target campaign. Neither
an input-specific production defect nor sanitizer qualification success is
claimed until that replay/campaign completes.

## Final release qualification (2026-10-02)

This section supersedes the historical pending statements above. The approved
scope ships the merged transaction/read-cache designs; it does not claim the
original stronger bounded-source or cross-checkpoint-cache design. #670 remains
measured and deferred. All required fixes #662–#669 and qualification repairs
#686, #688 and #690 are closed. The consolidated coverage interpretation is in
[discovery-0.3.1.md](discovery-0.3.1.md#final-qualification-coverage-interpretation).

Promotion [#687](https://github.com/cntryl/midge/pull/687) merged main
`227cea4a377dfd4e113bc7aa48aca22e442e63d6`; its tree
`3745764fe76c3ddad176ae05828a14a822e0ac9d` matches qualified develop `6aaa0c9f`.
The immutable annotated `v0.3.1` tag peels to that main commit.

| Exact-main qualification | Passed run |
| --- | --- |
| Linux/macOS/Windows CI | [37040320734](https://github.com/cntryl/midge/actions/runs/37040320734) |
| MSRV/no-default and isolated features | [37040320748](https://github.com/cntryl/midge/actions/runs/37040320748) |
| Complete Docker all-feature suite | [37040320755](https://github.com/cntryl/midge/actions/runs/37040320755) |
| Native provider and release-mode recovery cost | [37040321159](https://github.com/cntryl/midge/actions/runs/37040321159) |
| Retained input replay and four sanitized fuzz targets | [37040324103](https://github.com/cntryl/midge/actions/runs/37040324103) |
| All-OS lifecycle benchmarks plus repeated CI | [37041401665](https://github.com/cntryl/midge/actions/runs/37041401665) |

Managed CodeQL passed the identical qualified source tree before promotion in
[37039099316](https://github.com/cntryl/midge/actions/runs/37039099316).
`cargo package --locked` on actual main passed. Full clean-Git release discovery
at `f0e01c96` passed all eighteen tests, 1,024 fixture histories / 117,380 actions,
eight actual process aborts and fourteen reopens/scans. Its production and
discovery-driver code match final main; the only later source change is the
qualified takeover-test timing repair. Logical artifact:
`798ed253-eb3d-49bd-aeb5-4d164a724c42`; physical artifact:
`c69c7d3c-4786-4021-baec-f028f8343501`.

Final native-provider artifact `153c964e-b29e-41d7-94e5-ffaa2750f4b8` records
134,219,148 WAL bytes and all 16,212 source records verified after interrupted
recovery, repeated cache loss, mixed writes and compaction. The final cold-open
phase used 4,258 HTTP ranges / 905,371,044 consumed response bytes, 1.902 seconds
of coverage work and 4.183 seconds to open. Against the recorded historical
33,536 ranges / 2,553,475,173 bytes, reductions are 87.3% / 64.5%; timing remains
informational across hosts. The final whole-campaign verification includes
additional read requests and is not substituted for the cold-open phase.
Tracked local-file peak was 2,172,547 bytes under the 32 MiB local budget;
whole-process RSS peaked at 94,896,128 bytes and is not a 64 MiB process-cap claim.
Reader/block accounting tests separately enforce recovery-pool limits.

The Windows one-second lease test failed twice in promotion run 37035723691.
Repair #691 uses matching 30-second holder TTLs, waits actual predecessor expiry,
and retains takeover, stale-write rejection and successor restart assertions.
The focused Local/CloudSimulated test and hosted Linux/macOS/Windows passed.
No production TTL, expiry assertion or gate was disabled. Docker archive smoke
records explicit caller-provided provenance; full discovery still rejects
archives and requires a clean Git checkout. Sanitized fuzz passed without
relaxing RSS/time limits, including the preserved eighteen-byte intent input.

[Publish run 37042539032](https://github.com/cntryl/midge/actions/runs/37042539032)
passed its full release gate and trusted crates.io publication. A fresh external
consumer depends only on registry `cntryl-midge = "=0.3.1"`; its lockfile records
source `registry+https://github.com/rust-lang/crates.io-index` and checksum
`727af8f9a206a494b872b3b7a690af6d2084448a981a9e9b5afb58af446111e9`.
Write, flush, bounded shutdown, reopen, read and scan passed on Local,
CloudSimulated and native Sqrzl S3. No path or Git patch was used. Earlier clean
0.3.0/candidate upgrade/downgrade and backup/restore results remain scoped to
clean fixtures and the constrained rollback guidance.

The [GitHub release](https://github.com/cntryl/midge/releases/tag/v0.3.1) records
migration, rollback, qualified scope and known limits. The 0.3.1 milestone closes
after this completion record merges. Main/tag remain at the qualified release
commit; this final evidence update is documentation on develop.
