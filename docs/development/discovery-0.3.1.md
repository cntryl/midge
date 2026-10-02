# 0.3.1 discovery campaign

This is the execution map for the expanded discovery in
[the release roadmap](roadmap-0.3.1.md). The logical scaffold covers seven history
templates. It does not establish the full 21-template discovery exit gate.
Physical recovery prefixes, named race schedules, retry timing, provider failures,
TTL, and additional conflict policies need their own probes.

## Logical oracle and profiles

`tests/discovery_model.rs` uses an independent `BTreeMap` oracle. Transaction reads
evaluate the starting snapshot and accepted intents in ordinal order. A
LastWriteWins commit applies its intents to current committed state, retaining
intervening unrelated writes. Flush and compaction preserve state. Clean restart
ends every live transaction. Crash durability and ambiguous acknowledgments are
outside this driver's scope.

All histories are generated before any backend executes, with Proptest's ChaCha
runner and fixed seed `0x4d49444745303331`. Values are concrete recipes: a byte and
a length expand to exactly that many repeated bytes. Keys include empty keys,
zero bytes, all-`0xff` boundaries, and arbitrary binary keys. Bounds are inclusive
at the start and exclusive at the end; the oracle filters prefix intersections
directly before direction and limit, without engine normalization helpers.

| Profile | Histories | Maximum operations | Fixtures |
| --- | ---: | ---: | --- |
| Default smoke | 1 | 64 | Local resident |
| `pr` | 32 | 64 | Local resident and spilled |
| `discovery` / `release` | 256 | 256 | Local and CloudSimulated, resident and spilled |
| `sqrzl` (`sqrzl-tests` feature) | 16 | 64 | S3, Azure, GCS XML and JSON, resident and spilled |

Histories hold at most two transactions, four maintenance operations, and two
restarts. The spilled fixture uses an 8 KiB transaction pool and verifies that the
bootstrap actually creates spill files. OrdinalIntents and BoundedScan also add
12 KiB of padding inside their mixed-intent transaction before range/point writes,
so those targeted histories cross the spill threshold themselves. The resident
fixture uses 2 MiB. Both use
64 MiB engine memory, 64 KiB memtables, and explicit compaction. Local commits
request sync durability; CloudSimulated commits request CloudStrict durability.

```sh
cargo test --test discovery_model
MIDGE_DISCOVERY_PROFILE=pr cargo test --test discovery_model
MIDGE_DISCOVERY_PROFILE=discovery cargo test --test discovery_model -- --nocapture
```

Full discovery and release runs require a clean committed revision. CI runs the
normal PR profile on Ubuntu and retains its artifacts. Default platform runs
include the narrow smoke test. CI job timeouts serve as hang watchdogs; elapsed
time does not determine logical success.

## Artifacts and replay

Each run owns a unique directory under `target/discovery-031`, or the directory
selected by `MIDGE_DISCOVERY_ARTIFACT_DIR`. The driver persists its corpus before
execution, revision and dirty status, seed, profile, backend variants, operation
counts, and successful-case counters. Every mismatch fails the campaign and
produces an original and structurally minimized concrete history, observations,
failure class, backend, durability, counters, and failpoint ordinal (null for
logical histories).

The original counterexample is saved before minimization. Artifact replacement
stages and syncs a sibling temporary file before atomic publication, retaining the
prior readable artifact if staging or pre-publication fails. Shrinking removes legal
chunks and preserves the failure class and, for API errors, operation and error
variant. It performs at most 128 replays per mismatch and records whether that
budget ended minimization. Spill-coverage failures retain the original inducing
workload, skip shrinking, and keep their required spill check in replay. Their
strategy is `retain_fixture_workload`, with zero minimization replays and completion
false. Removing their writes would manufacture failure on a correctly spilling
engine. Logical mismatches use `legal_chunk_deletion` and may omit the spill bootstrap.
Artifacts record both execution requirements. A minimized mismatch is an investigation lead; matching
classes alone do not establish that two failures have the same root cause.

```sh
MIDGE_DISCOVERY_REPLAY=target/discovery-031/RUN/failure-N-Local-Spilled.json \
  cargo test --test discovery_model should_match_transaction_oracle -- --nocapture
```

Replay executes the saved concrete minimized history and fixture. It fails while
the mismatch remains and passes after a repair. Preserve original artifacts with
their revision; do not relabel later replay success as original-revision proof.

## Architectural coverage map

This map records scaffold capability, not completed full-campaign evidence.
Each physical probe must record its exact revision, seed or fixed crash tuple,
backend, durability, ordinal, counters, and outcome. Applicable focused properties
run 256 cases; actual abort scenarios are serial and capped at twelve beyond
issue-specific regressions. Sqrzl uses sixteen histories of at most 64 operations
through supported adapters, rather than treating a filesystem mock as provider
qualification.

| Family / template | Logical scaffold | Remaining physical or focused evidence |
| --- | --- | --- |
| Recovery: durable commits across restart | DurableRestart; clean sync restart | Actual abort prefixes and acknowledgment classification |
| Recovery: epoch, hole, torn tail | 256 fixed-seed epoch/prefix properties; #663 sealed/active/combined regressions | Five actual floor/rename/retirement abort tuples; broader torn-tail campaign remains |
| Recovery: predecessor after takeover and GC | None | #662 validity, takeover, GC, and reopen probes |
| Snapshot: held snapshot versus later commits | HeldSnapshot, PinnedCompaction | Large public transaction plus maintenance and conflict-policy probes |
| Snapshot: resident/spilled own writes | OrdinalIntents; both read fixtures | Scaling and read-source bounds for #667 |
| Snapshot: bounded/reverse/limited overlapping tombstones | BoundedScan | Late SST discovery and event-work bounds for #668 |
| Compaction: logical equivalence | CompactionEquivalence | Full backend campaign |
| Compaction: obsolete tombstones yield zero output | Empty logical state after compaction | Actual empty-output publication abort and lifecycle proof |
| Compaction: preserve pinned versions | PinnedCompaction | Retention and reclamation evidence under scheduled GC |
| Lifecycle: manifest publication versus reclamation | None | Publication/reclamation crash tuples |
| Lifecycle: repeated crash/recovery | None | Repeated actual process aborts |
| Lifecycle: preserve reachable/ambiguous orphans | None | Reachability and partial-provider failure probes |
| Concurrency: held iterator versus maintenance | None | Iterator held across explicit maintenance schedule |
| Concurrency: capture/publication/GC | None | Named capture-to-pin race; sequential reads are insufficient |
| Concurrency: CloudAck admission versus catalog | None | #666 sibling reservation and waiter completion schedule |
| Failure: restore abort and retry | #664/#665 merged in PR #675 | Eight physical tuples qualified on Linux/macOS/Windows; foreign stages and target collisions pass |
| Failure: partial provider operations/cancellation | None | Supported provider adapter and cancellation probes |
| Failure: permanent flush errors/retry/shutdown | None | Retry timing and bounded shutdown probes |
| Boundary: binary keys and ranges | BinaryBoundaries; generator includes edges | Full backend campaign and encoder exclusion audit |
| Boundary: sequence/allocation limits | None | Focused arithmetic properties and realistic reachability triage |
| Boundary: malformed frame/block lengths | None | Fixed-seed 256-case malformed-input properties |

For every meaningful investigation, record contract, Jev prioritization, focused
probe, Jev challenge, and confirmed/deepened/discarded outcome. Jev judgments do
not substitute for implementation and test evidence. Deduplicate confirmed P1/P2
findings against open issues before adding them to milestone 0.3.1. The user directed confirmed repairs to proceed immediately after the full logical
campaign. Complete the findings report and coverage map before release promotion.

## Named process-abort driver

With the `failpoints` feature, `MIDGE_PHYSICAL_DISCOVERY=1` runs eight serial
fixed tuples: restore object-copy interruption and three empty-compaction
publication boundaries, each on Local and CloudSimulated. The corpus is persisted
before execution. Each child must leave the existing synced named-trigger sentinel
and exit by process abort; a fallback panic cannot satisfy the probe. Restore
then retries the same artifact. Compaction requires a durable remove-only intent
and validates the empty state through two reopens. Only the private aborted
fixture's expired owner lease is adjusted to permit immediate takeover.

```sh
MIDGE_PHYSICAL_DISCOVERY=1 cargo test --test discovery_model --features failpoints \
  physical::should_execute_named_abort_histories -- --nocapture --test-threads=1
MIDGE_DISCOVERY_PROFILE=sqrzl cargo test --test discovery_model --features sqrzl-tests \
  -- --test-threads=1
```

The physical campaign requires a clean committed revision and records the seed,
concrete tuple, revision, backend, durability, failpoint ordinal, failure message,
and actual validated abort/reopen/scan counters. Planned reopen counts are
separate from completed counters. Every failed tuple fails the campaign; known
defects are retained as counterexamples rather than accepted outcomes. Native
Sqrzl execution is part of the hosted Cloud qualification workflow.

## Completed logical evidence

The full logical campaign on `0e0a6f7c6b1bbe2316402b18b0c6af251fd11e7e`
passed all 1,024 fixture histories (256 per fixture) with no ignored tests.
The generated corpus contains 29,345 actions, repeated across four fixtures,
for 117,380 scheduled actions. It used the fixed seed above and a clean revision.
The local evidence directory is
`target/discovery-031/full-0e0a6f7c/0e24860e-f333-44fe-a112-9359805c1b3d`.
This establishes the seven logical templates; the remaining physical, race,
retry and focused-property evidence is still required for discovery exit.

## Authority and salvage repair qualification

PR #675 merged at `a06756579351c6e19484e33fc46d3a9a876806ec`; its
actual merged tree matches the qualified candidate. Hosted CI and provider
qualification passed, and downloaded artifacts prove eight named aborts with
fourteen reopens/scans on each OS and 128 native provider history replays.
Issues #664, #665, and harness-validity issue #674 are closed.

The next repair batch covers #662/#663. Baseline probes demonstrate expired
monotonic validity with cached health still true admitting an Engine commit,
sealed epochs `8,7,9` replaying IDs `[1,3]`, and a stale active WAL losing its
sequence floor. Candidate regressions cover resident/coalesced/spilled
preparation boundaries, public Engine admission, strict nonmutation, and 256
fixed-seed recovery-plan cases. Five actual abort tuples exercise durable floor
publication, each of three quarantine renames, and pre-retirement interruption.
Their test-only driver preserves tuple metadata and outcome counters under
`target/discovery-031/salvage-*`; child SIGABRT and synced named sentinels prove
the intended boundary. These are issue-specific regressions, separate from the
twelve-scenario external-pattern budget.

```sh
cargo test --lib validity --all-features -- --test-threads=1
cargo test --lib streaming_wal_plan::tests --all-features -- --test-threads=1
cargo test --lib should_reject_predecessor --all-features -- --test-threads=1
```

Independent failure review identified an additional #662 continuation: the
physical WAL worker could resume a queued append after its handle preparation
outlived validity. The pre-guard runner regression and real flush-publication
regression both fail. The repair carries the same validity into queued WAL
write/fsync, replacement writers, flush finalization and control mirroring, and
compaction publication. A scheduled regression blocks persistent handle open,
queues while authorized, expires validity, and releases the worker; rejection
must leave WAL bytes unchanged and retain operation-specific admission proof.
Rotation and physical fsync have separate regressions. Provider-error
classification remains unchanged. These checks extend the existing issue's
expired-source contract rather than introducing a separate finding.

The second independent review identified related rollback, rotation, completion
and startup continuations. Focused pre-guard regressions fail for expiry inside
a partial WAL write, before rotation rename, during flush provider validation,
during startup validation, and before compaction input GC. The repair rechecks
authority before rollback truncate and sync (retaining disk admission without
a durable unchanged proof), after rotation preparation/shutdown, between intent
persistence and manifest publication, and before clearing compaction intents.
Compaction expiry before GC retains inputs and its intent; expiry after a valid
GC retains the durable intent for recovery. None of these tests claim that a
lease check can cancel an I/O call that has already entered the provider.

Further call-site qualification exercises the real compaction completion handler,
including its failure cleanup, and the paired cloud-flush/rotation transition.
The corresponding pre-guard regressions reproduce unwanted input deletion and
an unsettled-transition assertion. Both now fence without additional mutation.
Salvage has an authority callback through floor publication, each quarantine
rename after path preparation, directory sync, and catalog retirement. Expiry
after floor publication or after one rename preserves a successor's active WAL
and the complete raised floor. Failpoint tests acquire the repository's
isolation guard before the fail crate's scenario lock; the expiry corpus also
runs with eight test threads to check the required lock ordering.
