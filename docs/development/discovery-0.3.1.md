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

Histories hold at most two transactions, four maintenance operations, and two
restarts. The spilled fixture uses an 8 KiB transaction pool and verifies that the
bootstrap actually creates spill files; the resident fixture uses 2 MiB. Both use
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

The original counterexample is saved before minimization. Shrinking removes legal
chunks and preserves the failure class and, for API errors, operation and error
variant. It performs at most 128 replays per mismatch and records whether that
budget ended minimization. A minimized mismatch is an investigation lead; matching
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
| Recovery: epoch, hole, torn tail | None | Recovery-plan property and #663 combined quarantine/crash probes |
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
| Failure: restore abort and retry | None | #664 actual process abort; foreign stages and target collisions |
| Failure: partial provider operations/cancellation | None | Supported provider adapter and cancellation probes |
| Failure: permanent flush errors/retry/shutdown | None | Retry timing and bounded shutdown probes |
| Boundary: binary keys and ranges | BinaryBoundaries; generator includes edges | Full backend campaign and encoder exclusion audit |
| Boundary: sequence/allocation limits | None | Focused arithmetic properties and realistic reachability triage |
| Boundary: malformed frame/block lengths | None | Fixed-seed 256-case malformed-input properties |

For every meaningful investigation, record contract, Jev prioritization, focused
probe, Jev challenge, and confirmed/deepened/discarded outcome. Jev judgments do
not substitute for implementation and test evidence. Deduplicate confirmed P1/P2
findings against open issues before adding them to milestone 0.3.1. Publish the
completed findings report and coverage map before beginning repairs.
