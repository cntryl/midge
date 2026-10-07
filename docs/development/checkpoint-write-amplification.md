# Checkpoint Write Amplification

**Status: measured; B and C miss the snapshot-payload target.** All nine
release repeats pass construction and independent readback at the source
recorded below. Checkpoint cadence remains unchanged. Catalog mapping and
final one-hour correctness/recovery acceptance are tracked through #711.

This campaign evaluates the conditional checkpoint investigation recorded in
[#715](https://github.com/cntryl/midge/issues/715). It measures metadata payload
issued through Midge's filesystem interface and checkpoint time within actual
flush publication. It does not measure physical device write amplification or
cloud transfer/write cost.

## Fixed construction

The registered target is `tier4_system_checkpoint_write_amplification`, gated
by the explicit `checkpoint-bench` feature. Each cell runs three fresh release
processes on three fresh hosted Ubuntu runners: nine preregistered attempts.

| Cell | Flush cycles | Logical value bytes per cycle | Column families | Warmup cycles | Measured cycles |
| --- | ---: | ---: | ---: | ---: | ---: |
| A | 256 | 1 MiB | 1 | 26 | 230 |
| B | 512 | 256 KiB | 16 | 52 | 460 |
| C | 1,024 | 64 KiB | 1 | 103 | 921 |

Each cycle commits one transaction with `WriteOptions::sync()` and explicitly
flushes its selected family. Values are deterministic 1 KiB pseudorandom
payloads; keys and encoding overhead are additional bytes. Families rotate in
cell B. The first ceiling-rounded 10% of cycles is warmup. Compaction remains
enabled and at least one genuine compaction must complete inside the measured
ingestion window. Resolved options and actual completion counts are retained.
Typed pre-admission `WriteStall` is retried by reconstructing the exact same
transaction and using the public stall waiter. One allowance is captured
before the first attempt: the earlier of 30 seconds and the original 15-minute
cell deadline. Wait slices are at most one second. Other commit/wait errors,
including unknown-outcome `Timeout`, remain terminal. Neither retries nor
waiter clears advance progress or acknowledged-row counts.

Cumulative attempt, strict-success, stall, waiter-result and wait-time counters
are retained before and after the measured interval and in final status.
Admission and wait costs stay inside the ingestion clock; they remain outside
the publication clock's documented boundary below. The independent reader
checks counter subtraction, completed-cycle arithmetic and the original
60-second native no-progress configuration. Terminal errors and exhausted
pressure remain unsuccessful construction, rather than passing zero work.

Each metadata boundary uses one allowance capped at 30 seconds and the
original cell deadline. A timed runtime query precedes the retained owner's
accounting snapshot. Active Persistent metadata candidates are resampled
after a requested pause of at most 1 ms; only an idle candidate before expiry
is accepted. Actual pause duration is retained, including scheduling delays.
Earlier warmup candidates and pauses stay outside measurement, while the
accepted query starts the measured clock. All end-boundary queries and pauses
stay inside that clock. Sampling advances no watchdog progress and establishes
adjacent observations, rather than a global runtime barrier. Both raw endpoint
snapshots and the final owner integrity checks still require zero active
Persistent metadata operations.

After ingestion, the workload verifies every acknowledged key/value and the
complete ordered scan, shuts down, reopens the same path, verifies again and
performs a second owned shutdown. The original counter handle survives both
engines without retaining filesystem locks or runtime workers.

## Predeclared gate

Only the measured `ordinary_local_flush` / `persistent` bucket enters these
ratios. For one valid repeat, a miss is either:

- Issued snapshot payload bytes are **at least 5%** of uniquely committed
  ordinary-flush SST bytes; or
- Checkpoint elapsed time is **at least 20%** of full logical flush-publication
  elapsed time **and** checkpoint p95 is **at least 5 ms**.

The p95 predicate uses the lower bound of the measured fixed-bucket histogram;
the receipt retains both bounds and all bucket counts. Interval histograms
come from subtracting cumulative bucket counts, not subtracting percentiles.
SST input values, current live SST footprint, compaction outputs and retry
attempt bytes are not substituted for the committed-flush denominator.

A cell qualifies for conditional policy investigation only when at least two
of its three valid repeats miss, **and all nine campaign attempts are valid**.
A missing, skipped, failed or incomplete attempt is invalid, rather than a
passing zero-cost observation. Zero denominators, unclassified measured cost,
counter overflow, escaped or incomplete accounting, missing genuine
compaction, mismatched flush counts or incomplete histogram coverage also
invalidate an attempt.

Persistent failed, abandoned or incomplete metadata/checkpoint operations
invalidate the measurement even when their origin is forced. The original
and reopened final owner snapshots must pass the same integrity checks after
owned shutdown. Later forced costs stay outside the measured ratios.
Every report retains all nine rows and their invalid reasons. A new workflow
attempt preserves its own run/attempt identity; do not silently replace a
failed repeat or select favorable repeats from different campaigns.

The first campaign at source `c8983928ce285ea3889e1fa84d7ca0408802527d`,
[run 37268459342](https://github.com/cntryl/midge/actions/runs/37268459342)
attempt 1, failed construction when all three A repeats encountered ordinary
L0 `WriteStall` after 119, 155 and 173 cycles. All six B/C repeats completed,
but this incomplete campaign does not qualify a cell or permit policy
investigation. Its original archives, native failures and invalid nine-row
readback are retained. [#729](https://github.com/cntryl/midge/issues/729)
adds the bounded construction policy above; the corrected source requires
a fresh smoke and all nine new attempts. This is a benchmark defect, not
evidence of lost acknowledged rows or engine deadlock.

The second campaign at source `32911805e5b10c01086e9270e71cc7990d36c168`,
[run 37270845568](https://github.com/cntryl/midge/actions/runs/37270845568)
attempt 1, retained six and nine actual rejected/cleared commit attempts in
A/r1 and A/r2. Both completed all fixed writes, verification, reopen and two
shutdowns, but captured their measured endpoint during active compaction
metadata work. The zero-active gate correctly rejected both endpoints; the
whole campaign remains invalid despite the seven other valid rows.
[#731](https://github.com/cntryl/midge/issues/731) adds the bounded boundary
capture above. Settled final-owner snapshots do not replace these invalid
measured endpoints. The corrected source again requires its own smoke and
all nine new attempts.

## Accounting boundary

An engine owns one bounded accounting owner. Operations carry an explicit
origin through retries: ordinary local flush, cloud flush, recovery,
bootstrap, DDL, compaction before GC, administration, shutdown or unclassified.
Persistent and memory-only metadata are separate buckets; origin is not
inferred from the benchmark's current phase.

Issued bytes count the payload passed to an actual delegated `write_at` or
`append`, including repeated attempts. Successful write-return bytes and
durability-confirmed snapshot/journal bytes remain separate. Snapshot
durability does not imply checkpoint completion: the required journal
truncation/sync must also succeed. A failed write can retain issued bytes
without claiming a successful return or physical partial-write quantity.

Full publication spans the first accepted publish-worker submission through
installation of that immutable's matching publication, including subsequent
retry wait, metadata work and checkpointing. It excludes the earlier SST
build and time before publication admission. Worker-attempt
duration/count/failure and public `flush_cf` latency are separate diagnostics.
Only one matching successful installation credits SST bytes; stale or failed
completions do not duplicate that credit.

The workload clock is captured before `Engine::open`; the first exported
owner snapshot already includes owner-visible startup work. Pre-state FORMAT
creation, loading/repair/hydration and independent bootstrap staging are
explicitly uncovered; their cost is unknown, not zero. Warmup, measured
window, forced operations and settled owner lifetime remain distinct.
`active_operations` counts accounting operations, not all runtime activity.

Filesystem-issued payload excludes filesystem metadata/journaling,
copy-on-write, delayed writeback, compression/deduplication, other processes
and SSD FTL effects. It is not a physical-device byte counter. Cloud request
bodies, network retries and provider-side writes are outside this campaign's
byte gate. Optional device observations need a separate stated boundary and
cannot replace the preregistered gate.

## Hosted construction and readback

Build and test outside the measured interval. The Tier 4 workflow builds the
release benchmark and Rust reader on a separate VM, records the source/tree,
lockfile, toolchain, compiler artifacts and executable hashes, then transfers
the benchmark to fresh measurement runners. Companion native/accounting
artifacts use tmpfs; the database uses the runner filesystem. The measured
step runs no Cargo build, test, container or resource sampler.

From the exact reviewed checkout, build the reader and confirm target build:

```bash
cargo bench --locked --no-run --bench tier4_system_checkpoint_write_amplification --features checkpoint-bench
cargo build --release --locked --example checkpoint_campaign_readback --features internal-testing
```

Set `CHECKPOINT_REF` to that branch or tag and `CHECKPOINT_SHA` to its exact
40-character commit. First dispatch the dedicated construction smoke:

```bash
gh workflow run bench-tier4.yml --ref "$CHECKPOINT_REF" \
  -f mode=checkpoint-smoke -f expected_sha="$CHECKPOINT_SHA"
```

After the run completes, set `CHECKPOINT_SMOKE_RUN` and
`CHECKPOINT_SMOKE_ATTEMPT` to its actual IDs. Capture into an unused directory,
with the report outside that directory:

```bash
bash examples/checkpoint_campaign_readback/capture-and-readback.sh \
  construction-smoke cntryl/midge "$CHECKPOINT_SMOKE_RUN" \
  "$CHECKPOINT_SMOKE_ATTEMPT" "$CHECKPOINT_SHA" \
  /tmp/midge715-smoke-fresh target/release/examples/checkpoint_campaign_readback \
  /tmp/midge715-smoke-report.json
```

This smoke runs actual A/r1 with all 256 cycles. Readback must validate its
native receipt, both exact-row verifications, accounting owners, actual build
and archive transport. The other eight rows remain explicitly unexecuted and
invalid; smoke never qualifies a cell or accepts cadence changes.

Dispatch the full matrix only after that same-SHA smoke validates:

```bash
gh workflow run bench-tier4.yml --ref "$CHECKPOINT_REF" \
  -f mode=checkpoint -f expected_sha="$CHECKPOINT_SHA" \
  -f transport_smoke_run_id="$CHECKPOINT_SMOKE_RUN" \
  -f transport_smoke_attempt="$CHECKPOINT_SMOKE_ATTEMPT"
```

The full workflow independently downloads and revalidates the originating
smoke evidence before admitting the nine measurement jobs; a summary JSON
alone is insufficient. A different source SHA requires a new smoke. After
the entire full run completes, set its `CHECKPOINT_RUN` and
`CHECKPOINT_ATTEMPT`, then capture separately:

```bash
bash examples/checkpoint_campaign_readback/capture-and-readback.sh \
  readback cntryl/midge "$CHECKPOINT_RUN" "$CHECKPOINT_ATTEMPT" \
  "$CHECKPOINT_SHA" /tmp/midge715-campaign-fresh \
  target/release/examples/checkpoint_campaign_readback \
  /tmp/midge715-campaign-report.json
```

Use new paths for every capture. The reader binds originating REST
run/attempt/jobs/artifact identities, raw ZIPs, extracted bytes, exact source,
compiled selectors, workload/PID canonical receipt and both final owners.
The capture helper retains an invalid nine-row report if evidence acquisition
fails. Uploads and canonical finalization preserve unsuccessful outcomes.

Each native process contributes one external measurement row. Preserve the
native diagnostic trust class and `TooFewSamples` assessment; three fresh
processes provide the declared operational repeat rule, not an invented
statistical confidence claim or revised native trust class.

## Separate hour qualification and conditional policy

All nine Tier 5 and five Tier 6 one-hour workloads receive diagnostic owner
snapshots for ingestion, explicit final flush, owned shutdown and actual
recovery/reopen. Original and reopened owners remain distinct. Those files
preserve forced costs and lifetime integrity, never infer byte totals from
directory growth, and introduce no timer, progress pulse or recovery-work
event. They do not alter or independently prove watchdog liveness. The hour
workloads are separate correctness/recovery qualification and do not replace
the controlled A/B/C gate; their observations make no cadence or ratio
release claim.

No cadence change is accepted by the measurement receipt. If no cell qualifies,
retain the current policy and publish the measured outcome. If a cell
qualifies, any later cadence proposal must preserve allocation, `AddSst` and
WAL-frontier journaling; forced cloud/recovery/DDL/administration,
compaction-before-GC and clean-shutdown checkpoints; and crash recovery after
prune and snapshot rename before journal truncation. A persistence failure
does not establish a hard journal-growth bound.

Acceptance of a later policy additionally requires actual crash/frontier
controls, at least 90% lower snapshot bytes in fixed-cardinality probes and
50% lower attributable checkpoint bytes/time in qualifying cells, with no
more than 10% stable-throughput or flush-p95 regression. Repeat matched release
measurements and obtain final hour correctness evidence at that changed
immutable SHA before accepting the policy.

## Recorded hosted outcome

[Run 37274074026](https://github.com/cntryl/midge/actions/runs/37274074026),
attempt **1**, measured source **`c8f0de80de7925c4e3fe92b665639e4615e53281`** on 2026-10-05.
Its prerequisite [smoke 37273556125](https://github.com/cntryl/midge/actions/runs/37273556125)
passed original-archive construction readback at that same source. All nine
new attempts are valid; no row from either earlier invalid campaign was used.

| Repeat | PID | Measured compactions | Snapshot / committed SST | Checkpoint / publication time | Checkpoint p95 bin (ms) | Miss |
| --- | ---: | ---: | ---: | ---: | --- | --- |
| A/r1 | 2186 | 99 | 0.837% | 28.296% | [32.00, 64.00) | time |
| A/r2 | 2199 | 100 | 0.819% | 12.729% | [2.00, 2.25) | none |
| A/r3 | 2409 | 98 | 0.924% | 18.613% | [1.50, 1.75) | none |
| B/r1 | 2432 | 151 | 13.534% | 19.519% | [1.00, 1.25) | payload |
| B/r2 | 2181 | 144 | 13.023% | 24.916% | [2.00, 2.25) | payload |
| B/r3 | 2420 | 150 | 13.401% | 21.733% | [1.25, 1.50) | payload |
| C/r1 | 2391 | 351 | 80.261% | 22.334% | [1.25, 1.50) | payload |
| C/r2 | 2461 | 351 | 80.261% | 21.416% | [1.00, 1.25) | payload |
| C/r3 | 2388 | 351 | 80.262% | 21.676% | [1.25, 1.50) | payload |

**The existing cadence misses the predeclared snapshot-payload target in B
and C (three of three repeats each).** A has one time miss and does not satisfy
the two-of-three rule. The complete campaign therefore meets the conditional
policy-investigation predicate; `cadence_change_accepted` remains false and
checkpoint cadence remains unchanged. This records the sustained measurement
requested by [#715](https://github.com/cntryl/midge/issues/715) and replaces the
APFS-only assurance boundary of [#670](https://github.com/cntryl/midge/issues/670).
A later policy still needs the matched measurements and crash/frontier controls
specified above.

The nine processes completed **1,376,256 strict acknowledged rows**, exact
point and full ordered-scan verification before and after same-path reopen,
and two actual owned shutdowns each. Ordinary measured publications are
230/460/921 per A/B/C repeat. Measured ingestion intervals span 4.70–10.61s.
The fixed continuous-flush probes and the separate fourteen one-hour workloads
serve different acceptance criteria. Native external rows retain their
`TooFewSamples` / invalid / untrustworthy diagnostics; the declared operational
repeat rule does not upgrade native statistical confidence.

The new capture exercised real busy boundaries: A/r1 sampled an active end
candidate, requested one 1ms pause (actual 1.060862ms), and captured idle on its
second sample after 4.138791ms, all inside measured ingestion. B/r1 and B/r3
sampled active warmup candidates and then idle; earlier warmup retries remain
outside measurement while each selected final query is included. Every chosen
Persistent endpoint and both final owners passed the unchanged integrity gate.

The [original readback JSON](evidence/checkpoint-715-readback.json) is copied
byte for byte, SHA256 **`443395a3c19159b8113b5da98762c5b65d4b8cc6a78fe73286470c0ae0563c92`**. It retains
original identities, captured paths, native diagnostics and all nine records;
its paths describe the originating capture. Original raw ZIPs and captures
are also retained, including both invalid campaigns. All seven exact measured
head [CI](https://github.com/cntryl/midge/actions/runs/37273523451) and
[CodeQL](https://github.com/cntryl/midge/actions/runs/37273520361) checks passed.
The results documentation was added after measurement; the measured source
identity is preserved.

Catalog mapping and final fourteen one-hour plus native cloud readback are
tracked through [#711](https://github.com/cntryl/midge/issues/711). Their final
acceptance uses the final merged source and remains separate from this local
checkpoint cost result.
