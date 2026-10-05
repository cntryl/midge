# Checkpoint Write Amplification

**Status: measurement pending.** The controlled hosted construction smoke,
nine-repeat release campaign, and final fourteen one-hour workloads have not
yet qualified this change. Local instrumentation and reader tests establish
their tested contracts; they do not supply those measurements. Checkpoint
cadence remains unchanged.

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

Add actual hosted run/attempt IDs, exact measured SHA, all nine row outcomes,
raw readback links and final hour evidence here once available. Until then,
the result remains **measurement pending**.
