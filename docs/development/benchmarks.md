# Benchmarks

Midge benchmark suites live in `benches/` and use `cntryl-stress` across every
tier. Criterion-era guidance is obsolete here.

For regression thresholds, cloud and hybrid guardrails, and external LSM
comparison rules, see [Performance Targets](performance-targets.md).

## Running Benchmarks

Run one suite:

```bash
cargo bench --bench tier1_hotpath_bloom
cargo bench --bench tier2_subsystem_event_loop
cargo bench --bench tier4_ycsb_workload_a
```

Filter rows inside a suite:

```bash
cargo bench --bench tier2_subsystem_event_loop -- --workload 'direct_call'
```

List registered rows without running:

```bash
cargo bench --bench tier4_ycsb_workload_a -- --list
```

Choose a harness profile explicitly:

```bash
cargo bench --bench tier1_hotpath_bloom -- --profile smoke
cargo bench --bench tier4_ycsb_workload_a -- --profile default
cargo bench --bench tier4_ycsb_workload_a -- --profile release
```

Emit machine-readable output:

```bash
cargo bench --bench tier4_ycsb_workload_c -- --json
```

Stress artifacts are written under `target/stress/{suite}/` as `latest.json`,
`latest.md`, and `latest.txt` plus timestamped copies.

The bounded compaction qualification needs explicit dataset, path, and output
arguments. It is excluded from plain `cargo bench` by its existing `failpoints`
feature requirement; use the commands in
[Bounded Compaction Qualification](bounded-compaction-qualification.md).

The controlled checkpoint target requires the explicit `checkpoint-bench`
feature and dedicated Tier 4 workflow modes. Run the actual same-SHA
construction smoke before its nine fresh release processes, then retain the
native diagnostic trust and all invalid attempts during readback. See
[Checkpoint Write Amplification](checkpoint-write-amplification.md) for the
fixed cells, commands, gate and filesystem-issued byte limits. Measurement is
pending.

The Tier 2 read amplification target opens a local Engine with three overlapping
flushed SSTs. It records Engine point-read amplification and block-cache deltas
alongside point-only and mixed point/short-scan throughput. The metrics are
captured outside the measured window, and scans are excluded from the
point-read amplification denominator. Cache and bloom observations cover the
whole measured workload.
Its row names and results start a new baseline; old simulator numbers cannot
be compared with Engine throughput or block counts.

Tier benchmarks run in separate manually dispatched workflows. Tier 4 uses
`Benchmark Tier 4` (`bench-tier4.yml`) for 25 system scenarios and the checkpoint
modes, and `Benchmark Tier 4 YCSB` (`bench-tier4-ycsb.yml`) for 48 YCSB A–F
scenarios. The other tiers use `Benchmark Tier 1` through `Benchmark Tier 6`.
Tier 3 runs all registered Tier 3 targets on Ubuntu, Windows, and macOS; the
repeated flush-cycle row enables compaction so L0 slots can be recycled.

Tier 4, 5 and 6 workflows share a `profile` dropdown, defaulting to `standard`:

| Profile | Tier 4 scenario | Tier 5/6 workload |
| --- | --- | --- |
| `smoke` | One original short window or complete system cycle | 60 seconds |
| `standard` | 10 minutes | 10 minutes |
| `full` | 60 minutes | 60 minutes |

Each Tier 4 scenario has its own ARM matrix job. YCSB, streaming and backpressure
keep one engine through the selected measured window. Fixed-count system cases
repeat complete bounded cycles, retaining each cycle's checks; their long-run
clock includes setup and teardown and can exceed the window by the final cycle.
Their SST footprint observations sum the final footprints of completed cycles.
Smoke keeps the original timing boundaries and workload warmups. Checkpoint
workflow modes retain their separate fixed-cardinality campaign.

These workflow profiles use one native harness sample without native warmups.
They establish workload execution and correctness; a single sample does not
establish a statistical performance baseline. Scheduled Tier 5/6 runs use `full`.
Automation using their former `duration_seconds` input must switch to `profile`.

For a direct Tier 4 standard run, supply both the workload profile and the
matching native duration:

```bash
MIDGE_BENCH_PROFILE=standard cargo bench --bench tier4_ycsb_workload_a -- \
  --workload 'tier4_ycsb_workload_a::tier4_ycsb_a_memory_1_client' \
  --profile smoke --sample-duration-ms 600000 --timeout-secs 1500
```

Use `MIDGE_BENCH_PROFILE=full`, `--sample-duration-ms 3600000` and
`--timeout-secs 4500` for an hour. Direct invocations without the workload profile
keep their existing short Tier 4 windows. A fixed-cycle case rejects a native
measurement shorter than its selected standard/full window.

The old Destroyer scenarios now live in the opt-in `stress-soak` bench targets.
The feature gates both Tier 5 and Tier 6, so a plain `cargo bench` cannot start
an hours-long run. Local Tier 5 runs use one hour per workload/backend
concurrency sweep; Tier 6 uses one hour per composite soak. Short smoke runs
can set `MIDGE_TIER5_DURATION_SECS` or `MIDGE_TIER6_DURATION_SECS` to a smaller
positive value. Sqrzl protocol cases use the local emulator at
`MIDGE_STRESS_SQRZL_ENDPOINT` (default `http://127.0.0.1:9000`); they measure
those protocol surfaces and do not claim live-provider capacity. Namespace
setup reads `SQRZL_SECRET_ACCESS_KEY`, which must match the Sqrzl credentials;
the benchmark workflows set it for their local emulator jobs.

Tier 5/6 workflows set a 60-second `cntryl-stress` no-progress watchdog.
Only successful client operations advance workload progress; retryable
rejections and background database growth do not reset that watchdog during
client stages or shutdown. Workload artifacts under `target/midge-stress/`
include status, stage latency and saturation summaries, per-client snapshots,
phase timings, resource samples, shutdown results, and flush/reopen or
cloud-cache-loss verification summaries. `ResourceLimit` and `WriteStall` responses are
reported as saturation; repeated responses back off exponentially up to 256 ms
and the delay decays only after 32 successful operations. The accumulated
backoff is recorded separately. Sqrzl Tier 5 sweeps cover 1, 2, and 4
clients; Tier 6 uses four local clients and two Sqrzl clients. The resource
sampler follows database-file changes only during flush and recovery, so the
no-progress heartbeat reflects observed storage work in those phases.
Client snapshots are published atomically before the first operation,
periodically during the stage, and at completion or a terminal error. They
retain partial counters if the external watchdog abandons the worker;
`cntryl-stress` supplies the authoritative timeout receipt. Artifact collection
finalizes abandoned workload status from that receipt and preserves emulator
logs before teardown. Completed-stage totals and partial-stage counters are
reported separately.

Shutdown calls use a 45-second caller budget, leaving 15 seconds before the
workflows' 60-second watchdog to record the result and unwind. A stricter
watchdog selected on the command line can expire earlier. Runtime worker joins
continue to retain fencing after a caller timeout. Phase traces show those
joins without treating them as successful workload progress. Sqrzl
workloads use a 5-second cloud WAL seal window to batch objects during the long
sweeps. Data mismatches, failed recovery, a stalled workload, or the hard
benchmark deadline fail the run.

## Comparing Changes

Build registered benchmarks with `cargo bench --no-run`, then run the relevant
tiers on the same runner for both base and candidate revisions. For a performance
pull request, attach the commands, measurements, and summary. Add a target to
`Cargo.toml` before advertising it in this guide.

Run comparisons on an otherwise idle host. Concurrent tests, browser automation,
and filesystem indexing can change CPU scheduling and disk latency enough to
make variance reports unsuitable for optimization decisions. Repeat a noisy row
after the competing load ends before changing its workload or trust class.

## Tier Model

- Tier 1: hot-path microbenchmarks. Small, deterministic, allocation-aware
  rows that answer one tight latency question.
- Tier 2: subsystem rows. Fixed-operation batches that measure one subsystem
  surface under realistic internal work.
- Tier 3: system rows. Duration-based engine scenarios that exercise real
  storage/runtime behavior.
- Tier 4: workload rows. Duration-based end-to-end workloads such as YCSB.

Tier 3 owns clean open/drop lifecycle coverage over empty persisted state.
Tier 4 owns recovery and reopen measurements once persisted state (WAL,
manifest, flush, or compaction layout) changes the recovery question.

`cntryl-stress` derives the benchmark mode from the tier:

- Tier 1: micro timing
- Tier 2: fixed operations
- Tier 3-4: fixed duration

Do not force old fixed-op throughput semantics into Tier 3 or Tier 4 main rows.
For YCSB and other throughput workloads, the benchmark question is sustained
throughput over a fixed measured window.

## Trust Classes

Each row is classified in artifacts and reports:

- `gate`: semantically correct, stable enough to participate in perf gating,
  and free of blocking diagnostics
- `diagnostic`: intentionally tiny, capped, or otherwise useful but not a perf
  gate
- `experimental`: still visible, but the measured question or normalization
  still needs follow-up
- `invalid`: known-bad semantics or blocking diagnostics

Only `gate` rows drive performance quality and regression gates. Non-gate rows
still run and still appear in the reports.

## Unit Semantics

Every batch-style row should declare the measured logical unit so the report
shows the question directly instead of a bare `ns/op`.

Use these metadata and parameter keys:

- `measurement_mode`: derived by the harness; `micro`, `fixed_ops`, or
  `duration`
- `logical_unit`: what one counted operation means, for example
  `engine_put_commit`, `sst_point_lookup`, or `block_byte`
- `items_per_batch` and `lookups_per_batch` when the counted operation is a batch
- `batch_per_logical_operation = 1` for a row whose counted unit is one batch;
  `cntryl-stress` requires an explicit normalization basis for batch rows
- `operations_per_client`
- `validated_micro`
- `trust_class`

Examples:

- `800 ns/block` for a Tier 1 compression row counted by blocks
- `31.2 us/transaction`
- `185.4 Kops/s` with `question=logical_unit=transaction, mode=duration`

The displayed unit must match the measured count. A row that counts 100 batches
of 1,000 lookups reports time per batch and records `lookups_per_batch = 1000`
and `batch_per_logical_operation = 1`.
To report time per lookup, it must count 100,000 lookups instead. Metadata alone
does not divide the measured time.

The corrected Tier 1 batch and compression units change report labels and
comparison meaning. Start a new saved baseline for those rows before using them
to judge a code optimization.

## Authoring Rules

### Tier 1

- Keep setup out of the measured closure.
- Precompute data, buffers, and lookup windows.
- Vary inputs when the row is small enough to risk dead-code elimination.
- Accumulate observable outputs with `black_box`.
- Use `validated_micro = "true"` only after anti-DCE is explicit.
- Intentionally tiny rows should default to `trust_class = "diagnostic"`.

### Tier 2

- Count the actual logical work completed by each batch.
- Set `logical_unit` on every `measure_batch` row.
- Prefer singular units such as `cache_block_access` or `transaction`, not
  vague names like `batch`.

### Tier 3-4

- Measure sustained behavior over a fixed duration.
- Use `ctx.measure_batch(...)` when the harness owns the timing window.
- Use `ctx.record_external(...)` or local helpers built on it when the
  benchmark must own concurrency or wall-clock timing directly.
- Main throughput rows should not mix fixed-op and duration semantics inside one
  workload family.

## Profiles

- `smoke`: fastest diagnostic pass
- `default`: normal day-to-day benchmark profile
- `lab`: longer exploratory runs
- `release`: release-quality gate profile

`STRESS_PROFILE` can set the default profile for local runs.

## Interpreting Reports

Human and markdown reports now surface:

- the metric unit
- the logical unit
- the normalization basis
- the measurement mode
- the trust class

Tier 4 YCSB A–F rows also record measured-window write stalls, WAL appends,
cache hits and misses, SST candidate checks, data-block reads, and cloud WAL
upload outcomes. `cache_hit_ratio` is a 0–1 ratio. These observations are
captured outside the timed window; asynchronous cloud WAL upload failures also
contribute to the row's correctness failures.

That output is the truth surface for deciding whether a row should stay a gate,
move to diagnostic, or be rewritten.
