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

The Tier 2 read amplification target opens a local Engine with three overlapping
flushed SSTs. It records Engine point-read amplification and block-cache deltas
alongside point-only and mixed point/short-scan throughput. The metrics are
captured outside the measured window, and scans are excluded from the
point-read amplification denominator. Cache and bloom observations cover the
whole measured workload.
Its row names and results start a new baseline; old simulator numbers cannot
be compared with Engine throughput or block counts.

Tier benchmarks run in separate manually dispatched workflows named
`Benchmark Tier 1` through `Benchmark Tier 6`. Tier 3 runs all registered Tier
3 targets on Ubuntu, Windows, and macOS; the repeated flush-cycle row enables
compaction so L0 slots can be recycled.

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

Tier 5/6 runs set a 60-second `cntryl-stress` no-progress watchdog. Workload
artifacts under `target/midge-stress/` include status, stage latency and
saturation summaries, resource samples, and flush/reopen or cloud-cache-loss
verification summaries. `ResourceLimit` and `WriteStall` responses are
reported as saturation; repeated responses back off exponentially up to 16 ms,
and the accumulated backoff is recorded separately. Sqrzl workloads use a
5-second cloud WAL seal window to batch objects during the long sweeps. Data
mismatches, failed recovery, a stalled workload, or the hard benchmark deadline
fail the run.

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
