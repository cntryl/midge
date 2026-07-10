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
- `items_per_logical_operation`
- `lookups_per_logical_operation`
- `operations_per_client`
- `validated_micro`
- `trust_class`

Examples:

- `24.9 ns/block_byte`
- `31.2 us/transaction`
- `185.4 Kops/s` with `question=logical_unit=transaction, mode=duration`

If one measured call completes a batch, either count the true logical work
directly or declare the normalization basis explicitly.

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

That output is the truth surface for deciding whether a row should stay a gate,
move to diagnostic, or be rewritten.
