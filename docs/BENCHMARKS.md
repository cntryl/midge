# Benchmarks

This repo uses [Criterion](https://bheisler.github.io/criterion.rs/book/) for micro- and subsystem-level benchmarking.

## Running benchmarks

- Run the whole suite:

  ```bash
  cargo bench
  ```

- Run a single benchmark target:

  ```bash
  cargo bench --bench tier1_hotpath_api
  ```

- Run with filters (Criterion):

  ```bash
  cargo bench --bench tier1_hotpath_api -- "get"
  ```

## Benchmark layout

Benchmarks live in `benches/` and are organized by "tiers":

- **Tier 1 (hotpath)**: tight inner-loop microbenchmarks of critical components.
- **Tier 2 (subsystem)**: measures end-to-end behavior for a subsystem.
- **Tier 3/4 (system/workloads)**: larger scenarios closer to real workloads.

Common Criterion configuration helpers live in `benches/criterion_helper.rs`.

## Rules (important)

To keep results stable and meaningful:

- Precompute all data outside `b.iter(|| ...)`.
- No allocations, I/O, or RNG inside the hot loop.
- Use deterministic seeds when randomness is required.
- Use `black_box` on inputs/outputs when relevant.
- Prefer `group.sampling_mode(SamplingMode::Flat)` and `group.throughput(...)` (see `benches/criterion_helper.rs`).

## Interpreting results

- Prefer comparing **relative changes** on the same machine.
- If numbers move a lot run-to-run, look for:
  - warmup effects (disk cache / allocator)
  - background system load
  - cloud/local filesystem variability

## Common workflows

- Validate correctness first:

  ```bash
  cargo test
  ```

- Then benchmark targeted code:

  ```bash
  cargo bench --bench tier1_hotpath_memtable
  ```

## Notes for contributors

- If you introduce a new hotpath, consider adding a Tier 1 benchmark.
- Keep benchmark names descriptive and stable; they become part of long-term performance tracking.
