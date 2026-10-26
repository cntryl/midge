# Issue #940 Strict fsync A/B evidence

## Scope

This is the preregistered AC3/AC6 comparison in `docs/development/performance-targets.md`:

- Base: `e9a0d86b7be0c1b298ef8f546326c5893aedba04`
- Candidate: `e849fd96a5f528f794c313f6857a06c5b135c38b`
- Benchmark: `tier2_subsystem_durability_commit_latency`
- Local Strict 16- and 64-writer rows, release profile, five measured samples and one warmup.

The comparison covers the integrated commit range and is not isolated causal attribution. Base benchmark source SHA-256 is `cb4a804b3c1d5d625adf87a49d9c1885441f12e1b1d122b4dc7536f8654c61de`; candidate benchmark source SHA-256 is `19c8d8ba5fbd3bae599a9f465d38188fd1428cea827a58c1c5c086a6389c03cc`. The candidate adds ledger sync observations. Base artifacts do not expose ledger file/directory counters, so those base measurements are unavailable; candidate counts are reported separately. WAL writer fsync counts are not total physical sync counts.

## Run status

A1/B1 and A2/B2 are complete accepted pairs. The initial A3 failed the variance gate on the 16-writer row (15.3% RSD) and is retained in `A3/` as a noisy attempt. The A3 repeat has acceptable 16/64 rows, but the complete invocation failed its overall quality gate because the unrelated single-writer row had 14.0% RSD; it is retained in `A3-repeat/` as diagnostic. B3 passed all benchmark quality gates. Thus this run set does not satisfy the preregistered three-pair quality requirement; A3-repeat/B3 values are shown only as diagnostic observations.

Each run directory contains the published CSV, JSON, Markdown and text artifacts. The `latest` artifacts were copied byte-for-byte from the harness output. Runs used separate base/candidate Cargo target directories and isolated output directories.

## Pair results

Values below use each run's summary median. Throughput is transactions per second. Commit p99 is `latency_distribution.p99` in nanoseconds. `commits/fsync` is the harness observation. Candidate ledger file and directory sync counters are reported from the candidate artifacts; base counters were not emitted by the base benchmark source.

| Writers | Pair | Base throughput | Candidate throughput | Throughput delta | Base commits/fsync | Candidate commits/fsync | Commits/fsync delta | Base commit p99 ns | Candidate commit p99 ns |
| ---: |:---:| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 16 | A1/B1 | 1843.161 | 1461.685 | -20.70% | 15.6935 | 15.8453 | +0.97% | 16,711,680 | 20,054,016 |
| 16 | A2/B2 | 1849.376 | 1494.045 | -19.21% | 15.7236 | 15.8453 | +0.77% | 16,056,320 | 12,779,520 |
| 16 | A3-repeat/B3 diagnostic | 1859.180 | 1355.466 | -27.09% | 15.7236 | 15.8453 | +0.77% | 16,908,288 | 21,626,880 |
| 64 | A1/B1 | 7019.810 | 5663.998 | -19.31% | 63.3810 | 63.5039 | +0.19% | 11,075,584 | 13,828,096 |
| 64 | A2/B2 | 7313.822 | 5783.209 | -20.93% | 63.5039 | 63.5039 | 0.00% | 10,551,296 | 15,007,744 |
| 64 | A3-repeat/B3 diagnostic | 6982.305 | 5124.443 | -26.61% | 63.2587 | 63.3810 | +0.19% | 13,172,736 | 22,413,312 |

Across the two complete pairs, the median commits/fsync gains are about 0.87% (16 writers) and 0.10% (64 writers), below the 10% requirement. Throughput regresses 19.21–20.70% at 16 writers and 19.31–20.93% at 64 writers, outside the 5% allowance. The diagnostic third pair has the same direction and larger throughput regressions. Commit p99 is mixed and does not remedy the failed throughput and commits/fsync gates.

Candidate ledger observations (file sync attempts/successes; directory sync attempts/successes):

| Run | 16 writers | 64 writers |
|:---:|:---|:---|
| B1 | 517/517; 0/0 | 516/516; 0/0 |
| B2 | 517/517; 0/0 | 516/516; 0/0 |
| B3 | 517/517; 0/0 | 517/517; 0/0 |

These results do not meet AC3/AC6 and do not qualify a performance benefit. Issue #940 remains open for these acceptance gates. No production change is justified by this benchmark alone.

## Environment notes

All runs reported AC power. A1 began at 2026-10-26 04:36:45 UTC; B1 at 04:44:07; A2 at 04:52:56; B2 at 04:58:47; A3 at 05:05:56; A3-repeat at approximately 05:12 UTC; and B3 at approximately 05:18 UTC. Host uptime/load snapshots were recorded interactively for the starts/transition points; the machine remained under background load (load averages generally around 1.4–2.9). These are local diagnostic measurements, not provider or production qualification.
