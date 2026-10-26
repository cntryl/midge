# Issue #905 Strict hot-key coalescing evidence

## Scope and source

This measures the issue's 16-writer Strict shared-hot-key and disjoint rows with three alternating pairs:

- Base runtime: `e9a0d86b7be0c1b298ef8f546326c5893aedba04`
- Candidate runtime: `e849fd96a5f528f794c313f6857a06c5b135c38b`
- Release profile, five measured samples and one warmup, `cntryl-stress` workload filter `tier2_durability_commit_sync_local_16_writers*`.

The candidate predates the benchmark-only hot-key row commit `c639ca3e811cecd1e4883dd95ef0885adf4e5e97`. The same shared-key row and `avg_txn_records_per_append` observation were added to the base benchmark source for this comparison; no base production code was changed. The candidate used the benchmark source from c639ca3e. The base source omits ledger sync observations because those metrics do not exist at the base SHA. Source SHA-256 values: base instrumented benchmark `142b1a9f7bcbe7df7b5a06a7d0bbfb2c1d4f80dfd9808b0b7af3210bde1dc67f`; candidate benchmark `8e49a403889de98a10af8959ec941198fd4b3deefbee57212cd00e622c10b839`.

The candidate's added ledger observations are taken outside the timed transaction interval. Candidate ledger file sync attempts/successes are reported in the raw artifacts; directory syncs were zero. The base cannot report those counters. This is an issue-specific local comparison, not a complete device-wide fsync count.

## Results

Throughput values are summary medians in transactions per second. `records/append` is the mean transaction-record count per WAL append. The target is candidate hot-key throughput at least 80% of candidate disjoint throughput, with `records/append >= 7.0`; the A/B pair deltas are reported as requested but do not establish a general speedup claim.

| Pair | Base disjoint tx/s | Candidate disjoint tx/s | Delta | Base hot tx/s | Candidate hot tx/s | Delta | Base hot/disjoint | Candidate hot/disjoint | Candidate hot records/append |
|:---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| A1/B1 | 1851.138 | 1472.911 | -20.43% | 123.768 | 1475.341 | +1092.02% | 6.69% | 100.16% | 15.907 |
| A2/B2 | 1903.505 | 1504.040 | -20.99% | 124.866 | 1501.032 | +1102.11% | 6.56% | 99.80% | 15.845 |
| A3/B3 | 1907.018 | 1492.279 | -21.75% | 119.560 | 1496.680 | +1151.82% | 6.27% | 100.29% | 15.876 |
| Median | — | — | -20.99% disjoint | — | — | +1102.11% hot | 6.56% | 100.16% | 15.876 |

All six counted invocations passed their intended Tier 2 quality gates. Candidate row RSDs were at most 2.6%. The initial B3 had 10.1% RSD on the disjoint row and is retained in `B3-initial-noisy/` as diagnostic; the passing rerun is the counted B3 pair. All base rows had acceptable quality, with at most 4.4% RSD.

The candidate meets AC1 and AC4: every candidate hot/disjoint ratio is above 99.8%, and every candidate average records-per-append value exceeds 15.84. The base demonstrates why the hot-key row matters: it reached only 6.27–6.69% of disjoint throughput and one record per append.

## Raw artifacts and environment

Each run directory contains the exact published CSV, JSON, Markdown and text artifacts copied from the harness `latest` files. Base/candidate targets and output directories were separate. The invocations reported AC power. Uptime/load snapshots were captured for A1, A2, A3, B3 and its rerun; B1 and B2 did not get individual snapshots, so their local load context is not independently recorded. Observed load averages around the campaign ranged roughly 1.7–2.7. This evidence is local to the measured machine and does not qualify production or another platform.
