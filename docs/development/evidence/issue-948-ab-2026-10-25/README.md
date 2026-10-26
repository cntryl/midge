# Issue #948 grandparent cut A/B evidence

## Source and method

- Base: `e9a0d86b7be0c1b298ef8f546326c5893aedba04`
- Candidate: `e849fd96a5f528f794c313f6857a06c5b135c38b`
- Machine: Apple M5, 10 cores, macOS aarch64, Rust `1.99.0 (b940084d7 2026-09-28)`, AC power.
- The identical benchmark source SHA-256 was `b3773d6f893dd76a9f7bcdf80d99be3ea6ece5f0a40f32f83297debb252d1c52` in the main checkout and both detached worktrees.
- Workload: 2.7M base keys, 550K deterministic uniform updates, seed `0x948020261006`, production defaults. Each run verifies over 320 MiB of update-span overlap across at least two L2 SSTs, each smaller than the 320 MiB grandparent limit.
- Invocation: `cargo bench --locked --bench tier4_system_compaction_grandparent_cuts -- --profile release --samples 5 --warmup-samples 1 --no-progress`
- Runs alternated A/B. Separate target directories were used. `uptime.txt` captures load near each run.
- The initial B1 run had 8.03% throughput RSD. It is retained as `B1-initial-noisy.json`, excluded, and repeated as `B1.json`; the repeat passed at 1.00% RSD. All six reported pair artifacts have no diagnostics, gate trust, and throughput RSD below 1.31%.

## Results

The primary output metric is published compaction output SST bytes per logical update byte. Flush bytes are excluded; this is not whole-engine physical write amplification. The throughput guardrail permits no more than a 15% regression.

| Pair | Base throughput p50 (logical B/s) | Candidate throughput p50 | Throughput change | Base output ratio | Candidate output ratio | Base RSD | Candidate RSD |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A1/B1 | 12,877,217 | 10,844,076 | -15.79% | 6.174 | 10.816 | 1.16% | 1.00% |
| A2/B2 | 12,810,403 | 10,852,846 | -15.28% | 6.174 | 10.816 | 0.43% | 1.30% |
| A3/B3 | 12,723,531 | 10,745,873 | -15.54% | 6.174 | 10.816 | 0.40% | 1.01% |
| Median change | — | — | **-15.54%** | **6.174** | **10.816** | — | — |

The three candidate pairs exceed the 15% maximum throughput regression by 0.28–0.79 percentage points. Candidate output bytes are higher in each pair (about 856.7 MB vs 488.95 MB). Therefore this candidate does not qualify the preregistered #948 performance gate. The implementation remains integrated based on correctness coverage; this measurement does not support a performance benefit claim. Jev selected `does_not_qualify` with confidence 1.0; source and artifact arithmetic independently support the same disposition.
