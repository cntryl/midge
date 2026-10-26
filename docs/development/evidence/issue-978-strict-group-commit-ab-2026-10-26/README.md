# Issue #978 Tier 4 strict group commit A/B evidence

## Source and method

- Base: `9f24a58869cc21cb26642fca73de479855c233e9`
- Candidate: `1a161dcb248b98e82b71f537494b89bc06ec6f74`
- Machine: Apple M5 (`Mac17,2`), 10 cores, macOS aarch64, Rust `1.99.0 (b940084d7 2026-09-28)`, AC power.
- Identical benchmark source SHA-256: `8f4a8bc7cd1f17c805e383f6a07fbd2d4d71a749a0971d3eaa11f41392954c39`.
- Workload: registered `tier4_system_strict_group_commit`; 16 writers, 8 flush waves, 32,768 strict transactions per complete cycle, background compaction, clean shutdown, reopen, digest verification, and SST footprint checks.
- Invocation in each worktree: `CARGO_TARGET_DIR=<separate-target> cargo bench --locked --bench tier4_system_strict_group_commit -- --samples 5 --warmup-samples 1 --no-progress`.
- Runs alternated A/B. Base and candidate used separate Cargo targets; the accidental shared-target candidate attempt was interrupted before using results. Each retained artifact reports `163840/163840` operations completed across five samples, zero failures/timeouts, and zero write-stall recoveries.
- The benchmark declares `role = diagnostic` and `diagnostic_reason = strict_group_commit_promotion_probe`. Artifacts therefore have diagnostic trust, despite acceptable quality and zero diagnostics. An initial forced `--profile release` run is excluded because the harness correctly returned `QualityFailed` with no intended gate rows.
- No per-run uptime snapshots were retained. The source range covers the local prune and direct-rotation changes; CloudAsync seal integration (`6a417384`) is outside this local-only benchmark and remains supported by its separate boundary matrix.

## Results

The paired metric is complete-system throughput in transactions per second. Deltas use exact artifact p50 values.

| Pair | Base p50 tx/s | Candidate p50 tx/s | Change | Base RSD | Candidate RSD |
| --- | ---: | ---: | ---: | ---: | ---: |
| A1/B1 | 1167.832 | 1236.817 | +5.91% | 2.27% | 5.41% |
| A2/B2 | 1157.899 | 1164.929 | +0.61% | 2.33% | 0.72% |
| A3/B3 | 1191.146 | 1113.514 | -6.52% | 2.02% | 2.37% |
| Median pair change | — | — | **+0.61%** | — | — |

The worst pair was -6.52%, within the documented Tier 4 15% sustained-throughput investigation guardrail. The median pair change is not a regression. One candidate run had 5.41% RSD; the remaining five runs were at or below 2.37%. Jev selected the narrow AC3 no-regression disposition (confidence 0.98), while all qualification boundaries above remain explicit. The benchmark data support the measured local Strict workload statement only; they do not qualify CloudAsync seal performance or convert the diagnostic row into gate-trusted evidence.
