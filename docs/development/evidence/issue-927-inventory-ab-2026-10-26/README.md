# Issue #927 10,000-SST recovery inventory A/B

## Source and method

- Base: `b1f36ecc5a38ab826e25124c3ebbbe95a3ec8bfc` (serial manifest inventory).
- Candidate: `aca68bedc2310ffaea816d0d661a825ca022e90c` (32 bounded inventory workers and completion-order reconciliation).
- This merged candidate commit also contains #915 receipt changes. The measured workload exercises only cold-start manifest HEAD inventory; it does not upload SSTs or exercise multipart publication.
- The existing `tier4_system_recovery_throughput` Cargo benchmark was extended with `tier4_recovery_cloud_inventory_10k`. It seeds 10,000 valid SST objects in the filesystem object-store mock before timing, then runs the production `CloudStartupRecovery::validate_manifest_ssts_in_store` path. Each mock HEAD response has 1 ms simulated delay. The benchmark asserts that all 10,000 HEADs and size validations complete, every entry remains selected, and no mismatch occurs.
- Each run used release mode, five measured samples, one warmup, the same Apple M5/macOS arm64 host, AC power, and rustc 1.99.0. Fixture seeding is outside the explicit timed interval. Harness wall-clock includes fixture setup; the comparison uses the explicit per-operation measurement (`ns_per_op`) and its corresponding throughput.
- The same benchmark and fixture overlay was applied to both exact source revisions. Copies of the measured benchmark and fixture implementation are retained beside the raw artifacts.

## Results

Throughput and elapsed time are per-sample medians. Elapsed time is for the 10,000-object validation only. All six rows report `quality=acceptable`; harness row RSD ranges from 0.39% to 2.15%.

| Pair | Base SST/s | Base time | Candidate SST/s | Candidate time | Speedup |
| --- | ---: | ---: | ---: | ---: | ---: |
| A1/B1 | 578.50 | 17.286 s | 18,153.70 | 0.551 s | 31.38x |
| A2/B2 | 577.66 | 17.311 s | 18,103.20 | 0.552 s | 31.34x |
| A3/B3 | 577.90 | 17.304 s | 18,160.42 | 0.551 s | 31.42x |
| Median | — | — | — | — | **31.38x** |

This exceeds AC1's 10x provider-mock target. Jev favored treating AC1 as met while preserving the evidence boundary (confidence 0.76).

## Harness boundary and remaining acceptance

The harness marks this fixed 10,000-operation throughput row `trust_class=diagnostic` with `flat_or_capped_throughput`, because each sample completes the same fixed work and has very low throughput variance. The row-level quality remains acceptable. Each filtered command exits nonzero because this diagnostic-only benchmark invocation has no intended gate rows; this is not a green Tier 4 suite result. The measurements are retained as issue-specific A/B evidence, with that trust boundary explicit.

The broader acceptance of #927 remains open for Sqrzl/provider qualification of late or unknown mutation effects and the required provider recovery matrix. Existing source tests cover bounded inventory concurrency, completion reordering, failed-probe watchdog behavior, transient immutable upload retry/readback, and no-blind-replay mutation cases; those tests do not replace Sqrzl qualification. Per-run uptime-before snapshots and a campaign-end snapshot are retained; an after snapshot was not captured for each command because the diagnostic-only harness returned nonzero.
