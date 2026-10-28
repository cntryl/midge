# Issue #937 durability sampler evidence

## Source and environment

- Candidate source: `ed366ad6602ed9e4efe351861c5ade65de89c718`.
- Host: Apple M5, macOS 27.0.1, arm64, AC power.
- Compiler: rustc 1.99.0 (`b940084d7`).
- Sqrzl: `ghcr.io/sqrzl/sqrzl-emulator:latest`, image digest
  `sha256:3a89796f08aac55d970a987e544e42c752740d7ae7a2e24b9678d13a488b4dc8`.
- The Sqrzl service was healthy on localhost during the Tier 5 runs. Its local
  test credential is not recorded here.
- Post-run host snapshot: uptime 16:11, load averages 4.10/4.19/3.73. A
  per-run start snapshot was not captured; these runs are diagnostic evidence,
  not a quiet-window A/B campaign.

## Commands

The rows used five measured samples and one warmup. Tier 5 rows ran one at a
time against the existing local Sqrzl service. The local credential was
provided through `SQRZL_SECRET_ACCESS_KEY`.

```text
cargo check --locked --features failpoints,stress-soak --bench tier2_subsystem_hybrid_durability_lag --bench tier5_hybrid_durability_lag
cargo bench --locked --bench tier2_subsystem_hybrid_durability_lag -- --samples 5 --warmup-samples 1 --timeout-secs 1200 --no-progress --output-dir target/stress/issue-937-ed366ad-tier2
cargo bench --locked --features stress-soak --bench tier5_hybrid_durability_lag -- --workload tier5_hybrid_durability_lag::tier5_sqrzl_cloud_async_commit_vs_durable_lag_1_client --samples 5 --warmup-samples 1 --timeout-secs 1200 --no-progress --output-dir target/stress/issue-937-ed366ad-tier5-async1
cargo bench --locked --features stress-soak --bench tier5_hybrid_durability_lag -- --workload tier5_hybrid_durability_lag::tier5_sqrzl_cloud_strict_commit_vs_durable_lag_1_client --samples 5 --warmup-samples 1 --timeout-secs 1200 --no-progress --output-dir target/stress/issue-937-ed366ad-tier5-strict1
cargo bench --locked --features stress-soak --bench tier5_hybrid_durability_lag -- --workload tier5_hybrid_durability_lag::tier5_sqrzl_cloud_strict_commit_vs_durable_lag_4_clients --samples 5 --warmup-samples 1 --timeout-secs 1200 --no-progress --output-dir target/stress/issue-937-ed366ad-tier5-strict4
cargo bench --locked --features stress-soak --bench tier5_hybrid_durability_lag -- --workload tier5_hybrid_durability_lag::tier5_sqrzl_local_sync_commit_floor_1_client --samples 5 --warmup-samples 1 --timeout-secs 1200 --no-progress --output-dir target/stress/issue-937-ed366ad-tier5-local-floor
```

Each report directory contains timestamped cntryl-stress `JSON.gz`, CSV, text,
and Markdown artifacts. The uncompressed runner reports and `latest` copies
are retained in the local `target/stress` output. The Tier 2 reports have gate
trust; Tier 5 reports have diagnostic trust.

## Results

All Tier 2 rows passed all correctness checks over 10,240 transactions per
row. All four Tier 5 rows also completed 10,240 transactions with no failures,
timeouts, duplicates, dropped operations, or malformed provider observations.

| Row | Trust / quality | Throughput | Cloud lag p50 / p99 / max | Max sampler gap | Other observations |
| --- | --- | ---: | ---: | ---: | --- |
| Tier 2 cloud async, 1 client | gate / acceptable | 2.18K tx/s | 0 / 192 / 192 seq | 15 ms | probe p50/p99 21.9/25.0 ms; 0 probe timeouts |
| Tier 2 cloud strict, 1 client | gate / acceptable | 40.24 tx/s | 0 / 3 / 3 seq | 33 ms | commit p50/p99 25.3/50.4 ms |
| Tier 2 cloud strict, 4 clients | gate / acceptable | 142.76 tx/s | 0 / 12 / 12 seq | 30 ms | commit p50/p99 27.6/42.0 ms |
| Tier 2 local sync floor | gate / acceptable | 97.73 tx/s | n/a | n/a | commit p50/p99 10.0/13.1 ms |
| Tier 5 Sqrzl cloud async, 1 client | diagnostic / noisy (31.5% RSD) | 833.52 tx/s | 0 / 192 / 192 seq | 15 ms | 0 probe timeouts; 1.314 requests/commit; 3.067 upload bytes/committed byte |
| Tier 5 Sqrzl cloud strict, 1 client | diagnostic / acceptable | 15.66 tx/s | 3 / 3 / 3 seq | 34 ms | commit p50/p99 47.9/176.0 ms; 25.943 requests/commit; 312.630 upload bytes/committed byte |
| Tier 5 Sqrzl cloud strict, 4 clients | diagnostic / noisy (20.3% RSD) | 37.47 tx/s | 12 / 12 / 12 seq | 18 ms | commit p50/p99 85.3/379.1 ms; 10.782 requests/commit; 123.099 upload bytes/committed byte |
| Tier 5 local sync floor | diagnostic / acceptable | 95.25 tx/s | n/a | n/a | commit p50/p99 10.8/13.9 ms |

Cloud samplers targeted 10 ms. Achieved median period was 13 ms for the Tier 2
strict rows and all three Tier 5 cloud rows. The strict rows stayed below the
existing 40 ms maximum-gap guard. Tier 2's 4-client simulator row retained
gate quality; the equivalent Tier 5 row is noisy and remains diagnostic.
Simulator request/byte counters and native Sqrzl HTTP request/body-byte
counters have different scopes and are not compared as provider cost deltas.

## Pre-fix comparison and limits

The retained pre-fix artifacts use `7db3af174c9b00c120ceab43fc24ff725a77a935`.
The one-client strict run failed after its first sample at an 87 ms sampler
gap; the four-client strict run failed at 283 ms. Both exceeded the same 40 ms
guard and were inconclusive. On the candidate, the corresponding five-sample
runs completed with maximum gaps of 34 ms and 18 ms. This is a sampler-health
contrast, not a throughput A/B result: the pre-fix rows did not produce valid
performance samples, and this evidence does not claim a three-pair A/B delta.

Sqrzl results qualify only the local S3-compatible emulator. The Azure
streamed-checksum receipt test passed in the provider qualification suite;
Sqrzl S3 checksum multipart returned `501 NotImplemented`, and its GCS XML/JSON
front doors did not provide a unique resumable upload session in the prior
qualification attempt. Those provider capabilities remain unqualified here.
The results also do not establish RocksDB parity or production cloud latency.
