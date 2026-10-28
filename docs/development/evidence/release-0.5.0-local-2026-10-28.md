# 0.5.0 local qualification evidence, 2026-10-28

This is a source-specific checkpoint, not a release-ready declaration. The
owning gates remain #914, #929, #937 and the #935 release checklist. The owner
accepted local evidence and requested unchanged workflows. Raw JSON, uptime
and image observations remain under `target/stress/release-050-*`; only this
concise summary is committed. Preserve invalid attempts alongside successful
reports. No external-engine parity or live-cloud result is claimed.

## Environment and source attribution

Apple M5, 10 cores, macOS 27.0.1/aarch64, APFS, mains power;
`rustc 1.99.0 (b940084d7 2026-09-28)`. Native rows use Sqrzl `latest`, resolved
to `sha256:da1a2a731fe6de71ce8241e3d9fda9ae48dec6015f8b531f4f3129e4ca9969a8`.
The provider endpoint is the local Compose fixture, not a real cloud account.

Reports retain their actual source SHA:

- Recovery: `96aa05b541b33c066596bc32247b54d57488fab2`.
- Simulator strict and native async/strict-four:
  `b8e30ab5e26c8549f6414d7fb1474b90283e55fb`.
- Native local floor: `f6846d724533bd6069a5b093fd20e0e677fba02d`.

The reviewed `b8e30ab5` tree and PR #1008 merge `f6846d72` tree are identical
(`8a19e3cbbb8af7a89b702d12284152e028023365`). This does not relabel earlier
reports as later release-candidate measurements. PR #1009 adds conditional
mutation qualification tests at `6f522167c97e10ac96f2bd75bffb2ea2377887e1`.

## Hybrid durability and provider costs

Each successful row has five measured samples, one warmup, 2,048 transactions
per measured sample and 10,240 completed transactions with no errors. Native
rows have diagnostic trust; simulator rows use the gate trust declared by the
existing benchmark. Throughput is the sample median. Commit p99 comes from
the aggregate transaction latency distribution. Lag columns are the medians
of each sample's p50/p99/max sequence lag, with the largest sampler gap across
all samples reported separately.

| Row | Transactions/s | Commit p99, ms | Lag p50/p99/max, sequences | Largest sampler gap, ms |
| --- | ---: | ---: | --- | ---: |
| Simulator strict, one writer | 40.66 | 39.06 | 0 / 3 / 3 | 22 |
| Simulator strict, four writers | 167.96 | 52.69 | 0 / 12 / 12 | 24 |
| Sqrzl async, one writer | 911.47 | 5.21 | 0 / 192 / 192 | 13 |
| Sqrzl strict, four writers | 61.61 | 191.89 | 12 / 12 / 12 | 29 |
| Native-suite local sync floor | 76.29 | 38.54 | not a cloud row | not sampled |

The sampler guard remains 40 ms. Configured/observed periods are 11–12 ms
for these runs. Native strict-four throughput RSD is 9.82%; async is 0.87%.
These diagnostic rows do not establish sub-millisecond commit latency.

| Native row | Requests/commit, median (maximum) | PUTs/commit, median | Request-body bytes/logical committed bytes, median (maximum) |
| --- | ---: | ---: | ---: |
| Async, one writer | 1.314 (1.314) | 0.053 | 3.067 (3.069) |
| Strict, four writers | 6.968 (12.426) | 0.780 | 77.957 (144.556) |

Native counters cover actual engine HTTP attempts/body bytes after column-family
setup through drain and shutdown, including lease/control/background work and
retries. Headers are excluded. This is not a cloud billing or egress estimate.
Simulator upload bytes count stored WAL bytes and are not equivalent to native
request-body bytes; do not compare their ratios as an efficiency delta.

Commands use the existing targets and row-name glob, with fixture credentials
provided through `SQRZL_SECRET_ACCESS_KEY`:

```text
cargo bench --locked --features stress-soak --bench tier5_hybrid_durability_lag -- --workload '*cloud_async_commit_vs_durable_lag_1_client' --samples 5 --warmup-samples 1 --timeout-secs 1200 --no-progress --output-dir target/stress/release-050-sqrzl-async1
cargo bench --locked --features stress-soak --bench tier5_hybrid_durability_lag -- --workload '*cloud_strict_commit_vs_durable_lag_4_clients' --samples 5 --warmup-samples 1 --timeout-secs 1200 --no-progress --output-dir target/stress/release-050-sqrzl-strict4
cargo bench --locked --features stress-soak --bench tier5_hybrid_durability_lag -- --workload '*local_sync_commit_floor_1_client' --samples 5 --warmup-samples 1 --timeout-secs 1200 --no-progress --output-dir target/stress/release-050-sqrzl-local-floor
cargo bench --locked --bench tier2_subsystem_hybrid_durability_lag -- --workload '*cloud_strict_commit_vs_durable_lag_1_client' --samples 5 --warmup-samples 1 --timeout-secs 1200 --no-progress --output-dir target/stress/release-050-hybrid-strict1-retry
cargo bench --locked --bench tier2_subsystem_hybrid_durability_lag -- --workload '*cloud_strict_commit_vs_durable_lag_4_clients' --samples 5 --warmup-samples 1 --timeout-secs 1200 --no-progress --output-dir target/stress/release-050-hybrid-strict4
```

Earlier simulator strict-one and native strict-one attempts had sampler gaps
of 57 ms and 42 ms respectively; those attempts are invalid/inconclusive.
The native strict-one retry remains pending at this checkpoint. The simulator
strict-one retry above is valid; a failed attempt is not erased by a retry.

## Recovery

Three runs, five measured samples and one warmup each:

```text
cargo bench --locked --bench tier4_system_recovery_throughput -- --samples 5 --warmup-samples 1 --no-progress --output-dir target/stress/release-050-recovery-run2
cargo bench --locked --bench tier4_system_recovery_throughput -- --samples 5 --warmup-samples 1 --no-progress --output-dir target/stress/release-050-recovery-run3
cargo bench --locked --bench tier4_system_recovery_throughput -- --samples 5 --warmup-samples 1 --no-progress --output-dir target/stress/release-050-recovery-run4
```

| Row | Run 2 | Run 3 | Run 4 | Interpretation |
| --- | ---: | ---: | ---: | --- |
| 10k inventory validation, ms per inventory | 446.903 | 466.460 | 461.542 | Stable diagnostic phase measurement |
| Cloud-simulated reopen after flush, reopens/s | 8.454 | 7.290 | 7.169 | Cross-run throughput spread exceeds 15%; performance inconclusive |
| Local reopen after flush, reopens/s | 17.842 | 14.928 | 15.156 | Cross-run throughput spread exceeds 15%; performance inconclusive |
| Cloud-simulated reopen after compaction, reopens/s | 8.383 | 7.184 | 7.221 | Cross-run spread exceeds 15%; run 3 also has noisy quality |
| Local reopen after compaction, reopens/s | 17.987 | 15.199 | 17.826 | Cross-run throughput spread exceeds 15%; performance inconclusive |

Every correctness assertion passed. The 10k fixture times production manifest
SST inventory validation with a 1 ms simulated HEAD delay and verifies all
10,000 HEAD/size validations and retained entries. It is not 10,000-SST
end-to-end `Engine::open` or a native-provider startup result. Reopen rows use
the filesystem cloud simulator and repeat full engine opens over their smaller
fixtures. Capped-work throughput diagnostics are intentional for these rows.
The earlier loaded run 1 remains diagnostic only and is not folded into these
results. Hosted load-guard failures likewise remain invalid environment runs.

## Metadata common-path scope (#914)

The two-request proof is a warm metadata-publication operation with a valid
claimed lease/pointer identity, cached control-file fingerprints, and validated
checksum receipts: one immutable edit PUT and one conditional pointer PUT,
with zero GETs including range reads. It is not a promise of two total HTTP
requests per user transaction. Cold authority, snapshots, missing receipts,
unknown outcomes, heartbeat/control work and retention can add requests.

## Remaining gates

- Native strict-one healthy sampler run and owning #937 evidence disposition.
- Complete #929 matrix, including YCSB A–F; preserve historical sources and
  inconclusive rows rather than inventing final-head parity.
- Four machine-hours of `engine_differential` and registered-target smokes on
  the candidate; no elapsed budget is claimed here.
- The final release checklist, exact-head CI, version/package/operator docs
  and reviewed promotion. No release tag or publication is claimed.
