# Tier 2 compaction CPU baseline attempt

Source: `c2929e8e81d06691fb765882bfeffcdcc99b45d9` (2026-10-26).
The existing `tier2_subsystem_compaction` row writes eight 100,000-row SSTs
outside the measured interval, then measures one production `compact_all`
operation. Its CPU counter is process CPU time, including worker threads.

Only run 1 met the harness quality gate. Runs 2–4 at the default
`operations_per_sample=1` were noisy, with RSDs of 17.05%, 29.50%, and 35.31%.
A separate trial using the harness-supported `--operations-per-sample 3`
override was also noisy at 41.39% RSD. All artifacts passed correctness. The
override was kept separate and did not improve quality; no settings or quality
thresholds were changed for the default series. This campaign is inconclusive
and does not establish a stable absolute compaction CPU baseline.

| Run | Configuration | Quality | Median throughput | RSD | SHA-256 |
| --- | --- | --- | ---: | ---: | --- |
| 1 | default, 1 operation/sample | acceptable | 25,783.35 MB/s | 4.23% | `a5ac2b205ac6f72b1111ac3ce9246ecb10ce8add326dc4ba73ff2b76cd9cb80a` |
| 2 | default, 1 operation/sample | noisy | 27,010.55 MB/s | 17.05% | `3bf818152e966e7171771904783bd806bb213631a6b934e040683ba42a7a11ce` |
| 3 | default, 1 operation/sample | noisy | 25,841.18 MB/s | 29.50% | `22b3d2366d8055e35f051137b31c2650da0a8e6538ed29f1d2ea69d4ba03d1b1` |
| 4 | default, 1 operation/sample | noisy | 20,600.86 MB/s | 35.31% | `ca8edad8f7c58411a12c0e30b1d5491e0bb97f8d7b5ea02610b66ea82f385bca` |
| trial | override, 3 operations/sample | noisy | 25,714.29 MB/s | 41.39% | `a589a23292bcceac3d46dc97c806214ed6fefcbd30f16c3684f19e4a5157c5b5` |

Uptime before and after each attempt and the raw harness JSON are retained.
The default row remains gate-trust only for the one acceptable run; noisy
artifacts carry diagnostic trust. No A/B or stable-baseline conclusion is
claimed.
