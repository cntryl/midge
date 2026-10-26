# Tier 4 local recovery baseline evidence

Source: `f726628ce0ebe5e7acfa05619a31833d36968f5a` (2026-10-26).
Command:
`cargo bench --locked --bench tier4_system_recovery_throughput -- --workload tier4_recovery_reopen_after_flush_local --no-progress --require-quiet-env`.
The benchmark prepares a local flushed fixture outside measurement, then
measures ten engine reopens per fixed-duration sample. The host was an Apple
M5, 10-core macOS arm64 machine on AC power. This benchmark is explicitly
`role=diagnostic`; three clean runs are contextual measurements, not gate
qualification.

Runs 1, 3, and 4 are acceptable, with a median of 16.94 reopens/s and 6.58%
spread across their run medians. Run 2 is retained and excluded because it was
classified noisy at 11.74% within-run RSD. All runs passed correctness; no
diagnostics were emitted for selected runs.

| Run | Quality | Median reopens/s | Within-run RSD | SHA-256 |
| --- | --- | ---: | ---: | --- |
| 1 | acceptable | 16.55 | 5.14% | `002d358ea3fdebd5c53692bfd198be8fee19f1d3053477e5f6c2df0a305ea684` |
| 2 | noisy; excluded | 16.45 | 11.74% | `ecd874ebc5fb4335e34a25fda7e15e21f25fed416e58fa5a664ad52455280cb4` |
| 3 | acceptable | 16.94 | 8.15% | `7afaa4c8d8fb4de59f802aa492b78767a11414eb1ee7fa2494a7e2a1e742e874` |
| 4 | acceptable | 17.67 | 6.55% | `9eef2ff40647d2080db78bfe9b2682f862e001c1b58a4435f30dd89e9071efef` |

Raw JSON includes exact source revision, benchmark parameters, correctness and
quality classification. Uptime snapshots before and after all four runs are
retained. Cloud recovery, compacted recovery, 10,000-SST inventory, and other
recovery rows remain unmeasured here.
