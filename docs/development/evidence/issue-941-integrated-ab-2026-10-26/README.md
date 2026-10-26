# Supplemental integrated WAL pipeline A/B evidence

This broader-range comparison is supplemental only; it does not isolate #941.
The exact-source #941 comparison and acceptance disposition are documented in
[`../issue-941-exact-source-ab-2026-10-26/README.md`](../issue-941-exact-source-ab-2026-10-26/README.md).

## Scope and source

This is a controlled integrated comparison of the existing #940/#941 pipeline
range. It does not isolate #941 causality.

- Base runtime: `e9a0d86b7be0c1b298ef8f546326c5893aedba04`
- Candidate runtime: `e849fd96a5f528f794c313f6857a06c5b135c38b`
- Benchmark: `tier2_subsystem_durability_commit_latency`, registered 4 KiB
  rotation row, release profile, five harness samples and one warmup.
- A1 additionally ran the registered 16/64-writer and single-writer rows.
  B1/A2/B2/A3/B3 were rotation-only. The 16/64 guardrail comparison is
  cross-checked against the exact-SHA campaign in
  `../issue-940-strict-fsync-ab-2026-10-26/README.md`.
- Runtime event-loop timer and public metric overlay was identical in intent
  across both worktrees. Instrumented benchmark SHA-256: base
  `e0f1cb1cf4be1e88276b4fa09e98757347c1956383003e3913a8c28c22d47c62`,
  candidate
  `d7ddd98d1074018b3780c4acb2f9859deabd90280141f81e3d028e49b55afab0`.
  `base-benchmark.rs` and `candidate-benchmark.rs` preserve the exact sources.
- Every accepted invocation had five measured samples and one warmup; all
  rotation rows passed the harness quality gate (RSD 0.2–6.8%). Runs reported
  AC power. Per-run uptime snapshots are retained where captured. Artifacts
  retain CSV, JSON, Markdown and text output.

The event-loop value is summed wall service in calls to
`apply_transaction_with_coalescing`, divided by successful measured 4 KiB
commits. It includes coalescing, admission and handler deferral, but excludes
caller-side wait. Writer encode and append values are per successful 4 KiB
commit. Append service includes file-handle checks and segment digest updates.
The combined writer value is encode plus append service. These counters are
not process CPU time.

## 4 KiB rotation results

Throughput and all service values are per invocation medians. Pair deltas use
candidate divided by base minus one. Service reduction is reported as a
positive reduction.

| Pair | Base tx/s | Candidate tx/s | Throughput delta | Base event-loop ns | Candidate event-loop ns | Event-loop reduction | Base writer encode+append ns | Candidate writer encode+append ns | Writer service delta | Base p99 us | Candidate p99 us | p99 delta |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A1/B1 | 89.465 | 80.979 | -9.49% | 8,713,485 | 325,060 | 96.27% | 132,907 | 48,336 | -63.63% | 17,695 | 19,471 | +10.04% |
| A2/B2 | 93.241 | 81.937 | -12.12% | 8,364,686 | 327,823 | 96.08% | 128,990 | 46,251 | -64.14% | 17,695 | 19,519 | +10.31% |
| A3/B3 | 92.693 | 82.827 | -10.64% | 8,435,295 | 316,168 | 96.25% | 130,873 | 46,882 | -64.18% | 19,935 | 19,471 | -2.33% |
| Median delta | — | — | **-10.64%** | — | — | **96.25%** | — | — | **-64.14%** | — | — | **+10.04%** |

The primary event-loop service target of at least 20% reduction passes in all
three pairs. Combined writer encode/append service does not regress; it falls
by 63.6–64.2%. The separate encode counter rises from roughly 5.9–6.0 μs to
8.8–9.2 μs per commit, while append service falls from roughly 123–127 μs to
37–39 μs. This is included in the combined writer measurement and should not
be represented as an encode-only improvement.

The 4 KiB row's median throughput regresses 10.64%. Commit p99 exceeds the
5% ceiling in two of three pairs and the median is +10.04%. These are failed
guardrails, even though event-loop service improves substantially.

## 16/64-writer and complete-system guardrails

The exact-SHA #940 campaign at the same base/candidate source revisions found
throughput regressions of 19.21–20.70% at 16 writers and 19.31–20.93% at 64
writers in its two accepted pairs. Both exceed #941's required 10% throughput
improvement; its third pair was not quality-complete. #941 therefore does not
meet the 16/64 guardrail. The #940 raw files are retained in the linked
evidence directory.

The required `tier4_complete_local_strict_group_commit_total` no-regression
row has not been rerun at these exact base/candidate SHAs with this overlay.
No complete-system conclusion is made here. Issue #941 remains open.

## Raw artifacts

`A1/` is the complete four-row base invocation; `B1/`, `A2/`, `B2/`, `A3/`
and `B3/` are the rotation-only invocations. All `latest` files are copied
byte-for-byte from each harness output directory.
