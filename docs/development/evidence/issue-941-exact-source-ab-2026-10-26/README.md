# Issue #941 exact-source A/B evidence

## Source and method

- Base: `172c941201f42e0206ca912d080e65c23ff4133c`.
- Candidate: `90eb0b5c684267f45605e1f2d42580b4da45cbf9`.
- Each worktree received the same minimal runtime instrumentation from commit `b0ae9227`; no production behavior was changed by the overlay.
- The registered `tier2_subsystem_durability_commit_latency` benchmark ran in release mode with five measured samples and one warmup, on the same Apple M5/macOS arm64 host, AC power, rustc 1.99.0.
- The 4 KiB row was run in three alternating pairs. The first full-filter base run and a later 64-writer retry failed quality, so accepted standalone rotation runs were used. One additional base rotation attempt (A2) failed at 18.3% RSD; it is retained as diagnostic and replaced by A2-retry.
- Accepted runs retain raw harness JSON, CSV, Markdown and text outputs under each run directory. Failed/noisy diagnostic runs retain CSV, Markdown and text, with full JSON snapshots gzip-compressed. Uptime snapshots are retained where captured. `base-benchmark.rs` and `candidate-benchmark.rs` preserve the measured benchmark source.

The event-loop value is the measured wall service time in `apply_transaction_with_coalescing`, normalized by successful committed 4 KiB units. It includes handler work and deferral, so it is not process CPU time. Writer encode and append-service values are also normalized by successful 4 KiB commits. Append service includes file-handle checks and segment digest updates.

## Accepted 4 KiB rotation pairs

Values are harness medians per run. Pair deltas are candidate/base minus one. Event-loop reduction is shown as a positive percentage.

| Pair | Base tx/s | Candidate tx/s | Throughput delta | Base loop ns | Candidate loop ns | Loop reduction | Base writer ns | Candidate writer ns | Writer delta | Base latency p99 | Candidate latency p99 | p99 delta |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A1/B1 | 94.020 | 78.796 | -16.19% | 492,279 | 329,043 | 33.16% | 140,329 | 50,127 | -64.28% | 16.02 ms | 19.53 ms | +21.88% |
| A2-retry/B2 | 93.484 | 81.182 | -13.16% | 487,679 | 329,721 | 32.39% | 140,085 | 50,269 | -64.12% | 16.19 ms | 17.69 ms | +9.31% |
| A3/B3 | 93.402 | 82.040 | -12.16% | 490,506 | 322,543 | 34.24% | 141,703 | 52,372 | -63.04% | 16.40 ms | 25.44 ms | +55.13% |
| Median delta | — | — | **-13.16%** | — | — | **33.16%** | — | — | **-64.12%** | — | — | **+21.88%** |

All six accepted rotation invocations passed harness quality (RSD 0.09–5.06%). The primary event-loop service target of at least 20% reduction passes in all three pairs. Combined writer service improves in all pairs and meets the no-regression target. The rotation row's throughput falls in every pair, and its latency p99 exceeds the 5% guardrail in every pair; these are material regressions for follow-up.

## Other required gates and disposition

- Exact-source 16-writer standalone base attempt failed harness quality at 40.4% RSD; its latency p99 was 143.65 ms. A prior multirow base attempt recorded 48.4% RSD on the 64-writer row, and an isolated 64-writer retry recorded 31.4% RSD. These remain diagnostic; there is no accepted exact-source 16/64 pair set.
- Exact-source `tier4_complete_local_strict_group_commit_total` was not run.
- Therefore the required 16/64 throughput and p99 guardrails and complete-system no-regression gate remain unqualified. Do not substitute the broader integrated-range results in `../issue-941-integrated-ab-2026-10-26/README.md`; those use different SHAs and are supplemental only.
- Jev judged the accepted single-row result “promising,” not qualified, and weakly favored deferring further measurements after the repeated multiwriter quality failures. Source SHAs, harness evidence and issue criteria determine this disposition: #941 remains open, with primary service improvement demonstrated but performance acceptance incomplete and the 4 KiB throughput/p99 regressions requiring investigation.
