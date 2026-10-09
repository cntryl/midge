# Checkpoint cost isolation (#752)

This branch is experimental until correctness and the original matched performance gates pass. Neither rejected policy in #780 is accepted. Baseline is develop d5bcb607b75b1fa5cf5e4f4f910d7e49f9ebe12b, preserving every ordinary checkpoint.

The first source variant changes only the durable flush batch: include BumpWalSeq alongside AddSst and BumpNextSstSeq. It retains the original forced cadence and adds no preflight filesystem stats or locks. A new deterministic optional-snapshot-failure test first recovered frontier 0 instead of 123, then recovered all three published identities/frontiers with the complete batch. This isolates the durability prerequisite's cost from checkpoint-policy checks. It is not an optimization acceptance claim or an assertion that baseline loses acknowledged data.

Compare original A/B/C fixed cells, three fresh repeats each, same original warmup/limits, actual immutable source/binary identities and independent sealed raw-artifact readback. Preserve every adverse repeat. A newly dispatched unchanged-cadence baseline is run 37834100918. Same-source transport smoke must precede the variant's nine-cell campaign.

A later variant may avoid duplicated manifest stat/lock preflight for a cheap flush only when cached state is already checkpointed and the original append plus forced snapshot still validates authority. Unknown or deferred authority must retain checkpoint failure backpressure; costly deferred flushes retain external-change/stale-caller validation and bounds. Confirm this with counted filesystem operations and all existing durability regressions before measuring. Do not assume redundant checks caused the hosted SST-build regression.

Full acceptance remains: fixed-cardinality snapshot reduction, >=50% qualifying-cell checkpoint byte/time reduction, <=10% stable throughput/public-flush-p95 regression, bounded journal/replay/resource reporting, real ACK/crash/frontier correctness, exact-head CI and all fourteen original hour-long final-source cases. No watchdog, resource bound, cloud/GC authority, or acknowledgement proof is relaxed.

## Cheap forced-path candidate

The next variant restores the bounded expensive-flush policy from #780 and removes duplicated preflight for cheap, bounded, already-checkpointed cached state. It explicitly forces the original post-append snapshot on this path; the durable append still checks actual file lengths under the writer stripe, and noncontiguous edits keep the caller horizon behind the checkpoint so intervening authority is replayed. Unknown/failed checkpoint state, deferred tail, stale horizon or oversized record cannot use this path.

A counted real-filesystem regression was RED at 704 ordinary versus 576 forced manifest stats over 64 cheap publications, then GREEN at equal counts. Every recovered SST/frontier remains exact. All 82 all-feature runtime-state tests pass (one explicit measurement ignored): both expensive deferred and cheap forced snapshot failures stop further journal growth at write/ENOSPC/rename boundaries; external SST/frontier publication survives the forced path, stale memory is fenced and reload restores publication. Clippy across all targets/features passes. The first isolated frontier test was primed with a checkpoint in this variant so its injection still targets the optional post-append snapshot, not mandatory unknown-authority preflight.

Local controls do not establish performance. The frontier-only and final candidate require independent original nine-cell release measurements with all rows retained. The source remains unmerged pending the original gates.

## Isolation result and candidate qualification

Frontier-only source 2df046faa998c5078456abaa5549a6475531f8f1 passed transport smoke 37834588611, all-platform CI 37834592656 and all nine measurements 37835121743. Independent sealed raw-artifact readback completed for this campaign and baseline 37834100918. A/r2 and A/r3 still show elapsed ratios 1.361/1.317 and flush-p95 ratios 2.318/2.343 despite unchanged checkpoint cadence and no preflight stats. A/r1 is favorable; every row is retained. Thus duplicated preflight is a confirmed avoidable operation cost, but is not established as the sole cause of the prior A regression. Further measurements must preserve this adverse control.

The cheap-forced-path candidate passed all 186 local all-feature fault-injection tests, 124 all-feature metadata controls, 82 all-feature runtime-state tests, strict lint, format and test contracts. All these are local proof; exact-head hosted measurement and full-hour acceptance remain required. Captured original archives, comparisons and readbacks remain under /tmp/midge-752-frontier-* and /tmp/midge-752-isolation-baseline-*.

### Second variant remains unqualified

Source 569b705d74fc8ebce89281f42b11d113bf05ddcb passed sanitized fuzz replay 37836652652 and transport smoke 37836637459. Nine-cell campaign 37837300747 has complete sealed raw-artifact readback, compared with baseline 37834100918. A/r2 and A/r3 retain elapsed ratios 1.308/1.255 and flush-p95 ratios 2.286/2.308; A/r1 is favorable. B checkpoint bytes fall 93.61--96.35 percent and checkpoint time 87.48--94.20 percent; C falls 100 percent in both. These cost savings do not override the failed A gate. All rows remain in /tmp/midge-752-fast-comparison.json and the original archives/readbacks in /tmp/midge-752-fast-*. Removing redundant stats is not an established causal fix for the SST-build regression.

Hosted CI 37836654343 passed Linux and Windows but failed the macOS cloud-catalog mirror comparison after reopen (185 of 186 fault tests passed). The mismatch is retained in /tmp/midge-752-fast-ci-failure.log; do not describe this head as all-platform green or dismiss the failure without source/test evidence. No merge or closure is authorized by these results.

## Preserve healthy forced-path journal cost

The third variant restores the original two-edit AddSst/BumpNextSstSeq batch for cheap, bounded, already-checkpointed ordinary local flushes. Every such healthy publication still saves a full snapshot containing its persisted frontier. Expensive/deferred publications retain the atomic three-edit batch. A counted real-Fs regression was RED at 3376 journal bytes versus the original 3176 for eight primed cheap publications; the restored path is GREEN at equal bytes and retains equal manifest-stat counts.

If an optional snapshot fails and the frontier was not journaled (cheap flush or duplicate identity), the runtime must durably append both the next SST counter and persisted frontier before returning success. A failed fallback append returns an error and fences metadata until reload. The store retains one checkpoint-retry bit: failed positioning/staging/rename/truncation sets it under the writer stripe; only a successful checkpoint clears it. A fallback append or cache refresh cannot erase this pressure. The new store regression was RED when cache refresh allowed further growth, then GREEN. Repeated write/ENOSPC/rename failures retain exact journal bytes across blocked successors.

No new journal format, public API or metadata cache is introduced. Added deferred authority remains limited by 16 records/16 KiB plus one bounded 4 KiB crossing; the cheap failure path starts without a deferred tail and uses two small durable records. This is not a bound on pre-existing authority or forced oversized publications. Before-ACK interruption between the cheap batch and its snapshot/fallback never counts as a successful flush. The existing conservative WAL/SST retention proofs and mandatory cloud/GC snapshots remain in force.

Controls include fallback marker-write and required-sync failures (no false ACK; exact successor after reload), duplicate higher counter/frontier progress, foreign structural edits both before and after the cheap append, and a real-engine cheap-flush snapshot failure followed by acknowledged flush, abrupt child abort, WAL removal and exact SST-only recovery. Successful checkpoint reconstruction already carries monotonic caller progress while replaying foreign edits; these cases require no speculative runtime change. All 187 all-feature fault tests pass locally, including the catalog mirror test. Its independently confirmed observation race is tracked as #782; that green local rerun does not erase the macOS failure or qualify hosted CI.

This restores an observed payload cost; performance causation and acceptance still require the original three-repeat A/B/C campaign, sealed artifact readback, exact-head CI/fuzz and fourteen final-source full-hour cases. All prior adverse rows remain retained.

### Third variant also misses the no-regression gate

Source 640d4692cfcb570518bdb9fa6dc0eac86a0d21e9 passed all-platform CI 37841020105, fuzz replay 37841109237, transport smoke 37841105678 and all nine measurements 37841786212. Sealed raw-artifact readback is complete. Against baseline 37834100918, A/r2--r3 elapsed ratios are 1.645/1.333 and flush-p95 2.708/2.506; C/r1 is also adverse at 1.661/2.021. B retains >93 percent checkpoint time and >96 percent byte savings, C 100 percent. All rows remain in /tmp/midge-752-v3-comparison.json and /tmp/midge-752-v3-full-*. No merge or full-hour acceptance follows. A 25-case release Engine diagnostic recovered exact values and retained at most 8235 tail bytes/15 records; record 16 and oversized keys checkpointed. Its observed 13.86 MB process RSS and <=37.714 ms reopen are finite-case observations, not stable performance acceptance.

Further small policy guesses are not justified by these controls. The `checkpoint-pair` diagnostic mode downloads the original provider-sealed build archives, verifies their actual immutable source/tree/lock/executable, executes A in three balanced ABBA/BAAB blocks on one host, then separately profiles sync/CPU work. It preserves original workload/configuration/deadlines and real-disk databases with tmpfs companion receipts. The diagnostic workflow head is recorded separately from actual benchmark checkout heads; no Git head or original receipt is rewritten. Instrumented timings cannot replace the uninstrumented comparison or original nine-cell campaign. Warmup SST/compaction state is retained to investigate the observed 96/100-compaction timing split in both sources. No causal or acceptance conclusion is predeclared.

## Same-host A isolation result

Diagnostic run 37844882145 at controller fdc9d9ec6dae4fdbf7c18511758cc3ce328105d8 completed all twelve plain fresh-process A trials. Original provider ZIP SHA-256 is 8d615b740b04e1617e24e55ca45eff0d701bf74d3cc8b7f7ef13f56658f24967; captured REST metadata, originating build archives and actual executables were independently verified. Baseline source remains d5bcb607; candidate runtime remains 640d4692. Both sources use identical original workloads and limits. All trials verify every acknowledgement and reopened value. The diagnostic controller's exact-head three-platform CI 37844872529 passed.

| Block | Source | Elapsed seconds | Public flush p95 ms | Compactions |
| --- | --- | ---: | ---: | ---: |
| 1 | baseline | 5.699 | 37.847 | 100 |
| 1 | candidate | 5.703 | 41.252 | 100 |
| 1 | candidate | 5.517 | 39.173 | 99 |
| 1 | baseline | 5.601 | 39.655 | 99 |
| 2 | candidate | 5.538 | 40.510 | 100 |
| 2 | baseline | 5.443 | 40.074 | 100 |
| 2 | baseline | 5.455 | 38.382 | 100 |
| 2 | candidate | 5.597 | 39.638 | 100 |
| 3 | baseline | 5.524 | 38.026 | 99 |
| 3 | candidate | 5.358 | 36.888 | 100 |
| 3 | candidate | 5.995 | 40.334 | 100 |
| 3 | baseline | 5.566 | 38.219 | 99 |

Candidate/baseline overall median ratios are 1.004 elapsed and 1.044 public flush p95. Within-block elapsed ratios are 0.993/1.022/1.024 and p95 ratios 1.038/1.022/1.013. This one host does not reproduce the earlier large source difference, but does not erase the original adverse rows or establish a mechanism. All rows enter measurement with the same SST count/bytes, no active compaction and eight completed warmup compactions. Compaction counts vary 99--100 on this host for both sources; they are not established as the cause. Whole-process sync profiles record 7,738/7,741 fsync calls and 0.564/0.550 traced syscall seconds, including warmup, verification and reopen.

Both CPU-instrumented benchmarks passed, but CPU report rendering failed after ownership restoration because the analyzer still ran as root against a runner-owned file. Those reports are invalid analysis; raw perf data remains retained. The controller now analyzes as the runner and records an unsuccessful analysis explicitly. The earlier transport failure 37844054046 has no usable uploaded archive and remains separate.

Next controlled comparison uses all three original cells, three independently provisioned hosts and twelve balanced plain trials per cell per host. Cell order rotates across hosts; each native warmup, value/flush count, compaction admission, data/reopen checks and deadline remains unchanged. This diagnoses stability and preserves every row; it does not replace the original nine-cell artifacts or fourteen final-source full-hour cases. No additional runtime policy change follows from an unproven cause.
