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
