# Checkpoint cost isolation (#752)

This branch is experimental until correctness and the original matched performance gates pass. Neither rejected policy in #780 is accepted. Baseline is develop d5bcb607b75b1fa5cf5e4f4f910d7e49f9ebe12b, preserving every ordinary checkpoint.

The first source variant changes only the durable flush batch: include BumpWalSeq alongside AddSst and BumpNextSstSeq. It retains the original forced cadence and adds no preflight filesystem stats or locks. A new deterministic optional-snapshot-failure test first recovered frontier 0 instead of 123, then recovered all three published identities/frontiers with the complete batch. This isolates the durability prerequisite's cost from checkpoint-policy checks. It is not an optimization acceptance claim or an assertion that baseline loses acknowledged data.

Compare original A/B/C fixed cells, three fresh repeats each, same original warmup/limits, actual immutable source/binary identities and independent sealed raw-artifact readback. Preserve every adverse repeat. A newly dispatched unchanged-cadence baseline is run 37834100918. Same-source transport smoke must precede the variant's nine-cell campaign.

A later variant may avoid duplicated manifest stat/lock preflight for a cheap flush only when cached state is already checkpointed and the original append plus forced snapshot still validates authority. Unknown or deferred authority must retain checkpoint failure backpressure; costly deferred flushes retain external-change/stale-caller validation and bounds. Confirm this with counted filesystem operations and all existing durability regressions before measuring. Do not assume redundant checks caused the hosted SST-build regression.

Full acceptance remains: fixed-cardinality snapshot reduction, >=50% qualifying-cell checkpoint byte/time reduction, <=10% stable throughput/public-flush-p95 regression, bounded journal/replay/resource reporting, real ACK/crash/frontier correctness, exact-head CI and all fourteen original hour-long final-source cases. No watchdog, resource bound, cloud/GC authority, or acknowledgement proof is relaxed.
