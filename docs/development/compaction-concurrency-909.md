# Bounded compaction and flush concurrency (#909)

The candidate supports independently configured `compaction_concurrency` and
`flush_concurrency` in `1..=64`. Both default to one until workload measurements
justify a larger default. Compute workers are fixed and reused. Each compaction
slot owns its captured inputs, cancellation, deadline, result and publication
identity; a generic completion cannot replace another slot's result.

The shared registry reserves input names and source/target key spans until
publication settles. Unknown bounds conflict conservatively. One L0 job per
family is allowed; repairs and source fragmentation are exclusive. The picker
can continue through other legal levels/families when a candidate is reserved.
Completed publication marks existing debt due immediately.

Each ordinary compaction has a child memory quota of the total pool divided by
the configured slot count. Every child reservation also charges the same parent.
Repairs use the full pool exclusively. Hybrid storage attaches this parent to
the existing shared maintenance admission pool. Per-worker scratch ownership is
separate; the write-rate limiter remains shared across flush and compaction.

Flush ownership persists across build, publication and mirror phases, including
parked builds. There is at most one owned generation per family and at most the
configured number of owned pipelines. Physical completion consumes an exact
phase, epoch, family, sequence and flush ID, after the worker's terminal flag.
Duplicate or early results cannot release that owner or its staging admission.
The eligible-family selector skips owned and retry-delayed families.

Active regression source uses actual filesystem workers and event-loop
publication for simultaneous disjoint jobs, conflicting spans, one-L0 ownership,
hot/newer L0 ordering, immediate rescheduling, shared memory failure isolation,
per-family concurrent flush and restart. The phase regression holds real build
and publication work while submitting early, stale and duplicate completions.
These tests have been compiled; execution, throughput/fairness measurements and
larger-default qualification remain pending in the batch validation phase.

Cloud metadata publication independence is tracked separately by #910. Logical
level moves with no SST rewrite are tracked by #926.
