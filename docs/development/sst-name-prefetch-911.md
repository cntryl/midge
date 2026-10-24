# Shared SST name prefetch (#911)

Flush and compaction consume one creation-order cursor per column family. The
cursor is independent of data and WAL sequence numbers. Cloud admission still
requires each name to fall below the exclusive bound this writer has both
journaled and mirrored. The existing block size is 16 and maintenance starts
the next block when eight names remain.

A compaction consumes its name once before a provider wait. Its accepted plan
keeps that generation and original deadline through reservation completion and
later preparation. Resuming a preassigned plan consumes no new name. If a short
local publication turn or conflicting admitted job owns the required resources,
the accepted continuation remains queued and becomes actionable when that owner
settles. Provider failures still fail the original attempt; admission waiting
does not acknowledge success.

Restart begins at the durable exclusive reservation bound, past all names the
lost session could have handed out, including abandoned output objects. Local
startup also conservatively seeds from existing and retired immutable names
because older local compactions did not reserve this shared counter. Existing
WAL epoch and recovered sequence-floor rules retain their conservative skips;
this change removes filename allocation from the WAL sequence reservation path.

Active regressions cover mixed flush/compaction names under a high WAL sequence
without extra journal/mirror work, compaction-driven low-watermark prefetch,
preassigned-plan reuse, actual reservation completion under a held `WalPrune`
turn with the exact original deadline, and cloud-only cold replacement with an
abandoned immutable output preserved. These tests are compiled, with behavior
execution and request/latency measurements pending batch validation.
