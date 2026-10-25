# Snapshot retention under spill pressure

Run `cargo test --test snapshot_spill_pressure -- --nocapture`.

The public-API regression uses filesystem-backed simulated cloud with a 2 MiB
local budget, 128 KiB transaction pool and controlled TTL clock. Thirty-two
seeded keys have deterministic incompressible values; only the first eight
expire. A held snapshot freezes their original visibility. A new reader must
observe expiry before sixteen acknowledged range-delete/overwrite rounds,
flushes and actual compactions. Every round checks exact frozen and current
scans and point reads. Range deletes affect initially non-expiring keys, so
TTL expiry cannot make those assertions vacuous.

An uncommitted transaction then spills until actual `NoSpace`. Physical spill
files must exceed the 128 KiB resident transaction pool. The test requires an
actual `TransactionSpill` local-capacity rejection whose request exceeds the
recorded free bytes, verifies every shared disk charge and the unchanged
2 MiB total, and reports the variable WAL contribution. Measured boundaries
include 1,479,750 physical spill bytes with 529,488 WAL bytes (95% committed)
and 2,071,650 physical spill bytes with no WAL charge (98% committed). The final
green fixture measured 1,282,450 physical spill bytes with 794,232 WAL bytes
(99% committed). A fixed 1.5 MiB spill-only floor would incorrectly reject
valid boundaries with more retained WAL.
A separate bounded write must be rejected before acceptance; its key and all
uncommitted spill keys remain absent. Dropping the spill owner must remove its
actual files, clear its charge and restore capacity, then let the same 64 KiB
write succeed while the original snapshot remains open and exact.

WAL retirement can also lag a successful flush. If acknowledged, manifest-
covered WAL alone prevents the reusable compaction staging request, the fixture
drives ordinary runtime progress for at most ten seconds. It permits one
compaction retry only after the measured WAL charge decreases and sufficient
capacity returns. Unexplained charges, permanent pressure and other errors
still fail; neither configured budget increases.

Compaction must have retired actual snapshot input names. Those files must
physically remain while pinned, then all disappear within ten seconds after
snapshot release. Subsequent compaction, a strict write, flush, shutdown and
exact reopen must succeed. The runtime response budget is ten seconds and
shutdown has fifteen seconds; corruption, indeterminate timeout and failure
to reclaim are failures. Resource exhaustion is the intended admission boundary.

A temporary mutation bypassing GC's snapshot pin check makes the fixture fail
with `NotFound` on frozen reads. That mutation is restored before green checks
and never committed. The test adds composed regression coverage for #760;
it does not establish a production defect. Its deterministic simulated-cloud
schedule proves neither process-crash/power-loss recovery, native-provider
behavior, whole-process RSS bounds, nor exhaustive concurrency.
