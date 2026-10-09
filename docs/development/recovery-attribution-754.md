# Cold Engine recovery attribution (#754)

Baseline: merged `897c833c46c20533f0a0f9ea7a7c5f036644f2b0`, after #753's
diagnostic-only write-pressure work. No default recovery policy is changed here.

## Predeclared finite experiment

Generate one immutable backup through public Engine writes, explicit flushes,
compaction and `backup_to`, using filesystem-backed CloudSimulated storage.
Use 8,192 deterministic 512-byte values in each of two families. Alternate
128-row transactions; interleave keys over sixteen disjoint ranges. Flush only
one family and compact it into multiple real SSTs; retain the other family's WAL.
A failpoints-only optional cleanup deferral retains WAL publication authority.
It does not synthesize WAL records, SSTs, metadata, catalog entries or leases.
Fixture capture may retry Busy or missing-file I/O at most three times inside
one fixed thirty-second bound; every attempt is retained. This occurs before
measurement and does not extend the open target or deadline.
Keep the backup inventory and every object SHA256; every trial restores exactly
those bytes into a private fresh database before measurement. Require genuine
coverage probes, predicate rejections, reader churn, recovery checkpoint releases
and index rebuilds before claiming the intended workload shape.

Cold means a fresh Engine/cache. Backup validation/copy warms the filesystem's OS
pagecache. This is not cold-disk latency or a production-provider SLO.

Use 128 MiB Engine memory, 1 GiB local working storage and a 256 KiB recovery
memtable target. Keep normal I/O limits and a 30-second whole-open deadline.
The diagnostic target is **at most five seconds** to open this fixed fixture.
Never loosen the target or resource/deadline bounds after observing a miss.

Run three fresh processes per mode, rotating order: accepted sequence index/four
readers, diagnostic key index/four readers, sequence index/one reader, and accepted
selection with new phase clocks disabled. All alternatives retain at most the
accepted four readers, four blocks per reader and the same shared byte budget.
The phase-clock control measures incremental timer overhead; work counters remain
active. It is not a comparison with an uninstrumented historical binary.

Retain all twelve trials and outcomes. Then profile one fresh trial per mode on
the same hosted ARM machine using 999-Hz native CPU sampling, with acknowledged
sampling enable/disable immediately around `Engine::open`. Restore, fixture
construction, verification and shutdown are outside CPU sampling. Each profiled
trial retains its own native data/resource receipts and a plain same-mode control.
Report any lost samples, failures and perturbation; never substitute a failed run.
Perf records native DWARF callchains without build-id postprocessing/cache updates;
the exact executable is archived and executed from its immutable artifact path.
Reports retain native symbols/callchains with inline expansion disabled and a fixed
sixty-second postprocessing bound. The process bound remains three hundred seconds.
These tooling controls avoid slow addr2line finalization outside measured open;
they do not change sampling boundaries, frequency, fixture or acceptance targets.

A cost is dominant only with at least half of exclusive coverage work in all
three accepted-baseline repeats and native CPU support. Inclusive phase durations
must not be summed. Candidate changes need at least 20% less actual recovery time,
no greater than 10% regression in other declared controls, exact data, bounds and
successful cleanup. Any selected default optimization still requires focused
RED/GREEN tests and fourteen original full-hour cases at its final source.
Stop after this finite matrix with supported attribution and separately tracked
work, or an explicit null result if alternatives do not justify a default change.
A diagnostic experiment is not a shipped optimization.

## Proof and resource boundaries

The key selector is available only through unsupported internal-testing options.
It reserves its index before allocation, holds indices into the same immutable
manifest and invokes the existing exact key/sequence candidate predicate. Every
selected file still requires immutable identity/checksum and exact value/version/
TTL/operation proof. An index admission miss uses the original linear fallback.
The one-reader experiment reduces retention. Reader/proof/index state is released
before checkpoints and at lifetime end; report shared limit/peak/final charges.

The candidate phase includes traversal, predicate rejection and coverage-state
aggregation, excluding separately timed index construction and file proof.
Separate index construction, candidate work, identity
verification, reader construction and exact point work. Range attempt/completion/
byte/time receipts describe the filesystem-backed remote adapter; they do not
represent real-provider request billing. Observations never advance liveness.

Each trial verifies every point and a full scan in both families after recovery,
then both full scans after clean shutdown/reopen: six exact checks and two
shutdowns. Require the exact pre-compaction committed WAL frontier and reject salvage.
Check `wal_cloud_durable_seq` against the captured strict-ACK frontier. Runtime
`current_sequence` and the backup barrier also include reserved compaction/SST
generations; recovery checkpoints can allocate further generations without new
WAL records. Those allocator counters must not be mistaken for the WAL frontier. Controls retain sparse/
overlapping candidate equality, arbitrary/missing keys, duplicate/TTL/operation
semantics, insufficient-budget fallback, corruption/identity changes, typed
cancellation/deadlines and full reservation release.
