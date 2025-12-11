# BEHAVIORS_GAP

This file lists the gaps between the intended behaviors (see `BEHAVIORS.md`) and the current implementation.

## High Priority (correctness)
- TTL
  - `put_with_ttl`/`insert_with_ttl` now persist expiration in WAL/memtable and reads honor TTL, but compaction/SST paths still do not drop expired keys; snapshot/get_at do not use TTL.
  - `WriteBatch::put_with_ttl` exists but engine write_batch path ignores TTL (sets ttl_seconds: None for batch processing).
  - `compact_all` is stubbed and not invoked; TTL cleanup via compaction unimplemented.
- Snapshots
  - `get_at` is a stub that delegates to `get` (no snapshot isolation); snapshot structure is not hooked into runtime reads.
- Flush/Compaction control
  - `flush_cf`/`flush` are stubs; compaction triggers are not wired for tests expecting manual compaction control.
- Insert semantics
  - `insert_with_ttl` and `insert_with_value_and_ttl` delegate to `put_with_ttl` but LSM/runtime do not enforce TTL or uniqueness beyond existing `get` check; no atomicity with WAL write.
- Persistence/Restart helpers
  - `with_engine_restart` uses `open_with_options` but recovery paths for TTL/transactions/spill are not implemented.

## Medium Priority (feature coverage)
- Transaction features in tests (isolation levels, spill to disk, conflict detection, deadlock handling) are largely unimplemented or stubbed in the current engine/runtime.
- Segment/compaction pipeline behaviors (segment states, promotion, bloom per-block integration) are partially represented but many tests reference functionality not wired to current code paths.
- Stress workloads (hot partition, rolling window, burst/idle) rely on compaction/backpressure behaviors not fully implemented.

## Low Priority (infrastructure)
- Test hooks (fsync gating, WAL append counters, compaction gates) may be missing; meta-tests may fail once re-enabled.

## Recommended Next Steps
1) TTL end-to-end: persist TTL in memtable/WAL/SST metadata, enforce on reads and compaction, add time source abstraction, wire batch TTL, add tests back.
2) Snapshot-aware reads: plumb snapshot sequence into runtime read path; implement `get_at` properly.
3) Flush/compaction wiring: implement `flush_cf` and manual compaction trigger; remove stubs.
4) Insert semantics with TTL: ensure atomic WAL append + uniqueness check and batch support.
5) Re-enable targeted tests incrementally (TTL suite first) to drive implementation.
