# Behaviors Covered by tests/

This file summarizes the intended behaviors expressed by the integration and system tests under `tests/`.

## TTL
- Return value while TTL not elapsed; return None after TTL expires; TTL=0 means no expiration.
- Persist TTL metadata across restart; expire keys if TTL elapsed while offline.
- Compaction removes expired entries and preserves non-expired ones.
- Snapshots: snapshots after expiry hide expired keys; snapshots before expiry see the key.
- WriteBatch supports TTL entries; mixed TTL and non-TTL keys behave as expected; overwriting updates TTL.

## Transactions – Basic & Advanced
- Commit/rollback semantics for empty, read-only, and multi-op transactions; drop => rollback.
- Range delete within transactions hides deleted ranges and remains atomic.
- Snapshot isolation for concurrent writes; reads see own writes and staged mutations.
- Transactions survive commit/restart; aborts do not persist.
- Stress: rapid transaction creation, sequential and concurrent execution.
- Atomicity for multi-key/100-op transactions; all-or-nothing even under concurrency.

## Transactions – Conflicts & Deadlocks
- Last-writer-wins (LWW) semantics for concurrent puts/deletes/delete-ranges.
- Conflict detection for inserts on existing keys and lost-update prevention (CAS patterns).
- Concurrent delete-range and overlapping ops remain consistent.
- Deadlock scenarios: only one insert succeeds on same key; retries succeed when appropriate; recovery after deadlock.
- High-contention and optimistic-locking stress without panics; recovery of conflict state after restart.

## Transactions – Isolation
- Prevent dirty reads; snapshot isolation blocks concurrent uncommitted writes from leaking.
- Read-your-own-writes within a transaction; snapshot views consistent across multiple reads.
- Phantom-read protection on range scans; isolation preserved under load; recovery of snapshot views after restart.

## Transactions – Spill to Disk
- Large transactions spilling to disk: commit/rollback correctness, order preservation, integrity of specific values.
- Rollback cleans spill files; restart before/after commit recovers or discards appropriately.
- Concurrent large transactions under memory pressure; tiny memory limits; mixed value sizes.
- Memory-mode avoids creating spill artifacts; no starvation of foreground writes during background spill.

## Write Path / Inserts
- Inserts fail when key exists; insert-with-value returns existing value; insert_with_ttl expected to exist.
- Delete range operations remove keys and are visible consistently in scans/transactions.

## Stress & Workloads
- Hot partition overwrites with compaction; mixed reads/writes/deletes interleaved.
- High-throughput small writes maintain order; rolling window workloads delete oldest/insert newest.
- Append-only workloads remain consistent and recover after crash.
- Burst-then-idle patterns handled; prefix-partitioned scans correct.
- Large value stress: store/flush/backpressure; crash recovery; snapshot reads after overwrites; compaction reclaims space.

## SST / Storage Format
- Per-block Bloom filters written, preserved, loaded, and used to skip blocks; offsets included in index/meta.
- Per-block Bloom integration for writer and reader; roundtrip correctness for values and blooms.
- Block summary persisted in meta index.
- Trie index compatibility: deterministic decode, legacy fallback, mixed formats, long keys, overlapping prefixes, empty ranges.

## Segments & Flush/Compaction Coordination
- Segment read path collects/seals/promotes segments correctly; filters by CF and range; detects overlap.
- Segment state transitions (mutable→sealed→promoted) validated; manifest tracks metadata; SST naming on promotion.

## Test Infrastructure
- Meta-tests enforce naming convention `should_{action}_when_{context}` and AAA structure with single behavior per test.
- Test hooks for skipping fsync, counting WAL appends, and gating compaction.

## Miscellaneous
- Bloom/TRIE/SST interoperability and backward compatibility.
- Stress workloads include TTL-like rolling deletions.

> Note: Some tests currently fail to compile; they still express intended behavior contracts and should guide implementation.
