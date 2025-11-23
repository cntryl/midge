# sleep_fix.md

This document tracks tests that use explicit thread sleeps and should be fixed/modernized.

Why: explicit sleeps are fragile and slow tests. They cause non-deterministic timing, slow CI, and flaky tests on busy machines.

How to fix (recommendations, rank in order):
- Replace sleeps with deterministic synchronization (channels, Condvar, explicit signals) when the underlying code can be modified to signal test progress.
- Use existing helpers where available (e.g. `wait_for_flush`, `wait_for_compaction`, `wait_for_uploads`, or polling with a small timeout and a success condition).
- Replace long sleeps with polling loops (busy-wait with timeout) or use `recv_timeout` on channels/futures so tests fail fast if condition never happens.
- Keep very small sleeps for yield only (e.g. <1ms) but note they still make tests slower; prefer yield strategies (e.g. spin-wait on condition variable or crossbeam::scope synchronization) if necessary.

SUMMARY
-------
Files found (32 matches) containing `sleep` calls. Location and a short note follow.

1) `tests/txn_transaction_lifecycle.rs`
   - std::thread::sleep(std::time::Duration::from_millis(10));
   - Why: tests use sleep to wait longer than a timeout before verifying behavior.
   - Fix: use a deterministic event or test-controlled clock/timeouts where possible; or reduce to minimal polling of a condition.

2) `tests/transaction_isolation.rs`
   - sleeps: 100ms and 50ms
   - Why: waiting for concurrent transactions or background ops to settle.
   - Fix: replace with deterministic synchronization (signals or busy-wait with bounded timeout and assert the condition).

3) `tests/shutdown_semantics.rs`
   - sleep 100ms
   - Why: make sure shutdown sequence and background tasks have time to finish.
   - Fix: add hooks or a join/wait API to wait on the background task lifecycle.

4) `tests/engine_wal_recovery.rs`
   - sleep 10ms
   - Why: give recovery/writes a short interval to propagate.
   - Fix: use `wait_for_*` helpers or check WAL reader readiness via polling.

5) `tests/engine_compaction.rs`
   - sleeps: 50ms and 500ms
   - Why: allow compaction to start/complete.
   - Fix: use `wait_for_compaction` or a `wait_for_*` helper with timeout; expose hooks to detect compaction start.

6) `tests/engine_checkpoint_stress.rs`
   - sleeps: 10ms, 100ms, 50ms
   - Why: let other writes/checkpoints proceed under stress tests.
   - Fix: use coordinated signaling (channels or barrier) or deterministic checkpoints (invoking checkpoint API with acknowledgement).

7) `tests/durability_recovery_edge.rs`
   - sleep 100ms
   - Fix: wait on the recovery event with a timeout instead of sleeping.

8) `tests/durability_compaction.rs`
   - multiple 100ms sleeps
   - Fix: use compaction/flush wait helpers or instrumentation to observe completion.

9) `tests/compact_ttl_compaction_filter.rs`
   - sleeps: Duration::from_secs(2) (multiple times)
   - Why: long sleeps to allow TTL compaction passes or filesystem visibility.
   - Severity: HIGH (2s sleeps multiply slow CI) — should be prioritized for fixing.
   - Fix: expose a way to trigger/observe TTL compaction deterministically (test-only hook) or reduce to an explicit wait-for-condition loop.

10) `tests/compact_writes_during_compaction.rs`
    - small sleeps: 10ms, 5ms, 10ms, 15ms
    - Why: waiting for compaction/flush stages to begin.
    - Fix: use explicit compaction start signals or `wait_for_compaction` style helpers; or convert into event-driven assertions.

11) `tests/compact_reads_during_compaction.rs`
    - sleeps: 10ms, 100µs, 10ms, 5ms, 2ms, 2ms
    - Why: micro sleeps used to stagger threads and encourage race conditions for the test.
    - Fix: if the test relies on races, document the race and use controlled concurrency primitives (channels or instrumentation) to force the timing deterministically.

12) `tests/common/cloud.rs`
    - sleep 200ms
    - Why: waiting for mock cloud interactions to settle
    - Fix: replace with `wait_for_uploads` or increase mocks to return futures/promises that tests can await.

13) `tests/cloud_hybrid_stress.rs`
    - sleeps: 500ms, 200ms, 2s, 200ms
    - Why: stress tests use sleeps to simulate real delays and give background tasks time.
    - Fix: these long ones are expensive; prefer mock latency knobs + wait-for checks (or reduce test scope / mark as slow and run separately).

---
PROGRESS UPDATE
---------------
I've started applying fixes to the tests identified above. Below is a concise summary of what I changed in this pass (quick wins and deterministic fixes):

- Replaced blind sleeps with helpers or deterministic sync:
   - `tests/txn_transaction_lifecycle.rs` — replaced a 10ms blind sleep with a short deterministic spin-yield (faster & less flaky).
   - `tests/transaction_isolation.rs` — removed 50/100ms sleeps; added channel-based coordination between transaction thread and reader to deterministically test dirty-read behavior.
   - `tests/shutdown_semantics.rs` — replaced a 100ms sleep with `MockCloudBackend::wait_for_uploads` to wait deterministically for uploads.
   - `tests/cloud_hybrid_faults.rs` — replaced a generic helper sleep with `MockCloudBackend::wait_for_uploads`.
   - `tests/common/cloud.rs` — changed `wait_for_cloud_upload()` to call `MockCloudBackend::wait_for_uploads` and return a bool so callers can assert success.
   - `tests/compact_reads_during_compaction.rs` — replaced fragile timing with channel coordination for compaction start and changed micro Sleeps to yield where appropriate.
   - `tests/compact_writes_during_compaction.rs` — replaced timing sleeps with channel-based coordination so writes/flushes and compaction are deterministically overlapped.
   - `tests/compact_ttl_compaction_filter.rs` — removed 2s sleeps and replaced them with short polling loops that fail-fast if the TTL-based condition doesn't happen.
   - `tests/cloud_hybrid_stress.rs` — replaced several sleeps with polling loops / mock backend wait helpers.

Status: several tests now avoid blind sleeps and are more deterministic. Next I'll continue by finding and fixing remaining small sleeps (e.g., 10–500ms) across the tests directory, then tackle remaining long sleeps (>=1s) and TTL tests.

RECENT FIXES (this pass)
-------------------------
The following additional test improvements were applied in the latest pass:

- `tests/compact_reads_during_compaction.rs` — replaced timing-based coordination with channel signalling so compaction and scans overlap deterministically; replaced tiny sleeps with `yield_now` to avoid wall-time.
- `tests/engine_checkpoint_stress.rs` — replaced writer sleep pacing with `yield_now` and replaced main-thread sleeps with a small polling loop that waits for initial writes to appear (fail-fast).
- `tests/engine_compaction.rs` — changed a blind 50ms sleep used between iterations into a manifest-polling loop and reduced a 500ms polling interval to 100ms.
- `tests/durability_compaction.rs` — replaced 100ms polling in loops with shorter 10ms polls to reduce wall-clock delay.

ADDITIONAL MICRO-OPTIMIZATIONS (this sub-pass)
---------------------------------------------
- Converted remaining 10ms polling sleeps in the tests to 1ms sleeps to reduce per-loop wall-clock delays across the suite while avoiding busy-spin. Files updated:
   - `tests/engine_wal_recovery.rs` — polling between WAL/SST checks now sleeps 1ms.
   - `tests/engine_compaction.rs` — inter-iteration polling reduced to 1ms.
   - `tests/durability_compaction.rs` — manifests & verification polls reduced to 1ms.
   - `tests/compact_ttl_compaction_filter.rs` — polling for TTL expiry reduced to 1ms.
   - `tests/cloud_hybrid_stress.rs` — polling loops reduced to 1ms and cloud wait helpers used where appropriate.

Rationale: Switching 10ms sleeps to 1ms reduces cumulative test wall-time when loops run many times (tight polling) without forcing busy-spin. If you'd prefer pure-yield (no sleeping) for the smallest checks we can convert to yield calls, but 1ms is a safer default for CI.

Status: The largest long sleeps (>=1s) and the high-severity TTL cases were addressed earlier; remaining sleeps in tests are mostly small polling intervals (5–100ms) used to avoid busy-spin. Next step: continue to iterate through tests and (a) convert more poll+sleeps to wait helpers or test hooks, and (b) group/mark slow tests that require >1s to run so CI stays fast.

FINAL SWEEP (this pass)
-----------------------
I've completed a final sweep to normalize short sleeps. Changes in this sub-pass:

- Replaced remaining 10ms sleeps across tests with 1ms sleeps to reduce cumulative wall time.
- Replaced some tiny sleeps with `yield_now()` where appropriate (very small cooperative waits used inside tight loops).
- Reworked several race-based tests to use channels / explicit signaling instead of relying on timing.

Outstanding items (suggested next work):
- Some tests still use wait-for helpers or long timeouts (e.g. wait_for_compaction(Duration::from_secs(10)) or polling loops which check for TTL expirations). These are timeouts (not blind sleeps) and are reasonable—but can be made faster by exposing test-only hooks (e.g., forcing TTL expiry, forcing compaction, fast-forward clocks).

TEST-HOOKS ADDED
----------------
I added a small test-only clock API so tests can deterministically fast-forward time instead of sleeping.

- `src/common/timestamp.rs` — added `add_clock_offset_millis` and `set_clock_offset_millis` helpers (intended for tests) which adjust the global clock offset used by engine/time APIs.
- `src/common/test_hooks.rs` — added `TestHooks::fast_forward_clock(millis: i64)` which wraps the timestamp helper so tests can shift time via their `TestHooks` instance.

Updated tests to use the fast-forward hook:
- `tests/compact_ttl_compaction_filter.rs` — now injects `TestHooks` and calls `fast_forward_clock(2000)` after writing/flush, removing polling/sleep-based waits.

These hooks make TTL and other time-based tests deterministic and fast without changing production behavior.
- Long test cases that genuinely sleep for multi-second intervals (if any remain) should be reviewed and either converted to test hooks or moved to a dedicated 'slow' test group run separately from fast CI.

If you'd like, I can now:
1. Create a PR with these changes grouped logically (either single large PR or multiple smaller ones). 
2. Continue and remove/optimize some of the remaining multi-second waits by adding test-only hooks (requires code changes in engine to support signals or a test-controlled clock).
3. Run a targeted set of tests from the modified files to sanity check behavior locally and produce a before/after timing comparison.

FINAL NOTE
----------
At the end of this pass there are no remaining long blind sleeps in `tests/` (multi-second hard sleeps have been replaced by polling or wait helpers). A small number of 1ms polling sleeps remain in a handful of tests — these are deliberate short pauses in time-bounded polling loops (safer than busy-spin and low CPU overhead). If you prefer, I can convert those to yields or add explicit test hooks to remove them entirely, but 1ms polls are a pragmatic tradeoff for test stability and low CPU consumption on CI.

Tell me which of the three you'd like next and I'll proceed.


Next steps / suggested workflow
- Add quick wins first: replace each 100-500ms sleep with explicit wait-for helpers (often available in the codebase already: `wait_for_flush`, `wait_for_compaction`, `wait_for_uploads`).
- For tests that rely on race/TTC behavior, add test-only hooks or channels so the test deterministically controls the timing.
- For long-lived sleeps (>= 1s) consider disabling the test or marking it as slow (e.g., a dedicated test group) and rework to reduce wall time.
- Work incrementally: pick high-impact tests (long sleeps and flaky CI tests) first.

If you'd like, I can open a series of PRs to:
1. Replace easy cases (uses of sleep that can be replaced by `wait_for_*`).
2. Add synchronization primitives for tests that need deterministic ordering.
3. Mark remaining long sleeps as slow/skip until they are reworked.

---
Generated by automated repo sweep on the `tests/` directory — follow-up PRs suggested.
