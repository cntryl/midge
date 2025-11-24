# Test Sweep: Replace timing-based waits with deterministic sync

This document captures a plan to convert tests that rely on timing (sleeps, short timeouts, polling) into deterministic patterns using TestHooks, gates, and deterministic engine methods.

Format per task: file -> task -> suggested change -> priority -> notes

## Completed (already applied)
- `tests/engine_sst_operations.rs`:
  - Replaced `eng.wait_for_flush(...)` with `hooks.wait_for_manifest_update(prev, timeout)` after using a gate and `WriteBatch` to cause background flushes.
  - Kept `gate.wait_until_blocked(timeout)` with a timeout to prevent hanging tests, which is still deterministic.
- `tests/engine_compaction.rs`:
  - Replaced assert that checked `hooks.manifest_update_count()` with `hooks.wait_for_manifest_update(manifest_updates, timeout)` for deterministic manifest recognition.
- `src/common/test_hooks.rs`:
  - Added `wait_for_manifest_update(prev_count, timeout)` helper and notifier channel mechanics.
  - Added unit tests verifying this helper.

## Priority: High — Replace timing waits with deterministic helpers
- `tests/**` (sweep, general):
  - Task: Search for tests that rely on `eng.wait_for_flush(timeout)` with short timeouts or on small sleeps. Replace with `eng.flush()` when a synchronous flush is acceptable, or use `TestHooks` gates and `wait_for_manifest_update()` when exercising background flush pipeline.
  - Suggested fix: If the test needs to ensure a background worker did the work -> use gates + `wait_for_manifest_update`. Otherwise, prefer `eng.flush()` for deterministic blocking behavior.

### Specific files to process
- `tests/engine_compaction.rs`:
  - Task: Confirm `wait_for_compaction(10s)` is required. If compaction behavior is being tested, keep it; if not, prefer a gate or `eng.compact_all()` if synchronous compaction is acceptable.
  - Suggested fix: For background compaction tests, consider adding `install_compaction_gate()` if we need to assert steps in compaction; for general tests where compaction is incidental, prefer `eng.compact_all()`.
  - Priority: Medium

- `tests/engine_sst_operations.rs`:
  - Task: Confirm current usage of `gate.wait_until_blocked(timeout)` is necessary. Gate is the recommended pattern and can be left, but some tests still capture `manifest_update_count()` directly; we replaced them with `wait_for_manifest_update()` already.
  - Priority: Low (completed)

- Other tests: (Automation — find occurrences and decide per-file)
  - Task: Find files using `std::thread::sleep`, `Duration::from_millis(...)` or small `Duration::from_secs(1..5)` in tests. Consider replacing them with one of the below approaches.
  - Priority: High (sweep step) 

## Priority: Low — Non-test production code sleeps and waits under review
- `src/core/engine/operations/reads.rs` has a short backoff `std::thread::sleep(Duration::from_millis(5))`.
  - Task: Evaluate if this tiny sleep is necessary in production logic (it may be a minor backoff in a tight loop). If needed in production, keep; otherwise switch to a `yield` or use a more robust synchronization method.
  - Suggested fix: If we can avoid blocking, consider a `std::thread::yield_now()` or a retry counter with exponential backoff. If it’s only for tests, consider exposing a test-hook or a small synchronous check (non-production change) or making the logic event-driven.
  - Priority: Low

## Additions / Enhancements
- Add a `TestHooks::wait_for_manifest_updates(prev: u64, expected_count: u64, timeout: Duration) -> bool` helper for tests that require multiple manifest updates in sequence.
  - Priority: Low
- Document `TestHooks` usage patterns in `(src/common/test_hooks.rs)` and update `tests/README` or `testutils/README.md` with best practices for deterministic tests and gate usage.
  - Priority: Medium

## Work plan / workflow
1. Sweep to locate remaining tests with timing-based waiting.
2. For each test, decide if it needs background path tests (use gates + `wait_for_manifest_update`), or if synchronous `eng.flush()`/`eng.compact_all()` will do.
3. Update test to use deterministic helper(s), add gating where needed, and remove `std::thread::sleep()`/timeout-based assertions.
4. Run tests (one file at a time), fix failures. Repeat until the set passes consistently.
5. Consider improvements to TestHooks (e.g., `wait_for_manifest_updates`, `wait_for_compaction_count`) and add documentation.

## Tasks to pick next
- [ ] Sweep tests and list all files with timeouts or sleeps (automate using pattern searches).
- [ ] For each file, add a recommended change comment in this TODO.md.
- [ ] Implement and validate changes in batches (1-3 files at a time), and run targeted tests.
- [ ] Create PR with changes and test results.

## Checklist (repo-wide)
- [x] `tests/engine_sst_operations.rs` — converted to deterministic wait using `hooks.wait_for_manifest_update` + gate
- [x] `tests/engine_compaction.rs` — converted to deterministic wait using `hooks.wait_for_manifest_update` (kept `wait_for_compaction` for background compaction semantics)
- [ ] Sweep for `std::thread::sleep(...)` usage in tests and consider deterministic replacements
- [ ] Sweep for any explicit counter-based asserts and convert to `wait_for_manifest_update` or other wait helpers
- [ ] Document `TestHooks::wait_for_manifest_update` usage and add sample snippet in `src/common/test_hooks.rs` header or repo testutils README

## Next action (short term)
- Sweep and report: I will sweep the entire `tests/` folder for any use of waits/polls/short timeouts and create detailed per-file tasks here.
- After that, I'll implement changes in small batches and run targeted tests.

---

If you'd like, I can start the first action now: "Sweep tests and list all files with timing waits and proposed fix per file" and add the findings to this TODO.md.
