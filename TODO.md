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

## Sweep findings (detected timing-based waits and suggested changes)

Below are the tests that include timing-based waits, timeouts, or small sleeps identified during the sweep. Each file includes a suggested change and a short rationale.

- `tests/engine_compaction.rs`
  - Occurrences: `hooks.wait_for_manifest_update(..., Duration::from_secs(5))`, `eng.wait_for_compaction(Duration::from_secs(10))`
  - Suggestion: Keep gates for background compaction tests; consider using `eng.compact_all()` for synchronous behavior in tests where background compaction isn't the focus.
  - Priority: Medium

- `tests/engine_sst_operations.rs`
  - Occurrences: `gate.wait_until_blocked(Duration::from_secs(5))`, `hooks.wait_for_manifest_update(..., Duration::from_secs(5))`
  - Suggestion: Already deterministic; keep gate + manifest wait helpers. Consider lowering timeouts slightly if stable under CI.
  - Priority: Low (completed)

- `tests/txn_transaction_lifecycle.rs`
  - Occurrence: `Duration::from_millis(1)` used for short waits (already manually converted to a spin/yield loop in test)
  - Suggestion: Keep spin/yield approach; it's deterministic and fast.
  - Priority: Low (done)

- `tests/test_hooks_integration.rs`
  - Occurrences: compaction gate and `eng.wait_for_compaction(Duration::from_secs(10))`
  - Suggestion: Use compaction gate to wait for the compaction step (already present). Consider replacing the final `wait_for_compaction` with `hooks.wait_for_manifest_update(prev, timeout)` or `eng.compact_all()` if you don't want background scheduling. Maintain timeout guard to avoid flakiness.
  - Priority: Medium

- `tests/snapshot_lifecycle_compaction.rs`, `tests/range_delete_edge_cases.rs`, `tests/multicf_compaction_recovery.rs`, `tests/durability_compaction.rs`, `tests/memtable_concurrency.rs`, `tests/lsm_global_invariants.rs`, `tests/fitz_style_workloads.rs`, `tests/engine_merge_operator_correctness.rs`, `tests/compact_amplification_measurement.rs`, `tests/concurrency_internal_invariants.rs`, `tests/compact_amplification_measurement.rs`
  - Occurrences: `wait_until_blocked(Duration::from_secs(10))` gate usage; many use `eng.wait_for_compaction(Duration::from_secs(...))`
  - Suggestion: For tests that exercise background compaction semantics, keep `wait_for_compaction` with a longer timeout and use `install_compaction_gate` where you need to observe intermediate steps. For tests that don't require background compaction semantics, prefer `eng.compact_all()`.
  - Priority: Medium

- `tests/shutdown_semantics.rs` and `tests/common/cloud.rs`
  - Occurrences: `MockCloudBackend::with_latency(Duration::from_millis(500))`, `backend.wait_for_uploads(1, Duration::from_secs(2))`
  - Suggestion: These mocks intentionally use latency to simulate cloud behavior. Keep them; replace blocking timeouts with deterministic `wait_for_uploads` where possible. If a test simply needs to ensure upload behavior (and not the exact timing), prefer pushing data and calling a deterministic `wait_for_uploads` helper.
  - Priority: Low

- `tests/error_handling_flush.rs` & `tests/error_handling_core.rs`
  - Occurrences: `handle.wait_until_blocked(Duration::from_secs(2))` and `recv_timeout(Duration::from_millis(200))` in channels.
  - Suggestion: Keep guard-style waits for gates and channel timeouts to avoid hanging tests. Where feasible, prefer use of gates and hook-based notifications over `recv_timeout`-based polling.
  - Priority: Medium

- `tests/engine_checkpoint_stress.rs`
  - Occurrences: `let timeout = Duration::from_secs(1)` used in a number of assertions
  - Suggestion: If the test is sensitive to checkpoint timing, use `hooks.wait_for_manifest_update` or checkpoint-specific gate points. Otherwise increase timeout only as necessary for stable CI.
  - Priority: Medium

- `tests/cloud_*` files (durability and hybrid faults/stress)
  ### Expanded sweep findings (detailed per-file suggestions)

  - `tests/txn_transaction_lifecycle.rs`
    - Occurrences: `Duration::from_millis(1)` used for very short waits/spins.
    - Suggestion: Replace with a deterministic spin/yield loop or a lightweight gate; keep only if the spin is performance-sensitive and not a test flake source.
    - Priority: Low

  - `tests/test_hooks_integration.rs`
    - Occurrences: `install_compaction_gate`, `wait_until_blocked(Duration::from_secs(10))`, `eng.wait_for_compaction(10s)`.
    - Suggestion: Keep the gate usage (it's testing hook behavior). Replace final `eng.wait_for_compaction` with `hooks.wait_for_manifest_update(prev, timeout)` or use `eng.compact_all()` if the test does not require background scheduling semantics.
    - Priority: Medium

  - `tests/snapshot_lifecycle_compaction.rs`
    - Occurrences: `install_compaction_gate(...)`, `gate.wait_until_blocked(10s)`, `eng.wait_for_compaction(10s)`.
    - Suggestion: If the test validates background compaction semantics (intermediate states), keep the gate; otherwise prefer `eng.compact_all()` and `hooks.wait_for_manifest_update` for deterministic checks.
    - Priority: Medium

  - `tests/range_delete_edge_cases.rs`, `tests/multicf_compaction_recovery.rs`, `tests/engine_merge_operator_correctness.rs`, `tests/engine_compaction.rs`, `tests/durability_compaction.rs`, `tests/compact_amplification_measurement.rs`, `tests/concurrency_internal_invariants.rs`:
    - Occurrences: `install_compaction_gate(...)`, `gate.wait_until_blocked(Duration::from_secs(10))`, `eng.wait_for_compaction(10s)`.
    - Suggestion: Keep gates when observing steps in compaction. For general compaction assertions, prefer `eng.compact_all()` and gates only when step visibility is needed. Use `hooks.wait_for_manifest_update` to detect compaction-related manifest changes deterministically.
    - Priority: Medium

  - `tests/memtable_concurrency.rs`:
    - Occurrences: `.wait_for_flush(Duration::from_secs(10))`, `eng.wait_for_compaction(10s)`.
    - Suggestion: Use `eng.flush()` for synchronous blocking when allowable. Keep `wait_for_flush` for background flush path tests and prefer gates + `hooks.wait_for_manifest_update` where exact timing offset is asserted.
    - Priority: Medium

  - `tests/lsm_global_invariants.rs`:
    - Occurrences: `eng.wait_for_compaction(Duration::from_millis(500))` and `eng.wait_for_compaction(Duration::from_millis(200))` (short timeouts)
    - Suggestion: Replace short timeouts with synchronous compaction calls `eng.compact_all()` where semantics permit; otherwise increase timeouts or use gates + manifest update waiting to avoid CI flakiness.
    - Priority: Medium

  - `tests/fitz_style_workloads.rs`:
    - Occurrences: `eng.wait_for_compaction(10s)`.
    - Suggestion: If workload is intended to hit background compaction, keep waits; consider `compact_all()` for simpler assertions.
    - Priority: Medium

  - `tests/engine_sst_operations.rs` (already updated):
    - Occurrences: `hooks.wait_for_manifest_update(prev, Duration::from_secs(5))`, `gate.wait_until_blocked(Duration::from_secs(5))`.
    - Suggestion: Gate + `wait_for_manifest_update` is the recommended deterministic approach — complete.
    - Priority: Low (done)

  - `tests/shutdown_semantics.rs` and `tests/common/cloud.rs`:
    - Occurrences: `MockCloudBackend::with_latency(Duration::from_millis(500))`, `backend.wait_for_uploads(1, Duration::from_secs(2))`
    - Suggestion: Keep latency modeling for tests that validate timing and cloud behavior. Prefer `wait_for_uploads` deterministic helpers rather than `sleep`. Consider centralizing simulated latencies.
    - Priority: Low

  - `tests/error_handling_flush.rs` & `tests/error_handling_core.rs`:
    - Occurrences: `install_flush_gate(...); handle.wait_until_blocked(Duration::from_secs(2))`, `recv_timeout(Duration::from_millis(200))` and `recv_timeout(Duration::from_secs(1))`.
    - Suggestion: Keep small guards (timeouts) to avoid hanging tests; prefer hooks when asserting internal steps or results, and preserve `recv_timeout` as guard fallback.
    - Priority: Medium

  - `tests/engine_checkpoint_stress.rs`:
    - Occurrences: `ready_rx.recv_timeout(timeout)` (timeouts used in coordination)
    - Suggestion: Switch to a deterministic handshake where the writer signals via channel and the test awaits it with a reasonably long guard (timeout). If the guard cannot be avoided, ensure it's documented and not too narrow for CI.
    - Priority: Medium

  - `tests/cloud_hybrid_stress.rs`, `tests/cloud_durability.rs`, `tests/cloud_hybrid_faults.rs`:
    - Occurrences: `wait_for_uploads(..., Duration::from_secs(..))`
    - Suggestion: Keep `wait_for_uploads` calls; prefer explicit counts and deterministic checks rather than sleeps or shorter guards.
    - Priority: Low

  ### Next actions and prioritization
  - First pass: Prefer smaller, low-risk conversions — replace `eng.wait_for_compaction` with `eng.compact_all` where appropriate and replace `wait_for_flush` with `eng.flush()` when not testing background paths.
  - Next pass: For the tests exercising background compaction/flush/upload, ensure that each uses gates and `wait_for_manifest_update` where possible. Keep timeout guards only as safety nets and increase if CI shows flakiness.
  - Implement changes in small batches (1–3 files), run targeted tests.

  - Occurrences: `wait_for_uploads(..., Duration::from_secs(...))` in MockCloudBackend
  - Suggestion: These are okay to keep; prefer to use `wait_for_uploads` (deterministic) instead of `sleep`. Consider reducing explicit time constants or centralize them in a test helper for consistency.
  - Priority: Low

### Summary of recommended patterns
- If a test is exercising the background pipeline (flush/compaction/upload): use TestHooks gates (`install_*_gate`) and one of `wait_for_manifest_update(prev, timeout)` or `wait_until_blocked` + `release()` to reliably observe the behavior.
- If the test asserts a behavior that can be checked synchronously (flush/compaction not under test): prefer engine-provided synchronous calls (`eng.flush()`, `eng.compact_all()`) instead of polling/timeouts.
- Avoid `std::thread::sleep` or `Duration::from_millis(min)` where a deterministic hook or small spin-yield loop would suffice.
- For mock backends that simulate latency (e.g., `MockCloudBackend.with_latency()`), keep the latency used to model behavior but avoid relying on it for correctness – prefer `wait_for_uploads`.

I propose we proceed by implementing changes in small batches (1–3 files):
1. Migrate `error_handling_flush.rs`/`error_handling_core.rs` to use gates + hook notifications where applicable.
2. For compaction tests that are not asserting background schedulers, replace `wait_for_compaction` with `compact_all()`.

## CI Automation: Timing check script
- Added `scripts/check_test_timing.ps1` — a PowerShell script to scan `tests/` for timing-related patterns such as `std::thread::sleep`, `Duration::from_millis`, `.recv()` without timeout, and `wait_for_compaction`/`wait_for_flush`.
- Usage:
  - Run locally via: `pwsh -File scripts/check_test_timing.ps1` (from the repo root).
  - Exit code `1` indicates matches that require manual review; `0` means no matches found.

3. For `engine_compaction.rs` and `test_hooks_integration.rs`, where the background semantics are vital, add `install_compaction_gate` if missing and use `hooks.wait_for_manifest_update` to detect progress.

Next step: implement one or two sample changes and run targeted tests to validate deterministic behavior.

### Batch 1: Completed changes
- Updated tests to remove or replace `wait_for_compaction` with `compact_all()` where the test does not require background compaction semantics:
  - `tests/lsm_global_invariants.rs`
  - `tests/concurrency_internal_invariants.rs`
  - `tests/memtable_concurrency.rs`
  - `tests/compact_amplification_measurement.rs`
  - `tests/fitz_style_workloads.rs`
  - `tests/engine_merge_operator_correctness.rs`

Notes:
- These changes were verified by targeted test runs; no hangs observed.
- Kept gate-based compaction tests (those using `install_compaction_gate`) intact; they validate background compaction semantics and should not be changed to synchronous compaction.

### Next actions (short term)
- Sweep the remaining `wait_for_compaction` uses and decide per-file whether to convert to `compact_all()` or keep as gate-based wait.
- Add lints/checks for `std::thread::sleep(...)` and calls to `.recv()` without a timeout in `tests/`.


