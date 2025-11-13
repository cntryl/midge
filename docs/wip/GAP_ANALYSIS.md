# Midge - Gap Analysis (automatic scan)

Date: 2025-11-12

This document captures a quick, reproducible gap analysis run against the repository root. It lists the high-level findings (TODOs, failing checks), priorities for shipping, and an actionable plan to close remaining blockers.

## Summary of automatic scan

- TODO/FIXME occurrences found (approx): 134 matches across tests and core code. Many are test TODOs like `// TODO: ...` in tests related to transaction semantics, durability, and instrumentation.
- CI files present: `.github/workflows/ci.yml`, `.github/workflows/bench.yml`, `.github/dependabot.yml`.
- Running `cargo test` failed: the meta-test enforcement suite (`tests/test_guidelines_compliance.rs`) failed. The failure categories:
  - Missing AAA comments (Arrange / Act / Assert) across ~312 tests.
  - Multi-behavior test names detected (~50 tests where names contain `_and_` or imply multiple behaviors).
  - As a result, the test run exits non-zero and blocks CI.

These meta-test failures are the immediate gating factor for CI and shipping.

## Where problems live (high-level)

- tests/*: large fraction of `// TODO` markers and many tests missing required AAA comments per the project's test guidelines.
- src/*: scattered TODOs in core engine code (transaction handling, memtable merge semantics, write stall mechanism, autotuner initialization). These are design/feature TODOs (Phase 4/5 work).
- wal and durability tests contain many TODOs and instrumentation suggestions (fsync boundaries, truncated WAL simulation) — these are high-priority for correctness guarantees.

## Immediate shipping blockers (priority order)

1. test-guidelines meta-test failing — blocks CI and release. Fix options:
   - A: Fix tests to comply with AAA + single-behavior rules (correct, but large work).
   - B: Temporarily narrow the meta-test to only assert rules for a curated subset (new or high-value tests) while we triage and incrementally remediate the test suite.
   - C: Add a temporary exemption list (paths excluded) to meta-test until tests are brought into compliance.
   Recommendation: take option B or C as a short-term unblock; simultaneously start fixing tests in priority order.

2. Critical durability tests with TODOs that reference fsync and WAL correctness — must be validated before shipping a stable release.

3. Engine-level TODOs that affect correctness under concurrency (write stall, merge semantics) — prioritize if they currently impact CI or customers.

## Proposed phased remediation plan

Phase 0 — Short unblock (1-2 days)
- Modify the `tests/test_guidelines_compliance.rs` to only run on a curated list or to accept an env flag (`CI_STRICT_TEST_GUIDELINES=1`). This gets CI green quickly so other fixes can land without blocking.
- Create a tracked issue and milestone `ship-1.0` listing the exact tests/features we will fix before GA.

Phase 1 — Critical fixes (1-3 weeks)
- Triage failing/important tests and fix them one-by-one following project test-guidelines:
  - Add `// Arrange`, `// Act`, `// Assert` comments where tests >5 lines and are missing them.
  - When a test covers multiple behaviors, split into separate tests.
  - For tests that are flaky/overly large, mark as `#[ignore]` and create a ticket to re-enable after refactor.
- Address durability/instrumentation TODOs that cause incorrect behavior (fsync, WAL replay) — prioritize based on test failures and code review.

Phase 2 — Non-critical cleanup (2-4 weeks)
- Remove/resolve lower-priority TODOs in code (autotuner, merge operator polish, health manager Phase 5) or convert into tracked issues if implementation is deferred.
- Add missing benchmarks and docs referenced by wip/ files.

Phase 3 — Release hardening (1 week)
- Run full test matrix and clippy; fix warnings that indicate real issues.
- Update `.github/workflows/ci.yml` to run the curated test matrix, clippy, and a release smoke test.

## Concrete immediate actions I can take now (pick one or more)

1) Create `docs/GAP_ANALYSIS.md` (this file) — done.

2) Make the meta-test non-blocking by changing `tests/test_guidelines_compliance.rs` to apply only to a small allowlist or accept an env flag. This is a small change that I can implement now and validate.

3) Start an automated script to add AAA comments to tests that are >5 lines and missing AAA (careful: automated edits can be noisy; better to do this test-by-test with reviewers). I can prepare a script to surface candidate test files and the exact locations.

4) Triage the TODO list: generate a CSV or GitHub issues for each TODO with path, line, and brief suggestion. I can produce that CSV next.

## Quick verification commands

Run tests locally:

```powershell
cargo test --all
``` 

Run just the meta-test so you can iterate faster while working on it:

```powershell
cargo test --test test_guidelines_compliance
```

Run clippy:

```powershell
cargo clippy --all-targets --all-features -- -D warnings
```

## Recommended immediate next step (my suggested action)

I'll implement the short-term unblock: modify `tests/test_guidelines_compliance.rs` so it respects an allowlist or environment variable, then re-run `cargo test` to ensure CI is no longer blocked. This gives us breathing room to fix tests incrementally while keeping CI green.

If you want I can proceed with that change now and push it as a small PR branch; otherwise I'll produce the TODO CSV and a prioritized patch list next.

---
Notes: this is an automated reconnaissance pass. I can continue with the short-term meta-test change now if you authorize it; otherwise tell me which of the concrete next actions you'd like me to take first (1–4 above).
