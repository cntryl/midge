# 1.0 Readiness Scorecard

This scorecard is a maintainer-facing readiness assessment for Midge's path to `1.0`.

It uses a local-first bar:

- single-process embedded deployment
- local-disk storage mode
- strict recovery policy

It does not treat cloud-backed production as part of the initial `1.0` promise unless the product scope changes and the contract/support docs are updated accordingly.

## Verdict

Midge looks late pre-`1.0`, not early-stage. The repo shows a serious documentation surface, substantial live unit and integration coverage, explicit recovery and crash-boundary testing, and disciplined bench structure. The remaining risk is mostly contract and qualification discipline rather than missing engineering work. A local-first `1.0` looks plausible once transaction semantics, compatibility verification, and operator-envelope guidance are tightened. A cloud-inclusive `1.0` is not yet justified because the current docs still keep cloud-backed production outside the promised `1.0` surface.

## Green

### Documentation Surface

**Rating:** Green

Evidence:

- The development and operations docs already cover the expected `1.0` categories:
  - `one-dot-zero-contract.md`
  - `release-policy.md`
  - `stability-policy.md`
  - `format-compatibility.md`
  - `support-matrix.md`
  - `storage-invariants.md`
  - `recovery-internals.md`
  - `cloud-setup.md`
  - `production-runbook.md`
  - `resource-limits.md`
  - `release-checklist.md`
- The surface is shaped like a real adopter-facing project rather than an internal prototype.
- The docs already distinguish supported scope, experimental scope, release requirements, and operator workflows.

### Correctness, Recovery, and Durability

**Rating:** Green

Evidence:

- `cargo test -- --list` shows live suites for WAL, corruption, restart, and recovery boundaries rather than only happy-path coverage.
- Visible recovery-oriented tests include WAL corruption, manifest corruption/interruption, no-space failures, reopen idempotence, and transaction crash boundaries.
- The repo includes cloud persistence and cloud recovery suites, indicating cloud durability behavior is being exercised even though it is not yet part of the local-first `1.0` promise.
- `external_adopter_smoke.rs` exists as a live integration suite, which is a strong signal that adoption workflows are being tested directly.
- Tiered benches exist across `tier1` through `tier4`, including system behavior and failure-scenario benches rather than only microbenchmarks.

## Yellow

### Transaction Semantics

**Rating:** Yellow

Evidence:

- The repo has substantial transaction coverage:
  - `transaction_basic.rs`
  - `transaction_conflicts.rs`
  - `transaction_isolation.rs`
  - `transaction_isolation_audit.rs`
  - `transaction_isolation_lww.rs`
  - `transaction_spill.rs`
  - `transaction_crash_boundaries.rs`
- The live suite explicitly exercises LWW behavior, lost-update cases, crash atomicity, and restart behavior.
- The gap is not lack of testing. The gap is that the current `1.0` contract reads more like a release target than a single canonical public transaction contract.
- Before `1.0`, external semantics should be easy to answer in one place:
  - what isolation is provided
  - where lost updates are permitted or prevented
  - what LWW means externally
  - what atomic commit guarantees survive crash by durability mode

### Operational Readiness

**Rating:** Yellow

Evidence:

- `production-runbook.md`, `resource-limits.md`, verification APIs, recovery metrics, and observability tests all exist.
- Live tests such as `observability_api.rs`, `recovery_metrics_api.rs`, `recovery_policy_api.rs`, and `resource_cleanup.rs` show that operator-facing surfaces are not just aspirational.
- The current runbook still frames production support conservatively and keeps the supported production topology narrow.
- The remaining gap is sharper operational-envelope guidance for adopters:
  - sizing expectations
  - startup/recovery expectations
  - disk-pressure behavior
  - degraded-mode guidance
  - storage-latency assumptions

### Cloud-Backed Production Readiness

**Rating:** Yellow/Red

Evidence:

- Cloud mode is meaningfully implemented and tested:
  - `engine_cloud.rs`
  - `cloud_recovery.rs`
  - `cloud_persistence_hardening.rs`
  - `hybrid_storage.rs`
  - cloud-oriented `tier3` and `tier4` benches
- The current docs are explicit that cloud-backed production is still pre-`1.0`:
  - `one-dot-zero-contract.md` keeps cloud-backed production outside the `1.0` in-scope list
  - `support-matrix.md` marks cloud-backed mode as experimental/evaluation
  - `cloud-setup.md` says compatibility and operational guarantees are still being tightened before `1.0`
- This is not a statement that cloud mode is weak. It is a statement that the project has not yet promoted it into the promised production contract.

For a local-first `1.0`, this is out of scope. For a cloud-inclusive `1.0`, it is a blocker until the contract and qualification posture change.

## Yellow To Red

### Compatibility Promise

**Rating:** Yellow/Red

Evidence:

- `format-compatibility.md` defines the right policy surface for `1.0`.
- `stability-policy.md` is explicit that the strongest compatibility contract does not exist yet in `0.x`.
- `one-dot-zero-contract.md` freezes the intended `1.x` compatibility rules at the policy level.
- The blocking gap is also documented in-repo: golden fixtures and compatibility CI are still described as delivery work, not completed qualification evidence.
- That means the project has a compatibility policy and target, but not yet the full verification machinery needed to claim the promise confidently.

## Hard Gates Before 1.0

1. Publish one canonical public contract for transaction and durability semantics, with explicit language for isolation, LWW, lost updates, crash atomicity, and recovery behavior by `WriteOptions`.
2. Implement and gate compatibility verification with released-version fixtures, explicit future-format rejection checks, and release-time compatibility evidence.
3. Tighten the operational envelope for the supported local-first topology so an outside adopter can size, deploy, restart, and diagnose the engine from docs alone.
4. Run at least one release-candidate cycle with the existing qualification gates and no last-minute changes to core durability or supported format semantics.
5. Keep cloud-backed production explicitly out of the `1.0` promise unless the contract, support matrix, qualification gates, and operator docs are all updated to promote it.

## Recommendation

It is reasonable to aim for a local-first `1.0` soon. The repo no longer reads like it needs broad random hardening; it reads like it needs final contract discipline, compatibility verification, and operator-packaging work.

It is not yet justified to claim cloud-backed production `1.0`. The implementation and test surface are substantial, but the authoritative docs still keep cloud-backed production outside the current promised contract.
