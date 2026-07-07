# 1.0 Readiness Scorecard

This is a maintainer-facing readiness assessment for Midge's path to `1.0`.

The initial `1.0` bar is local-first:

- single-process embedded deployment
- local-disk storage mode
- `RecoveryPolicy::Strict`

Cloud-backed production should stay outside the initial `1.0` promise unless
the contract, support matrix, qualification gates, and operator docs are all
updated to promote it.

## Verdict

Midge looks late pre-`1.0`, not early-stage. The repo has a serious
documentation surface, substantial live unit and integration coverage, explicit
recovery and crash-boundary testing, and disciplined benchmark structure. The
remaining risk is mostly contract and qualification discipline rather than broad
missing engineering work.

A local-first `1.0` looks plausible once transaction semantics, compatibility
verification, and operator-envelope guidance are tightened. A cloud-inclusive
`1.0` is not yet justified because the current authoritative docs keep
cloud-backed production outside the promised `1.0` surface.

## Current Assessment

- **Documentation surface:** strong. The project already has contract, release,
  stability, format, support, recovery, and operations docs.
- **Correctness and recovery:** strong. WAL, corruption, restart, no-space,
  transaction crash-boundary, cloud persistence, and adopter smoke suites are
  live tests, not roadmap placeholders.
- **Transaction semantics:** needs final contract discipline. The behavior is
  tested, but the public semantics must remain easy to answer in one canonical
  place.
- **Operational readiness:** needs sharper adopter guidance for sizing,
  startup/recovery expectations, disk pressure, degraded operation, and storage
  latency assumptions.
- **Compatibility promise:** not ready for a `1.x` claim until golden fixtures,
  future-format rejection, and release-time compatibility evidence are gated.
- **Cloud-backed production:** implemented and meaningfully tested, but not yet
  promoted into the `1.0` production contract.

## Hard Gates Before 1.0

1. Publish one canonical public contract for transaction and durability semantics, with explicit language for isolation, LWW, lost updates, crash atomicity, and recovery behavior by `WriteOptions`.
2. Implement and gate compatibility verification with released-version fixtures, explicit future-format rejection checks, and release-time compatibility evidence.
3. Tighten the operational envelope for the supported local-first topology so an outside adopter can size, deploy, restart, and diagnose the engine from docs alone.
4. Run at least one release-candidate cycle with the existing qualification gates and no last-minute changes to core durability or supported format semantics.
5. Keep cloud-backed production explicitly out of the `1.0` promise unless the contract, support matrix, qualification gates, and operator docs are all updated to promote it.

## Recommendation

It is reasonable to aim for a local-first `1.0` soon. The repo no longer reads
like it needs broad random hardening; it reads like it needs final contract
discipline, compatibility verification, and operator-packaging work.

It is not yet justified to claim cloud-backed production `1.0`. The
implementation and test surface are substantial, but the authoritative docs
still keep cloud-backed production outside the current promised contract.
