# Support Matrix

This matrix defines what Midge supports today and what must be true before a capability is promoted to production-ready.

## Current Matrix

| Capability | Current status | Production target |
|---|---|---|
| Local single-process embedded mode | Evaluation-ready | Production-ready first |
| Cloud-backed mode | Supported pre-1.0; self-contained Sqrzl qualification is the CI authority | Stabilize compatibility and operational contracts for 1.0 |
| `WriteOptions::sync()` | Supported for local or in-memory mode only | Freeze local-only semantics at 1.0 |
| `WriteOptions::buffered()` | Supported for local or in-memory mode only | Freeze local-only semantics at 1.0 |
| `WriteOptions::best_effort()` | Supported with explicit caveats | Supported only for documented reloadable-data workflows |
| `WriteOptions::cloud_async()` | Supported as the non-blocking cloud-backed durability mode | Freeze semantics at 1.0 |
| `WriteOptions::cloud_strict()` | Supported as the waited-for cloud durability mode | Freeze semantics at 1.0 |
| `RecoveryPolicy::Strict` | Supported | Production recovery default |
| `RecoveryPolicy::Salvage` | Diagnostic/degraded path | Keep out of production contract unless explicitly promoted |
| Verification APIs | Present | Stabilize schemas and operator guidance before 1.0 |
| Offline `midge verify` | Present | Required part of upgrade/recovery workflow before 1.0 |

## Promotion Rules

A capability may move into the stable 1.0 contract only when:

- its semantics are documented in a stable contract
- trust-critical tests cover failure boundaries
- upgrade/rollback behavior is documented and tested
- operator guidance exists for failure handling
- CI and qualification gates exercise it directly

For cloud-backed storage, Sqrzl is the authoritative continuous qualification
environment. Manual real-cloud integration testing validates emulator fidelity
and deployment assumptions; live credentials are not required in repository CI.
See the [cloud qualification policy](cloud-qualification-policy.md).

## Explicit Non-Goals For 1.0 Unless Revisited

- multi-process shared-writer support
- undocumented salvage-based production workflows
- format evolution without compatibility tests
- “best effort” operational guarantees stronger than current docs state

## Reading Order

1. [stability-policy.md](stability-policy.md)
2. [format-compatibility.md](format-compatibility.md)
3. [release-policy.md](release-policy.md)
4. [../operations/operator-runbook.md](../operations/operator-runbook.md)
