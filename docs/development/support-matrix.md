# Support Matrix

This matrix defines what Midge supports today and what must be true before a capability is promoted to production-ready.

## Current Matrix

| Capability | Current status | Production target |
|---|---|---|
| Local single-process embedded mode | Evaluation-ready | Production-ready first |
| Cloud-backed mode | Experimental/evaluation | Promote only after parity, lease, upload, and qualification gates pass |
| `WriteOptions::sync()` | Supported | Freeze semantics at 1.0 |
| `WriteOptions::buffered()` | Supported | Freeze semantics at 1.0 |
| `WriteOptions::best_effort()` | Supported with explicit caveats | Supported only for documented reloadable-data workflows |
| `WriteOptions::cloud_strict()` | Supported for evaluation | Production-ready only if cloud-backed mode is promoted |
| `RecoveryPolicy::Strict` | Supported | Production recovery default |
| `RecoveryPolicy::Salvage` | Diagnostic/degraded path | Keep out of production contract unless explicitly promoted |
| Verification APIs | Present | Stabilize schemas and operator guidance before 1.0 |
| Offline `midge verify` | Present | Required part of upgrade/recovery workflow before 1.0 |

## Promotion Rules

A capability may move from experimental to production-ready only when:

- its semantics are documented in a stable contract
- trust-critical tests cover failure boundaries
- upgrade/rollback behavior is documented and tested
- operator guidance exists for failure handling
- CI and qualification gates exercise it directly

## Explicit Non-Goals For 1.0 Unless Revisited

- multi-process shared-writer support
- undocumented salvage-based production workflows
- format evolution without compatibility tests
- “best effort” operational guarantees stronger than current docs state

## Reading Order

1. [one-dot-zero-contract.md](one-dot-zero-contract.md)
2. [format-compatibility.md](format-compatibility.md)
3. [release-policy.md](release-policy.md)
4. [../operations/production-runbook.md](../operations/production-runbook.md)
