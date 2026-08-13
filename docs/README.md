# Midge documentation

This documentation describes Midge `0.1.0`, an embedded Rust LSM key-value
engine with MSRV Rust `1.97`. It is the current 0.x contract, not a promise of
long-term API or persisted-format stability. Cloud-backed storage is supported
pre-1.0 and uses self-contained Sqrzl qualification.

## Start here

1. [Overview](user-guides/overview.md)
2. [Quick start](user-guides/quick-start.md)
3. [API guide](user-guides/api-guide.md)
4. [Transaction durability contract](user-guides/transaction-durability-contract.md)

## By audience

- Operations: [operator runbook](operations/operator-runbook.md), [cloud setup](operations/cloud-setup.md), [troubleshooting](user-guides/troubleshooting.md).
- Storage contributors: [architecture](development/architecture.md), [recovery](development/recovery-internals.md), [invariants](development/storage-invariants.md), [testing](development/testing.md), [cloud qualification](development/cloud-qualification-policy.md).
- Release evidence: [support matrix](development/support-matrix.md), [format compatibility](development/format-compatibility.md), [release policy](development/release-policy.md), [release checklist](operations/release-checklist.md).

The root [README](../README.md) is the package-facing entry point. Rust API
documentation and `examples/documented_quick_start.rs` are API authority.
