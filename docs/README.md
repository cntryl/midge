# Midge documentation

Midge is an embedded Rust LSM key-value engine. See the [support matrix](development/support-matrix.md) for the supported toolchain, features, and storage modes.

## Start here

1. [Overview](user-guides/overview.md)
2. [Quick start](user-guides/quick-start.md)
3. [API guide](user-guides/api-guide.md)
4. [Transaction durability contract](user-guides/transaction-durability-contract.md)
5. [Backup and restore](user-guides/backup-and-restore.md)
6. [Storage verification](user-guides/verification.md)

## By audience

- Operations: [operator runbook](operations/operator-runbook.md), [storage verification](user-guides/verification.md), [cloud setup](operations/cloud-setup.md), [troubleshooting](user-guides/troubleshooting.md).
- Storage contributors: [architecture](development/architecture.md), [system invariant catalog and gap analysis](development/invariant-catalog.md), [storage invariants](development/storage-invariants.md), [recovery](development/recovery-internals.md), [testing](development/testing.md), [cloud qualification](development/cloud-qualification-policy.md).
- Release evidence: [support matrix](development/support-matrix.md), [format compatibility](development/format-compatibility.md), [release policy](development/release-policy.md), [release checklist](operations/release-checklist.md), [performance baseline procedure](development/baseline-procedure.md).

The root [README](../README.md) is the package-facing entry point. Rust API
documentation and [the quick-start example](../crates/midge/examples/documented_quick_start.rs) are API authority.
