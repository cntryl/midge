# Midge Documentation

Midge is an experimental embedded LSM crate with explicit durability controls and failure-path testing. It is positioned for evaluation by experienced engineers, not as production-ready storage.

## What To Read Before Trying Midge

1. [development/storage-invariants.md](development/storage-invariants.md)
2. [development/architecture.md](development/architecture.md)
3. [development/architecture-diagrams.md](development/architecture-diagrams.md)
4. [user-guides/durability.md](user-guides/durability.md)
5. [development/recovery-internals.md](development/recovery-internals.md)
6. [development/testing.md](development/testing.md)

These documents define what Midge claims, how it implements those claims, and which tests back them up.

For the single-page external behavior contract, read [user-guides/transaction-durability-contract.md](user-guides/transaction-durability-contract.md).

## Documentation Structure

- [user-guides/](user-guides/) for API and operator-facing usage
- [operations/](operations/) for deployment and tuning
- [development/](development/) for architecture, recovery, invariants, and tests
- [transactions-and-mvcc.md](transactions-and-mvcc.md) for transaction semantics and snapshot behavior

## Recommended Reading Paths

### Evaluating Midge

`storage-invariants -> architecture -> durability -> recovery -> testing`

### Contributing To Storage Correctness

`architecture -> architecture-diagrams -> recovery -> testing -> source audit order in architecture`

### General Usage

`overview -> quick-start -> api-guide`

## Important Positioning

- Experimental: yes
- Durability-tested: yes
- Safe enough for careful evaluation: yes
- Production-ready: no

See [development/stability-policy.md](development/stability-policy.md) for the pre-1.0 compatibility contract.

## What To Read Before Calling It Production-Ready

1. [development/one-dot-zero-contract.md](development/one-dot-zero-contract.md)
2. [development/one-dot-zero-readiness-scorecard.md](development/one-dot-zero-readiness-scorecard.md)
3. [development/support-matrix.md](development/support-matrix.md)
4. [development/format-compatibility.md](development/format-compatibility.md)
5. [development/release-policy.md](development/release-policy.md)
6. [operations/production-runbook.md](operations/production-runbook.md)
7. [operations/release-checklist.md](operations/release-checklist.md)
