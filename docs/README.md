# Midge Documentation

Midge is an experimental embedded LSM crate with explicit durability controls and failure-path testing. It is positioned for evaluation by experienced engineers, not as production-ready storage.

## What To Read Before Trying Midge

1. [development/storage-invariants.md](development/storage-invariants.md)
2. [development/architecture.md](development/architecture.md)
3. [user-guides/durability.md](user-guides/durability.md)
4. [development/recovery-internals.md](development/recovery-internals.md)
5. [development/testing.md](development/testing.md)

These five documents define what Midge claims, how it implements those claims, and which tests back them up.

## Documentation Structure

- [user-guides/](user-guides/) for API and operator-facing usage
- [operations/](operations/) for deployment and tuning
- [development/](development/) for architecture, recovery, invariants, and tests

## Recommended Reading Paths

### Evaluating Midge

`storage-invariants -> architecture -> durability -> recovery -> testing`

### Contributing To Storage Correctness

`architecture -> recovery -> testing -> source audit order in architecture`

### General Usage

`overview -> quick-start -> api-guide`

## Important Positioning

- Experimental: yes
- Durability-tested: yes
- Safe enough for careful evaluation: yes
- Production-ready: no

See [development/stability-policy.md](development/stability-policy.md) for the pre-1.0 compatibility contract.
