# Stability Policy

## Current Status

Midge is in the 0.1 release line. It is usable for evaluation and targeted deployments, but it is not yet making a blanket 1.0-style compatibility promise.

## What Pre-1.0 Means

- Minor version updates in the 0.x line may include API, operational, or on-disk compatibility changes.
- Patch version updates are intended to be low risk, but the strongest compatibility contract will not exist until 1.0.
- Public docs should describe guarantees only where there is matching implementation and test coverage.

## What Is Expected To Stay Stable

- The crate remains focused on single-process embedded storage.
- Durability choices stay explicit through `WriteOptions`; callers must choose the policy they want.
- Upgrade-impacting changes should be called out in the changelog and migration guide.

## What Is Not Frozen Yet

- Pre-1.0 APIs may still be renamed, removed, or tightened.
- Operational guidance may change as crash safety, compaction, and observability harden.
- Storage compatibility guarantees may expand over time, but should not be assumed beyond what current docs and tests state.

## Upgrade Guidance

Before adopting a new version:

1. Review [../operations/migration-guide.md](../operations/migration-guide.md).
2. Review [../../CHANGELOG.md](../../CHANGELOG.md).
3. Revalidate the durability and recovery behavior you depend on in your environment.

## Documentation Rule

Midge docs should not claim production readiness, stability, or durability guarantees that exceed the current tested and documented implementation.