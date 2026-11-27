# Stability Policy

## Current Status

The Midge 0.6 line supports an explicit, opt-in migration from FORMAT 9
to FORMAT 10. It is usable for evaluation and targeted deployments under
the pre-1.0 compatibility policy below.

## What Pre-1.0 Means

- Minor version updates in the 0.x line may include API, operational, or on-disk compatibility changes.
- Patch version updates are intended to be low risk, but the strongest compatibility contract will not exist until 1.0.
- Public docs should describe guarantees only where there is matching implementation and test coverage.
- The crate is pre-1.0; this policy describes the current 0.x compatibility boundary.

FORMAT 10 changes metadata authority, not the SST layout: it keeps SST V5 and
is selected only by calling `Engine::upgrade_format_9`. Existing FORMAT 9
databases and new databases do not migrate automatically. Database formats 1
through 8 remain unsupported, and v0.5.3 cannot open FORMAT 10. The explicit
upgrade, compatibility gate, and rollback boundary are documented in the
[migration guide](../operations/migration-guide.md) and
[format compatibility policy](format-compatibility.md).

## What Is Expected To Stay Stable

- The crate remains focused on single-process embedded storage.
- Durability choices stay explicit through `WriteOptions`; callers must choose the policy they want.
- Upgrade-impacting changes should be called out in the changelog and migration guide.
- The supported and stable production surfaces should be tracked in [support-matrix.md](support-matrix.md).

## What Is Not Frozen Yet

- Pre-1.0 APIs may still be renamed, removed, or tightened.
- Operational guidance may change as crash safety, compaction, and observability harden.
- Storage compatibility guarantees may expand over time, but should not be assumed beyond what current docs and tests state.

Provider-backed cloud storage is supported in the 0.x line and qualified through
Sqrzl. Its pre-1.0 label describes compatibility and operational stability, not
an experimental implementation status. See the
[cloud qualification policy](cloud-qualification-policy.md).

## Upgrade Guidance

Before adopting a new version:

1. Review [../operations/migration-guide.md](../operations/migration-guide.md).
2. Review [../../CHANGELOG.md](../../CHANGELOG.md).
3. Run `midge verify` and revalidate the durability and recovery behavior you depend on in your environment.

## Documentation Rule

Midge docs should not claim production readiness, stability, or durability guarantees that exceed the current tested and documented implementation.

## Release evidence

See [format compatibility](format-compatibility.md), [release policy](release-policy.md),
the [support matrix](support-matrix.md), and the [operator runbook](../operations/operator-runbook.md).
