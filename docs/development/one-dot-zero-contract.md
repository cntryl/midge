# 1.0 Production Contract

This document defines the contract Midge must satisfy before it can be called production-stable at `1.x`.

It is a target contract, not a claim that Midge meets it today.

## Contract Scope

The 1.0 promise applies only to the explicitly supported surface listed here.

### In scope for 1.0

- single-process embedded deployment
- local-disk storage mode
- explicit durability choices through `WriteOptions`
- strict recovery behavior
- on-disk compatibility within the `1.x` major line for supported formats
- upgrade guidance and verification tooling shipped with the crate
- operator-facing metrics and verification APIs documented as stable

### Potentially in scope later, but not promised unless promoted explicitly

- cloud-backed production deployments
- salvage-mode operational use
- any feature still described as experimental in docs or support matrices

## API Compatibility Rules

Within `1.x`:

- patch releases must preserve public API compatibility
- minor releases may add APIs but must not break supported existing APIs
- removals require a prior deprecation period and migration guidance
- behavior changes that affect durability, recovery, or upgrade safety require changelog and migration documentation

## On-Disk Compatibility Rules

Within `1.x`:

- supported WAL, manifest, intent-log, and SST formats must open across patch and minor releases unless docs explicitly declare a migration requirement
- incompatible format changes require a major-version bump or a documented, verified migration path
- unsupported future formats must fail with explicit compatibility errors, never silent fallback

## Durability Contract

The `1.x` contract freezes:

- what `sync()`, `buffered()`, `best_effort()`, and `cloud_strict()` mean
- which crash outcomes are expected for each mode
- which recovery policy behaviors are supported operationally
- which documented guarantees are backed by named tests

## Release Requirements

No release may be labeled production-ready unless all of the following are true:

- trust-critical suite passes on supported production platforms
- compatibility suite passes against supported prior versions
- qualification smoke and release checks pass in CI
- changelog, migration notes, and rollback notes are complete
- known unsupported features are called out explicitly

## Current Status

Today, Midge is still pre-1.0 and should be treated as:

- experimental
- suitable for careful evaluation
- not yet under this full contract

See [stability-policy.md](stability-policy.md) for current status and [support-matrix.md](support-matrix.md) for the supported/unsupported production surface.
