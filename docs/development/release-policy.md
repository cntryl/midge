# Release Policy

This document defines how Midge moves from development builds to release candidates and stable releases.

## Branching Model

- `main`: active development
- `release-candidate/*`: release hardening branches with frozen durability semantics and format/API changes
- tagged releases: only after qualification and release checks pass

## Versioning Rules

### Pre-1.0

- patch releases should be low-risk
- minor releases may still change API or on-disk behavior, but changes must be documented

### 1.x

- patch: bug fixes and low-risk operational/documentation changes only
- minor: additive changes allowed, no breaking supported API or format changes
- major: required for breaking API or supported format changes

## Mandatory Gates

Every release candidate must pass:

- `cargo clippy --all-targets -- -D warnings`
- full test suite
- trust-critical smoke suite
- compatibility smoke suite
- release check script
- documentation consistency review

## Qualification Evidence

Before promoting a release candidate:

- attach qualification results
- attach migration note
- attach rollback statement
- attach known-risk summary

## Rollback Rule

Every release note must explicitly state one of:

- rollback supported
- rollback supported with constraints
- rollback unsupported; restore from backup/export only

## 1.0 Declaration Rule

Do not declare `1.0` until there has been at least one release-candidate cycle with:

- no core durability semantic changes
- no supported format changes
- stable migration and rollback guidance
- qualification gates passing across supported production platforms
