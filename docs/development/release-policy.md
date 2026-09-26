# Release Policy

This document defines how Midge moves from development builds to release candidates and stable releases.

## Branching Model

- `develop` (default): active development; feature and fix PRs target this branch and are squash merged after CI passes
- `main`: release branch; promote only through a PR from this repository's `develop` branch, using a merge commit to preserve the tested commit history
- `release-candidate/*`: optional release hardening branches; merge their changes into `develop` before promotion, with frozen durability semantics and format/API changes
- tagged releases: create tags from commits on `main` only after qualification and release checks pass

Only CI runs on PRs into `develop`. Promotion PRs into `main` run CI, OS matrix,
cloud integration, compatibility, repository and Docker qualification, and
CodeQL. Require passing checks on
the current PR head before merging; verify the resulting `main` checks before
tagging or publishing. Scheduled qualification tests the default `develop`
revision. Dispatch the OS matrix and cloud integration workflows against
`main` when qualifying a release.

Promote one PR at a time against the latest `main`. Promotion checks run on
the proposed merge commit. The `main` ruleset does not require `develop` to
contain the previous promotion's merge commit, because that commit exists
only on `main` after a release promotion.

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
