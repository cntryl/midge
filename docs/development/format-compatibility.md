# Format Compatibility Policy

This document defines the rules Midge will use to freeze and verify on-disk compatibility on the path to `1.0`.

## Covered Formats

The compatibility policy applies to:

- WAL frames and records
- manifest files and manifest journal
- intent-log persistence format
- SST file layout and footer/version identifiers

## Required Rules For 1.0

- every persistent format must carry an explicit version identifier or versioned decoding path
- current builds must distinguish:
  - supported historical format
  - current format
  - unsupported future format
- unsupported future format must return `CompatibilityError`
- corruption and incompatibility must remain distinct error classes

## Compatibility Guarantees

### Before 1.0

- no blanket on-disk compatibility promise
- any format movement must be called out in the changelog and migration guide

### At 1.0 and Within 1.x

- patch upgrades must preserve supported on-disk compatibility
- minor upgrades must preserve supported on-disk compatibility unless a verified migration step is explicitly required and documented
- incompatible format changes require a major release

## Golden Fixture Requirements

Before 1.0, Midge should maintain fixtures for:

- opening current code on prior released data
- verifying prior released data in strict mode
- recovering from prior released WAL/manifests
- rejecting synthetic future-version data with `CompatibilityError`

## Upgrade and Rollback Rules

- upgrade support must be explicit per release
- rollback support must be explicit per release
- if rollback is unsupported, docs must say so and provide export/import or backup/restore guidance
- `midge verify` should be part of the documented pre-upgrade and post-upgrade workflow

## Current Gap

This policy is the contract target. The full golden-fixture and compatibility CI implementation is still a delivery item, not a completed guarantee.
