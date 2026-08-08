# Format Compatibility Policy

This document defines the rules Midge will use to freeze and verify on-disk compatibility on the path to `1.0`.

## Covered Formats

The compatibility policy applies to:

- WAL frames and records
- manifest files and manifest journal
- intent-log persistence format
- SST file layout and footer/version identifiers
- cloud WAL publication catalog and epoch-scoped object-key layout

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

The current cloud WAL layout is publication catalog format v1. Mere object
presence is not authority: recovery reads only entries in
`wal/publication-catalog.v1.json`. The pre-v1 segment-only cloud layout is not
auto-migrated because it cannot prove whether a late stale-writer upload was
accepted before fencing.

The v1 JSON document contains `format_version`, the current `fencing_epoch`,
and a segment-id map. Each segment entry records its writer epoch, maximum
sequence, exact byte length, CRC32C, and canonical epoch-scoped object key.
Unknown versions, malformed entries, a future writer epoch, or WAL objects
without the required catalog fail startup explicitly; salvage mode does not
invent publication authority.

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
