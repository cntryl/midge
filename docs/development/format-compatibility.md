# Format Compatibility Policy

This document defines the rules Midge will use to freeze and verify on-disk compatibility on the path to `1.0`.

## Covered Formats

The compatibility policy applies to:

- WAL frames and records
- manifest files and manifest journal
- intent-log persistence format
- SST file layout and footer/version identifiers
- cloud WAL publication catalog and epoch-scoped object-key layout

## Current Local Format

Database `FORMAT` version 3 requires SST V4. This is an intentional pre-1.0
break: current binaries reject database FORMAT versions 1 and 2, SST versions
1 through 3, and unknown future versions. There is no legacy decoder or
best-effort fallback in the V4 reader.

SST V4 has the following integrity and compatibility boundaries:

- an 84-byte fixed footer containing its version, encoded footer length,
  self-identifying magic, and CRC32C
- exact, non-overlapping block handles validated against the file length
- a mandatory five-byte compression trailer (`codec` plus CRC32C) on every
  persisted block
- explicit TTL presence, so `None` and `Some(u64::MAX)` remain distinct
- codec identifiers 0 through 3 only: raw, LZ4, Zstd level 3, and Zstd level 9

Removed codec identifiers and malformed or unknown identifiers fail with
corruption or compatibility errors. The reader never retries such bytes as an
uncompressed block.

New writes enforce a 64 MiB encoded-entry admission ceiling, including key and
header bytes, and validate range-tombstone endpoints before staging. This does
not change SST V4. Historical V4 raw blocks larger than that ceiling remain
readable and can be rewritten by budgeted compaction; their output stays raw
even if compression is configured. Compaction still requires enough memory for
its reserved working buffers. Oversized compressed blocks remain corruption
errors under the existing decoded-size limit. See the
[PR #278 compatibility regressions](evidence/pr278-review-fixes.md).

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
presence is not authority: recovery reads only entries in the validated
publication catalog. Midge stores identical primary and recovery-mirror copies
at `wal/publication-catalog.v1.json` and
`wal/publication-catalog.v1.mirror.json`; the mirror is used only when the
primary is missing or malformed. The pre-v1 segment-only cloud layout is not
auto-migrated because it cannot prove whether a late stale-writer upload was
accepted before fencing.

The v1 JSON document contains `format_version`, the current `fencing_epoch`,
and a segment-id map. Each segment entry records its writer epoch, maximum
sequence, exact byte length, CRC32C, and canonical epoch-scoped object key.
Unknown versions, malformed entries, a future writer epoch, or WAL objects
without the required catalog fail startup explicitly; salvage mode does not
invent publication authority. If both catalog copies are invalid, startup
fails closed.

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

The FORMAT 3/SST V4 transition has no in-place upgrade and no binary rollback.
Before installing a FORMAT 3 binary, use the old binary to open the old
database and export its logical column-family key/value contents through the
public API. Import that logical export into a newly created FORMAT 3 database.
Applications that use TTL must also preserve enough application metadata to
reconstruct each remaining expiration; the public scan result intentionally
does not expose internal persisted expiration timestamps. Keep the original
database as the rollback copy. Once a FORMAT 3 binary creates or writes the
new database, an older binary must not open it.

## Fixture Coverage

Repository gates verify, open, and scan a checked-in populated FORMAT 3/SST V4
fixture with a stable logical digest; they also reject a prior FORMAT 2 fixture
and a synthetic future FORMAT 4 fixture. Format changes must update these
fixtures and their CI assertions together.
