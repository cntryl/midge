# Format Compatibility Policy

This document defines the rules Midge will use to freeze and verify on-disk compatibility on the path to `1.0`.

## Shared specification target

The FORMAT 8 baseline is the normative profile in
[`cntryl/lsm-spec`](https://github.com/cntryl/lsm-spec) at commit
`111bccdd5fa6c9ea2d13d1131b17807016c770c` (`1.0.0-dev`). Keep this reference
aligned with the commit recorded in Pants's Midge test-parity inventory.
The local 0.5.0 candidate extends that baseline with FORMAT 9 below; shared
specification parity and release qualification for that extension are pending.

## Covered Formats

The compatibility policy applies to:

- WAL frames and records
- manifest files and manifest journal
- intent-log persistence format
- SST file layout and footer/version identifiers
- cloud WAL publication catalog and epoch-scoped object-key layout
- cloud lease metadata authority and immutable control-metadata generations

## Current Local Format

Database `FORMAT` version 9 is the current local candidate. Version 5 named the WAL's history
with a segment ledger, `wal/LEDGER` (#842): a checksummed journal that records
which `wal/{id:020}.wal` files are history, their order, the next segment id and
the retirement point. Segments carry their final name from creation; there is
no active `wal.log` and no rename on rotation, and recovery never lists `wal/`
to decide what to replay.

Version 6 (#843) links the segments themselves and moves the writer epoch out
of the records:

- **Segment format v2.** Every segment starts with a 128-byte header (magic
  `MWSEGHDR`, version 2, segment id, writer epoch, holder, first sequence, the
  link of the segment before it, and a CRC32C). A sealed segment ends with a
  footer frame naming its own link: its id, last sequence and an xxh3-128
  digest of every byte before the footer, with its record count and
  per-family maximum sequences. Nothing may follow the footer. Recovery
  admits a segment only when its header names the segment the ledger (or,
  for cloud objects, the catalog) expects, its link matches the segment
  before it and its epoch does not go backwards. A cloud object must be a
  sealed v2 segment whose header id and epoch match its catalog entry.
- **Record format v2.** A WAL record no longer carries a writer epoch; the
  segment header does. Transaction frames carry their transaction's column
  family, and every operation in a transaction batch must belong to it.
- **LEDGER v2.** Members record their header facts and, once sealed, their
  footer facts; the ledger keeps the link of the last retired segment as its
  anchor and a durable sequence reservation, so a sequence the engine handed
  out is never handed out again after a crash.

Version 7 (#844) chains the cloud WAL publication catalog and drops the
metadata compatibility paths older databases needed:

- **Catalog v2.** The cloud WAL publication catalog is a chain like the
  ledger, with an anchor, a next segment id and a sequence floor. Its
  document is format version 2; see the catalog description below.
- **No compatibility encodings.** Every manifest journal record must carry
  its edit envelope; a record without one is corruption. Manifest key bounds
  decode only as hex strings; the byte-array form written before FORMAT 4 is
  rejected.
- **Removed metadata.** The manifest no longer has `next_wal_seq` or
  `cloud_checkpoint`, and the `SetCloudCheckpoint` journal record is gone.
  Maintenance no longer re-summarizes SSTs whose manifest entry lacks
  complete key bounds.

Version 8 (#819) replaces SST value-by-value WAL coverage with durable per-family
flush frontiers. The manifest requires canonical `flushed_through` rows sorted by
family id: default family 0 starts at zero, every created family starts at zero,
and a dropped family retains `u64::MAX` permanently. A flush journals name
allocation, `AddSst`, and `AdvanceFlushFrontier` in one atomic batch; the frontier
is the frozen commit boundary, including best-effort writes. Compaction and SST
reclamation leave these frontiers unchanged. Missing, duplicate, unsorted or
inconsistent rows are corruption in strict and salvage recovery.

Catalog v3 persists each sealed segment's record count and canonical per-family
maximum sequences. Retirement requires every family maximum to be covered by
the committed shared manifest generation. The catalog records the generation
authorizing retirement; physical deletion is conditional on that exact WAL
object identity. Shared metadata publication order is a separate checked ordinal
from the manifest journal horizon. New generations preserve every old frontier,
never regress live families, and never revive dropped families.

Version 9 (#953) stores manifest snapshots and manifest journal payloads in a
shared binary codec. MessagePack maps retain explicit field names and encode
integers and key bytes compactly. Each frame records its kind and length and
has a big-endian CRC32C over the header and exact payload. Checksum byte order
preserves a useful checksum over the complete encoded object, including its
checksum field. Length fields remain little-endian. The same decoder reads local
checkpoints and cloud metadata bodies. Decoding checks the complete extent,
checksum and nesting bound before applying semantic manifest validation.

The intent file is an append-only sequence of binary suffix edits. Each edit
names its predecessor checksum and next sequence, retains a prefix of the prior
intent set and records the replacement suffix. A torn final append retains the
prior complete intent set; an invalid complete frame or predecessor chain fails
recovery. A subsequent append first durably repairs any torn tail. At 1 MiB or
1,024 records the log atomically checkpoints its current intent set. Existing
metadata filenames retain their historical `.json` suffix, but their contents
are binary and there is no JSON fallback under FORMAT 9.

Version 9 is an intentional pre-1.0 break with no in-place upgrade and no
migration: current binaries reject database FORMAT versions 1 through 8 with
`CompatibilityError`, and leave the marker untouched. Cloud databases are
rejected the same way, because their hydrated metadata carries the marker,
and a publication catalog of any other format version fails with
`CompatibilityError` and neither catalog copy changes. FORMAT 9 requires SST
V5; current binaries reject SST versions 1 through 4 and unknown future
versions. There is no legacy decoder or best-effort fallback in the V5
reader.

SST V5 has the following integrity and compatibility boundaries:

- an 84-byte fixed footer containing its version, encoded footer length,
  self-identifying magic, and CRC32C
- exact, non-overlapping block handles validated against the file length
- a mandatory five-byte compression trailer (`codec` plus CRC32C) on every
  persisted block
- explicit TTL presence, so `None` and `Some(u64::MAX)` remain distinct
- codec identifiers 0 through 3 only: raw, LZ4, Zstd level 3, and Zstd level 9
- canonical unsigned varint shared-prefix, key-delta and value lengths, an
  eight-byte sequence, and one type/TTL flag byte; an expiration contributes
  eight bytes only when present
- an independent key every 16 entries and a checked restart-offset directory
  at the end of each data block; the high bit marks a continued user-key group

Removed codec identifiers and malformed or unknown identifiers fail with
corruption or compatibility errors. The reader never retries such bytes as an
uncompressed block.

New writes enforce a 64 MiB complete single-entry block admission ceiling,
including the key, worst-case TTL and restart trailer, and validate
range-tombstone endpoints before staging. Compressed decode limits and
compaction working-buffer admission remain enforced. V4 bytes require a
logical export with a V4-capable binary and import into a fresh database.

Point reads binary-search independent restart keys. For unique keys, decoding
cost is at most the binary-search probes, one 16-entry interval and one
lookahead entry. Equal keys can cross any number of restarts: search finds the
first equal restart and its continuation bit includes the preceding interval
when the group began there. The bound is therefore the search probes plus at
most 16 preceding entries, all versions of that user key, and one lookahead;
an older snapshot cannot skip a visible version. Reverse scans decode one
restart interval at a time, retaining the existing snapshot, tombstone and TTL
rules. Small entries without TTL use a 12-byte header instead of 26 bytes;
the directory adds four bytes per 16 entries and a four-byte count per block.

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

The current cloud WAL layout is publication catalog format version 3 (#819).
Mere object presence is not authority: recovery reads only entries in the
validated publication catalog. Midge stores identical primary and
recovery-mirror copies at `wal/publication-catalog.v1.json` and
`wal/publication-catalog.v1.mirror.json`; the `v1` in the object names is not
the document's format version. The mirror is used only when the primary is
missing or malformed. The pre-v1 segment-only cloud layout is not
auto-migrated because it cannot prove whether a late stale-writer upload was
accepted before fencing.

The version 3 JSON document is a chain, like the WAL ledger. It contains
`format_version`, the current `fencing_epoch`, an `anchor`, a
`next_segment_id`, a `seq_floor`, the `retired_by` shared-generation id, and
`segments`, oldest first. The anchor is the link of the last segment the
catalog retired, or the genesis link. Each entry records its writer epoch, the
link of the segment before it (`prev`), its sealed facts (`link`, record count,
and canonical per-family maximum sequences), and its object's exact byte
length and CRC32C; the epoch-scoped object key derives from the id and epoch.
The first entry links the anchor and each later one links the entry before it.
Epochs never fall and none passes the fencing epoch. `next_segment_id` passes
the head and never falls, so retiring entries never loses the chain or hands
out a published id again. Retirement removes only the oldest entries and moves
the anchor; salvage removes only a suffix and raises `seq_floor` over what it
set aside. A cloud object must be the sealed segment its entry names: the same
size and CRC32C, and a header and footer naming the entry's epoch, `prev` and
`link`.

A catalog of any other format version fails startup with `CompatibilityError`;
Midge neither repairs it from the mirror nor converts it, and older binaries
cannot read a version 3 catalog. Malformed entries, a broken chain, a future
writer epoch, or WAL objects without the required catalog fail startup
explicitly; salvage mode does not invent publication authority. If both
catalog copies are invalid, startup fails closed. Metadata bootstrap refuses
any catalog that is not pristine: a catalog whose entries have all retired
still holds history.

Provider-backed cloud metadata uses layout v2, lease document version 4 and
initialization sentinel version 3. A separately checksummed committed pointer
at `metadata/committed-pointer.v1` selects the exact closure of immutable
snapshots and edits under `manifest/`. Heartbeat renewal changes the lease
identity without changing the pointer identity. Acquiring a successor lease
must CAS-claim the pointer with its epoch, holder and owner token while
preserving committed predecessor history. An uploaded ordinal, LIST order,
legacy `metadata/generations/` objects and mutable metadata copies grant no
recovery authority. See [the publication protocol](cloud-metadata-914.md).
The cloud DDL registry remains a
separate conditional authority. Its version 2 wire format records the active
lease epoch and omits the legacy required `epoch` field, so older writers cannot
parse or rewrite it. A successor conditionally fences the registry before
recovery; a predecessor write that wins first is incorporated before serving.
The `metadata/authority-initialized.v1` marker starts pending and becomes active
only after the lease and initial pointer claim complete. Loss of a lease or
committed pointer with an active marker is a fail-closed condition. A version
4 lease document intentionally cannot be parsed by older binaries, which
prevents an old writer from renewing the new lease. Legacy lease/sentinel
versions do not use the pending-initialization repair path.

Legacy provider-backed cloud databases require offline logical export and
import into a new empty prefix as described in the
[cloud setup guide](../operations/cloud-setup.md). Current binaries reject
legacy lease documents and uncommitted legacy metadata. Binary rollback against
the new prefix is unsupported; keep the original prefix as the rollback copy.

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

Compatibility tests verify the checked-in populated FORMAT 9/SST V5 fixture
at `tests/fixtures/compatibility/v9_populated_v5_sst_db`. They
also open a copy with strict recovery and read its persisted value across a
shutdown and reopen. This fixture records new writer-produced SST bytes with
their finalized size/CRC, binary key bounds and per-family frontiers, and omits the
pre-ledger WAL.

Repository tests retain and reject the FORMAT 9/SST V4 fixture under strict
and salvage recovery. They also reject the populated FORMAT 8 and FORMAT 3/SST V4 fixtures, a prior FORMAT 2
fixture and a synthetic future FORMAT 10 fixture with `CompatibilityError`,
without rewriting their markers. Format changes must update these fixtures and
their CI assertions together.
