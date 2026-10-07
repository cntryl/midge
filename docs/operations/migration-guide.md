# Migration guide

Midge is a 0.x crate. Treat upgrades as application migrations: read
the release notes, inspect [format compatibility](../development/format-compatibility.md),
run compatibility and recovery tests, and keep a verified application-level
backup before changing binaries.

## `midge verify --json` schema version 1

The CLI success and error JSON objects include `schema_version: 1`. Consumers
of earlier unversioned output should update to require schema version 1 and
ignore unknown fields. The version applies to the CLI JSON contract, not to the
crate version or the public Rust `StorageVerificationReport` type. See the
[storage verification guide](../user-guides/verification.md) for the complete
field, error, exit-code, and local/cloud coverage contract.

## 0.1.0 to 0.1.1

Version `0.1.1` is a durability-correctness patch with no public API or
persisted-format change. Stop writers, complete `engine.shutdown(timeout)`,
preserve the database directory and relevant cloud prefix, then replace the
binary or crate version. Existing FORMAT 3, SST V4, manifest, and WAL data can
be opened directly without conversion.

Rollback from `0.1.1` to `0.1.0` is supported after a clean shutdown because
both versions write the same persisted formats. Do not roll back a live or
still-fenced process in place: stop it first and retain all WAL files so the
selected binary can perform ordinary restart recovery.

## 0.1.1 to 0.2.0

Version `0.2.0` introduces database FORMAT 4. FORMAT 3 remains readable, but a
writable `0.2.0` open upgrades its marker to FORMAT 4 in place before writing
manifest state. The manifest key-bound encoding changes from JSON byte arrays
to hex strings; both formats use SST V4. A read-only `midge verify` does not
upgrade the marker.

Before upgrading, stop writers, complete `engine.shutdown(timeout)`, and
preserve the full database directory and relevant cloud prefix. Test with a
separate copy, run `midge verify`, and exercise application reads, writes, and
restart recovery before switching traffic. Keep the original copy until the
new version is qualified in your environment.

Rollback has constraints. Do not open a database that `0.2.0` has writable-
opened with `0.1.1`; the older binary does not support FORMAT 4. To roll back,
restore the pre-upgrade database copy and use `0.1.1`. Any writes made after
that copy was taken will be absent from the restored database. Preserve a
logical export or application-level recovery path if those writes must be
retained.

## 0.2.0 to 0.3.0

Local databases remain at FORMAT 4 and SST V4. Quiesce writes, complete
`engine.shutdown(timeout)`, and preserve a verified copy of the full database
directory before upgrading. Run `midge verify` on a separate copy and test
application reads, writes, and restart recovery before switching traffic.
`midge verify --json` now uses schema version 1; update consumers of its
previously unversioned output as described above.

Provider-backed cloud storage changes its control metadata authority. Version
`0.3.0` commits immutable `FORMAT`, manifest, journal, and intent files under
`metadata/generations/` through a version 2 lease descriptor and uses a
version 2 DDL registry. It rejects the legacy lease and mutable metadata used
by `0.2.0`. There is no safe in-place upgrade of that prefix:

1. Quiesce application writes. While `0.2.0` can still read the old database,
   export every column family's logical key/value contents through its public
   API. Preserve application metadata needed to reconstruct TTL expiration;
   public scans do not expose the internal expiration timestamps.
2. Complete `engine.shutdown(timeout)` with `0.2.0`, stop all old writers, and
   preserve the entire original cloud prefix and local cache as the rollback
   copy.
3. With `0.3.0`, create a new empty cloud prefix and fresh local cache. Recreate
   the column families and import the logical data. Do not copy the old lease,
   DDL registry, WAL, or mutable metadata objects into the new prefix.
4. Test reads, writes, restart recovery, and recovery after local-cache loss
   before switching clients. Use the open-engine storage verifier for cloud
   diagnostics; the path-only `midge verify` command cannot inspect remote
   authority. Complete the required Sqrzl and deployment-specific qualification.

Rollback is supported with constraints. For a local database, restore the
verified pre-upgrade copy and use `0.2.0`. For cloud, binary rollback against
the new prefix is unsupported: return to the preserved original prefix and
its `0.2.0` binary. Never let an old writer open the new prefix. Writes made
after cutover are absent from the preserved copy and require a separate
application-level reconciliation path.

## 0.3.0 to 0.3.1

Version `0.3.1` preserves public Rust signatures and error variants, CLI JSON
schema version 1, FORMAT 4, SST V4, and cloud lease, DDL, and WAL catalog formats.
No offline export/import is required for a `0.3.0` database. Stop writes,
complete shutdown, and preserve a verified copy of the database directory and
relevant cloud prefix. Test Local and provider-backed recovery on a separate
copy before switching traffic.

Rollback is supported with constraints: restore the preserved pre-upgrade copy
and use `0.3.0`. Do not treat a salvage-mutated database as a qualified rollback
fixture. Writes made after that copy was taken need separate reconciliation.
These format statements do not remove the authority and recovery defects fixed
in `0.3.1`; returning to `0.3.0` also returns to its known defects.

## 0.3.1 to 0.3.2

Version `0.3.2` preserves FORMAT 4, SST V4, CLI JSON schema version 1, and cloud
lease, DDL, and WAL catalog formats. Existing Rust call sites and error variants
remain compatible; new options and diagnostics are additive. No offline
export/import is required for a `0.3.1` database. Stop writes, complete
`engine.shutdown(timeout)`, and preserve a verified copy of the database
directory and relevant cloud prefix. Exercise reads, writes, and restart
recovery on a separate copy before switching traffic.

Cloud transactions are invalid after lease loss. New transactions, point reads,
new scans, and the next advance of an active iterator return
`MidgeError::Fenced`, including when monotonic lease validity expires before
the lease-loss callback runs. A successor may reclaim the predecessor's remote
SSTs; process-local snapshot pins do not keep them readable across takeover.
Discard fenced transactions, shut down the predecessor, and reopen under a
healthy writer. Local and in-memory snapshot behavior is unchanged.

`OpenOptionsBuilder::open_timeout(Duration)` optionally bounds one aggregate
startup attempt. It defaults to no aggregate deadline. A timeout does not prove
that an entered syscall or provider mutation stopped; the startup worker retains
ownership while it settles. An unresolved acquisition can return
`LeaseIndeterminate`, and an immediate replacement may encounter a retained
lease. See the [API guide](../user-guides/api-guide.md) for retry and ownership
boundaries. Manual compaction now carries its original caller deadline through
publication rather than granting later phases a fresh allowance.

Rollback is supported with constraints: restore the preserved pre-upgrade copy
and use `0.3.1`. Salvage-mutated databases are excluded from rollback claims.
Writes made after that copy was taken require separate reconciliation. Returning
to `0.3.1` also restores its known lease, progress, and cloud read-authority
defects.

## FORMAT 3 and SST V4

FORMAT 3 is a breaking local-storage transition. It requires checksummed SST
V4 files and rejects FORMAT 1/2 databases and SST V1-V3 files. There is no
in-place conversion and no legacy read fallback.

Migrate with the old binary while it can still read the source database:

1. Stop writers and complete `engine.shutdown(timeout)`.
2. Preserve the entire old database as a rollback copy.
3. With the old binary, enumerate every application column family and export
   every logical key/value pair through the public transaction/scan API.
4. Create a new empty database with the new binary and recreate the column
   families, then import the exported values.
5. Run `midge verify` and application read/restart tests against the new
   database before switching traffic.

TTL expiration timestamps are internal metadata and are not returned by a
public scan. Applications that need to preserve TTL during this logical
migration must export their own expiration source of truth and reconstruct the
remaining TTL when importing. Do not overwrite the old database: it is the
only binary rollback path. Older binaries cannot open a FORMAT 3 database.

## Cloud provider configuration

The provider-backed cloud configuration API intentionally changed before
1.0. Replace `CloudStorageBuckets` with one `CloudStorageLocation` passed to
`OpenOptions::cloud`. If separate locations remain necessary, construct a
`CloudStorageTopology`, apply the per-class overrides, and pass it to
`OpenOptions::cloud_multi`.

Direct field construction and field pattern matching on `CloudProviderConfig`
is no longer supported. Construct `AwsS3Config`, `AzureBlobConfig`, `GcsConfig`,
`OciObjectStorageConfig`, or `S3CompatibleConfig`, then pass it directly to
`CloudStorageLocation::new`; the existing unambiguous
`CloudProviderConfig::aws_s3`, `azure_blob`, `gcs`, and related helpers remain.
Credential and endpoint modifiers now live on their provider-specific config,
which prevents cross-provider credential combinations. `OpenOptions::build`
performs structural validation only and may therefore reject names or endpoints
that older releases deferred until startup.

## Cloud WAL publication catalog v1

Cloud WAL publication catalog format v1 is a breaking persisted-layout
change. Sealed objects now use
`wal/epochs/<writer-epoch>/<segment-id>.wal`, and
`wal/publication-catalog.v1.json` and its identically encoded
`wal/publication-catalog.v1.mirror.json` recovery copy represent the sole
authority for remote WAL recovery. Current Midge releases create the mirror
when opening a valid primary-only v1 deployment. A database prefix that
contains the older segment-only `wal/<segment-id>.wal` layout without a v1
catalog is rejected explicitly;
Midge does not guess whether those objects were published before or after a
lease takeover. Epoch-scoped WAL objects without the catalog are also rejected
as ambiguous instead of being silently ignored during catalog initialization.
Preserve the old database, open/export it with a compatible release, then
import into a new prefix. Do not synthesize a catalog by hand.

Migrate cloud WAL state as a logical database move:

1. Stop writers and complete `engine.shutdown(timeout)` with the compatible old
   binary.
2. Preserve the old database prefix and local cache as the rollback copy.
3. Export every column family's logical key/value contents while the old binary
   can still recover the segment-only layout.
4. Create a new empty prefix with the new binary, recreate the column families,
   and import the logical data.
5. Verify reads, writes, restart recovery, and the required Sqrzl or real-cloud
   qualification before switching traffic.

Rollback is unsupported within a migrated prefix. Roll back by restoring
traffic to the preserved old prefix with its compatible binary; do not let an
older binary open or write the new prefix.

## General rollout checks

For any Midge binary upgrade:

1. Stop writers and complete `engine.shutdown(timeout)`.
2. Preserve the database directory, local WAL, and relevant cloud prefix as a
   recoverable copy.
3. Test the new binary against a separate copy with verification and
   compatibility checks.
4. Roll forward only after reads, writes, restart recovery, and required cloud
   qualification pass.

If the application intentionally abandons a database, recreate it from its
source of truth after preserving any required evidence. That is not a generic
repair step.
