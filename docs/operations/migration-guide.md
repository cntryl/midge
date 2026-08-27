# Migration guide

Midge `0.1.0` is a 0.x crate. Treat upgrades as application migrations: read
the release notes, inspect [format compatibility](../development/format-compatibility.md),
run compatibility and recovery tests, and keep a verified application-level
backup before changing binaries.

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
