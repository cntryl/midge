# Cloud setup

Cloud-backed storage is a supported pre-1.0 capability. Midge continuously
qualifies its provider protocol paths and engine recovery behavior through the
Sqrzl multi-provider emulator. Pre-1.0 means that APIs, persisted formats, and
operational guidance can still evolve; it does not mean the cloud path is
experimental.

Operators must validate deployment-specific credentials, IAM, network policy,
provider configuration, quotas, and workload capacity. See the canonical
[cloud qualification policy](../development/cloud-qualification-policy.md).

## Feature selection

Use the provider feature required by the build (`cloud-aws`, `cloud-azure`,
`cloud-gcp`, or `cloud-oci`) and enable `cloud-common` when composing a custom
feature set. The default feature set currently includes the cloud integrations.
`CloudSimulated` needs no external provider and is backed by local filesystem
state, so it is suitable for deterministic tests rather than service
qualification.

`cloud-oci` owns `OciObjectStorageConfig`. Midge derives OCI's commercial-realm
S3-compatible endpoint from its namespace and region. For government,
sovereign, or dedicated realms, set the realm-specific S3 Compatibility API
origin with `OciObjectStorageConfig::with_endpoint`. OCI uses the shared S3
protocol implementation while retaining OCI-specific validation and diagnostics.

## Configuration

Construct `CloudProviderConfig` with the provider-specific public credential
source types. Prefer environment, shared-profile, workload-identity, or the
provider's documented default chain over putting secrets in source. Midge does
not manage secret rotation, a secret store, IAM policy, or network access.
Never commit credentials or place them in a checked-in example.

### Azure protocol version

Midge pins Azure Blob operations to `2026-10-06`. Shared Key, OAuth and managed
identity requests send that `x-ms-version`. SAS requests select it through
`api-version`; the signed `sv`, permissions and encoded signature remain
unchanged. An older `api-version` embedded in a SAS is replaced by Midge's pin.

The [Azure versioning documentation](https://learn.microsoft.com/en-us/rest/api/storageservices/versioning-for-the-azure-storage-services)
lists `2026-10-06` as the GA SDK default, with a small number of tenants in
`useast2euap` and `uscentraleuap` still limited to `2026-04-06` at the
2026-10-24 review. Verify service-version support for the target account;
Midge does not silently downgrade the protocol after a failed request.

Qualify the selected version in a dedicated test prefix using authenticated
writes and reads, conditional create, matching and stale ETags, conditional
deletion, and engine recovery after local cache loss. A successful HEAD or
read-only preflight alone does not establish those properties. Local signer
tests and Sqrzl qualification exercise the modeled contracts; deployment
credentials, permissions and live Azure acceptance need separate evidence.

The recommended deployment uses one bucket or container and one database
prefix. Midge keeps WAL, SST, metadata, and lease keys beneath that prefix.
Keep versioning disabled by default. To use AWS Backup for S3, follow the
[S3 versioning and lifecycle guidance](#s3-versioning-and-lifecycle).
Use a multi-location topology only when separate IAM, ownership, or lifecycle
boundaries are operationally valuable.

```rust,no_run
# use cntryl_midge::{CloudProviderConfig, CloudStorageLocation, OpenOptions};
let location = CloudStorageLocation::new(
    CloudProviderConfig::aws_s3("midge-data", "us-east-1"),
    "database-a",
);
let options = OpenOptions::cloud("/var/lib/midge-cache", location)
    .local_storage_budget(20 * 1024 * 1024 * 1024)
    .build()?;
# Ok::<(), cntryl_midge::MidgeError>(())
```

`OpenOptions::build` automatically validates names, endpoints, credential
pairings, and prefixes without reading environment variables or credential
files and without making network requests. Before deployment, explicitly call
`location.preflight(CloudPreflightOptions::default())` (or preflight the full
topology). Preflight resolves the production backend, lists the namespace, and,
when an object exists, performs HEAD plus a zero- or one-byte bounded GET. Its
serializable report is redacted and distinguishes structural validity,
deployment readiness, and complete read verification. An empty namespace can
be ready but cannot be fully verified.

Preflight is deliberately read-only. It does not prove PUT, conditional-write,
fencing, CAS, or DELETE permission or semantics; Sqrzl qualification remains
authoritative for those behaviors.

The shared location contains `wal/`, `sst/`, `metadata/`,
`wal/publication-catalog.v1.json`, `metadata/ddl.registry.json`, and
`midge_primary_lease.json`. Keep all keys that the current recovery point
references. Take the original writer out of service before routing traffic
to a restored namespace, which has its own lease.

Provider-backed databases fence writers with the lease. The committed
metadata pointer selects the immutable metadata required for recovery.
The [format compatibility policy](../development/format-compatibility.md)
describes the current lease, pointer, and format gates.
The remote DDL registry uses a strict version 2 document. On takeover, the new
holder conditionally rewrites it with its lease epoch before recovery or serving
requests; a delayed conditional write from the prior holder cannot land after
that fence. A write that wins earlier is read and reconciled by the successor.
The `metadata/authority-initialized.v1` marker becomes permanent and active
after the first lease and pointer claim complete. An active marker prevents a
missing lease or pointer from being mistaken for a new database. Retain the
marker, current lease and pointer, and objects selected by the pointer.

An existing cloud database with a legacy lease and mutable `metadata/` files
requires an **offline migration to a new, empty database prefix** before this
version can write it. Stop every old writer, use the previous binary to export
the logical contents through its public API, then import them with this version
into the new prefix and a fresh local cache. Preserve application data needed
to reconstruct TTL expirations. Verify the imported data before switching
clients, and retain the original prefix as the rollback copy. Current writers
reject a legacy lease. Do not copy the legacy lease, DDL registry, or mutable
metadata objects into the new prefix. There is no safe automatic in-place
upgrade while an old writer might resume a metadata PUT.

For advanced routing, start with `CloudStorageTopology::new(shared)`, override
individual locations with `with_wal`, `with_sst`, or `with_control`, and pass
the result to `OpenOptions::cloud_multi`.

Use `WriteOptions::cloud_async()` when local acknowledgement may precede remote
upload, or `WriteOptions::cloud_strict()` when the commit must wait for the
cloud upload. These choices are meaningful only with cloud-backed storage.

## Cloud request volume

Each upload asks the provider to prove what it stored, and keeps the stored
identity from the response:

| Provider | Upload check | Identity |
|---|---|---|
| AWS S3 | `x-amz-checksum-crc32c`, validated by S3 | `ETag` |
| Azure Blob | `Content-MD5`, validated by Put Blob | `ETag` |
| GCS (JSON and XML) | CRC32C GCS computes over the stored object | generation |
| S3-compatible, OCI | opt-in, see below | `ETag` |

With a receipt, the engine skips reading the upload back. Once the WAL
catalog is warm, publishing one segment normally makes three requests: the
immutable WAL PUT and conditional PUTs of the primary catalog and its mirror.
Catalog identities are reused only after confirmed writes; conflicts, missing
receipts, and uncertain outcomes require fresh reads. A response without a
checksum falls back to verified readback, and a checksum that names other
bytes fails the upload. Startup, lease renewal, SST publication, and WAL
retirement add their own requests.

Generic S3-compatible endpoints and OCI keep verified readback by default,
because an endpoint may echo `x-amz-checksum-crc32c` without validating it.
After confirming an endpoint rejects a PUT whose body does not match that
header, enable receipts with `S3CompatibleConfig::with_checksum_receipts(true)`
or `OciObjectStorageConfig::with_checksum_receipts(true)`.

Sealing, upload admission, and acknowledgement check the writer's monotonic
lease window. The heartbeat renews that window, and conditional catalog
publication rejects an epoch superseded by another writer. WAL write-group
admission still checks remote ownership before applying writes, so a stale
writer is rejected before a local WAL effect. This check is separate from
the three publication requests above.

The default WAL seal delay remains 500 ms, with size and write-count limits
also triggering seals. `OpenOptions::cloud_write_policy` can change that delay;
a longer delay increases the time before asynchronous writes become cloud
durable and can increase strict-commit latency. Request reduction does not
require changing this durability timing.

## Object versioning and lifecycle rules

Provider lifecycle policy is bucket/container provisioning, not a Midge data
plane operation. Midge does not query, warn about, or reject provider
versioning state. Prefer versioning disabled. If operators enable versioning,
Midge exposes stable suffixes through `CloudObjectLayout` so provisioning can
bound cleanup of noncurrent versions. Disable GCS soft delete and Azure
Blob soft delete for the Midge location; otherwise deleted objects continue to
consume storage until the provider retention period expires. GCS enables soft
delete with a seven-day retention period on new buckets by default, unless an
organization policy or explicit bucket setting changes it.

Native AWS configuration uses virtual-hosted HTTPS endpoints for ordinary
bucket names. Because standard wildcard certificates do not match dotted
bucket names, Midge automatically uses AWS path-style addressing for them. S3 Express
directory buckets, access-point ARNs, and their specialized endpoints are not
supported by `CloudProviderConfig::aws_s3`.

### S3 versioning and lifecycle

AWS Backup requires bucket versioning. For first-time enablement on an existing
bucket, stop its writers so Midge lease renewals and background deletes pause.
Allow [15 minutes for propagation][aws-enable-versioning] before resuming
writes or deletes. Use the [backup and restore guide](../user-guides/backup-and-restore.md#aws-backup-for-s3)
to choose and verify a recovery policy.

Keep current Midge objects readable for as long as Midge needs them. Apply
age-based expiration exclusively to **noncurrent** versions. The clock starts
when an object is overwritten or deleted. This protects current WAL, SST,
metadata, and fencing state even when those objects have old creation dates.

| Keys under the database prefix | Noncurrent retention |
| --- | --- |
| Immutable WAL segments, SSTs, and metadata objects | Start with seven days if a week of object history is useful. Use age alone. A minimum number of newer versions can leave the only data version of a retired key in storage indefinitely. |
| `midge_primary_lease.json` | Use a shorter window, such as one day, when the backup vault supplies the longer recovery window. At a 10-second renewal interval, one writer produces about 8,640 lease versions per day. |
| WAL catalog copies and other mutable control objects | Choose a bounded window after measuring overwrite frequency and object size. The two catalog keys are rewritten as full objects, and AWS Backup can capture multiple versions of each. |

Enable **expired object delete marker** cleanup so S3 removes a marker after
the underlying versions are gone. Use a separate action for abandoned multipart
uploads. Lifecycle acts asynchronously, so a days-based rule sets eligibility
and allows the live version inventory to exceed a simple daily count estimate.
Track noncurrent bytes, versions per key, and delete marker counts separately
from the live database size. See the [S3 lifecycle rules][aws-lifecycle].

### WAL authority and retirement

The WAL store contains epoch-scoped immutable segment objects and two identical
copies of the mutable publication authority document:
`wal/publication-catalog.v1.json` and
`wal/publication-catalog.v1.mirror.json`. The `v1` in these names is not the
document's format version. Current binaries use catalog format version 3,
described in the
[format compatibility policy](../development/format-compatibility.md). Lease
acquisition conditionally advances both catalog copies before recovery.
Uploads are recoverable only after both copies converge on a conditional
catalog publication; an unlisted object is an orphan and is ignored. WAL
pruning removes the catalog entry only after an exact committed metadata
snapshot has been proved, its manifest has been validated, and every
referenced SST dependency has passed stable object proof.
Midge then conditionally retires the matching catalog entry before best-effort
deleting the now-ignored object; retirement removes only the catalog's oldest
entries, in order. Missing, mismatched, changed, or timed-out proof
retains catalog authority and the WAL object. Restore the catalog copies from
the same recovery point as metadata and data objects. An older primary
catalog made current on its own can replace a newer mirror during repair.

## Qualification

The repository uses Sqrzl as the authoritative, self-contained environment for
continuous provider qualification. Scheduled and release workflows run
provider operations plus engine cache-loss and restart recovery through its S3,
Azure, and GCS front doors. Once explicitly selected, an unreachable emulator
fails qualification rather than silently skipping it.

Manual real-cloud integration testing validates Sqrzl fidelity and
deployment-specific assumptions. When it exposes a provider difference, that
behavior should be reproduced in Sqrzl and retained as a Midge regression test.
Live cloud credentials are deliberately not required for ordinary repository CI.
Before enabling versioning in production, test the selected lifecycle rules,
writer takeover, WAL pruning, and compaction in a versioned test bucket.

A Sqrzl pass is evidence for the protocol paths and failure scenarios it models;
it is not evidence for a deployment's provider availability, IAM, quotas,
lifecycle configuration, network policy, or production capacity. Validate those
environmental conditions with the feature flags and configuration intended for
deployment.

If the local cache is lost, recovery depends on the remote WAL/manifest state
and the qualified provider path. Preserve remaining evidence and inspect the
recovery error; do not remove database files as a generic fix.

See the [durability contract](../user-guides/transaction-durability-contract.md)
and [operator runbook](operator-runbook.md).

[aws-enable-versioning]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/manage-versioning-examples.html
[aws-lifecycle]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html
