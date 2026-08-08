# Cloud setup (pre-1.0)

Cloud-backed storage is an optional, pre-1.0 integration. Qualify the exact
provider, feature set, credentials, network policy, and failure behavior before
using it. Midge does not endorse a cloud provider for production deployment.

## Feature selection

Use the provider feature required by the build (`cloud-aws`, `cloud-azure`,
`cloud-gcp`, or `cloud-oci`) and enable `cloud-common` when composing a custom
feature set. The default feature set currently includes the cloud integrations.
`CloudSimulated` needs no external provider and is backed by local filesystem
state, so it is suitable for deterministic tests rather than service
qualification.

`cloud-oci` enables the generic S3-compatible backend; it is not a native OCI
Object Storage client and does not add an OCI-specific configuration variant,
credential resolver, signer, or error taxonomy. Configure OCI through
`CloudProviderConfig::s3_compatible` and OCI's S3 Compatibility API. The
isolated `cloud-oci` CI leg is compile-only, so operators must independently
qualify conditional writes, missing-object responses, credentials, and endpoint
behavior against the exact OCI tenancy before relying on it.

## Configuration

Construct `CloudProviderConfig` with the provider-specific public credential
source types. Prefer environment, shared-profile, workload-identity, or the
provider's documented default chain over putting secrets in source. Midge does
not manage secret rotation, a secret store, IAM policy, or network access.
Never commit credentials or place them in a checked-in example.

The recommended deployment uses one unversioned bucket/container and one
database prefix. Midge keeps WAL, SST, metadata, and lease keys in disjoint
namespaces beneath that prefix. Use a multi-location topology only when
separate IAM, ownership, or lifecycle boundaries are operationally valuable.

```rust,no_run
# use cntryl_midge::{CloudProviderConfig, CloudStorageLocation, OpenOptions};
let location = CloudStorageLocation::new(
    CloudProviderConfig::aws_s3("midge-data", "us-east-1"),
    "database-a",
);
let options = OpenOptions::cloud("/var/lib/midge-cache", location).build()?;
# Ok::<(), cntryl_midge::MidgeError>(())
```

The shared location contains `wal/`, `sst/`, `metadata/`,
`wal/publication-catalog.v1.json`, `metadata/ddl.registry.json`, and
`midge_primary_lease.json`. Configure the shared location without object
versioning. Never point a writer at an empty control namespace while another
writer can still hold a lease for the same database.

For advanced routing, start with `CloudStorageTopology::new(shared)`, override
individual locations with `with_wal`, `with_sst`, or `with_control`, and pass
the result to `OpenOptions::cloud_multi`.

Use `WriteOptions::cloud_async()` when local acknowledgement may precede remote
upload, or `WriteOptions::cloud_strict()` when the commit must wait for the
cloud upload. These choices are meaningful only with cloud-backed storage.

## Object versioning and lifecycle rules

Provider lifecycle policy is bucket/container provisioning, not a Midge data
plane operation. Midge does not query, warn about, or reject provider
versioning state. Prefer versioning disabled. If operators enable versioning,
Midge exposes stable suffixes through `CloudObjectLayout` so provisioning can
bound cleanup of noncurrent versions:

| Object class | Store | Lifecycle requirement |
| --- | --- | --- |
| `wal/` | WAL | Never age-expire current objects. Bound cleanup of noncurrent versions after Midge's guarded prune. |
| `sst/` | SST | Never age-expire current objects. Give noncurrent versions a bounded recovery window. |
| `metadata/` | control | Never age-expire current objects. Retain only a small, bounded noncurrent recovery window. |
| `midge_primary_lease.json` | control | Keep current lease state; use the shortest practical noncurrent-version lifetime. |

The WAL store contains epoch-scoped immutable segment objects and the mutable
`wal/publication-catalog.v1.json` authority document. Lease acquisition
conditionally advances the catalog fencing epoch before recovery. Uploads are
recoverable only after a conditional catalog publication; an unlisted object
is an orphan and is ignored. WAL pruning removes the catalog entry only after
manifest/SST coverage has been validated, then best-effort deletes the orphaned
object. Operators must not edit or reconstruct this catalog.

Do not configure age-based expiration for current WAL, SST, or metadata
objects. Their safe-deletion decision depends on manifest coverage and is owned
by Midge. Lifecycle rules should remove only noncurrent versions and incomplete
multipart uploads. A lifecycle rule bounds future cost; it does not remove
already retained versions immediately.

## Qualification

The repository uses the Sqrzl emulator for provider-compatible qualification.
An emulator pass is evidence for the tested protocol path, not evidence for
provider availability, IAM, quotas, durability policy, or production scale.
Run the provider qualification and cache-loss/restart tests with the same
feature flags and configuration intended for deployment.

If the local cache is lost, recovery depends on the remote WAL/manifest state
and the qualified provider path. Preserve remaining evidence and inspect the
recovery error; do not remove database files as a generic fix.

See the [durability contract](../user-guides/transaction-durability-contract.md)
and [operator runbook](operator-runbook.md).
