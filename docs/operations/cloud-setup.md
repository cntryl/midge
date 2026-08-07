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

## Configuration

Construct `CloudProviderConfig` with the provider-specific public credential
source types. Prefer environment, shared-profile, workload-identity, or the
provider's documented default chain over putting secrets in source. Midge does
not manage secret rotation, a secret store, IAM policy, or network access.
Never commit credentials or place them in a checked-in example.

Provider-backed cloud mode requires three distinct buckets/containers. This is
a hard configuration boundary: WAL, SST, and mutable control objects cannot be
co-located, even under different prefixes. Object versioning is bucket-wide on
providers such as S3, so a prefix-only split cannot provide independent
versioning policy.

```rust,no_run
# use cntryl_midge::{CloudProviderConfig, CloudStorageBuckets, CloudStorageLocation, OpenOptions};
let location = |bucket| CloudStorageLocation::new(
    CloudProviderConfig::aws_s3(bucket, "us-east-1"),
    "database-a",
);
let buckets = CloudStorageBuckets::new(
    location("midge-wal"),
    location("midge-sst"),
    location("midge-control"),
);
let options = OpenOptions::cloud("/var/lib/midge-cache", buckets).build()?;
# Ok::<(), cntryl_midge::MidgeError>(())
```

The WAL bucket contains `wal/`, the SST bucket contains `sst/`, and the control
bucket contains `metadata/`, `metadata/ddl.registry.json`, and
`midge_primary_lease.json`. Configure the control bucket without object
versioning. Never point a writer at an empty control namespace while another
writer can still hold a lease for the same database.

Use `WriteOptions::cloud_async()` when local acknowledgement may precede remote
upload, or `WriteOptions::cloud_strict()` when the commit must wait for the
cloud upload. These choices are meaningful only with cloud-backed storage.

## Object versioning and lifecycle rules

Provider lifecycle policy is bucket/container provisioning, not a Midge data
plane operation. Midge exposes the stable suffixes through `CloudObjectLayout`
so provisioning can give each class a separate rule:

| Object class | Store | Lifecycle requirement |
| --- | --- | --- |
| `wal/` | WAL | Never age-expire current objects. Expire noncurrent versions shortly after Midge's guarded prune. |
| `sst/` | SST | Never age-expire current objects. Give noncurrent versions a separately bounded recovery window. |
| `metadata/` | control | Prefer versioning disabled. Otherwise retain only a small, bounded noncurrent recovery window. |
| `midge_primary_lease.json` | control | Prefer versioning disabled. Otherwise use the shortest noncurrent-version lifetime supported. |

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
