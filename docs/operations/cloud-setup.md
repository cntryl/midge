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

The recommended deployment uses one unversioned bucket/container, with provider
soft-delete retention disabled, and one database prefix. Midge keeps WAL, SST,
metadata, and lease keys in disjoint namespaces beneath that prefix. Use a
multi-location topology only when separate IAM, ownership, or lifecycle
boundaries are operationally valuable.

```rust,no_run
# use cntryl_midge::{CloudProviderConfig, CloudStorageLocation, OpenOptions};
let location = CloudStorageLocation::new(
    CloudProviderConfig::aws_s3("midge-data", "us-east-1"),
    "database-a",
);
let options = OpenOptions::cloud("/var/lib/midge-cache", location).build()?;
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
bound cleanup of noncurrent versions. Also disable GCS soft delete and Azure
Blob soft delete for the Midge location; otherwise deleted objects continue to
consume storage until the provider retention period expires. GCS enables soft
delete with a seven-day retention period on new buckets by default, unless an
organization policy or explicit bucket setting changes it.

Native AWS configuration uses virtual-hosted HTTPS endpoints for ordinary
bucket names. Because standard wildcard certificates do not match dotted
bucket names, Midge automatically uses AWS path-style addressing for them. S3 Express
directory buckets, access-point ARNs, and their specialized endpoints are not
supported by `CloudProviderConfig::aws_s3`.

Provision lifecycle behavior by object class:

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

The repository uses Sqrzl as the authoritative, self-contained environment for
continuous provider qualification. Scheduled and release workflows run
provider operations plus engine cache-loss and restart recovery through its S3,
Azure, and GCS front doors. Once explicitly selected, an unreachable emulator
fails qualification rather than silently skipping it.

Manual real-cloud integration testing validates Sqrzl fidelity and
deployment-specific assumptions. When it exposes a provider difference, that
behavior should be reproduced in Sqrzl and retained as a Midge regression test.
Live cloud credentials are deliberately not required for ordinary repository CI.

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
