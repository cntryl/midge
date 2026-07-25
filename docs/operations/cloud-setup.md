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

Open with `OpenOptions::cloud(local_cache, provider, prefix)`. Use
`WriteOptions::cloud_async()` when local acknowledgement may precede remote
upload, or `WriteOptions::cloud_strict()` when the commit must wait for the
cloud upload. These choices are meaningful only with cloud-backed storage.

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
