# Cloud Storage Setup

Guide to Midge's real cloud storage mode, local Peas qualification, and the lightweight credential support model.

> **STATUS: Pre-1.0**
>
> `OpenOptions::cloud(...)` now builds a real provider-backed object-store path. Midge intentionally does not depend on the AWS, Azure, or Google SDK crates; provider access is implemented through lean REST clients, signing, endpoint handling, credential resolution, and callback-based storage adapters.
>
> Keep using `OpenOptions::cloud_simulated(...)` for deterministic engine tests that need the old filesystem-backed cloud simulation.

## Table of Contents

- [Cloud Storage Features](#cloud-storage-features)
- [Public API](#public-api)
- [Peas Emulator](#peas-emulator)
- [Credential Matrix](#credential-matrix)
- [Provider Notes](#provider-notes)
- [Engine Safety](#engine-safety)
- [Testing Cloud Storage](#testing-cloud-storage)
- [Operational Guidance](#operational-guidance)

## Cloud Storage Features

Midge has two separate cloud surfaces:

- **Real cloud mode**: `OpenOptions::cloud(local_cache_path, CloudProviderConfig, prefix)` uses the selected object-store provider through `CloudStorage` and `HybridStorage`.
- **Filesystem simulation**: `OpenOptions::cloud_simulated(local_cache_path, bucket, prefix)` keeps the deterministic local simulation for tests and failure injection.
- **Provider coverage**: AWS S3, S3-compatible endpoints, MinIO, Wasabi, OCI S3-compatible, Azure Blob, and GCS.
- **Local emulator**: Peas is the canonical local qualification target because it exposes S3, Azure Blob, GCS, and OCI-compatible front doors.
- **No heavy SDKs**: credential loading and request signing are implemented directly by Midge.

### Hybrid Storage Architecture

Real cloud mode uses a local cache/staging directory plus a remote object store.

```text
Application
    |
    v
Engine / Runtime
    |
    v
HybridStorage
    |-------------------|
    v                   v
Local cache          CloudStorage
FileSystem           CloudBackend provider
```

`CloudStorage` wraps the selected provider as a `StorageBackend`, preserves the configured prefix/namespace, and translates callback results into the same storage events used by the rest of the engine.

## Public API

### Real Cloud Mode

```rust
use cntryl_midge::{CloudProviderConfig, Engine, OpenOptions};

let opts = OpenOptions::cloud(
    "./target/midge-cache",
    CloudProviderConfig::peas_s3("midge-dev"),
    "databases/example/",
)
.build();

let engine = Engine::open(opts)?;
# Ok::<(), cntryl_midge::MidgeError>(())
```

### Filesystem Simulation

Use this for deterministic engine tests, failpoints, and local recovery scenarios that should not depend on HTTP or credentials.

```rust
use cntryl_midge::{Engine, OpenOptions};

let opts = OpenOptions::cloud_simulated(
    "./target/midge-sim-cache",
    "test-bucket",
    "test-prefix/",
)
.build();

let engine = Engine::open(opts)?;
# Ok::<(), cntryl_midge::MidgeError>(())
```

### AWS S3

```rust
use cntryl_midge::{
    CloudProviderConfig, Engine, OpenOptions, S3CredentialSource,
};

let provider = CloudProviderConfig::AwsS3 {
    bucket: "midge-prod".to_string(),
    region: "us-east-1".to_string(),
    credentials: S3CredentialSource::AwsDefaultChain,
};

let engine = Engine::open(
    OpenOptions::cloud("./cache", provider, "app-a/").build()
)?;
# Ok::<(), cntryl_midge::MidgeError>(())
```

### S3-Compatible / MinIO / Wasabi / OCI

```rust
use cntryl_midge::{
    CloudProviderConfig, Engine, OpenOptions, S3CredentialSource,
};

let provider = CloudProviderConfig::S3Compatible {
    bucket: "midge-dev".to_string(),
    region: "us-east-1".to_string(),
    endpoint: "http://127.0.0.1:9000".to_string(),
    path_style: true,
    credentials: S3CredentialSource::Environment,
};

let engine = Engine::open(
    OpenOptions::cloud("./cache", provider, "dev/").build()
)?;
# Ok::<(), cntryl_midge::MidgeError>(())
```

For local MinIO with explicit credentials:

```rust
let provider = CloudProviderConfig::minio(
    "midge-dev",
    "http://127.0.0.1:9000",
    "minioadmin",
    "minioadmin",
);
```

### Azure Blob

```rust
use cntryl_midge::{
    AzureCredentialSource, CloudProviderConfig, Engine, OpenOptions,
};

let provider = CloudProviderConfig::AzureBlob {
    account: "mystorageaccount".to_string(),
    container: "midge".to_string(),
    endpoint: None,
    credential: AzureCredentialSource::LightweightDefaultChain,
};

let engine = Engine::open(
    OpenOptions::cloud("./cache", provider, "prod/").build()
)?;
# Ok::<(), cntryl_midge::MidgeError>(())
```

### GCS

```rust
use cntryl_midge::{
    CloudProviderConfig, Engine, GcsApiStyle, GcsCredentialSource, OpenOptions,
};

let provider = CloudProviderConfig::Gcs {
    bucket: "midge-prod".to_string(),
    project_id: "my-project".to_string(),
    endpoint: None,
    api: GcsApiStyle::Json,
    credential: GcsCredentialSource::ApplicationDefault,
};

let engine = Engine::open(
    OpenOptions::cloud("./cache", provider, "prod/").build()
)?;
# Ok::<(), cntryl_midge::MidgeError>(())
```

## Peas Emulator

Peas is the canonical local emulator for cross-vendor cloud qualification.

Start it from the repository compose file:

```bash
docker compose up -d peas
```

The built-in helpers use explicit emulator credentials only:

```rust
use cntryl_midge::CloudProviderConfig;

let s3 = CloudProviderConfig::peas_s3("midge-peas-s3");
let azure = CloudProviderConfig::peas_azure("midge-peas-azure");
let gcs = CloudProviderConfig::peas_gcs("midge-peas-gcs");
```

Peas helper defaults:

| Helper | Endpoint | Credentials | Notes |
|---|---|---|---|
| `peas_s3` | `http://127.0.0.1:9000` | `admin` / `easy-peasy` | S3-compatible, path-style |
| `peas_azure` | `http://127.0.0.1:9000` | shared key `easy-peasy` | Azure emulator path-style URL |
| `peas_gcs` | `http://127.0.0.1:9000` | HMAC `admin` / `easy-peasy` | GCS XML API with `GOOG1` signing |

Known Peas emulator quirks observed during qualification:

- Namespace/container initialization can return `500` when the namespace already exists; Midge treats this as emulator-specific setup noise, not a provider contract.
- Azure SharedKey canonicalization for Peas path-style URLs differs from production Azure endpoint paths; Midge uses emulator-compatible canonicalization only when an explicit emulator endpoint is configured.
- Some S3 conditional behavior is normalized client-side for Peas qualification, but production safety still relies on provider-side object preconditions.

## Credential Matrix

Midge follows the official provider credential families where practical, but it implements a lightweight subset. It does not attempt SDK `DefaultCredential` parity, and it never shells out to CLIs or `credential_process`.

Official references:

- [AWS SDK for Rust credential provider chain](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credproviders.html)
- [Azure credential chains](https://learn.microsoft.com/en-us/dotnet/azure/sdk/authentication/credential-chains)
- [Google Application Default Credentials](https://docs.cloud.google.com/docs/authentication/application-default-credentials)
- [GCS HMAC keys](https://docs.cloud.google.com/storage/docs/authentication/hmackeys)

### AWS / S3-Compatible

| Credential source | Midge enum | AWS S3 | S3-compatible | Peas behavior |
|---|---|---:|---:|---|
| Explicit access key | `S3CredentialSource::Static` | Supported | Supported | Supported |
| Environment variables | `S3CredentialSource::Environment` | Supported | Supported | Supported if env is set |
| Shared profile files | `S3CredentialSource::SharedProfile` | Supported | Supported | Supported if profile has static keys |
| Lightweight AWS default chain | `S3CredentialSource::AwsDefaultChain` | Supported | Not allowed | Not used |
| AWS IAM Identity Center / SSO | Profile metadata | Unsupported | Unsupported | Not used |
| `credential_process` | Profile metadata | Unsupported | Unsupported | Not used |

Midge's AWS default chain resolves:

- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN`
- `~/.aws/credentials` and `~/.aws/config`, with `AWS_PROFILE`
- web identity environment variables
- ECS/EKS container credentials
- EC2 IMDSv2

For S3-compatible providers, prefer `Static`, `Environment`, or `SharedProfile`. `AwsDefaultChain` is restricted to `CloudProviderConfig::AwsS3` so custom endpoints do not accidentally contact AWS role or metadata endpoints.

### Azure Blob

| Credential source | Midge enum | Supported | Peas behavior |
|---|---|---:|---|
| Shared key | `AzureCredentialSource::SharedKey` | Supported | Supported |
| SAS token | `AzureCredentialSource::SasToken` | Supported | Supported if emulator accepts SAS |
| Connection string | `AzureCredentialSource::ConnectionString` | Supported | Supported with emulator endpoint |
| Storage account env | `AzureCredentialSource::StorageEnvironment` | Supported | Supported if env is set |
| Client secret env | `AzureCredentialSource::EnvironmentClientSecret` | Supported | Not used |
| Workload identity | `AzureCredentialSource::WorkloadIdentity` | Supported | Not used |
| Managed identity | `AzureCredentialSource::ManagedIdentity` | Supported | Not used |
| Lightweight chain | `AzureCredentialSource::LightweightDefaultChain` | Supported | Uses env/config sources only |
| Azure CLI / PowerShell / Developer CLI | SDK developer credentials | Unsupported | Not used |
| Visual Studio / VS Code / broker / browser | SDK developer credentials | Unsupported | Not used |

Midge's Azure lightweight chain tries storage credentials first, then client-secret environment credentials, workload identity, and managed identity. It intentionally skips developer-tool credentials because those require SDK helpers or process execution.

### GCS

| Credential source | Midge enum | API style | Supported | Peas behavior |
|---|---|---|---:|---|
| Bearer token | `GcsCredentialSource::BearerToken` | JSON | Supported | Not used |
| HMAC key | `GcsCredentialSource::HmacKey` | XML | Supported | Supported |
| Application Default Credentials | `GcsCredentialSource::ApplicationDefault` | JSON | Supported subset | Not used |
| Service account JSON file | `GcsCredentialSource::ServiceAccountJsonFile` | JSON | Supported | Not used |
| Authorized user JSON file | `GcsCredentialSource::AuthorizedUserJsonFile` | JSON | Supported | Not used |
| Metadata server | `GcsCredentialSource::MetadataServer` | JSON | Supported | Not used |
| Executable external-account ADC | ADC JSON | JSON | Unsupported | Not used |

Midge's ADC subset resolves:

- `GOOGLE_APPLICATION_CREDENTIALS`
- local ADC file at the standard gcloud ADC location
- service-account JWT bearer exchange
- authorized-user refresh token exchange
- metadata server tokens

ADC, service-account, authorized-user, and metadata-server access tokens are cached and refreshed before expiry. Explicit bearer tokens are treated as static.

GCS HMAC keys are XML API credentials. Midge enforces that by requiring `GcsApiStyle::Xml` for `GcsCredentialSource::HmacKey`.

## Provider Notes

### S3 / MinIO / Wasabi / OCI

- Uses SigV4 signing with the correct credential scope.
- Supports path-style endpoints for emulators and S3-compatible providers.
- Sends conditional object headers for create/update safety.
- Supports range reads and prefix listing.
- OCI support is through OCI's S3-compatible front door, not native OCI auth.

### Azure Blob

- Supports shared key, SAS token, and bearer-token authorization.
- Supports emulator path-style URLs such as `http://127.0.0.1:9000/{account}/{container}`.
- Uses Azure SharedKey canonicalization for production endpoints and Peas-compatible canonicalization for explicit emulator endpoints.

### GCS

- Supports JSON API bearer-token auth for ADC-style credentials.
- Supports XML API `GOOG1` HMAC signing for Peas and HMAC deployments.
- HMAC is XML-only, matching Google Cloud Storage restrictions.

## Engine Safety

Real cloud mode now uses provider-backed engine startup and lease behavior:

- Builds `HybridStorage` from the selected provider backend.
- Acquires a provider-backed primary lease using conditional object create/update and releases it only with holder verification plus provider preconditions.
- Lists and downloads remote `wal/` objects through `CloudBackend` for WAL replay.
- Stages manifest and intent-referenced `sst/` objects before intent replay, then validates the final manifest SST cache.
- Hydrates metadata files under remote `metadata/` before recovery and mirrors metadata back only after startup recovery succeeds.
- Fails `RecoveryPolicy::Strict` startup if reachability, metadata hydration, lease acquisition, WAL replay, or SST restore cannot complete.

Important limitations:

- Azure native blob leases are not used yet; the current lease path uses the portable conditional-object lease.
- Peas is an emulator. Conditional writes are normalized by Midge for qualification, but production providers must still enforce object preconditions atomically.
- Midge's lightweight default chains are intentionally narrower than the provider SDK chains.

## Testing Cloud Storage

### Deterministic Engine Tests

Use the filesystem simulation:

```bash
cargo test --test engine_cloud
cargo test --test cloud_persistence_hardening
```

### Provider Unit Tests

```bash
cargo test --lib storage::providers
```

### Peas Qualification Tests

Peas qualification is part of the default `cargo test` path. Start Peas first:

```bash
docker compose up -d peas
```

Then run the full suite:

```bash
cargo test
```

Or narrow to just the provider qualification module:

```bash
cargo test storage::providers::qualification -- --test-threads=1
```

If Peas is not running, these tests are expected to fail instead of being skipped.

The provider contract covers:

- `PUT`
- `GET`
- `HEAD`
- `LIST`
- range read
- `DELETE`
- missing object behavior
- overwrite behavior
- conditional create
- conditional update

Use one bucket/container per vendor family and a UUID prefix per test for isolation.

### Mock Cloud Backend

Keep `MockCloudBackend` tests for deterministic failure injection, retries, timeouts, and edge cases that are difficult or impossible to trigger reliably against Peas or a real provider.

## Operational Guidance

Use real cloud mode when you want to qualify object-store behavior, Peas compatibility, or cloud-backed recovery paths. Use local mode for the most conservative production-style evaluation while the 0.x cloud API is still moving.

Recommended rollout order:

1. Run provider qualification against Peas.
2. Run provider qualification against the target vendor with a disposable bucket/container.
3. Run engine-level recovery tests with an isolated prefix.
4. Enable `RecoveryPolicy::Strict` for production-style validation.
5. Monitor primary lease health through `Engine::is_primary_lease_healthy()`.

Related docs:

- [Architecture](../development/architecture.md)
- [Durability](../user-guides/durability.md)
- [Stability Policy](../development/stability-policy.md)
- [API Guide](../user-guides/api-guide.md)
