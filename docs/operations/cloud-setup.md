# Cloud Storage Setup

Guide to Midge's real cloud storage mode, local Sqrzl qualification, and the lightweight credential support model.

> **STATUS: Pre-1.0**
>
> `OpenOptions::cloud(...)` now builds a real provider-backed object-store path. Midge intentionally does not depend on the AWS, Azure, or Google SDK crates; provider access is implemented through lean REST clients, signing, endpoint handling, credential resolution, and callback-based storage adapters.
>
> Keep using `OpenOptions::cloud_simulated(...)` for deterministic engine tests that need the old filesystem-backed cloud simulation.

## Table of Contents

- [Cloud Storage Features](#cloud-storage-features)
- [Public API](#public-api)
- [Sqrzl Emulator](#sqrzl-emulator)
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
- **Local emulator**: Sqrzl is the canonical local qualification target because it exposes S3, Azure Blob, GCS, and OCI-compatible front doors.
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
    CloudProviderConfig::aws_s3("midge-prod", "us-east-1"),
    "databases/example/",
)
.build()?;

let engine = Engine::open(opts)?;
# Ok::<(), cntryl_midge::MidgeError>(())
```

The first argument is the local cache/staging directory, the second is the provider, and the third is the object prefix inside the bucket/container.

### Filesystem Simulation

Use this for deterministic engine tests, failpoints, and local recovery scenarios that should not depend on HTTP or credentials.

```rust
use cntryl_midge::{Engine, OpenOptions};

let opts = OpenOptions::cloud_simulated(
    "./target/midge-sim-cache",
    "test-bucket",
    "test-prefix/",
)
.build()?;

let engine = Engine::open(opts)?;
# Ok::<(), cntryl_midge::MidgeError>(())
```

### AWS S3

```rust
use cntryl_midge::{CloudProviderConfig, Engine, OpenOptions};

let provider = CloudProviderConfig::aws_s3("midge-prod", "us-east-1");

let engine = Engine::open(
    OpenOptions::cloud("./cache", provider, "app-a/").build()?
)?;
# Ok::<(), cntryl_midge::MidgeError>(())
```

For explicit access keys:

```rust
let access_key = std::env::var("AWS_ACCESS_KEY_ID")?;
let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY")?;

let provider = CloudProviderConfig::aws_s3_static(
    "midge-prod",
    "us-east-1",
    access_key,
    secret_key,
);
```

### S3-Compatible / MinIO / Wasabi / OCI

```rust
use cntryl_midge::{CloudProviderConfig, Engine, OpenOptions};

let provider = CloudProviderConfig::s3_compatible_env(
    "midge-dev",
    "http://127.0.0.1:9000",
);

let engine = Engine::open(
    OpenOptions::cloud("./cache", provider, "dev/").build()?
)?;
# Ok::<(), cntryl_midge::MidgeError>(())
```

For explicit access keys and provider-specific endpoints, use `S3Compatible` directly:

```rust
let minio = CloudProviderConfig::s3_compatible_static(
    "midge-dev",
    "http://127.0.0.1:9000",
    "minioadmin",
    "minioadmin",
);
let wasabi = CloudProviderConfig::s3_compatible_env(
    "midge-prod",
    "https://s3.us-east-1.wasabisys.com",
)
.with_s3_region("us-east-1")?;
let oci = CloudProviderConfig::s3_compatible_env(
    "midge-prod",
    "https://oci-namespace.compat.objectstorage.us-phoenix-1.oraclecloud.com",
)
.with_s3_region("us-phoenix-1")?
.with_path_style(false)?;
# Ok::<(), cntryl_midge::MidgeError>(())
```

### Azure Blob

```rust
use cntryl_midge::{CloudProviderConfig, Engine, OpenOptions};

let provider = CloudProviderConfig::azure_blob("mystorageaccount", "midge");

let engine = Engine::open(
    OpenOptions::cloud("./cache", provider, "prod/").build()?
)?;
# Ok::<(), cntryl_midge::MidgeError>(())
```

For storage credentials:

```rust
let shared_key = CloudProviderConfig::azure_blob_shared_key(
    "mystorageaccount",
    "midge",
    "base64-account-key",
);
let sas = CloudProviderConfig::azure_blob_sas(
    "mystorageaccount",
    "midge",
    "?sv=...",
);
let conn = CloudProviderConfig::azure_blob_connection_string(
    "midge",
    "DefaultEndpointsProtocol=https;AccountName=mystorageaccount;AccountKey=...",
);
```

### GCS

```rust
use cntryl_midge::{CloudProviderConfig, Engine, OpenOptions};

let provider = CloudProviderConfig::gcs("midge-prod");

let engine = Engine::open(
    OpenOptions::cloud("./cache", provider, "prod/").build()?
)?;
# Ok::<(), cntryl_midge::MidgeError>(())
```

For explicit GCS credentials:

```rust
let service_account = CloudProviderConfig::gcs_service_account_file(
    "midge-prod",
    "/var/run/secrets/gcp/service-account.json",
);
let hmac = CloudProviderConfig::gcs_hmac(
    "midge-prod",
    "GOOG_ACCESS_ID",
    "GOOG_HMAC_SECRET",
);
```

### Advanced Configuration

Use fluent modifiers when you need endpoint, path-style, region, or credential overrides:

```rust
use cntryl_midge::{CloudProviderConfig, S3CredentialSource};

let provider = CloudProviderConfig::s3_compatible_env("bucket", "http://old-endpoint")
    .with_endpoint("http://new-endpoint")?
    .with_s3_region("eu-west-1")?
    .with_path_style(true)?
    .with_s3_credentials(S3CredentialSource::access_key("key", "secret"))?;
```

The raw `CloudProviderConfig` enum variants remain public for advanced/manual configuration, but the constructors above are the recommended path.

Endpoint, path-style, region, project-ID, and credential overrides are fallible on purpose. Midge rejects unsupported combinations instead of silently ignoring them. For example, use `s3_compatible_*` or `sqrzl_s3(...)` for custom S3 endpoints; `aws_s3(...)` always targets AWS S3.

## Sqrzl Emulator

Sqrzl is the canonical local emulator for cross-vendor cloud qualification.

Start it from the repository compose file:

```bash
docker compose up -d sqrzl
```

The built-in helpers use explicit emulator credentials only:

```rust
use cntryl_midge::CloudProviderConfig;

let s3 = CloudProviderConfig::sqrzl_s3("midge-sqrzl-s3");
let azure = CloudProviderConfig::sqrzl_azure("midge-sqrzl-azure");
let gcs = CloudProviderConfig::sqrzl_gcs("midge-sqrzl-gcs");
```

Sqrzl helper defaults:

| Helper | Endpoint | Credentials | Notes |
|---|---|---|---|
| `sqrzl_s3` | `http://127.0.0.1:9000` | `admin` / `easy-peasy` | S3-compatible, path-style |
| `sqrzl_azure` | `http://127.0.0.1:9000` | shared key `easy-peasy` | Azure emulator path-style URL |
| `sqrzl_gcs` | `http://127.0.0.1:9000` | HMAC `admin` / `easy-peasy` | GCS XML API with `GOOG1` signing |

Known Sqrzl emulator quirks observed during qualification:

- Namespace/container initialization can return `500` when the namespace already exists; Midge treats this as emulator-specific setup noise, not a provider contract.
- Azure SharedKey canonicalization for Sqrzl path-style URLs differs from production Azure endpoint paths; Midge uses emulator-compatible canonicalization only when an explicit emulator endpoint is configured.
- Some S3 conditional behavior is normalized client-side for Sqrzl qualification, but production safety still relies on provider-side object preconditions.
- Sqrzl's GCS JSON bearer path does not pass Midge's full round-trip contract; use the `sqrzl_gcs` XML/HMAC helper for qualification.

## Credential Matrix

Midge follows official provider credential families where practical, but it implements a lean subset without SDK crates and without shelling out to CLIs or process helpers. “SDK-equivalent supported” means Midge matches the provider’s names, search order, token caching, and refresh behavior for that source.

Official references:

- [AWS standardized credential providers](https://docs.aws.amazon.com/sdkref/latest/guide/standardized-credentials.html)
- [Azure credential chains](https://learn.microsoft.com/en-us/dotnet/azure/sdk/authentication/credential-chains)
- [Google Application Default Credentials](https://docs.cloud.google.com/docs/authentication/application-default-credentials)
- [GCS HMAC keys](https://docs.cloud.google.com/storage/docs/authentication/hmackeys)

### AWS / S3-Compatible

| Credential source | Constructor/source | AWS S3 | S3-compatible | Sqrzl |
|---|---|---:|---:|---|
| Default AWS chain | `aws_s3(...)` / `AwsDefaultChain` | SDK-equivalent supported | Not allowed | Not used |
| Explicit access keys | `aws_s3_static(...)`, `s3_compatible_static(...)` | Supported | Supported | Supported |
| Environment access keys | `S3CredentialSource::Environment` | Supported | Supported | Supported if env is set |
| Shared static/session profile | `S3CredentialSource::SharedProfile` | Supported | Supported | Supported if profile has static keys |
| Web identity / IRSA | Default chain | Supported | Not allowed | Not used |
| ECS/EKS container credentials | Default chain | Supported | Not allowed | Not used |
| EC2 IMDSv2 | Default chain | Supported | Not allowed | Not used |
| AWS SSO / IAM Identity Center | Profile metadata | Lean unsupported: SDK/process-backed | Unsupported | Not used |
| `credential_process` | Profile metadata | Lean unsupported: process execution | Unsupported | Not used |

Midge's AWS default chain resolves:

- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN`
- `~/.aws/credentials` and `~/.aws/config`, with `AWS_PROFILE`
- web identity environment variables
- ECS/EKS container credentials
- EC2 IMDSv2

For S3-compatible providers, use explicit keys, environment keys, or shared profiles. `AwsDefaultChain` is restricted to AWS S3 so custom endpoints do not accidentally contact AWS role or metadata endpoints.

### Azure Blob

| Credential source | Constructor/source | Support status | Sqrzl |
|---|---|---:|---|
| Lean Azure identity chain | `azure_blob(...)` / `LightweightDefaultChain` | SDK-equivalent for supported identity sources | Not used |
| Shared key | `azure_blob_shared_key(...)` | Supported | Supported |
| SAS token | `azure_blob_sas(...)` | Supported | Supported if emulator accepts SAS |
| Connection string | `azure_blob_connection_string(...)` | Supported | Supported with emulator endpoint |
| Storage env key/SAS/connection string | `AzureCredentialSource::StorageEnvironment` | Supported as explicit storage credential source | Supported if env is set |
| Client secret env | `EnvironmentClientSecret` | Supported | Not used |
| Workload identity | `WorkloadIdentity` | Supported | Not used |
| Managed identity | `ManagedIdentity` | Supported | Not used |
| Azure CLI / PowerShell / Developer CLI | SDK developer credentials | Lean unsupported: process execution | Not used |
| Visual Studio / VS Code / broker / browser | SDK developer credentials | Lean unsupported: dev-tool/broker integration | Not used |

Midge's Azure identity chain tries client-secret environment credentials, workload identity, then managed identity. It intentionally skips developer-tool credentials because those require SDK helpers, broker integration, or process execution. Shared key, SAS, connection strings, and storage environment credentials are explicit storage-credential paths, not part of the identity default chain.

### GCS

| Credential source | Constructor/source | API style | Support status | Sqrzl |
|---|---|---|---:|---|
| Application Default Credentials | `gcs(...)` / `ApplicationDefault` | JSON | SDK-equivalent for supported ADC files | Not used |
| Service account JSON file | `gcs_service_account_file(...)` | JSON | Supported | Not used |
| Authorized user JSON file | `gcs_authorized_user_file(...)` | JSON | Supported | Not used |
| Metadata server | `GcsCredentialSource::MetadataServer` | JSON | Supported | Not used |
| Non-executable external-account file ADC | ADC JSON | JSON | Supported for file-sourced subject tokens | Not used |
| Bearer token | `gcs_bearer_token(...)` | JSON | Supported, non-refreshable | Not used |
| HMAC key | `gcs_hmac(...)` | XML | Supported | Supported |
| Executable external-account ADC | ADC JSON | JSON | Lean unsupported: process execution | Not used |

Midge's ADC subset resolves:

- `GOOGLE_APPLICATION_CREDENTIALS`
- local ADC file at the standard gcloud ADC location
- service-account JWT bearer exchange
- authorized-user refresh token exchange
- non-executable file-sourced external-account STS exchange
- metadata server tokens

ADC, service-account, authorized-user, external-account, and metadata-server access tokens are cached and refreshed before expiry. Explicit bearer tokens are treated as static.

GCS HMAC keys are XML API credentials. `gcs_hmac(...)` selects XML automatically; ADC and bearer constructors select JSON automatically.

## Provider Notes

### S3 / MinIO / Wasabi / OCI

- Uses SigV4 signing with the correct credential scope.
- Supports path-style endpoints for emulators and S3-compatible providers.
- Sends conditional object headers for create/update safety.
- Supports range reads and prefix listing.
- OCI support is through OCI's S3-compatible front door, not native OCI auth.

### Azure Blob

- Supports shared key, SAS token, and bearer-token authorization.
- `azure_blob(...)` uses identity credentials only; storage keys, SAS, and connection strings are explicit constructors.
- Supports emulator path-style URLs such as `http://127.0.0.1:9000/{account}/{container}`.
- Uses Azure SharedKey canonicalization for production endpoints and Sqrzl-compatible canonicalization for explicit emulator endpoints.

### GCS

- Supports JSON API bearer-token auth for ADC-style credentials, including service-account, authorized-user, metadata-server, and non-executable file-sourced external-account ADC.
- Supports XML API `GOOG1` HMAC signing for Sqrzl and HMAC deployments.
- HMAC is XML-only, matching Google Cloud Storage restrictions, and `gcs_hmac(...)` selects XML automatically.

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
- Sqrzl is an emulator. Conditional writes are normalized by Midge for qualification, but production providers must still enforce object preconditions atomically.
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

### Sqrzl Qualification Tests

Sqrzl qualification is feature-gated, and the CI workflow runs it as an explicit gate. Start Sqrzl first:

```bash
docker compose up -d sqrzl
```

Then run the full suite:

```bash
cargo test
```

Or narrow to just the provider qualification module:

```bash
cargo test --lib --features sqrzl-tests storage::providers::qualification -- --test-threads=1
```

If Sqrzl is not running, these tests are expected to fail instead of being skipped.

For an opt-in real S3-compatible smoke run, set `MIDGE_REAL_S3_BUCKET`,
`MIDGE_REAL_S3_ENDPOINT`, `MIDGE_REAL_S3_ACCESS_KEY`, and
`MIDGE_REAL_S3_SECRET_KEY` before running the same qualification module.
`MIDGE_REAL_S3_REGION` defaults to `us-east-1`, and `MIDGE_REAL_S3_PATH_STYLE`
defaults to `true`.

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

Keep `MockCloudBackend` tests for deterministic failure injection, retries, timeouts, and edge cases that are difficult or impossible to trigger reliably against Sqrzl or a real provider.

## Operational Guidance

Use real cloud mode when you want to qualify object-store behavior, Sqrzl compatibility, or cloud-backed recovery paths. Use local mode for the most conservative production-style evaluation while the 0.x cloud API is still moving.

Recommended rollout order:

1. Run provider qualification against Sqrzl.
2. Run provider qualification against the target vendor with a disposable bucket/container.
3. Run engine-level recovery tests with an isolated prefix.
4. Enable `RecoveryPolicy::Strict` for production-style validation.
5. Monitor primary lease health through `Engine::is_primary_lease_healthy()`.

Related docs:

- [Architecture](../development/architecture.md)
- [Durability](../user-guides/durability.md)
- [Stability Policy](../development/stability-policy.md)
- [API Guide](../user-guides/api-guide.md)
