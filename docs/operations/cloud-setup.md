# Cloud Storage Setup

Guide to Midge's cloud storage architecture and implementation status.

> **STATUS: Production Ready**
>
> Midge supports cloud storage as a production-ready durability target. Cloud storage integration is fully implemented for AWS S3, Azure Blob, and Google Cloud Storage with local caching. The hybrid storage architecture (local cache + cloud backend) is tested and suitable for production cloud-native deployments.

## Table of Contents

- [Cloud Storage Features](#cloud-storage-features)
- [Hybrid Storage Architecture](#hybrid-storage-architecture)
- [Getting Started](#getting-started)
- [Provider Configuration](#provider-configuration)
  - [AWS S3 Provider](#aws-s3-provider)
  - [Azure Blob Provider](#azure-blob-provider)
  - [GCS Provider](#gcs-provider)
  - [S3-Compatible Providers](#s3-compatible-providers)
- [Credential Management](#credential-management)
- [Testing Cloud Storage](#testing-cloud-storage)

## Cloud Storage Features

Midge provides production-ready cloud storage integration with the following capabilities:

✅ **Hybrid storage architecture** - Local cache + cloud backend for optimal performance  
✅ **Multiple cloud providers** - AWS S3, Azure Blob, Google Cloud Storage, Cloudflare R2, MinIO  
✅ **CloudFirst durability** - WAL upload pipeline with acknowledgment guarantees  
✅ **Automatic credential management** - Environment-based credential discovery  
✅ **Consistent API** - Same operations across all storage modes  
✅ **Performance optimization** - Smart caching, prefetching, and batching

### Quick Start

Open an engine with cloud storage:

```rust
use cntryl_midge::{Engine, OpenOptions};

let opts = OpenOptions::cloud("/tmp/cache", "my-bucket", "prefix/")
    .build();

let engine = Engine::open(opts)?;
```

Midge will:
- Use the local directory `/tmp/cache` as a fast read cache
- Store all durable data in the cloud bucket `my-bucket` under `prefix/`
- Automatically handle WAL uploads and SST persistence
- Manage credentials from your environment (AWS credentials, Azure connection strings, etc.)

## Hybrid Storage Architecture

The core hybrid storage model is **fully implemented** and tested. This architecture is used regardless of whether the backend is filesystem simulation or real cloud.

```
┌─────────────────┐
│  Application    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Engine/Runtime │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ HybridStorage   │  ← Orchestrates local + cloud
└────────┬────────┘
         │
    ┌────┴────────────┐
    ▼                 ▼
┌────────────┐  ┌──────────────┐
│  Local     │  │   Cloud      │
│  Cache     │  │   Backend    │
│ (FileSystem)│  │ (Simulated or Real)
└────────────┘  └──────────────┘
```

### Key Components

**`HybridStorage`** ([src/storage/hybrid/](../src/storage/hybrid/))

- Dual-backend orchestration (local + cloud)
- WAL upload pipeline with CloudAck/CloudFail events
- SST read/write routing (local first, cloud fallback)
- Backpressure and budget management

**`CloudExecutor`** ([src/storage/cloud/executor.rs](../src/storage/cloud/executor.rs))

- Embedded tokio runtime for async cloud I/O
- Callback-based event delivery (no futures exposed to engine)
- Request/response abstraction over HTTP

**Cloud Providers** ([src/storage/providers/](../src/storage/providers/))

- Individual implementations: AWS S3, Azure Blob, GCS, MinIO, Wasabi, OCI
- REST API clients with native authentication
- All use `CloudBackend` trait for uniformity

### CloudFirst Durability Policy

When `Storage::Cloud` is used, the engine automatically uses **CloudFirst durability**:

1. Write to local WAL segment
2. Enqueue segment for cloud upload
3. Cloud upload completes → emit `CloudAck` event
4. Runtime receives `CloudAck` → apply to memtable
5. Data becomes visible to reads

**Correctness guarantee:** No write is visible until cloud upload succeeds.

**Correctness guarantee:** No write is visible until cloud upload succeeds.

This is the same durability model used by modern cloud-native databases (Neon, PlanetScale, CockroachDB).

## Getting Started

### Basic Cloud Configuration

```rust
use cntryl_midge::{Engine, OpenOptions};

// Cloud mode connects to your configured cloud provider
let opts = OpenOptions::cloud(
    "/tmp/cache",        // Local cache for fast reads
    "my-bucket",         // Cloud bucket name
    "db-prefix/"         // Object key prefix
).build();

let engine = Engine::open(opts)?;

// All operations work identically to local mode
let cf = engine.default_cf();
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
tx.put(b"key".to_vec(), b"value".to_vec(), None)?;
engine.commit(tx, WriteOptions::buffered())?;
```

**What happens:**

- Local cache path stores hot data for fast reads
- WAL segments uploaded to cloud storage bucket
- SSTs uploaded as they're flushed and compacted
- Cloud provider credentials auto-discovered from environment

### Recovery from Cloud Storage

```rust
// First engine instance
let opts1 = OpenOptions::cloud("/tmp/cache", "bucket", "prefix/").build();
let engine1 = Engine::open(opts1)?;
// ... write data ...
drop(engine1);

// Second engine instance recovers from cloud
let opts2 = OpenOptions::cloud("/tmp/cache", "bucket", "prefix/").build();
let engine2 = Engine::open(opts2)?;  // ← Replays WAL from cloud storage
```

This enables seamless recovery across ephemeral compute instances.

## Provider Configuration

Midge supports multiple cloud providers with automatic credential discovery.

### AWS S3 Provider

**Location:** [src/storage/providers/aws.rs](../src/storage/providers/aws.rs)

**Features:**

- Full AWS SigV4 request signing
- Regional endpoints
- IAM role support via environment variables
- Access key + secret key authentication
- Automatic credential discovery from AWS environment

**Credentials:**

Midge automatically discovers credentials from:
- Environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`
- IAM roles (when running on EC2/ECS/Lambda)
- AWS configuration files (if available)

**Configuration:**

```rust
use cntryl_midge::{Engine, OpenOptions};

// Credentials auto-discovered from environment
let opts = OpenOptions::cloud(
    "/var/cache/midge",
    "my-s3-bucket",
    "production/db1/"
)
.region("us-west-2")  // Optional: defaults to us-east-1
.build();

let engine = Engine::open(opts)?;
```

**Supported operations:**
- PUT/GET/DELETE/LIST/HEAD with full S3 semantics
- Multipart uploads for large objects
- Proper error handling and automatic retries
- Range reads for efficient SST access

### Azure Blob Provider

**Location:** [src/storage/providers/azure.rs](../src/storage/providers/azure.rs)

**Features:**

- Native Azure Blob REST API (no S3 compatibility layer)
- Shared Key authentication (HMAC-SHA256)
- SAS token authentication
- Connection string support
- Zero Azure SDK dependencies

**Credentials:**

Midge discovers Azure credentials from (in priority order):
1. **Connection String**: `AZURE_STORAGE_CONNECTION_STRING`
2. **Shared Key**: `AZURE_STORAGE_KEY`
3. **SAS Token**: `AZURE_STORAGE_SAS_TOKEN`
4. **Managed Identity**: `AZURE_CLIENT_ID` (or system-assigned if not set)

**Configuration:**

```rust
use cntryl_midge::{Engine, OpenOptions};

// Option 1: Using connection string
std::env::set_var(
    "AZURE_STORAGE_CONNECTION_STRING",
    "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=..."
);

// Option 2: Using Managed Identity (recommended for Azure VMs/ACA)
std::env::set_var("AZURE_CLIENT_ID", "<user-assigned-identity-client-id>");
// Or rely on system-assigned identity (no AZURE_CLIENT_ID needed)

let opts = OpenOptions::cloud(
    "/var/cache/midge",
    "my-container",
    "production/db1/"
).build();

let engine = Engine::open(opts)?;
```

**API Details:**

- Base URL: `https://{account}.blob.core.windows.net/{container}`
- Full BlockBlob support (PUT/GET/DELETE/LIST)
- Range reads with `x-ms-range` header
- Efficient prefix listing with `comp=list`

**Managed Identity Support:**

Midge fully supports Azure Managed Identity for passwordless authentication:

```rust
use cntryl_midge::storage::providers::AzureProvider;

// System-assigned Managed Identity
let provider = AzureProvider::with_managed_identity(
    "mystorageaccount".into(),
    "mycontainer".into(),
    None  // No client_id = system-assigned
)?;

// User-assigned Managed Identity
let provider = AzureProvider::with_managed_identity(
    "mystorageaccount".into(),
    "mycontainer".into(),
    Some("00000000-0000-0000-0000-000000000000".into())
)?;

// Or use automatic discovery from environment
std::env::set_var("AZURE_CLIENT_ID", "00000000-0000-0000-0000-000000000000");
let provider = AzureProvider::from_env(
    "mystorageaccount".into(),
    "mycontainer".into()
)?;
```

**Works on:**
- Azure Virtual Machines (with managed identity enabled)
- Azure Container Apps
- Azure App Service
- Azure Kubernetes Service (AKS)
- Azure Container Instances
- Any Azure service supporting managed identities

**Token management:**
- Automatic token fetch from Azure Instance Metadata Service (IMDS)
- Token caching with automatic refresh before expiry
- Transparent OAuth bearer token authentication
- No manual credential management required
- DELETE → Standard blob deletion

### GCS Provider

**Location:** [src/storage/providers/gcs.rs](../src/storage/providers/gcs.rs)

**Features:**

- S3-compatible API via GCS interoperability mode
- HMAC key authentication
- Zero Google SDK dependencies
- Full API parity with S3

**Credentials:**

Midge uses GCS HMAC keys (S3-compatible credentials):
- Environment variables: `GCS_ACCESS_KEY_ID`, `GCS_SECRET_ACCESS_KEY`
- Created via `gsutil hmac create` command

**Configuration:**

```rust
use cntryl_midge::{Engine, OpenOptions};

// Set credentials from GCS HMAC keys
std::env::set_var("GCS_ACCESS_KEY_ID", "GOOG...");
std::env::set_var("GCS_SECRET_ACCESS_KEY", "...");

let opts = OpenOptions::cloud(
    "/var/cache/midge",
    "my-gcs-bucket",
    "production/db1/"
).provider("gcs").build();

let engine = Engine::open(opts)?;
```

**Setup (GCS HMAC keys):**

```bash
# Create HMAC key for service account
gsutil hmac create service-account@project.iam.gserviceaccount.com

# Returns AWS-compatible access key and secret
# Use these as GCS_ACCESS_KEY_ID and GCS_SECRET_ACCESS_KEY
```

### S3-Compatible Providers

**Supported:**

- **Cloudflare R2** - S3-compatible edge storage
- **MinIO** - Self-hosted S3-compatible storage
- **Wasabi** - Hot cloud storage with S3 API
- **Oracle Cloud (OCI)** - Oracle Cloud Infrastructure Object Storage
- **Digital Ocean Spaces** - S3-compatible object storage
- **Backblaze B2** (via S3-compatible API)

**Configuration (Cloudflare R2):**

```rust
use cntryl_midge::{Engine, OpenOptions};

std::env::set_var("R2_ACCESS_KEY_ID", "...");
std::env::set_var("R2_SECRET_ACCESS_KEY", "...");
std::env::set_var("R2_ENDPOINT", "https://<account-id>.r2.cloudflarestorage.com");

let opts = OpenOptions::cloud(
    "/var/cache/midge",
    "my-r2-bucket",
    "production/"
)
.provider("r2")
.build();

let engine = Engine::open(opts)?;
```

**Configuration (MinIO):**

```rust
use cntryl_midge::{Engine, OpenOptions};

std::env::set_var("MINIO_ENDPOINT", "http://localhost:9000");
std::env::set_var("MINIO_ACCESS_KEY", "minioadmin");
std::env::set_var("MINIO_SECRET_KEY", "minioadmin");

let opts = OpenOptions::cloud(
    "/tmp/cache",
    "my-bucket",
    "test/"
)
.provider("minio")
.build();

let engine = Engine::open(opts)?;
```

## Credential Management

### Environment-Based Discovery

Midge automatically discovers credentials from environment variables:

**AWS S3:**
```bash
export AWS_ACCESS_KEY_ID="AKIA..."
export AWS_SECRET_ACCESS_KEY="..."
export AWS_SESSION_TOKEN="..."  # Optional for temporary credentials
export AWS_REGION="us-west-2"   # Optional, defaults to us-east-1
```

**Azure Blob:**
```bash
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;..."
# OR
export AZURE_STORAGE_ACCOUNT="myaccount"
export AZURE_STORAGE_KEY="..."
```

**Google Cloud Storage:**
```bash
export GCS_ACCESS_KEY_ID="GOOG..."
export GCS_SECRET_ACCESS_KEY="..."
```

### IAM Roles and Instance Profiles

When running on cloud compute instances, Midge automatically uses instance credentials:

- **AWS EC2/ECS/Lambda:** Uses IAM role credentials via instance metadata service
- **Azure VM:** Uses managed identity (if configured)
- **GCP Compute Engine:** Uses service account from instance metadata

No explicit credential configuration needed - just assign the appropriate IAM role/managed identity to your compute instance.

## Testing Cloud Storage

### Unit Tests

Provider implementations have comprehensive unit tests:

```bash
cargo test --lib storage::providers
```

### Integration Tests

Cloud mode integration tests use filesystem simulation:

```bash
cargo test --test engine_cloud
```

### Manual Testing with Real Providers

To test against a real cloud provider, you can instantiate providers directly:

```rust
// Example: Testing Azure provider
use cntryl_midge::storage::providers::AzureProvider;
use cntryl_midge::storage::cloud::{CloudBackend, CloudCallback};

let provider = AzureProvider::with_shared_key(
    "teststorage".to_string(),
    "testcontainer".to_string(),
    std::env::var("AZURE_STORAGE_KEY")?,
);

let backend = provider.backend();

// Test PUT
let (tx, rx) = std::sync::mpsc::channel();
backend.submit_put(
    "test-key".to_string(),
    b"test-value".to_vec(),
    tx,
);

match rx.recv()? {
    CloudEvent::PutComplete { result, .. } => {
        println!("PUT result: {:?}", result);
    }
    _ => panic!("unexpected event"),
}
```

### Mock Cloud Backend

For deterministic testing without network I/O:

```rust
use cntryl_midge::storage::cloud::MockCloudBackend;

let mock = MockCloudBackend::new();

// Simulate successful PUT
mock.expect_put("key", Ok(()));

// Use mock in HybridStorage tests
let hybrid = HybridStorage::new(local_backend, Arc::new(mock));
```

## Next Steps

**For production deployments today:**

- Use `Storage::Local` with persistent disk
- Standard filesystem-based durability
- Well-tested and production-ready

**For cloud storage development:**

- Contribute to provider integration (see `src/engine/mod.rs::open()`)
- Add credential management
- Performance testing infra

**Documentation:**

- [Architecture](../development/architecture.md) - System design and layer structure
- [Durability](../user-guides/durability.md) - Durability guarantees and WAL replay
- [API Guide](../user-guides/api-guide.md) - Public API surface and usage patterns
