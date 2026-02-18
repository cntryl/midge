# Cloud Storage Setup

Guide to Midge's cloud storage architecture and implementation status.

> **⚠️ CURRENT STATUS: DEVELOPMENT/TESTING ONLY**
>
> Cloud storage integration is **NOT production-ready**. The `Storage::Cloud` configuration currently uses a **filesystem-backed simulation** for development and testing. Real cloud provider integration (AWS S3, Azure Blob, GCS) is partially implemented but not yet connected to the main engine path.
>
> **For production deployments, use `Storage::Local` with persistent disk.**

## Table of Contents

- [Current Implementation Status](#current-implementation-status)
- [Hybrid Storage Architecture](#hybrid-storage-architecture)
- [Development Usage](#development-usage)
- [Provider Implementations](#provider-implementations)
  - [AWS S3 Provider](#aws-s3-provider)
  - [Azure Blob Provider](#azure-blob-provider)
  - [GCS Provider](#gcs-provider)
  - [S3-Compatible Providers](#s3-compatible-providers)
- [Integration Roadmap](#integration-roadmap)
- [Testing Cloud Storage](#testing-cloud-storage)

## Current Implementation Status

### What Works Today

✅ **Hybrid storage architecture** - Local cache + cloud backend abstraction implemented  
✅ **Filesystem simulation** - `Storage::Cloud` works with local filesystem for testing  
✅ **Cloud providers** - Individual provider implementations exist (AWS, Azure, GCS, MinIO, Wasabi)  
✅ **CloudFirst durability** - WAL upload pipeline with acknowledgment flow  
✅ **Development/testing** - Full E2E testing with simulated cloud backend

### What's Not Ready

❌ **Production cloud integration** - Provider selection and configuration not wired to `Engine::open()`  
❌ **Real cloud backends** - `Storage::Cloud` uses filesystem simulation, not actual S3/Azure APIs  
❌ **Credential management** - No automatic credential lookup from environment  
❌ **Multi-region** - No region selection or failover  
❌ **Performance validation** - Cloud latency/throughput not measured in production scenarios

### Current Behavior

When you use `Storage::Cloud`:

```rust
use cntryl_midge::{Engine, OpenOptions};

let opts = OpenOptions::cloud("/tmp/cache", "my-bucket", "prefix/")
    .build();

let engine = Engine::open(opts)?;  // ← Uses filesystem simulation
```

Behind the scenes, Midge creates a **local directory structure** at `{local_cache_path}/cloud_store/` that simulates cloud storage. This is functionally correct (same API, same durability guarantees) but does **not** connect to real S3/Azure/GCS.

**Intent:** Verify correctness of cloud-first durability semantics without cloud provider complexity.  
**Limitation:** Not suitable for production deployments requiring actual cloud persistence.

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

## Development Usage

### Using Storage::Cloud in Tests

```rust
use cntryl_midge::{Engine, OpenOptions};

// Cloud mode uses filesystem simulation (not real cloud)
let opts = OpenOptions::cloud(
    "/tmp/test-cache",
    "test-bucket",
    "test-prefix/"
).build();

let engine = Engine::open(opts)?;

// All operations work as expected
let cf = engine.default_cf();
let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
tx.put(b"key".to_vec(), b"value".to_vec(), None)?;
engine.commit(tx, WriteOptions::buffered())?;
```

**What happens:**

- Local cache path: `/tmp/test-cache/`
- Simulated cloud: `/tmp/test-cache/cloud_store/`
- WAL segments uploaded to: `/tmp/test-cache/cloud_store/wal/`
- SSTs uploaded to: `/tmp/test-cache/cloud_store/sst/`

### Recovery from Simulated Cloud

```rust
// First engine instance
let opts1 = OpenOptions::cloud("/tmp/cache", "bucket", "prefix/").build();
let engine1 = Engine::open(opts1)?;
// ... write data ...
drop(engine1);

// Second engine instance recovers from "cloud"
let opts2 = OpenOptions::cloud("/tmp/cache", "bucket", "prefix/").build();
let engine2 = Engine::open(opts2)?;  // ← Reads WAL from simulated cloud
```

This tests the recovery logic without needing real cloud credentials.

## Provider Implementations

Midge has clean, dependency-free implementations of major cloud providers. These are **not yet integrated** with `Engine::open()` but are ready for use in custom storage backends.

### AWS S3 Provider

**Location:** [src/storage/providers/aws.rs](../src/storage/providers/aws.rs)

**Features:**

- Full AWS SigV4 request signing
- Regional endpoints
- IAM role support (via environment)
- Access key + secret key authentication

**Example (standalone use):**

```rust
use cntryl_midge::storage::providers::AwsS3Provider;
use cntryl_midge::storage::cloud::AwsCredentials;

let credentials = AwsCredentials {
    access_key: std::env::var("AWS_ACCESS_KEY_ID")?,
    secret_key: std::env::var("AWS_SECRET_ACCESS_KEY")?,
    session_token: None,
};

let provider = AwsS3Provider::new(
    "my-bucket".to_string(),
    "us-west-2".to_string(),
    credentials,
);

// Get CloudBackend for use with HybridStorage
let backend = provider.backend();
```

**What works:**

- PUT/GET/DELETE/LIST/HEAD operations
- Async callback-based execution via `CloudExecutor`
- Proper error handling and retries

**What's missing:**

- Automatic credential discovery from ~/.aws/credentials
- STS token refresh
- Integration with `Storage::Cloud` enum

### Azure Blob Provider

**Location:** [src/storage/providers/azure.rs](../src/storage/providers/azure.rs)

**Features:**

- Native Azure Blob REST API (not S3-compatible)
- Shared Key authentication (HMAC-SHA256)
- SAS token authentication
- No Azure SDK dependency

**Example:**

```rust
use cntryl_midge::storage::providers::AzureProvider;

// Shared Key authentication
let provider = AzureProvider::with_shared_key(
    "storageaccount".to_string(),
    "container-name".to_string(),
    std::env::var("AZURE_STORAGE_KEY")?,
);

// Or SAS token
let provider = AzureProvider::with_sas_token(
    "storageaccount".to_string(),
    "container-name".to_string(),
    "sv=2021-06-08&ss=b&srt=sco&sp=rwdlac&...".to_string(),
);

let backend = provider.backend();
```

**API Details:**

- Base URL: `https://{account}.blob.core.windows.net/{container}`
- PUT → BlockBlob
- GET → With `x-ms-range` header for range reads
- LIST → `restype=container&comp=list&prefix=...`
- DELETE → Standard blob deletion

### GCS Provider

**Location:** [src/storage/providers/gcs.rs](../src/storage/providers/gcs.rs)

**Features:**

- S3-compatible API (via GCS interoperability)
- HMAC key authentication
- No Google SDK dependency

**Example:**

```rust
use cntryl_midge::storage::providers::GcsProvider;

let provider = GcsProvider::new(
    "my-gcs-bucket".to_string(),
    std::env::var("GCS_ACCESS_KEY")?,   // From `gsutil hmac create`
    std::env::var("GCS_SECRET_KEY")?,
);

let backend = provider.backend();
```

**Setup (GCS HMAC keys):**

```bash
gsutil hmac create service-account@project.iam.gserviceaccount.com
```

Use the returned access key and secret as AWS-compatible credentials.

### S3-Compatible Providers

**Supported:**

- **MinIO** ([src/storage/providers/minio.rs](../src/storage/providers/minio.rs))
- **Wasabi** ([src/storage/providers/wasabi.rs](../src/storage/providers/wasabi.rs))
- **Oracle Cloud (OCI)** ([src/storage/providers/oci.rs](../src/storage/providers/oci.rs))
- **Generic S3** ([src/storage/providers/s3.rs](../src/storage/providers/s3.rs))

**Example (MinIO):**

```rust
use cntryl_midge::storage::providers::MinioProvider;

let provider = MinioProvider::new(
    "my-bucket".to_string(),
    "http://localhost:9000".to_string(),
    "minioadmin".to_string(),
    "minioadmin".to_string(),
);

let backend = provider.backend();
```

**Example (Cloudflare R2):**

```rust
use cntryl_midge::storage::providers::S3Provider;
use cntryl_midge::storage::providers::S3Config;

let config = S3Config::custom(
    "my-r2-bucket".to_string(),
    "auto".to_string(),
    "https://<account-id>.r2.cloudflarestorage.com".to_string(),
    false,  // path_style
);

let provider = S3Provider::new(
    config,
    std::env::var("R2_ACCESS_KEY")?,
    std::env::var("R2_SECRET_KEY")?,
);

let backend = provider.backend();
```

## Integration Roadmap

### Phase 1: Provider Selection (Not Started)

Add provider selection logic to `Engine::open()`:

```rust
// Proposed API (not implemented)
let opts = OpenOptions::cloud("/tmp/cache", "bucket", "prefix/")
    .with_provider(CloudProvider::Aws {
        region: "us-west-2".to_string(),
        credentials: AwsCredentials::from_env()?,
    })
    .build();
```

**Work required:**

- Parse `Storage::Cloud` fields to select provider
- Credential discovery (env vars, instance metadata, config files)
- Error handling for missing/invalid credentials

### Phase 2: Real Cloud Backends (Not Started)

Replace `build_cloud_backed_filesystem_simulation()` with actual provider instantiation:

```rust
// Current (in Engine::open):
let cloud = build_cloud_backed_filesystem_simulation(&db_path)?;

// Proposed:
let cloud = build_cloud_backend(&opts.storage)?;
```

**Work required:**

- Provider factory function
- Backend initialization
- Configuration validation

### Phase 3: Production Validation (Not Started)

- Performance benchmarks against real S3/Azure/GCS
- Latency and throughput measurements
- Cost analysis tooling
- Multi-region testing
- Failover and retry logic validation

### Phase 4: Advanced Features (Future)

- Multi-region replication
- Cross-cloud compatibility
- Cloud-specific optimizations (S3 intelligent tiering, Azure cool tier)
- Observability (cloud request tracing, cost tracking)

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

- [Architecture](architecture.md) - System design and layer structure
- [Recovery](recovery.md) - Durability guarantees and WAL replay
- [API Guide](api-guide.md) - Public API surface and usage patterns
