# Cloud Backend Swap - Configuration Only

This document shows how trivially you can swap cloud backends in Midge. **ONLY the backend construction changes** - all durability modes, caching, eviction, and APIs remain identical.

## The Swap

### Development/Testing (MockCloudBackend)

```rust
use midge::cloud::MockCloudBackend;
use midge::config::cloud_builder::CloudConfigBuilder;

let backend = Arc::new(MockCloudBackend::new());

let storage_mode = CloudConfigBuilder::strict_durability(backend, "./cache")
    .with_max_cache_size_mb(1024)
    .with_path("my-app/production")
    .build();
```

### Production - AWS S3

```rust
use midge::cloud::AwsS3Backend;  // ← ONLY THIS LINE CHANGES
use midge::config::cloud_builder::CloudConfigBuilder;

let backend = Arc::new(AwsS3Backend::new(
    "us-east-1",           // AWS region
    "my-midge-bucket",     // S3 bucket name
    None,                  // Optional: custom endpoint
)?);

let storage_mode = CloudConfigBuilder::strict_durability(backend, "./cache")
    .with_max_cache_size_mb(1024)
    .with_path("my-app/production")
    .build();
```

### Production - Azure Blob Storage

```rust
use midge::cloud::AzureBlobBackend;  // ← ONLY THIS LINE CHANGES
use midge::config::cloud_builder::CloudConfigBuilder;

let backend = Arc::new(AzureBlobBackend::new(
    "mystorageaccount",    // Azure storage account
    "mycontainer",         // Blob container name
    None,                  // Optional: SAS token
)?);

let storage_mode = CloudConfigBuilder::strict_durability(backend, "./cache")
    .with_max_cache_size_mb(1024)
    .with_path("my-app/production")
    .build();
```

### Production - GCP Cloud Storage

```rust
use midge::cloud::GcpStorageBackend;  // ← ONLY THIS LINE CHANGES
use midge::config::cloud_builder::CloudConfigBuilder;

let backend = Arc::new(GcpStorageBackend::new(
    "my-gcs-bucket",       // GCS bucket name
    None,                  // Optional: custom endpoint
)?);

let storage_mode = CloudConfigBuilder::strict_durability(backend, "./cache")
    .with_max_cache_size_mb(1024)
    .with_path("my-app/production")
    .build();
```

## What Works Identically

✅ **All durability modes:**
- `CloudConfigBuilder::strict_durability(backend, path)`
- `CloudConfigBuilder::balanced_durability(backend, path)`
- `CloudConfigBuilder::replicated_durability(backend, path)`

✅ **All cache features:**
- HybridStorage local caching
- LRU eviction
- Background upload workers
- Background eviction workers
- Cache statistics

✅ **All engine APIs:**
- `engine.put(key, value)`
- `engine.get(key)`
- `engine.delete(key)`
- `engine.scan(start, end)`
- Transactions, snapshots, column families

✅ **All operational features:**
- Crash recovery
- Manifest management
- Compaction
- Metrics

## Environment-Based Selection

```rust
fn get_backend() -> Arc<dyn StorageBackend> {
    match std::env::var("CLOUD_PROVIDER").unwrap_or_default().as_str() {
        "s3" => Arc::new(AwsS3Backend::new("us-east-1", "bucket", None).unwrap()),
        "azure" => Arc::new(AzureBlobBackend::new("account", "container", None).unwrap()),
        "gcp" => Arc::new(GcpStorageBackend::new("bucket", None).unwrap()),
        _ => Arc::new(MockCloudBackend::new()),
    }
}

// Use it:
let backend = get_backend();
let storage_mode = CloudConfigBuilder::strict_durability(backend, "./cache").build();
```

## Feature Flags

Enable the cloud provider you need:

```toml
[dependencies]
midge = { version = "0.1", features = ["cloud-aws"] }      # AWS S3
# OR
midge = { version = "0.1", features = ["cloud-azure"] }    # Azure Blob
# OR
midge = { version = "0.1", features = ["cloud-gcp"] }      # GCP Cloud Storage
# OR (no feature flag needed for mock)
midge = "0.1"                                              # MockCloudBackend only
```

## Key Insight

The `StorageBackend` trait abstraction means:

1. **Write once** - Implement your application logic once
2. **Test with mock** - Use MockCloudBackend for fast, local testing
3. **Deploy anywhere** - Swap to S3/Azure/GCP with 1 line change
4. **Same guarantees** - All durability modes work identically across backends

This is **configuration-driven cloud portability** - the backend is injected, not hardcoded.

