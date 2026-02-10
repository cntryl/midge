# Cloud Storage Setup

Guide to configuring Midge for cloud object storage backends.

## Table of Contents

- [Overview](#overview)
- [Storage Providers](#storage-providers)
- [AWS S3 Setup](#aws-s3-setup)
- [Azure Blob Storage](#azure-blob-storage)
- [Google Cloud Storage](#google-cloud-storage)
- [Cloudflare R2](#cloudflare-r2)
- [MinIO / S3-Compatible](#minio--s3-compatible)
- [Hybrid Storage Model](#hybrid-storage-model)
- [Performance Considerations](#performance-considerations)
- [Cost Optimization](#cost-optimization)
- [Troubleshooting](#troubleshooting)

## Overview

Midge's Cloud storage mode treats **cloud object storage as the source of truth**, with local disk as an ephemeral cache.

**Key characteristics:**
- WAL and SSTs persist to cloud storage
- Local disk is cache only (can be lost without data loss)
- Recovery reads from cloud, not local disk
- Designed for serverless, cloud-native deployments

**When to use Cloud mode:**
- Serverless applications (AWS Lambda, Cloud Functions)
- Container-based deployments (Kubernetes, ECS)
- Distributed systems with ephemeral compute
- Multi-region durability requirements
- When local disk may disappear without warning

**When NOT to use Cloud mode:**
- Single-node deployments with persistent disk → use `Storage::Local`
- Latency-critical workloads (cloud adds network RTT)
- Extreme write throughput (cloud upload bandwidth limited)

## Storage Providers

Midge supports any S3-compatible object storage service.

| Provider | Compatibility | Notes |
|----------|--------------|-------|
| **AWS S3** | Native | Full support, recommended for AWS deployments |
| **Azure Blob** | S3-compatible | Via Azure S3 gateway |
| **Google Cloud Storage** | S3-compatible | Via GCS S3 interoperability |
| **Cloudflare R2** | S3-compatible | Zero egress fees, fast edge access |
| **MinIO** | S3-compatible | Self-hosted, on-prem or private cloud |
| **Wasabi** | S3-compatible | Hot storage with predictable pricing |
| **DigitalOcean Spaces** | S3-compatible | Works out of the box |
| **Backblaze B2** | S3-compatible | Via S3-compatible API |

**Credential mechanism:**
- Standard AWS environment variables
- IAM roles / instance profiles
- Service accounts (GCP)
- Access key + secret key

## AWS S3 Setup

### Basic Configuration

```rust
use cntryl_midge::{Engine, OpenOptions, Storage};

let opts = OpenOptions::new()
    .storage(Storage::Cloud {
        local_cache_path: "/tmp/midge-cache".into(),
        bucket: "my-app-database".to_string(),
        prefix: "prod/db1/".to_string(),
        endpoint: None,  // Use default S3 endpoint
        region: Some("us-west-2".to_string()),
    })
    .build();

let engine = Engine::open(opts)?;
```

### Credentials

**Option 1: Environment variables** (recommended)

```bash
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_REGION=us-west-2
```

**Option 2: IAM Role** (EC2, ECS, Lambda)

No credentials needed. Midge uses instance metadata service automatically.

```rust
// No AWS_ACCESS_KEY_ID needed when running on EC2 with IAM role
let opts = OpenOptions::new()
    .storage(Storage::Cloud {
        local_cache_path: "/tmp/cache".into(),
        bucket: "my-bucket".to_string(),
        prefix: "db/".to_string(),
        endpoint: None,
        region: Some("us-east-1".to_string()),
    })
    .build();
```

**Option 3: AWS credentials file** (`~/.aws/credentials`)

```ini
[default]
aws_access_key_id = AKIAIOSFODNN7EXAMPLE
aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

### S3 Bucket Setup

Create bucket with appropriate permissions:

```bash
aws s3api create-bucket \
    --bucket my-app-database \
    --region us-west-2 \
    --create-bucket-configuration LocationConstraint=us-west-2

# Enable versioning (recommended for recovery)
aws s3api put-bucket-versioning \
    --bucket my-app-database \
    --versioning-configuration Status=Enabled
```

**IAM Policy** (minimum permissions):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::my-app-database",
        "arn:aws:s3:::my-app-database/*"
      ]
    }
  ]
}
```

### Regional Endpoints

Specify region for lowest latency:

```rust
.storage(Storage::Cloud {
    // ...
    region: Some("eu-central-1".to_string()),
    endpoint: None,  // Use AWS regional endpoint
})
```

**Best practice:** Co-locate compute and bucket in same region.

## Azure Blob Storage

Azure provides S3-compatible API via storage accounts.

### Configuration

```rust
let opts = OpenOptions::new()
    .storage(Storage::Cloud {
        local_cache_path: "/tmp/cache".into(),
        bucket: "my-container".to_string(),
        prefix: "db/".to_string(),
        endpoint: Some("https://myaccount.blob.core.windows.net".to_string()),
        region: None,
    })
    .build();
```

### Credentials

**Environment variables:**

```bash
export AZURE_STORAGE_ACCOUNT=myaccount
export AZURE_STORAGE_KEY=<base64-encoded-key>
```

Or use Azure AD authentication (managed identity).

### Create Storage Container

```bash
az storage container create \
    --name my-container \
    --account-name myaccount
```

## Google Cloud Storage

GCS provides S3-compatible interoperability API.

### Configuration

```rust
let opts = OpenOptions::new()
    .storage(Storage::Cloud {
        local_cache_path: "/tmp/cache".into(),
        bucket: "my-gcs-bucket".to_string(),
        prefix: "db/".to_string(),
        endpoint: Some("https://storage.googleapis.com".to_string()),
        region: None,
    })
    .build();
```

### Credentials

**Option 1: Service Account Key**

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
```

**Option 2: Workload Identity** (GKE)

Automatic when running on GKE with workload identity enabled.

### Enable S3 Interoperability

```bash
gsutil hmac create <service-account-email>
```

Use HMAC keys as AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.

### Create GCS Bucket

```bash
gcloud storage buckets create gs://my-gcs-bucket \
    --location=us-central1
```

## Cloudflare R2

R2 provides S3-compatible API with zero egress fees.

### Configuration

```rust
let opts = OpenOptions::new()
    .storage(Storage::Cloud {
        local_cache_path: "/tmp/cache".into(),
        bucket: "my-r2-bucket".to_string(),
        prefix: "db/".to_string(),
        endpoint: Some("https://<account-id>.r2.cloudflarestorage.com".to_string()),
        region: Some("auto".to_string()),
    })
    .build();
```

### Credentials

Get R2 API token from Cloudflare dashboard:

```bash
export AWS_ACCESS_KEY_ID=<r2-access-key>
export AWS_SECRET_ACCESS_KEY=<r2-secret-key>
```

### Create R2 Bucket

Via Cloudflare dashboard or API:

```bash
curl -X POST "https://api.cloudflare.com/client/v4/accounts/<account-id>/r2/buckets" \
  -H "Authorization: Bearer <api-token>" \
  -H "Content-Type: application/json" \
  --data '{"name":"my-r2-bucket"}'
```

**Benefits:**
- Zero egress fees (huge cost savings)
- Fast edge network
- S3-compatible without AWS lock-in

## MinIO / S3-Compatible

Self-hosted S3-compatible storage.

### Configuration

```rust
let opts = OpenOptions::new()
    .storage(Storage::Cloud {
        local_cache_path: "/tmp/cache".into(),
        bucket: "midge-db".to_string(),
        prefix: "test/".to_string(),
        endpoint: Some("http://localhost:9000".to_string()),
        region: Some("us-east-1".to_string()),
    })
    .build();
```

### Credentials

```bash
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
```

### Run MinIO Locally

```bash
docker run -p 9000:9000 -p 9001:9001 \
    --name minio \
    -e MINIO_ROOT_USER=minioadmin \
    -e MINIO_ROOT_PASSWORD=minioadmin \
    minio/minio server /data --console-address ":9001"
```

Create bucket:

```bash
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb local/midge-db
```

**Use cases:**
- Development/testing
- On-premise deployments
- Private cloud
- Air-gapped environments

## Hybrid Storage Model

Cloud mode uses a **hybrid storage architecture**:

```
┌─────────────────┐
│  Application    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Midge Engine   │
└────────┬────────┘
         │
    ┌────┴────┐
    ▼         ▼
┌────────┐  ┌──────────────┐
│ Local  │  │ Cloud Object │
│ Cache  │  │   Storage    │ ◄── Source of Truth
└────────┘  └──────────────┘
 (Ephemeral)   (Durable)
```

### Local Cache Directory

The `local_cache_path` is used for:
- WAL buffering (write locally, upload async)
- SST prefetch (download hot blocks)
- Temporary compaction output

**Important:** This directory can be deleted without data loss. Recovery reads from cloud.

**Size requirements:**
- Minimum: 2-3x write_buffer_size (for WAL rotation)
- Recommended: 10-20% of total dataset (for SST cache)
- Example: 10GB dataset → 1-2GB local cache

**Location choices:**

```rust
// Ephemeral container storage
.local_cache_path: "/tmp/midge-cache".into()

// Fast NVMe (if available)
.local_cache_path: "/mnt/nvme/cache".into()

// Shared volume (Kubernetes)
.local_cache_path: "/var/lib/midge/cache".into()
```

### Write Path (Cloud Mode)

1. **Write to local WAL** (low latency)
2. **Apply to memtable**
3. **Return to caller** (write acknowledged)
4. **Background: Upload WAL to cloud** (async)
5. **Background: Flush memtable to SST**
6. **Background: Upload SST to cloud**
7. **Update manifest** (cloud-persisted)

**Key point:** Caller sees fast write latency (local write). Cloud upload happens asynchronously.

### Read Path (Cloud Mode)

1. **Check memtable** (in-memory, fast)
2. **Check block cache** (local cache, fast)
3. **Check local SST files** (if cached, fast)
4. **Download from cloud** (network RTT, slower)
5. **Cache locally** (for future reads)

**Key point:** Hot data is cached locally. Cold data fetched from cloud on demand.

## Performance Considerations

### Write Performance

**Factors affecting write latency:**
- Local WAL write speed (fast, typically <1ms)
- WriteOptions choice (sync vs buffered)
- Background cloud upload bandwidth

**Optimization tips:**
1. Use `WriteOptions::buffered()` (not `sync()`) for throughput
2. Batch writes in transactions (100-1000 ops)
3. Provision adequate upload bandwidth
4. Use CloudFirst policy for true cloud durability

**Expected write latency:**
- `buffered()`: 1-5ms (local WAL only)
- `sync()`: 5-20ms (local fsync)
- `cloud_strict()`: 50-200ms (cloud round-trip)

### Read Performance

**Factors affecting read latency:**
- Cache hit rate (in-memory = fast, cloud = slow)
- Block size (larger blocks = fewer cloud requests)
- Network bandwidth and RTT

**Optimization tips:**
1. Increase memory_budget (larger cache)
2. Use prefix scans (better than full range)
3. Set read-ahead for sequential access
4. Keep hot data in local cache (via access patterns)

**Expected read latency:**
- Cache hit: <1ms
- Local SST hit: 1-5ms
- Cloud fetch: 50-200ms (first access)

### Bandwidth Requirements

**Upload bandwidth** (write-heavy):
- 1000 writes/sec × 1KB avg = ~1 MB/sec upload
- Flush every 64MB → ~1 upload per minute
- Compaction output → sustained uploads

**Download bandwidth** (read-heavy, cold cache):
- 1000 reads/sec × 4KB blocks = ~4 MB/sec download
- Cache warms up over time (downloads decrease)

**Recommendation:** Provision 10-20 Mbps for typical workloads.

## Cost Optimization

### Storage Costs

**Cost breakdown:**
- **Storage**: $0.023/GB/month (S3 Standard)
- **PUT requests**: $0.005/1000 requests
- **GET requests**: $0.0004/1000 requests
- **Data transfer out**: $0.09/GB (AWS, free on Cloudflare R2)

**Optimization strategies:**

1. **Use appropriate storage class**
   - Hot data: S3 Standard
   - Warm data: S3 Infrequent Access
   - Cold data: S3 Glacier (not recommended for Midge)

2. **Minimize PUTs** (most expensive)
   - Increase memtable size (fewer flushes)
   - Tune compaction (fewer SST writes)
   - Batch WAL uploads

3. **Enable compression** (default in Midge)
   - Reduces storage bytes
   - Reduces transfer costs
   - Minor CPU overhead

4. **Use Cloudflare R2** (zero egress)
   - Same S3 API
   - No data transfer out fees
   - Significant savings for read-heavy workloads

5. **Set lifecycle policies**
   - Delete old WAL segments (after compaction)
   - Archive old SSTs (if time-series data)

### Request Costs

**WAL uploads:**
- 1000 commits/sec → ~60 uploads/minute (batched)
- ~$0.30/month for 1M commits

**SST operations:**
- Flush: 1 PUT per SST (~64MB)
- Compaction: N PUTs per compaction
- Reads: 1 GET per uncached block (4KB)

**Typical monthly cost** (example):
- 1GB dataset, 100k ops/day
- Storage: $0.023
- Requests: ~$1-2
- Transfer (AWS): ~$5-10
- **Total: ~$6-12/month**

**Same workload on R2:**
- Storage: $0.015/GB
- Requests: ~$1-2
- Transfer: $0 (zero egress)
- **Total: ~$1-2/month** (80% savings)

## Troubleshooting

### Connection Issues

**Symptom:** `Error: connection refused`

**Causes:**
- Incorrect endpoint URL
- Firewall blocking outbound HTTPS
- Wrong region specified

**Solutions:**
```rust
// Verify endpoint
.endpoint: Some("https://s3.us-west-2.amazonaws.com".to_string())

// Check connectivity
$ curl -I https://s3.us-west-2.amazonaws.com
```

### Authentication Failures

**Symptom:** `Error: 403 Forbidden` or `SignatureDoesNotMatch`

**Causes:**
- Missing/incorrect credentials
- IAM policy too restrictive
- Clock skew (S3 signature validation)

**Solutions:**
```bash
# Verify credentials
$ aws s3 ls s3://my-bucket/ --profile default

# Check IAM permissions
$ aws iam get-user

# Fix clock skew
$ sudo ntpdate -s time.nist.gov
```

### Slow Performance

**Symptom:** High read latency (>500ms)

**Causes:**
- Cold cache (all reads from cloud)
- Small memory_budget (poor cache hit rate)
- Far region (high network RTT)

**Solutions:**
```rust
// Increase cache size
.memory_budget: MemoryBudget::Bytes(2 * 1024 * 1024 * 1024)  // 2GB

// Use closer region
.region: Some("us-west-2".to_string())  // Match compute region

// Pre-warm cache (run read workload on startup)
```

### High Cloud Costs

**Symptom:** Unexpected S3 bill

**Causes:**
- Too many PUT requests (frequent flushes)
- High egress (many reads from cloud)
- Small block sizes (more requests)

**Solutions:**
```rust
// Increase memtable size (fewer flushes)
.goal: Goal::Economy  // Optimizes for cost

// Use Cloudflare R2 (zero egress)
.endpoint: Some("https://<account>.r2.cloudflarestorage.com".to_string())

// Enable request logging
$ aws s3api get-bucket-logging --bucket my-bucket
```

### Recovery Failures

**Symptom:** `Error: manifest not found` on Engine::open

**Causes:**
- Empty bucket (first time setup)
- Wrong prefix path
- Bucket versioning disabled + data loss

**Solutions:**
```rust
// Verify bucket and prefix
.bucket: "my-bucket".to_string()
.prefix: "prod/db1/"  // Must match previous prefix exactly

// Check manifest exists
$ aws s3 ls s3://my-bucket/prod/db1/MANIFEST
```

### Local Cache Issues

**Symptom:** `Error: disk full` or cache thrashing

**Causes:**
- Local cache path out of space
- Cache directory not writable
- Shared cache between multiple engines

**Solutions:**
```bash
# Check disk space
$ df -h /tmp

# Ensure writable
$ chmod 755 /tmp/midge-cache

# Use unique cache per engine
.local_cache_path: "/tmp/midge-cache-db1".into()
```

## Security Considerations

### Encryption at Rest

**S3:** Enable server-side encryption

```bash
aws s3api put-bucket-encryption \
    --bucket my-bucket \
    --server-side-encryption-configuration '{
      "Rules": [{
        "ApplyServerSideEncryptionByDefault": {
          "SSEAlgorithm": "AES256"
        }
      }]
    }'
```

**Midge:** Supports SSE-S3 and SSE-KMS transparently.

### Encryption in Transit

All cloud communication uses HTTPS by default. No configuration needed.

### Access Control

**Principle of least privilege:**
- Grant only required S3 permissions (PutObject, GetObject, ListBucket)
- Use IAM roles instead of access keys when possible
- Separate buckets per environment (dev, staging, prod)
- Use bucket policies to restrict access by IP or VPC

### Credential Management

**Best practices:**
- Never commit credentials to git
- Use environment variables or secrets manager
- Rotate credentials regularly
- Use short-lived STS tokens in production

## Next Steps

- **Recovery guarantees**: [RECOVERY.md](RECOVERY.md)
- **Performance tuning**: [PERFORMANCE_TUNING.md](PERFORMANCE_TUNING.md)
- **API reference**: [API_GUIDE.md](API_GUIDE.md)
