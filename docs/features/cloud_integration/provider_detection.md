# Cloud Provider Auto-Detection

Midge's cloud backends automatically detect credentials and authentication using platform-specific metadata services and environment variables. This enables seamless deployment across AWS, Azure, and GCP without hardcoded credentials.

## Overview

Each cloud provider backend (`AwsS3Backend`, `AzureBlobBackend`, `GcpStorageBackend`) implements a **credential discovery chain** that attempts multiple authentication methods in priority order, falling back gracefully when a method is unavailable.

**Key Benefits:**
- **Zero-config deployment**: Works automatically on cloud VMs, containers, and serverless
- **Security**: No credentials in code or config files
- **Portability**: Same binary runs on dev machines and production environments
- **Best practices**: Follows each cloud provider's recommended authentication patterns

## AWS S3 Auto-Detection

**File:** `src/cloud/aws.rs`

### Credential Discovery Chain

1. **Environment Variables** (highest priority)
   - `AWS_ACCESS_KEY_ID`
   - `AWS_SECRET_ACCESS_KEY`
   - `AWS_SESSION_TOKEN` (optional, for temporary credentials)
   - **Use case:** Local development, CI/CD pipelines, AWS Lambda

2. **ECS Task Metadata Service**
   - Endpoint: `http://169.254.170.2`
   - Triggered by: `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` environment variable
   - **Use case:** AWS Fargate, ECS on EC2 (task IAM roles)
   - Returns temporary credentials with automatic rotation

3. **EC2 Instance Metadata Service (IMDSv2)**
   - Endpoint: `http://169.254.169.254/latest/`
   - Requires session token (IMDSv2 security)
   - **Use case:** EC2 instances with instance profiles
   - Steps:
     1. Get session token via PUT to `/latest/api/token`
     2. Retrieve IAM role name from `/latest/meta-data/iam/security-credentials/`
     3. Fetch temporary credentials for the role

### Supported Environments

| Environment | Detection Method | Credential Source |
|-------------|------------------|-------------------|
| AWS Lambda | Environment variables | Lambda execution role (auto-injected) |
| AWS Fargate | ECS metadata (169.254.170.2) | Task IAM role |
| ECS on EC2 | ECS metadata (169.254.170.2) | Task IAM role |
| EC2 | IMDSv2 (169.254.169.254) | Instance profile |
| Local/Dev | Environment variables | `aws configure` or manual export |

### Implementation Details

```rust
impl AwsCredentials {
    fn load() -> MidgeResult<Self> {
        // 1. Try environment variables
        if let (Ok(access_key), Ok(secret_key)) = (
            std::env::var("AWS_ACCESS_KEY_ID"),
            std::env::var("AWS_SECRET_ACCESS_KEY"),
        ) {
            return Ok(Self { access_key_id, secret_access_key, ... });
        }

        // 2. Try ECS task metadata
        if let Ok(uri) = std::env::var("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI") {
            return Self::from_ecs(&uri);
        }

        // 3. Fall back to EC2 instance metadata
        Self::from_imds()
    }
}
```

**Security Note:** IMDSv2 requires a session token (obtained via PUT request) to prevent SSRF attacks and credential theft.

## GCP Cloud Storage Auto-Detection

**File:** `src/cloud/gcp.rs`

### Credential Discovery Chain

1. **Service Account JSON File** (highest priority)
   - Environment variable: `GOOGLE_APPLICATION_CREDENTIALS`
   - Points to JSON key file path
   - **Use case:** Local development, non-GCP deployments
   - Creates JWT bearer token from private key

2. **Compute Engine Metadata Server**
   - Endpoint: `http://169.254.169.254/computeMetadata/v1/`
   - Header required: `Metadata-Flavor: Google`
   - **Use case:** GCE VMs, Cloud Run, Cloud Functions, GKE
   - Returns OAuth 2.0 access tokens with automatic expiry

### Supported Environments

| Environment | Detection Method | Credential Source |
|-------------|------------------|-------------------|
| GCE VM | Metadata server | VM service account |
| Cloud Run | Metadata server | Service identity (auto-injected) |
| Cloud Functions | Metadata server | Function identity |
| GKE | Metadata server or Workload Identity | Kubernetes service account |
| Local/Dev | Service account JSON | `GOOGLE_APPLICATION_CREDENTIALS` |

### Implementation Details

```rust
impl GcpCredentials {
    fn load() -> MidgeResult<Self> {
        // 1. Try service account JSON from environment
        if let Ok(creds_path) = std::env::var("GOOGLE_APPLICATION_CREDENTIALS") {
            return Self::from_service_account_file(&creds_path);
        }

        // 2. Try GCE metadata server
        Self::from_metadata_server()
    }

    fn from_metadata_server() -> MidgeResult<Self> {
        let url = "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token";
        let response = ureq::get(url)
            .header("Metadata-Flavor", "Google")
            .call()?;
        // Parse access token and expiry...
    }
}
```

**Token Management:** Access tokens are cached and automatically refreshed before expiry (typically 3600 seconds).

## Azure Blob Storage Auto-Detection

**File:** `src/cloud/azure.rs`

### Credential Discovery Chain

1. **Explicit Storage Account Key** (constructor parameter)
   - Passed directly to `AzureBlobBackend::new()`
   - **Use case:** Quick setup, legacy systems
   - **Security:** Avoid in production (use Managed Identity instead)

2. **Azure Managed Identity (Planned)**
   - Endpoint: `http://169.254.169.254/metadata/identity/oauth2/token`
   - Header required: `Metadata: true`
   - **Use case:** Azure VMs, App Service, AKS (recommended for production)
   - Returns Azure AD access tokens for blob operations

3. **Environment Variables** (fallback)
   - `AZURE_STORAGE_ACCOUNT`
   - `AZURE_STORAGE_KEY`
   - **Use case:** Local development, CI/CD

### Supported Environments

| Environment | Detection Method | Credential Source |
|-------------|------------------|-------------------|
| Azure VM | Managed Identity (IMDS) | System/user-assigned identity |
| App Service | Managed Identity | Service identity |
| AKS | Managed Identity or Pod Identity | Kubernetes service account |
| Local/Dev | Environment variables or constructor | Manual configuration |

### Current Implementation

```rust
impl AzureBlobBackend {
    pub fn new(account: &str, container: &str, access_key: &str) -> MidgeResult<Self> {
        // Currently requires explicit key
        // Future: Auto-detect Managed Identity if access_key is None
    }
}
```

**Roadmap:** Full Managed Identity support with IMDS integration is planned for Azure parity with AWS/GCP auto-detection.

## Metadata Service Endpoints Reference

| Cloud Provider | Metadata Endpoint | Detection Header/Variable |
|----------------|-------------------|---------------------------|
| AWS ECS/Fargate | `http://169.254.170.2` | `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` |
| AWS EC2 | `http://169.254.169.254/latest/` | IMDSv2 session token (PUT) |
| GCP | `http://169.254.169.254/computeMetadata/v1/` | `Metadata-Flavor: Google` |
| Azure | `http://169.254.169.254/metadata/identity/` | `Metadata: true` |

**Link-Local Addressing:** All metadata services use the reserved `169.254.169.254` IP (except AWS ECS which uses `169.254.170.2`), which is only accessible from within the cloud platform.

## Error Handling and Fallbacks

All backends implement graceful degradation:

1. **Timeout Protection**: Metadata requests timeout after ~5 seconds to avoid blocking
2. **Retry Logic**: Transient network errors trigger exponential backoff (3 retries default)
3. **Clear Error Messages**: Failed credential discovery returns descriptive errors
4. **No Silent Failures**: Missing credentials produce `MidgeError::CloudError` immediately

### Example Error Flow

```rust
AwsCredentials::load()
  → Try env vars → Not found
  → Try ECS metadata → Not found (not in ECS)
  → Try EC2 IMDSv2 → Success (running on EC2)
```

If all methods fail: `Err(MidgeError::CloudError("Failed to load AWS credentials: ..."))`

## Best Practices

### Production Deployments

✅ **DO:**
- Use IAM roles/Managed Identity (never hardcoded keys)
- Grant least-privilege permissions to service accounts
- Enable IMDSv2 on AWS EC2 (default on new instances)
- Use private VPC endpoints for metadata services when possible

❌ **DON'T:**
- Commit credentials to source control
- Use root/admin credentials for database operations
- Disable metadata services for security (breaks auto-detection)

### Local Development

**AWS:**
```bash
aws configure  # Creates ~/.aws/credentials
# OR
export AWS_ACCESS_KEY_ID=AKIA...
export AWS_SECRET_ACCESS_KEY=...
```

**GCP:**
```bash
gcloud auth application-default login  # Creates ~/.config/gcloud/application_default_credentials.json
# OR
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

**Azure:**
```bash
export AZURE_STORAGE_ACCOUNT=mystorageaccount
export AZURE_STORAGE_KEY=base64key...
```

### Multi-Cloud Deployments

Midge automatically adapts to the detected environment:

```rust
// Same code works on AWS, GCP, or Azure!
#[cfg(feature = "cloud-aws")]
let backend = AwsS3Backend::new("my-bucket", "us-west-2")?;

#[cfg(feature = "cloud-gcp")]
let backend = GcpStorageBackend::new("my-bucket")?;

#[cfg(feature = "cloud-azure")]
let backend = AzureBlobBackend::new("myaccount", "container", &key)?;

let wal = CloudWalWriter::new(Arc::new(backend))?;
```

Credentials are detected automatically based on the runtime environment.

## Debugging Auto-Detection

Enable debug logging to trace credential discovery:

```bash
RUST_LOG=cntryl_midge::cloud=debug cargo run
```

**AWS Example Output:**
```
DEBUG cntryl_midge::cloud::aws: Loaded AWS credentials from environment variables
```

**GCP Example Output:**
```
DEBUG cntryl_midge::cloud::gcp: Fetching credentials from GCE metadata server
DEBUG cntryl_midge::cloud::gcp: Successfully obtained access token (expires in 3600s)
```

**Troubleshooting:**
- **"No credentials found"**: Check environment variables and metadata service accessibility
- **IMDSv2 timeout**: Ensure security groups allow outbound to 169.254.169.254
- **GCP 403 Forbidden**: Verify service account has "Storage Object Admin" role
- **Azure 401 Unauthorized**: Check storage account key or Managed Identity role assignment

## Future Enhancements

- [ ] Azure Managed Identity full integration (IMDS with Azure AD tokens)
- [ ] OCI (Oracle Cloud Infrastructure) auth detection
- [ ] AWS STS AssumeRole support for cross-account access
- [ ] Credential caching with TTL-based refresh
- [ ] Support for AWS Secrets Manager / GCP Secret Manager integration
- [ ] Multi-region failover for metadata services

## Related Documentation

- [AWS S3 Integration](./aws_s3.md) - S3-specific features and examples
- [GCP Cloud Storage Integration](./gcs.md) - GCS-specific features
- [Azure Blob Storage Integration](./azure_blob.md) - Azure-specific features
- [Hybrid Storage Architecture](./hybrid_wal.md) - Combining local and cloud storage
- [Distributed Locking](./distributed_locking.md) - Multi-node coordination in cloud
