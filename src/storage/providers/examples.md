# S3-Compatible Provider Usage Examples

## AWS S3 (with SigV4 credential handling)

```rust
use cntryl_midge::storage::providers::S3Provider;
use cntryl_midge::storage::cloud::executor::AwsCredentials;

let creds = AwsCredentials {
    access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
    secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
    region: "us-east-1".to_string(),
    session_token: None,
};

let provider = S3Provider::aws(
    "my-bucket".to_string(),
    "us-east-1".to_string(),
    creds
);
```

## Wasabi (simple access key/secret)

```rust
use cntryl_midge::storage::WasabiProvider;

let provider = WasabiProvider::wasabi(
    "my-bucket".to_string(),
    "us-east-1".to_string(),
    "wasabi-access-key".to_string(),
    "wasabi-secret-key".to_string()
);
```

## MinIO (local or cloud)

```rust
use cntryl_midge::storage::MinioProvider;

let provider = MinioProvider::minio(
    "my-bucket".to_string(),
    "http://localhost:9000".to_string(),
    "minioadmin".to_string(),
    "minioadmin".to_string()
);
```

## OCI S3 Compatibility Layer

```rust
use cntryl_midge::storage::OciS3CompatProvider;

let provider = OciS3CompatProvider::oci_s3_compat(
    "my-bucket".to_string(),
    "my-namespace".to_string(),
    "us-ashburn-1".to_string(),
    "oci-access-key".to_string(),
    "oci-secret-key".to_string()
);
```

## Custom S3-Compatible Endpoint

```rust
use cntryl_midge::storage::{S3Provider, S3Config};

let config = S3Config::custom(
    "my-bucket".to_string(),
    "us-east-1".to_string(),
    "https://custom-s3.example.com".to_string(),
    true  // use path-style URLs
);

let provider = S3Provider::custom(
    config,
    "access-key".to_string(),
    "secret-key".to_string()
);
```

## Architecture Notes

- **Generic Implementation**: Single S3-compatible backend supports all vendors
- **Vendor Wrappers**: Convenience constructors for each vendor's specifics
- **Credential Handling**: 
  - AWS: Full SigV4 signing with IAM credential chains (complex)
  - Wasabi/MinIO/OCI: Simple access key/secret (straightforward)
- **Endpoint Flexibility**: Custom endpoints for self-hosted or regional services
- **Path Style**: Configurable virtual-hosted vs path-style URLs
- **No SDK Bloat**: Direct REST API calls without heavy vendor SDKs
