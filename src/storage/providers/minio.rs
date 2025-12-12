#![cfg_attr(not(feature = "cloud-common"), allow(unused))]
//! MinIO S3-compatible object storage provider
//!
//! Specialized implementation leveraging generic S3 provider for MinIO:
//! - Local or cloud-hosted MinIO deployments
//! - Simple access key / secret key authentication
//! - Automatic path-style endpoint configuration
//! - Support for custom endpoints (local, self-hosted, or cloud providers)

#[cfg(feature = "cloud-common")]
use super::s3::{S3Config, S3Provider};

#[cfg(not(feature = "cloud-common"))]
/// Stub MinIO provider when async cloud features are disabled.
pub struct MinioProvider;

#[cfg(not(feature = "cloud-common"))]
impl MinioProvider {
    pub fn new(
        _bucket: String,
        _endpoint: String,
        _access_key: String,
        _secret_key: String,
    ) -> Self {
        Self
    }
}

/// MinIO S3-compatible object storage provider
///
/// Leverages the generic S3 implementation with MinIO-specific defaults.
/// MinIO is fully S3-compatible and uses standard access key/secret key authentication.
///
/// # Deployment modes
/// - **Local**: `http://localhost:9000` for development
/// - **Self-hosted**: Private MinIO cluster on your infrastructure
/// - **Cloud**: MinIO Operator on Kubernetes or other managed services
#[cfg(feature = "cloud-common")]
pub struct MinioProvider {
    inner: S3Provider,
}

#[cfg(feature = "cloud-common")]
impl MinioProvider {
    /// Create MinIO provider
    ///
    /// # Arguments
    /// * `bucket` - MinIO bucket name
    /// * `endpoint` - MinIO endpoint URL (e.g., "http://localhost:9000" or "https://minio.example.com")
    /// * `access_key` - MinIO access key
    /// * `secret_key` - MinIO secret key
    pub fn new(
        bucket: String,
        endpoint: String,
        access_key: String,
        secret_key: String,
    ) -> Self {
        let config = S3Config::minio(bucket, endpoint);
        let inner = S3Provider::custom(config, access_key, secret_key);
        Self { inner }
    }

    /// Access the underlying S3Provider for lower-level operations
    pub fn inner(&self) -> &S3Provider {
        &self.inner
    }

    /// Convert into the underlying S3Provider
    pub fn into_inner(self) -> S3Provider {
        self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(feature = "cloud-common")]
    fn should_create_minio_provider_local() {
        let provider = MinioProvider::new(
            "my-bucket".into(),
            "http://localhost:9000".into(),
            "minioadmin".into(),
            "minioadmin".into(),
        );
        let _ = provider.inner();
    }

    #[test]
    #[cfg(feature = "cloud-common")]
    fn should_create_minio_provider_remote() {
        let provider = MinioProvider::new(
            "data-bucket".into(),
            "https://minio.example.com".into(),
            "access-key-id".into(),
            "secret-access-key".into(),
        );
        let _ = provider.inner();
    }

    #[test]
    #[cfg(feature = "cloud-common")]
    fn should_support_different_endpoints() {
        let endpoints = vec![
            "http://localhost:9000",
            "http://minio:9000",
            "https://minio.example.com",
            "https://s3.minio.io",
        ];
        for endpoint in endpoints {
            let provider = MinioProvider::new(
                "bucket".into(),
                endpoint.into(),
                "key".into(),
                "secret".into(),
            );
            let _ = provider.inner();
        }
    }

    #[test]
    #[cfg(not(feature = "cloud-common"))]
    fn stub_minio_provider_compiles() {
        let _provider = MinioProvider::new(
            "bucket".into(),
            "http://localhost:9000".into(),
            "key".into(),
            "secret".into(),
        );
    }
}
