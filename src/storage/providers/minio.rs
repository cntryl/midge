//! `MinIO` S3-compatible object storage provider
//!
//! Specialized implementation leveraging generic S3 provider for `MinIO`:
//! - Local or cloud-hosted `MinIO` deployments
//! - Simple access key / secret key authentication
//! - Automatic path-style endpoint configuration
//! - Support for custom endpoints (local, self-hosted, or cloud providers)

use super::s3::{S3Config, S3Provider};
use crate::common::MidgeResult;

/// `MinIO` S3-compatible object storage provider
///
/// Leverages the generic S3 implementation with MinIO-specific defaults.
/// `MinIO` is fully S3-compatible and uses standard access key/secret key authentication.
///
/// # Deployment modes
/// - **Local**: `http://localhost:9000` for development
/// - **Self-hosted**: Private `MinIO` cluster on your infrastructure
/// - **Cloud**: `MinIO` Operator on Kubernetes or other managed services
pub struct MinioProvider {
    inner: S3Provider,
}

impl MinioProvider {
    /// Create `MinIO` provider
    ///
    /// # Arguments
    /// * `bucket` - `MinIO` bucket name
    /// * `endpoint` - `MinIO` endpoint URL (e.g., "<http://localhost:9000>" or "<https://minio.example.com>")
    /// * `access_key` - `MinIO` access key
    /// * `secret_key` - `MinIO` secret key
    pub fn new(
        bucket: String,
        endpoint: String,
        access_key: String,
        secret_key: String,
    ) -> MidgeResult<Self> {
        let config = S3Config::minio(bucket, endpoint);
        S3Provider::custom(config, access_key, secret_key).map(|inner| Self { inner })
    }

    /// Access the underlying `S3Provider` for lower-level operations
    pub fn inner(&self) -> &S3Provider {
        &self.inner
    }

    /// Convert into the underlying `S3Provider`
    pub fn into_inner(self) -> S3Provider {
        self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn should_create_minio_provider_local() {
        // Arrange

        // Act
        let provider = MinioProvider::new(
            "my-bucket".into(),
            "http://localhost:9000".into(),
            "minioadmin".into(),
            "minioadmin".into(),
        )
        .expect("should create local minio provider");

        let backend = provider.inner().backend();

        // Assert
        assert!(Arc::strong_count(&backend) >= 1);
    }

    #[test]
    fn should_create_minio_provider_remote() {
        // Arrange

        // Act
        let provider = MinioProvider::new(
            "data-bucket".into(),
            "https://minio.example.com".into(),
            "access-key-id".into(),
            "secret-access-key".into(),
        )
        .expect("should create remote minio provider");

        let backend = provider.inner().backend();

        // Assert
        assert!(Arc::strong_count(&backend) >= 1);
    }

    #[test]
    fn should_support_different_endpoints() {
        // Arrange
        let endpoints = [
            "http://localhost:9000",
            "http://minio:9000",
            "https://minio.example.com",
            "https://s3.minio.io",
        ];

        // Act
        let backend_ref_counts: Vec<usize> = endpoints
            .iter()
            .map(|endpoint| {
                let provider = MinioProvider::new(
                    "bucket".into(),
                    (*endpoint).into(),
                    "key".into(),
                    "secret".into(),
                )
                .expect("should create minio provider for endpoint");
                let backend = provider.inner().backend();
                Arc::strong_count(&backend)
            })
            .collect();

        // Assert
        assert_eq!(backend_ref_counts.len(), endpoints.len());
        for count in backend_ref_counts {
            assert!(count >= 1);
        }
    }

    #[test]
    fn should_convert_minio_provider_into_inner_s3_provider() {
        // Arrange
        let provider = MinioProvider::new(
            "my-bucket".into(),
            "http://localhost:9000".into(),
            "minioadmin".into(),
            "minioadmin".into(),
        )
        .expect("should create minio provider");

        // Act
        let backend = provider.into_inner().backend();

        // Assert
        assert!(Arc::strong_count(&backend) >= 1);
    }
}
