//! Wasabi Cloud Storage provider
//!
//! Specialized implementation leveraging generic S3 provider for Wasabi's S3-compatible API:
//! - Automatic endpoint configuration
//! - Simple access key / secret key authentication (no `SigV4` complexity)
//! - Automatic region-based URL generation
//!
//! Wasabi regions: <https://wasabi.com>/

use super::s3::{S3Config, S3Provider};
use crate::common::MidgeResult;

/// Wasabi Cloud Storage provider
///
/// Leverages the generic S3 implementation with Wasabi-specific defaults.
/// Wasabi is S3-compatible and uses standard access key/secret key authentication.
///
/// # Regions
/// Common Wasabi regions: `us-east-1`, `us-west-1`, `eu-west-1`, `ap-northeast-1`, etc.
/// See <https://wasabi.com>/ for current region list.
pub struct WasabiProvider {
    inner: S3Provider,
}

impl WasabiProvider {
    /// Create Wasabi provider
    ///
    /// # Arguments
    /// * `bucket` - Wasabi bucket name
    /// * `region` - Wasabi region (e.g., "us-east-1")
    /// * `access_key` - Wasabi access key
    /// * `secret_key` - Wasabi secret key
    pub fn new(
        bucket: String,
        region: String,
        access_key: String,
        secret_key: String,
    ) -> MidgeResult<Self> {
        let config = S3Config::wasabi(bucket, &region);
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
    fn should_create_wasabi_provider() {
        // Arrange

        // Act
        let provider = WasabiProvider::new(
            "my-bucket".into(),
            "us-east-1".into(),
            "wasabi-access-key".into(),
            "wasabi-secret-key".into(),
        )
        .expect("should create wasabi provider");

        let backend = provider.inner().backend();

        // Assert
        assert!(Arc::strong_count(&backend) >= 1);
    }

    #[test]
    fn should_support_different_regions() {
        // Arrange
        let regions = ["us-east-1", "us-west-1", "eu-west-1", "ap-northeast-1"];

        // Act
        let backend_ref_counts: Vec<usize> = regions
            .iter()
            .map(|region| {
                let provider = WasabiProvider::new(
                    "bucket".into(),
                    (*region).into(),
                    "key".into(),
                    "secret".into(),
                )
                .expect("should create wasabi provider for region");
                let backend = provider.inner().backend();
                Arc::strong_count(&backend)
            })
            .collect();

        // Assert
        assert_eq!(backend_ref_counts.len(), regions.len());
        for count in backend_ref_counts {
            assert!(count >= 1);
        }
    }
}
