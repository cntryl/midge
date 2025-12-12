#![cfg_attr(not(feature = "cloud-common"), allow(unused))]
//! Wasabi Cloud Storage provider
//!
//! Specialized implementation leveraging generic S3 provider for Wasabi's S3-compatible API:
//! - Automatic endpoint configuration
//! - Simple access key / secret key authentication (no SigV4 complexity)
//! - Automatic region-based URL generation
//!
//! Wasabi regions: https://wasabi.com/

#[cfg(feature = "cloud-common")]
use super::s3::{S3Config, S3Provider};

#[cfg(not(feature = "cloud-common"))]
/// Stub Wasabi provider when async cloud features are disabled.
pub struct WasabiProvider;

#[cfg(not(feature = "cloud-common"))]
impl WasabiProvider {
    pub fn new(_bucket: String, _region: String, _access_key: String, _secret_key: String) -> Self {
        Self
    }
}

/// Wasabi Cloud Storage provider
///
/// Leverages the generic S3 implementation with Wasabi-specific defaults.
/// Wasabi is S3-compatible and uses standard access key/secret key authentication.
///
/// # Regions
/// Common Wasabi regions: `us-east-1`, `us-west-1`, `eu-west-1`, `ap-northeast-1`, etc.
/// See https://wasabi.com/ for current region list.
#[cfg(feature = "cloud-common")]
pub struct WasabiProvider {
    inner: S3Provider,
}

#[cfg(feature = "cloud-common")]
impl WasabiProvider {
    /// Create Wasabi provider
    ///
    /// # Arguments
    /// * `bucket` - Wasabi bucket name
    /// * `region` - Wasabi region (e.g., "us-east-1")
    /// * `access_key` - Wasabi access key
    /// * `secret_key` - Wasabi secret key
    pub fn new(bucket: String, region: String, access_key: String, secret_key: String) -> Self {
        let config = S3Config::wasabi(bucket, region);
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
    fn should_create_wasabi_provider() {
        let provider = WasabiProvider::new(
            "my-bucket".into(),
            "us-east-1".into(),
            "wasabi-access-key".into(),
            "wasabi-secret-key".into(),
        );
        let _ = provider.inner();
    }

    #[test]
    #[cfg(feature = "cloud-common")]
    fn should_support_different_regions() {
        let regions = vec!["us-east-1", "us-west-1", "eu-west-1", "ap-northeast-1"];
        for region in regions {
            let provider = WasabiProvider::new(
                "bucket".into(),
                region.into(),
                "key".into(),
                "secret".into(),
            );
            let _ = provider.inner();
        }
    }

    #[test]
    #[cfg(not(feature = "cloud-common"))]
    fn stub_wasabi_provider_compiles() {
        let _provider = WasabiProvider::new(
            "bucket".into(),
            "region".into(),
            "key".into(),
            "secret".into(),
        );
    }
}
