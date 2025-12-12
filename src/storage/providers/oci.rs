#![cfg_attr(not(feature = "cloud-common"), allow(unused))]
//! Oracle Cloud Infrastructure (OCI) Object Storage Provider
//!
//! Two modes of operation:
//! 1. **S3-compatible mode** (recommended): Uses OCI's S3-compatible API via S3Provider
//!    - Standard access key / secret key authentication
//!    - Full CloudBackend support via S3Provider
//!
//! 2. **Native OCI API** (future): Direct REST API with OCI signature-based auth
//!    - Would use OCI's proprietary authentication headers
//!    - Not yet implemented (stub functions provided)

#[cfg(feature = "cloud-common")]
use super::s3::S3Config;
#[cfg(feature = "cloud-common")]
use super::s3::S3Provider;

#[cfg(not(feature = "cloud-common"))]
/// Stub OCI provider when async cloud features are disabled.
pub struct OciProvider;

#[cfg(not(feature = "cloud-common"))]
impl OciProvider {
    pub fn new(_namespace: String, _bucket: String, _region: String) -> Self {
        Self
    }

    pub fn s3_compat(
        _bucket: String,
        _namespace: String,
        _region: String,
        _access_key: String,
        _secret_key: String,
    ) -> Self {
        Self
    }
}

/// Oracle Cloud Infrastructure Object Storage provider
///
/// Supports both:
/// - **S3-compatible API**: Leverages generic S3Provider for easy integration
/// - **Native OCI API**: (future) Direct REST API with OCI signature-based auth
#[cfg(feature = "cloud-common")]
pub struct OciProvider {
    inner: S3Provider,
}

#[cfg(feature = "cloud-common")]
impl OciProvider {
    /// Create OCI provider using S3-compatible API (recommended)
    ///
    /// OCI Object Storage offers an S3-compatible API endpoint that works
    /// seamlessly with standard S3 clients and libraries.
    ///
    /// # Arguments
    /// * `bucket` - Object Storage bucket name
    /// * `namespace` - OCI namespace (found in bucket details)
    /// * `region` - OCI region (e.g., "us-phoenix-1")
    /// * `access_key` - OCI user's API signing key ID
    /// * `secret_key` - OCI user's API signing key
    pub fn s3_compat(
        bucket: String,
        namespace: String,
        region: String,
        access_key: String,
        secret_key: String,
    ) -> Self {
        let config = S3Config::oci_s3_compat(bucket, namespace, region);
        let inner = S3Provider::custom(config, access_key, secret_key);
        Self { inner }
    }

    /// Create OCI provider (using S3-compatible API)
    ///
    /// Convenience constructor equivalent to `s3_compat()`.
    pub fn new(namespace: String, bucket: String, region: String) -> Self {
        // Note: This is a stub constructor that creates a provider with the namespace/bucket/region
        // but no credentials. Real usage should call `s3_compat()` with full credentials.
        let config = S3Config::oci_s3_compat(bucket, namespace, region.clone());
        let inner = S3Provider::custom(config, String::new(), String::new());
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
    fn should_create_oci_s3_compat_provider() {
        let provider = OciProvider::s3_compat(
            "my-bucket".into(),
            "mynamespace".into(),
            "us-phoenix-1".into(),
            "oci-access-key".into(),
            "oci-secret-key".into(),
        );
        let _ = provider.inner();
    }

    #[test]
    #[cfg(feature = "cloud-common")]
    fn should_create_oci_provider_with_new() {
        let provider = OciProvider::new(
            "mynamespace".to_string(),
            "mybucket".to_string(),
            "us-phoenix-1".to_string(),
        );
        let _ = provider.inner();
    }

    #[test]
    #[cfg(not(feature = "cloud-common"))]
    fn stub_oci_provider_compiles() {
        let _provider = OciProvider::new(
            "namespace".into(),
            "bucket".into(),
            "region".into(),
        );
    }
}

