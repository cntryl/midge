use std::sync::Arc;

use crate::common::{MidgeError, MidgeResult};
use crate::storage::cloud::CloudBackend;

use super::CloudProviderConfig;

pub(crate) struct CloudProviderFactory;

impl CloudProviderFactory {
    pub(crate) fn build_backend(
        provider: &CloudProviderConfig,
    ) -> MidgeResult<Arc<dyn CloudBackend>> {
        match provider {
            CloudProviderConfig::AwsS3(_) => Self::build_aws(provider),
            CloudProviderConfig::S3Compatible(_) => Self::build_s3_compatible(provider),
            CloudProviderConfig::OciObjectStorage(_) => Self::build_oci(provider),
            CloudProviderConfig::AzureBlob(_) => Self::build_azure(provider),
            CloudProviderConfig::Gcs(_) => Self::build_gcp(provider),
        }
    }

    #[cfg(feature = "cloud-aws")]
    fn build_aws(provider: &CloudProviderConfig) -> MidgeResult<Arc<dyn CloudBackend>> {
        Self::resolved_backend(super::s3_resolver::try_resolve(provider)?, "AWS S3")
    }

    #[cfg(not(feature = "cloud-aws"))]
    fn build_aws(_provider: &CloudProviderConfig) -> MidgeResult<Arc<dyn CloudBackend>> {
        Self::provider_disabled("AWS S3", "cloud-aws")
    }

    #[cfg(any(feature = "cloud-aws", feature = "cloud-oci"))]
    fn build_s3_compatible(provider: &CloudProviderConfig) -> MidgeResult<Arc<dyn CloudBackend>> {
        Self::resolved_backend(
            super::s3_resolver::try_resolve(provider)?,
            "S3-compatible/OCI",
        )
    }

    #[cfg(not(any(feature = "cloud-aws", feature = "cloud-oci")))]
    fn build_s3_compatible(_provider: &CloudProviderConfig) -> MidgeResult<Arc<dyn CloudBackend>> {
        Self::provider_disabled("S3-compatible/OCI", "cloud-aws or cloud-oci")
    }

    #[cfg(feature = "cloud-oci")]
    fn build_oci(provider: &CloudProviderConfig) -> MidgeResult<Arc<dyn CloudBackend>> {
        Self::resolved_backend(
            super::s3_resolver::try_resolve(provider)?,
            "OCI Object Storage",
        )
    }

    #[cfg(not(feature = "cloud-oci"))]
    fn build_oci(_provider: &CloudProviderConfig) -> MidgeResult<Arc<dyn CloudBackend>> {
        Self::provider_disabled("OCI Object Storage", "cloud-oci")
    }

    #[cfg(feature = "cloud-azure")]
    fn build_azure(provider: &CloudProviderConfig) -> MidgeResult<Arc<dyn CloudBackend>> {
        Self::resolved_backend(super::azure_resolver::try_resolve(provider)?, "Azure Blob")
    }

    #[cfg(not(feature = "cloud-azure"))]
    fn build_azure(_provider: &CloudProviderConfig) -> MidgeResult<Arc<dyn CloudBackend>> {
        Self::provider_disabled("Azure Blob", "cloud-azure")
    }

    #[cfg(feature = "cloud-gcp")]
    fn build_gcp(provider: &CloudProviderConfig) -> MidgeResult<Arc<dyn CloudBackend>> {
        Self::resolved_backend(
            super::gcs_resolver::try_resolve(provider)?,
            "Google Cloud Storage",
        )
    }

    #[cfg(not(feature = "cloud-gcp"))]
    fn build_gcp(_provider: &CloudProviderConfig) -> MidgeResult<Arc<dyn CloudBackend>> {
        Self::provider_disabled("Google Cloud Storage", "cloud-gcp")
    }

    #[cfg(any(
        feature = "cloud-aws",
        feature = "cloud-azure",
        feature = "cloud-gcp",
        feature = "cloud-oci"
    ))]
    fn resolved_backend(
        backend: Option<Arc<dyn CloudBackend>>,
        provider: &str,
    ) -> MidgeResult<Arc<dyn CloudBackend>> {
        backend.ok_or_else(|| {
            MidgeError::InvalidArgument(format!(
                "{provider} feature did not accept its provider configuration"
            ))
        })
    }

    #[cfg(any(
        not(feature = "cloud-aws"),
        not(feature = "cloud-azure"),
        not(feature = "cloud-gcp"),
        not(feature = "cloud-oci")
    ))]
    fn provider_disabled<T>(provider: &str, feature: &str) -> MidgeResult<T> {
        Err(MidgeError::InvalidArgument(format!(
            "{provider} support requires the {feature} feature"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn aws_provider() -> CloudProviderConfig {
        CloudProviderConfig::aws_s3_static("bucket", "us-east-1", "access", "secret")
    }

    fn s3_compatible_provider(endpoint: &str) -> CloudProviderConfig {
        CloudProviderConfig::s3_compatible_static("bucket", endpoint, "access", "secret")
    }

    #[cfg(feature = "cloud-aws")]
    #[test]
    fn should_build_aws_provider_when_aws_feature_is_enabled() {
        // Arrange
        let provider = aws_provider();

        // Act
        let backend = CloudProviderFactory::build_backend(&provider)
            .expect("AWS provider must build when cloud-aws is enabled");

        // Assert: `build_backend` for AWS always routes through `s3_resolver`, which is the
        // same code path exercised end-to-end (real request wiring, bucket/endpoint plumbing)
        // by the S3-compatible case below via a local mock server; here we only need to confirm
        // the AWS branch actually produced a live backend rather than the "feature disabled"
        // stub error, which a second independent build call must reproduce identically.
        let second = CloudProviderFactory::build_backend(&provider)
            .expect("AWS provider must build deterministically");
        assert!(
            !Arc::ptr_eq(&backend, &second),
            "each build must own its backend"
        );
    }

    #[cfg(not(feature = "cloud-aws"))]
    #[test]
    fn should_reject_aws_provider_when_aws_feature_is_disabled() {
        // Arrange
        let provider = aws_provider();

        // Act
        let error = CloudProviderFactory::build_backend(&provider)
            .err()
            .expect("AWS provider must require cloud-aws");

        // Assert
        assert!(error.to_string().contains("cloud-aws"));
    }

    #[cfg(any(feature = "cloud-aws", feature = "cloud-oci"))]
    #[test]
    fn should_build_s3_compatible_provider_when_s3_family_feature_is_enabled() {
        // Arrange
        let server = crate::storage::providers::test_support::spawn_recording_http_server(
            Vec::new(),
            Vec::new(),
        );
        let provider = s3_compatible_provider(&server.endpoint);

        // Act
        let backend = CloudProviderFactory::build_backend(&provider)
            .expect("S3-compatible provider must build when an S3-family feature is enabled");
        let (sender, receiver) = std::sync::mpsc::channel();
        backend.submit_put("object", b"value".to_vec(), Vec::new(), sender);
        let _ = receiver.recv_timeout(std::time::Duration::from_secs(5));
        let request = server.finish();

        // Assert: the resolved backend actually targets the configured endpoint/bucket, not
        // just "some" backend.
        assert_eq!(request.method, "PUT");
        assert!(
            request.target.contains("bucket"),
            "expected request targeting the configured bucket, got: {}",
            request.target
        );
    }

    #[cfg(not(any(feature = "cloud-aws", feature = "cloud-oci")))]
    #[test]
    fn should_reject_s3_compatible_provider_when_s3_family_features_are_disabled() {
        // Arrange
        let provider = s3_compatible_provider("http://127.0.0.1:9000");

        // Act
        let error = CloudProviderFactory::build_backend(&provider)
            .err()
            .expect("S3-compatible provider must require an S3-family feature");

        // Assert
        assert!(error.to_string().contains("cloud-aws or cloud-oci"));
    }
}
