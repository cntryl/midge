use std::sync::Arc;

use crate::common::MidgeResult;
use crate::config::CloudProviderConfig;
use crate::storage::cloud::CloudBackend;

pub(super) trait CloudProviderResolver {
    fn resolve(&self, provider: &CloudProviderConfig) -> MidgeResult<Arc<dyn CloudBackend>>;
}

pub(crate) struct CloudProviderFactory;

impl CloudProviderFactory {
    pub(crate) fn build_backend(
        provider: &CloudProviderConfig,
    ) -> MidgeResult<Arc<dyn CloudBackend>> {
        match provider {
            CloudProviderConfig::AwsS3 { .. }
            | CloudProviderConfig::S3Compatible { .. }
            | CloudProviderConfig::Minio { .. }
            | CloudProviderConfig::Wasabi { .. }
            | CloudProviderConfig::OciS3Compatible { .. } => {
                super::s3_resolver::S3ProviderResolver.resolve(provider)
            }
            CloudProviderConfig::AzureBlob { .. } => {
                super::azure_resolver::AzureProviderResolver.resolve(provider)
            }
            CloudProviderConfig::Gcs { .. } => {
                super::gcs_resolver::GcsProviderResolver.resolve(provider)
            }
        }
    }
}
