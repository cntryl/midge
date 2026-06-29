use std::sync::Arc;

use crate::common::{MidgeError, MidgeResult};
use crate::storage::cloud::CloudBackend;

use super::CloudProviderConfig;

pub(crate) struct CloudProviderFactory;

impl CloudProviderFactory {
    pub(crate) fn build_backend(
        provider: &CloudProviderConfig,
    ) -> MidgeResult<Arc<dyn CloudBackend>> {
        if let Some(backend) = super::s3_resolver::try_resolve(provider)? {
            return Ok(backend);
        }
        if let Some(backend) = super::azure_resolver::try_resolve(provider)? {
            return Ok(backend);
        }
        if let Some(backend) = super::gcs_resolver::try_resolve(provider)? {
            return Ok(backend);
        }

        Err(MidgeError::InvalidArgument(
            "unsupported cloud provider configuration".to_string(),
        ))
    }
}
