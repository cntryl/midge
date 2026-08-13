use std::sync::Arc;

use super::{CloudProviderConfig, GcsApiStyle, GcsCredentialSource};
use crate::common::{MidgeError, MidgeResult};
use crate::storage::cloud::CloudBackend;

pub(super) fn try_resolve(
    provider: &CloudProviderConfig,
) -> MidgeResult<Option<Arc<dyn CloudBackend>>> {
    let CloudProviderConfig::Gcs(_) = provider else {
        return Ok(None);
    };

    Ok(Some(resolve_gcs(provider)?))
}

fn resolve_gcs(provider: &CloudProviderConfig) -> MidgeResult<Arc<dyn CloudBackend>> {
    let CloudProviderConfig::Gcs(config) = provider else {
        return Err(MidgeError::InvalidArgument(
            "expected a GCS provider".to_string(),
        ));
    };

    let provider = match &config.credential {
        GcsCredentialSource::BearerToken { token } => {
            super::gcs::GcsProvider::with_bearer_token_endpoint(
                config.bucket.clone(),
                config.project_id.clone(),
                token.clone(),
                config.endpoint.clone(),
            )?
        }
        GcsCredentialSource::HmacKey { access_id, secret } => match config.api_style() {
            GcsApiStyle::Xml => super::gcs::GcsProvider::with_hmac_key_xml_endpoint(
                config.bucket.clone(),
                config.project_id.clone(),
                access_id.clone(),
                secret.clone(),
                config.endpoint.clone(),
            )?,
            GcsApiStyle::Json => {
                return Err(MidgeError::InvalidArgument(
                    "GCS HMAC credentials currently require XML API mode".to_string(),
                ))
            }
        },
        GcsCredentialSource::ApplicationDefault
        | GcsCredentialSource::ServiceAccountJsonFile { .. }
        | GcsCredentialSource::AuthorizedUserJsonFile { .. }
        | GcsCredentialSource::MetadataServer => {
            if config.api_style() == GcsApiStyle::Xml {
                return Err(MidgeError::InvalidArgument(
                    "GCS ADC/bearer credentials require JSON API mode".to_string(),
                ));
            }
            super::gcs::GcsProvider::with_bearer_credential_endpoint(
                config.bucket.clone(),
                config.project_id.clone(),
                &config.credential,
                config.endpoint.clone(),
            )?
        }
    };

    Ok(provider.backend())
}
