use std::sync::Arc;

use super::factory::CloudProviderResolver;
use crate::common::{MidgeError, MidgeResult};
use crate::config::{CloudProviderConfig, GcsApiStyle, GcsCredentialSource};
use crate::storage::cloud::CloudBackend;

pub(super) struct GcsProviderResolver;

impl CloudProviderResolver for GcsProviderResolver {
    fn resolve(&self, provider: &CloudProviderConfig) -> MidgeResult<Arc<dyn CloudBackend>> {
        let CloudProviderConfig::Gcs {
            bucket,
            project_id,
            endpoint,
            api,
            credential,
        } = provider
        else {
            return Err(MidgeError::InvalidArgument(
                "expected a GCS provider".to_string(),
            ));
        };

        let provider = match credential {
            GcsCredentialSource::BearerToken { token } => {
                super::gcs::GcsProvider::with_bearer_token_endpoint(
                    bucket.clone(),
                    project_id.clone(),
                    token.clone(),
                    endpoint.clone(),
                )?
            }
            GcsCredentialSource::HmacKey { access_id, secret } => match api {
                GcsApiStyle::Xml => super::gcs::GcsProvider::with_hmac_key_xml_endpoint(
                    bucket.clone(),
                    project_id.clone(),
                    access_id.clone(),
                    secret.clone(),
                    endpoint.clone(),
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
                if *api == GcsApiStyle::Xml {
                    return Err(MidgeError::InvalidArgument(
                        "GCS ADC/bearer credentials require JSON API mode".to_string(),
                    ));
                }
                super::gcs::GcsProvider::with_bearer_credential_endpoint(
                    bucket.clone(),
                    project_id.clone(),
                    credential,
                    endpoint.clone(),
                )?
            }
        };

        Ok(provider.backend())
    }
}
