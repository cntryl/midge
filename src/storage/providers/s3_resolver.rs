use std::sync::Arc;

use super::{CloudProviderConfig, S3CredentialSource};
use crate::common::{MidgeError, MidgeResult};
use crate::storage::cloud::CloudBackend;

pub(super) fn try_resolve(
    provider: &CloudProviderConfig,
) -> MidgeResult<Option<Arc<dyn CloudBackend>>> {
    match provider {
        CloudProviderConfig::AwsS3(_)
        | CloudProviderConfig::S3Compatible(_)
        | CloudProviderConfig::OciObjectStorage(_) => Ok(Some(resolve_s3_family(provider)?)),
        CloudProviderConfig::AzureBlob(_) | CloudProviderConfig::Gcs(_) => Ok(None),
    }
}

fn resolve_s3_family(provider: &CloudProviderConfig) -> MidgeResult<Arc<dyn CloudBackend>> {
    match provider {
        CloudProviderConfig::AwsS3(config) => match &config.credentials {
            S3CredentialSource::AwsDefaultChain => {
                let provider = super::s3::S3Provider::aws_default(
                    config.bucket.clone(),
                    config.region.clone(),
                )?;
                Ok(provider.backend())
            }
            S3CredentialSource::Static { .. }
            | S3CredentialSource::Environment
            | S3CredentialSource::SharedProfile { .. } => {
                let creds = resolve_s3_credentials(&config.credentials, &config.region, true)?;
                let provider = super::s3::S3Provider::aws(
                    config.bucket.clone(),
                    config.region.clone(),
                    creds,
                )?;
                Ok(provider.backend())
            }
        },
        CloudProviderConfig::S3Compatible(config) => {
            let creds = resolve_s3_credentials(&config.credentials, &config.region, false)?;
            let config = super::s3::S3Config::custom(
                config.bucket.clone(),
                config.region.clone(),
                config.endpoint.clone(),
                config.path_style,
            );
            let provider = super::s3::S3Provider::custom_with_credentials(config, creds)?;
            Ok(provider.backend())
        }
        CloudProviderConfig::OciObjectStorage(config) => {
            let credentials = resolve_s3_credentials(&config.credentials, &config.region, false)?;
            let s3 = super::s3::S3Config::custom(
                config.bucket.clone(),
                config.region.clone(),
                config.endpoint(),
                true,
            );
            Ok(super::s3::S3Provider::custom_with_credentials(s3, credentials)?.backend())
        }
        CloudProviderConfig::AzureBlob(_) | CloudProviderConfig::Gcs(_) => Err(
            MidgeError::InvalidArgument("expected an S3-family provider".to_string()),
        ),
    }
}

fn resolve_s3_credentials(
    source: &S3CredentialSource,
    region: &str,
    allow_aws_default_chain: bool,
) -> MidgeResult<super::s3::AwsCredentials> {
    match source {
        S3CredentialSource::Static {
            access_key,
            secret_key,
            session_token,
        } => Ok(super::s3::AwsCredentials {
            access_key: access_key.clone(),
            secret_key: secret_key.clone(),
            region: region.to_string(),
            session_token: session_token.clone(),
        }),
        S3CredentialSource::Environment => {
            super::s3::resolve_static_env_credentials(region).ok_or_else(|| {
                MidgeError::InvalidArgument("missing AWS-style environment credentials".to_string())
            })
        }
        S3CredentialSource::SharedProfile {
            profile,
            credentials_file,
            config_file,
        } => super::s3::resolve_static_profile_credentials(
            region,
            profile.as_deref(),
            credentials_file.as_deref(),
            config_file.as_deref(),
        )?
        .ok_or_else(|| {
            MidgeError::InvalidArgument("missing AWS-style shared profile credentials".to_string())
        }),
        S3CredentialSource::AwsDefaultChain if allow_aws_default_chain => Err(
            MidgeError::InvalidArgument(
                "AWS default chain should be built with S3Provider::aws_default".to_string(),
            ),
        ),
        S3CredentialSource::AwsDefaultChain => Err(MidgeError::InvalidArgument(
            "AwsDefaultChain is only valid for AWS S3; use Static, Environment, or SharedProfile for S3-compatible providers"
                .to_string(),
        )),
    }
}
