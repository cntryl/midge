use std::sync::Arc;

use super::factory::CloudProviderResolver;
use crate::common::{MidgeError, MidgeResult};
use crate::config::{CloudProviderConfig, S3CredentialSource};
use crate::storage::cloud::CloudBackend;

pub(super) struct S3ProviderResolver;

impl CloudProviderResolver for S3ProviderResolver {
    fn resolve(&self, provider: &CloudProviderConfig) -> MidgeResult<Arc<dyn CloudBackend>> {
        match provider {
            CloudProviderConfig::AwsS3 {
                bucket,
                region,
                credentials,
            } => match credentials {
                S3CredentialSource::AwsDefaultChain => {
                    let provider =
                        super::s3::S3Provider::aws_default(bucket.clone(), region.clone())?;
                    Ok(provider.backend())
                }
                S3CredentialSource::Static { .. }
                | S3CredentialSource::Environment
                | S3CredentialSource::SharedProfile { .. } => {
                    let creds = resolve_s3_credentials(credentials, region, true)?;
                    let provider =
                        super::s3::S3Provider::aws(bucket.clone(), region.clone(), creds)?;
                    Ok(provider.backend())
                }
            },
            CloudProviderConfig::S3Compatible {
                bucket,
                region,
                endpoint,
                path_style,
                credentials,
            } => {
                let creds = resolve_s3_credentials(credentials, region, false)?;
                let config = super::s3::S3Config::custom(
                    bucket.clone(),
                    region.clone(),
                    endpoint.clone(),
                    *path_style,
                );
                let provider = super::s3::S3Provider::custom_with_credentials(config, creds)?;
                Ok(provider.backend())
            }
            CloudProviderConfig::Minio {
                bucket,
                endpoint,
                credentials,
            } => {
                let creds = resolve_s3_credentials(credentials, "us-east-1", false)?;
                let config = super::s3::S3Config::minio(bucket.clone(), endpoint.clone());
                let provider = super::s3::S3Provider::custom_with_credentials(config, creds)?;
                Ok(provider.backend())
            }
            CloudProviderConfig::Wasabi {
                bucket,
                region,
                endpoint,
                credentials,
            } => {
                let creds = resolve_s3_credentials(credentials, region, false)?;
                let config = match endpoint {
                    Some(endpoint) => super::s3::S3Config::custom(
                        bucket.clone(),
                        region.clone(),
                        endpoint.clone(),
                        true,
                    ),
                    None => super::s3::S3Config::wasabi(bucket.clone(), region.clone()),
                };
                let provider = super::s3::S3Provider::custom_with_credentials(config, creds)?;
                Ok(provider.backend())
            }
            CloudProviderConfig::OciS3Compatible {
                bucket,
                namespace,
                region,
                endpoint,
                path_style,
                credentials,
            } => {
                let creds = resolve_s3_credentials(credentials, region, false)?;
                let config = match endpoint {
                    Some(endpoint) => super::s3::S3Config::custom(
                        bucket.clone(),
                        region.clone(),
                        endpoint.clone(),
                        *path_style,
                    ),
                    None => super::s3::S3Config::oci_s3_compat(
                        bucket.clone(),
                        namespace.clone(),
                        region.clone(),
                    ),
                };
                let provider = super::s3::S3Provider::custom_with_credentials(config, creds)?;
                Ok(provider.backend())
            }
            CloudProviderConfig::AzureBlob { .. } | CloudProviderConfig::Gcs { .. } => Err(
                MidgeError::InvalidArgument("expected an S3-family provider".to_string()),
            ),
        }
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
