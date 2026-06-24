//! Cloud provider implementations
//!
//! Custom, lean implementations for each cloud vendor without heavy SDKs.
//! Each provider is callback-based, non-blocking, and asynchronous.
//!
//! ## Provider Architecture
//!
//! Implementations are organized by capability:
//!
//! ### S3-Compatible Layer
//!
//! **Base**: [s3.rs] - Generic S3-compatible REST implementation
//! - Object PUT/GET/DELETE/LIST/HEAD
//! - SigV4 signing (optional, can be extended)
//! - Works with any S3-compatible service
//!
//! **AWS**: [aws.rs] - AWS S3 with full SigV4 signing
//! - Uses AWS region, access key, secret key
//! - Proper AWS SigV4 request signing
//! - Extends [S3Provider]
//!
//! **Wasabi**: [wasabi.rs] - Wasabi Cloud Storage
//! - S3-compatible API
//! - Access key + secret key auth
//! - Extends [S3Provider]
//!
//! **MinIO**: [minio.rs] - MinIO S3-compatible storage
//! - On-premise or cloud-hosted MinIO
//! - Access key + secret key auth
//! - Extends [S3Provider]
//!
//! **OCI**: [oci.rs] - Oracle Cloud Infrastructure S3-compatible API
//! - OCI Namespace + bucket structure
//! - Custom signing (placeholder)
//! - Extends [S3Provider]
//!
//! ### Direct REST APIs
//!
//! **Google Cloud Storage**: [gcs.rs]
//! - Direct REST API (no SDK)
//! - OAuth2 authentication (placeholder)
//! - Standalone implementation
//!
//! **Azure Blob Storage**: [azure.rs]
//! - Direct REST API (no SDK)
//! - SAS token or shared key auth (placeholder)
//! - Standalone implementation
//!
//! ## Async Model
//!
//! All providers are non-blocking callback-based:
//! - `submit_put()`, `submit_get()`, etc. return immediately
//! - Results sent via `CloudCallback` channels
//! - Actual HTTP execution happens in `CloudExecutor`'s embedded tokio runtime
//!
//! ## Example Usage

#[cfg(feature = "cloud-common")]
pub mod aws;
#[cfg(feature = "cloud-common")]
pub mod azure;
#[cfg(feature = "cloud-common")]
pub mod gcs;
#[cfg(feature = "cloud-common")]
pub mod minio;
#[cfg(feature = "cloud-common")]
pub mod oci;
#[cfg(all(test, feature = "cloud-common", feature = "peas-tests"))]
pub mod qualification;
#[cfg(feature = "cloud-common")]
pub mod s3;
#[cfg(feature = "cloud-common")]
pub mod wasabi;

use std::sync::Arc;

use crate::common::{MidgeError, MidgeResult};
#[cfg(not(feature = "cloud-common"))]
use crate::engine::api::CloudProviderConfig;
#[cfg(feature = "cloud-common")]
use crate::engine::api::{
    AzureCredentialSource, CloudProviderConfig, GcsApiStyle, GcsCredentialSource,
    S3CredentialSource,
};
#[cfg(feature = "cloud-common")]
use crate::storage::cloud::CloudBackend;
use crate::storage::cloud::CloudStorage;

#[cfg(feature = "cloud-common")]
pub(crate) fn build_cloud_backend(
    provider: &CloudProviderConfig,
) -> MidgeResult<Arc<dyn CloudBackend>> {
    match provider {
        CloudProviderConfig::AwsS3 {
            bucket,
            region,
            credentials,
        } => match credentials {
            S3CredentialSource::AwsDefaultChain => {
                let provider = s3::S3Provider::aws_default(bucket.clone(), region.clone())?;
                Ok(provider.backend())
            }
            S3CredentialSource::Static { .. }
            | S3CredentialSource::Environment
            | S3CredentialSource::SharedProfile { .. } => {
                let creds = resolve_s3_credentials(credentials, region, true)?;
                let provider = s3::S3Provider::aws(bucket.clone(), region.clone(), creds)?;
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
            let config = s3::S3Config::custom(
                bucket.clone(),
                region.clone(),
                endpoint.clone(),
                *path_style,
            );
            let provider = s3::S3Provider::custom_with_credentials(config, creds)?;
            Ok(provider.backend())
        }
        CloudProviderConfig::Minio {
            bucket,
            endpoint,
            credentials,
        } => {
            let creds = resolve_s3_credentials(credentials, "us-east-1", false)?;
            let config = s3::S3Config::minio(bucket.clone(), endpoint.clone());
            let provider = s3::S3Provider::custom_with_credentials(config, creds)?;
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
                Some(endpoint) => {
                    s3::S3Config::custom(bucket.clone(), region.clone(), endpoint.clone(), true)
                }
                None => s3::S3Config::wasabi(bucket.clone(), region.clone()),
            };
            let provider = s3::S3Provider::custom_with_credentials(config, creds)?;
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
                Some(endpoint) => s3::S3Config::custom(
                    bucket.clone(),
                    region.clone(),
                    endpoint.clone(),
                    *path_style,
                ),
                None => {
                    s3::S3Config::oci_s3_compat(bucket.clone(), namespace.clone(), region.clone())
                }
            };
            let provider = s3::S3Provider::custom_with_credentials(config, creds)?;
            Ok(provider.backend())
        }
        CloudProviderConfig::AzureBlob {
            account,
            container,
            endpoint,
            credential,
        } => {
            let provider = match credential {
                AzureCredentialSource::SharedKey { account_key } => {
                    azure::AzureProvider::with_shared_key_and_endpoint(
                        account.clone(),
                        container.clone(),
                        account_key.clone(),
                        endpoint.clone(),
                    )?
                }
                AzureCredentialSource::SasToken { token } => {
                    azure::AzureProvider::with_sas_token_and_endpoint(
                        account.clone(),
                        container.clone(),
                        token.clone(),
                        endpoint.clone(),
                    )?
                }
                AzureCredentialSource::ConnectionString { connection_string } => {
                    azure::AzureProvider::from_connection_string_and_endpoint(
                        connection_string.clone(),
                        container.clone(),
                        endpoint.clone(),
                    )?
                }
                AzureCredentialSource::StorageEnvironment => {
                    azure::AzureProvider::from_env_and_endpoint(
                        account.clone(),
                        container.clone(),
                        endpoint.clone(),
                    )?
                }
                AzureCredentialSource::ManagedIdentity { client_id } => {
                    if endpoint.is_some() {
                        return Err(MidgeError::InvalidArgument(
                            "managed identity cannot be used with an emulator endpoint".to_string(),
                        ));
                    }
                    azure::AzureProvider::with_managed_identity(
                        account.clone(),
                        container.clone(),
                        client_id.clone(),
                    )?
                }
                AzureCredentialSource::EnvironmentClientSecret
                | AzureCredentialSource::WorkloadIdentity { .. }
                | AzureCredentialSource::LightweightDefaultChain => {
                    if endpoint.is_some() {
                        return Err(MidgeError::InvalidArgument(
                            "Azure OAuth credential sources cannot be used with an emulator endpoint"
                                .to_string(),
                        ));
                    }
                    azure::AzureProvider::from_lightweight_credential_source(
                        account.clone(),
                        container.clone(),
                        credential.clone(),
                    )?
                }
            };
            Ok(provider.backend())
        }
        CloudProviderConfig::Gcs {
            bucket,
            project_id,
            endpoint,
            api,
            credential,
        } => {
            let provider = match credential {
                GcsCredentialSource::BearerToken { token } => {
                    gcs::GcsProvider::with_bearer_token_endpoint(
                        bucket.clone(),
                        project_id.clone(),
                        token.clone(),
                        endpoint.clone(),
                    )?
                }
                GcsCredentialSource::HmacKey { access_id, secret } => match api {
                    GcsApiStyle::Xml => gcs::GcsProvider::with_hmac_key_xml_endpoint(
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
                    gcs::GcsProvider::with_bearer_credential_endpoint(
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
}

#[cfg(feature = "cloud-common")]
fn resolve_s3_credentials(
    source: &S3CredentialSource,
    region: &str,
    allow_aws_default_chain: bool,
) -> MidgeResult<s3::AwsCredentials> {
    match source {
        S3CredentialSource::Static {
            access_key,
            secret_key,
            session_token,
        } => Ok(s3::AwsCredentials {
            access_key: access_key.clone(),
            secret_key: secret_key.clone(),
            region: region.to_string(),
            session_token: session_token.clone(),
        }),
        S3CredentialSource::Environment => {
            s3::resolve_static_env_credentials(region).ok_or_else(|| {
                MidgeError::InvalidArgument(
                    "missing AWS-style environment credentials".to_string(),
                )
            })
        }
        S3CredentialSource::SharedProfile {
            profile,
            credentials_file,
            config_file,
        } => s3::resolve_static_profile_credentials(
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

#[cfg(feature = "cloud-common")]
pub(crate) fn build_cloud_storage(
    provider: &CloudProviderConfig,
    prefix: &str,
) -> MidgeResult<Arc<CloudStorage>> {
    let backend = build_cloud_backend(provider)?;
    Ok(Arc::new(CloudStorage::new(
        backend,
        prefix.trim_matches('/').to_string(),
    )))
}

#[cfg(not(feature = "cloud-common"))]
pub(crate) fn build_cloud_storage(
    _provider: &CloudProviderConfig,
    _prefix: &str,
) -> MidgeResult<Arc<CloudStorage>> {
    Err(MidgeError::InvalidArgument(
        "real cloud storage requires the cloud-common feature".to_string(),
    ))
}
