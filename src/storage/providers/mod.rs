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
//! - `SigV4` signing (optional, can be extended)
//! - Works with any S3-compatible service
//!
//! **AWS**: configured through `CloudProviderConfig::AwsS3`
//! - Uses AWS region, access key, secret key, or the AWS default chain
//! - Proper AWS `SigV4` request signing
//!
//! **Other S3-compatible providers**: configured through `CloudProviderConfig::S3Compatible`
//! - Covers `MinIO`, `Wasabi`, `OCI` S3 front doors, and generic S3-compatible services
//! - Caller supplies endpoint, region, path-style preference, and credentials
//!
//! ### Direct REST APIs
//!
//! **Google Cloud Storage**: [gcs.rs]
//! - Direct REST API (no SDK)
//! - `OAuth2` authentication (placeholder)
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
pub mod azure;
#[cfg(feature = "cloud-common")]
mod azure_resolver;
pub(crate) mod config;
#[cfg(feature = "cloud-common")]
mod factory;
#[cfg(feature = "cloud-common")]
pub mod gcs;
#[cfg(feature = "cloud-common")]
mod gcs_resolver;
#[cfg(all(test, feature = "cloud-common", feature = "peas-tests"))]
pub mod qualification;
#[cfg(feature = "cloud-common")]
pub mod s3;
#[cfg(feature = "cloud-common")]
mod s3_resolver;

use std::sync::Arc;

#[cfg(not(feature = "cloud-common"))]
use crate::common::MidgeError;
use crate::common::MidgeResult;
#[cfg(feature = "cloud-common")]
use crate::storage::cloud::CloudBackend;
use crate::storage::cloud::CloudStorage;

pub use config::{
    AzureCredentialSource, CloudCredentialSource, CloudProviderConfig, GcsApiStyle,
    GcsCredentialSource, S3CredentialSource,
};

#[cfg(feature = "cloud-common")]
pub(crate) fn build_cloud_backend(
    provider: &CloudProviderConfig,
) -> MidgeResult<Arc<dyn CloudBackend>> {
    factory::CloudProviderFactory::build_backend(provider)
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
