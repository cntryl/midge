//! Custom cloud provider implementations
//!
//! Lean, custom implementations for each cloud vendor without heavy SDKs.
//! Each provider implements the callback-based architecture via CloudCallback.
//!
//! Architecture:
//! - **s3.rs**: Generic S3-compatible implementation (base layer)
//! - **aws.rs**: AWS S3 with SigV4 authentication (extends s3.rs)
//! - **wasabi.rs**: Wasabi Cloud Storage (extends s3.rs)
//! - **minio.rs**: MinIO S3-compatible storage (extends s3.rs)
//! - **oci.rs**: Oracle Cloud Infrastructure S3-compatible API (extends s3.rs)
//! - **gcs.rs**: Google Cloud Storage (standalone REST API)
//! - **azure.rs**: Azure Blob Storage (standalone REST API)

pub mod azure;
pub mod aws;
pub mod gcs;
pub mod minio;
pub mod oci;
pub mod s3;
pub mod wasabi;

// Re-export for convenience
pub use azure::AzureProvider;
pub use aws::AwsS3Provider;
pub use gcs::GcsProvider;
pub use minio::MinioProvider;
pub use oci::OciProvider;
pub use s3::S3Provider;
pub use wasabi::WasabiProvider;

#[cfg(feature = "cloud-common")]
pub use s3::S3Config;
