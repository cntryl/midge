//! Custom cloud provider implementations
//!
//! Lean, custom implementations for each cloud vendor without heavy SDKs.
//! Each provider implements the callback-based architecture via CloudCallback.
//!
//! Providers:
//! - Generic S3-compatible (AWS, Wasabi, MinIO, OCI, etc.)
//! - Google Cloud Storage: Direct REST API with OAuth2
//! - Azure Blob Storage: Direct REST API with SAS tokens
//! - Oracle Cloud Infrastructure: Direct REST API with signature-based auth

pub mod azure;
pub mod gcs;
pub mod oci;
pub mod s3;

// Re-export for convenience
pub use azure::AzureProvider;
pub use gcs::GcsProvider;
pub use oci::OciProvider;
pub use s3::S3Provider;

#[cfg(feature = "cloud-common")]
pub use s3::S3Config;

// Vendor-specific type aliases for clarity
#[cfg(feature = "cloud-common")]
pub type AwsS3Provider = S3Provider;
#[cfg(feature = "cloud-common")]
pub type WasabiProvider = S3Provider;
#[cfg(feature = "cloud-common")]
pub type MinioProvider = S3Provider;
#[cfg(feature = "cloud-common")]
pub type OciS3CompatProvider = S3Provider;
