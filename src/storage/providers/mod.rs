//! Custom cloud provider implementations
//!
//! Lean, custom implementations for each cloud vendor without heavy SDKs.
//! Each provider implements the callback-based architecture via CloudCallback.
//!
//! Providers:
//! - AWS S3: Direct REST API with SigV4 authentication
//! - Google Cloud Storage: Direct REST API with OAuth2
//! - Azure Blob Storage: Direct REST API with SAS tokens
//! - Oracle Cloud Infrastructure: Direct REST API with signature-based auth

pub mod s3;
pub mod gcs;
pub mod azure;
pub mod oci;

// Re-export for convenience
pub use s3::S3Provider;
pub use gcs::GcsProvider;
pub use azure::AzureProvider;
pub use oci::OciProvider;
