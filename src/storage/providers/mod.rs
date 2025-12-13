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
//!
//! ```ignore
//! use midge::storage::providers::S3Provider;
//! use midge::storage::cloud::CloudCallback;
//!
//! let provider = S3Provider::new(
//!     "s3.amazonaws.com".into(),
//!     "my-bucket".into(),
//!     None, // no signer for unsigned requests
//! );
//!
//! let (tx, rx) = std::sync::mpsc::channel();
//! provider.submit_put("key".into(), vec![1, 2, 3], tx);
//! let event = rx.recv().unwrap();
//! ```

pub mod aws;
pub mod azure;
pub mod gcs;
pub mod minio;
pub mod oci;
pub mod s3;
pub mod wasabi;

// Re-export for convenience
pub use aws::AwsS3Provider;
pub use azure::AzureProvider;
pub use gcs::GcsProvider;
pub use minio::MinioProvider;
pub use oci::OciProvider;
pub use s3::S3Provider;
pub use wasabi::WasabiProvider;

#[cfg(feature = "cloud-common")]
pub use s3::S3Config;
