//! Cloud storage backend abstraction layer.
//!
//! This module provides a generic blob storage interface (`StorageBackend`) and
//! implementations for various cloud providers. All backends operate at the blob
//! level with no knowledge of higher-level concepts like WAL segments or SST files.
//!
//! # Architecture
//!
//! - `backend` - Core `StorageBackend` trait and `BlobMeta` type
//! - `mock` - Filesystem-based mock implementation for testing
//! - `aws`, `azure`, `gcp`, `oci` - Cloud provider implementations (feature-gated)
//!
//! # Examples
//!
//! ```no_run
//! use midge::cloud::{StorageBackend, MockCloudBackend};
//! use bytes::Bytes;
//!
//! let backend = MockCloudBackend::new();
//! backend.put_blob("my-key", Bytes::from("data")).unwrap();
//! let data = backend.get_blob("my-key").unwrap();
//! ```

// Core trait and types
pub mod backend;
pub use backend::{BlobMeta, StorageBackend};

// Hybrid storage layer (local cache + cloud tier)
pub mod hybrid;
pub use hybrid::{CacheStats, CloudMetricsSnapshot, HybridStorage, HybridStorageBackend};

// Mock implementation (always available)
pub mod mock;
pub use mock::MockCloudBackend;

// Cloud provider implementations (feature-gated)
#[cfg(feature = "cloud-aws")]
pub mod aws;
#[cfg(feature = "cloud-aws")]
pub use aws::AwsS3Backend;

#[cfg(feature = "cloud-azure")]
pub mod azure;
#[cfg(feature = "cloud-azure")]
pub use azure::AzureBlobBackend;

#[cfg(feature = "cloud-gcp")]
pub mod gcp;
#[cfg(feature = "cloud-gcp")]
pub use gcp::GcpStorageBackend;

#[cfg(feature = "cloud-oci")]
pub mod oci;
#[cfg(feature = "cloud-oci")]
pub use oci::OciObjectStorageBackend;
