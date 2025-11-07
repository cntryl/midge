//! Blob-level cloud storage trait and minimal metadata.
//!
//! Providers should implement this trait to expose plain blob operations
//! (put/get/delete/list/head/ranged reads). Higher-level concepts like WAL
//! segments, SST metadata, or locks should be implemented in higher-level
//! modules (e.g., `wal::cloud` or `sst::cloud`) that translate those concepts
//! to blob operations.

// ```rust
use crate::error::{MidgeError, MidgeResult};
use bytes::Bytes;
use std::time::SystemTime;

/// Minimal metadata about a blob/object returned by `head_blob`.
#[derive(Debug, Clone)]
pub struct BlobMeta {
    /// Size in bytes
    pub size: u64,
    /// Optional ETag or checksum returned by the provider
    pub etag: Option<String>,
    /// Last modified timestamp if available
    pub last_modified: Option<SystemTime>,
}

/// Blob-level storage backend trait.
///
/// This is intentionally minimal and provider-agnostic: it models blobs
/// (keyed byte sequences) and a few common operations required by the
/// rest of the codebase.
pub trait StorageBackend: Send + Sync {
    /// Put (create or replace) a blob identified by `key` with the provided bytes.
    fn put_blob(&self, key: &str, data: Bytes) -> MidgeResult<()>;

    /// Get the full blob as bytes.
    fn get_blob(&self, key: &str) -> MidgeResult<Bytes>;

    /// Get a byte range from the blob. `end` is exclusive when Some.
    fn get_blob_range(&self, key: &str, start: u64, end: Option<u64>) -> MidgeResult<Bytes>;

    /// Delete the blob if it exists.
    fn delete_blob(&self, key: &str) -> MidgeResult<()>;

    /// List blob keys with the given prefix. Returns a lexicographically sorted list.
    fn list_blobs(&self, prefix: &str) -> MidgeResult<Vec<String>>;

    /// Retrieve metadata (size, etag, last_modified) for the given key.
    /// Returns Ok(None) if the blob does not exist.
    fn head_blob(&self, key: &str) -> MidgeResult<Option<BlobMeta>>;

    /// Put blob only if it does not already exist. Returns the ETag or error.
    fn put_blob_if_not_exists(&self, key: &str, data: Bytes) -> MidgeResult<String>;

    /// Get the blob bytes along with an optional ETag/version token.
    ///
    /// Default implementation fetches the full blob and then calls `head_blob`
    /// to obtain the ETag where available.
    fn get_with_etag(&self, key: &str) -> MidgeResult<(Bytes, Option<String>)> {
        let bytes = self.get_blob(key)?;
        let meta = self.head_blob(key)?;
        let etag = meta.and_then(|m| m.etag);
        Ok((bytes, etag))
    }

    /// Conditionally replace the blob only if the current ETag matches
    /// `expected_etag`. Returns the new ETag on success.
    ///
    /// Default implementation uses `head_blob` + `put_blob` as a best-effort
    /// emulation. Providers with native conditional-put support should
    /// override this for atomic semantics.
    fn put_if_match(&self, key: &str, data: Bytes, expected_etag: &str) -> MidgeResult<String> {
        match self.head_blob(key)? {
            Some(meta) => {
                if meta.etag.as_deref() == Some(expected_etag) {
                    self.put_blob(key, data)?;
                    // Return new ETag if provider provides one
                    match self.head_blob(key)? {
                        Some(new_meta) => Ok(new_meta.etag.unwrap_or_default()),
                        None => Ok(String::new()),
                    }
                } else {
                    Err(MidgeError::internal("etag mismatch"))
                }
            }
            None => Err(MidgeError::internal("blob not found")),
        }
    }

    /// Convenience alias matching existing call-sites: put the blob only if it
    /// does not already exist. By default forwards to `put_blob_if_not_exists`.
    fn put_if_not_exists(&self, key: &str, data: Bytes) -> MidgeResult<String> {
        self.put_blob_if_not_exists(key, data)
    }
}

// Small convenience re-exports for users of this module
pub use bytes::Bytes as BlobBytes;
