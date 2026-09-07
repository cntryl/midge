use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use crate::common::MidgeResult;

use super::filesystem::FileSystem;
use super::{HybridStorage, StorageEvent};

/// Shared forwarding for test backends that override selected storage
/// operations. Fault-injecting methods stay explicit in each wrapper.
///
/// Mirrors `crate::storage::cloud::forward_cloud_backend` for the
/// `StorageBackend` trait. `$inner` names a field holding the delegate; both
/// `T` and `Arc<T>` work, because forwarding uses method-call syntax.
#[cfg(test)]
macro_rules! forward_storage_backend {
    ($inner:ident; $($method:ident),+ $(,)?) => {
        $($crate::storage::forward_storage_backend!(@method $inner, $method);)+
    };
    (@method $inner:ident, submit_read) => {
        fn submit_read(&self, key: &str, callback: $crate::storage::StorageCallback) {
            self.$inner.submit_read(key, callback);
        }
    };
    (@method $inner:ident, submit_read_with_timeout) => {
        fn submit_read_with_timeout(&self, key: &str, timeout: std::time::Duration, callback: $crate::storage::StorageCallback) {
            self.$inner.submit_read_with_timeout(key, timeout, callback);
        }
    };
    (@method $inner:ident, submit_read_with_metadata) => {
        fn submit_read_with_metadata(&self, key: &str, timeout: std::time::Duration, callback: $crate::storage::MetadataReadCallback) {
            self.$inner.submit_read_with_metadata(key, timeout, callback);
        }
    };
    (@method $inner:ident, submit_read_range) => {
        fn submit_read_range(&self, key: &str, start: u64, end: u64,
            expected: $crate::storage::StorageObjectMetadata, timeout: std::time::Duration,
            callback: $crate::storage::RangeReadCallback) {
            self.$inner.submit_read_range(key, start, end, expected, timeout, callback);
        }
    };
    (@method $inner:ident, submit_read_range_with_reservation) => {
        fn submit_read_range_with_reservation(&self, key: &str, range: std::ops::Range<u64>,
            expected: $crate::storage::StorageObjectMetadata, timeout: std::time::Duration,
            reservation: std::sync::Arc<$crate::common::resource_budget::ResourceReservation>,
            callback: $crate::storage::RangeReadCallback) {
            self.$inner.submit_read_range_with_reservation(key, range, expected, timeout, reservation, callback);
        }
    };
    (@method $inner:ident, submit_range_head) => {
        fn submit_range_head(&self, key: &str, timeout: std::time::Duration, callback: $crate::storage::StorageCallback) {
            self.$inner.submit_range_head(key, timeout, callback);
        }
    };
    (@method $inner:ident, submit_write) => {
        fn submit_write(&self, key: &str, data: Vec<u8>, callback: $crate::storage::StorageCallback) {
            self.$inner.submit_write(key, data, callback);
        }
    };
    (@method $inner:ident, submit_write_with_headers) => {
        fn submit_write_with_headers(&self, key: &str, data: Vec<u8>,
            headers: Vec<(String, String)>, callback: $crate::storage::StorageCallback) {
            self.$inner.submit_write_with_headers(key, data, headers, callback);
        }
    };
    (@method $inner:ident, submit_write_with_headers_and_timeout) => {
        fn submit_write_with_headers_and_timeout(&self, key: &str, data: Vec<u8>,
            headers: Vec<(String, String)>, timeout: std::time::Duration,
            callback: $crate::storage::StorageCallback) {
            self.$inner.submit_write_with_headers_and_timeout(key, data, headers, timeout, callback);
        }
    };
    (@method $inner:ident, submit_write_with_reservation) => {
        fn submit_write_with_reservation(&self, key: &str, data: Vec<u8>,
            headers: Vec<(String, String)>, timeout: std::time::Duration,
            reservation: std::sync::Arc<$crate::common::resource_budget::ResourceReservation>,
            callback: $crate::storage::StorageCallback) {
            self.$inner.submit_write_with_reservation(key, data, headers, timeout, reservation, callback);
        }
    };
    (@method $inner:ident, submit_delete) => {
        fn submit_delete(&self, key: &str, callback: $crate::storage::StorageCallback) {
            self.$inner.submit_delete(key, callback);
        }
    };
    (@method $inner:ident, submit_delete_with_headers) => {
        fn submit_delete_with_headers(&self, key: &str, headers: Vec<(String, String)>,
            callback: $crate::storage::StorageCallback) {
            self.$inner.submit_delete_with_headers(key, headers, callback);
        }
    };
    (@method $inner:ident, submit_list) => {
        fn submit_list(&self, prefix: &str, callback: $crate::storage::StorageCallback) {
            self.$inner.submit_list(prefix, callback);
        }
    };
    (@method $inner:ident, submit_head) => {
        fn submit_head(&self, key: &str, callback: $crate::storage::StorageCallback) {
            self.$inner.submit_head(key, callback);
        }
    };
    (@method $inner:ident, submit_head_with_timeout) => {
        fn submit_head_with_timeout(&self, key: &str, timeout: std::time::Duration, callback: $crate::storage::StorageCallback) {
            self.$inner.submit_head_with_timeout(key, timeout, callback);
        }
    };
}

#[cfg(test)]
pub(crate) use forward_storage_backend;

pub(crate) struct CloudBackedTestSetup {
    pub hybrid_storage: Arc<HybridStorage>,
    pub events: crossbeam::channel::Receiver<StorageEvent>,
    pub cloud_root: PathBuf,
    pub recovery_cloud_wal_dir: PathBuf,
}

/// Builds a deterministic, filesystem-backed “cloud” for tests.
///
/// The engine/testkit should not know about folders/blobs; it only needs the
/// resulting `HybridStorage`, event stream, and a recovery directory.
pub(crate) fn build_cloud_backed_filesystem_simulation(
    db_path: &Path,
    local_storage_budget_bytes: Option<u64>,
) -> MidgeResult<CloudBackedTestSetup> {
    // Simulate cloud with a separate filesystem-backed store under db_path.
    let cloud_root = db_path.join("cloud_store");
    let recovery_cloud_wal_dir = cloud_root.join("wal");
    let _ = std::fs::create_dir_all(&recovery_cloud_wal_dir);

    let local_backend = Arc::new(FileSystem::new(db_path.join("hybrid_local"))?);
    let cloud_backend = Arc::new(FileSystem::new(cloud_root.clone())?);

    let (tx, rx) = crossbeam::channel::bounded::<StorageEvent>(
        crate::storage::hybrid::backend::HYBRID_STORAGE_EVENT_CHANNEL_CAPACITY,
    );
    let hybrid_storage = if let Some(budget_bytes) = local_storage_budget_bytes {
        Arc::new(HybridStorage::with_policy_and_event_sender(
            local_backend,
            cloud_backend,
            crate::storage::hybrid::policy::StorageBudgetPolicy::new(budget_bytes),
            Some(tx),
        ))
    } else {
        Arc::new(HybridStorage::new_with_event_sender(
            local_backend,
            cloud_backend,
            tx,
        ))
    };

    Ok(CloudBackedTestSetup {
        hybrid_storage,
        events: rx,
        cloud_root,
        recovery_cloud_wal_dir,
    })
}
