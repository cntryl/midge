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
