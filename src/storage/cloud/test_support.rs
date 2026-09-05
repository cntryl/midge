//! Shared forwarding for test backends that override selected cloud operations.
//! Fault-injecting methods stay explicit in each wrapper.

macro_rules! forward_cloud_backend {
    ($inner:ident; $($method:ident),+ $(,)?) => {
        $($crate::storage::cloud::forward_cloud_backend!(@method $inner, $method);)+
    };
    (@method $inner:ident, submit_put) => {
        fn submit_put(&self, key: &str, data: Vec<u8>, headers: Vec<(String, String)>, callback: $crate::storage::cloud::CloudCallback) {
            self.$inner.submit_put(key, data, headers, callback);
        }
    };
    (@method $inner:ident, submit_get) => {
        fn submit_get(&self, key: &str, callback: $crate::storage::cloud::CloudCallback) {
            self.$inner.submit_get(key, callback);
        }
    };
    (@method $inner:ident, submit_get_with_metadata) => {
        fn submit_get_with_metadata(&self, key: &str, callback: $crate::storage::cloud::CloudCallback) {
            self.$inner.submit_get_with_metadata(key, callback);
        }
    };
    (@method $inner:ident, submit_get_range) => {
        fn submit_get_range(&self, key: &str, start: u64, end: Option<u64>, callback: $crate::storage::cloud::CloudCallback) {
            self.$inner.submit_get_range(key, start, end, callback);
        }
    };
    (@method $inner:ident, submit_get_range_with_identity) => {
        fn submit_get_range_with_identity(&self, key: &str, start: u64, end: u64,
            expected: $crate::storage::StorageObjectMetadata, timeout: std::time::Duration,
            callback: $crate::storage::cloud::CloudCallback) {
            self.$inner.submit_get_range_with_identity(key, start, end, expected, timeout, callback);
        }
    };
    (@method $inner:ident, submit_delete) => {
        fn submit_delete(&self, key: &str, headers: Vec<(String, String)>, callback: $crate::storage::cloud::CloudCallback) {
            self.$inner.submit_delete(key, headers, callback);
        }
    };
    (@method $inner:ident, submit_list) => {
        fn submit_list(&self, prefix: &str, callback: $crate::storage::cloud::CloudCallback) {
            self.$inner.submit_list(prefix, callback);
        }
    };
    (@method $inner:ident, submit_head) => {
        fn submit_head(&self, key: &str, callback: $crate::storage::cloud::CloudCallback) {
            self.$inner.submit_head(key, callback);
        }
    };
}

pub(crate) use forward_cloud_backend;
