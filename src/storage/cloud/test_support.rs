//! Shared forwarding for test backends that override selected cloud operations.
//! Fault-injecting methods stay explicit in each wrapper.

/// Explicit delegate for test doubles that intentionally do not model a core
/// provider operation. Production backends cannot opt into these fallbacks:
/// every core operation remains required by [`CloudBackend`].
pub(crate) struct UnsupportedCloudBackend;

impl UnsupportedCloudBackend {
    pub(crate) fn submit_get(key: &str, callback: &crate::storage::cloud::CloudCallback) {
        let _ = callback.send(crate::storage::cloud::CloudEvent::Get {
            key: key.to_string(),
            result: Err(crate::storage::cloud::CloudError::Protocol(
                "cloud backend does not support GET".to_string(),
            )),
        });
    }

    pub(crate) fn submit_get_with_metadata(
        key: &str,
        callback: &crate::storage::cloud::CloudCallback,
    ) {
        let _ = callback.send(crate::storage::cloud::CloudEvent::GetWithMetadata {
            key: key.to_string(),
            result: Err(crate::storage::cloud::CloudError::Protocol(
                "cloud backend does not support metadata-bearing GET".to_string(),
            )),
        });
    }

    pub(crate) fn submit_delete(key: &str, callback: &crate::storage::cloud::CloudCallback) {
        let _ = callback.send(crate::storage::cloud::CloudEvent::Delete {
            key: key.to_string(),
            result: Err(crate::storage::cloud::CloudError::Protocol(
                "cloud backend does not support DELETE".to_string(),
            )),
        });
    }

    pub(crate) fn submit_list(prefix: &str, callback: &crate::storage::cloud::CloudCallback) {
        let _ = callback.send(crate::storage::cloud::CloudEvent::List {
            prefix: prefix.to_string(),
            result: Err(crate::storage::cloud::CloudError::Protocol(
                "cloud backend does not support LIST".to_string(),
            )),
        });
    }

    pub(crate) fn submit_head(key: &str, callback: &crate::storage::cloud::CloudCallback) {
        let _ = callback.send(crate::storage::cloud::CloudEvent::Head {
            key: key.to_string(),
            result: Err(crate::storage::cloud::CloudError::Protocol(
                "cloud backend does not support HEAD".to_string(),
            )),
        });
    }
}

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

macro_rules! unsupported_cloud_backend {
    ($($method:ident),+ $(,)?) => {
        $($crate::storage::cloud::unsupported_cloud_backend!(@method $method);)+
    };
    (@method submit_get) => {
        fn submit_get(&self, key: &str, callback: $crate::storage::cloud::CloudCallback) {
            $crate::storage::cloud::UnsupportedCloudBackend::submit_get(key, &callback);
        }
    };
    (@method submit_get_with_metadata) => {
        fn submit_get_with_metadata(&self, key: &str, callback: $crate::storage::cloud::CloudCallback) {
            $crate::storage::cloud::UnsupportedCloudBackend::submit_get_with_metadata(key, &callback);
        }
    };
    (@method submit_delete) => {
        fn submit_delete(&self, key: &str, _headers: Vec<(String, String)>, callback: $crate::storage::cloud::CloudCallback) {
            $crate::storage::cloud::UnsupportedCloudBackend::submit_delete(key, &callback);
        }
    };
    (@method submit_list) => {
        fn submit_list(&self, prefix: &str, callback: $crate::storage::cloud::CloudCallback) {
            $crate::storage::cloud::UnsupportedCloudBackend::submit_list(prefix, &callback);
        }
    };
    (@method submit_head) => {
        fn submit_head(&self, key: &str, callback: $crate::storage::cloud::CloudCallback) {
            $crate::storage::cloud::UnsupportedCloudBackend::submit_head(key, &callback);
        }
    };
}

pub(crate) use unsupported_cloud_backend;
