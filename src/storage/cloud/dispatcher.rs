//! Namespace-aware cloud request dispatch.

use super::{
    set_request_timeout_header, CloudBackend, CloudCallback, CloudDeleteCapability,
    CloudHeadCapability, CloudListCapability, CloudReadCapability, CloudReadOptions,
    CloudRequestOptions, CloudWriteCapability,
};
use std::sync::Arc;

/// Namespace-aware dispatcher that forwards calls to the active backend.
pub struct CloudStorage {
    pub(super) reads: Arc<dyn CloudReadCapability>,
    pub(super) writes: Arc<dyn CloudWriteCapability>,
    pub(super) deletes: Arc<dyn CloudDeleteCapability>,
    pub(super) lists: Arc<dyn CloudListCapability>,
    pub(super) heads: Arc<dyn CloudHeadCapability>,
    namespace: String,
    pub(super) callback_timeout: std::time::Duration,
    pub(super) startup_scope: Option<crate::common::DeadlineScope>,
}

impl CloudStorage {
    pub(crate) fn set_retry_policy(&self, policy: super::CloudRetryPolicy) {
        self.writes.set_retry_policy(policy);
    }

    #[cfg(test)]
    pub fn new(backend: Arc<dyn CloudBackend>, namespace: String) -> Self {
        Self::new_with_timeout(
            backend,
            namespace,
            crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
        )
    }

    pub(crate) fn new_with_timeout(
        backend: Arc<dyn CloudBackend>,
        namespace: String,
        callback_timeout: std::time::Duration,
    ) -> Self {
        Self {
            reads: backend.clone(),
            writes: backend.clone(),
            deletes: backend.clone(),
            lists: backend.clone(),
            heads: backend,
            namespace,
            callback_timeout,
            startup_scope: None,
        }
    }

    #[cfg(test)]
    pub fn with_mock() -> Self {
        let backend = Arc::new(super::MockCloudBackend::new());
        Self::new(backend, "midge".to_string())
    }

    pub(crate) fn callback_timeout(&self) -> std::time::Duration {
        self.scoped_timeout(self.callback_timeout)
    }

    pub(crate) fn with_startup_scope(&self, scope: crate::common::DeadlineScope) -> Self {
        Self {
            reads: Arc::clone(&self.reads),
            writes: Arc::clone(&self.writes),
            deletes: Arc::clone(&self.deletes),
            lists: Arc::clone(&self.lists),
            heads: Arc::clone(&self.heads),
            namespace: self.namespace.clone(),
            callback_timeout: self.callback_timeout,
            startup_scope: Some(scope),
        }
    }

    pub(super) fn scoped_timeout(&self, timeout: std::time::Duration) -> std::time::Duration {
        self.startup_scope.as_ref().map_or(timeout, |scope| {
            let deadline = scope.deadline();
            if deadline.is_bounded() {
                deadline.clamp(timeout.min(self.callback_timeout))
            } else {
                timeout
            }
        })
    }

    pub(super) fn check_startup_scope(&self, context: &str) -> crate::common::MidgeResult<()> {
        self.startup_scope
            .as_ref()
            .map_or(Ok(()), |scope| scope.check(context))
    }

    fn scope_request_headers(
        &self,
        headers: &mut Vec<(String, String)>,
    ) -> Result<(), super::CloudError> {
        let Some(scope) = self.startup_scope.as_ref() else {
            return Ok(());
        };
        let deadline = scope.deadline();
        if !deadline.is_bounded() {
            return Ok(());
        }
        let mut timeout = self.callback_timeout;
        for (_, value) in headers
            .iter()
            .filter(|(name, _)| name.eq_ignore_ascii_case(super::REQUEST_TIMEOUT_HEADER))
        {
            timeout = std::time::Duration::from_millis(value.parse::<u64>().map_err(|error| {
                super::CloudError::Protocol(format!("invalid internal request timeout: {error}"))
            })?);
        }
        let timeout = deadline.clamp(timeout.min(self.callback_timeout));
        if timeout.is_zero() {
            return Err(super::CloudError::Timeout(
                "cloud mutation has no startup budget".into(),
            ));
        }
        set_request_timeout_header(headers, timeout);
        Ok(())
    }

    pub(super) fn full_path(&self, suffix: &str) -> String {
        let namespace = self.namespace.trim_matches('/');
        let suffix = suffix.trim_start_matches('/');
        if namespace.is_empty() {
            suffix.to_string()
        } else if suffix.is_empty() {
            namespace.to_string()
        } else {
            format!("{namespace}/{suffix}")
        }
    }

    pub(crate) fn strip_namespace<'a>(&self, key: &'a str) -> &'a str {
        let namespace = self.namespace.trim_matches('/');
        if namespace.is_empty() {
            return key;
        }
        key.strip_prefix(namespace)
            .and_then(|rest| rest.strip_prefix('/'))
            .unwrap_or(key)
    }

    pub fn submit_put(
        &self,
        key: &str,
        data: Vec<u8>,
        mut headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        if let Err(error) = self.scope_request_headers(&mut headers) {
            let _ = callback.send(super::CloudEvent::Put {
                key: key.to_string(),
                result: Err(error),
            });
            return;
        }
        let full_key = self.full_path(key);
        let options = headers
            .into_iter()
            .fold(CloudRequestOptions::default(), |options, (name, value)| {
                options.with_header(name, value)
            });
        self.writes.write(&full_key, data, options, callback);
    }

    #[cfg(test)]
    pub fn submit_get(&self, key: &str, callback: CloudCallback) {
        let full_key = self.full_path(key);
        self.reads
            .read(&full_key, CloudReadOptions::default(), callback);
    }

    pub(crate) fn submit_get_within(
        &self,
        key: &str,
        timeout: std::time::Duration,
        callback: CloudCallback,
    ) {
        let timeout = self.scoped_timeout(timeout);
        if timeout.is_zero() {
            let _ = callback.send(super::CloudEvent::Get {
                key: key.to_string(),
                result: Err(super::CloudError::Timeout(
                    "cloud GET has no remaining budget".to_string(),
                )),
            });
            return;
        }
        let request = CloudRequestOptions::default()
            .with_deadline(crate::common::OperationDeadline::from_budget(timeout));
        self.reads.read(
            &self.full_path(key),
            CloudReadOptions::default().with_request_options(request),
            callback,
        );
    }

    pub(crate) fn submit_get_with_metadata_within(
        &self,
        key: &str,
        timeout: std::time::Duration,
        callback: CloudCallback,
    ) {
        let timeout = self.scoped_timeout(timeout);
        if timeout.is_zero() {
            let _ = callback.send(super::CloudEvent::GetWithMetadata {
                key: key.to_string(),
                result: Err(super::CloudError::Timeout(
                    "cloud metadata GET has no remaining budget".to_string(),
                )),
            });
            return;
        }
        let request = CloudRequestOptions::default()
            .with_deadline(crate::common::OperationDeadline::from_budget(timeout));
        self.reads.read(
            &self.full_path(key),
            CloudReadOptions::default()
                .with_request_options(request)
                .with_metadata(),
            callback,
        );
    }

    #[cfg(test)]
    pub fn submit_get_range(
        &self,
        key: &str,
        start: u64,
        end: Option<u64>,
        callback: CloudCallback,
    ) {
        let full_key = self.full_path(key);
        self.reads.submit_get_range(&full_key, start, end, callback);
    }

    /// Submit an unconditional DELETE bounded by this adapter's callback
    /// timeout.
    pub fn submit_delete(&self, key: &str, callback: CloudCallback) {
        let mut headers = Vec::new();
        set_request_timeout_header(&mut headers, self.callback_timeout());
        self.submit_delete_with_headers(key, headers, callback);
    }

    pub fn submit_delete_with_headers(
        &self,
        key: &str,
        mut headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        if let Err(error) = self.scope_request_headers(&mut headers) {
            let _ = callback.send(super::CloudEvent::Delete {
                key: key.to_string(),
                result: Err(error),
            });
            return;
        }
        let full_key = self.full_path(key);
        self.deletes.delete(
            &full_key,
            CloudRequestOptions::default().with_headers(headers),
            callback,
        );
    }

    #[cfg(test)]
    pub fn submit_list(&self, prefix: &str, callback: CloudCallback) {
        self.submit_list_within(prefix, self.callback_timeout(), callback);
    }

    pub(crate) fn submit_list_within(
        &self,
        prefix: &str,
        timeout: std::time::Duration,
        callback: CloudCallback,
    ) {
        let timeout = self.scoped_timeout(timeout);
        if timeout.is_zero() {
            let _ = callback.send(super::CloudEvent::List {
                prefix: prefix.to_string(),
                result: Err(super::CloudError::Timeout(
                    "cloud LIST has no remaining budget".into(),
                )),
            });
            return;
        }
        let full_prefix = self.full_path(prefix);
        self.lists.list(
            &full_prefix,
            CloudRequestOptions::default()
                .with_deadline(crate::common::OperationDeadline::from_budget(timeout)),
            callback,
        );
    }

    #[cfg(test)]
    pub fn submit_head(&self, key: &str, callback: CloudCallback) {
        self.submit_head_within(key, self.callback_timeout(), callback);
    }

    pub fn submit_head_within(
        &self,
        key: &str,
        timeout: std::time::Duration,
        callback: CloudCallback,
    ) {
        let timeout = self.scoped_timeout(timeout);
        if timeout.is_zero() {
            let _ = callback.send(super::CloudEvent::Head {
                key: key.to_string(),
                result: Err(super::CloudError::Timeout(
                    "cloud HEAD has no remaining budget".into(),
                )),
            });
            return;
        }
        let full_key = self.full_path(key);
        self.heads.head(
            &full_key,
            CloudRequestOptions::default()
                .with_deadline(crate::common::OperationDeadline::from_budget(timeout)),
            callback,
        );
    }
}
