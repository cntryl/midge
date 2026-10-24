use super::ListPageParser;
use crate::storage::cloud::{
    CloudError, CloudExecutor, CloudListPageCallback, CloudListPageRequest, CloudRequestOptions,
    CloudRetryPolicy, MAX_LIST_PAGE_BYTES,
};

pub(in crate::storage::providers) fn submit_list_page<P: ListPageParser + Send + 'static>(
    executor: &CloudExecutor,
    provider: P,
    prefix: &str,
    page: CloudListPageRequest,
    options: CloudRequestOptions,
    callback: CloudListPageCallback,
) {
    if let Err(error) = page.validate(prefix) {
        let _ = callback.send(Err(error));
        return;
    }
    let timeout = options.timeout();
    if timeout.is_some_and(|timeout| timeout.is_zero()) {
        let _ = callback.send(Err(CloudError::Timeout(
            "bounded LIST page deadline exhausted".into(),
        )));
        return;
    }
    let url = format!(
        "{}&{}={}",
        provider.url(prefix, page.continuation.as_deref()),
        provider.list_limit_parameter(),
        page.max_keys,
    );
    let mut request = provider
        .page_request(url)
        .with_response_limit(MAX_LIST_PAGE_BYTES)
        .with_retry_policy(CloudRetryPolicy {
            max_retries: 0,
            ..Default::default()
        })
        .with_reservation(options.reservation);
    if let Some(timeout) = timeout {
        request = request.with_timeout(timeout);
    }
    executor.spawn_typed_request(
        request,
        prefix.to_owned(),
        callback,
        move |prefix, response| {
            let response = response.map_err(CloudError::from_protocol_or_timeout_error)?;
            if response.status != 200 {
                return Err(provider.response_error(&response));
            }
            let (keys, continuation) = provider
                .parse_page(&response)
                .map_err(CloudError::from_protocol_or_timeout_error)?;
            page.accept(&prefix, keys, continuation).map(Some)
        },
    );
}
