//! Helpers shared by the REST providers.
//!
//! Each function is gated by exactly the providers that call it, so no
//! feature combination compiles code it does not use.

#[cfg(any(
    feature = "cloud-aws",
    feature = "cloud-oci",
    feature = "cloud-azure",
    feature = "cloud-gcp"
))]
use crate::storage::cloud::CloudError;
#[cfg(any(
    feature = "cloud-aws",
    feature = "cloud-oci",
    feature = "cloud-azure",
    feature = "cloud-gcp"
))]
use crate::storage::cloud::{CloudEvent, CloudListBudget};
#[cfg(any(feature = "cloud-aws", feature = "cloud-oci", feature = "cloud-azure"))]
use crate::storage::cloud::{CloudOutcome, CloudResponse, ObjectMetadata};

/// State shared by the paginated REST provider loops. Provider-specific URL
/// construction and page parsing stay in the caller.
#[cfg(any(
    feature = "cloud-aws",
    feature = "cloud-oci",
    feature = "cloud-azure",
    feature = "cloud-gcp"
))]
pub(super) struct PagedList<P: ListPageParser> {
    pub(super) prefix: String,
    pub(super) provider: P,
    pub(super) token: Option<String>,
    pub(super) items: Vec<String>,
    pub(super) budget: CloudListBudget,
    pub(super) error: Option<CloudError>,
}

#[cfg(any(
    feature = "cloud-aws",
    feature = "cloud-oci",
    feature = "cloud-azure",
    feature = "cloud-gcp"
))]
pub(super) trait ListPageParser {
    fn url(&self, prefix: &str, token: Option<&str>) -> String;
    fn parse_page(
        &self,
        response: &crate::storage::cloud::CloudResponse,
    ) -> crate::common::MidgeResult<(Vec<String>, Option<String>)>;
    fn response_error(&self, response: &crate::storage::cloud::CloudResponse) -> CloudError;
}

#[cfg(any(
    feature = "cloud-aws",
    feature = "cloud-oci",
    feature = "cloud-azure",
    feature = "cloud-gcp"
))]
impl<P: ListPageParser> PagedList<P> {
    pub(super) fn new(prefix: String, provider: P) -> Self {
        Self {
            prefix,
            provider,
            token: None,
            items: Vec::new(),
            budget: CloudListBudget::default(),
            error: None,
        }
    }

    pub(super) fn record_page(
        &mut self,
        items: Vec<String>,
        token: Option<String>,
    ) -> crate::common::MidgeResult<bool> {
        self.budget.record_page(&items, token.as_deref())?;
        self.items.extend(items);
        self.token = token;
        Ok(self.token.is_some())
    }

    pub(super) fn url(&self) -> String {
        self.provider.url(&self.prefix, self.token.as_deref())
    }

    pub(super) fn accept_page(
        &mut self,
        response: &crate::storage::cloud::CloudResponse,
    ) -> crate::common::MidgeResult<bool> {
        if response.status != 200 {
            self.error = Some(self.provider.response_error(response));
            return Ok(false);
        }
        let (items, token) = self.provider.parse_page(response)?;
        self.record_page(items, token)
    }
}

#[cfg(any(
    feature = "cloud-aws",
    feature = "cloud-oci",
    feature = "cloud-azure",
    feature = "cloud-gcp"
))]
pub(super) fn finish_paged_list<P: ListPageParser>(
    prefix: String,
    result: crate::common::MidgeResult<PagedList<P>>,
) -> CloudEvent {
    let result = match result {
        Ok(state) => match state.error {
            Some(error) => Err(error),
            None => Ok(state.items),
        },
        Err(error) => Err(CloudError::from_protocol_or_timeout_error(error)),
    };
    CloudEvent::List { prefix, result }
}

/// The three providers use the same error text layout, but S3 and Azure
/// intentionally omit a message when the response has no error code.
#[cfg(any(
    feature = "cloud-aws",
    feature = "cloud-oci",
    feature = "cloud-azure",
    feature = "cloud-gcp"
))]
pub(super) fn response_error_detail(
    operation: &str,
    code: Option<&str>,
    message: Option<&str>,
    message_without_code: bool,
) -> String {
    match (code, message) {
        (Some(code), Some(message)) => format!("{operation}: {code}: {message}"),
        (Some(code), None) => format!("{operation}: {code}"),
        (None, Some(message)) if message_without_code => format!("{operation}: {message}"),
        (None, _) => operation.to_string(),
    }
}

#[cfg(any(
    feature = "cloud-aws",
    feature = "cloud-oci",
    feature = "cloud-azure",
    feature = "cloud-gcp"
))]
pub(super) fn classify_response_error(
    status: u16,
    detail: String,
    precondition_failed: bool,
) -> CloudError {
    if precondition_failed {
        CloudError::PreconditionFailed(format!("status {status}: {detail}"))
    } else {
        CloudError::from_http_status(status, detail)
    }
}

/// Provider-specific status and error-code rules around the common REST error
/// detail and HTTP fallback mapping.
#[cfg(any(
    feature = "cloud-aws",
    feature = "cloud-oci",
    feature = "cloud-azure",
    feature = "cloud-gcp"
))]
pub(super) trait ProviderDialect {
    fn put_ok(&self, status: u16) -> bool;

    fn delete_ok(&self, status: u16) -> bool;

    fn error_parts(
        &self,
        response: &crate::storage::cloud::CloudResponse,
    ) -> (Option<String>, Option<String>);

    fn precondition_failed(&self, status: u16, code: Option<&str>) -> bool;

    fn message_without_code(&self) -> bool {
        false
    }

    fn special_error(
        &self,
        _status: u16,
        _code: Option<&str>,
        _detail: &str,
    ) -> Option<CloudError> {
        None
    }

    fn response_error(
        &self,
        response: &crate::storage::cloud::CloudResponse,
        operation: &str,
        conditional_mutation: bool,
    ) -> CloudError {
        let (code, message) = self.error_parts(response);
        let detail = response_error_detail(
            operation,
            code.as_deref(),
            message.as_deref(),
            self.message_without_code(),
        );
        if conditional_mutation && self.precondition_failed(response.status, code.as_deref()) {
            return classify_response_error(response.status, detail, true);
        }
        if let Some(error) = self.special_error(response.status, code.as_deref(), &detail) {
            return error;
        }
        classify_response_error(response.status, detail, false)
    }
}

/// Map a completed HTTP request into the operation result. Provider modules
/// keep URL/signing and event construction; this owns the common status and
/// transport branches.
#[cfg(any(
    feature = "cloud-aws",
    feature = "cloud-oci",
    feature = "cloud-azure",
    feature = "cloud-gcp"
))]
pub(super) fn map_response<T>(
    result: crate::common::MidgeResult<crate::storage::cloud::CloudResponse>,
    accepts: impl FnOnce(u16) -> bool,
    success: impl FnOnce(crate::storage::cloud::CloudResponse) -> Result<T, CloudError>,
    error: impl FnOnce(&crate::storage::cloud::CloudResponse) -> CloudError,
) -> Result<T, CloudError> {
    match result {
        Ok(response) if accepts(response.status) => success(response),
        Ok(response) => Err(error(&response)),
        Err(error) => Err(CloudError::from_transport_error(error)),
    }
}

/// Current Unix time in whole seconds, or zero if the clock is before the epoch.
#[cfg(any(feature = "cloud-aws", feature = "cloud-oci", feature = "cloud-gcp"))]
pub(super) fn current_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Identity headers for a conditional ranged read, after checking the request.
///
/// A ranged read is pinned to the object version the caller already verified,
/// so a request whose object has no identity is refused. The range must also be
/// non-empty and lie within the expected size. Identity is checked first.
#[cfg(any(
    feature = "cloud-aws",
    feature = "cloud-oci",
    feature = "cloud-azure",
    feature = "cloud-gcp"
))]
pub(super) fn conditional_range_preconditions(
    range: &std::ops::Range<u64>,
    expected: &crate::storage::StorageObjectMetadata,
) -> Result<Vec<(String, String)>, CloudError> {
    let conditions = crate::storage::cloud::object_match_precondition_headers(
        &expected.etag,
        expected.generation.as_deref(),
    )
    .ok_or_else(|| CloudError::Protocol("range request lacks object identity".into()))?;
    if range.start >= range.end || range.end > expected.size {
        return Err(CloudError::Protocol("invalid object byte range".into()));
    }
    Ok(conditions)
}

/// Prepare an exact range request after the caller has placed its identity
/// condition in the provider's required header or query location.
#[cfg(any(
    feature = "cloud-aws",
    feature = "cloud-oci",
    feature = "cloud-azure",
    feature = "cloud-gcp"
))]
pub(super) fn conditional_range_request(
    range: &std::ops::Range<u64>,
    expected: &crate::storage::StorageObjectMetadata,
    timeout: std::time::Duration,
    make_request: impl FnOnce(Vec<(String, String)>) -> crate::storage::cloud::CloudRequest,
) -> Result<crate::storage::cloud::CloudRequest, CloudError> {
    let conditions = conditional_range_preconditions(range, expected)?;
    Ok(make_request(conditions)
        .with_header("Range", format!("bytes={}-{}", range.start, range.end - 1))
        .with_timeout(timeout)
        .with_response_limit(usize::try_from(range.end - range.start).unwrap_or(usize::MAX)))
}

/// Object size and `ETag` from a metadata (HEAD or GET) response.
///
/// With `known_size` the body length is validated against `Content-Length`;
/// without it `Content-Length` must be present and numeric. `provider` names the
/// service in error messages.
#[cfg(any(feature = "cloud-aws", feature = "cloud-oci", feature = "cloud-azure"))]
pub(super) fn object_metadata_from_response(
    response: &CloudResponse,
    known_size: Option<u64>,
    provider: &str,
) -> CloudOutcome<ObjectMetadata> {
    let size = match known_size {
        Some(_) => crate::storage::cloud::executor::validate_get_response_length(response)?,
        None => response
            .headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
            .ok_or_else(|| {
                CloudError::Protocol(format!(
                    "{provider} metadata response is missing Content-Length"
                ))
            })?
            .1
            .parse::<u64>()
            .map_err(|error| {
                CloudError::Protocol(format!(
                    "{provider} metadata response has invalid Content-Length: {error}"
                ))
            })?,
    };
    let etag = response
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("etag"))
        .map(|(_, value)| value.trim())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            CloudError::Protocol(format!("{provider} metadata response is missing ETag"))
        })?;
    Ok(ObjectMetadata::new(size, etag.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageObjectMetadata;

    fn expected(size: u64, etag: &str) -> StorageObjectMetadata {
        StorageObjectMetadata {
            size,
            etag: etag.to_string(),
            generation: None,
        }
    }

    fn protocol_message(error: CloudError) -> String {
        match error {
            CloudError::Protocol(message) => message,
            other => panic!("expected a protocol error, got {other:?}"),
        }
    }

    #[test]
    fn should_return_identity_headers_when_the_range_lies_within_the_object() {
        // Arrange
        let object = expected(100, "\"v1\"");

        // Act
        let conditions = conditional_range_preconditions(&(0..100), &object);

        // Assert
        assert!(!conditions.expect("a valid range is accepted").is_empty());
    }

    #[test]
    fn should_refuse_a_range_read_when_the_object_has_no_identity() {
        // Arrange: without an etag or generation the read is not pinned to a version.
        let object = expected(100, "");

        // Act
        let error = conditional_range_preconditions(&(0..10), &object).unwrap_err();

        // Assert
        assert_eq!(
            protocol_message(error),
            "range request lacks object identity"
        );
    }

    #[test]
    fn should_refuse_a_range_read_when_the_range_is_empty_or_inverted() {
        // Arrange
        let object = expected(100, "\"v1\"");

        for range in [
            5..5,
            // Deliberately inverted, which the literal `9..3` form lints against.
            std::ops::Range { start: 9, end: 3 },
        ] {
            // Act
            let error = conditional_range_preconditions(&range, &object).unwrap_err();

            // Assert
            assert_eq!(protocol_message(error), "invalid object byte range");
        }
    }

    #[test]
    fn should_refuse_a_range_read_when_the_range_runs_past_the_object() {
        // Arrange
        let object = expected(100, "\"v1\"");

        // Act
        let error = conditional_range_preconditions(&(90..101), &object).unwrap_err();

        // Assert
        assert_eq!(protocol_message(error), "invalid object byte range");
    }

    #[test]
    fn should_check_identity_before_the_range_when_both_are_wrong() {
        // Arrange
        let object = expected(100, "");

        // Act
        let error = conditional_range_preconditions(&std::ops::Range { start: 9, end: 3 }, &object)
            .unwrap_err();

        // Assert
        assert_eq!(
            protocol_message(error),
            "range request lacks object identity"
        );
    }
}
