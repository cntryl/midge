//! Transport observations contain costs, never URLs, keys, credentials, or bodies.

use super::CloudRequest;
use std::time::Instant;

pub(super) struct Attempt {
    started: Instant,
    method: reqwest::Method,
    range: bool,
    request_body_bytes: usize,
    pub status: u16,
    pub response_body_bytes: u64,
    pub transport_error: bool,
    pub cancelled: bool,
}

impl Attempt {
    pub fn new(request: &CloudRequest) -> Self {
        Self {
            started: Instant::now(),
            method: request.method.clone(),
            range: request
                .headers
                .iter()
                .any(|(name, _)| name.eq_ignore_ascii_case("range")),
            request_body_bytes: request.shared_body.as_ref().map_or_else(
                || request.body.as_ref().map_or(0, Vec::len),
                bytes::Bytes::len,
            ),
            status: 0,
            response_body_bytes: 0,
            transport_error: false,
            cancelled: true,
        }
    }
}

impl Drop for Attempt {
    fn drop(&mut self) {
        tracing::debug!(
            target: "midge::cloud_io",
            method = self.method.as_str(),
            range = self.range,
            request_body_bytes = self.request_body_bytes,
            response_body_bytes = self.response_body_bytes,
            status = self.status,
            transport_error = self.transport_error,
            cancelled = self.cancelled,
            elapsed_ns = u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            "cloud HTTP attempt completed"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_count_reserved_upload_bytes_after_body_is_shared_for_retries() {
        // Arrange
        let budget = crate::common::resource_budget::ResourceBudget::new(4096);
        let reservation = std::sync::Arc::new(budget.reserve(4096, "observed upload").unwrap());
        let request = CloudRequest::new(reqwest::Method::PUT, "http://unused/object".into())
            .with_body(vec![7; 4096])
            .with_reservation(Some(reservation))
            .share_admitted_body();

        // Act
        let initial = Attempt::new(&request);
        let retry = Attempt::new(&request.clone());

        // Assert
        assert_eq!(initial.request_body_bytes, 4096);
        assert_eq!(retry.request_body_bytes, 4096);
        drop(request);
        assert_eq!(budget.used(), 0);
    }
}
