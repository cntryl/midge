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
            request_body_bytes: request.body.as_ref().map_or(0, Vec::len),
            status: 0,
            response_body_bytes: 0,
            transport_error: false,
            cancelled: true,
        }
    }
}

impl Drop for Attempt {
    fn drop(&mut self) {
        tracing::info!(
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
