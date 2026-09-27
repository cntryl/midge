//! Callback-based cloud storage abstractions.
//!
//! Aligns with the actor runtime model: synchronous submission + async completion.
//! - `CloudBackend` defines submit-only methods (PUT/GET/DELETE/LIST/HEAD).
//! - Backends send results via `CloudCallback` channels (no futures in the engine).
//! - `CloudStorage` is a namespace-aware dispatcher that shields the rest of the engine.
//! - `MockCloudBackend` keeps deterministic testing without async runtimes.
//!
//! ## Architecture
//!
//! ```text
//! CloudStorage (namespace-aware dispatcher)
//!     ↓
//! CloudBackend trait (interface: submit_put, submit_get, etc.)
//!     ↓
//! [Real backends via CloudExecutor]  [MockCloudBackend for testing]
//! ```
//!
//! ## Async Model
//!
//! - `submit_*()` methods return immediately (non-blocking)
//! - Results are sent back via `CloudCallback` channels (`mpsc::Sender<CloudEvent>`)
//! - Events are received asynchronously but callback processing is synchronous
//! - No futures in the engine: all async work happens in `CloudExecutor` embedded tokio runtime

mod blocking;
mod config;
#[cfg(any(
    feature = "cloud-aws",
    feature = "cloud-azure",
    feature = "cloud-gcp",
    feature = "cloud-oci"
))]
pub(crate) mod range;
#[cfg(test)]
mod test_support;
#[cfg(test)]
pub(crate) use test_support::{
    forward_cloud_backend, unsupported_cloud_backend, UnsupportedCloudBackend,
};
#[cfg(any(
    feature = "cloud-aws",
    feature = "cloud-azure",
    feature = "cloud-gcp",
    feature = "cloud-oci"
))]
pub mod executor;
#[cfg(any(
    feature = "cloud-aws",
    feature = "cloud-azure",
    feature = "cloud-gcp",
    feature = "cloud-oci"
))]
mod list_budget;

pub(crate) use blocking::BlockingCloud;
pub use config::CloudWritePolicy;
pub(crate) use config::CloudWritePolicyConfig;

use super::{StorageBackend, StorageCallback, StorageEvent, StorageObjectMetadata, StorageOutcome};
#[cfg(test)]
use crate::common::MidgeError;
use std::sync::Arc;

pub(crate) const REQUEST_TIMEOUT_HEADER: &str = "x-midge-internal-request-timeout-ms";

#[cfg(any(
    feature = "cloud-aws",
    feature = "cloud-azure",
    feature = "cloud-gcp",
    feature = "cloud-oci"
))]
pub(crate) fn parse_request_timeout(value: &str) -> Result<std::time::Duration, String> {
    value
        .parse::<u64>()
        .map(std::time::Duration::from_millis)
        .map_err(|error| format!("invalid internal request timeout: {error}"))
}

/// Bound the provider request by the same budget the storage adapter waits
/// on, replacing any timeout the caller already supplied. Without it the
/// provider falls back to the executor default and a mutation can commit
/// remotely after the caller has already reported a timeout.
pub(crate) fn set_request_timeout_header(
    headers: &mut Vec<(String, String)>,
    timeout: std::time::Duration,
) {
    headers.retain(|(name, _)| !name.eq_ignore_ascii_case(REQUEST_TIMEOUT_HEADER));
    headers.push((
        REQUEST_TIMEOUT_HEADER.into(),
        timeout.as_millis().max(1).to_string(),
    ));
}

/// Wire headers plus the provider request timeout split out of them.
#[cfg(any(
    feature = "cloud-aws",
    feature = "cloud-azure",
    feature = "cloud-gcp",
    feature = "cloud-oci"
))]
pub(crate) type SplitRequestHeaders = (Vec<(String, String)>, Option<std::time::Duration>);

/// Split the internal timeout pseudo-header from the headers a provider
/// sends on the wire.
#[cfg(any(
    feature = "cloud-aws",
    feature = "cloud-azure",
    feature = "cloud-gcp",
    feature = "cloud-oci"
))]
pub(crate) fn split_request_timeout_header(
    headers: Vec<(String, String)>,
) -> Result<SplitRequestHeaders, String> {
    let mut timeout = None;
    let mut wire_headers = Vec::with_capacity(headers.len());
    for (name, value) in headers {
        if name.eq_ignore_ascii_case(REQUEST_TIMEOUT_HEADER) {
            timeout = Some(parse_request_timeout(&value)?);
        } else {
            wire_headers.push((name, value));
        }
    }
    Ok((wire_headers, timeout))
}

#[cfg(any(
    feature = "cloud-aws",
    feature = "cloud-azure",
    feature = "cloud-gcp",
    feature = "cloud-oci"
))]
pub use executor::{CloudExecutor, CloudRequest, CloudResponse, CloudSigner};
#[cfg(any(
    feature = "cloud-aws",
    feature = "cloud-azure",
    feature = "cloud-gcp",
    feature = "cloud-oci"
))]
pub(crate) use list_budget::CloudListBudget;

mod error;
#[cfg(test)]
use error::cloud_outcome_from_result;
pub(crate) use error::contextualize_operation_error;
pub use error::{CloudError, CloudOutcome};

mod event;
pub(crate) use event::object_match_precondition_headers;
pub use event::{CloudCallback, CloudEvent, ObjectMetadata};

mod backend;
pub use backend::CloudBackend;

#[cfg(test)]
mod mock;
#[cfg(test)]
pub use mock::MockCloudBackend;

mod dispatcher;
pub use dispatcher::CloudStorage;

mod proof;
pub(crate) use proof::{
    blocking_cloud_object_proof, blocking_cloud_object_proof_within, validate_object_proof,
    CloudObjectProof,
};

pub(crate) fn is_not_found_error(error: &CloudError) -> bool {
    error.is_not_found()
}

mod adapter;
use adapter::{cloud_to_storage_outcome, storage_error_from_cloud};

mod admitted;

#[cfg(test)]
mod retained_tests;

#[cfg(test)]
mod tests;
