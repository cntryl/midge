//! Helpers shared by the REST providers.
//!
//! Each function is gated by exactly the providers that call it, so no
//! feature combination compiles code it does not use.

#[cfg(any(feature = "cloud-aws", feature = "cloud-oci", feature = "cloud-azure"))]
use crate::storage::cloud::{CloudError, CloudOutcome, CloudResponse, ObjectMetadata};

/// Current Unix time in whole seconds, or zero if the clock is before the epoch.
#[cfg(any(feature = "cloud-aws", feature = "cloud-oci", feature = "cloud-gcp"))]
pub(super) fn current_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
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
