//! The structured cloud error taxonomy and outcome type.

#[cfg(test)]
use crate::common::MidgeError;

/// Structured cloud provider failure, classified at the point the HTTP
/// response (or transport failure) is first observed — inside each provider
/// implementation (`s3.rs`, `azure.rs`, `gcs.rs`), where the real status code
/// or connection error is still a typed value.
///
/// Downstream consumers (lease acquisition, WAL/SST GC, flush publication)
/// match on these variants directly instead of re-deriving meaning from a
/// formatted message string. In particular, [`CloudError::PreconditionFailed`]
/// is reserved for a genuine conditional-write/delete race lost to another
/// writer after the provider-specific error code has been checked — every
/// other failure mode (auth, transport, server
/// error, malformed protocol response) uses a distinct variant so callers
/// can no longer conflate "someone else holds it" with "we don't know".
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CloudError {
    /// The object does not exist (404 / `NoSuchKey` / `NotFound` /
    /// `BlobNotFound`).
    #[cfg(any(test, feature = "cloud-common"))]
    NotFound(String),
    /// A conditional request (`If-Match` / `If-None-Match`) lost a genuine
    /// race with a concurrent writer.
    #[cfg_attr(not(any(test, feature = "cloud-common")), allow(dead_code))]
    PreconditionFailed(String),
    /// Authentication or authorization failure (401 / 403).
    #[cfg(any(test, feature = "cloud-common"))]
    Unauthorized(String),
    /// The provider rejected the request as malformed (non-retryable 4xx other
    /// than not-found/precondition/auth).
    #[cfg(any(test, feature = "cloud-common"))]
    InvalidRequest(String),
    /// The provider reported a retryable or server-side failure
    /// (408 / 425 / 429 / 5xx).
    #[cfg(any(test, feature = "cloud-common"))]
    ServerError(String),
    /// The request could not reach the provider (network, DNS, TLS, or
    /// connection failure) without a confirmed deadline expiry.
    #[cfg_attr(not(any(test, feature = "cloud-common")), allow(dead_code))]
    Transport(String),
    /// A typed executor or provider deadline expired.
    #[cfg_attr(
        not(any(
            test,
            feature = "cloud-aws",
            feature = "cloud-azure",
            feature = "cloud-gcp",
            feature = "cloud-oci"
        )),
        allow(dead_code)
    )]
    Timeout(String),
    /// The response did not match the expected protocol: malformed body,
    /// unexpected status, or a required header was missing.
    Protocol(String),
}

impl CloudError {
    /// True when the object is confirmed absent.
    #[must_use]
    pub fn is_not_found(&self) -> bool {
        #[cfg(any(test, feature = "cloud-common"))]
        {
            matches!(self, Self::NotFound(_))
        }
        #[cfg(not(any(test, feature = "cloud-common")))]
        {
            let _ = std::mem::discriminant(self);
            false
        }
    }

    /// True when a conditional write/delete genuinely lost a race to a
    /// concurrent writer, as opposed to failing for an unrelated reason.
    #[must_use]
    pub fn is_precondition_failed(&self) -> bool {
        matches!(self, Self::PreconditionFailed(_))
    }

    /// True when the provider or executor specifically reports deadline
    /// exhaustion, rather than another transport failure such as DNS or TLS.
    #[must_use]
    pub(crate) fn is_timeout(&self) -> bool {
        match self {
            Self::Timeout(_) => true,
            #[cfg(any(test, feature = "cloud-common"))]
            Self::ServerError(message) if message.starts_with("status 408:") => true,
            _ => false,
        }
    }

    #[cfg(any(
        feature = "cloud-aws",
        feature = "cloud-azure",
        feature = "cloud-gcp",
        feature = "cloud-oci"
    ))]
    #[must_use]
    pub(crate) fn from_transport_error(error: crate::common::MidgeError) -> Self {
        match error {
            crate::common::MidgeError::Timeout(message) => Self::Timeout(message),
            other => Self::Transport(format!("{other:?}")),
        }
    }

    /// Preserve parser/protocol classification while retaining a typed
    /// executor deadline from a multi-request operation such as LIST.
    #[cfg(any(
        test,
        feature = "cloud-aws",
        feature = "cloud-azure",
        feature = "cloud-gcp",
        feature = "cloud-oci"
    ))]
    #[must_use]
    pub(crate) fn from_protocol_or_timeout_error(error: crate::common::MidgeError) -> Self {
        match error {
            crate::common::MidgeError::Timeout(message) => Self::Timeout(message),
            other => Self::Protocol(format!("{other:?}")),
        }
    }

    /// Classify a raw HTTP status code from a provider response.
    ///
    /// Status alone is intentionally insufficient to classify a lost
    /// precondition: providers also use 409/412 for leases, retention policy,
    /// snapshots, and other failures. Provider adapters must inspect their
    /// structured error code before constructing [`Self::PreconditionFailed`].
    #[cfg(any(test, feature = "cloud-common"))]
    #[must_use]
    pub(crate) fn from_http_status(status: u16, detail: impl std::fmt::Display) -> Self {
        match status {
            404 => Self::NotFound(format!("status {status}: {detail}")),
            401 | 403 => Self::Unauthorized(format!("status {status}: {detail}")),
            408 | 425 | 429 | 500..=599 => Self::ServerError(format!("status {status}: {detail}")),
            400..=499 => Self::InvalidRequest(format!("status {status}: {detail}")),
            _ => Self::Protocol(format!("unexpected status {status}: {detail}")),
        }
    }
}

impl std::fmt::Display for CloudError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(any(test, feature = "cloud-common"))]
            Self::NotFound(msg) => write!(f, "not found: {msg}"),
            Self::PreconditionFailed(msg) => write!(f, "precondition failed: {msg}"),
            #[cfg(any(test, feature = "cloud-common"))]
            Self::Unauthorized(msg) => write!(f, "unauthorized: {msg}"),
            #[cfg(any(test, feature = "cloud-common"))]
            Self::InvalidRequest(msg) => write!(f, "invalid request: {msg}"),
            #[cfg(any(test, feature = "cloud-common"))]
            Self::ServerError(msg) => write!(f, "server error: {msg}"),
            Self::Transport(msg) => write!(f, "transport error: {msg}"),
            Self::Timeout(msg) => write!(f, "timeout: {msg}"),
            Self::Protocol(msg) => write!(f, "protocol error: {msg}"),
        }
    }
}

impl std::error::Error for CloudError {}

pub(crate) fn contextualize_operation_error(
    error: &CloudError,
    context: impl std::fmt::Display,
    deadline: &crate::common::OperationDeadline,
) -> crate::common::MidgeError {
    let message = format!("{context}: {error}");
    if deadline.is_expired() || error.is_timeout() {
        crate::common::MidgeError::Timeout(message)
    } else {
        crate::common::MidgeError::Internal(message)
    }
}

/// Cloud operation outcome sent across the callback boundary.
pub type CloudOutcome<T> = Result<T, CloudError>;

#[cfg(test)]
pub(super) fn cloud_outcome_from_result<T>(result: Result<T, MidgeError>) -> CloudOutcome<T> {
    result.map_err(|error| match error {
        MidgeError::NotFound => CloudError::NotFound(format!("{error:?}")),
        other => CloudError::Protocol(format!("{other:?}")),
    })
}
