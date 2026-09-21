//! Completion events and object metadata exchanged with cloud backends.

#[allow(clippy::wildcard_imports)]
use super::*;

/// Cloud operation completion events sent back via callback.
#[derive(Clone, Debug)]
pub enum CloudEvent {
    #[cfg_attr(not(any(test, feature = "cloud-common")), allow(dead_code))]
    Put {
        key: String,
        result: CloudOutcome<()>,
    },
    Get {
        key: String,
        result: CloudOutcome<Vec<u8>>,
    },
    GetWithMetadata {
        key: String,
        result: CloudOutcome<(Vec<u8>, ObjectMetadata)>,
    },
    GetRange {
        key: String,
        start: u64,
        end: Option<u64>,
        result: CloudOutcome<Vec<u8>>,
    },
    Delete {
        key: String,
        result: CloudOutcome<()>,
    },
    List {
        prefix: String,
        result: CloudOutcome<Vec<String>>,
    },
    Head {
        key: String,
        result: CloudOutcome<ObjectMetadata>,
    },
}

/// Callback type used to send `CloudEvent`s back to the runtime.
pub type CloudCallback = std::sync::mpsc::Sender<CloudEvent>;

/// Basic metadata emitted by HEAD operations.
#[derive(Clone, Debug)]
pub struct ObjectMetadata {
    pub size: u64,
    pub etag: String,
    pub generation: Option<String>,
}
impl ObjectMetadata {
    #[cfg(any(
        test,
        feature = "cloud-aws",
        feature = "cloud-azure",
        feature = "cloud-gcp",
        feature = "cloud-oci"
    ))]
    pub fn new(size: u64, etag: String) -> Self {
        Self {
            size,
            etag,
            generation: None,
        }
    }

    #[cfg(feature = "cloud-gcp")]
    pub fn with_generation(size: u64, etag: String, generation: impl Into<String>) -> Self {
        Self {
            size,
            etag,
            generation: Some(generation.into()),
        }
    }
}

pub(crate) fn object_match_precondition_headers(
    etag: &str,
    generation: Option<&str>,
) -> Option<Vec<(String, String)>> {
    crate::storage::conditional_object_identity(etag, generation)
        .map(|(header, value)| vec![(header.to_string(), value.to_string())])
}
