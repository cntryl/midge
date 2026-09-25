//! Completion events and object metadata exchanged with cloud backends.

#[allow(clippy::wildcard_imports)]
use super::*;

/// Cloud operation completion events sent back via callback.
#[derive(Clone, Debug)]
pub enum CloudEvent {
    #[cfg_attr(not(any(test, feature = "cloud-common")), allow(dead_code))]
    Put {
        /// The provider's key, kept for diagnostics: adapters report the
        /// caller's key instead (#514).
        #[cfg_attr(not(test), allow(dead_code))]
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
        /// The provider's key, kept for diagnostics: adapters report the
        /// caller's key instead (#514).
        #[cfg_attr(not(test), allow(dead_code))]
        key: String,
        result: CloudOutcome<()>,
    },
    List {
        prefix: String,
        result: CloudOutcome<Vec<String>>,
    },
    Head {
        /// The provider's key, kept for diagnostics: adapters report the
        /// caller's key instead (#514).
        #[cfg_attr(not(test), allow(dead_code))]
        key: String,
        result: CloudOutcome<ObjectMetadata>,
    },
}

/// Callback type used to send `CloudEvent`s back to the runtime.
pub type CloudCallback = std::sync::mpsc::Sender<CloudEvent>;

/// Basic metadata emitted by HEAD operations.
///
/// The same type the storage layer uses to revalidate cached proofs, so a backend's
/// answer needs no conversion on its way up.
pub use crate::storage::StorageObjectMetadata as ObjectMetadata;

pub(crate) fn object_match_precondition_headers(
    etag: &str,
    generation: Option<&str>,
) -> Option<Vec<(String, String)>> {
    crate::storage::conditional_object_identity(etag, generation)
        .map(|(header, value)| vec![(header.to_string(), value.to_string())])
}
