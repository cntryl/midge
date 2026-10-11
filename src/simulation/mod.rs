//! Deterministic simulated storage for crash and fault campaigns (#808 stage
//! 1b). Test support only: compiled for unit tests and the `internal-testing`
//! feature, never into production builds.

pub(crate) mod crash_fs;
#[cfg(any(test, feature = "cloud-common"))]
pub(crate) mod object_store;
pub(crate) mod oracle;
