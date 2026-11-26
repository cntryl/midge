//! Deterministic simulated storage for crash and fault campaigns (#808 stage
//! 1b). Test support only: compiled for unit tests and the `internal-testing`
//! feature, never into production builds.

pub mod clock;
pub mod crash_fs;
