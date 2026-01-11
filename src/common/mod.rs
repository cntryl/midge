//! Foundational types and traits - zero external dependencies

pub mod error;
pub mod singleflight;

pub use error::{MidgeError, MidgeResult};
pub use singleflight::KeyedGroupCommit;
