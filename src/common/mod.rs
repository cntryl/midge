//! Foundational types and traits - zero external dependencies

pub mod error;
pub mod singleflight;
pub mod tlv;

pub use error::{MidgeError, MidgeResult};
pub use singleflight::KeyedGroupCommit;
