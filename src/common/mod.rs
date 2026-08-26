//! Foundational types and traits - zero external dependencies

pub mod deadline;
pub mod error;
pub mod singleflight;
pub mod time;
pub mod tlv;

pub use deadline::OperationDeadline;
pub use error::{MidgeError, MidgeResult};
pub use singleflight::KeyedGroupCommit;
