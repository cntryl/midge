//! Foundational types and traits - zero external dependencies

pub mod deadline;
pub mod error;
#[doc(hidden)]
pub mod resource_budget;
pub mod singleflight;
pub mod time;
pub mod tlv;

pub use deadline::OperationDeadline;
pub use error::{MidgeError, MidgeResult};
pub use singleflight::KeyedGroupCommit;
