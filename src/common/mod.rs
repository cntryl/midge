//! Foundational types and traits - zero external dependencies

pub mod deadline;
pub mod error;
pub mod keyed_group_commit;
#[doc(hidden)]
pub mod resource_budget;
pub mod time;
pub mod tlv;

pub use deadline::OperationDeadline;
pub use error::{MidgeError, MidgeResult, Severity};
pub use keyed_group_commit::KeyedGroupCommit;
