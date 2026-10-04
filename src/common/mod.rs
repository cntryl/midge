//! Foundational types and traits - zero external dependencies

pub mod deadline;
mod deadline_scope;
pub mod error;
#[doc(hidden)]
pub mod resource_budget;
pub mod time;

pub use deadline::OperationDeadline;
pub(crate) use deadline_scope::DeadlineScope;
pub(crate) use error::is_no_space;
pub use error::{MidgeError, MidgeResult, Severity};
