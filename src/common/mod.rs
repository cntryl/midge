//! Foundational types and traits - zero external dependencies

pub mod deadline;
mod deadline_scope;
pub mod error;
pub(crate) mod latency_histogram;
#[doc(hidden)]
pub mod resource_budget;
pub mod time;
pub(crate) mod write_rate;

pub use deadline::OperationDeadline;
pub(crate) use deadline_scope::DeadlineScope;
pub(crate) use error::is_no_space;
pub use error::{MidgeError, MidgeResult, Severity};
pub use latency_histogram::LatencySummary;
