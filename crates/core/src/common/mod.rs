//! Foundational types and traits - zero external dependencies

pub mod deadline;
mod deadline_scope;
pub mod error;
pub mod latency_histogram;
#[doc(hidden)]
pub mod resource_budget;
pub mod time;
pub mod write_rate;

pub use deadline::OperationDeadline;
/// Default timeout for one filesystem/provider operation.
pub const DEFAULT_STORAGE_IO_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
pub use deadline_scope::DeadlineScope;
pub(crate) use error::is_no_space;
pub use error::{MidgeError, MidgeResult, Severity};
pub use latency_histogram::LatencySummary;
pub use time::{Clock, ObservedClock, SystemClock};
