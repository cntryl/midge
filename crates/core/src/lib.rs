//! Foundational building blocks shared by Midge crates.

#![cfg_attr(not(test), deny(clippy::unwrap_used))]

#[doc(hidden)]
pub mod cloud_layout;
#[doc(hidden)]
pub mod codec;
#[doc(hidden)]
pub mod common;
#[doc(hidden)]
pub mod failpoints;
#[doc(hidden)]
pub mod io;
#[doc(hidden)]
pub mod telemetry;
#[doc(hidden)]
pub mod types;

#[cfg(any(test, feature = "internal-testing"))]
#[doc(hidden)]
pub mod simulation;

pub use cloud_layout::CloudObjectLayout;
pub use common::{
    Clock, DeadlineScope, MidgeError, MidgeResult, OperationDeadline, Severity, SystemClock,
    DEFAULT_STORAGE_IO_TIMEOUT,
};
pub use types::{
    ColumnFamilyId, ConflictPolicy, DurabilityFrontiersSnapshot, EngineHealth, EntryType, KeyState,
    RangeTombstone, SharedGenerationId,
};
