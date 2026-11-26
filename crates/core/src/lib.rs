//! Foundational building blocks shared by Midge crates.

#![cfg_attr(not(test), deny(clippy::unwrap_used))]

pub mod cloud_layout;
pub mod codec;
pub mod common;
pub mod failpoints;
pub mod io;
pub mod telemetry;
pub mod types;

#[cfg(any(test, feature = "internal-testing"))]
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
