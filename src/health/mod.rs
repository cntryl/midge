//! Health and lifecycle management for safe deployments.
//!
//! This module provides probes and coordination for blue-green deployments:
//! - Lifecycle state tracking (Starting → Ready → Draining → Sealed)
//! - Rehydration progress monitoring
//! - Graceful drain coordination
//! - State validation against cloud

pub mod monitor;
pub mod rehydration;
pub mod state;

pub use monitor::{HealthConfig, HealthMonitor};
pub use rehydration::{RehydrationProgress, RehydrationStatus};
pub use state::{DrainResult, LifecycleState, ReadinessStatus, SyncpointStatus, ValidationResult};
