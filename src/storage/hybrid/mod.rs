//! Hybrid storage with cloud integration and disk pressure management
//!
//! Provides:
//! - **`HybridStorage`**: Combines local and cloud backends with WAL durability
//! - **`StorageBudgetActor`**: Tracks disk usage and enforces watermarks
//! - **`StorageBudgetPolicy`**: Watermark thresholds and eviction strategy
//! - **`DiskState`**: Disk usage accounting
//!
//! # Architecture
//!
//! - **backend.rs**: `HybridStorage` - main coordinator for local + cloud storage
//! - **actor.rs**: `StorageBudgetActor` - disk space management
//! - **policy.rs**: `StorageBudgetPolicy` - watermark thresholds
//! - **state.rs**: `DiskState` - disk accounting
//!
//! # Two Storage Roles
//!
//! `HybridStorage` manages:
//! 1. **Object Storage**: SSTs via `submit_read/write/delete/list`
//! 2. **WAL Durability**: Segments via `enqueue_wal_segment()` + `process_uploads()`
//!
//! WAL Flow: local write → `enqueue_wal_segment()` → `process_uploads()` → `CloudAck`
//!
//! # Usage

pub mod actor;
pub mod backend;
pub mod policy;
pub mod state;
