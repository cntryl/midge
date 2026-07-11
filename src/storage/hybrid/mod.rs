//! Hybrid storage with cloud integration and disk pressure management
//!
//! Provides:
//! - **`HybridStorage`**: Combines local and cloud backends with bounded object I/O
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
//! # Storage Roles
//!
//! `HybridStorage` manages:
//! 1. **Object Storage**: keyed bytes via `submit_read/write/delete/list`
//! 2. **Queued Durability**: explicit keys via `enqueue_object_upload()` + `process_uploads()`
//!
//! The runtime maps WAL/SST names, validates formats, and proves manifest coverage. Storage never interprets those formats.
//!
//! # Usage

pub mod actor;
pub mod backend;
pub mod policy;
pub mod state;
