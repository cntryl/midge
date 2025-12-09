//! Persistence layer for WAL and flushing subsystems.
//!
//! This module coordinates the write-ahead log (WAL) and background flushing
//! of memtables to SST files, forming the persistence backbone of the LSM tree.
//!
//! # Flush Subsystem Structure
//!
//! The flush module is organized into focused submodules:
//! - **`flush::worker`**: Background flush worker thread management
//! - **`flush::process`**: Core flush job processing and WAL pruning
//! - **`flush::bounds`**: Key bounds computation and synchronous flush
//! - **`flush::stats`**: Flush statistics and metrics helpers
//! - **`flush::traits`**: FlushOutput trait for extensible notifications

pub mod flush;
pub mod flush_coordinator;
pub mod wal_replay;

pub use flush::{FlushJob, FlushWorker, FlushWorkerConfig};
pub use flush_coordinator::FlushCoordinator;
pub use wal_replay::WalReplayIterator;
