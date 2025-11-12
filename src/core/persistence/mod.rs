//! Persistence layer for WAL and flushing subsystems.
//!
//! This module coordinates the write-ahead log (WAL) and background flushing
//! of memtables to SST files, forming the persistence backbone of the LSM tree.

pub mod flush;
pub mod flush_coordinator;
pub mod wal_replay;

pub use flush::{FlushJob, FlushWorker, FlushWorkerConfig};
pub use flush_coordinator::FlushCoordinator;
pub use wal_replay::WalReplayIterator;
