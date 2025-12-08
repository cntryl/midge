//! Unified Write Path Module
//!
//! Centralizes all write operations behind a single coordinator that handles:
//! - Sequence number allocation
//! - WAL durability
//! - Memtable updates
//! - Background work signaling (flush/compaction)
//!
//! This module enables:
//! - Single point of control for write semantics
//! - Future group commit and batching optimizations
//! - Clearer error handling for all write paths

pub mod coordinator;

pub use coordinator::WritePathCoordinator;
