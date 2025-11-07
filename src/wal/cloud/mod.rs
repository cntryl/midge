//! Cloud-backed WAL implementation.
//!
//! Provides WAL storage using cloud blob storage with:
//! - **Segment-based uploads** for efficient batching (16-64 MB configurable)
//! - **Bincode serialization** for compact storage
//! - **Durability promises** with std::thread async uploads (no tokio dependency)
//! - **Automatic buffering** to prevent "death by tiny files"
//!
//! # Architecture
//!
//! ## CloudWalWriter
//!
//! Buffers WAL records into segments before uploading to cloud storage.
//! Automatically flushes segments when they reach the configured size.
//!
//! - `append_*()`: Non-blocking, buffers locally
//! - `flush()`: Triggers async upload (non-blocking)
//! - `sync()`: Flushes and waits for all uploads (blocking, durability guarantee)
//!
//! ## WalBatchManager
//!
//! Manages segment lifecycle, uploads, and durability tracking:
//! - Accumulates records into segments
//! - Spawns background threads for cloud uploads (std::thread, no tokio)
//! - Tracks durability promises for sync() semantics
//! - Rotates segments at configurable size threshold
//!
//! ## CloudWalReader
//!
//! Downloads and replays WAL segments from cloud storage:
//! - Lists available segments
//! - Downloads specific segments by ID
//! - Provides replay iterator for crash recovery
//! - Supports incremental replay from checkpoints
//!
//! ## DurabilityPromise
//!
//! Promise that allows callers to wait for upload completion using
//! parking_lot::Condvar. Enables sync() to block until all pending
//! uploads complete, providing durability guarantees without tokio.
//!
//! # Durability Guarantees
//!
//! Per the configuration spec:
//!
//! - **flush()**: Triggers upload but returns immediately (best-effort)
//! - **sync()**: Blocks until all pending uploads complete (strict durability)
//! - **close()**: Calls sync() to ensure no data loss
//!
//! Segment size is tunable (default 16-64 MB) to balance upload overhead
//! vs. durability lag.
//!
//! # Example
//!
//! ```no_run
//! use midge::wal::cloud::{CloudWalWriter, WalBatchManager};
//! use midge::cloud::MockCloudBackend;
//! use midge::wal::WalWriter;
//! use std::sync::Arc;
//!
//! let backend = Arc::new(MockCloudBackend::new());
//! let batch_size = 16 * 1024 * 1024; // 16 MB segments
//! let writer = CloudWalWriter::new(backend, batch_size, None, None);
//!
//! // Append operations (buffered)
//! writer.append_op(midge::wal::WalOpKind::Put, b"key", Some(b"value")).unwrap();
//!
//! // Ensure durability
//! writer.sync().unwrap();
//! ```

mod reader;
mod shared;
mod writer;

pub use reader::CloudWalReader;
pub use shared::{CloudStorageBackend, DurabilityPromise, WalBatchManager, WalSegment};
pub use writer::CloudWalWriter;
