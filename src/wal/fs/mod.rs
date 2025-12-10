//! Filesystem-backed Write-Ahead Log (WAL)
//!
//! This module exposes the concrete WAL factory, reader, and writer
//! implementations used by the runtime. The WAL subsystem is responsible for:
//!   - Durable write-ahead logging of all mutations
//!   - Sequencing and ordering guarantees
//!   - Replay support for crash recovery (via `wal::recovery`)
//!
//! Architectural rules for Copilot:
//! --------------------------------
//! - **Do NOT introduce new WAL entry formats here.**
//! - **Do NOT add sequencing logic.** Sequence ownership lives in
//!     `RuntimeState` and is assigned inside `WalActor::append`.
//! - **Do NOT modify read/write APIs.** They are consumed by the runtime
//!     actors, not by the engine.
//! - **Do NOT add async or background threads in this module.**
//!
//! Files:
//!   - `factory.rs` — creates FsWalReader + FsWalWriter pairs.
//!   - `reader.rs`  — low-level sequential WAL record reader.
//!   - `writer.rs`  — low-level WAL appender. Does *not* manage sequences.
//!
//! Higher-level WAL behavior (rotation, sync, recovery, sequencing) is handled
//! by the runtime actors, not by the types in this module.

mod factory;
mod reader;
mod writer;

pub use factory::FsWalFactory;
pub use reader::FsWalReader;
pub use writer::FsWalWriter;
