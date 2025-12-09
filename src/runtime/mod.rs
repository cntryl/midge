//! Runtime - background task execution
//!
//! Actor-based task coordination for compaction, flushing, cloud ops, etc.

pub mod event_loop;
pub mod state;
pub mod task;
pub mod scheduler;
pub mod dispatch;
pub mod actors;

pub use event_loop::EventLoop;
pub use state::State;
pub use task::Task;
pub use scheduler::Scheduler;
pub use dispatch::Dispatcher;
pub use actors::{FlushActor, CompactionActor, WalActor, CloudActor, GcActor, ManifestActor};

use crate::common::MidgeResult;

/// Runtime task
pub enum RuntimeTask {
    Flush,
    Compact,
    WalSync,
    CloudUpload,
}

/// Main runtime for background operations
pub struct Runtime {
    // Will be populated in backfill phase
}

impl Runtime {
    pub fn new() -> MidgeResult<Self> {
        Ok(Self {})
    }

    pub fn submit_task(&mut self, _task: RuntimeTask) -> MidgeResult<()> {
        todo!("Implement task submission")
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new().expect("Failed to create default runtime")
    }
}
