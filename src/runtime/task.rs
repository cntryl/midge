//! Task definitions for the runtime scheduler
//!
//! IMPORTANT (Copilot guidance):
//! - Tasks are *not* request/response messages.
//! - Tasks do *not* carry request_id information.
//! - Tasks are scheduling units internal to the runtime.
//! - Response routing is done exclusively by ResponseRouter, not here.
//!
//! A Task represents a unit of background work chosen by Scheduler and
//! executed by EventLoop. Think of Tasks as "planned actor work", not
//! messages sent by clients.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Unique task identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskId(pub u64);

impl TaskId {
    /// Generate a new unique task ID.
    pub fn new() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        Self(COUNTER.fetch_add(1, Ordering::SeqCst))
    }
}

impl Default for TaskId {
    fn default() -> Self {
        Self::new()
    }
}

/// Task priority levels.
///
/// Copilot: DO NOT add "request" terminology here.
/// Priorities are scheduling hints, not message routing traits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[derive(Default)]
pub enum TaskPriority {
    /// Lowest priority — background maintenance.
    Low = 0,
    /// Normal priority — regular operations.
    #[default]
    Normal = 1,
    /// High priority — user-initiated operations.
    High = 2,
    /// Critical — must complete ASAP (e.g., WAL rotate).
    Critical = 3,
}


/// Category of task, used by Scheduler to apply concurrency limits.
///
/// Copilot: This is NOT actor routing and MUST NOT include request_id logic.
/// It simply tells Scheduler which bucket the task belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskKind {
    /// Memtable flush to SST.
    Flush,

    /// SST compaction.
    Compaction,

    /// WAL operations (append, sync, rotate).
    Wal,

    /// Cloud upload/download.
    Cloud,

    /// Garbage collection.
    Gc,

    /// Manifest updates.
    Manifest,

    /// User-initiated operation (e.g., read, control messages).
    User,
}

/// A scheduled unit of work.
///
/// Tasks are enqueued and later selected by Scheduler, not sent as messages.
/// EventLoop runs a task by executing its associated actor logic.
#[derive(Debug)]
pub struct Task {
    /// Globally unique task identifier.
    pub id: TaskId,

    /// Kind of work (flush, compaction, etc.).
    pub kind: TaskKind,

    /// Priority hint used for ordering by Scheduler.
    pub priority: TaskPriority,

    /// Human-readable description (for debugging / tracing).
    pub description: String,

    /// Creation timestamp, used for FIFO ordering among equal priorities.
    pub created_at: Instant,
}

impl Task {
    /// Create a new task with default priority.
    pub fn new(kind: TaskKind, description: impl Into<String>) -> Self {
        Self {
            id: TaskId::new(),
            kind,
            priority: TaskPriority::default(),
            description: description.into(),
            created_at: Instant::now(),
        }
    }

    /// Same as `new` but explicitly sets priority.
    pub fn with_priority(mut self, priority: TaskPriority) -> Self {
        self.priority = priority;
        self
    }
}
