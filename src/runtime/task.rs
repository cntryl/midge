//! Task definitions for the runtime scheduler
//!
//! Tasks represent units of work that can be scheduled and prioritized.

use std::sync::atomic::{AtomicU64, Ordering};

/// Unique task identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskId(pub u64);

impl TaskId {
    /// Generate a new unique task ID
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

/// Task priority levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TaskPriority {
    /// Lowest priority - background maintenance
    Low = 0,
    /// Normal priority - regular operations
    Normal = 1,
    /// High priority - user-initiated operations
    High = 2,
    /// Critical priority - must complete ASAP
    Critical = 3,
}

impl Default for TaskPriority {
    fn default() -> Self {
        Self::Normal
    }
}

/// Kind of task for instrumentation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskKind {
    /// Memtable flush to SST
    Flush,
    /// SST compaction
    Compaction,
    /// WAL operations (append, sync, rotate)
    Wal,
    /// Cloud upload/download
    Cloud,
    /// Garbage collection
    Gc,
    /// Manifest updates
    Manifest,
    /// User-initiated operation
    User,
}

/// A scheduled task in the runtime
#[derive(Debug)]
pub struct Task {
    /// Unique identifier
    pub id: TaskId,
    /// Kind of task
    pub kind: TaskKind,
    /// Priority level
    pub priority: TaskPriority,
    /// Human-readable description
    pub description: String,
    /// Creation timestamp (for ordering)
    pub created_at: std::time::Instant,
}

impl Task {
    /// Create a new task
    pub fn new(kind: TaskKind, description: impl Into<String>) -> Self {
        Self {
            id: TaskId::new(),
            kind,
            priority: TaskPriority::default(),
            description: description.into(),
            created_at: std::time::Instant::now(),
        }
    }

    /// Set the task priority
    pub fn with_priority(mut self, priority: TaskPriority) -> Self {
        self.priority = priority;
        self
    }
}