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
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    // =========== TaskId Tests ===========

    #[test]
    fn should_generate_unique_task_ids() {
        // Arrange & Act
        let id1 = TaskId::new();
        let id2 = TaskId::new();
        let id3 = TaskId::new();

        // Assert
        assert_ne!(id1, id2);
        assert_ne!(id2, id3);
        assert_ne!(id1, id3);
    }

    #[test]
    fn should_increment_task_ids_monotonically() {
        // Arrange & Act
        let id1 = TaskId::new();
        let id2 = TaskId::new();
        let id3 = TaskId::new();

        // Assert
        assert!(id1.0 < id2.0);
        assert!(id2.0 < id3.0);
    }

    #[test]
    fn should_create_task_id_from_default() {
        // Arrange & Act
        let id1 = TaskId::default();
        let id2 = TaskId::default();

        // Assert
        assert_ne!(id1, id2);
    }

    #[test]
    fn should_generate_unique_ids_across_threads() {
        // Arrange
        let handles: Vec<_> = (0..5)
            .map(|_| {
                thread::spawn(|| {
                    let mut ids = vec![];
                    for _ in 0..20 {
                        ids.push(TaskId::new());
                    }
                    ids
                })
            })
            .collect();

        // Act
        let all_ids: Vec<TaskId> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();

        // Assert - All IDs should be unique
        for i in 0..all_ids.len() {
            for j in (i + 1)..all_ids.len() {
                assert_ne!(all_ids[i], all_ids[j]);
            }
        }
    }

    #[test]
    fn should_allow_task_id_cloning() {
        // Arrange
        let id = TaskId::new();

        // Act
        let cloned = id;

        // Assert
        assert_eq!(id, cloned);
    }

    #[test]
    fn should_hash_task_ids() {
        // Arrange
        use std::collections::HashSet;
        let mut set = HashSet::new();

        // Act
        let id1 = TaskId::new();
        let id2 = TaskId::new();
        set.insert(id1);
        set.insert(id2);

        // Assert
        assert_eq!(set.len(), 2);
    }

    // =========== TaskPriority Tests ===========

    #[test]
    fn should_have_four_priority_levels() {
        // Assert
        assert!(TaskPriority::Low < TaskPriority::Normal);
        assert!(TaskPriority::Normal < TaskPriority::High);
        assert!(TaskPriority::High < TaskPriority::Critical);
    }

    #[test]
    fn should_order_priorities_correctly() {
        // Arrange
        let priorities = vec![
            TaskPriority::Critical,
            TaskPriority::Low,
            TaskPriority::High,
            TaskPriority::Normal,
        ];

        // Act
        let mut sorted = priorities.clone();
        sorted.sort();

        // Assert
        assert_eq!(sorted[0], TaskPriority::Low);
        assert_eq!(sorted[1], TaskPriority::Normal);
        assert_eq!(sorted[2], TaskPriority::High);
        assert_eq!(sorted[3], TaskPriority::Critical);
    }

    #[test]
    fn should_default_to_normal_priority() {
        // Act
        let default_priority = TaskPriority::default();

        // Assert
        assert_eq!(default_priority, TaskPriority::Normal);
    }

    #[test]
    fn should_support_priority_comparison() {
        // Act & Assert
        assert_eq!(TaskPriority::Low, TaskPriority::Low);
        assert_ne!(TaskPriority::Low, TaskPriority::High);
        assert!(TaskPriority::High < TaskPriority::Critical);
    }

    // =========== TaskKind Tests ===========

    #[test]
    fn should_have_all_task_kinds() {
        // Arrange & Act
        let kinds = [
            TaskKind::Flush,
            TaskKind::Compaction,
            TaskKind::Wal,
            TaskKind::Cloud,
            TaskKind::Gc,
            TaskKind::Manifest,
            TaskKind::User,
        ];

        // Assert
        assert_eq!(kinds.len(), 7);
    }

    #[test]
    fn should_compare_task_kinds() {
        // Assert
        assert_eq!(TaskKind::Flush, TaskKind::Flush);
        assert_ne!(TaskKind::Flush, TaskKind::Compaction);
    }

    #[test]
    fn should_hash_task_kinds() {
        // Arrange
        use std::collections::HashSet;
        let mut set = HashSet::new();

        // Act
        set.insert(TaskKind::Flush);
        set.insert(TaskKind::Compaction);
        set.insert(TaskKind::Flush); // Duplicate

        // Assert
        assert_eq!(set.len(), 2); // Only 2 unique kinds
    }

    // =========== Task Tests ===========

    #[test]
    fn should_create_task_with_default_priority() {
        // Arrange & Act
        let task = Task::new(TaskKind::Flush, "test task");

        // Assert
        assert_eq!(task.kind, TaskKind::Flush);
        assert_eq!(task.priority, TaskPriority::Normal);
        assert_eq!(task.description, "test task");
    }

    #[test]
    fn should_create_task_with_custom_priority() {
        // Arrange & Act
        let task = Task::new(TaskKind::Compaction, "compact").with_priority(TaskPriority::High);

        // Assert
        assert_eq!(task.kind, TaskKind::Compaction);
        assert_eq!(task.priority, TaskPriority::High);
    }

    #[test]
    fn should_generate_unique_task_id_per_task() {
        // Arrange & Act
        let task1 = Task::new(TaskKind::Flush, "task1");
        let task2 = Task::new(TaskKind::Flush, "task2");

        // Assert
        assert_ne!(task1.id, task2.id);
    }

    #[test]
    fn should_preserve_task_description() {
        // Arrange & Act
        let description = "flush memtable for cf_0";
        let task = Task::new(TaskKind::Flush, description);

        // Assert
        assert_eq!(task.description, description);
    }

    #[test]
    fn should_record_task_creation_time() {
        // Arrange
        let before = Instant::now();

        // Act
        let task = Task::new(TaskKind::Wal, "wal sync");
        let after = Instant::now();

        // Assert
        assert!(task.created_at >= before);
        assert!(task.created_at <= after);
    }

    #[test]
    fn should_support_priority_chaining() {
        // Arrange & Act
        let task = Task::new(TaskKind::Cloud, "upload").with_priority(TaskPriority::Critical);

        // Assert
        assert_eq!(task.priority, TaskPriority::Critical);
        assert_eq!(task.kind, TaskKind::Cloud);
    }

    #[test]
    fn should_create_tasks_with_all_kinds() {
        // Arrange & Act
        let tasks = vec![
            Task::new(TaskKind::Flush, "flush"),
            Task::new(TaskKind::Compaction, "compact"),
            Task::new(TaskKind::Wal, "wal"),
            Task::new(TaskKind::Cloud, "cloud"),
            Task::new(TaskKind::Gc, "gc"),
            Task::new(TaskKind::Manifest, "manifest"),
            Task::new(TaskKind::User, "user"),
        ];

        // Assert
        assert_eq!(tasks.len(), 7);
        for task in &tasks {
            assert!(task.created_at <= Instant::now());
        }
    }

    #[test]
    fn should_create_tasks_with_all_priorities() {
        // Arrange & Act
        let tasks = [
            Task::new(TaskKind::Flush, "t1").with_priority(TaskPriority::Low),
            Task::new(TaskKind::Flush, "t2").with_priority(TaskPriority::Normal),
            Task::new(TaskKind::Flush, "t3").with_priority(TaskPriority::High),
            Task::new(TaskKind::Flush, "t4").with_priority(TaskPriority::Critical),
        ];

        // Assert
        assert_eq!(tasks[0].priority, TaskPriority::Low);
        assert_eq!(tasks[1].priority, TaskPriority::Normal);
        assert_eq!(tasks[2].priority, TaskPriority::High);
        assert_eq!(tasks[3].priority, TaskPriority::Critical);
    }

    #[test]
    fn should_preserve_task_state_immutably() {
        // Arrange
        let task = Task::new(TaskKind::Flush, "immutable").with_priority(TaskPriority::High);
        let original_id = task.id;
        let original_kind = task.kind;
        let original_priority = task.priority;

        // Act & Assert - Fields remain unchanged
        assert_eq!(task.id, original_id);
        assert_eq!(task.kind, original_kind);
        assert_eq!(task.priority, original_priority);
    }

    #[test]
    fn should_handle_empty_description() {
        // Arrange & Act
        let task = Task::new(TaskKind::Wal, "");

        // Assert
        assert_eq!(task.description, "");
    }

    #[test]
    fn should_handle_long_description() {
        // Arrange
        let long_desc = "a".repeat(1000);

        // Act
        let task = Task::new(TaskKind::Manifest, &long_desc);

        // Assert
        assert_eq!(task.description.len(), 1000);
    }
}
