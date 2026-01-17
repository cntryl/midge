//! Scheduler — prioritizes and batches work.
//!
//! IMPORTANT (Copilot guidance):
//! - Scheduler does *not* perform per-request routing.
//! - Scheduler does *not* interact with ResponseRouter.
//! - Scheduler only orders and limits concurrent tasks by TaskKind.
//!
//! The EventLoop executes work; scheduler merely selects which task should run next.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};

use super::task::{Task, TaskId, TaskKind};

/// Wrapper for BinaryHeap scheduling.
///
/// NOTE: BinaryHeap is a max-heap — the `Ord` implementation defines
/// which tasks are considered "higher priority."
struct ScheduledTask {
    task: Task,
}

impl PartialEq for ScheduledTask {
    fn eq(&self, other: &Self) -> bool {
        self.task.id == other.task.id
    }
}

impl Eq for ScheduledTask {}

impl PartialOrd for ScheduledTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScheduledTask {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher priority should come first.
        match self.task.priority.cmp(&other.task.priority) {
            Ordering::Equal => {
                // For equal priority tasks, older tasks should run first.
                // BinaryHeap pops the "largest", so we reverse age ordering.
                other.task.created_at.cmp(&self.task.created_at)
            }
            other => other,
        }
    }
}

/// Priority + concurrency-aware scheduler.
pub struct Scheduler {
    queue: BinaryHeap<ScheduledTask>,
    max_concurrent: usize,

    /// Tracks active tasks by kind.
    running: HashMap<TaskKind, usize>,
}

impl Scheduler {
    /// Create a new scheduler.
    pub fn new() -> Self {
        Self {
            queue: BinaryHeap::new(),
            max_concurrent: 4, // Default concurrency limit.
            running: HashMap::new(),
        }
    }

    /// Schedule a task for future execution.
    pub fn schedule(&mut self, task: Task) {
        self.queue.push(ScheduledTask { task });
    }

    /// Get the next task to run, respecting concurrency per TaskKind.
    ///
    /// Removes and returns exactly one schedulable task, or None if none can run.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<Task> {
        let mut deferred = Vec::new();
        let mut selected = None;

        // Pop until we find something runnable or queue is empty.
        while let Some(scheduled) = self.queue.pop() {
            let kind = scheduled.task.kind;
            let running = *self.running.get(&kind).unwrap_or(&0);

            if running < self.max_concurrent {
                // Select this task to execute.
                self.running.insert(kind, running + 1);
                selected = Some(scheduled.task);
                break;
            } else {
                // Can't run this task yet; store it temporarily.
                deferred.push(scheduled);
            }
        }

        // Restore deferred tasks back into the queue.
        for t in deferred {
            self.queue.push(t);
        }

        selected
    }

    /// Mark a completed task, decrementing its concurrency counter.
    pub fn complete(&mut self, _task_id: TaskId, kind: TaskKind) {
        if let Some(count) = self.running.get_mut(&kind) {
            *count = count.saturating_sub(1);
        }
    }

    /// Return number of pending tasks.
    pub fn pending_count(&self) -> usize {
        self.queue.len()
    }

    /// Returns true if any task of any kind is currently active.
    pub fn has_running(&self) -> bool {
        self.running.values().any(|&c| c > 0)
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::task::TaskPriority;
    use std::thread;
    use std::time::Duration;

    // =========== Scheduler Creation Tests ===========

    #[test]
    fn should_create_scheduler() {
        // Arrange
        // (no setup)

        // Act
        let scheduler = Scheduler::new();

        // Assert
        assert_eq!(scheduler.pending_count(), 0);
        assert!(!scheduler.has_running());
        assert_eq!(scheduler.max_concurrent, 4);
    }

    #[test]
    fn should_create_scheduler_with_default() {
        // Arrange
        // (no setup)

        // Act
        let scheduler = Scheduler::default();

        // Assert
        assert_eq!(scheduler.pending_count(), 0);
    }

    // =========== Task Scheduling Tests ===========

    #[test]
    fn should_schedule_single_task() {
        // Arrange
        let mut scheduler = Scheduler::new();
        let task = Task::new(TaskKind::Flush, "flush task");

        // Act
        scheduler.schedule(task);

        // Assert
        assert_eq!(scheduler.pending_count(), 1);
    }

    #[test]
    fn should_schedule_multiple_tasks() {
        // Arrange
        let mut scheduler = Scheduler::new();

        // Act
        for i in 0..10 {
            let task = Task::new(TaskKind::Flush, format!("task_{}", i));
            scheduler.schedule(task);
        }

        // Assert
        assert_eq!(scheduler.pending_count(), 10);
    }

    // =========== Task Selection Tests ===========

    #[test]
    fn should_get_next_task_when_available() {
        // Arrange
        let mut scheduler = Scheduler::new();
        let task = Task::new(TaskKind::Flush, "flush");
        scheduler.schedule(task);

        // Act
        let next = scheduler.next();

        // Assert
        assert!(next.is_some());
        assert_eq!(scheduler.pending_count(), 0);
    }

    #[test]
    fn should_return_none_when_no_tasks_available() {
        // Arrange
        let mut scheduler = Scheduler::new();

        // Act
        let next = scheduler.next();

        // Assert
        assert!(next.is_none());
    }

    #[test]
    fn should_respect_priority_ordering() {
        // Arrange
        let mut scheduler = Scheduler::new();
        let low = Task::new(TaskKind::Flush, "low").with_priority(TaskPriority::Low);
        let high = Task::new(TaskKind::Flush, "high").with_priority(TaskPriority::High);
        let normal = Task::new(TaskKind::Flush, "normal").with_priority(TaskPriority::Normal);

        // Act - Schedule in mixed order
        scheduler.schedule(low);
        scheduler.schedule(high);
        scheduler.schedule(normal);

        // Assert - Should pop in priority order (high, normal, low)
        let task1 = scheduler.next().unwrap();
        assert_eq!(task1.priority, TaskPriority::High);
        assert_eq!(task1.description, "high");

        let task2 = scheduler.next().unwrap();
        assert_eq!(task2.priority, TaskPriority::Normal);

        let task3 = scheduler.next().unwrap();
        assert_eq!(task3.priority, TaskPriority::Low);
    }

    #[test]
    fn should_enforce_fifo_within_same_priority() {
        // Arrange
        let mut scheduler = Scheduler::new();

        // Act - Schedule tasks with same priority in order: 1, 2, 3
        let task1 = Task::new(TaskKind::Flush, "first").with_priority(TaskPriority::Normal);
        let task2 = Task::new(TaskKind::Flush, "second").with_priority(TaskPriority::Normal);
        let task3 = Task::new(TaskKind::Flush, "third").with_priority(TaskPriority::Normal);

        let id1 = task1.id;
        let id2 = task2.id;
        let id3 = task3.id;

        scheduler.schedule(task1);
        // Small delay to ensure different created_at times
        thread::sleep(Duration::from_millis(1));
        scheduler.schedule(task2);
        thread::sleep(Duration::from_millis(1));
        scheduler.schedule(task3);

        // Assert - Should pop in FIFO order (oldest first)
        assert_eq!(scheduler.next().unwrap().id, id1);
        assert_eq!(scheduler.next().unwrap().id, id2);
        assert_eq!(scheduler.next().unwrap().id, id3);
    }

    #[test]
    fn should_limit_concurrent_tasks_per_kind() {
        // Arrange
        let mut scheduler = Scheduler::new();

        // Act - Schedule 4 Flush tasks and 4 Compaction tasks
        for i in 0..4 {
            let task = Task::new(TaskKind::Flush, format!("flush_{}", i));
            scheduler.schedule(task);
        }
        for i in 0..4 {
            let task = Task::new(TaskKind::Compaction, format!("compact_{}", i));
            scheduler.schedule(task);
        }

        // Assert - Should be able to get 8 tasks total but respecting per-kind limits
        for _ in 0..4 {
            let task = scheduler.next();
            assert!(task.is_some());
        }

        // After 4 tasks, no more should be available (at limit)
        assert_eq!(scheduler.pending_count(), 4);
        assert!(scheduler.has_running());
    }

    #[test]
    fn should_not_exceed_max_concurrent_per_kind() {
        // Arrange
        let mut scheduler = Scheduler::new();

        // Schedule 8 Flush tasks (exceeds default max of 4)
        for i in 0..8 {
            let task = Task::new(TaskKind::Flush, format!("flush_{}", i));
            scheduler.schedule(task);
        }

        // Act - Try to get all 8 at once
        let task1 = scheduler.next();
        let task2 = scheduler.next();
        let task3 = scheduler.next();
        let task4 = scheduler.next();
        let task5 = scheduler.next();

        // Assert - Should only get 4 at max
        assert!(task1.is_some());
        assert!(task2.is_some());
        assert!(task3.is_some());
        assert!(task4.is_some());
        assert!(task5.is_none()); // 5th blocked by concurrency limit
    }

    #[test]
    fn should_allow_different_kinds_to_run_concurrently() {
        // Arrange
        let mut scheduler = Scheduler::new();

        // Schedule tasks of different kinds
        let flush_task = Task::new(TaskKind::Flush, "flush");
        let compact_task = Task::new(TaskKind::Compaction, "compact");
        let wal_task = Task::new(TaskKind::Wal, "wal");

        scheduler.schedule(flush_task);
        scheduler.schedule(compact_task);
        scheduler.schedule(wal_task);

        // Act - Get all three
        let t1 = scheduler.next();
        let t2 = scheduler.next();
        let t3 = scheduler.next();

        // Assert - All should be available (different kinds don't block each other)
        assert!(t1.is_some());
        assert!(t2.is_some());
        assert!(t3.is_some());
        assert_eq!(scheduler.pending_count(), 0);
    }

    // =========== Task Completion Tests ===========

    #[test]
    fn should_mark_task_as_complete() {
        // Arrange
        let mut scheduler = Scheduler::new();
        let task = Task::new(TaskKind::Flush, "flush");
        let task_id = task.id;
        scheduler.schedule(task);

        // Act - Get and complete task
        let got = scheduler.next();
        assert!(got.is_some());
        scheduler.complete(task_id, TaskKind::Flush);

        // Assert - Now should be able to get more Flush tasks
        let flush2 = Task::new(TaskKind::Flush, "flush2");
        scheduler.schedule(flush2);
        assert!(scheduler.next().is_some());
    }

    #[test]
    fn should_decrement_running_counter_after_single_completion() {
        // Arrange
        let mut scheduler = Scheduler::new();

        // Schedule and get 2 flush tasks
        for i in 0..2 {
            let task = Task::new(TaskKind::Flush, format!("flush_{}", i));
            scheduler.schedule(task);
        }

        let t1 = scheduler.next().unwrap();
        let _t2 = scheduler.next().unwrap();
        assert!(scheduler.has_running());

        // Act
        scheduler.complete(t1.id, TaskKind::Flush);

        // Assert - Still has 1 running
        assert!(scheduler.has_running());
    }

    #[test]
    fn should_decrement_running_counter_to_zero_after_completing_all() {
        // Arrange
        let mut scheduler = Scheduler::new();

        // Schedule and get 2 flush tasks
        for i in 0..2 {
            let task = Task::new(TaskKind::Flush, format!("flush_{}", i));
            scheduler.schedule(task);
        }

        let t1 = scheduler.next().unwrap();
        let t2 = scheduler.next().unwrap();
        assert!(scheduler.has_running());

        // Act
        scheduler.complete(t1.id, TaskKind::Flush);
        scheduler.complete(t2.id, TaskKind::Flush);

        // Assert - No longer has running
        assert!(!scheduler.has_running());
    }

    #[test]
    fn should_saturate_subtract_on_complete() {
        // Arrange
        let mut scheduler = Scheduler::new();

        // Act - Complete a task that was never started (simulates underflow)
        scheduler.complete(TaskId::new(), TaskKind::Flush);

        // Assert - Should not panic, running count should be 0
        assert!(!scheduler.has_running());
    }

    #[test]
    fn should_handle_completing_different_kinds() {
        // Arrange
        let mut scheduler = Scheduler::new();

        // Schedule and get tasks of different kinds
        let flush = Task::new(TaskKind::Flush, "flush");
        let compact = Task::new(TaskKind::Compaction, "compact");

        let flush_id = flush.id;
        let compact_id = compact.id;

        scheduler.schedule(flush);
        scheduler.schedule(compact);

        let f = scheduler.next().unwrap();
        let c = scheduler.next().unwrap();

        assert_eq!(f.id, flush_id);
        assert_eq!(c.id, compact_id);

        // Act - Complete only flush
        scheduler.complete(flush_id, TaskKind::Flush);

        // Assert - Can now schedule more flush but compact is still running
        let flush2 = Task::new(TaskKind::Flush, "flush2");
        scheduler.schedule(flush2);
        assert!(scheduler.next().is_some()); // flush2 should be available
    }

    // =========== Pending Count Tests ===========

    #[test]
    fn should_track_pending_count_accurately() {
        // Arrange
        let mut scheduler = Scheduler::new();

        // Act
        // (none)

        // Assert
        assert_eq!(scheduler.pending_count(), 0);

        scheduler.schedule(Task::new(TaskKind::Flush, "t1"));
        assert_eq!(scheduler.pending_count(), 1);

        scheduler.schedule(Task::new(TaskKind::Flush, "t2"));
        assert_eq!(scheduler.pending_count(), 2);

        scheduler.next();
        assert_eq!(scheduler.pending_count(), 1);

        scheduler.next();
        assert_eq!(scheduler.pending_count(), 0);
    }

    // =========== Running Tasks Tests ===========

    #[test]
    fn should_track_running_tasks() {
        // Arrange
        let mut scheduler = Scheduler::new();

        // Act
        // (none)

        // Assert
        assert!(!scheduler.has_running());

        scheduler.schedule(Task::new(TaskKind::Flush, "flush"));
        scheduler.next();

        assert!(scheduler.has_running());
    }

    #[test]
    fn should_return_false_when_no_running_tasks() {
        // Arrange
        let mut scheduler = Scheduler::new();

        // Act
        // (none)

        // Assert
        assert!(!scheduler.has_running());

        scheduler.schedule(Task::new(TaskKind::Flush, "flush"));
        let task = scheduler.next();
        scheduler.complete(task.unwrap().id, TaskKind::Flush);

        assert!(!scheduler.has_running());
    }

    // =========== Edge Cases ===========

    #[test]
    fn should_handle_critical_priority() {
        // Arrange
        let mut scheduler = Scheduler::new();

        let normal = Task::new(TaskKind::Flush, "normal").with_priority(TaskPriority::Normal);
        let critical = Task::new(TaskKind::Flush, "critical").with_priority(TaskPriority::Critical);

        scheduler.schedule(normal);
        scheduler.schedule(critical);

        // Act
        let first = scheduler.next().unwrap();

        // Assert
        assert_eq!(first.priority, TaskPriority::Critical);
    }

    #[test]
    fn should_handle_mixed_kinds_priorities() {
        // Arrange
        let mut scheduler = Scheduler::new();

        // Schedule: low flush, high compaction, normal wal
        scheduler.schedule(Task::new(TaskKind::Flush, "flush").with_priority(TaskPriority::Low));
        scheduler
            .schedule(Task::new(TaskKind::Compaction, "compact").with_priority(TaskPriority::High));
        scheduler.schedule(Task::new(TaskKind::Wal, "wal").with_priority(TaskPriority::Normal));

        // Act - All should be available (different kinds)
        let t1 = scheduler.next();
        let t2 = scheduler.next();
        let t3 = scheduler.next();

        // Assert
        assert!(t1.is_some());
        assert!(t2.is_some());
        assert!(t3.is_some());
        assert_eq!(scheduler.pending_count(), 0);
    }

    #[test]
    fn should_handle_empty_schedule() {
        // Arrange
        let mut scheduler = Scheduler::new();

        // Act
        // (none)

        // Assert - Multiple calls to next on empty scheduler
        assert!(scheduler.next().is_none());
        assert!(scheduler.next().is_none());
        assert!(scheduler.next().is_none());
        assert_eq!(scheduler.pending_count(), 0);
    }
}
