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
