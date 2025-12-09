//! Scheduler - prioritizes and batches work
//!
//! Orders tasks by priority and batches related operations for efficiency.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use super::task::{Task, TaskId, TaskKind};

/// Scheduled task wrapper for priority queue
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
        // Higher priority first, then older tasks first
        match self.task.priority.cmp(&other.task.priority) {
            Ordering::Equal => other.task.created_at.cmp(&self.task.created_at),
            other => other,
        }
    }
}

/// Task scheduler with priority queue
pub struct Scheduler {
    /// Priority queue of pending tasks
    queue: BinaryHeap<ScheduledTask>,
    /// Maximum concurrent tasks per kind
    max_concurrent: usize,
    /// Current running tasks per kind
    running: std::collections::HashMap<TaskKind, usize>,
}

impl Scheduler {
    /// Create a new scheduler
    pub fn new() -> Self {
        Self {
            queue: BinaryHeap::new(),
            max_concurrent: 4,
            running: std::collections::HashMap::new(),
        }
    }

    /// Schedule a task
    pub fn schedule(&mut self, task: Task) {
        self.queue.push(ScheduledTask { task });
    }

    /// Get the next task to run, respecting concurrency limits
    pub fn next(&mut self) -> Option<Task> {
        // Find a task that can run (not exceeding concurrency for its kind)
        let mut temp = Vec::new();
        let mut result = None;

        while let Some(scheduled) = self.queue.pop() {
            let running = self.running.get(&scheduled.task.kind).copied().unwrap_or(0);
            if running < self.max_concurrent {
                // Can run this task
                *self.running.entry(scheduled.task.kind).or_insert(0) += 1;
                result = Some(scheduled.task);
                break;
            } else {
                // Can't run yet, save for later
                temp.push(scheduled);
            }
        }

        // Put back tasks we couldn't run
        for t in temp {
            self.queue.push(t);
        }

        result
    }

    /// Mark a task as completed
    pub fn complete(&mut self, task_id: TaskId, kind: TaskKind) {
        if let Some(count) = self.running.get_mut(&kind) {
            *count = count.saturating_sub(1);
        }
        let _ = task_id; // Used for logging/debugging
    }

    /// Get number of pending tasks
    pub fn pending_count(&self) -> usize {
        self.queue.len()
    }

    /// Check if any tasks are running
    pub fn has_running(&self) -> bool {
        self.running.values().any(|&c| c > 0)
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}
