//! Compaction task planning and persistence
//!
//! Converts compaction plans into deterministic tasks and logs them for
//! durability. This layer does NOT execute compaction; it records:
//!   - which input SSTs participate in the compaction
//!   - which CF + levels are involved
//!   - generation of durable task IDs
//!   - recording of output SST files (once executor completes)
//!
//! NOTE: This intentionally avoids embedding any runtime objects or engine state.
//! It must be safe to serialize, persist, replay, and use for crash recovery.

use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// A single compaction task extracted from a CompactionPlan.
///
/// Invariants:
///   - `task_id` is monotonically increasing and unique within the log.
///   - `input_files` are fully qualified *relative* paths (e.g. cf_00/000123.sst).
///   - `output_files` is empty until compaction finishes.
///
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompactionTask {
    /// Unique task identifier (monotonic)
    pub task_id: u64,

    /// The column family this compaction belongs to
    pub cf_id: u32,

    /// Source LSM level (e.g., 0 → 1)
    pub source_level: u32,

    /// Target LSM level
    pub target_level: u32,

    /// Input SST file names participating in this compaction
    pub input_files: Vec<String>,

    /// Final output SST files (filled in by executor)
    pub output_files: Vec<String>,

    /// Task creation time (seconds since epoch)
    pub created_at: u64,
}

impl CompactionTask {
    pub fn new(
        task_id: u64,
        cf_id: u32,
        source_level: u32,
        target_level: u32,
        input_files: Vec<String>,
    ) -> Self {
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        Self {
            task_id,
            cf_id,
            source_level,
            target_level,
            input_files,
            output_files: Vec::new(),
            created_at,
        }
    }

    /// Serialize task to bytes (durable, stable)
    pub fn to_bytes(&self) -> std::io::Result<Vec<u8>> {
        serde_json::to_vec(self)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
    }

    /// Deserialize task from bytes
    pub fn from_bytes(data: &[u8]) -> std::io::Result<Self> {
        serde_json::from_slice(data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
    }
}

/// Persistent compaction log.
/// This does **not** enforce limits, pruning, or garbage collection.
/// Those are handled by the compaction scheduler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionLog {
    /// All compaction tasks in insertion order.
    pub tasks: Vec<CompactionTask>,

    /// Next task ID to assign (monotonic, never decreases)
    pub next_task_id: u64,
}

impl CompactionLog {
    pub fn new() -> Self {
        Self {
            tasks: Vec::new(),
            next_task_id: 1,
        }
    }

    /// Add a new compaction task and return its ID.
    ///
    /// This is deterministic: ID increments strictly by 1,
    /// and tasks are appended in the same order they were created.
    pub fn add_task(
        &mut self,
        cf_id: u32,
        source_level: u32,
        target_level: u32,
        input_files: Vec<String>,
    ) -> u64 {
        let task_id = self.next_task_id;
        self.next_task_id += 1;

        let task = CompactionTask::new(task_id, cf_id, source_level, target_level, input_files);
        self.tasks.push(task);

        task_id
    }

    /// Get immutable reference to a task
    pub fn get_task(&self, task_id: u64) -> Option<&CompactionTask> {
        self.tasks.iter().find(|t| t.task_id == task_id)
    }

    /// Get mutable reference to a task
    pub fn get_task_mut(&mut self, task_id: u64) -> Option<&mut CompactionTask> {
        self.tasks.iter_mut().find(|t| t.task_id == task_id)
    }

    /// Mark a task as complete (assign output SSTs).
    pub fn complete_task(&mut self, task_id: u64, output_files: Vec<String>) -> bool {
        if let Some(task) = self.get_task_mut(task_id) {
            task.output_files = output_files;
            true
        } else {
            false
        }
    }

    /// Serialize log to bytes
    pub fn to_bytes(&self) -> std::io::Result<Vec<u8>> {
        serde_json::to_vec(self)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
    }

    /// Deserialize entire log from bytes
    pub fn from_bytes(data: &[u8]) -> std::io::Result<Self> {
        serde_json::from_slice(data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
    }
}

impl Default for CompactionLog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_task_with_provided_fields_when_new() {
        let task = CompactionTask::new(1, 0, 0, 1, vec!["file1.sst".to_string()]);

        assert_eq!(task.task_id, 1);
        assert_eq!(task.source_level, 0);
        assert_eq!(task.target_level, 1);
        assert_eq!(task.cf_id, 0);
        assert!(task.output_files.is_empty());
        assert!(task.created_at > 0);
    }

    #[test]
    fn should_roundtrip_task_when_converting_to_bytes() {
        let task = CompactionTask::new(1, 0, 0, 1, vec!["file1.sst".to_string()]);

        let bytes = task.to_bytes().unwrap();
        let deserialized = CompactionTask::from_bytes(&bytes).unwrap();

        assert_eq!(task, deserialized);
    }

    #[test]
    fn should_create_empty_log_when_new() {
        let log = CompactionLog::new();
        assert_eq!(log.tasks.len(), 0);
        assert_eq!(log.next_task_id, 1);
    }

    #[test]
    fn should_increment_task_id_when_adding_task() {
        let mut log = CompactionLog::new();
        let id = log.add_task(0, 0, 1, vec!["file1.sst".to_string()]);

        assert_eq!(id, 1);
        assert_eq!(log.next_task_id, 2);
        assert_eq!(log.tasks.len(), 1);
    }

    #[test]
    fn should_set_output_files_when_completing_task() {
        let mut log = CompactionLog::new();
        let id = log.add_task(0, 0, 1, vec!["file1.sst".to_string()]);

        let success = log.complete_task(id, vec!["output1.sst".to_string()]);
        assert!(success);

        let task = log.get_task(id).unwrap();
        assert_eq!(task.output_files, vec!["output1.sst".to_string()]);
    }

    #[test]
    fn should_roundtrip_log_when_converting_to_bytes() {
        let mut log = CompactionLog::new();
        log.add_task(0, 0, 1, vec!["file1.sst".to_string()]);
        log.add_task(1, 1, 2, vec!["file2.sst".to_string()]);

        let bytes = log.to_bytes().unwrap();
        let deserialized = CompactionLog::from_bytes(&bytes).unwrap();

        assert_eq!(deserialized.tasks.len(), 2);
        assert_eq!(deserialized.next_task_id, 3);
    }
}
