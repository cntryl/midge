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

    // ============================================================================
    // Tests for CompactionTask initialization and field invariants
    // ============================================================================

    #[test]
    fn should_create_task_with_provided_fields_when_new() {
        // Arrange / Act
        let task = CompactionTask::new(1, 0, 0, 1, vec!["file1.sst".to_string()]);

        // Assert
        assert_eq!(task.task_id, 1);
        assert_eq!(task.source_level, 0);
        assert_eq!(task.target_level, 1);
        assert_eq!(task.cf_id, 0);
        assert!(task.output_files.is_empty());
        assert!(task.created_at > 0);
    }

    #[test]
    fn should_initialize_output_files_empty_when_task_created() {
        // Arrange / Act
        let task = CompactionTask::new(5, 2, 1, 2, vec!["input.sst".to_string()]);

        // Assert: output_files must be empty initially
        assert!(task.output_files.is_empty());
    }

    #[test]
    fn should_preserve_input_files_when_creating_task() {
        // Arrange
        let input_files = vec![
            "file1.sst".to_string(),
            "file2.sst".to_string(),
            "file3.sst".to_string(),
        ];

        // Act
        let task = CompactionTask::new(1, 0, 0, 1, input_files.clone());

        // Assert: input files preserved exactly
        assert_eq!(task.input_files, input_files);
    }

    #[test]
    fn should_capture_current_time_when_creating_task() {
        // Arrange
        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Act
        let task = CompactionTask::new(1, 0, 0, 1, vec![]);

        // Assert
        let after = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        assert!(task.created_at >= before);
        assert!(task.created_at <= after + 1); // Allow 1 sec drift
    }

    #[test]
    fn should_preserve_all_task_parameters_when_creating() {
        // Arrange
        let cf_id = 3;
        let source_level = 2;
        let target_level = 3;
        let task_id = 42;

        // Act
        let task = CompactionTask::new(task_id, cf_id, source_level, target_level, vec![]);

        // Assert
        assert_eq!(task.task_id, task_id);
        assert_eq!(task.cf_id, cf_id);
        assert_eq!(task.source_level, source_level);
        assert_eq!(task.target_level, target_level);
    }

    // ============================================================================
    // Tests for CompactionTask serialization invariants
    // ============================================================================

    #[test]
    fn should_roundtrip_task_when_converting_to_bytes() {
        // Arrange
        let task = CompactionTask::new(1, 0, 0, 1, vec!["file1.sst".to_string()]);

        // Act
        let bytes = task.to_bytes().unwrap();
        let deserialized = CompactionTask::from_bytes(&bytes).unwrap();

        // Assert: full equality after round-trip
        assert_eq!(task, deserialized);
    }

    #[test]
    fn should_roundtrip_task_with_multiple_input_files() {
        // Arrange
        let task = CompactionTask::new(
            10,
            1,
            1,
            2,
            vec![
                "cf_01/file1.sst".to_string(),
                "cf_01/file2.sst".to_string(),
                "cf_01/file3.sst".to_string(),
            ],
        );

        // Act
        let bytes = task.to_bytes().unwrap();
        let deserialized = CompactionTask::from_bytes(&bytes).unwrap();

        // Assert
        assert_eq!(task.input_files.len(), 3);
        assert_eq!(task, deserialized);
    }

    #[test]
    fn should_roundtrip_completed_task_with_output_files() {
        // Arrange
        let mut task = CompactionTask::new(1, 0, 0, 1, vec!["input.sst".to_string()]);
        task.output_files = vec!["output.sst".to_string()];

        // Act
        let bytes = task.to_bytes().unwrap();
        let deserialized = CompactionTask::from_bytes(&bytes).unwrap();

        // Assert
        assert_eq!(deserialized.output_files, vec!["output.sst".to_string()]);
        assert_eq!(task, deserialized);
    }

    #[test]
    fn should_preserve_serialization_format_stability() {
        // Arrange: create two identical tasks
        let task1 = CompactionTask::new(1, 0, 0, 1, vec!["file.sst".to_string()]);
        let task2 = CompactionTask::new(1, 0, 0, 1, vec!["file.sst".to_string()]);

        // Act: serialize both
        let bytes1 = task1.to_bytes().unwrap();
        let bytes2 = task2.to_bytes().unwrap();

        // Assert: serialization format differs only in created_at timestamp
        // This tests that the serialization is deterministic for same fields
        let deser1 = CompactionTask::from_bytes(&bytes1).unwrap();
        let deser2 = CompactionTask::from_bytes(&bytes2).unwrap();
        assert_eq!(deser1.task_id, deser2.task_id);
        assert_eq!(deser1.cf_id, deser2.cf_id);
    }

    // ============================================================================
    // Tests for CompactionLog initialization invariants
    // ============================================================================

    #[test]
    fn should_create_empty_log_when_new() {
        // Arrange / Act
        let log = CompactionLog::new();

        // Assert
        assert_eq!(log.tasks.len(), 0);
        assert_eq!(log.next_task_id, 1);
    }

    #[test]
    fn should_create_empty_log_when_default() {
        // Arrange / Act
        let log = CompactionLog::default();

        // Assert
        assert_eq!(log.tasks.len(), 0);
        assert_eq!(log.next_task_id, 1);
    }

    #[test]
    fn should_initialize_next_task_id_to_one() {
        // Arrange / Act
        let log = CompactionLog::new();

        // Assert: next_task_id must start at 1 (not 0)
        assert_eq!(log.next_task_id, 1);
    }

    // ============================================================================
    // Tests for task ID monotonicity invariant
    // ============================================================================

    #[test]
    fn should_increment_task_id_when_adding_task() {
        // Arrange
        let mut log = CompactionLog::new();

        // Act
        let id = log.add_task(0, 0, 1, vec!["file1.sst".to_string()]);

        // Assert
        assert_eq!(id, 1);
        assert_eq!(log.next_task_id, 2);
        assert_eq!(log.tasks.len(), 1);
    }

    #[test]
    fn should_assign_strictly_increasing_task_ids() {
        // Arrange
        let mut log = CompactionLog::new();

        // Act: add multiple tasks
        let id1 = log.add_task(0, 0, 1, vec!["file1.sst".to_string()]);
        let id2 = log.add_task(0, 1, 2, vec!["file2.sst".to_string()]);
        let id3 = log.add_task(0, 2, 3, vec!["file3.sst".to_string()]);

        // Assert: IDs strictly increasing by 1
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(id3, 3);
        assert_eq!(log.next_task_id, 4);
    }

    #[test]
    fn should_never_reuse_task_ids() {
        // Arrange
        let mut log = CompactionLog::new();

        // Act: add tasks and verify IDs never repeat
        let mut ids = Vec::new();
        for i in 0..10 {
            let id = log.add_task(0, 0, 1, vec![format!("file{}.sst", i)]);
            ids.push(id);
        }

        // Assert: all IDs unique
        let unique_count = ids.iter().collect::<std::collections::HashSet<_>>().len();
        assert_eq!(unique_count, 10);

        // Assert: IDs are 1..10
        for (i, &id) in ids.iter().enumerate() {
            assert_eq!(id, (i + 1) as u64);
        }
    }

    #[test]
    fn should_maintain_monotonic_next_task_id() {
        // Arrange
        let mut log = CompactionLog::new();
        let initial_next = log.next_task_id;

        // Act: add tasks
        for _ in 0..5 {
            log.add_task(0, 0, 1, vec![]);
        }

        // Assert: next_task_id increased by exactly the number of tasks
        assert_eq!(log.next_task_id, initial_next + 5);
    }

    // ============================================================================
    // Tests for task ordering invariant
    // ============================================================================

    #[test]
    fn should_append_tasks_in_creation_order() {
        // Arrange
        let mut log = CompactionLog::new();

        // Act
        log.add_task(0, 0, 1, vec!["first.sst".to_string()]);
        log.add_task(1, 1, 2, vec!["second.sst".to_string()]);
        log.add_task(2, 2, 3, vec!["third.sst".to_string()]);

        // Assert: tasks in creation order
        assert_eq!(log.tasks[0].task_id, 1);
        assert_eq!(log.tasks[1].task_id, 2);
        assert_eq!(log.tasks[2].task_id, 3);

        assert_eq!(log.tasks[0].input_files[0], "first.sst");
        assert_eq!(log.tasks[1].input_files[0], "second.sst");
        assert_eq!(log.tasks[2].input_files[0], "third.sst");
    }

    #[test]
    fn should_preserve_input_file_order_when_adding_task() {
        // Arrange
        let mut log = CompactionLog::new();
        let input_files = vec![
            "z.sst".to_string(),
            "a.sst".to_string(),
            "m.sst".to_string(),
        ];

        // Act
        log.add_task(0, 0, 1, input_files.clone());

        // Assert: input files in same order (not sorted)
        assert_eq!(log.tasks[0].input_files, input_files);
    }

    // ============================================================================
    // Tests for task lookup invariants
    // ============================================================================

    #[test]
    fn should_get_task_by_task_id() {
        // Arrange
        let mut log = CompactionLog::new();
        let id = log.add_task(0, 0, 1, vec!["file.sst".to_string()]);

        // Act
        let task = log.get_task(id);

        // Assert
        assert!(task.is_some());
        assert_eq!(task.unwrap().task_id, id);
    }

    #[test]
    fn should_return_none_when_task_not_found() {
        // Arrange
        let log = CompactionLog::new();

        // Act
        let task = log.get_task(999);

        // Assert
        assert!(task.is_none());
    }

    #[test]
    fn should_get_task_mut_and_modify() {
        // Arrange
        let mut log = CompactionLog::new();
        let id = log.add_task(0, 0, 1, vec![]);

        // Act: get mutable reference and modify
        if let Some(task) = log.get_task_mut(id) {
            task.output_files.push("output.sst".to_string());
        }

        // Assert
        assert_eq!(log.get_task(id).unwrap().output_files.len(), 1);
    }

    #[test]
    fn should_return_correct_task_from_multiple_tasks() {
        // Arrange
        let mut log = CompactionLog::new();
        let id1 = log.add_task(0, 0, 1, vec!["file1.sst".to_string()]);
        let id2 = log.add_task(1, 1, 2, vec!["file2.sst".to_string()]);
        let id3 = log.add_task(2, 2, 3, vec!["file3.sst".to_string()]);

        // Act & Assert: verify each task retrieved correctly
        assert_eq!(log.get_task(id1).unwrap().cf_id, 0);
        assert_eq!(log.get_task(id2).unwrap().cf_id, 1);
        assert_eq!(log.get_task(id3).unwrap().cf_id, 2);

        assert_eq!(log.get_task(id1).unwrap().input_files[0], "file1.sst");
        assert_eq!(log.get_task(id2).unwrap().input_files[0], "file2.sst");
        assert_eq!(log.get_task(id3).unwrap().input_files[0], "file3.sst");
    }

    // ============================================================================
    // Tests for task completion invariants
    // ============================================================================

    #[test]
    fn should_set_output_files_when_completing_task() {
        // Arrange
        let mut log = CompactionLog::new();
        let id = log.add_task(0, 0, 1, vec!["file1.sst".to_string()]);

        // Act
        let success = log.complete_task(id, vec!["output1.sst".to_string()]);

        // Assert
        assert!(success);
        let task = log.get_task(id).unwrap();
        assert_eq!(task.output_files, vec!["output1.sst".to_string()]);
    }

    #[test]
    fn should_return_false_when_completing_nonexistent_task() {
        // Arrange
        let mut log = CompactionLog::new();

        // Act
        let success = log.complete_task(999, vec!["output.sst".to_string()]);

        // Assert
        assert!(!success);
    }

    #[test]
    fn should_accept_multiple_output_files_when_completing() {
        // Arrange
        let mut log = CompactionLog::new();
        let id = log.add_task(0, 0, 1, vec!["input.sst".to_string()]);
        let output_files = vec![
            "out1.sst".to_string(),
            "out2.sst".to_string(),
            "out3.sst".to_string(),
        ];

        // Act
        log.complete_task(id, output_files.clone());

        // Assert
        assert_eq!(log.get_task(id).unwrap().output_files, output_files);
    }

    #[test]
    fn should_allow_replacing_output_files_on_completion() {
        // Arrange
        let mut log = CompactionLog::new();
        let id = log.add_task(0, 0, 1, vec![]);

        // Act: complete with first output set
        log.complete_task(id, vec!["first.sst".to_string()]);
        assert_eq!(log.get_task(id).unwrap().output_files.len(), 1);

        // Complete again with different output set
        log.complete_task(id, vec!["second.sst".to_string()]);

        // Assert: latest output set wins
        assert_eq!(
            log.get_task(id).unwrap().output_files,
            vec!["second.sst".to_string()]
        );
    }

    // ============================================================================
    // Tests for CompactionLog serialization invariants
    // ============================================================================

    #[test]
    fn should_roundtrip_log_when_converting_to_bytes() {
        // Arrange
        let mut log = CompactionLog::new();
        log.add_task(0, 0, 1, vec!["file1.sst".to_string()]);
        log.add_task(1, 1, 2, vec!["file2.sst".to_string()]);

        // Act
        let bytes = log.to_bytes().unwrap();
        let deserialized = CompactionLog::from_bytes(&bytes).unwrap();

        // Assert
        assert_eq!(deserialized.tasks.len(), 2);
        assert_eq!(deserialized.next_task_id, 3);
    }

    #[test]
    fn should_preserve_task_count_after_roundtrip() {
        // Arrange
        let mut log = CompactionLog::new();
        for i in 0..5 {
            log.add_task(i % 2, i / 2, i / 2 + 1, vec![format!("file{}.sst", i)]);
        }

        // Act
        let bytes = log.to_bytes().unwrap();
        let deserialized = CompactionLog::from_bytes(&bytes).unwrap();

        // Assert
        assert_eq!(deserialized.tasks.len(), 5);
    }

    #[test]
    fn should_preserve_next_task_id_after_roundtrip() {
        // Arrange
        let mut log = CompactionLog::new();
        log.add_task(0, 0, 1, vec![]);
        log.add_task(0, 1, 2, vec![]);
        let expected_next_id = log.next_task_id;

        // Act
        let bytes = log.to_bytes().unwrap();
        let deserialized = CompactionLog::from_bytes(&bytes).unwrap();

        // Assert
        assert_eq!(deserialized.next_task_id, expected_next_id);
    }

    #[test]
    fn should_roundtrip_log_with_completed_tasks() {
        // Arrange
        let mut log = CompactionLog::new();
        let id1 = log.add_task(0, 0, 1, vec!["in1.sst".to_string()]);
        let id2 = log.add_task(1, 1, 2, vec!["in2.sst".to_string()]);

        log.complete_task(id1, vec!["out1.sst".to_string()]);
        log.complete_task(id2, vec!["out2a.sst".to_string(), "out2b.sst".to_string()]);

        // Act
        let bytes = log.to_bytes().unwrap();
        let deserialized = CompactionLog::from_bytes(&bytes).unwrap();

        // Assert
        assert_eq!(deserialized.get_task(id1).unwrap().output_files.len(), 1);
        assert_eq!(deserialized.get_task(id2).unwrap().output_files.len(), 2);
    }

    #[test]
    fn should_preserve_all_fields_during_roundtrip() {
        // Arrange
        let mut log = CompactionLog::new();
        log.add_task(5, 2, 3, vec!["cf_05/file.sst".to_string()]);

        // Act
        let bytes = log.to_bytes().unwrap();
        let deserialized = CompactionLog::from_bytes(&bytes).unwrap();

        // Assert: all fields preserved
        let task = deserialized.get_task(1).unwrap();
        assert_eq!(task.cf_id, 5);
        assert_eq!(task.source_level, 2);
        assert_eq!(task.target_level, 3);
        assert_eq!(task.input_files, vec!["cf_05/file.sst".to_string()]);
    }

    // ============================================================================
    // Tests for edge cases and robustness
    // ============================================================================

    #[test]
    fn should_handle_empty_input_files_list() {
        // Arrange
        let mut log = CompactionLog::new();

        // Act
        let id = log.add_task(0, 0, 1, vec![]);

        // Assert
        assert!(log.get_task(id).unwrap().input_files.is_empty());
    }

    #[test]
    fn should_handle_large_task_count() {
        // Arrange
        let mut log = CompactionLog::new();

        // Act: add many tasks
        for i in 0..1000 {
            log.add_task(0, 0, 1, vec![format!("file{}.sst", i)]);
        }

        // Assert
        assert_eq!(log.tasks.len(), 1000);
        assert_eq!(log.next_task_id, 1001);
    }

    #[test]
    fn should_handle_maximum_values() {
        // Arrange
        let mut log = CompactionLog::new();

        // Act: create task with max values
        let id = log.add_task(u32::MAX, u32::MAX - 1, u32::MAX, vec![]);

        // Assert
        let task = log.get_task(id).unwrap();
        assert_eq!(task.cf_id, u32::MAX);
        assert_eq!(task.source_level, u32::MAX - 1);
        assert_eq!(task.target_level, u32::MAX);
    }
}
