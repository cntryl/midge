//! Compaction task planning and persistence
//!
//! Converts compaction plans into deterministic tasks and logs them for durability.

use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// A single compaction task extracted from a CompactionPlan
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompactionTask {
    /// Unique task identifier
    pub task_id: u64,
    /// Source level
    pub source_level: u32,
    /// Target level
    pub target_level: u32,
    /// Column family ID
    pub cf_id: u32,
    /// Input SST file names
    pub input_files: Vec<String>,
    /// Output SST file names (initially empty, filled by executor)
    pub output_files: Vec<String>,
    /// Task creation timestamp (seconds since epoch)
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
            source_level,
            target_level,
            cf_id,
            input_files,
            output_files: Vec::new(),
            created_at,
        }
    }

    /// Serialize task to bytes
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

/// Log of compaction tasks for durability
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionLog {
    pub tasks: Vec<CompactionTask>,
    pub next_task_id: u64,
}

impl CompactionLog {
    pub fn new() -> Self {
        Self {
            tasks: Vec::new(),
            next_task_id: 1,
        }
    }

    /// Add a task and return its assigned task_id
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

    /// Get task by ID
    pub fn get_task(&self, task_id: u64) -> Option<&CompactionTask> {
        self.tasks.iter().find(|t| t.task_id == task_id)
    }

    /// Get mutable task by ID
    pub fn get_task_mut(&mut self, task_id: u64) -> Option<&mut CompactionTask> {
        self.tasks.iter_mut().find(|t| t.task_id == task_id)
    }

    /// Mark task as complete with output files
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

    /// Deserialize log from bytes
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
        // Arrange
        // Act
        let task = CompactionTask::new(1, 0, 0, 1, vec!["file1.sst".to_string()]);

        // Assert
        assert_eq!(task.task_id, 1);
        assert_eq!(task.source_level, 0);
        assert_eq!(task.target_level, 1);
        assert_eq!(task.cf_id, 0);
        assert!(task.output_files.is_empty());
    }

    #[test]
    fn should_roundtrip_task_when_converting_to_bytes() {
        // Arrange
        let task = CompactionTask::new(1, 0, 0, 1, vec!["file1.sst".to_string()]);

        // Act
        let bytes = task.to_bytes().unwrap();
        let deserialized = CompactionTask::from_bytes(&bytes).unwrap();

        // Assert
        assert_eq!(task, deserialized);
    }

    #[test]
    fn should_create_empty_log_when_new() {
        // Arrange
        // Act
        let log = CompactionLog::new();

        // Assert
        assert_eq!(log.tasks.len(), 0);
        assert_eq!(log.next_task_id, 1);
    }

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
}
