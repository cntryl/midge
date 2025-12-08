//! Durability and persistence for compaction logs.
//!
//! Handles reading/writing compaction logs to disk for crash recovery
//! and deterministic replay of compaction decisions.

use crate::error::MidgeResult;
use crate::core::compaction::CompactionTask;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

/// Compaction log file path (relative to engine directory)
const COMPACTION_LOG_FILE: &str = "compaction.log";

/// Manager for compaction log persistence
pub struct CompactionLogManager {
    log_dir: std::path::PathBuf,
}

impl CompactionLogManager {
    pub fn new(engine_dir: &Path) -> Self {
        Self {
            log_dir: engine_dir.to_path_buf(),
        }
    }

    /// Get the full path to the compaction log file
    fn log_path(&self) -> std::path::PathBuf {
        self.log_dir.join(COMPACTION_LOG_FILE)
    }

    /// Load compaction log from disk, or empty if not found
    pub fn load(&self) -> MidgeResult<Vec<CompactionTask>> {
        let path = self.log_path();
        if !path.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(&path)
            .map_err(|e| crate::error::MidgeError::internal(format!("Failed to open compaction log: {}", e)))?;
        let mut reader = BufReader::new(file);
        let mut contents = Vec::new();
        reader
            .read_to_end(&mut contents)
            .map_err(|e| crate::error::MidgeError::internal(format!("Failed to read compaction log: {}", e)))?;

        if contents.is_empty() {
            return Ok(Vec::new());
        }

        // Parse JSON lines format (one task per line)
        let mut tasks = Vec::new();
        for line in String::from_utf8_lossy(&contents).lines() {
            if line.trim().is_empty() {
                continue;
            }
            let task = CompactionTask::from_bytes(line.as_bytes())?;
            tasks.push(task);
        }

        Ok(tasks)
    }

    /// Append a compaction task to the log
    pub fn append(&self, task: &CompactionTask) -> MidgeResult<()> {
        let path = self.log_path();
        
        // Create parent directory if needed
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| crate::error::MidgeError::internal(format!("Failed to create log directory: {}", e)))?;
        }

        // Open file in append mode
        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| crate::error::MidgeError::internal(format!("Failed to open compaction log for append: {}", e)))?;

        let mut writer = BufWriter::new(file);
        let bytes = task.to_bytes()?;
        writer
            .write_all(&bytes)
            .map_err(|e| crate::error::MidgeError::internal(format!("Failed to write compaction log: {}", e)))?;
        writer.write_all(b"\n")
            .map_err(|e| crate::error::MidgeError::internal(format!("Failed to write newline: {}", e)))?;
        writer
            .flush()
            .map_err(|e| crate::error::MidgeError::internal(format!("Failed to flush compaction log: {}", e)))?;

        Ok(())
    }

    /// Clear the compaction log (after successful checkpoint)
    pub fn clear(&self) -> MidgeResult<()> {
        let path = self.log_path();
        if path.exists() {
            fs::remove_file(&path)
                .map_err(|e| crate::error::MidgeError::internal(format!("Failed to clear compaction log: {}", e)))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn should_create_empty_log_when_file_not_exists() {
        let temp_dir = TempDir::new().unwrap();
        let manager = CompactionLogManager::new(temp_dir.path());
        
        let tasks = manager.load().unwrap();
        assert!(tasks.is_empty());
    }

    #[test]
    fn should_persist_and_recover_task() {
        let temp_dir = TempDir::new().unwrap();
        let manager = CompactionLogManager::new(temp_dir.path());

        let task = CompactionTask::new(
            1,
            &crate::core::compaction::CompactionPlan {
                source_level: 0,
                target_level: 1,
                cf_id: 0,
                input_files: vec!["sst_001.blob".to_string()],
                output_files: Vec::new(),
            },
        );

        manager.append(&task).unwrap();
        
        let recovered = manager.load().unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].task_id, task.task_id);
        assert_eq!(recovered[0].cf_id, task.cf_id);
    }

    #[test]
    fn should_support_multiple_appends() {
        let temp_dir = TempDir::new().unwrap();
        let manager = CompactionLogManager::new(temp_dir.path());

        let task1 = CompactionTask::new(
            1,
            &crate::core::compaction::CompactionPlan {
                source_level: 0,
                target_level: 1,
                cf_id: 0,
                input_files: vec!["sst_001.blob".to_string()],
                output_files: Vec::new(),
            },
        );

        let task2 = CompactionTask::new(
            2,
            &crate::core::compaction::CompactionPlan {
                source_level: 1,
                target_level: 2,
                cf_id: 0,
                input_files: vec!["sst_002.blob".to_string()],
                output_files: Vec::new(),
            },
        );

        manager.append(&task1).unwrap();
        manager.append(&task2).unwrap();
        
        let recovered = manager.load().unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].task_id, 1);
        assert_eq!(recovered[1].task_id, 2);
    }

    #[test]
    fn should_clear_log_successfully() {
        let temp_dir = TempDir::new().unwrap();
        let manager = CompactionLogManager::new(temp_dir.path());

        let task = CompactionTask::new(
            1,
            &crate::core::compaction::CompactionPlan {
                source_level: 0,
                target_level: 1,
                cf_id: 0,
                input_files: vec!["sst_001.blob".to_string()],
                output_files: Vec::new(),
            },
        );

        manager.append(&task).unwrap();
        let path = manager.log_path();
        assert!(path.exists());

        manager.clear().unwrap();
        assert!(!path.exists());
    }
}
