//! Intent log persistence - serialization and file I/O
//!
//! Persists runtime intent log to disk in YAML format to enable recovery of
//! actor intent states across restarts.

use crate::runtime::IntentLogEntry;
use std::fs;
use std::path::{Path, PathBuf};

pub struct IntentPersistence;

impl IntentPersistence {
    const INTENT_FILE: &'static str = "intent_log.yaml";

    pub fn intent_path(db_path: &Path) -> PathBuf {
        db_path.join(Self::INTENT_FILE)
    }

    pub fn load(db_path: &Path) -> Result<Vec<IntentLogEntry>, String> {
        let p = Self::intent_path(db_path);
        if !p.exists() {
            tracing::debug!(path = ?p, "intent file not found, using empty log");
            return Ok(Vec::new());
        }

        let contents =
            fs::read_to_string(&p).map_err(|e| format!("failed to read intent file: {}", e))?;

        let intents: Vec<IntentLogEntry> = serde_yaml::from_str(&contents)
            .map_err(|e| format!("failed to parse intent YAML: {}", e))?;

        tracing::debug!(path = ?p, entries = intents.len(), "intent log loaded");
        Ok(intents)
    }

    pub fn save(db_path: &Path, intents: &[IntentLogEntry]) -> Result<(), String> {
        fs::create_dir_all(db_path)
            .map_err(|e| format!("failed to create database directory: {}", e))?;

        let p = Self::intent_path(db_path);
        let yaml = serde_yaml::to_string(intents)
            .map_err(|e| format!("failed to serialize intent log to YAML: {}", e))?;

        let temp = p.with_extension("yaml.tmp");
        fs::write(&temp, &yaml)
            .map_err(|e| format!("failed to write temporary intent file: {}", e))?;
        fs::rename(&temp, &p)
            .map_err(|e| format!("failed to rename intent file atomically: {}", e))?;

        tracing::debug!(path = ?p, entries = intents.len(), "intent log persisted");
        Ok(())
    }

    pub fn delete(db_path: &Path) -> Result<(), String> {
        let p = Self::intent_path(db_path);
        if !p.exists() {
            return Ok(());
        }

        fs::remove_file(&p).map_err(|e| format!("failed to delete intent file: {}", e))?;
        tracing::debug!(path = ?p, "intent file deleted");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::IntentLogEntry;

    fn create_test_dir() -> std::path::PathBuf {
        use std::time::{SystemTime, UNIX_EPOCH};
        let pid = std::process::id();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let test_dir = std::env::temp_dir().join(format!("midge_intent_test_{}_{}", pid, nanos));
        let _ = std::fs::remove_dir_all(&test_dir);
        std::fs::create_dir_all(&test_dir).expect("failed to create test dir");
        test_dir
    }

    #[test]
    fn should_roundtrip_intent_log() {
        let test_dir = create_test_dir();
        let intents = vec![IntentLogEntry::WalSynced {
            segment_id: 1,
            seqno: 42,
        }];

        IntentPersistence::save(&test_dir, &intents).expect("save should succeed");
        let loaded = IntentPersistence::load(&test_dir).expect("load should succeed");

        assert_eq!(loaded.len(), 1);
        match &loaded[0] {
            IntentLogEntry::WalSynced { segment_id, seqno } => {
                assert_eq!(*segment_id, 1);
                assert_eq!(*seqno, 42);
            }
            _ => panic!("unexpected intent variant"),
        }
    }
}
