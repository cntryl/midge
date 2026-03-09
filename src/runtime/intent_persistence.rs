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

    pub fn load_with_fs(
        fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
    ) -> Result<Vec<IntentLogEntry>, String> {
        Self::load_with_fs_and_policy(fs, crate::engine::RecoveryPolicy::Strict)
    }

    pub fn load_with_fs_and_policy(
        fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
        recovery_policy: crate::engine::RecoveryPolicy,
    ) -> Result<Vec<IntentLogEntry>, String> {
        use crate::io::traits::FsPath;

        let p = FsPath::new(Self::INTENT_FILE);
        match fs.exists(&p) {
            Ok(false) => {
                tracing::debug!(path = ?p, "intent file not found, using empty log");
                return Ok(Vec::new());
            }
            Err(e) => return Err(format!("fs exists error: {:?}", e)),
            Ok(true) => {}
        }

        let file = fs
            .open(
                &p,
                crate::io::traits::OpenOptions {
                    mode: crate::io::traits::OpenMode::ReadOnly,
                    create: false,
                    create_new: false,
                    truncate: false,
                },
            )
            .map_err(|e| format!("failed to open intent file: {:?}", e))?;
        let len = file
            .len()
            .map_err(|e| format!("failed to stat intent file: {:?}", e))?;
        let data = file
            .read_at(0, len)
            .map_err(|e| format!("failed to read intent file: {:?}", e))?;
        let contents =
            String::from_utf8(data.to_vec()).map_err(|e| format!("intent file not utf8: {}", e))?;

        let intents: Vec<IntentLogEntry> = serde_yaml::from_str(&contents).map_err(|e| {
            if recovery_policy == crate::engine::RecoveryPolicy::Strict {
                format!("failed to parse intent YAML: {}", e)
            } else {
                format!("failed to parse intent YAML (salvage mode): {}", e)
            }
        })?;

        tracing::debug!(path = ?p, entries = intents.len(), "intent log loaded");
        Ok(intents)
    }

    pub fn load(db_path: &Path) -> Result<Vec<IntentLogEntry>, String> {
        Self::load_with_policy(db_path, crate::engine::RecoveryPolicy::Salvage)
    }

    pub fn load_with_policy(
        db_path: &Path,
        recovery_policy: crate::engine::RecoveryPolicy,
    ) -> Result<Vec<IntentLogEntry>, String> {
        use crate::io::real::RealFs;
        use std::sync::Arc;

        let real =
            RealFs::new(db_path).map_err(|e| format!("failed to initialize real fs: {:?}", e))?;
        let fs: Arc<dyn crate::io::traits::Fs> = Arc::new(real);
        Self::load_with_fs_and_policy(&fs, recovery_policy)
    }

    #[allow(dead_code)]
    fn load_legacy(db_path: &Path) -> Result<Vec<IntentLogEntry>, String> {
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

    pub fn save_with_fs(
        fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
        intents: &[IntentLogEntry],
    ) -> Result<(), String> {
        use crate::io::traits::{Durability, FsPath, OpenMode, OpenOptions};

        let yaml = serde_yaml::to_string(intents)
            .map_err(|e| format!("failed to serialize intent log to YAML: {}", e))?;

        let temp = FsPath::new("intent_log.yaml.tmp");
        let mut f = fs
            .open(
                &temp,
                OpenOptions {
                    mode: OpenMode::ReadWrite,
                    create: true,
                    create_new: false,
                    truncate: true,
                },
            )
            .map_err(|e| format!("failed to open temp intent file: {:?}", e))?;
        f.write_at(0, bytes::Bytes::from(yaml.clone()))
            .map_err(|e| format!("failed to write temp intent: {:?}", e))?;
        f.sync(Durability::Durable)
            .map_err(|e| format!("failed to sync temp intent: {:?}", e))?;

        fs.rename_atomic(&temp, &FsPath::new(Self::INTENT_FILE))
            .map_err(|e| format!("failed to rename intent file atomically: {:?}", e))?;

        tracing::debug!(path = ?Self::INTENT_FILE, entries = intents.len(), "intent log persisted");
        Ok(())
    }

    pub fn save(db_path: &Path, intents: &[IntentLogEntry]) -> Result<(), String> {
        use crate::io::real::RealFs;
        use std::sync::Arc;

        let real =
            RealFs::new(db_path).map_err(|e| format!("failed to initialize real fs: {:?}", e))?;
        let fs: Arc<dyn crate::io::traits::Fs> = Arc::new(real);
        Self::save_with_fs(&fs, intents)
    }

    pub fn delete_with_fs(fs: &std::sync::Arc<dyn crate::io::traits::Fs>) -> Result<(), String> {
        use crate::io::traits::FsPath;

        let p = FsPath::new(Self::INTENT_FILE);
        match fs.exists(&p) {
            Ok(false) => return Ok(()),
            Err(e) => return Err(format!("fs exists error: {:?}", e)),
            Ok(true) => {}
        }

        fs.remove_file(&p)
            .map_err(|e| format!("failed to delete intent file: {:?}", e))?;
        tracing::debug!(path = ?p, "intent file deleted");
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
    use proptest::prelude::*;

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
        // Arrange
        let test_dir = create_test_dir();
        let intents = vec![IntentLogEntry::WalSynced {
            segment_id: 1,
            seqno: 42,
        }];

        // Act
        IntentPersistence::save(&test_dir, &intents).expect("save should succeed");
        let loaded = IntentPersistence::load(&test_dir).expect("load should succeed");

        // Assert
        assert_eq!(loaded.len(), 1);
        assert!(
            matches!(
                loaded[0],
                IntentLogEntry::WalSynced {
                    segment_id: 1,
                    seqno: 42
                }
            ),
            "expected WalSynced entry, got: {:?}",
            loaded[0]
        );
    }

    proptest! {
        #[test]
        fn should_roundtrip_arbitrary_wal_synced_intents(
            pairs in proptest::collection::vec((0u64..10_000, 0u64..10_000), 0..32)
        ) {
            // Arrange
            let test_dir = create_test_dir();
            let intents: Vec<IntentLogEntry> = pairs
                .iter()
                .map(|(segment_id, seqno)| IntentLogEntry::WalSynced {
                    segment_id: *segment_id,
                    seqno: *seqno,
                })
                .collect();

            // Act
            IntentPersistence::save(&test_dir, &intents).expect("save should succeed");
            let loaded = IntentPersistence::load(&test_dir).expect("load should succeed");

            // Assert
            prop_assert_eq!(loaded.len(), intents.len());
            for (loaded_entry, expected_entry) in loaded.iter().zip(intents.iter()) {
                match (loaded_entry, expected_entry) {
                    (
                        IntentLogEntry::WalSynced {
                            segment_id: loaded_segment,
                            seqno: loaded_seqno,
                        },
                        IntentLogEntry::WalSynced {
                            segment_id: expected_segment,
                            seqno: expected_seqno,
                        },
                    ) => {
                        prop_assert_eq!(loaded_segment, expected_segment);
                        prop_assert_eq!(loaded_seqno, expected_seqno);
                    }
                    other => prop_assert!(false, "unexpected intent roundtrip pair: {:?}", other),
                }
            }
        }
    }
}
