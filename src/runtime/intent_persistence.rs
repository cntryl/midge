//! Intent log persistence - serialization and file I/O
//!
//! Persists runtime intent log to disk in JSON format to enable recovery of
//! actor intent states across restarts.

use crate::runtime::IntentLogEntry;
use std::fs;
use std::path::{Path, PathBuf};

pub struct IntentPersistence;

impl IntentPersistence {
    const INTENT_FILE: &'static str = "intent_log.json";
    const INTENT_FILE_TEMP: &'static str = "intent_log.json.tmp";

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
        let intents: Vec<IntentLogEntry> = serde_json::from_slice(&data).map_err(|e| {
            if recovery_policy == crate::engine::RecoveryPolicy::Strict {
                format!("failed to parse intent JSON: {}", e)
            } else {
                format!("failed to parse intent JSON (salvage mode): {}", e)
            }
        })?;

        tracing::debug!(path = ?p, entries = intents.len(), "intent log loaded");
        Ok(intents)
    }

    pub fn load(db_path: &Path) -> Result<Vec<IntentLogEntry>, String> {
        Self::load_with_policy(db_path, crate::engine::RecoveryPolicy::Strict)
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

    pub fn save_with_fs(
        fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
        intents: &[IntentLogEntry],
    ) -> Result<(), String> {
        use crate::io::staging;
        use crate::io::traits::FsPath;

        let json = serde_json::to_vec_pretty(intents)
            .map_err(|e| format!("failed to serialize intent log to JSON: {}", e))?;

        let temp = FsPath::new(Self::INTENT_FILE_TEMP);
        let target = FsPath::new(Self::INTENT_FILE);
        staging::stage_bytes_with_hook(
            fs,
            &temp,
            &target,
            &json,
            || {
                fail::fail_point!("midge::intent::inject_no_space_on_save", |_| Err(
                    "failpoint: no space while saving intent log".to_string()
                ));
                Ok(())
            },
            |msg| msg,
        )?;

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
        let temp = FsPath::new(Self::INTENT_FILE_TEMP);
        match fs.exists(&p) {
            Ok(false) => {}
            Err(e) => return Err(format!("fs exists error: {:?}", e)),
            Ok(true) => {}
        }

        if fs
            .exists(&p)
            .map_err(|e| format!("fs exists error: {:?}", e))?
        {
            fs.remove_file(&p)
                .map_err(|e| format!("failed to delete intent file: {:?}", e))?;
        }
        if fs
            .exists(&temp)
            .map_err(|e| format!("fs exists error: {:?}", e))?
        {
            fs.remove_file(&temp)
                .map_err(|e| format!("failed to delete temp intent file: {:?}", e))?;
        }
        tracing::debug!(path = ?p, temp_path = ?temp, "intent file deleted");
        Ok(())
    }

    pub fn delete(db_path: &Path) -> Result<(), String> {
        let p = Self::intent_path(db_path);
        let temp = db_path.join(Self::INTENT_FILE_TEMP);
        if p.exists() {
            fs::remove_file(&p).map_err(|e| format!("failed to delete intent file: {}", e))?;
        }
        if temp.exists() {
            fs::remove_file(&temp)
                .map_err(|e| format!("failed to delete temp intent file: {}", e))?;
        }
        tracing::debug!(path = ?p, temp_path = ?temp, "intent file deleted");
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
    fn should_fail_default_load_when_intent_log_is_corrupt() {
        // Arrange
        let test_dir = create_test_dir();
        std::fs::write(test_dir.join("intent_log.json"), "not-json")
            .expect("write corrupt intent log");

        // Act
        let error =
            IntentPersistence::load(&test_dir).expect_err("default intent load must be strict");

        // Assert
        assert!(
            error.contains("failed to parse intent JSON"),
            "expected strict intent parse error, got: {error}"
        );
    }

    #[test]
    fn should_ignore_temp_intent_file_when_loading_empty_state() {
        // Arrange
        let test_dir = create_test_dir();
        std::fs::write(
            test_dir.join(IntentPersistence::INTENT_FILE_TEMP),
            br#"[{"WalSynced":{"segment_id":7,"seqno":11}}]"#,
        )
        .expect("write temp intent log");

        // Act
        let loaded = IntentPersistence::load(&test_dir).expect("load should succeed");

        // Assert
        assert!(
            loaded.is_empty(),
            "temp intent log must not become authoritative on load"
        );
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
        assert!(
            !test_dir.join(IntentPersistence::INTENT_FILE_TEMP).exists(),
            "intent staging temp file should not remain after save"
        );
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
        assert!(
            IntentPersistence::intent_path(&test_dir).ends_with("intent_log.json"),
            "intent log should persist to intent_log.json"
        );
    }

    #[test]
    fn should_delete_temp_intent_file_when_requested() {
        // Arrange
        let test_dir = create_test_dir();
        let temp_path = test_dir.join(IntentPersistence::INTENT_FILE_TEMP);
        std::fs::write(
            &temp_path,
            br#"[{"WalSynced":{"segment_id":7,"seqno":11}}]"#,
        )
        .expect("write temp intent log");

        // Act
        IntentPersistence::delete(&test_dir).expect("delete should succeed");

        // Assert
        assert!(
            !temp_path.exists(),
            "temp intent log should not exist after delete"
        );
    }

    #[test]
    fn should_return_empty_intent_log_after_delete_when_temp_file_exists() {
        // Arrange
        let test_dir = create_test_dir();
        let intents = vec![IntentLogEntry::WalSynced {
            segment_id: 1,
            seqno: 42,
        }];
        IntentPersistence::save(&test_dir, &intents).expect("save should succeed");
        std::fs::write(
            test_dir.join(IntentPersistence::INTENT_FILE_TEMP),
            br#"[{"WalSynced":{"segment_id":9,"seqno":99}}]"#,
        )
        .expect("write temp intent log");

        // Act
        IntentPersistence::delete(&test_dir).expect("delete should succeed");
        let loaded = IntentPersistence::load(&test_dir).expect("load should succeed");

        // Assert
        assert!(
            loaded.is_empty(),
            "load after delete should not recover intent entries from temp residue"
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
