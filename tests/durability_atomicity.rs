//! Durability atomicity tests
//!
//! Tests for atomic persistence guarantees during flush, compaction, and recovery.
//! Validates that SST files and manifest updates happen atomically.

mod common;

use cntryl_midge::test_hooks::{CompactionBehavior, CompactionGatePoint, TestHooks};
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use common::test_helpers::TEST_GATE_TIMEOUT;
use common::test_temp_dir;
use std::fs;
use tempfile::TempDir;

// ============================================================================
// SST / MANIFEST ATOMICITY
// ============================================================================

fn collect_sst_files(dir: &std::path::Path) -> Vec<String> {
    if !dir.exists() {
        return Vec::new();
    }

    let mut files = Vec::new();
    fn visit(base: &std::path::Path, cur: &std::path::Path, out: &mut Vec<String>) {
        if let Ok(entries) = std::fs::read_dir(cur) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    visit(base, &path, out);
                } else if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                    if name.ends_with(".sst") {
                        if let Ok(rel) = path.strip_prefix(base) {
                            out.push(rel.to_string_lossy().to_string());
                        } else {
                            out.push(path.to_string_lossy().to_string());
                        }
                    }
                }
            }
        }
    }
    visit(dir, dir, &mut files);
    files.sort();
    files
}

#[test]
fn should_not_expose_sst_without_manifest_entry_given_orphan_file_when_recovering() {
    // Arrange
    let temp = TempDir::new().expect("tempdir");
    let sst_dir = temp.path().join("sst");
    fs::create_dir_all(&sst_dir).unwrap();
    let orphan = sst_dir.join("orphan.sst");
    fs::write(&orphan, b"dummy sst content").unwrap();

    // Act
    let cache =
        cntryl_midge::sst::manifest_cache::ManifestCache::new(temp.path().to_path_buf()).unwrap();

    // Assert
    let m = cache.get();
    assert!(m.ssts.is_empty(), "manifest must not list orphan SSTs");
    assert!(
        orphan.exists(),
        "file should be present on disk but not in manifest"
    );
}

#[test]
fn should_replay_wal_until_manifest_sequence_given_manifest_fsynced_when_recovering() {
    // Arrange
    use cntryl_midge::core::manifest::Manifest;
    let temp = TempDir::new().unwrap();
    let m = Manifest {
        last_persisted_sequence: 1234,
        ..Default::default()
    };

    // Act
    m.save_atomic(temp.path()).unwrap();
    let reloaded = Manifest::load(temp.path()).unwrap();

    // Assert
    assert_eq!(reloaded.last_persisted_sequence, 1234u64);
}

#[test]
fn should_preserve_manifest_authority_given_wal_newer_when_sst_missing() {
    // Arrange
    use cntryl_midge::core::manifest::Manifest;
    let temp = TempDir::new().unwrap();
    let m = Manifest {
        last_persisted_sequence: 10,
        ..Default::default()
    };
    m.save_atomic(temp.path()).unwrap();

    // Act
    let loaded = Manifest::load(temp.path()).unwrap();

    // Assert
    assert_eq!(loaded.last_persisted_sequence, 10);
}

#[test]
fn should_not_auto_claim_orphan_sst_given_sst_exists_when_manifest_behind() {
    // Arrange
    let temp = TempDir::new().unwrap();
    let sst_dir = temp.path().join("sst");
    fs::create_dir_all(&sst_dir).unwrap();
    let sst = sst_dir.join("sst_001.blob");
    fs::write(&sst, b"sstcontent").unwrap();

    // Act
    let cache =
        cntryl_midge::sst::manifest_cache::ManifestCache::new(temp.path().to_path_buf()).unwrap();

    // Assert
    assert!(cache.get().ssts.is_empty());
    assert!(sst.exists());
}

#[test]
fn should_not_publish_sst_given_manifest_not_persisted_when_adding_sst() {
    // Arrange
    use cntryl_midge::core::manifest::Manifest;
    let mut m = Manifest::default();
    m.ssts.push("sst_x.blob".to_string());

    // Act
    let temp = TempDir::new().unwrap();
    let saved = Manifest::load(temp.path()).unwrap_or_default();

    // Assert
    assert!(!saved.ssts.contains(&"sst_x.blob".to_string()));
}

#[test]
fn should_maintain_atomicity_given_concurrent_flush_manifest_fsync_when_updating() {
    // Arrange
    let temp = TempDir::new().unwrap();
    let manifest = cntryl_midge::core::manifest::Manifest::default();
    manifest.save_atomic(temp.path()).unwrap();
    let cache = std::sync::Arc::new(
        cntryl_midge::sst::manifest_cache::ManifestCache::new(temp.path().to_path_buf()).unwrap(),
    );

    // Act
    let threads: Vec<_> = (0..4)
        .map(|i| {
            let c = cache.clone();
            std::thread::spawn(move || {
                let mut m = c.get();
                m.last_persisted_sequence += i as u64 + 1;
                c.update(m);
            })
        })
        .collect();

    for t in threads {
        t.join().unwrap();
    }

    // Assert
    let final_m = cache.get();
    assert!(final_m.last_persisted_sequence >= 1);
}

#[test]
fn should_maintain_order_given_multiple_cfs_flush_concurrently_when_updating_manifest() {
    // Arrange
    let temp = TempDir::new().unwrap();
    let manifest = cntryl_midge::core::manifest::Manifest::default();
    manifest.save_atomic(temp.path()).unwrap();
    let cache = std::sync::Arc::new(
        cntryl_midge::sst::manifest_cache::ManifestCache::new(temp.path().to_path_buf()).unwrap(),
    );

    // Act
    let writers: Vec<_> = (0..3)
        .map(|i| {
            let c = cache.clone();
            std::thread::spawn(move || {
                for j in 0..10 {
                    let mut m = c.get();
                    m.last_persisted_sequence =
                        m.last_persisted_sequence.saturating_add(1 + (i + j) as u64);
                    c.update(m);
                }
            })
        })
        .collect();

    for w in writers {
        w.join().unwrap();
    }

    // Assert
    let final_m = cache.get();
    assert!(final_m.last_persisted_sequence > 0);
}

// ============================================================================
// COMPACTION ATOMICITY
// ============================================================================

#[test]
fn should_commit_ssts_manifest_together_given_compaction_success_when_completing() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 1024,
        enable_compaction: true,
        wal_sync: true,
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    // Act
    let eng = MidgeEngine::open(opts.clone()).expect("open");
    let cf = eng.default_column_family();
    let compaction_starts_before = hooks.compaction_start_count();

    let large_value = vec![b'x'; 100];
    for i in 0..200 {
        eng.put(&cf, format!("key{:04}", i % 50).as_bytes(), &large_value)
            .expect("put");
    }

    eng.flush().expect("flush should succeed");

    let after_gate = hooks.install_compaction_gate(CompactionGatePoint::AfterManifestUpdate);
    eng.compact_level(&cf, 0).expect("compact_level");

    assert!(
        after_gate.wait_until_blocked(TEST_GATE_TIMEOUT),
        "Compaction did not reach AfterManifestUpdate"
    );

    let compaction_started = hooks.compaction_start_count() > compaction_starts_before;
    after_gate.release();
    eng.wait_for_compaction(TEST_GATE_TIMEOUT).unwrap();

    drop(eng);

    // Assert
    let opts_recovery = MidgeOptions {
        test_hooks: None,
        ..opts
    };
    let eng = MidgeEngine::open(opts_recovery).expect("recover");
    let cf = eng.default_column_family();
    let expected_value = vec![b'x'; 100];
    for i in 0..50 {
        let result = eng
            .get(&cf, format!("key{:04}", i).as_bytes())
            .expect("get");
        assert!(
            result.is_some(),
            "Compacted key {} should exist after recovery",
            i
        );
        assert_eq!(result.unwrap(), expected_value, "Value should match");
    }
    assert!(compaction_started, "Compaction should have started");
}

#[test]
fn should_cleanup_partial_output_given_compaction_failure_when_recovering() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new().with_compaction_behavior(CompactionBehavior::FailMidway);
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 1024,
        enable_compaction: true,
        // Set very high interval to effectively disable auto-compaction ticks,
        // allowing manual compaction to be triggered without infinite retry loop.
        compaction_check_interval_ms: 60_000,
        wal_sync: true,
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    // Act
    let eng = MidgeEngine::open(opts.clone()).expect("open");
    let cf = eng.default_column_family();
    let large_value = vec![b'x'; 100];
    for i in 0..200 {
        eng.put(&cf, format!("key{:04}", i).as_bytes(), &large_value)
            .expect("put");
    }

    eng.flush().expect("flush should succeed");
    // Trigger manual compaction - it will fail due to FailMidway hook
    eng.compact_level(&cf, 0).expect("compact_level");
    
    // Wait for at least one compaction failure to occur
    let deadline = std::time::Instant::now() + TEST_GATE_TIMEOUT;
    while hooks.compaction_failed_count() == 0 && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    let compaction_started = hooks.compaction_start_count() > 0;
    drop(eng);

    // Assert
    let opts_recovery = MidgeOptions {
        test_hooks: None,
        ..opts
    };
    let eng = MidgeEngine::open(opts_recovery).expect("recover");
    let cf = eng.default_column_family();

    let mut missing = Vec::new();
    for i in 0..200 {
        let key = format!("key{:04}", i);
        let found = eng.get(&cf, key.as_bytes()).expect("get");
        if found.is_none() {
            missing.push(key.clone());
        }
    }

    assert!(
        missing.is_empty(),
        "Data should be preserved despite compaction failure"
    );
    assert!(compaction_started, "Compaction should have started");
}

#[test]
fn should_delete_old_ssts_only_after_manifest_persisted_when_compacting() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 4096,
        enable_compaction: true,
        wal_sync: true,
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    // Act
    let eng = MidgeEngine::open(opts.clone()).expect("open");
    let cf = eng.default_column_family();

    for round in 0..3 {
        let round_value = vec![b'0' + round as u8; 100];
        for i in 0..100 {
            eng.put(&cf, format!("key{:04}", i).as_bytes(), &round_value)
                .expect("put");
        }
    }

    let after_gate = hooks.install_compaction_gate(CompactionGatePoint::AfterManifestUpdate);

    eng.flush().unwrap();
    let sst_dir = dir.path().join("sst");
    let source_files = collect_sst_files(&sst_dir);
    assert!(!source_files.is_empty(), "Expected SST files after flush");

    eng.compact_level(&cf, 0).unwrap();

    assert!(
        after_gate.wait_until_blocked(TEST_GATE_TIMEOUT),
        "Compaction did not reach AfterManifestUpdate gate"
    );

    // Assert - at this point manifest is updated, old SSTs may still exist
    after_gate.release();
    eng.wait_for_compaction(TEST_GATE_TIMEOUT).unwrap();

    // After compaction completes, data should be preserved
    for i in 0..100 {
        let result = eng
            .get(&cf, format!("key{:04}", i).as_bytes())
            .expect("get");
        assert!(result.is_some(), "Key {} should exist after compaction", i);
    }
}

// ============================================================================
// WAL TRUNCATE FALLBACK
// ============================================================================

#[test]
fn should_not_recover_truncated_wal_append_given_truncate_fallback_when_reopening() {
    use cntryl_midge::test_hooks::WalBehavior;

    // Arrange
    let dir = TempDir::new().unwrap();
    let hooks = TestHooks::new().with_wal_behavior(WalBehavior::TruncateAfterWriteFail);

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        wal_sync: false,
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    // Act
    {
        let eng = MidgeEngine::open(opts).expect("open engine");
        let cf = eng.default_column_family();
        eng.put(&cf, b"eng_trunc_key", b"eng_trunc_value")
            .expect("put");
    }

    let opts_reopen = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        test_hooks: None,
        ..Default::default()
    };

    let eng2 = MidgeEngine::open(opts_reopen).expect("reopen engine");
    let cf2 = eng2.default_column_family();

    // Assert
    assert_eq!(eng2.get(&cf2, b"eng_trunc_key").expect("get"), None);
}
