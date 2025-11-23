mod common;
use cntryl_midge::test_hooks::{CompactionBehavior, CompactionGatePoint, TestHooks};
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use common::test_temp_dir;

fn collect_sst_files(dir: &std::path::Path) -> Vec<String> {
    if !dir.exists() {
        return Vec::new();
    }

    let mut files = Vec::new();
    // Recursive traversal: collect SST files in nested directories too
    fn visit(base: &std::path::Path, cur: &std::path::Path, out: &mut Vec<String>) {
        if let Ok(entries) = std::fs::read_dir(cur) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    visit(base, &path, out);
                } else if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                    if name.ends_with(".sst") {
                        // Push path relative to base so callers can join with base
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
fn should_commit_new_ssts_manifest_together_on_compaction_success() {
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

    // Act - Write overlapping keys to trigger compaction
    let eng = MidgeEngine::open(opts.clone()).expect("open");
    let cf = eng.default_column_family();
    let compaction_starts_before = hooks.compaction_start_count();
    // Write larger values to exceed memtable_size and trigger flushes
    let large_value = vec![b'x'; 100]; // 100 bytes per value
    for i in 0..200 {
        eng.put(&cf, format!("key{:04}", i % 50).as_bytes(), &large_value)
            .expect("put");
    }
    // Wait for compaction to trigger
    eng.wait_for_compaction(std::time::Duration::from_secs(2))
        .expect("compaction should complete");
    let compaction_complete = hooks.compaction_start_count() > compaction_starts_before;

    // DEBUG: Check compaction trigger counts
    if !compaction_complete {
        tracing::debug!(
            "Compaction didn't start. Starts: {} (before: {})",
            hooks.compaction_start_count(),
            compaction_starts_before
        );
    }

    drop(eng);

    // Assert - all latest values should be present after restart
    let opts_recovery = MidgeOptions {
        test_hooks: None,
        ..opts
    };
    let eng = MidgeEngine::open(opts_recovery).expect("recover");
    let cf = eng.default_column_family();
    // DEBUG: inspect manifest after recovery to understand which files were loaded
    match cntryl_midge::manifest::Manifest::load(&dir.path()) {
        Ok(m) => {
            eprintln!("[DEBUG] manifest after recovery: ssts={} files={}", m.ssts.len(), m.files.len());
            for f in &m.files {
                eprintln!("[DEBUG] file meta after recovery: name={} seq={} entries={} smallest_seq={:?} largest_seq={:?}", f.name, f.sst_seq, f.total_entries, f.smallest_seq, f.largest_seq);
            }
        }
        Err(e) => eprintln!("[DEBUG] failed to load manifest after recovery: {}", e),
    }
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
    assert!(compaction_complete, "Compaction should have started");
}

#[test]
fn should_cleanup_partial_output_given_compaction_failure() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new().with_compaction_behavior(CompactionBehavior::FailMidway);
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

    // Act - Write data and trigger compaction with failure injection
    let eng = MidgeEngine::open(opts.clone()).expect("open");
    let cf = eng.default_column_family();
    let large_value = vec![b'x'; 100]; // 100 bytes per value
    for i in 0..200 {
        eng.put(&cf, format!("key{:04}", i).as_bytes(), &large_value)
            .expect("put");
    }
    // Wait for compaction attempt to execute
    eng.wait_for_compaction(std::time::Duration::from_secs(2))
        .expect("compaction should complete");
    let compaction_started = hooks.compaction_start_count() > 0;
    let compaction_failed = hooks.compaction_failed_count() > 0;

    // DEBUG: record current SST file list + manifest snapshot prior to closing engine
    let sst_dir = dir.path().join("sst");
    if sst_dir.exists() {
        // Recursively list SST files in column family directories for better visibility
        let mut all_files: Vec<String> = vec![];
        for entry in std::fs::read_dir(&sst_dir).unwrap().flatten() {
            if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                let cf_dir = entry.path();
                for f in std::fs::read_dir(&cf_dir).unwrap().flatten() {
                    if let Some(name) = f.file_name().to_str() {
                        all_files.push(format!("{}/{}", cf_dir.file_name().unwrap().to_string_lossy(), name));
                    }
                }
            } else if let Some(name) = entry.file_name().to_str() {
                all_files.push(name.to_string());
            }
        }
        eprintln!("[DEBUG] sst files before drop (count={}): {:?}", all_files.len(), all_files);

        // Print sizes for each SST file and sample a few keys per SST for diagnostics
        for f in &all_files {
            let p = dir.path().join("sst").join(f);
            match std::fs::metadata(&p) {
                Ok(md) => eprintln!("[DEBUG] sst {} size={} bytes", f, md.len()),
                Err(e) => eprintln!("[DEBUG] sst {} missing metadata: {}", f, e),
            }
            // Try to open the SST and inspect a few entries
            match cntryl_midge::sst::SstFile::open(&p) {
                Ok(sst) => match cntryl_midge::sst::SstStateReader::scan_range_state(&sst, None, None) {
                    Ok(rows) => {
                        eprintln!("[DEBUG] sst {} rows_count={}", f, rows.len());
                        if !rows.is_empty() {
                            let sample_keys: Vec<_> = rows.iter().take(3).map(|(k, _)| String::from_utf8_lossy(k).to_string()).collect();
                            eprintln!("[DEBUG] sst {} sample_keys={:?}", f, sample_keys);
                        }
                    }
                    Err(e) => eprintln!("[DEBUG] sst scan failed {}: {}", f, e),
                },
                Err(e) => eprintln!("[DEBUG] sst open failed {}: {}", f, e),
            }
        }
    // Check whether keys are present before closing engine - helps determine if loss occurred before or during recovery
    let mut missing_before = Vec::new();
    for i in 0..200 {
        let key = format!("key{:04}", i);
        let found = eng.get(&cf, key.as_bytes()).expect("get");
        if found.is_none() {
            missing_before.push(key);
        }
    }
    eprintln!("[DEBUG] missing keys before drop count = {}", missing_before.len());
    } else {
        eprintln!("[DEBUG] sst dir missing before drop");
    }

    match cntryl_midge::manifest::Manifest::load(&dir.path()) {
        Ok(m) => eprintln!("[DEBUG] manifest before drop: ssts={} files={}", m.ssts.len(), m.files.len()),
        Err(e) => eprintln!("[DEBUG] failed to load manifest before drop: {}", e),
    }
    // List WAL files before shutdown
    let wal_dir = dir.path().join("wal");
    if wal_dir.exists() {
        let wal_files: Vec<_> = std::fs::read_dir(&wal_dir)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        eprintln!("[DEBUG] wal files before drop: {:?}", wal_files);
    } else {
        eprintln!("[DEBUG] wal dir missing before drop");
    }
    // After recovery we'll also inspect SST files on disk to compare their content
    drop(eng);

    // Assert - database should be consistent (no orphaned partial SSTs)
    // Reopen with clean hooks (no failure injection) to verify recovery
    let opts_recovery = MidgeOptions {
        test_hooks: None,
        ..opts
    };
    eprintln!("[DEBUG] compaction_started={} compaction_failed={}", compaction_started, compaction_failed);
    let eng = MidgeEngine::open(opts_recovery).expect("recover");
    let cf = eng.default_column_family();
    // DEBUG: inspect manifest and WAL after recovery
    match cntryl_midge::manifest::Manifest::load(&dir.path()) {
        Ok(m) => {
            eprintln!("[DEBUG] manifest after recovery: last_persisted_seq={} ssts={} files={}", m.last_persisted_sequence, m.ssts.len(), m.files.len());
            for f in &m.files {
                eprintln!("[DEBUG] file meta after recovery: name={} seq={} entries={} smallest_seq={:?} largest_seq={:?}", f.name, f.sst_seq, f.total_entries, f.smallest_seq, f.largest_seq);
            }
        }
        Err(e) => eprintln!("[DEBUG] failed to load manifest after recovery: {}", e),
    }
    let wal_dir = dir.path().join("wal");
    if wal_dir.exists() {
        let wal_files: Vec<_> = std::fs::read_dir(&wal_dir)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        eprintln!("[DEBUG] wal files after recovery: {:?}", wal_files);
    } else {
        eprintln!("[DEBUG] wal dir missing after recovery");
    }
    // Inspect recovered SST files and print a small key sample for each
    let rec_sst_dir = dir.path().join("sst");
    if rec_sst_dir.exists() {
        let mut recovered_files: Vec<String> = vec![];
        for entry in std::fs::read_dir(&rec_sst_dir).unwrap().flatten() {
            if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                let cf_dir = entry.path();
                for f in std::fs::read_dir(&cf_dir).unwrap().flatten() {
                    if let Some(name) = f.file_name().to_str() {
                        recovered_files.push(format!("{}/{}", cf_dir.file_name().unwrap().to_string_lossy(), name));
                    }
                }
            } else if let Some(name) = entry.file_name().to_str() {
                recovered_files.push(name.to_string());
            }
        }
        eprintln!("[DEBUG] recovered sst files (count={}): {:?}", recovered_files.len(), recovered_files);
        for f in &recovered_files {
            let p = rec_sst_dir.join(f);
            match cntryl_midge::sst::SstFile::open(&p) {
                Ok(sst) => match cntryl_midge::sst::SstStateReader::scan_range_state(&sst, None, None) {
                    Ok(rows) => {
                        eprintln!("[DEBUG] recovered sst {} rows_count={}", f, rows.len());
                        if !rows.is_empty() {
                            let sample_keys: Vec<_> = rows.iter().take(3).map(|(k, _)| String::from_utf8_lossy(k).to_string()).collect();
                            eprintln!("[DEBUG] recovered sst {} sample_keys={:?}", f, sample_keys);
                        }
                    }
                    Err(e) => eprintln!("[DEBUG] recovered sst scan failed {}: {}", f, e),
                },
                Err(e) => eprintln!("[DEBUG] recovered sst open failed {}: {}", f, e),
            }
        }
    }
    // If any key is missing after recovery, log which one(s) and fail loudly.
    let mut missing = Vec::new();
    for i in 0..200 {
        let key = format!("key{:04}", i);
        let found = eng.get(&cf, key.as_bytes()).expect("get");
        if found.is_none() {
            missing.push(key.clone());
        }
    }

    if !missing.is_empty() {
        eprintln!("Missing keys after recovery (count = {}): {:?}", missing.len(), missing);
    }
    assert!(missing.is_empty(), "Data should be preserved despite compaction failure");
    assert!(compaction_started, "Compaction should have started");
}

#[test]
fn should_delete_old_sst_files_only_after_manifest_persisted() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 4096, // Increased from 1024
        enable_compaction: true,
        wal_sync: true,
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };

    // Act - Write data to create multiple SSTs and trigger compaction
    let eng = MidgeEngine::open(opts.clone()).expect("open");
    let cf = eng.default_column_family();
    let compaction_starts_before = hooks.compaction_start_count();
    for round in 0..3 {
        let round_value = vec![b'0' + round as u8; 100]; // 100 bytes per value
        for i in 0..100 {
            eng.put(&cf, format!("key{:04}", i).as_bytes(), &round_value)
                .expect("put");
        }
    }
    eng.flush().unwrap(); // Force flush to create SSTs and trigger compaction
                          // Manually trigger compaction of level 0
    eng.compact_level(&cf, 0).unwrap();
    // Wait for compaction to complete - use stability-aware wait
    eng.wait_for_compaction(std::time::Duration::from_secs(10))
        .expect("compaction should complete");
    let compaction_started = hooks.compaction_start_count() > compaction_starts_before;
    let compaction_completed = hooks.compaction_complete_count() > 0;

    // Wait for manifest to be updated
    let manifest_updates_before = hooks.manifest_update_count();
    for _ in 0..50 {
        // Wait up to 5 seconds for manifest update
        if hooks.manifest_update_count() > manifest_updates_before {
            break;
        }
        // Poll briefly to fail fast but avoid busy spin
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    drop(eng);

    // Assert - compaction should have started and completed
    assert!(compaction_started, "Compaction should have started");
    assert!(compaction_completed, "Compaction should have completed");

    // Assert - latest values should be present, old SSTs should be cleaned
    let opts_recovery = MidgeOptions {
        test_hooks: None,
        ..opts
    };
    let eng = MidgeEngine::open(opts_recovery).expect("recover");
    let expected_value = vec![b'2'; 100];
    let cf = eng.default_column_family();

    // Retry the check a few times in case of timing issues
    let mut all_correct = false;
    for _ in 0..3 {
        all_correct = true;
        for i in 0..100 {
            let result = eng
                .get(&cf, format!("key{:04}", i).as_bytes())
                .expect("get");
            if result.as_ref().map(|v| v.as_ref()) != Some(expected_value.as_slice()) {
                all_correct = false;
                break;
            }
        }
        if all_correct {
            break;
        }
        // Give a short pause between retries to avoid tight busy loops
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    assert!(
        all_correct,
        "All keys should have the latest value after compaction"
    );
}

#[test]
fn should_keep_source_ssts_present_until_manifest_persisted() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new();
    let before_gate = hooks.install_compaction_gate(CompactionGatePoint::BeforeExecution);
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
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();
    let sst_dir = dir.path().join("sst");

    // Act
    for round in 0..3 {
        let value = vec![b'a' + round as u8; 128];
        for i in 0..64 {
            eng.put(&cf, format!("key{:04}", i).as_bytes(), &value)
                .expect("put");
        }
    }

    // Force a flush so SST files exist deterministically before compaction begins.
    eng.flush().expect("flush should succeed");

    assert!(
        before_gate.wait_until_blocked(std::time::Duration::from_secs(5)),
        "Compaction should reach the BeforeExecution gate"
    );
    let source_files = collect_sst_files(&sst_dir);
    if source_files.is_empty() {
        // Diagnostics: dump sst dir and manifest to help debug timing/hang
        if sst_dir.exists() {
            eprintln!("DEBUG: sst_dir exists {}", sst_dir.display());
            match std::fs::read_dir(&sst_dir) {
                Ok(entries) => {
                    eprintln!("DEBUG: sst_dir listing:");
                    for ent in entries.flatten() {
                        eprintln!(" - {}", ent.path().display());
                    }
                }
                Err(e) => eprintln!("DEBUG: read_dir error: {}", e),
            }
        } else {
            eprintln!("DEBUG: sst_dir does not exist: {}", sst_dir.display());
        }

        // Show manifest if loadable
        match cntryl_midge::manifest::Manifest::load(&dir.path()) {
            Ok(m) => eprintln!("DEBUG: manifest ssts={} files={:?}", m.ssts.len(), m.files.iter().map(|f| &f.name).collect::<Vec<_>>()),
            Err(e) => eprintln!("DEBUG: manifest load error: {}", e),
        }

        panic!("Expected flushed SSTs before compaction proceeds (diagnostics above)");
    }

    let after_gate = hooks.install_compaction_gate(CompactionGatePoint::AfterManifestUpdate);
    before_gate.release();

    assert!(
        after_gate.wait_until_blocked(std::time::Duration::from_secs(5)),
        "Compaction should reach the AfterManifestUpdate gate"
    );

    // Assert
    for file in &source_files {
        assert!(
            sst_dir.join(file).exists(),
            "Source SST {} should remain until manifest persistence completes",
            file
        );
    }

    after_gate.release();
    eng.wait_for_compaction(std::time::Duration::from_secs(2))
        .expect("compaction should complete");
}

#[test]
fn should_fsync_new_ssts_before_updating_manifest() {
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

    // Act - Write overlapping keys and trigger compaction
    let eng = MidgeEngine::open(opts.clone()).expect("open");
    let cf = eng.default_column_family();
    let fsync_count_before = hooks.fsync_count();
    let compaction_starts_before = hooks.compaction_start_count();
    let large_value = vec![b'x'; 100]; // 100 bytes per value
    for i in 0..200 {
        eng.put(&cf, format!("key{:04}", i % 50).as_bytes(), &large_value)
            .expect("put");
    }
    // Wait for compaction
    eng.wait_for_compaction(std::time::Duration::from_secs(2))
        .expect("compaction should complete");
    let fsync_count_after = hooks.fsync_count();
    let compaction_completed = hooks.compaction_complete_count() > compaction_starts_before;
    drop(eng);

    // Assert - compacted data should be durable (new SSTs were fsynced)
    let opts_recovery = MidgeOptions {
        test_hooks: None,
        ..opts
    };
    let eng = MidgeEngine::open(opts_recovery).expect("recover");
    let cf = eng.default_column_family();
    for i in 0..50 {
        let result = eng
            .get(&cf, format!("key{:04}", i).as_bytes())
            .expect("get");
        assert!(result.is_some(), "Compacted key should be durable");
    }
    assert!(
        fsync_count_after >= fsync_count_before,
        "SST fsync should have occurred"
    );
    assert!(compaction_completed, "Compaction should have completed");
}

#[test]
fn should_recover_consistent_state_given_crash_mid_compaction_when_restart() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new().with_compaction_behavior(CompactionBehavior::CrashBeforeFsync);
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

    // Act - Write data and simulate crash during compaction
    let eng = MidgeEngine::open(opts.clone()).expect("open");
    let cf = eng.default_column_family();
    let large_value = vec![b'x'; 100]; // 100 bytes per value
    for i in 0..200 {
        eng.put(&cf, format!("key{:04}", i).as_bytes(), &large_value)
            .expect("put");
    }
    // Wait for compaction to reach crash point
    eng.wait_for_compaction(std::time::Duration::from_secs(2))
        .expect("compaction should complete");
    let compaction_attempted = hooks.compaction_start_count() > 0;
    drop(eng);

    // Assert - all data should be present after recovery (either from old SSTs or WAL)
    let opts_recovery = MidgeOptions {
        test_hooks: None,
        ..opts
    };
    let eng = MidgeEngine::open(opts_recovery).expect("recover");
    let cf = eng.default_column_family();
    for i in 0..200 {
        let result = eng
            .get(&cf, format!("key{:04}", i).as_bytes())
            .expect("get");
        assert!(result.is_some(), "Data should survive crash mid-compaction");
    }
    assert!(
        compaction_attempted,
        "Compaction should have been attempted"
    );
}

#[test]
fn should_preserve_source_ssts_when_compaction_output_not_fsynced() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new().with_compaction_behavior(CompactionBehavior::CrashBeforeFsync);
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

    // Act - Write data and simulate crash before compaction output fsync
    let eng = MidgeEngine::open(opts.clone()).expect("open");
    let cf = eng.default_column_family();
    let large_value = vec![b'x'; 100]; // 100 bytes per value
    for i in 0..200 {
        eng.put(&cf, format!("key{:04}", i).as_bytes(), &large_value)
            .expect("put");
    }
    // Wait for compaction to reach crash point
    eng.wait_for_compaction(std::time::Duration::from_secs(2))
        .expect("compaction should complete");
    let compaction_attempted = hooks.compaction_start_count() > 0;
    drop(eng);

    // Assert - data should be recoverable from source SSTs
    let opts_recovery = MidgeOptions {
        test_hooks: None,
        ..opts
    };
    let eng = MidgeEngine::open(opts_recovery).expect("recover");
    let cf = eng.default_column_family();
    for i in 0..200 {
        let result = eng
            .get(&cf, format!("key{:04}", i).as_bytes())
            .expect("get");
        assert!(result.is_some(), "Source SSTs should preserve data");
    }
    assert!(
        compaction_attempted,
        "Compaction should have been attempted"
    );
}
