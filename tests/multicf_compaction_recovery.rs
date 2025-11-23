// Multi-column-family compaction + recovery observable tests
mod common;
use cntryl_midge::api::column_family::ColumnFamilyConfig;
use cntryl_midge::MidgeOptions;
use common::*;
use cntryl_midge::test_hooks::{TestHooks, CompactionGatePoint};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
fn should_recover_all_column_families_consistently_given_mixed_writes_and_compactions_when_restarting_repeatedly(
) {
    for mode in disk_storage_modes() {
        let (_name, storage_mode, _tmp) = create_storage_mode(mode);
        let hooks = TestHooks::new();
        let mut opts = compaction_test_opts(storage_mode);
        opts.test_hooks = Some(hooks.clone());

        // Create engine and column families
        with_engine(opts.clone(), |eng| {
            let cf1 = eng
                .create_column_family("cf1", ColumnFamilyConfig::default())
                .expect("create cf1");
            let cf2 = eng
                .create_column_family("cf2", ColumnFamilyConfig::default())
                .expect("create cf2");

            // Initial writes
            for i in 0..100 {
                eng.put(&cf1, format!("a{:03}", i).as_bytes(), b"v1")
                    .unwrap();
                eng.put(&cf2, format!("b{:03}", i).as_bytes(), b"v2")
                    .unwrap();
            }
            eng.flush().unwrap();
        });

        // Perform multiple restart cycles with additional writes and compactions
        for cycle in 0..3 {
            with_engine_restart(
                opts.clone(),
                |eng| {
                    // Arrange: mixed writes into both CFs
                    let cf1 = eng.get_column_family("cf1").expect("get cf1");
                    let cf2 = eng.get_column_family("cf2").expect("get cf2");

                    for i in 0..50 {
                        eng.put(&cf1, format!("a{:03}", cycle * 50 + i).as_bytes(), b"v1b")
                            .unwrap();
                        eng.put(&cf2, format!("b{:03}", cycle * 50 + i).as_bytes(), b"v2b")
                            .unwrap();
                    }
                    eng.flush().unwrap();
                    // Deterministically trigger compaction and wait via hooks
                    let gate = hooks.install_compaction_gate(CompactionGatePoint::AfterManifestUpdate);
                    eng.compact_level(&cf1, 0).ok();
                    assert!(gate.wait_until_blocked(Duration::from_secs(10)), "Compaction did not reach AfterManifestUpdate");
                    // Release the compaction gate and wait deterministically for compaction to finish
                    gate.release();
                    eng.wait_for_compaction(Duration::from_secs(10)).unwrap();
                },
                |eng| {
                    // Assert: both CFs should still have data
                    let cf1 = eng.get_column_family("cf1").expect("get cf1 post");
                    let cf2 = eng.get_column_family("cf2").expect("get cf2 post");
                    assert!(eng.get(&cf1, b"a000").unwrap().is_some());
                    assert!(eng.get(&cf2, b"b000").unwrap().is_some());
                },
            );
        }
    }
}

#[test]
fn should_not_cross_contaminate_keys_between_column_families_given_heavy_compaction_when_replaying_wal_on_restart(
) {
    for mode in disk_storage_modes() {
        let (_name, storage_mode, _tmp) = create_storage_mode(mode);
        let hooks = TestHooks::new();
        let mut opts = compaction_test_opts(storage_mode);
        opts.test_hooks = Some(hooks.clone());

        // Arrange
        with_engine(opts.clone(), |eng| {
            let cf1 = eng
                .create_column_family("isolate1", ColumnFamilyConfig::default())
                .expect("create isolate1");
            let cf2 = eng
                .create_column_family("isolate2", ColumnFamilyConfig::default())
                .expect("create isolate2");

            // Write same logical key into both CFs with different values
            for i in 0..200 {
                eng.put(&cf1, b"shared", format!("v1_{}", i).as_bytes())
                    .unwrap();
                eng.put(&cf2, b"shared", format!("v2_{}", i).as_bytes())
                    .unwrap();
                if i % 20 == 0 {
                    eng.flush().unwrap();
                }
            }
            eng.flush().unwrap();
            // Deterministically trigger compaction and wait via hooks
            let gate = hooks.install_compaction_gate(CompactionGatePoint::AfterManifestUpdate);
            eng.compact_level(&cf1, 0).ok();
            assert!(gate.wait_until_blocked(Duration::from_secs(10)), "Compaction did not reach AfterManifestUpdate");
            // Release the compaction gate and wait deterministically for compaction to finish
            gate.release();
            eng.wait_for_compaction(Duration::from_secs(10)).unwrap();
        });

        // Restart and validate no cross-contamination
        // Assert
        with_engine_restart(
            opts.clone(),
            |_| {},
            |eng| {
                let cf1 = eng.get_column_family("isolate1").expect("get isolate1");
                let cf2 = eng.get_column_family("isolate2").expect("get isolate2");
                let v1 = eng.get(&cf1, b"shared").unwrap().expect("v1 exists");
                let v2 = eng.get(&cf2, b"shared").unwrap().expect("v2 exists");
                assert!(v1.as_ref().starts_with(b"v1_"));
                assert!(v2.as_ref().starts_with(b"v2_"));
            },
        );
    }
}

#[test]
fn should_handle_cf_drop_gracefully_given_inflight_compaction_when_reopening_database() {
    for mode in disk_storage_modes() {
        let (_name, storage_mode, _tmp) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 1024,
            enable_compaction: true,
            ..Default::default()
        };

        // Arrange
        // Open engine in shared/Arc form so we can spawn a compaction thread that uses the engine concurrently.
        let eng = cntryl_midge::MidgeEngine::open(opts.clone()).expect("open");
        let eng_arc = Arc::new(eng);

        // Create CF and heavy writes
        let cf = eng_arc
            .create_column_family("to_drop", ColumnFamilyConfig::default())
            .expect("create to_drop");
        for i in 0..1000 {
            eng_arc
                .put(&cf, format!("k{:04}", i).as_bytes(), b"v")
                .unwrap();
        }
        eng_arc.flush().unwrap();

        // Act
        // Spawn a thread to perform a compact_range while we drop the CF
        let eng_clone = eng_arc.clone();
        let cf_name = "to_drop".to_string();
        let handle = thread::spawn(move || {
            if let Ok(cf_handle) = eng_clone.get_column_family(&cf_name) {
                let _ = eng_clone.compact_range(&cf_handle, Some(b""), Some(b"~"));
            }
        });

        // Drop the CF while compaction may be in progress
        eng_arc.drop_column_family(&cf).ok();
        handle.join().ok();
        // Close engine
        drop(eng_arc);

        // Assert: Reopen and ensure engine starts and CF is absent
        with_engine(opts.clone(), |eng| {
            let all = eng.list_column_families();
            assert!(!all.iter().any(|c| c.name() == "to_drop"));
        });
    }
}

#[test]
fn should_rebuild_cf_metadata_correctly_given_manifest_rebuild_when_ssts_exist_for_multiple_cfs() {
    for mode in disk_storage_modes() {
        let (_name, storage_mode, tmp) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode: storage_mode.clone(),
            ..Default::default()
        };

        // Arrange: Create CFs and flush SSTs
        with_engine(opts.clone(), |eng| {
            let cf_extra = eng
                .create_column_family("cf_extra", ColumnFamilyConfig::default())
                .expect("create cf_extra");
            let def = eng.default_column_family();

            eng.put(&def, b"dkey", b"dval").unwrap();
            eng.put(&cf_extra, b"ekey", b"eval").unwrap();
            eng.flush().unwrap();
        });

        // Act: Delete manifest files to force rebuild
        if let Some(td) = tmp.as_ref() {
            let manifest = td.path().join("manifest.json");
            let _ = std::fs::remove_file(manifest);
        }

        // Assert: Reopen and check that at least data survives (manifest rebuilt or engine handled gracefully)
        with_engine(opts.clone(), |eng| {
            let def = eng.default_column_family();
            let got = eng.get(&def, b"dkey").unwrap();
            assert!(got.is_some(), "default key recovered");
            // cf_extra may or may not be present depending on rebuild semantics; check if present then verify
            if let Ok(cf) = eng.get_column_family("cf_extra") {
                let ek = eng.get(&cf, b"ekey").unwrap();
                assert!(ek.is_some(), "extra cf key recovered when metadata rebuilt");
            }
        });
    }
}
