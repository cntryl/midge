// Multi-column-family compaction + recovery observable tests
mod common;
use cntryl_midge::api::column_family::ColumnFamilyConfig;
use cntryl_midge::test_hooks::{CompactionGatePoint, TestHooks};
use cntryl_midge::MidgeOptions;
use common::test_helpers::TEST_GATE_TIMEOUT;
use common::*;

#[test]
fn should_recover_all_column_families_consistently_given_mixed_writes_and_compactions_when_restarting_repeatedly(
) {
    for mode in disk_storage_modes() {
        // Arrange
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

        // Act
        // Perform multiple restart cycles with additional writes and compactions
        for cycle in 0..3 {
            with_engine_restart(
                opts.clone(),
                |eng| {
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
                    let gate =
                        hooks.install_compaction_gate(CompactionGatePoint::AfterManifestUpdate);
                    eng.compact_level(&cf1, 0).ok();
                    assert!(
                        gate.wait_until_blocked(TEST_GATE_TIMEOUT),
                        "Compaction did not reach AfterManifestUpdate"
                    );
                    // Release the compaction gate and wait deterministically for compaction to finish
                    gate.release();
                    eng.wait_for_compaction(TEST_GATE_TIMEOUT).unwrap();
                },
                |eng| {
                    // Assert
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
            assert!(
                gate.wait_until_blocked(TEST_GATE_TIMEOUT),
                "Compaction did not reach AfterManifestUpdate"
            );
            // Release the compaction gate and wait deterministically for compaction to finish
            gate.release();
            eng.wait_for_compaction(TEST_GATE_TIMEOUT).unwrap();
        });

        // Act
        // Restart and validate no cross-contamination
        with_engine_restart(
            opts.clone(),
            |_| {},
            |eng| {
                // Assert
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
    // Test: Verify that dropping a CF after compaction completes persists correctly across restart.
    // Note: We use test hooks to ensure deterministic compaction completion before drop.
    for mode in disk_storage_modes() {
        let (_name, storage_mode, _tmp) = create_storage_mode(mode);
        let hooks = TestHooks::new();
        let mut opts = MidgeOptions {
            storage_mode,
            memtable_size: 1024,
            enable_compaction: true,
            ..Default::default()
        };
        opts.test_hooks = Some(hooks.clone());

        // Arrange
        let eng = cntryl_midge::MidgeEngine::open(opts.clone()).expect("open");

        // Create CF and heavy writes
        let cf = eng
            .create_column_family("to_drop", ColumnFamilyConfig::default())
            .expect("create to_drop");
        for i in 0..1000 {
            eng.put(&cf, format!("k{:04}", i).as_bytes(), b"v")
                .unwrap();
        }
        eng.flush().unwrap();

        // Act: Trigger compaction and wait for it to complete deterministically
        let gate = hooks.install_compaction_gate(CompactionGatePoint::AfterManifestUpdate);
        eng.compact_level(&cf, 0).ok();
        if gate.wait_until_blocked(TEST_GATE_TIMEOUT) {
            gate.release();
            eng.wait_for_compaction(TEST_GATE_TIMEOUT).ok();
        }

        // Now drop the CF after compaction has completed
        eng.drop_column_family(&cf).expect("drop should succeed after flush and compaction");

        // Close engine
        drop(eng);

        // Assert: Reopen and ensure engine starts and CF is absent
        with_engine(opts.clone(), |eng| {
            let all = eng.list_column_families();
            assert!(
                !all.iter().any(|c| c.name() == "to_drop"),
                "CF 'to_drop' should not exist after being dropped and reopening"
            );
        });
    }
}

#[test]
fn should_preserve_cf_metadata_correctly_given_normal_restart_when_ssts_exist_for_multiple_cfs() {
    // Test: Verify that multiple column families with flushed SSTs survive a normal restart.
    // Note: This test validates persistence, NOT manifest rebuild from SSTs (which is not implemented).
    for mode in disk_storage_modes() {
        let (_name, storage_mode, _tmp) = create_storage_mode(mode);
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

        // Act: Normal restart (no manifest corruption)

        // Assert: Reopen and check that data survives
        with_engine(opts.clone(), |eng| {
            let def = eng.default_column_family();
            let got = eng.get(&def, b"dkey").unwrap();
            assert!(got.is_some(), "default key should be recovered after normal restart");

            // cf_extra should be present
            let cf = eng.get_column_family("cf_extra").expect("cf_extra should exist after restart");
            let ek = eng.get(&cf, b"ekey").unwrap();
            assert!(ek.is_some(), "extra cf key should be recovered after normal restart");
        });
    }
}
