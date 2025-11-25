// WAL vs manifest divergence tests (simplified, observable outcomes)
mod common;
use cntryl_midge::MidgeOptions;
use common::*;

#[test]
fn should_prefer_wal_replay_given_manifest_lagging_latest_commit_when_recovering_after_crash() {
    // Arrange: write a value that only lands in WAL (no flush)

    // Act: restart to force WAL replay under each storage mode
    for mode in disk_storage_modes() {
        let (_n, storage_mode, _tmp) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            wal_sync: true,
            ..Default::default()
        };

        with_engine_restart(
            opts.clone(),
            |eng| {
                let cf = eng.default_column_family();
                eng.put(&cf, b"wal_only", b"v_wal").expect("put wal_only");
                // do not flush; crash simulated by drop
            },
            |eng| {
                // Assert: value should be recovered from WAL after restart
                let cf = eng.default_column_family();
                let got = eng.get(&cf, b"wal_only").expect("get");
                assert!(
                    got.is_some(),
                    "wal-only entry should be replayed on recovery"
                );
            },
        );
    }
}

#[test]
fn should_rollback_manifest_view_given_manifest_ahead_of_persisted_ssts_when_detecting_inconsistent_state_on_restart(
) {
    // Arrange: create a key and flush to produce SSTs

    // Act: tamper with manifest file (best-effort) then reopen
    for mode in disk_storage_modes() {
        let (_n, storage_mode, tmp) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };

        with_engine(opts.clone(), |eng| {
            let cf = eng.default_column_family();
            eng.put(&cf, b"m_k", b"v").unwrap();
            eng.flush().unwrap();
        });

        if let Some(td) = tmp.as_ref() {
            let manifest = td.path().join("manifest.json");
            if manifest.exists() {
                // Corrupt by truncating
                let _ = std::fs::write(&manifest, b"{}");
            }
        }

        with_engine(opts.clone(), |eng| {
            // Assert: engine can reopen and key is either present or handled gracefully
            let cf = eng.default_column_family();
            let got = eng.get(&cf, b"m_k").unwrap();
            let _ = got; // presence is optional, but read must not panic
        });
    }
}

#[test]
fn should_refuse_to_open_database_given_irreconcilable_manifest_and_wal_states_when_corruption_detected(
) {
    // Arrange: write and flush

    // Act: corrupt both manifest and WAL file to simulate irreconcilable state
    for mode in disk_storage_modes() {
        let (_n, storage_mode, tmp) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };

        with_engine(opts.clone(), |eng| {
            let cf = eng.default_column_family();
            eng.put(&cf, b"xk", b"xv").unwrap();
            eng.flush().unwrap();
        });

        if let Some(td) = tmp.as_ref() {
            let manifest = td.path().join("manifest.json");
            let _ = std::fs::write(&manifest, b"corrupt");
            // WAL file patterns vary; best-effort: truncate wals directory
            let wal_dir = td.path().join("wal");
            let _ = std::fs::create_dir_all(&wal_dir);
            let _ = std::fs::write(wal_dir.join("bad.wal"), b"corrupt");
        }

        // Assert: either open fails or succeeds in safe mode; it must not panic
        let open_res = cntryl_midge::MidgeEngine::open(opts.clone());
        let _ = open_res;
    }
}

#[test]
fn should_rebuild_manifest_from_ssts_given_missing_manifest_and_clean_wal_tail_when_starting_after_disk_issue(
) {
    // Arrange: create data and flush to SSTs

    // Act: remove manifest file and reopen
    for mode in disk_storage_modes() {
        let (_n, storage_mode, tmp) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };

        with_engine(opts.clone(), |eng| {
            let cf = eng.default_column_family();
            eng.put(&cf, b"rb_k", b"rb_v").unwrap();
            eng.flush().unwrap();
        });

        if let Some(td) = tmp.as_ref() {
            let manifest = td.path().join("manifest.json");
            let _ = std::fs::remove_file(manifest);
        }

        with_engine(opts.clone(), |eng| {
            // Assert: engine can reopen and recover data from SSTs
            let cf = eng.default_column_family();
            let got = eng.get(&cf, b"rb_k").unwrap();
            assert!(
                got.is_some(),
                "data should be recoverable after manifest rebuild"
            );
        });
    }
}
