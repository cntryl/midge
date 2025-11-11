mod common;
use cntryl_midge::MidgeOptions;
use common::{durability_opts, flush_test_opts, test_temp_dir, with_engine_restart};

#[test]
fn should_flush_and_fsync_all_memtables_given_shutdown_signal() {
    // Arrange
    let dir = test_temp_dir();
    let opts = flush_test_opts(dir.path().to_path_buf(), 1024 * 1024); // Large memtable

    // Act & Assert
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            for i in 0..100 {
                eng.put(&cf, format!("key{:03}", i).as_bytes(), b"value")
                    .expect("put");
            }
            // Clean shutdown (drop) should flush and fsync
        },
        |eng| {
            // Assert - all memtable data should be persisted
            let cf = eng.default_column_family();
            for i in 0..100 {
                let result = eng
                    .get(&cf, format!("key{:03}", i).as_bytes())
                    .expect("get");
                assert!(
                    result.is_some(),
                    "Memtable data should be fsynced on shutdown"
                );
            }
        },
    );
}

#[test]
fn should_complete_pending_compactions_given_shutdown_signal() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        memtable_size: 1024,
        enable_compaction: true,
        ..durability_opts(dir.path().to_path_buf())
    };

    // Act & Assert
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            // Write data that triggers compaction
            for i in 0..200 {
                eng.put(&cf, format!("key{:03}", i % 50).as_bytes(), b"value")
                    .expect("put");
            }
            // Shutdown should wait for compaction to complete or abort gracefully
        },
        |eng| {
            // Assert - all data should be present and consistent
            let cf = eng.default_column_family();
            for i in 0..50 {
                let result = eng
                    .get(&cf, format!("key{:03}", i).as_bytes())
                    .expect("get");
                assert!(
                    result.is_some(),
                    "Data should be consistent after shutdown during compaction"
                );
            }
        },
    );
}

#[test]
fn should_abort_long_running_uploads_given_shutdown_signal() {
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());

    // Act & Assert
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            eng.put(&cf, b"key1", b"value1").expect("put");
            // TODO: Test cloud storage mode with long-running uploads
            // Shutdown should abort gracefully without data loss
        },
        |eng| {
            // Assert - local data should be consistent
            let cf = eng.default_column_family();
            let result = eng.get(&cf, b"key1").expect("get");
            assert!(result.is_some(), "Data should survive aborted uploads");
        },
    );
}

#[test]
fn should_persist_all_memtables_given_shutdown_signal_when_clean_exit() {
    // Arrange
    let dir = test_temp_dir();
    let opts = flush_test_opts(dir.path().to_path_buf(), 1024 * 1024);

    // Act & Assert
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            // Multiple writes to memtable
            for batch in 0..3 {
                for i in 0..20 {
                    eng.put(
                        &cf,
                        format!("batch{}_key{:02}", batch, i).as_bytes(),
                        b"value",
                    )
                    .expect("put");
                }
            }
            // Clean shutdown should persist all memtables
        },
        |eng| {
            // Assert - all batches should be present
            let cf = eng.default_column_family();
            for batch in 0..3 {
                for i in 0..20 {
                    let key = format!("batch{}_key{:02}", batch, i);
                    let result = eng.get(&cf, key.as_bytes()).expect("get");
                    assert!(result.is_some(), "All memtable data should be persisted");
                }
            }
        },
    );
}

#[test]
fn should_reopen_without_recovery_needed_given_clean_shutdown() {
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());

    // Act & Assert
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            eng.put(&cf, b"key1", b"value1").expect("put");
            eng.put(&cf, b"key2", b"value2").expect("put");
            // Clean shutdown should mark database as cleanly closed
        },
        |eng| {
            // Assert - reopen should be fast (no WAL replay needed)
            // Data should be immediately available
            let cf = eng.default_column_family();
            let result = eng.get(&cf, b"key1").expect("get");
            assert!(
                result.is_some(),
                "Data should be immediately available after clean shutdown"
            );
            // TODO: Add instrumentation to verify no WAL replay occurred
        },
    );
}
