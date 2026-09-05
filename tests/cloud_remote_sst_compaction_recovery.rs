#![cfg(feature = "failpoints")]

use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;

static FAILPOINT_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn sst_names(directory: &Path) -> BTreeSet<String> {
    std::fs::read_dir(directory)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|entry| {
            entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "sst")
        })
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect()
}

#[test]
fn should_roll_back_proven_remote_compaction_orphan_when_local_output_is_corrupt() {
    // Arrange
    let _guard = FAILPOINT_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let scenario = fail::FailScenario::setup();
    let directory = tempfile::tempdir().expect("database directory");
    let options = OpenOptions::cloud_simulated(directory.path(), "bucket", "corrupt-local-orphan")
        .background_compaction(false)
        .build()
        .expect("options");
    let mut engine = Engine::open(options.clone()).expect("open");
    let cf = engine.create_column_family("data").expect("column family");
    for key in [b"first".as_slice(), b"second".as_slice()] {
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("transaction");
        tx.put(key.to_vec(), b"preserved".to_vec(), None)
            .expect("put");
        tx.commit(WriteOptions::cloud_strict())
            .expect("cloud commit");
        engine.flush_cf(&cf).expect("flush");
    }
    let remote_directory = directory.path().join("cloud_store/sst");
    let inputs = sst_names(&remote_directory);
    fail::cfg(
        "midge::manifest::inject_no_space_on_compaction_batch_edit",
        "return",
    )
    .expect("interrupt manifest publication");
    engine.compact_all().expect_err("publication must fail");
    fail::remove("midge::manifest::inject_no_space_on_compaction_batch_edit");
    let outputs = sst_names(&remote_directory)
        .difference(&inputs)
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(outputs.len(), 1);
    engine.shutdown(Duration::from_secs(30)).expect("shutdown");
    drop(engine);
    let remote_output = remote_directory.join(&outputs[0]);
    let local_output = directory.path().join("sst").join(&outputs[0]);
    let mut corrupt_bytes = std::fs::read(&remote_output).expect("read remote output");
    corrupt_bytes[0] ^= 1;
    std::fs::write(&local_output, corrupt_bytes).expect("retain corrupt local cache");

    // Act
    let mut reopened = Engine::open(options).expect("strict rollback with intact inputs");

    // Assert
    assert_eq!(sst_names(&remote_directory), inputs);
    assert!(!remote_output.exists());
    assert!(!local_output.exists());
    let cf = reopened.get_column_family("data").expect("recovered CF");
    let tx = reopened
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("read transaction");
    for key in [b"first".as_slice(), b"second".as_slice()] {
        assert_eq!(
            tx.get(key).expect("read preserved input").as_deref(),
            Some(b"preserved".as_slice())
        );
    }
    drop(tx);
    reopened
        .shutdown(Duration::from_secs(30))
        .expect("shutdown");
    scenario.teardown();
}

#[test]
fn should_retry_with_fresh_output_identity_after_remote_partition_eviction_fails() {
    // Arrange
    let _guard = FAILPOINT_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let scenario = fail::FailScenario::setup();
    let directory = tempfile::tempdir().expect("database directory");
    let options = OpenOptions::cloud_simulated(directory.path(), "bucket", "orphan-retry")
        .local_storage_budget(1024 * 1024)
        .background_compaction(false)
        .build()
        .expect("options");
    let mut engine = Engine::open(options.clone()).expect("open");
    let cf = engine.create_column_family("data").expect("column family");
    for key in [b"first".as_slice(), b"second".as_slice()] {
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("transaction");
        tx.put(key.to_vec(), b"preserved".to_vec(), None)
            .expect("put");
        tx.commit(WriteOptions::cloud_strict())
            .expect("cloud commit");
        engine.flush_cf(&cf).expect("flush");
    }
    let remote_directory = directory.path().join("cloud_store/sst");
    let inputs = sst_names(&remote_directory);
    fail::cfg(
        "midge::compaction::after_remote_partition_evicted",
        "return",
    )
    .expect("configure compaction interruption");

    // Act
    let interrupted = engine.compact_all();
    fail::remove("midge::compaction::after_remote_partition_evicted");
    assert!(
        interrupted.is_err(),
        "the injected partition failure must reach the caller"
    );
    let after_failure = sst_names(&remote_directory);
    assert!(
        after_failure.is_superset(&inputs),
        "unpublished output cannot retire inputs"
    );
    assert!(
        after_failure.len() > inputs.len(),
        "the failed job left a remote output"
    );
    assert!(sst_names(&directory.path().join("sst")).is_empty());
    engine
        .shutdown(Duration::from_secs(30))
        .expect("shutdown interrupted engine");
    drop(engine);
    let mut reopened = Engine::open(options).expect("reopen after interrupted job");
    reopened.compact_all().expect("retry remote compaction");

    // Assert
    let after_retry = sst_names(&remote_directory);
    assert!(
        after_retry.difference(&after_failure).next().is_some(),
        "retry must allocate a fresh generation instead of reusing an orphan identity"
    );
    assert!(sst_names(&directory.path().join("sst")).is_empty());
    let cf = reopened
        .get_column_family("data")
        .expect("recovered column family");
    let tx = reopened
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("read transaction");
    for key in [b"first".as_slice(), b"second".as_slice()] {
        assert_eq!(
            tx.get(key).expect("read after retry").as_deref(),
            Some(b"preserved".as_slice())
        );
    }
    drop(tx);
    reopened
        .shutdown(Duration::from_secs(30))
        .expect("shutdown recovered engine");
    scenario.teardown();
}
