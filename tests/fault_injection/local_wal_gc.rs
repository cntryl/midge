//! A failed local unlink must retain proof even after cloud authority retires.
use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const UNLINK: &str = "midge::cloud::before_local_wal_unlink";

fn options(root: &Path) -> OpenOptions {
    OpenOptions::cloud_simulated(root, "local-copy", "gc-proof")
        .background_compaction(false)
        .build()
        .unwrap()
}

fn absent(engine: &Engine) {
    let cf = engine.get_column_family("default").unwrap();
    let read = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
    assert_eq!(read.get(b"k").unwrap(), None);
}

fn surviving_copy(compact: bool) {
    // Arrange: replace only segment 1 with a directory at the unlink boundary.
    // remove_file then returns a real non-NotFound error. Its saved bytes model
    // the replayable file that survives a failed removal or an unsynced unlink.
    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let sealed = root
        .join("wal")
        .join(cntryl_midge::__internal::wal::segment_file_name(1));
    let saved = Arc::new(Mutex::new(None));
    let callback_saved = saved.clone();
    let callback_path = sealed.clone();
    fail::cfg_callback(UNLINK, move || {
        let mut saved = callback_saved.lock().unwrap();
        if saved.is_none() && callback_path.is_file() {
            *saved = Some(std::fs::read(&callback_path).unwrap());
            std::fs::remove_file(&callback_path).unwrap();
            std::fs::create_dir(&callback_path).unwrap();
        }
    })
    .unwrap();
    let mut engine = Engine::open(options(root)).unwrap();
    let cf = engine.get_column_family("default").unwrap();
    let mut put = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .unwrap();
    put.put(b"k".to_vec(), b"v1".to_vec(), None).unwrap();
    put.commit(WriteOptions::cloud_strict()).unwrap();
    engine.flush_cf(&cf).unwrap();
    let mut delete = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .unwrap();
    delete.delete(b"k".to_vec()).unwrap();
    delete.commit(WriteOptions::cloud_strict()).unwrap();
    engine.flush_cf(&cf).unwrap();

    // Act: prove both catalogs retired before collecting deletion proof.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let retired = [
            "publication-catalog.v1.json",
            "publication-catalog.v1.mirror.json",
        ]
        .iter()
        .all(|name| {
            let catalog: serde_json::Value = serde_json::from_slice(
                &std::fs::read(root.join("cloud_store/wal").join(name)).unwrap(),
            )
            .unwrap();
            catalog["segments"].as_object().unwrap().is_empty()
        });
        if retired {
            break;
        }
        assert!(Instant::now() < deadline, "catalog retirement timed out");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        sealed.is_dir(),
        "the failed local removal must actually occur"
    );
    assert!(saved.lock().unwrap().is_some());
    if compact {
        engine.compact_all().unwrap();
    }
    absent(&engine);
    engine.shutdown(Duration::from_secs(10)).unwrap();
    drop(engine);
    fail::remove(UNLINK);
    scenario.teardown();
    std::fs::remove_dir(&sealed).unwrap();
    std::fs::write(&sealed, saved.lock().unwrap().as_ref().unwrap()).unwrap();
    let mut reopened = Engine::open(options(root)).unwrap();

    // Assert: a replayable old put cannot resurrect a flushed deletion.
    absent(&reopened);
    reopened.shutdown(Duration::from_secs(10)).unwrap();
}

#[test]
fn should_not_resurrect_deleted_key_when_retired_local_wal_copy_survives_restart() {
    surviving_copy(true);
}

#[test]
fn should_keep_deleted_key_absent_without_compaction_when_local_wal_removal_fails() {
    surviving_copy(false);
}
