//! The engine reaches its metadata, WAL, lease and SSTs only through the injected
//! database root, so a fault or crash simulator observes every effect (#808).

use cntryl_midge::__internal::io::{Fs, MockFs, RealFs};
use cntryl_midge::{Engine, EngineHealth, OpenOptions, TransactionMode, WriteOptions};
use std::sync::Arc;

fn served_options(named: &std::path::Path, served: &std::path::Path) -> OpenOptions {
    let root: Arc<dyn Fs> = Arc::new(RealFs::new(served).expect("served root"));
    OpenOptions::local(named)
        .background_compaction(false)
        .filesystem_for_testing(root)
        .build()
        .expect("open options")
}

#[test]
fn should_serve_routed_files_from_injected_root_when_opening_local_database() {
    // Arrange: the configured path names the database; a different host
    // directory serves it.
    let named = tempfile::tempdir().expect("named directory");
    let served = tempfile::tempdir().expect("served directory");
    let options = served_options(named.path(), served.path());

    // Act
    {
        let mut engine = Engine::open(options.clone()).expect("open");
        let default = engine.get_column_family("default").expect("default family");
        let mut tx = engine
            .begin_tx(default.id(), TransactionMode::ReadWrite)
            .expect("begin");
        tx.put(b"key".to_vec(), b"value".to_vec(), None)
            .expect("put");
        tx.commit(WriteOptions::sync()).expect("commit");
        engine
            .shutdown(std::time::Duration::from_secs(5))
            .expect("shutdown");
    }
    let reopened = Engine::open(options).expect("reopen");
    let default = reopened
        .get_column_family("default")
        .expect("default family");
    let value = reopened
        .begin_tx(default.id(), TransactionMode::ReadOnly)
        .expect("read")
        .get(b"key")
        .expect("get");

    // Assert
    assert_eq!(value.as_deref(), Some(&b"value"[..]));
    for file in ["FORMAT", ".midge_leader"] {
        assert!(served.path().join(file).exists(), "{file} must be served");
        assert!(
            !named.path().join(file).exists(),
            "{file} must not be on the named path"
        );
    }
    let served_wal = std::fs::read_dir(served.path().join("wal"))
        .expect("served WAL directory")
        .count();
    assert!(served_wal > 0, "WAL must be served by the injected root");
    assert!(!named.path().join("wal").exists());
}

#[test]
fn should_recover_synced_write_when_injected_root_is_in_memory() {
    // Arrange: no host directory serves this database at all.
    let named = tempfile::tempdir().expect("named directory");
    let root: Arc<dyn Fs> = Arc::new(MockFs::new());
    let options = OpenOptions::local(named.path())
        .background_compaction(false)
        .filesystem_for_testing(root)
        .build()
        .expect("open options");
    {
        let mut engine = Engine::open(options.clone()).expect("open");
        let default = engine.get_column_family("default").expect("default family");
        let mut tx = engine
            .begin_tx(default.id(), TransactionMode::ReadWrite)
            .expect("begin");
        tx.put(b"key".to_vec(), b"value".to_vec(), None)
            .expect("put");
        tx.commit(WriteOptions::sync()).expect("commit");
        engine
            .shutdown(std::time::Duration::from_secs(5))
            .expect("shutdown");
    }

    // Act
    let reopened = Engine::open(options).expect("reopen");
    let default = reopened
        .get_column_family("default")
        .expect("default family");
    let value = reopened
        .begin_tx(default.id(), TransactionMode::ReadOnly)
        .expect("read")
        .get(b"key")
        .expect("get");

    // Assert
    assert_eq!(
        value.as_deref(),
        Some(&b"value"[..]),
        "a synced write must survive reopen through the injected root"
    );
    for routed in [
        "FORMAT",
        ".midge_leader",
        "wal",
        "manifest.journal",
        "intent_log.json",
    ] {
        assert!(
            !named.path().join(routed).exists(),
            "{routed} must stay on the injected root"
        );
    }
}

/// Write three flushed generations, then compact them into one level.
fn flush_three_generations(engine: &mut Engine, write: WriteOptions) {
    let default = engine.get_column_family("default").expect("default family");
    for generation in 0..3_u8 {
        let mut tx = engine
            .begin_tx(default.id(), TransactionMode::ReadWrite)
            .expect("begin");
        for index in 0..16_u8 {
            tx.put(vec![b'k', index], vec![generation, index], None)
                .expect("put");
        }
        tx.commit(write).expect("commit");
        engine.flush_cf(&default).expect("flush");
    }
    engine.compact_all().expect("compact");
}

fn read_compacted(engine: &Engine) -> Vec<Option<Vec<u8>>> {
    let default = engine.get_column_family("default").expect("default family");
    let tx = engine
        .begin_tx(default.id(), TransactionMode::ReadOnly)
        .expect("read");
    (0..16_u8)
        .map(|index| {
            tx.get(&[b'k', index])
                .expect("get")
                .map(|value| value.to_vec())
        })
        .collect()
}

fn expected_compacted() -> Vec<Option<Vec<u8>>> {
    (0..16_u8).map(|index| Some(vec![2, index])).collect()
}

fn host_entries(path: &std::path::Path) -> Vec<std::path::PathBuf> {
    std::fs::read_dir(path)
        .expect("named directory")
        .map(|entry| entry.expect("entry").path())
        .collect()
}

#[test]
fn should_keep_named_path_empty_when_flushing_through_injected_root() {
    // Arrange
    let named = tempfile::tempdir().expect("named directory");
    let served = tempfile::tempdir().expect("served directory");
    let options = served_options(named.path(), served.path());

    // Act
    let health = {
        let mut engine = Engine::open(options.clone()).expect("open");
        flush_three_generations(&mut engine, WriteOptions::sync());
        let health = engine
            .metrics()
            .get_runtime_metrics()
            .expect("metrics")
            .health;
        engine
            .shutdown(std::time::Duration::from_secs(5))
            .expect("shutdown");
        health
    };
    let reopened = Engine::open(options).expect("reopen");

    // Assert
    assert_eq!(read_compacted(&reopened), expected_compacted());
    assert_eq!(
        health,
        EngineHealth::Healthy,
        "SST residue is read from the root"
    );
    let served_ssts = std::fs::read_dir(served.path().join("sst"))
        .expect("served SST directory")
        .filter(|entry| {
            entry
                .as_ref()
                .is_ok_and(|entry| entry.path().extension().is_some_and(|ext| ext == "sst"))
        })
        .count();
    assert!(served_ssts > 0, "SSTs must be served by the injected root");
    assert_eq!(host_entries(named.path()), Vec::<std::path::PathBuf>::new());
}

#[test]
fn should_recover_compacted_data_when_injected_root_is_in_memory() {
    // Arrange
    let named = tempfile::tempdir().expect("named directory");
    let root: Arc<dyn Fs> = Arc::new(MockFs::new());
    let options = OpenOptions::local(named.path())
        .background_compaction(false)
        .filesystem_for_testing(root)
        .build()
        .expect("open options");
    {
        let mut engine = Engine::open(options.clone()).expect("open");
        flush_three_generations(&mut engine, WriteOptions::sync());
        engine
            .shutdown(std::time::Duration::from_secs(5))
            .expect("shutdown");
    }

    // Act
    let reopened = Engine::open(options).expect("reopen");

    // Assert
    assert_eq!(read_compacted(&reopened), expected_compacted());
    assert_eq!(host_entries(named.path()), Vec::<std::path::PathBuf>::new());
}

#[test]
fn should_report_orphan_residue_when_injected_root_holds_unpublished_sst() {
    // Arrange
    let named = tempfile::tempdir().expect("named directory");
    let served = tempfile::tempdir().expect("served directory");
    let mut engine = Engine::open(served_options(named.path(), served.path())).expect("open");
    flush_three_generations(&mut engine, WriteOptions::sync());
    std::fs::write(served.path().join("sst").join("orphan.sst"), b"unpublished")
        .expect("plant orphan");

    // Act
    let health = engine
        .metrics()
        .get_runtime_metrics()
        .expect("metrics")
        .health;

    // Assert
    assert_eq!(
        health,
        EngineHealth::Degraded,
        "residue is read from the root"
    );
    engine
        .shutdown(std::time::Duration::from_secs(5))
        .expect("shutdown");
}

#[test]
fn should_reject_injected_filesystem_when_storage_is_cloud_simulated() {
    // Arrange: cloud working storage is not yet served by the root, so a
    // partly injected cloud database would split its files across two roots.
    let named = tempfile::tempdir().expect("named directory");
    let root: Arc<dyn Fs> = Arc::new(MockFs::new());

    // Act
    let result = OpenOptions::cloud_simulated(named.path(), "bucket", "prefix")
        .filesystem_for_testing(root)
        .build();

    // Assert
    assert!(matches!(
        result,
        Err(cntryl_midge::MidgeError::InvalidArgument(_))
    ));
}

#[test]
fn should_keep_sst_effects_under_sst_dir_when_compacting_cloud_simulated_database() {
    // Arrange: the cloud SST view layers over the same root, so flush and
    // compaction outputs, scratch and repair runs all belong under `sst/`.
    let directory = tempfile::tempdir().expect("cache directory");
    let options = OpenOptions::cloud_simulated(directory.path(), "bucket", "sst-layout")
        .background_compaction(false)
        .build()
        .expect("open options");
    {
        let mut engine = Engine::open(options.clone()).expect("open");
        flush_three_generations(&mut engine, WriteOptions::cloud_strict());
        engine
            .shutdown(std::time::Duration::from_secs(5))
            .expect("shutdown");
    }

    // Act
    let reopened = Engine::open(options).expect("reopen");

    // Assert
    assert_eq!(read_compacted(&reopened), expected_compacted());
    let stray: Vec<_> = host_entries(directory.path())
        .into_iter()
        .filter(|path| {
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("");
            path.extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("sst"))
                || name.ends_with(".sst.tmp")
                || name.starts_with(".compaction")
                || name == ".flush-staging"
        })
        .collect();
    assert_eq!(stray, Vec::<std::path::PathBuf>::new());
}
