//! The engine reaches its metadata, WAL, lease, SSTs and transaction spill runs
//! only through the injected database root, so a fault or crash simulator
//! observes every effect (#808).

use cntryl_midge::__internal::io::{
    Fs, FsPath, MockFs, OpenMode, OpenOptions as FileOptions, RealFs,
};
use cntryl_midge::__internal::simulation::CrashDevice;
use cntryl_midge::__internal::storage::cloud::MockCloudBackend;
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

/// Flush and compact through `options`, restart, and return what reads back.
fn flush_restart_read(options: &OpenOptions) -> Vec<Option<Vec<u8>>> {
    {
        let mut engine = Engine::open(options.clone()).expect("open");
        flush_three_generations(&mut engine, WriteOptions::cloud_strict());
        engine
            .shutdown(std::time::Duration::from_secs(5))
            .expect("shutdown");
    }
    let mut reopened = Engine::open(options.clone()).expect("reopen");
    let values = read_compacted(&reopened);
    reopened
        .shutdown(std::time::Duration::from_secs(5))
        .expect("shutdown reopened");
    values
}

/// Kinds of data object the store received: `sst` files and `wal`
/// segments. Catalogs and other control objects are not counted.
fn stored_data_objects(store: &MockCloudBackend) -> std::collections::BTreeSet<String> {
    store
        .get_uploads()
        .into_iter()
        .filter_map(|(key, _)| {
            std::path::Path::new(&key)
                .extension()
                .and_then(|extension| extension.to_str())
                .filter(|extension| matches!(*extension, "sst" | "wal"))
                .map(str::to_string)
        })
        .collect()
}

#[test]
fn should_serve_cloud_simulated_database_when_every_store_is_injected() {
    // Arrange: no host directory serves this database or its cloud.
    let named = tempfile::tempdir().expect("named directory");
    let root: Arc<dyn Fs> = Arc::new(MockFs::new());
    let store = Arc::new(MockCloudBackend::new());
    let options = OpenOptions::cloud_simulated(named.path(), "bucket", "injected")
        .background_compaction(false)
        .filesystem_for_testing(root)
        .object_store_for_testing(store.clone())
        .build()
        .expect("open options");

    // Act
    let values = flush_restart_read(&options);

    // Assert
    assert_eq!(values, expected_compacted());
    assert_eq!(
        stored_data_objects(&store),
        ["sst", "wal"].map(str::to_string).into()
    );
    assert_eq!(host_entries(named.path()), Vec::<std::path::PathBuf>::new());
}

#[test]
fn should_keep_simulated_cloud_under_served_root_when_root_is_injected() {
    // Arrange
    let named = tempfile::tempdir().expect("named directory");
    let served = tempfile::tempdir().expect("served directory");
    let root: Arc<dyn Fs> = Arc::new(RealFs::new(served.path()).expect("served root"));
    let options = OpenOptions::cloud_simulated(named.path(), "bucket", "served")
        .background_compaction(false)
        .filesystem_for_testing(root)
        .build()
        .expect("open options");

    // Act
    let values = flush_restart_read(&options);

    // Assert
    assert_eq!(values, expected_compacted());
    assert!(served.path().join("cloud_store/sst").is_dir());
    assert_eq!(host_entries(named.path()), Vec::<std::path::PathBuf>::new());
}

#[test]
fn should_require_object_store_when_cloud_simulated_root_has_no_host_directory() {
    // Arrange: the default simulated cloud is a host directory under the
    // root, which an in-memory root does not have.
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
fn should_reject_injected_filesystem_when_storage_is_in_memory() {
    // Arrange
    let root: Arc<dyn Fs> = Arc::new(MockFs::new());

    // Act
    let result = OpenOptions::in_memory()
        .filesystem_for_testing(root)
        .build();

    // Assert
    assert!(matches!(
        result,
        Err(cntryl_midge::MidgeError::InvalidArgument(_))
    ));
}

#[test]
fn should_reject_restore_when_options_inject_storage() {
    // Arrange: a restore writes a host directory, which an injected root
    // would never serve.
    let named = tempfile::tempdir().expect("named directory");
    let artifact = tempfile::tempdir().expect("artifact directory");
    let root: Arc<dyn Fs> = Arc::new(MockFs::new());
    let options = OpenOptions::local(named.path().join("restored"))
        .filesystem_for_testing(root)
        .build()
        .expect("open options");

    // Act
    let result = Engine::restore_backup(artifact.path(), options);

    // Assert
    assert!(
        matches!(result, Err(cntryl_midge::MidgeError::NotSupported(_))),
        "{result:?}"
    );
    assert!(!named.path().join("restored").exists());
}

#[test]
fn should_serve_cloud_database_when_every_store_is_injected() {
    // Arrange: every class store and the lease document live in the injected
    // object store; the provider configuration only names them.
    let named = tempfile::tempdir().expect("named directory");
    let root: Arc<dyn Fs> = Arc::new(MockFs::new());
    let store = Arc::new(MockCloudBackend::new());
    let provider =
        cntryl_midge::CloudProviderConfig::aws_s3_static("bucket", "us-east-1", "access", "secret");
    let options = OpenOptions::cloud(
        named.path(),
        cntryl_midge::CloudStorageLocation::new(provider, "injected/"),
    )
    .background_compaction(false)
    .filesystem_for_testing(root)
    .object_store_for_testing(store.clone())
    .build()
    .expect("open options");

    // Act
    let values = flush_restart_read(&options);

    // Assert
    assert_eq!(values, expected_compacted());
    assert_eq!(
        stored_data_objects(&store),
        ["sst", "wal"].map(str::to_string).into()
    );
    assert_eq!(host_entries(named.path()), Vec::<std::path::PathBuf>::new());
}

#[test]
fn should_verify_injected_root_when_verifying_online() {
    // Arrange
    let named = tempfile::tempdir().expect("named directory");
    let served = tempfile::tempdir().expect("served directory");
    let mut engine = Engine::open(served_options(named.path(), served.path())).expect("open");
    flush_three_generations(&mut engine, WriteOptions::sync());

    // Act
    let report = engine
        .storage_verifier()
        .verify_storage(std::time::Duration::from_secs(30));

    // Assert
    let report = report.expect("the served root verifies");
    assert!(report.sst_files_verified > 0);
    engine
        .shutdown(std::time::Duration::from_secs(5))
        .expect("shutdown");
}

#[test]
fn should_back_up_served_root_when_root_is_injected() {
    // Arrange
    let named = tempfile::tempdir().expect("named directory");
    let served = tempfile::tempdir().expect("served directory");
    let artifacts = tempfile::tempdir().expect("artifact directory");
    let mut engine = Engine::open(served_options(named.path(), served.path())).expect("open");
    flush_three_generations(&mut engine, WriteOptions::sync());

    // Act
    let manifest = engine.backup_to(
        artifacts.path().join("backup"),
        std::time::Duration::from_secs(30),
    );

    // Assert
    let manifest = manifest.expect("the served root is backed up");
    assert!(manifest
        .objects
        .iter()
        .any(|object| object.path.starts_with("sst/")));
    engine
        .shutdown(std::time::Duration::from_secs(5))
        .expect("shutdown");
}

#[test]
fn should_reject_backup_when_root_has_no_host_directory() {
    // Arrange
    let named = tempfile::tempdir().expect("named directory");
    let artifacts = tempfile::tempdir().expect("artifact directory");
    let root: Arc<dyn Fs> = Arc::new(MockFs::new());
    let options = OpenOptions::local(named.path())
        .filesystem_for_testing(root)
        .build()
        .expect("open options");
    let mut engine = Engine::open(options).expect("open");

    // Act
    let result = engine.backup_to(
        artifacts.path().join("backup"),
        std::time::Duration::from_secs(30),
    );

    // Assert
    assert!(
        matches!(result, Err(cntryl_midge::MidgeError::NotSupported(_))),
        "{result:?}"
    );
    engine
        .shutdown(std::time::Duration::from_secs(5))
        .expect("shutdown");
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

/// A transaction larger than its memory pool spills intent runs to disk.
fn commit_spilled_transaction(engine: &Engine) {
    let default = engine.get_column_family("default").expect("default family");
    let mut tx = engine
        .begin_tx(default.id(), TransactionMode::ReadWrite)
        .expect("begin");
    for index in 0..64_u8 {
        tx.put(vec![b's', index], vec![index; 512], None)
            .expect("put");
    }
    assert_eq!(
        tx.get(&[b's', 7])
            .expect("read own spilled write")
            .as_deref(),
        Some(&[7_u8; 512][..])
    );
    tx.commit(WriteOptions::sync()).expect("commit");
}

#[test]
fn should_spill_transaction_into_injected_root_when_pool_is_small() {
    // Arrange
    let named = tempfile::tempdir().expect("named directory");
    let memory = Arc::new(MockFs::new());
    let options = OpenOptions::local(named.path())
        .background_compaction(false)
        .transaction_memory_pool_size(4 * 1024)
        .filesystem_for_testing(memory.clone())
        .build()
        .expect("open options");
    let spilled_on_host = {
        let mut engine = Engine::open(options.clone()).expect("open");
        commit_spilled_transaction(&engine);
        assert!(
            memory
                .exists(&FsPath::new("txn"))
                .expect("probe spill directory"),
            "the transaction must have spilled into the root"
        );
        let spilled_on_host = host_entries(named.path());
        engine
            .shutdown(std::time::Duration::from_secs(5))
            .expect("shutdown");
        spilled_on_host
    };

    // Act
    let reopened = Engine::open(options).expect("reopen");
    let default = reopened
        .get_column_family("default")
        .expect("default family");
    let value = reopened
        .begin_tx(default.id(), TransactionMode::ReadOnly)
        .expect("read")
        .get(&[b's', 63])
        .expect("get");

    // Assert
    assert_eq!(value.as_deref(), Some(&[63_u8; 512][..]));
    assert_eq!(spilled_on_host, Vec::<std::path::PathBuf>::new());
    assert_eq!(host_entries(named.path()), Vec::<std::path::PathBuf>::new());
}

#[test]
fn should_remove_orphaned_spill_runs_when_opening_from_injected_root() {
    // Arrange: a previous process left a spill run in the root.
    let named = tempfile::tempdir().expect("named directory");
    let memory = Arc::new(MockFs::new());
    memory
        .open(
            &FsPath::new("txn/0000000000000001-00000000.run"),
            FileOptions {
                mode: OpenMode::ReadWrite,
                create: true,
                create_new: true,
                truncate: false,
            },
        )
        .and_then(|mut file| file.append(bytes::Bytes::from_static(b"orphaned run")))
        .expect("plant orphaned run");
    let options = OpenOptions::local(named.path())
        .background_compaction(false)
        .filesystem_for_testing(memory.clone())
        .build()
        .expect("open options");

    // Act
    let engine = Engine::open(options).expect("open");

    // Assert
    assert!(!memory
        .exists(&FsPath::new("txn/0000000000000001-00000000.run"))
        .expect("probe orphaned run"));
    drop(engine);
}

/// Short enough that a restart after a simulated crash takes the dead
/// process's lease over quickly.
const LEASE_TTL: std::time::Duration = std::time::Duration::from_millis(100);

#[test]
fn should_keep_synced_commit_when_power_loss_tears_buffered_tail() {
    for seed in 0..8_u64 {
        // Arrange: a local engine whose every file lives on a simulated device.
        let named = tempfile::tempdir().expect("named directory");
        let device = CrashDevice::new();
        let options = |device: &CrashDevice| {
            let root: Arc<dyn Fs> = Arc::new(device.fs());
            OpenOptions::local(named.path())
                .background_compaction(false)
                .lease_ttl(LEASE_TTL)
                .lease_clock_skew_tolerance(std::time::Duration::ZERO)
                .filesystem_for_testing(root)
                .build()
                .expect("open options")
        };
        let engine = Engine::open(options(&device)).expect("open");
        let default = engine.get_column_family("default").expect("default family");
        let mut tx = engine
            .begin_tx(default.id(), TransactionMode::ReadWrite)
            .expect("begin");
        tx.put(b"key".to_vec(), seed.to_be_bytes().to_vec(), None)
            .expect("put");
        tx.commit(WriteOptions::sync()).expect("commit");
        let mut tx = engine
            .begin_tx(default.id(), TransactionMode::ReadWrite)
            .expect("begin");
        tx.put(b"buffered".to_vec(), b"tail".to_vec(), None)
            .expect("put");
        tx.commit(WriteOptions::buffered())
            .expect("buffered commit");
        let undecided = device.decisions();

        // Act: power is lost while the engine runs; the dead process can no
        // longer reach the device, so its shutdown changes nothing. A
        // restarted process takes the lease over once it is stale.
        device.power_loss(seed);
        drop(engine);
        std::thread::sleep(LEASE_TTL * 3);
        let reopened = Engine::open(options(&device)).expect("reopen");
        let default = reopened
            .get_column_family("default")
            .expect("default family");
        let tx = reopened
            .begin_tx(default.id(), TransactionMode::ReadOnly)
            .expect("read");
        let synced = tx.get(b"key").expect("get synced");
        let buffered = tx.get(b"buffered").expect("get buffered");

        // Assert: the buffered tail was undecided, and whatever part of it
        // survived, recovery keeps the synced commit and never invents bytes.
        assert!(
            !undecided.is_empty(),
            "seed {seed}: nothing was left unsynced"
        );
        assert_eq!(
            synced.as_deref(),
            Some(&seed.to_be_bytes()[..]),
            "seed {seed}"
        );
        assert!(
            buffered.is_none() || buffered.as_deref() == Some(&b"tail"[..]),
            "seed {seed}: {buffered:?}"
        );
        assert_eq!(host_entries(named.path()), Vec::<std::path::PathBuf>::new());
    }
}
