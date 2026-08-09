use bytes::Bytes;
use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug)]
enum TestStorage {
    Local,
    SimulatedCloud,
}

fn open_engine(path: &Path, storage: TestStorage) -> Engine {
    let builder = match storage {
        TestStorage::Local => OpenOptions::local(path),
        TestStorage::SimulatedCloud => {
            OpenOptions::cloud_simulated(path, "reclamation-test", "cf/")
        }
    };
    Engine::open(
        builder
            .background_compaction(false)
            .with_memtable_size_limit(16 * 1024)
            .with_memtable_flush_threshold(16 * 1024)
            .build()
            .expect("build options"),
    )
    .expect("open engine")
}

fn write_and_flush(
    engine: &Engine,
    cf: &cntryl_midge::ColumnFamilyHandle,
    storage: TestStorage,
) -> Vec<String> {
    let mut transaction = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin write transaction");
    transaction
        .put(b"retained-key".to_vec(), vec![b'x'; 32 * 1024], None)
        .expect("put retained value");
    transaction
        .commit(match storage {
            TestStorage::Local => WriteOptions::sync(),
            TestStorage::SimulatedCloud => WriteOptions::cloud_strict(),
        })
        .expect("commit retained value");
    engine.flush_cf(cf).expect("flush column family");

    let layout = engine.get_storage_layout().expect("storage layout");
    let files = layout
        .levels
        .iter()
        .flat_map(|level| &level.files)
        .filter(|file| file.cf_id == cf.id())
        .map(|file| file.name.clone())
        .collect::<Vec<_>>();
    assert!(!files.is_empty(), "flush must publish at least one SST");
    files
}

fn manifest_contains_cf_files(engine: &Engine, cf_id: u32) -> bool {
    engine
        .get_storage_layout()
        .expect("storage layout")
        .levels
        .iter()
        .flat_map(|level| &level.files)
        .any(|file| file.cf_id == cf_id)
}

fn object_paths(root: &Path, storage: TestStorage, names: &[String]) -> Vec<PathBuf> {
    names
        .iter()
        .flat_map(|name| {
            let mut paths = vec![root.join("sst").join(name)];
            if matches!(storage, TestStorage::SimulatedCloud) {
                paths.push(root.join("hybrid_local").join("sst").join(name));
                paths.push(root.join("cloud_store").join("sst").join(name));
            }
            paths
        })
        .filter(|path| path.exists())
        .collect()
}

fn wait_until_reclaimed(
    engine: &Engine,
    root: &Path,
    storage: TestStorage,
    cf_id: u32,
    names: &[String],
) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let manifest_reclaimed = !manifest_contains_cf_files(engine, cf_id);
        let retained_objects = object_paths(root, storage, names);
        if manifest_reclaimed && retained_objects.is_empty() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "dropped CF {cf_id} was not reclaimed: manifest_reclaimed={manifest_reclaimed}, retained_objects={retained_objects:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn should_reclaim_column_family_files_given_no_snapshot_pin_when_gc_runs() {
    for storage in [TestStorage::Local, TestStorage::SimulatedCloud] {
        // Arrange
        let temp = tempfile::tempdir().expect("temp dir");
        let engine = open_engine(temp.path(), storage);
        let cf = engine
            .create_column_family("reclaim-unpinned")
            .expect("create column family");
        let files = write_and_flush(&engine, &cf, storage);

        // Act
        engine
            .drop_column_family(cf.id())
            .expect("drop column family");

        // Assert
        wait_until_reclaimed(&engine, temp.path(), storage, cf.id(), &files);
    }
}

#[test]
fn should_preserve_snapshot_visibility_given_column_family_drop_when_old_snapshot_is_active() {
    for storage in [TestStorage::Local, TestStorage::SimulatedCloud] {
        // Arrange
        let temp = tempfile::tempdir().expect("temp dir");
        let engine = open_engine(temp.path(), storage);
        let cf = engine
            .create_column_family("snapshot-drop")
            .expect("create column family");
        let files = write_and_flush(&engine, &cf, storage);
        let snapshot = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin pre-drop snapshot");

        // Act
        engine
            .drop_column_family(cf.id())
            .expect("drop column family");
        let pre_drop_value = snapshot
            .get(b"retained-key")
            .expect("read through pre-drop snapshot");
        let new_transaction = engine.begin_tx(cf.id(), TransactionMode::ReadOnly);

        // Assert
        assert_eq!(pre_drop_value, Some(Bytes::from(vec![b'x'; 32 * 1024])));
        assert!(matches!(
            new_transaction,
            Err(cntryl_midge::MidgeError::InvalidArgument(message))
                if message == format!("column family {} does not exist", cf.id())
        ));
        drop(snapshot);
        wait_until_reclaimed(&engine, temp.path(), storage, cf.id(), &files);
    }
}

#[test]
fn should_defer_column_family_file_reclamation_given_old_snapshot_pin_when_gc_runs() {
    for storage in [TestStorage::Local, TestStorage::SimulatedCloud] {
        // Arrange
        let temp = tempfile::tempdir().expect("temp dir");
        let engine = open_engine(temp.path(), storage);
        let cf = engine
            .create_column_family("reclaim-pinned")
            .expect("create column family");
        let files = write_and_flush(&engine, &cf, storage);
        let snapshot = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin pinned snapshot");

        // Act
        engine
            .drop_column_family(cf.id())
            .expect("drop column family");

        // Assert
        assert_eq!(
            snapshot.get(b"retained-key").expect("read pinned value"),
            Some(Bytes::from(vec![b'x'; 32 * 1024]))
        );
        assert!(
            manifest_contains_cf_files(&engine, cf.id()),
            "authoritative manifest must retain dropped-CF SSTs while a snapshot can read them"
        );
        assert!(
            !object_paths(temp.path(), storage, &files).is_empty(),
            "dropped-CF objects must remain while pinned"
        );

        drop(snapshot);
        wait_until_reclaimed(&engine, temp.path(), storage, cf.id(), &files);
    }
}
