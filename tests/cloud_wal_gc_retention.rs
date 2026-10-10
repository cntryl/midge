use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
use std::time::{Duration, Instant};

fn put(engine: &Engine, cf: u32, key: &[u8], value: &[u8]) {
    let mut tx = engine.begin_tx(cf, TransactionMode::ReadWrite).unwrap();
    tx.put(key.to_vec(), value.to_vec(), None).unwrap();
    tx.commit(WriteOptions::cloud_strict()).unwrap();
}

#[test]
fn should_retire_cloud_wal_after_compaction_collects_tombstones_before_idle_family_flush() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let options = OpenOptions::cloud_simulated(directory.path(), "bucket", "gc-retention")
        .background_compaction(false)
        .with_memtable_size_limit(1024 * 1024)
        .build()
        .unwrap();
    let mut engine = Engine::open(options).unwrap();
    let idle = engine.create_column_family("idle").unwrap();
    let data = engine.create_column_family("data").unwrap();
    put(&engine, idle.id(), b"idle", b"unflushed");
    for value in [b"one".as_slice(), b"two", b"three"] {
        put(&engine, data.id(), b"deleted", value);
        engine.flush_cf(&data).unwrap();
    }
    let mut tx = engine
        .begin_tx(data.id(), TransactionMode::ReadWrite)
        .unwrap();
    tx.delete(b"deleted".to_vec()).unwrap();
    tx.commit(WriteOptions::cloud_strict()).unwrap();
    engine.flush_cf(&data).unwrap();
    engine.compact_all().unwrap();
    let read = engine
        .begin_tx(data.id(), TransactionMode::ReadOnly)
        .unwrap();
    assert!(read.get(b"deleted").unwrap().is_none());
    drop(read);
    let catalog_path = directory
        .path()
        .join("cloud_store/wal/publication-catalog.v1.json");
    let before: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&catalog_path).unwrap()).unwrap();
    let original: Vec<_> = before["segments"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    assert!(
        original.len() >= 4,
        "idle family must pin pre-compaction WAL"
    );

    // Act
    engine.flush_cf(&idle).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let remaining = loop {
        let catalog: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&catalog_path).unwrap()).unwrap();
        let segments = catalog["segments"].as_object().unwrap();
        let remaining: Vec<_> = original
            .iter()
            .filter(|id| segments.contains_key(*id))
            .cloned()
            .collect();
        if remaining.is_empty() || Instant::now() >= deadline {
            break remaining;
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    engine.shutdown(Duration::from_secs(30)).unwrap();

    // Assert
    assert!(
        remaining.is_empty(),
        "compacted, flushed history still pins WAL: {remaining:?}"
    );
}

fn options(root: &std::path::Path) -> cntryl_midge::OpenOptions {
    OpenOptions::cloud_simulated(root, "bucket", "gc-retention")
        .background_compaction(false)
        .with_memtable_size_limit(1024 * 1024)
        .build()
        .unwrap()
}

fn catalog(root: &std::path::Path, mirror: bool) -> serde_json::Value {
    let name = if mirror {
        "cloud_store/wal/publication-catalog.v1.mirror.json"
    } else {
        "cloud_store/wal/publication-catalog.v1.json"
    };
    serde_json::from_slice(&std::fs::read(root.join(name)).unwrap()).unwrap()
}

fn wait_for_retirement(root: &std::path::Path, original: &serde_json::Value) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let done = [false, true].into_iter().all(|mirror| {
            let current = catalog(root, mirror);
            original["segments"]
                .as_object()
                .unwrap()
                .iter()
                .all(|(id, entry)| {
                    current["segments"].get(id).is_none()
                        && !root
                            .join("cloud_store")
                            .join(entry["object_key"].as_str().unwrap())
                            .exists()
                })
        });
        if done {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "original catalog entries or WAL objects remain: {}",
            catalog(root, false)
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn assert_value(engine: &Engine, cf: u32, key: &[u8], expected: Option<&[u8]>) {
    let tx = engine.begin_tx(cf, TransactionMode::ReadOnly).unwrap();
    let actual = tx.get(key).unwrap();
    assert_eq!(actual.as_deref(), expected, "key {key:?}");
}

fn clear_caches(root: &std::path::Path) {
    for name in ["wal", "sst", "hybrid_local"] {
        let path = root.join(name);
        if path.exists() {
            std::fs::remove_dir_all(path).unwrap();
        }
    }
}

fn retention_control(range: bool) {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let mut engine = Engine::open(options(root)).unwrap();
    let idle = engine.create_column_family("idle").unwrap();
    let data = engine.create_column_family("data").unwrap();
    put(&engine, idle.id(), b"idle", b"unflushed");
    for value in [b"one".as_slice(), b"two", b"three"] {
        put(&engine, data.id(), b"deleted-a", value);
        put(&engine, data.id(), b"deleted-b", value);
        put(&engine, data.id(), value, value);
        engine.flush_cf(&data).unwrap();
    }
    let mut tx = engine
        .begin_tx(data.id(), TransactionMode::ReadWrite)
        .unwrap();
    if range {
        tx.delete_range(b"deleted-".to_vec(), b"deleted-z".to_vec())
            .unwrap();
    } else {
        tx.delete(b"deleted-a".to_vec()).unwrap();
        tx.delete(b"deleted-b".to_vec()).unwrap();
    }
    tx.commit(WriteOptions::cloud_strict()).unwrap();
    engine.flush_cf(&data).unwrap();
    let original = catalog(root, false);
    assert!(original["segments"].as_object().unwrap().len() >= 4);

    // Act: compact while the unrelated family pins every generation.
    engine.compact_all().unwrap();
    #[cfg(feature = "internal-testing")]
    assert_ne!(
        raw::tombstone_sequences(root, &engine, data.id()),
        Vec::<u64>::new()
    );
    assert_value(&engine, data.id(), b"deleted-a", None);
    assert_value(&engine, data.id(), b"deleted-b", None);
    assert_eq!(catalog(root, false)["segments"], original["segments"]);
    engine.flush_cf(&idle).unwrap();
    wait_for_retirement(root, &original);
    engine.shutdown(Duration::from_secs(30)).unwrap();
    drop(engine);
    clear_caches(root);
    let mut recovered = Engine::open(options(root)).unwrap();

    // Assert: a cold restart preserves every surviving acknowledged value and deletion.
    let data = recovered.get_column_family("data").unwrap();
    let idle = recovered.get_column_family("idle").unwrap();
    assert_value(&recovered, idle.id(), b"idle", Some(b"unflushed"));
    for value in [b"one".as_slice(), b"two", b"three"] {
        assert_value(&recovered, data.id(), value, Some(value));
    }
    assert_value(&recovered, data.id(), b"deleted-a", None);
    assert_value(&recovered, data.id(), b"deleted-b", None);
    recovered.shutdown(Duration::from_secs(30)).unwrap();
}

#[test]
fn should_preserve_point_delete_proof_until_idle_family_allows_wal_retirement() {
    retention_control(false);
}

#[test]
fn should_preserve_range_delete_proof_until_idle_family_allows_wal_retirement() {
    retention_control(true);
}

fn copy_fixture(source: &std::path::Path, destination: &std::path::Path) {
    std::fs::create_dir_all(destination).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let target = destination.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_fixture(&entry.path(), &target);
        } else if entry.file_name() != "README.txt" {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

#[test]
fn should_replay_missing_deletion_proof_when_restarting_pre_fix_stuck_history() {
    // Arrange: real pre-fix bytes, no locally cached WAL or SSTs.
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    copy_fixture(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/wal-tombstone-gc-9734fab3"),
        root,
    );
    let original = catalog(root, false);
    assert_eq!(original["segments"].as_object().unwrap().len(), 4);
    // Act: restart replays WAL state absent from the compacted SST layout.
    let mut engine = Engine::open(options(root)).unwrap();
    #[cfg(feature = "internal-testing")]
    assert_eq!(
        raw::tombstone_sequences(root, &engine, 2),
        Vec::<u64>::new()
    );
    let data = engine.get_column_family("data").unwrap();
    let idle = engine.get_column_family("idle").unwrap();
    assert_value(&engine, data.id(), b"deleted", None);
    assert_value(&engine, idle.id(), b"idle", Some(b"unflushed"));
    engine.flush_cf(&data).unwrap();
    wait_for_retirement(root, &original);
    engine.shutdown(Duration::from_secs(30)).unwrap();
    drop(engine);
    clear_caches(root);
    let mut recovered = Engine::open(options(root)).unwrap();

    // Assert
    assert_value(&recovered, data.id(), b"deleted", None);
    assert_value(&recovered, idle.id(), b"idle", Some(b"unflushed"));
    recovered.shutdown(Duration::from_secs(30)).unwrap();
}

#[cfg(feature = "internal-testing")]
#[path = "cloud_wal_gc_retention/raw.rs"]
mod raw;

#[cfg(feature = "internal-testing")]
#[test]
fn should_collect_older_tombstones_while_newer_wal_remains_pinned() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let mut engine = Engine::open(options(root)).unwrap();
    let idle = engine.create_column_family("idle").unwrap();
    let data = engine.create_column_family("data").unwrap();
    let mut older = Vec::new();

    // Act: each cycle retires its WAL, then the next cycle compacts its proof.
    for cycle in 0..4 {
        let point = format!("point-{cycle}");
        let range = format!("range-{cycle}");
        let end = format!("range-{cycle}z");
        put(&engine, idle.id(), b"idle", b"pinned");
        for value in [b"one".as_slice(), b"two", b"three"] {
            put(&engine, data.id(), point.as_bytes(), value);
            put(&engine, data.id(), range.as_bytes(), value);
            // Force overlapping plans and keep an acknowledged survivor per cycle.
            put(&engine, data.id(), b"a-survivor", b"alive");
            put(&engine, data.id(), b"z-survivor", b"alive");
            engine.flush_cf(&data).unwrap();
        }
        let mut tx = engine
            .begin_tx(data.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.delete(point.as_bytes().to_vec()).unwrap();
        tx.delete_range(range.as_bytes().to_vec(), end.as_bytes().to_vec())
            .unwrap();
        tx.commit(WriteOptions::cloud_strict()).unwrap();
        engine.flush_cf(&data).unwrap();
        let original = catalog(root, false);
        engine.compact_all().unwrap();

        // Assert: older proof is collected even though recent WAL is still authoritative.
        let recent = raw::tombstone_sequences(root, &engine, data.id());
        assert_ne!(recent, Vec::<u64>::new());
        assert!(
            older.iter().all(|seq| !recent.contains(seq)),
            "old proof was retained: {older:?} -> {recent:?}"
        );
        assert!(!catalog(root, false)["segments"]
            .as_object()
            .unwrap()
            .is_empty());
        for prior in 0..=cycle {
            assert_value(
                &engine,
                data.id(),
                format!("point-{prior}").as_bytes(),
                None,
            );
            assert_value(
                &engine,
                data.id(),
                format!("range-{prior}").as_bytes(),
                None,
            );
        }
        assert_value(&engine, data.id(), b"a-survivor", Some(b"alive"));
        assert_value(&engine, data.id(), b"z-survivor", Some(b"alive"));
        engine.flush_cf(&idle).unwrap();
        wait_for_retirement(root, &original);
        older = recent;
    }
    engine.shutdown(Duration::from_secs(30)).unwrap();
    drop(engine);
    clear_caches(root);
    let mut recovered = Engine::open(options(root)).unwrap();
    for cycle in 0..4 {
        assert_value(
            &recovered,
            data.id(),
            format!("point-{cycle}").as_bytes(),
            None,
        );
        assert_value(
            &recovered,
            data.id(),
            format!("range-{cycle}").as_bytes(),
            None,
        );
    }
    assert_value(&recovered, data.id(), b"a-survivor", Some(b"alive"));
    assert_value(&recovered, data.id(), b"z-survivor", Some(b"alive"));
    recovered.shutdown(Duration::from_secs(30)).unwrap();
}
