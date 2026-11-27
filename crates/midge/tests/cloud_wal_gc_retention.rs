use cntryl_midge::__internal::wal::catalog::{CatalogEntry, WalPublicationCatalog};
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
    let original: Vec<_> = catalog(directory.path(), false)
        .entries()
        .iter()
        .map(CatalogEntry::id)
        .collect();
    assert!(
        original.len() >= 4,
        "idle family must pin pre-compaction WAL"
    );

    // Act
    engine.flush_cf(&idle).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let remaining = loop {
        let current = catalog(directory.path(), false);
        let remaining: Vec<_> = original
            .iter()
            .filter(|id| current.entry(**id).is_some())
            .copied()
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

fn catalog(root: &std::path::Path, mirror: bool) -> WalPublicationCatalog {
    let name = if mirror {
        "cloud_store/wal/publication-catalog.v1.mirror.json"
    } else {
        "cloud_store/wal/publication-catalog.v1.json"
    };
    WalPublicationCatalog::decode(&std::fs::read(root.join(name)).unwrap()).unwrap()
}

fn wait_for_retirement(root: &std::path::Path, original: &WalPublicationCatalog) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let done = [false, true].into_iter().all(|mirror| {
            let current = catalog(root, mirror);
            original.entries().iter().all(|entry| {
                current.entry(entry.id()).is_none()
                    && !root.join("cloud_store").join(entry.object_key()).exists()
            })
        });
        if done {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "original catalog entries or WAL objects remain: {:?}",
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
    for name in ["wal", "sst"] {
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
    assert!(original.entries().len() >= 4);

    // Act: compact while the unrelated family pins every generation.
    engine.compact_all().unwrap();
    #[cfg(feature = "internal-testing")]
    assert!(
        raw::tombstone_sequences(root, &engine, data.id()).is_empty(),
        "WAL retention must not pin tombstones after a complete compaction"
    );
    assert_value(&engine, data.id(), b"deleted-a", None);
    assert_value(&engine, data.id(), b"deleted-b", None);
    assert_eq!(catalog(root, false).entries(), original.entries());
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
fn should_preserve_point_deletion_when_compacted_wal_history_is_retired() {
    retention_control(false);
}

#[test]
fn should_preserve_range_deletion_when_compacted_wal_history_is_retired() {
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
fn should_reject_unsupported_layout_when_opening_historical_pre_fix_gc_history() {
    // Arrange: preserve the original pre-FORMAT9 bytes and their stuck prefix.
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    copy_fixture(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/wal-tombstone-gc-9734fab3"),
        root,
    );
    let original = catalog(root, false);
    assert_eq!(original.entries().len(), 4);
    // Act
    let Err(error) = Engine::open(options(root)) else {
        panic!("historical layout must not be silently migrated");
    };

    // Assert
    assert!(
        matches!(error, cntryl_midge::MidgeError::LeaseIndeterminate(message)
            if message.contains("legacy cloud lease layout is unsupported")
                && message.contains("FORMAT 9"))
    );
    assert_eq!(catalog(root, false).entries(), original.entries());
}

fn current_proof_options(
    cache: &std::path::Path,
    store: &cntryl_midge::__internal::simulation::SimulatedObjectStore,
    clock: &cntryl_midge::__internal::simulation::SimulatedClock,
) -> OpenOptions {
    OpenOptions::cloud(
        cache,
        cntryl_midge::CloudStorageLocation::new(
            cntryl_midge::CloudProviderConfig::aws_s3_static(
                "bucket",
                "us-east-1",
                "key",
                "secret",
            ),
            "deletion-proof/",
        ),
    )
    .object_store_for_testing(std::sync::Arc::new(store.process()))
    .lease_clock_for_testing(std::sync::Arc::new(clock.clone()))
    .lease_ttl(Duration::from_secs(18))
    .background_compaction(false)
    .build()
    .unwrap()
}

#[test]
fn should_replay_deletion_when_cold_restart_has_no_sst_proof() {
    use cntryl_midge::__internal::simulation::{Schedule, SimulatedClock, SimulatedObjectStore};

    // Arrange: current-format committed WAL is the only deletion proof. The
    // compacted-history controls separately prove physical tombstone GC;
    // this legal preimage proves startup cannot infer coverage from absence.
    let cache = tempfile::tempdir().unwrap();
    let store = SimulatedObjectStore::new(Schedule::Eager);
    let clock = SimulatedClock::new();
    let engine = Engine::open(current_proof_options(cache.path(), &store, &clock)).unwrap();
    let idle = engine.create_column_family("idle").unwrap();
    let data = engine.create_column_family("data").unwrap();
    put(&engine, idle.id(), b"idle", b"unflushed");
    put(&engine, data.id(), b"deleted", b"old-value");
    let mut deletion = engine
        .begin_tx(data.id(), TransactionMode::ReadWrite)
        .unwrap();
    deletion.delete(b"deleted".to_vec()).unwrap();
    deletion.commit(WriteOptions::cloud_strict()).unwrap();
    assert!(engine
        .metrics()
        .get_storage_layout()
        .unwrap()
        .levels
        .iter()
        .flat_map(|level| &level.files)
        .all(|file| file.cf_id != data.id()));
    let image = store.image();
    let original = WalPublicationCatalog::decode(
        &image.objects["deletion-proof/wal/publication-catalog.v1.json"],
    )
    .unwrap();
    assert!(original.entries().len() >= 3);
    let recovered_store = SimulatedObjectStore::from_image(Schedule::Eager, &image);
    drop(engine);
    clock.advance(Duration::from_secs(36));
    let fresh_cache = tempfile::tempdir().unwrap();

    // Act: the independent image excludes every cleanup effect of Drop.
    let mut recovered = Engine::open(current_proof_options(
        fresh_cache.path(),
        &recovered_store,
        &clock,
    ))
    .unwrap();
    assert_value(&recovered, data.id(), b"deleted", None);
    assert_value(&recovered, idle.id(), b"idle", Some(b"unflushed"));
    assert!(
        recovered
            .metrics()
            .get_recovery_metrics()
            .unwrap()
            .wal_recovery_records_replayed
            > 0
    );
    recovered
        .flush_cf(&recovered.get_column_family("data").unwrap())
        .unwrap();
    recovered
        .flush_cf(&recovered.get_column_family("idle").unwrap())
        .unwrap();
    recovered.shutdown(Duration::from_secs(30)).unwrap();
    drop(recovered);
    let cold_cache = tempfile::tempdir().unwrap();
    let mut reopened = Engine::open(current_proof_options(
        cold_cache.path(),
        &recovered_store,
        &clock,
    ))
    .unwrap();

    // Assert: published coverage survives another complete cache loss.
    assert_value(&reopened, data.id(), b"deleted", None);
    assert_value(&reopened, idle.id(), b"idle", Some(b"unflushed"));
    let retired = WalPublicationCatalog::decode(
        &recovered_store.image().objects["deletion-proof/wal/publication-catalog.v1.json"],
    )
    .unwrap();
    assert!(original
        .entries()
        .iter()
        .all(|entry| retired.entry(entry.id()).is_none()));
    reopened.shutdown(Duration::from_secs(30)).unwrap();
}

#[cfg(feature = "internal-testing")]
#[path = "cloud_wal_gc_retention/raw.rs"]
mod raw;

#[cfg(feature = "internal-testing")]
#[test]
fn should_collect_tombstones_while_unrelated_family_keeps_wal_pinned() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let mut engine = Engine::open(options(root)).unwrap();
    let idle = engine.create_column_family("idle").unwrap();
    let data = engine.create_column_family("data").unwrap();

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

        // Assert: tombstones are collected while unrelated WAL remains authoritative.
        let recent = raw::tombstone_sequences(root, &engine, data.id());
        assert!(
            recent.is_empty(),
            "WAL still pinned collected tombstones: {recent:?}"
        );
        assert_ne!(catalog(root, false).entries().len(), 0);
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
