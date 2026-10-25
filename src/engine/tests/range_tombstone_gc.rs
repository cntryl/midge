use super::*;
#[test]
fn should_reclaim_covered_history_after_snapshot_release() -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let mut engine = Engine::open(
        OpenOptions::local(directory.path())
            .background_compaction(false)
            .build()?,
    )?;
    let cf = engine.get_column_family("default").expect("default CF");
    put_and_flush(&engine, &cf, b"h", b"old")?;
    engine.compact_all()?;
    put_and_flush(&engine, &cf, b"z", b"outside")?;
    engine.compact_all()?;
    let disjoint_sst = find_sst_containing(&engine, b"z")?;
    assert!(
        manifest_file(&engine, &disjoint_sst)?
            .expect("disjoint SST is published")
            .key_bounds_complete
    );
    let old_snapshot = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
    assert_eq!(old_snapshot.get(b"h")?.as_deref(), Some(b"old".as_slice()));

    // Act: compact the range delete while the old snapshot is still active.
    let mut delete = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    delete.delete_range(b"b".to_vec(), b"m".to_vec())?;
    delete.commit(WriteOptions::buffered())?;
    engine.flush_cf(&cf)?;
    engine.compact_all()?;

    // Assert the old and new snapshots observe their respective states, while
    // physical output retains the history required by SST-6.
    assert_eq!(old_snapshot.get(b"h")?.as_deref(), Some(b"old".as_slice()));
    let deleted_snapshot = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
    assert_eq!(deleted_snapshot.get(b"h")?, None);
    drop(deleted_snapshot);
    assert!(manifest_contains(&engine, &disjoint_sst)?);
    let retained = inspect_published_ssts(&engine)?;
    assert!(retained
        .range_markers
        .iter()
        .any(|marker| { marker.start == b"b" && marker.end == b"m" }));
    assert!(retained
        .versions
        .iter()
        .any(|version| { version.key == b"h" && version.value.as_deref() == Some(b"old") }));
    assert!(retained
        .versions
        .iter()
        .any(|version| { version.key == b"z" && version.value.as_deref() == Some(b"outside") }));

    // Release the pre-delete horizon, then create later L0 work in the same
    // span to drive another real bottommost compaction.
    drop(old_snapshot);
    put_and_flush(&engine, &cf, b"h", b"new")?;
    engine.compact_all()?;

    // Assert physical reclamation, the newer value, and unchanged disjoint SST.
    let reclaimed = inspect_published_ssts(&engine)?;
    assert!(!reclaimed
        .range_markers
        .iter()
        .any(|marker| { marker.start == b"b" && marker.end == b"m" }));
    assert!(!reclaimed
        .versions
        .iter()
        .any(|version| version.value.as_deref() == Some(b"old")));
    assert!(reclaimed
        .versions
        .iter()
        .any(|version| { version.key == b"z" && version.value.as_deref() == Some(b"outside") }));
    assert!(manifest_contains(&engine, &disjoint_sst)?);
    assert_eq!(
        reclaimed
            .versions
            .iter()
            .find(|version| version.key == b"h")
            .map(|version| version.value.as_deref()),
        Some(Some(b"new".as_slice()))
    );
    let latest = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
    assert_eq!(latest.get(b"h")?.as_deref(), Some(b"new".as_slice()));
    assert_eq!(latest.get(b"z")?.as_deref(), Some(b"outside".as_slice()));
    drop(latest);
    engine.shutdown(Duration::from_secs(5))
}

fn put_and_flush(
    engine: &Engine,
    cf: &ColumnFamilyHandle,
    key: &[u8],
    value: &[u8],
) -> MidgeResult<()> {
    let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
    tx.put(key.to_vec(), value.to_vec(), None)?;
    tx.commit(WriteOptions::buffered())?;
    engine.flush_cf(cf)
}

fn find_sst_containing(engine: &Engine, key: &[u8]) -> MidgeResult<String> {
    let factory = sst_factory(engine);
    let manifest = crate::metadata::ManifestPersistence::load(&engine.db_path)
        .map_err(crate::common::MidgeError::Internal)?;
    for file in manifest.files {
        let reader = factory.open(std::path::Path::new(&file.name))?;
        let cursor = reader.raw_version_cursor(None, None)?;
        if cursor
            .collect::<MidgeResult<Vec<_>>>()?
            .iter()
            .any(|version| version.key == key)
        {
            return Ok(file.name);
        }
    }
    Err(MidgeError::Internal(format!(
        "no published SST contains key {:?}",
        String::from_utf8_lossy(key)
    )))
}

fn manifest_contains(engine: &Engine, name: &str) -> MidgeResult<bool> {
    Ok(manifest_file(engine, name)?.is_some())
}

fn manifest_file(engine: &Engine, name: &str) -> MidgeResult<Option<crate::metadata::FileMeta>> {
    let manifest = crate::metadata::ManifestPersistence::load(&engine.db_path)
        .map_err(crate::common::MidgeError::Internal)?;
    Ok(manifest.files.into_iter().find(|file| file.name == name))
}

#[derive(Default)]
struct PublishedSstState {
    versions: Vec<crate::sst::traits::RawSstVersion>,
    range_markers: Vec<crate::types::RangeTombstone>,
}

fn inspect_published_ssts(engine: &Engine) -> MidgeResult<PublishedSstState> {
    let factory = sst_factory(engine);
    let manifest = crate::metadata::ManifestPersistence::load(&engine.db_path)
        .map_err(crate::common::MidgeError::Internal)?;
    let mut state = PublishedSstState::default();
    for file in manifest.files {
        let reader = factory.open(std::path::Path::new(&file.name))?;
        state.range_markers.extend(reader.range_tombstones());
        state.versions.extend(
            reader
                .raw_version_cursor(None, None)?
                .collect::<MidgeResult<Vec<_>>>()?,
        );
    }
    Ok(state)
}

fn sst_factory(engine: &Engine) -> crate::sst::FsSstFactoryIo {
    crate::sst::FsSstFactoryIo::new(
        crate::io::subdirectory(Arc::clone(&engine.root), crate::sst::fs::SST_DIR),
        64 * 1024,
    )
}
