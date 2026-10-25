use super::*;

#[test]
fn should_allocate_one_read_snapshot_when_committing_one_of_one_hundred_families() -> MidgeResult<()>
{
    // Arrange
    let directory = tempfile::tempdir()?;
    let mut engine = Engine::open(
        OpenOptions::local(directory.path())
            .background_compaction(false)
            .build()?,
    )?;
    let mut families = vec![engine
        .get_column_family("default")
        .expect("default family exists")];
    for index in 1..100 {
        families.push(engine.create_column_family(&format!("family-{index}"))?);
    }
    let snapshots = Arc::clone(&engine.runtime_handle.snapshot_cache);
    let before = families
        .iter()
        .map(|family| {
            (
                family.id(),
                snapshots
                    .load_family(family.id())
                    .expect("published family snapshot")
                    .1,
            )
        })
        .collect::<std::collections::HashMap<_, _>>();
    engine
        .runtime_handle
        .diagnostics
        .reset_read_snapshot_builds();

    // Act: the public transaction path takes its TTL-time wrapper clone, then
    // publishes a write to only one family.
    let mut transaction = engine.begin_tx(families[0].id(), TransactionMode::ReadWrite)?;
    transaction.put(b"one-family".to_vec(), b"visible".to_vec(), None)?;
    transaction.commit(WriteOptions::buffered())?;

    // Assert: the only new wrapper is the shallow clone made by begin_tx;
    // publication constructs no replacement snapshot for any family.
    assert_eq!(
        engine.runtime_handle.diagnostics.read_snapshot_builds(),
        1,
        "begin_tx should allocate one TTL-time wrapper and publication should reuse source Arcs"
    );
    for family in &families {
        let current = snapshots
            .load_family(family.id())
            .expect("published family snapshot")
            .1;
        assert!(
            Arc::ptr_eq(&before[&family.id()], &current),
            "snapshot sources changed for family {}",
            family.id()
        );
    }
    let (sequence, current) = snapshots
        .load_family(families[0].id())
        .expect("published write family snapshot");
    assert_eq!(
        current.get(b"one-family", sequence)?,
        Some(b"visible".to_vec())
    );

    engine.shutdown(Duration::from_secs(5))
}

#[test]
fn should_keep_captured_family_snapshot_and_pin_across_drop_recreation() -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let mut engine = Engine::open(
        OpenOptions::local(directory.path())
            .background_compaction(false)
            .build()?,
    )?;
    let old_family = engine.create_column_family("recreated")?;
    let mut old_write = engine.begin_tx(old_family.id(), TransactionMode::ReadWrite)?;
    old_write.put(b"generation".to_vec(), b"old".to_vec(), None)?;
    old_write.commit(WriteOptions::buffered())?;
    engine.flush_cf(&old_family)?;

    let old_snapshot = engine.begin_tx(old_family.id(), TransactionMode::ReadOnly)?;
    let old_sequence = old_snapshot.start_sequence();
    let old_pins = engine
        .runtime_handle
        .snapshot_pins
        .pinned_sst_names(Duration::from_secs(10));
    assert!(!old_pins.is_empty(), "flushed family snapshot pins its SST");
    assert_eq!(
        old_snapshot.get(b"generation")?.as_deref(),
        Some(b"old".as_slice())
    );

    // Act: drop and recreate the same name while the old snapshot remains live.
    engine.drop_column_family(old_family.id())?;
    let new_family = engine.create_column_family("recreated")?;
    assert_ne!(old_family.id(), new_family.id());
    let mut new_write = engine.begin_tx(new_family.id(), TransactionMode::ReadWrite)?;
    new_write.put(b"generation".to_vec(), b"new".to_vec(), None)?;
    new_write.commit(WriteOptions::buffered())?;
    engine.flush_cf(&new_family)?;
    let new_snapshot = engine.begin_tx(new_family.id(), TransactionMode::ReadOnly)?;

    // Assert: the captured old source and its GC pin survive later publications.
    assert!(old_pins.is_subset(
        &engine
            .runtime_handle
            .snapshot_pins
            .pinned_sst_names(Duration::from_secs(10))
    ));
    assert!(new_snapshot.start_sequence() > old_sequence);
    assert_eq!(
        old_snapshot.get(b"generation")?.as_deref(),
        Some(b"old".as_slice())
    );
    assert_eq!(
        new_snapshot.get(b"generation")?.as_deref(),
        Some(b"new".as_slice())
    );

    drop(new_snapshot);
    drop(old_snapshot);
    engine.shutdown(Duration::from_secs(5))
}
