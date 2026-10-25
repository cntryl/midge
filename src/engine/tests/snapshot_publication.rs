use super::*;

struct ReaderStopGuard(Arc<std::sync::atomic::AtomicBool>);

impl Drop for ReaderStopGuard {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::Release);
    }
}

struct SnapshotReader<'a> {
    engine: &'a Engine,
    old_id: u32,
    active_id: Arc<std::sync::atomic::AtomicU32>,
    stop: Arc<std::sync::atomic::AtomicBool>,
    old_ssts: Arc<std::collections::HashSet<String>>,
    old_captures: Arc<std::sync::atomic::AtomicUsize>,
    new_captures: Arc<std::sync::atomic::AtomicUsize>,
    old_rejections: Arc<std::sync::atomic::AtomicUsize>,
    start: Arc<std::sync::Barrier>,
    rejected_tx: std::sync::mpsc::Sender<()>,
}

fn run_snapshot_reader(reader: SnapshotReader<'_>) {
    reader.start.wait();
    let mut reported_rejection = false;
    while !reader.stop.load(std::sync::atomic::Ordering::Acquire) {
        let cf_id = reader.active_id.load(std::sync::atomic::Ordering::Acquire);
        match reader.engine.begin_tx(cf_id, TransactionMode::ReadOnly) {
            Ok(snapshot) => {
                let expected_ssts = if cf_id == reader.old_id {
                    Arc::clone(&reader.old_ssts)
                } else {
                    reader
                        .engine
                        .runtime_handle
                        .snapshot_cache
                        .load_family(cf_id)
                        .expect("published live family")
                        .1
                        .pinned_sst_names()
                };
                assert_generation_snapshot(&snapshot, reader.engine, cf_id, &expected_ssts);
                if cf_id == reader.old_id {
                    reader
                        .old_captures
                        .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                } else {
                    reader
                        .new_captures
                        .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                }
            }
            Err(MidgeError::InvalidArgument(_)) => {
                assert_eq!(
                    cf_id, reader.old_id,
                    "only the dropped family ID may reject"
                );
                reader
                    .old_rejections
                    .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                if !reported_rejection {
                    reader
                        .rejected_tx
                        .send(())
                        .expect("main thread waits for post-drop capture");
                    reported_rejection = true;
                }
            }
            Err(MidgeError::WriteStall(message)) if message == "runtime request queue is full" => {
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(error) => panic!("unexpected snapshot capture error: {error:?}"),
        }
        std::thread::yield_now();
    }
}

fn assert_generation_snapshot(
    snapshot: &crate::engine::api::Transaction,
    engine: &Engine,
    cf_id: u32,
    expected_ssts: &std::collections::HashSet<String>,
) {
    assert_eq!(
        snapshot
            .get(b"generation")
            .expect("read captured generation"),
        Some(bytes::Bytes::copy_from_slice(&cf_id.to_be_bytes())),
        "a snapshot for CF {cf_id} must retain that generation's source"
    );
    assert!(!expected_ssts.is_empty());
    assert!(
        expected_ssts.is_subset(
            &engine
                .runtime_handle
                .snapshot_pins
                .pinned_sst_names(Duration::from_secs(10))
        ),
        "captured snapshot sources must remain pinned while its transaction is live"
    );
}

fn wait_for_capture(count: &std::sync::atomic::AtomicUsize, description: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while count.load(std::sync::atomic::Ordering::Acquire) == 0 {
        assert!(std::time::Instant::now() < deadline, "{description}");
        std::thread::yield_now();
    }
}

fn write_and_flush_generation(
    engine: &Engine,
    family: &ColumnFamilyHandle,
) -> MidgeResult<Arc<std::collections::HashSet<String>>> {
    let mut write = engine.begin_tx(family.id(), TransactionMode::ReadWrite)?;
    write.put(
        b"generation".to_vec(),
        family.id().to_be_bytes().to_vec(),
        None,
    )?;
    write.commit(WriteOptions::buffered())?;
    engine.flush_cf(family)?;
    let pinned_ssts = engine
        .runtime_handle
        .snapshot_cache
        .load_family(family.id())
        .expect("published family generation")
        .1
        .pinned_sst_names();
    assert!(
        !pinned_ssts.is_empty(),
        "generation should publish SST files"
    );
    Ok(pinned_ssts)
}

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
fn should_capture_only_the_live_generation_during_family_recreation() -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let mut engine = Engine::open(
        OpenOptions::local(directory.path())
            .background_compaction(false)
            .build()?,
    )?;
    let old_family = engine.create_column_family("racing-generation")?;
    let old_id = old_family.id();
    let old_snapshot_ssts = write_and_flush_generation(&engine, &old_family)?;

    let active_id = Arc::new(std::sync::atomic::AtomicU32::new(old_id));
    let stop_reader = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let old_captures = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let new_captures = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let old_rejections = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let start_reader = Arc::new(std::sync::Barrier::new(2));
    let (rejected_tx, rejected_rx) = std::sync::mpsc::channel();

    // Act: capture repeatedly while the event loop removes and republishes the
    // same family name under a fresh ID.
    std::thread::scope(|scope| {
        let _stop_reader_on_scope_exit = ReaderStopGuard(Arc::clone(&stop_reader));
        let reader_engine = &engine;
        let reader_active_id = Arc::clone(&active_id);
        let reader_stop = Arc::clone(&stop_reader);
        let reader_old_ssts = Arc::clone(&old_snapshot_ssts);
        let reader_old_captures = Arc::clone(&old_captures);
        let reader_new_captures = Arc::clone(&new_captures);
        let reader_old_rejections = Arc::clone(&old_rejections);
        let reader_start = Arc::clone(&start_reader);
        let reader = scope.spawn(move || {
            run_snapshot_reader(SnapshotReader {
                engine: reader_engine,
                old_id,
                active_id: reader_active_id,
                stop: reader_stop,
                old_ssts: reader_old_ssts,
                old_captures: reader_old_captures,
                new_captures: reader_new_captures,
                old_rejections: reader_old_rejections,
                start: reader_start,
                rejected_tx,
            });
        });

        start_reader.wait();
        wait_for_capture(&old_captures, "reader captured old family");
        engine
            .drop_column_family(old_id)
            .expect("drop old family while captures are in flight");
        rejected_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("reader observes removal before recreation");

        let new_family = engine
            .create_column_family("racing-generation")
            .expect("recreate family with a fresh ID");
        let new_id = new_family.id();
        assert_ne!(old_id, new_id);
        write_and_flush_generation(&engine, &new_family).expect("publish new generation");
        active_id.store(new_id, std::sync::atomic::Ordering::Release);

        wait_for_capture(&new_captures, "reader captured new family");
        stop_reader.store(true, std::sync::atomic::Ordering::Release);
        reader.join().expect("join concurrent snapshot reader");
    });

    // Assert
    assert!(old_captures.load(std::sync::atomic::Ordering::Acquire) > 0);
    assert!(new_captures.load(std::sync::atomic::Ordering::Acquire) > 0);
    assert!(old_rejections.load(std::sync::atomic::Ordering::Acquire) > 0);
    engine.shutdown(Duration::from_secs(5))
}

#[test]
fn should_keep_retired_family_snapshot_pinned_after_recreation() -> MidgeResult<()> {
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
