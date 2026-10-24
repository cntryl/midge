//! Real loop-owned compute, publication and recovery with independent jobs.
use super::*;
use crate::lease::PrimaryLease as _;
use crate::runtime::{ResponseRouter, RuntimeConfig, RuntimeMsg, RuntimeState};
use crate::sst::{FsSstFactoryIo, SstFactory as _};
use crate::types::EntryType;
use std::sync::atomic::Ordering;
use std::time::Instant;
mod capture_queue_tests;
mod moves_tests;
mod publication_queue_tests;

struct ConcurrentFixture {
    event_loop: EventLoop,
    workers: crossbeam::channel::Receiver<RuntimeMsg>,
    requests: crossbeam::channel::Receiver<RuntimeMsg>,
    guard: crate::lease::LeaseGuard,
    lease: Arc<crate::lease::FileSystemLease>,
    directory: tempfile::TempDir,
}

impl ConcurrentFixture {
    fn new(memory: usize, debt: bool) -> MidgeResult<Self> {
        let directory = tempfile::tempdir()?;
        let fs = crate::io::real::test_root(directory.path());
        crate::metadata::ensure_or_create_format_marker(&fs, directory.path())?;
        crate::metadata::ManifestPersistence::save_with_fs(
            &fs,
            &crate::metadata::Manifest::default(),
        )
        .map_err(MidgeError::Internal)?;
        let lease = Arc::new(crate::lease::FileSystemLease::new(directory.path(), false)?);
        let guard = Arc::clone(&lease).try_acquire()?;
        let state = RuntimeState::new_in_epoch(directory.path(), false, lease.epoch());
        let (completed, workers) = crossbeam::channel::unbounded();
        let config = RuntimeConfig {
            compaction_concurrency: 2,
            flush_concurrency: 2,
            compaction_memory_limit: memory,
            background_compaction: debt,
            writer_epoch: lease.epoch(),
            leader_store: lease.get_leader_store(),
            leader_holder_id: Some(lease.holder_id()),
            lease_validity: Some(lease.lease_validity()),
            ..RuntimeConfig::default()
        };
        let mut event_loop = EventLoop::new(
            state,
            false,
            Arc::new(ResponseRouter::new()),
            config,
            crate::runtime::event_loop::FlushWorkerMode::Background(completed),
        )?;
        let factory = Arc::new(FsSstFactoryIo::new(
            crate::io::subdirectory(Arc::clone(&event_loop.state.fs), "sst"),
            64 * 1024,
        ));
        let mut config = crate::compaction::LeveledCompactionConfig::default();
        if debt {
            config.l1_target_size = 1;
            config.level_multiplier = 1;
        }
        let mut actor = crate::runtime::actors::CompactionActor::new_with_config(factory, config);
        // These fixtures exercise physical compute rather than a legal
        // metadata-only move of their tiny disjoint immutable sources.
        actor.set_execution_limits(128, memory);
        actor.configure_concurrency(2)?;
        actor.configure_work_target(None);
        event_loop.actors.compaction_actor = actor;
        Ok(Self {
            event_loop,
            workers,
            requests: crossbeam::channel::never(),
            guard,
            lease,
            directory,
        })
    }

    fn file(
        &mut self,
        level: u32,
        generation: u64,
        key: &[u8],
        value: &[u8],
        sequence: u64,
    ) -> MidgeResult<String> {
        let name = crate::cloud_layout::file_name(0, level, generation);
        let factory = FsSstFactoryIo::new(
            crate::io::subdirectory(Arc::clone(&self.event_loop.state.fs), "sst"),
            64 * 1024,
        )
        .with_compression_policy(crate::codec::CompressionPolicy::Fixed(
            crate::codec::CompressionAlgo::None,
        ));
        let mut writer = factory.create()?;
        writer.add_sorted_with_meta(key, Some(value), sequence, EntryType::Put, None)?;
        let finalized = writer
            .finish_to_path_with_summary(std::path::Path::new(&name))?
            .unwrap();
        let summary = finalized.summary.unwrap();
        self.event_loop
            .state
            .manifest
            .test_mut()
            .files
            .push(crate::metadata::FileMeta {
                name: name.clone(),
                level,
                cf_id: 0,
                sst_seq: generation,
                size_bytes: finalized.size_bytes,
                content_crc32c: Some(finalized.content_crc32c),
                smallest_key: Some(summary.smallest_key),
                largest_key: Some(summary.largest_key),
                smallest_seq: Some(summary.smallest_seq),
                largest_seq: Some(summary.largest_seq),
                key_bounds_complete: true,
                ..Default::default()
            });
        Ok(name)
    }

    fn persist(&mut self) -> MidgeResult<()> {
        let sequence = self
            .event_loop
            .state
            .manifest
            .files
            .iter()
            .filter_map(|file| file.largest_seq)
            .max()
            .unwrap_or(0);
        let mut value = serde_json::to_value(&*self.event_loop.state.manifest).unwrap();
        value["flushed_through"][0]["through_sequence"] = sequence.into();
        *self.event_loop.state.manifest.test_mut() = serde_json::from_value(value).unwrap();
        self.event_loop.state.sequence = sequence;
        self.event_loop.state.applied_sequence = sequence;
        crate::metadata::ManifestPersistence::save_with_fs(
            &self.event_loop.state.fs,
            &self.event_loop.state.manifest,
        )
        .map_err(MidgeError::Internal)?;
        self.event_loop.publish_snapshot();
        Ok(())
    }

    fn plan(&self, name: &str, source: u32, target: u32) -> crate::compaction::CompactionPlan {
        let cf_id = self
            .event_loop
            .state
            .manifest
            .files
            .iter()
            .find(|file| file.name == name)
            .expect("fixture source metadata")
            .cf_id;
        let mut plan = crate::compaction::CompactionPlan::new(cf_id, source, target);
        plan.input_files.push(name.into());
        plan.source_files.push(name.into());
        plan
    }

    fn assert_rows(&mut self, rows: &[(&[u8], &[u8])]) -> MidgeResult<()> {
        let snapshot = self
            .event_loop
            .create_read_snapshot(0)
            .expect("default family snapshot");
        let sequence = self.event_loop.state.applied_sequence;
        for &(key, value) in rows {
            assert_eq!(snapshot.get(key, sequence)?.as_deref(), Some(value));
        }
        let mut expected: Vec<_> = rows
            .iter()
            .map(|(key, value)| (key.to_vec(), value.to_vec()))
            .collect();
        expected.sort_unstable();
        assert_eq!(snapshot.range_scan(b"", b"", sequence)?, expected);
        Ok(())
    }

    fn progress(&mut self) {
        while let Ok(message) = self.workers.try_recv() {
            self.event_loop.process_one(message, &self.requests);
        }
        self.event_loop.background_progress(Some(&self.requests));
    }

    fn next_compute(&mut self, held: &HeldComputes) -> std::thread::ThreadId {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            self.progress();
            if let Ok(worker) = held.entered.try_recv() {
                return worker;
            }
            assert!(
                Instant::now() < deadline,
                "eligible debt must start promptly"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn drain(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while self.event_loop.actors.compaction_actor.active_count() > 0
            || self
                .event_loop
                .publication
                .compaction_publication
                .is_active()
        {
            assert!(
                Instant::now() < deadline,
                "all accepted compute/publication owners must settle"
            );
            self.progress();
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn restart(self, rows: &[(&[u8], &[u8])]) -> MidgeResult<()> {
        let Self {
            event_loop,
            guard,
            lease,
            directory,
            ..
        } = self;
        drop(event_loop);
        guard.release();
        lease.release()?;
        let mut reopened = crate::Engine::open(
            crate::OpenOptions::local(directory.path())
                .background_compaction(false)
                .compaction_concurrency(2)
                .build()?,
        )?;
        let cf = reopened.get_column_family("default").unwrap();
        let transaction = reopened.begin_tx(cf.id(), crate::TransactionMode::ReadOnly)?;
        for &(key, value) in rows {
            assert_eq!(transaction.get(key)?.as_deref(), Some(value));
        }
        let scanned = transaction.scan(&crate::Query::new())?.try_collect()?;
        let mut expected: Vec<_> = rows
            .iter()
            .map(|(key, value)| (key.to_vec(), value.to_vec()))
            .collect();
        expected.sort_unstable();
        assert_eq!(
            scanned
                .into_iter()
                .map(|(key, value)| (key.to_vec(), value.to_vec()))
                .collect::<Vec<_>>(),
            expected
        );
        drop(transaction);
        reopened.shutdown(Duration::from_secs(5))?;
        Ok(())
    }
}

#[test]
fn should_keep_older_overlapping_l0_sources_first_and_allow_only_one_l0_owner() -> MidgeResult<()> {
    let _guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    // Arrange: hot newer overlapping L0 files cannot pass the cold oldest
    // file; an unrelated level can still compute while that L0 owner is held.
    let mut fixture = ConcurrentFixture::new(16 * 1024 * 1024, false)?;
    for sequence in 1..=6 {
        let value = format!("value-{sequence}");
        fixture.file(0, sequence, b"a", value.as_bytes(), sequence)?;
    }
    let independent = fixture.file(3, 7, b"z", b"other", 6)?;
    for file in &fixture.event_loop.state.manifest.files {
        if file.level == 0 && file.sst_seq >= 4 {
            file.read_count.store(100, Ordering::Relaxed);
        }
    }
    fixture.persist()?;
    let plan = fixture
        .event_loop
        .actors
        .compaction_actor
        .check_manual_compaction(&fixture.event_loop.state)?
        .expect("L0 plan");
    let selected: std::collections::HashSet<_> = plan.source_files.iter().cloned().collect();
    let l0: Vec<_> = fixture
        .event_loop
        .state
        .manifest
        .files
        .iter()
        .filter(|file| file.level == 0)
        .collect();
    assert!(selected.contains(&crate::cloud_layout::file_name(0, 0, 1)));
    for older in l0.iter().filter(|file| !selected.contains(&file.name)) {
        for newer in l0.iter().filter(|file| selected.contains(&file.name)) {
            assert!(
                older.sst_seq > newer.sst_seq,
                "an older overlapping source must never remain above newer promoted data"
            );
        }
    }
    let competing = l0
        .iter()
        .find(|file| !selected.contains(&file.name))
        .map_or_else(|| plan.source_files[0].clone(), |file| file.name.clone());
    let held = HeldComputes::new();
    // Act
    fixture.event_loop.launch_compaction(plan)?;
    held.enter();
    assert!(matches!(
        fixture
            .event_loop
            .launch_compaction(fixture.plan(&competing, 0, 1)),
        Err(MidgeError::Busy(_))
    ));
    fixture
        .event_loop
        .launch_compaction(fixture.plan(&independent, 3, 4))?;
    held.enter();
    held.release.send(()).unwrap();
    held.release.send(()).unwrap();
    fixture.drain();
    // Assert: independent publication keeps the newest L0 version observable.
    fail::remove("midge::compaction::before_compute");
    drop(held);
    fixture.restart(&[(b"a", b"value-6"), (b"z", b"other")])?;
    scenario.teardown();
    Ok(())
}

#[test]
fn should_reuse_free_compute_slot_immediately_when_publication_leaves_level_debt() -> MidgeResult<()>
{
    let _guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    // Arrange: both worker slots are occupied; either completed output remains
    // above its one-byte target, independently of a new flush or timer tick.
    let mut fixture = ConcurrentFixture::new(16 * 1024 * 1024, true)?;
    let first = fixture.file(1, 1, b"a", b"first", 1)?;
    let second = fixture.file(3, 2, b"z", b"second", 2)?;
    fixture.persist()?;
    let held = HeldComputes::new();
    fixture
        .event_loop
        .launch_compaction(fixture.plan(&first, 1, 2))?;
    fixture
        .event_loop
        .launch_compaction(fixture.plan(&second, 3, 4))?;
    let workers = [held.enter(), held.enter()];
    // Act: one owner completes while the other remains physically held.
    held.release.send(()).unwrap();
    let replacement = fixture.next_compute(&held);
    // Assert: a reused fixed worker starts existing debt within five seconds;
    // the periodic idle timer is thirty seconds and no write or flush occurs.
    assert!(workers.contains(&replacement));
    assert_eq!(fixture.event_loop.actors.compaction_actor.active_count(), 2);
    fixture.event_loop.state.set_compaction_enabled(false);
    held.release.send(()).unwrap();
    held.release.send(()).unwrap();
    fixture.drain();
    fail::remove("midge::compaction::before_compute");
    drop(held);
    fixture.restart(&[(b"a", b"first"), (b"z", b"second")])?;
    scenario.teardown();
    Ok(())
}

#[test]
fn should_reject_conflicting_reserved_span_while_an_independent_job_publishes() -> MidgeResult<()> {
    let _guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    // Arrange: the first job owns both source a and its overlapping target.
    let mut fixture = ConcurrentFixture::new(16 * 1024 * 1024, false)?;
    let source = fixture.file(1, 1, b"a", b"current", 2)?;
    let target = fixture.file(2, 2, b"a", b"older", 1)?;
    let independent = fixture.file(3, 3, b"z", b"other", 2)?;
    fixture.persist()?;
    let mut first = fixture.plan(&source, 1, 2);
    first.input_files.push(target.clone());
    first.target_files.push(target.clone());
    let held = HeldComputes::new();
    fixture.event_loop.launch_compaction(first)?;
    held.enter();
    // Act: the second request would rewrite the captured target identity.
    let conflict = fixture
        .event_loop
        .launch_compaction(fixture.plan(&target, 2, 3));
    assert!(matches!(conflict, Err(MidgeError::Busy(_))));
    fixture
        .event_loop
        .launch_compaction(fixture.plan(&independent, 3, 4))?;
    held.enter();
    held.release.send(()).unwrap();
    held.release.send(()).unwrap();
    fixture.drain();
    // Assert: rejected work did not change the newer visible version; both
    // admitted owners survive publication and actual disk restart.
    assert_eq!(fixture.event_loop.actors.compaction_actor.active_count(), 0);
    fail::remove("midge::compaction::before_compute");
    drop(held);
    fixture.restart(&[(b"a", b"current"), (b"z", b"other")])?;
    scenario.teardown();
    Ok(())
}

#[test]
fn should_retain_oversized_input_while_sibling_job_uses_the_shared_memory_pool() -> MidgeResult<()>
{
    let _guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    // Arrange: one complete entry exceeds its 256KiB child quota; the sibling
    // uses the same 512KiB total pool and remains independently admissible.
    let mut fixture = ConcurrentFixture::new(512 * 1024, false)?;
    let large_value = vec![7; 256 * 1024];
    let oversized = fixture.file(1, 1, b"a", &large_value, 1)?;
    let small = fixture.file(3, 2, b"z", b"small", 2)?;
    fixture.persist()?;
    let held = HeldComputes::new();
    fixture
        .event_loop
        .launch_compaction(fixture.plan(&oversized, 1, 2))?;
    fixture
        .event_loop
        .launch_compaction(fixture.plan(&small, 3, 4))?;
    held.enter();
    held.enter();
    // Act
    held.release.send(()).unwrap();
    held.release.send(()).unwrap();
    fixture.drain();
    // Assert: no failed input is retired, the unrelated job commits, and the
    // authoritative shared ledger has no leaked or over-limit reservations.
    let files = &fixture.event_loop.state.manifest.files;
    assert!(files.iter().any(|file| file.name == oversized));
    assert!(!files.iter().any(|file| file.name == small));
    let (used, peak) = fixture
        .event_loop
        .actors
        .compaction_actor
        .shared_memory_usage();
    assert_eq!(used, 0);
    assert!(peak > 0 && peak <= 512 * 1024);
    fail::remove("midge::compaction::before_compute");
    drop(held);
    fixture.restart(&[(b"a", &large_value), (b"z", b"small")])?;
    scenario.teardown();
    Ok(())
}

struct HeldComputes {
    entered: crossbeam::channel::Receiver<std::thread::ThreadId>,
    release: crossbeam::channel::Sender<()>,
}
impl HeldComputes {
    fn new() -> Self {
        let (entered_tx, entered) = crossbeam::channel::bounded(64);
        let (release, release_rx) = crossbeam::channel::bounded(64);
        fail::cfg_callback("midge::compaction::before_compute", move || {
            entered_tx.send(std::thread::current().id()).unwrap();
            release_rx.recv().unwrap();
        })
        .unwrap();
        Self { entered, release }
    }
    fn enter(&self) -> std::thread::ThreadId {
        self.entered
            .recv_timeout(Duration::from_secs(5))
            .expect("compute worker entered")
    }
}
impl Drop for HeldComputes {
    fn drop(&mut self) {
        for _ in 0..64 {
            let _ = self.release.try_send(());
        }
    }
}

#[test]
fn should_publish_and_recover_two_disjoint_level_jobs_held_at_the_same_time() -> MidgeResult<()> {
    let _guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    // Arrange
    let mut fixture = ConcurrentFixture::new(16 * 1024 * 1024, false)?;
    let first = fixture.file(1, 1, b"a", b"first", 1)?;
    let second = fixture.file(3, 2, b"z", b"second", 2)?;
    fixture.persist()?;
    let held = HeldComputes::new();
    // Act: both real workers must enter before either receives a release.
    let first_plan = fixture.plan(&first, 1, 2);
    let second_plan = fixture.plan(&second, 3, 4);
    fixture.event_loop.launch_compaction(first_plan)?;
    fixture.event_loop.launch_compaction(second_plan)?;
    assert_ne!(held.enter(), held.enter());
    assert_eq!(
        fixture
            .event_loop
            .state
            .active_compactions
            .load(Ordering::Acquire),
        2
    );
    assert_eq!(
        fixture
            .event_loop
            .actors
            .compaction_actor
            .protected_output_generations()
            .len(),
        2
    );
    held.release.send(()).unwrap();
    held.release.send(()).unwrap();
    fixture.drain();
    // Assert: independent compute still uses current serialized publication.
    assert!(!fixture
        .event_loop
        .state
        .manifest
        .files
        .iter()
        .any(|file| file.name == first || file.name == second));
    assert_eq!(
        fixture
            .event_loop
            .state
            .manifest
            .files
            .iter()
            .map(|file| file.level)
            .collect::<std::collections::BTreeSet<_>>(),
        [2, 4].into_iter().collect()
    );
    fail::remove("midge::compaction::before_compute");
    drop(held);
    fixture.restart(&[(b"a", b"first"), (b"z", b"second")])?;
    scenario.teardown();
    Ok(())
}
