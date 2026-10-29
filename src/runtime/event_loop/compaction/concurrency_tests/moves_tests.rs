//! Metadata-only promotion through real publication and restart.
use super::*;
use crate::common::resource_budget::ResourceBudget;
use crate::sst::traits::{DynSstWriter, SstReaderExt};
use std::path::Path;

/// A move must never ask for an SST reader or writer. Other filesystem work
/// still uses the real database so journal/intent durability is exercised.
struct NoRewriteFactory(Arc<dyn crate::io::Fs>);
impl crate::sst::SstFactory for NoRewriteFactory {
    fn output_fs(&self) -> Arc<dyn crate::io::Fs> {
        Arc::clone(&self.0)
    }
    fn compaction_scratch_cleanup_verified(&self) -> bool {
        true
    }
    fn create(&self) -> MidgeResult<Box<dyn DynSstWriter>> {
        panic!("move created SST writer")
    }
    fn create_for_compaction(&self, _: ResourceBudget) -> MidgeResult<Box<dyn DynSstWriter>> {
        panic!("move created SST writer")
    }
    fn open(&self, _: &Path) -> MidgeResult<Box<dyn SstReaderExt>> {
        panic!("move opened SST reader")
    }
    fn open_for_compaction(
        &self,
        _: &Path,
        _: ResourceBudget,
    ) -> MidgeResult<Box<dyn SstReaderExt>> {
        panic!("move opened SST reader")
    }
}

fn move_fixture() -> MidgeResult<(ConcurrentFixture, String)> {
    let mut fixture = ConcurrentFixture::new(16 * 1024 * 1024, false)?;
    let name = fixture.file(0, 1, b"key", b"value", 1)?;
    fixture.persist()?;
    let factory = Arc::new(NoRewriteFactory(crate::io::subdirectory(
        Arc::clone(&fixture.event_loop.state.fs),
        "sst",
    )));
    let mut actor = crate::runtime::actors::CompactionActor::new_with_config(
        factory,
        crate::compaction::LeveledCompactionConfig::default(),
    );
    actor.set_execution_limits(64 * 1024, 16 * 1024 * 1024);
    actor.configure_concurrency(2)?;
    fixture.event_loop.actors.compaction_actor = actor;
    Ok((fixture, name))
}

fn range_tombstone_file(fixture: &mut ConcurrentFixture) -> MidgeResult<String> {
    let name = crate::cloud_layout::file_name(0, 0, 1);
    let factory = FsSstFactoryIo::new(
        crate::io::subdirectory(Arc::clone(&fixture.event_loop.state.fs), "sst"),
        64 * 1024,
    );
    let mut writer = factory.create()?;
    writer.add_sorted_with_meta(b"m", Some(b"stale"), 1, EntryType::Put, None)?;
    writer.add_range_tombstone(b"a", b"z", 2)?;
    let finalized = writer
        .finish_to_path_with_summary(std::path::Path::new(&name))?
        .expect("range tombstone SST summary");
    let summary = finalized.summary.expect("range tombstone key bounds");
    fixture
        .event_loop
        .state
        .manifest
        .test_mut()
        .files
        .push(crate::metadata::FileMeta {
            name: name.clone(),
            level: 0,
            cf_id: 0,
            sst_seq: 1,
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

fn current_level(fixture: &ConcurrentFixture, name: &str) -> u32 {
    let files = &fixture.event_loop.state.manifest.files;
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].name, name);
    assert!(fixture.event_loop.state.manifest.retired_ssts.is_empty());
    assert!(fixture.directory.path().join("sst").join(name).exists());
    files[0].level
}

struct HeldPublication {
    entered: crossbeam::channel::Receiver<()>,
    release: crossbeam::channel::Sender<()>,
}
impl HeldPublication {
    fn new() -> Self {
        let (entered_tx, entered) = crossbeam::channel::bounded(1);
        let (release, released) = crossbeam::channel::bounded(1);
        fail::cfg_callback(
            "slice7::after_compaction_output_durable_before_manifest_publish",
            move || {
                entered_tx.send(()).unwrap();
                released.recv().unwrap();
            },
        )
        .unwrap();
        Self { entered, release }
    }
}
impl Drop for HeldPublication {
    fn drop(&mut self) {
        let _ = self.release.try_send(());
    }
}

#[test]
fn should_promote_existing_l0_identity_without_reading_or_writing_sst_bytes() -> MidgeResult<()> {
    let _guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    // Arrange
    let (mut fixture, name) = move_fixture()?;
    fixture.assert_rows(&[(b"key", b"value")])?;
    let original = std::fs::read(fixture.directory.path().join("sst").join(&name))?;
    // Act
    fixture
        .event_loop
        .launch_compaction(fixture.plan(&name, 0, 1))?;
    fixture.drain();
    // Assert: the original level-zero object name is now logically level one.
    // The refusing factory proves no compaction reader/writer was constructed.
    assert_eq!(current_level(&fixture, &name), 1);
    assert_eq!(
        std::fs::read(fixture.directory.path().join("sst").join(&name))?,
        original
    );
    assert!(fixture.event_loop.state.intent_log.is_empty());
    fixture.assert_rows(&[(b"key", b"value")])?;
    fixture.restart(&[(b"key", b"value")])?;
    scenario.teardown();
    Ok(())
}

#[test]
fn should_preserve_range_tombstone_visibility_after_identity_move() -> MidgeResult<()> {
    let _guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    // Arrange
    let mut fixture = ConcurrentFixture::new(16 * 1024 * 1024, false)?;
    let name = range_tombstone_file(&mut fixture)?;
    let factory = Arc::new(NoRewriteFactory(crate::io::subdirectory(
        Arc::clone(&fixture.event_loop.state.fs),
        "sst",
    )));
    let mut actor = crate::runtime::actors::CompactionActor::new_with_config(
        factory,
        crate::compaction::LeveledCompactionConfig::default(),
    );
    actor.set_execution_limits(64 * 1024, 16 * 1024 * 1024);
    actor.configure_concurrency(2)?;
    fixture.event_loop.actors.compaction_actor = actor;
    fixture.persist()?;
    fixture.assert_rows(&[])?;
    let source = fixture
        .event_loop
        .state
        .manifest
        .files
        .iter()
        .find(|file| file.name == name)
        .unwrap()
        .clone();
    assert!(!crate::metadata::manifest::validate_sst_move(
        &fixture.event_loop.state.manifest.files,
        &[source],
        1,
    )?);
    let path = fixture.directory.path().join("sst").join(&name);
    let original = std::fs::read(&path)?;

    // Act
    fixture
        .event_loop
        .launch_compaction(fixture.plan(&name, 0, 1))?;
    fixture.drain();

    // Assert
    assert_eq!(current_level(&fixture, &name), 1);
    assert_eq!(std::fs::read(path)?, original);
    fixture.assert_rows(&[])?;
    fixture.restart(&[])?;
    scenario.teardown();
    Ok(())
}

#[test]
fn should_retain_live_identity_when_logical_move_is_canceled_before_publication() -> MidgeResult<()>
{
    crate::failpoints::with_read_gate(|| {
        // Arrange: the real move worker returns existing immutable names.
        let (mut fixture, name) = move_fixture()?;
        let original = std::fs::read(fixture.directory.path().join("sst").join(&name))?;
        fixture
            .event_loop
            .launch_compaction(fixture.plan(&name, 0, 1))?;
        let completion = fixture
            .workers
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
        assert!(matches!(completion, RuntimeMsg::CompactionComplete {
            output_ssts, succeeded: true, ..
        } if output_ssts == [name.clone()]));
        assert_eq!(fixture.event_loop.actors.compaction_actor.active_count(), 1);

        // Act: shutdown owns the native ticket before any publication begins.
        fixture
            .event_loop
            .actors
            .compaction_actor
            .cancel_and_join_worker(&mut fixture.event_loop.state, None);

        // Assert: the canceled logical edit leaves its exact live SST intact.
        assert_eq!(current_level(&fixture, &name), 0);
        assert_eq!(fixture.event_loop.actors.compaction_actor.active_count(), 0);
        assert!(fixture.event_loop.state.intent_log.is_empty());
        assert_eq!(
            std::fs::read(fixture.directory.path().join("sst").join(&name))?,
            original
        );
        fixture.assert_rows(&[(b"key", b"value")])?;
        fixture.restart(&[(b"key", b"value")])
    })
}

#[test]
fn should_preserve_newest_l0_visibility_when_hot_sources_overlap_cold_predecessors(
) -> MidgeResult<()> {
    let _guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    // Arrange: the newest file is hot and the target is empty, but both
    // overlapping L0 sources must be selected and rewritten together.
    let mut fixture = ConcurrentFixture::new(16 * 1024 * 1024, false)?;
    fixture
        .event_loop
        .actors
        .compaction_actor
        .set_execution_limits(64 * 1024, 16 * 1024 * 1024);
    let older = fixture.file(0, 1, b"key", b"v1", 1)?;
    let newer = fixture.file(0, 2, b"key", b"v2", 2)?;
    fixture
        .event_loop
        .state
        .manifest
        .files
        .iter()
        .find(|file| file.name == newer)
        .unwrap()
        .read_count
        .store(100, Ordering::Relaxed);
    fixture.persist()?;
    fixture.assert_rows(&[(b"key", b"v2")])?;
    let plan = fixture
        .event_loop
        .actors
        .compaction_actor
        .check_manual_compaction(&fixture.event_loop.state)?
        .expect("legal L0 source plan");
    assert!(plan.source_files.contains(&older));
    assert!(plan.source_files.contains(&newer));
    // Act
    fixture.event_loop.launch_compaction(plan)?;
    fixture.drain();
    // Assert: neither first-hit point reads nor scans can see v1 above v2.
    assert!(!fixture
        .event_loop
        .state
        .manifest
        .files
        .iter()
        .any(|file| file.name == newer));
    fixture.assert_rows(&[(b"key", b"v2")])?;
    fixture.restart(&[(b"key", b"v2")])?;
    scenario.teardown();
    Ok(())
}

#[test]
fn should_retain_live_objects_when_move_publication_acquires_conflicting_target_coverage(
) -> MidgeResult<()> {
    let _guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    // Arrange: hold the actual publication worker after its durable intent.
    let (mut fixture, source) = move_fixture()?;
    let held = HeldPublication::new();
    fixture
        .event_loop
        .launch_compaction(fixture.plan(&source, 0, 1))?;
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        fixture.progress();
        if held.entered.try_recv().is_ok() {
            break;
        }
        assert!(Instant::now() < deadline, "move publication entered");
        std::thread::sleep(Duration::from_millis(1));
    }
    // Act: simulate another committed edit making the captured target stale.
    let target = fixture.file(1, 2, b"key", b"older", 0)?;
    fixture.persist()?;
    held.release.send(()).unwrap();
    fixture.drain();
    fail::remove("slice7::after_compaction_output_durable_before_manifest_publish");
    // Assert: failed publication retains both live objects and rejects the
    // ambiguous restart without manufacturing a physical deletion obligation.
    assert!(fixture.event_loop.authority.compaction_fence.is_degraded());
    assert_eq!(fixture.event_loop.state.manifest.files.len(), 2);
    assert!(fixture.event_loop.state.manifest.retired_ssts.is_empty());
    let ConcurrentFixture {
        event_loop,
        guard,
        lease,
        directory,
        ..
    } = fixture;
    drop(event_loop);
    guard.release();
    lease.release()?;
    assert!(crate::Engine::open(crate::OpenOptions::local(directory.path()).build()?).is_err());
    for name in [source, target] {
        assert!(directory.path().join("sst").join(name).exists());
    }
    scenario.teardown();
    Ok(())
}

#[test]
fn should_recover_exact_original_identity_on_each_side_of_the_logical_move_edit() -> MidgeResult<()>
{
    let _guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    for (point, level) in [
        ("midge::manifest::inject_no_space_on_move_edit", 0),
        ("midge::compaction::inject_failure_after_manifest_batch", 1),
    ] {
        // Arrange: fail just before the edit, then just after its durable append.
        let (mut fixture, name) = move_fixture()?;
        fail::cfg(point, "return").unwrap();
        // Act
        fixture
            .event_loop
            .launch_compaction(fixture.plan(&name, 0, 1))?;
        fixture.drain();
        fail::remove(point);
        // Assert: neither failure can turn the reused live name into an orphan
        // or retired input. Startup replays the journal before the move intent.
        assert_eq!(current_level(&fixture, &name), level);
        assert!(fixture.event_loop.authority.compaction_fence.is_degraded());
        let recovered = crate::metadata::ManifestPersistence::load_with_fs_and_policy(
            &fixture.event_loop.state.fs,
            crate::config::RecoveryPolicy::Strict,
        )
        .map_err(MidgeError::Internal)?;
        assert_eq!(recovered.files.len(), 1);
        assert_eq!(recovered.files[0].name, name);
        assert_eq!(recovered.files[0].level, level);
        assert!(recovered.retired_ssts.is_empty());
        fixture.restart(&[(b"key", b"value")])?;
    }
    scenario.teardown();
    Ok(())
}
