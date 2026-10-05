//! Preserve actual input failures and healthy callerless repair behavior.

use super::*;
use crate::common::resource_budget::ResourceBudget;
use crate::sst::SstFactory;
use std::time::Duration;

struct SeededInputs {
    _directory: tempfile::TempDir,
    state: RuntimeState,
    factory: Arc<crate::sst::FsSstFactoryIo>,
    plan: crate::compaction::CompactionPlan,
    originals: Vec<(String, Vec<u8>)>,
}

impl SeededInputs {
    fn new(count: u32, source_level: u32, target_level: u32) -> MidgeResult<Self> {
        let directory = tempfile::tempdir()?;
        let mut state = RuntimeState::new(directory.path().to_path_buf(), false);
        let factory = Arc::new(crate::sst::FsSstFactoryIo::new(
            Arc::new(crate::io::RealFs::new(&state.sst_dir).map_err(FsError::into_midge)?),
            4096,
        ));
        let mut plan = crate::compaction::CompactionPlan::new(0, source_level, target_level)
            .with_output_seq(3);
        let mut originals = Vec::new();
        for index in 0..count {
            let name = format!("input-{index}.sst");
            let key = format!("key-{index}");
            let value = format!("value-{index}");
            let path = state.sst_dir.join(&name);
            let mut writer = factory.create()?;
            writer.add_with_meta(
                key.as_bytes(),
                Some(value.as_bytes()),
                10,
                EntryType::Put,
                None,
            )?;
            crate::sst::fs::finish_writer_to_path(writer, &path)?;
            assert!(
                matches!(factory.open(std::path::Path::new(&name))?.get_state_at(key.as_bytes(), u64::MAX)?,
                crate::types::KeyState::Value(bytes, 10, None, EntryType::Put)
                if bytes.as_ref() == value.as_bytes())
            );
            let bytes = std::fs::read(path)?;
            state
                .manifest
                .test_mut()
                .files
                .push(crate::metadata::FileMeta {
                    name: name.clone(),
                    cf_id: 0,
                    level: source_level,
                    size_bytes: bytes.len() as u64,
                    content_crc32c: Some(crc32c::crc32c(&bytes)),
                    ..Default::default()
                });
            plan.add_test_source(name.clone());
            originals.push((name, bytes));
        }
        Ok(Self {
            _directory: directory,
            state,
            factory,
            plan,
            originals,
        })
    }

    fn corrupt_first_trailer(&mut self) -> MidgeResult<()> {
        let (name, bytes) = &mut self.originals[0];
        // The first actual writer block begins at offset zero with its u32
        // payload length. Alter only the stored CRC at that block's end.
        let payload_len = u32::from_le_bytes(bytes[..4].try_into().expect("real block prefix"));
        let trailer = 4 + usize::try_from(payload_len).expect("small real block") - 1;
        assert!(
            payload_len >= 4 && trailer < bytes.len(),
            "actual first block contains its CRC"
        );
        bytes[trailer] ^= 0xff;
        std::fs::write(self.state.sst_dir.join(name), bytes.as_slice())?;
        self.state.manifest.test_mut().files[0].content_crc32c = Some(crc32c::crc32c(bytes));
        Ok(())
    }

    fn assert_inputs_retained(&self) -> MidgeResult<()> {
        for (name, bytes) in &self.originals {
            assert_eq!(std::fs::read(self.state.sst_dir.join(name))?, *bytes);
        }
        assert_eq!(self.state.active_compactions.load(Ordering::Acquire), 0);
        Ok(())
    }
}

struct HeldCorruptionFactory {
    delegate: Arc<crate::sst::FsSstFactoryIo>,
    deadline: OperationDeadline,
    observed_while_live: AtomicBool,
}

impl SstFactory for HeldCorruptionFactory {
    fn output_fs(&self) -> Arc<dyn crate::io::Fs> {
        self.delegate.output_fs()
    }

    fn compaction_scratch_cleanup_verified(&self) -> bool {
        self.delegate.compaction_scratch_cleanup_verified()
    }

    fn create(&self) -> MidgeResult<Box<dyn crate::sst::traits::DynSstWriter>> {
        self.delegate.create()
    }

    fn create_for_compaction(
        &self,
        budget: ResourceBudget,
    ) -> MidgeResult<Box<dyn crate::sst::traits::DynSstWriter>> {
        self.delegate.create_for_compaction(budget)
    }

    fn open(
        &self,
        path: &std::path::Path,
    ) -> MidgeResult<Box<dyn crate::sst::traits::SstReaderExt>> {
        self.delegate.open(path)
    }

    fn open_for_compaction(
        &self,
        path: &std::path::Path,
        budget: ResourceBudget,
    ) -> MidgeResult<Box<dyn crate::sst::traits::SstReaderExt>> {
        let reader = self.delegate.open_for_compaction(path, budget)?;
        let error = reader
            .get_state_at(b"key-0", u64::MAX)
            .expect_err("actual altered data trailer must fail CRC verification");
        self.observed_while_live.store(
            matches!(error, MidgeError::Corruption(_)) && !self.deadline.is_expired(),
            Ordering::Release,
        );
        while !self.deadline.is_expired() {
            std::thread::sleep(Duration::from_millis(5));
        }
        Err(error)
    }
}

fn held_corruption_case(asynchronous: bool) -> MidgeResult<()> {
    let mut fixture = SeededInputs::new(2, 0, 1)?;
    fixture.corrupt_first_trailer()?;
    let deadline = OperationDeadline::from_budget(Duration::from_secs(2));
    let factory = Arc::new(HeldCorruptionFactory {
        delegate: fixture.factory.clone(),
        deadline,
        observed_while_live: AtomicBool::new(false),
    });
    let mut actor = CompactionActor::new(factory.clone());
    let (tx, rx) = crossbeam::channel::unbounded();
    let result = actor.run_compaction(
        &mut fixture.state,
        &fixture.plan,
        None,
        asynchronous.then_some(tx),
        Some(deadline),
    );
    let error = if asynchronous {
        result?;
        let receipt = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("actual failed compute receipt");
        actor.join_completed_worker();
        assert!(matches!(
            receipt,
            RuntimeMsg::CompactionComplete {
                succeeded: false,
                ..
            }
        ));
        actor.take_worker_error().expect("real worker failure")
    } else {
        result.expect_err("actual corrupt input must fail compaction")
    };
    actor.cancel_and_join_worker(&mut fixture.state, None);
    fixture.assert_inputs_retained()?;
    assert!(
        factory.observed_while_live.load(Ordering::Acquire),
        "the genuine CRC failure was observed before the original deadline"
    );
    assert!(deadline.is_expired());
    assert!(
        matches!(error, MidgeError::Corruption(_)),
        "actual held error: {error:?}"
    );
    Ok(())
}

#[test]
fn should_preserve_actual_corruption_when_completed_input_error_returns_after_deadline(
) -> MidgeResult<()> {
    // Arrange: seed valid SSTs, then alter only a genuine data-block trailer.
    crate::failpoints::with_read_gate(|| {
        for asynchronous in [false, true] {
            // Act: hold the real CRC failure across the original captured budget.
            held_corruption_case(asynchronous)?;
            // Assert: the case joins ownership and checks Corruption/exact inputs.
        }
        Ok(())
    })
}

fn healthy_repair_case() -> MidgeResult<()> {
    let mut fixture = SeededInputs::new(3, 1, 1)?;
    let config = LeveledCompactionConfig {
        max_compaction_input_files: 2,
        ..Default::default()
    };
    let mut actor = CompactionActor::new_with_config(fixture.factory.clone(), config);
    let outputs = actor.run_compaction(&mut fixture.state, &fixture.plan, None, None, None)?;
    let origin = actor.manual_deadline_for_generation(0, 1, 3)?;
    let mut rows = Vec::new();
    for name in &outputs {
        rows.extend(
            fixture
                .factory
                .open(std::path::Path::new(name))?
                .scan_range_state(None, None)?,
        );
    }
    actor.cancel_and_join_worker(&mut fixture.state, None);
    fixture.assert_inputs_retained()?;
    assert!(
        origin.is_none(),
        "callerless repair retains its original None owner"
    );
    assert_ne!(outputs, [] as [String; 0]);
    assert_eq!(rows.len(), 3);
    for (index, (key, state)) in rows.into_iter().enumerate() {
        assert_eq!(key.as_ref(), format!("key-{index}").as_bytes());
        assert!(
            matches!(state, crate::types::KeyState::Value(value, 10, None, EntryType::Put)
            if value.as_ref() == format!("value-{index}").as_bytes())
        );
    }
    let scratch = fixture.state.sst_dir.join(".compaction-repair");
    assert!(
        scratch.exists(),
        "three inputs and fan-in two exercise real scratch repair"
    );
    assert_eq!(std::fs::read_dir(scratch)?.count(), 0);
    Ok(())
}

#[test]
fn should_keep_exact_rows_when_healthy_background_repair_uses_no_manual_deadline() -> MidgeResult<()>
{
    // Arrange: three genuine L1 inputs require a real fan-in-two scratch pass.
    // Act: run the actual actor with its callerless None owner.
    // Assert: the case verifies every row, retained inputs and empty scratch.
    crate::failpoints::with_read_gate(healthy_repair_case)
}
