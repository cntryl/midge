//! Cloud-backed metadata-only compaction moves.

use super::*;
use crate::common::resource_budget::ResourceBudget;
use crate::sst::traits::{DynSstWriter, SstReaderExt};
use std::path::Path;

struct NoSstIoFactory(Arc<dyn crate::io::Fs>);

impl crate::sst::SstFactory for NoSstIoFactory {
    fn output_fs(&self) -> Arc<dyn crate::io::Fs> {
        Arc::clone(&self.0)
    }

    fn compaction_scratch_cleanup_verified(&self) -> bool {
        true
    }

    fn create(&self) -> MidgeResult<Box<dyn DynSstWriter>> {
        panic!("logical move created an SST writer")
    }

    fn create_for_compaction(&self, _: ResourceBudget) -> MidgeResult<Box<dyn DynSstWriter>> {
        panic!("logical move created a compaction SST writer")
    }

    fn open(&self, _: &Path) -> MidgeResult<Box<dyn SstReaderExt>> {
        panic!("logical move opened an SST reader")
    }

    fn open_for_compaction(
        &self,
        _: &Path,
        _: ResourceBudget,
    ) -> MidgeResult<Box<dyn SstReaderExt>> {
        panic!("logical move opened a compaction SST reader")
    }
}

#[test]
fn should_preserve_cloud_sst_identity_without_upload_when_level_move_publishes() -> MidgeResult<()>
{
    // Arrange
    let (mut el, worker, _) = cloud_debt(1)?;
    el.state.set_compaction_enabled(false);
    el.state
        .manifest
        .test_mut()
        .files
        .retain(|file| file.cf_id == 0);
    let input = el.state.manifest.files[0].name.clone();
    let remote_path = remote_sst_path_for_test(&el, &input);
    let original_bytes = std::fs::read(&remote_path)?;
    let (cloud, _release, _reached) = controlled_storage::install(&mut el, false, false)?;
    let factory = Arc::new(NoSstIoFactory(crate::io::subdirectory(
        Arc::clone(&el.state.fs),
        "sst",
    )));
    let mut actor = crate::runtime::actors::CompactionActor::new(factory);
    actor.configure_concurrency(2)?;
    el.actors.compaction_actor = actor;
    let mut plan = crate::compaction::CompactionPlan::new(0, 0, 1);
    plan.input_files.push(input.clone());
    plan.source_files.push(input.clone());

    // Act
    el.launch_compaction(plan)?;
    complete_compaction(&mut el, &worker);
    settle_owned_work(&mut el, &worker);

    // Assert
    assert_eq!(el.state.manifest.files.len(), 1);
    assert_eq!(el.state.manifest.files[0].name, input);
    assert_eq!(el.state.manifest.files[0].level, 1);
    assert_eq!(std::fs::read(remote_path)?, original_bytes);
    assert!(cloud.uploads.lock().unwrap().is_empty());
    assert_eq!(
        el.actors
            .cloud_coordinator
            .hybrid_storage
            .as_ref()
            .unwrap()
            .budget_snapshot()
            .usage
            .reservations,
        0,
        "logical move must not reserve replacement SST storage"
    );
    Ok(())
}
