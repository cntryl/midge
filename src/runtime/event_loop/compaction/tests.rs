use super::*;
use crate::common::{MidgeError, MidgeResult};
use std::sync::Arc;

const LOCAL_BUDGET: u64 = 1_000;
const STAGING_BYTES: u64 = 500;

struct UnknownScratchFactory;

impl crate::sst::SstFactory for UnknownScratchFactory {
    fn create(&self) -> MidgeResult<Box<dyn crate::sst::traits::DynSstWriter>> {
        unreachable!("completion fixture already ran its worker")
    }
    fn open(
        &self,
        _path: &std::path::Path,
    ) -> MidgeResult<Box<dyn crate::sst::traits::SstReaderExt>> {
        unreachable!("completion fixture does not read inputs")
    }
}

#[test]
fn should_retain_failed_compaction_admission_when_anonymous_scratch_cleanup_is_unproven(
) -> MidgeResult<()> {
    for error in [
        MidgeError::NoSpace("partial final write".into()),
        MidgeError::Aborted("canceled after scratch write".into()),
        MidgeError::ResourceLimit("metadata limit after scratch write".into()),
        MidgeError::Corruption("late input corruption".into()),
    ] {
        // Arrange
        let directory = tempfile::tempdir()?;
        let (mut event_loop, hybrid) = event_loop_with_storage(&directory, true)?;
        event_loop.compaction_actor = crate::runtime::actors::compaction::CompactionActor::new(
            Arc::new(UnknownScratchFactory),
        );
        event_loop
            .compaction_actor
            .prepare_for_completion_with_storage_test(&mut event_loop.state, &[], Some(&hybrid))?;
        event_loop.compaction_actor.set_worker_error_for_test(error);
        // Act
        CompactionCoordinator::complete(
            &mut event_loop,
            CompactionCompleteRequest {
                request_id: 7603,
                input_ssts: Vec::new(),
                output_ssts: Vec::new(),
                cf_id: 0,
                target_level: 1,
                succeeded: false,
            },
        );
        // Assert
        assert_eq!(
            hybrid.budget_snapshot().total_committed_bytes,
            STAGING_BYTES
        );
    }
    Ok(())
}

fn event_loop_with_storage(
    directory: &tempfile::TempDir,
    ephemeral: bool,
) -> MidgeResult<(EventLoop, Arc<crate::storage::HybridStorage>)> {
    let state = crate::runtime::RuntimeState::new(directory.path().to_path_buf(), false);
    let local = Arc::new(crate::storage::filesystem::FileSystem::new(
        directory.path().join("hybrid_local"),
    )?);
    let cloud = Arc::new(crate::storage::filesystem::FileSystem::new(
        directory.path().join("cloud_store"),
    )?);
    let hybrid = Arc::new(crate::storage::HybridStorage::with_policy(
        local,
        cloud,
        crate::storage::hybrid::policy::StorageBudgetPolicy::new(LOCAL_BUDGET),
    ));
    if ephemeral {
        hybrid.enable_ephemeral_sst_cache(LOCAL_BUDGET);
    }
    let event_loop = EventLoop::new(
        state,
        false,
        Arc::new(crate::runtime::ResponseRouter::new()),
        crate::runtime::RuntimeConfig {
            hybrid_storage: Some(Arc::clone(&hybrid)),
            ..Default::default()
        },
        None,
    )?;
    Ok((event_loop, hybrid))
}

fn published_output(event_loop: &mut EventLoop) -> String {
    let output_name = crate::sst::compaction_file_name(0, 1, 42, 0);
    let metadata = crate::metadata::FileMeta {
        name: output_name.clone(),
        level: 1,
        cf_id: 0,
        size_bytes: 300,
        ..Default::default()
    };
    event_loop.state.manifest.files.push(metadata.clone());
    event_loop
        .state
        .intent_log
        .push(crate::runtime::IntentLogEntry::CompactionPublish {
            phase: crate::runtime::PublicationPhase::ManifestPublished,
            cf_id: 0,
            removed: Vec::new(),
            added: vec![crate::runtime::FileMeta {
                name: metadata.name,
                level: metadata.level,
                cf_id: metadata.cf_id,
                size_bytes: metadata.size_bytes,
                content_crc32c: None,
                smallest_key: None,
                largest_key: None,
                smallest_seq: None,
                largest_seq: None,
                key_bounds_complete: false,
            }],
        });
    output_name
}

fn settle_publication(
    event_loop: &mut EventLoop,
    output: &str,
    reservation: crate::storage::hybrid::actor::StorageReservationToken,
    complete: bool,
) {
    let outputs = vec![output.to_string()];
    if complete {
        CompactionCoordinator::finalize_published_compaction(
            event_loop,
            7_601,
            &[],
            &outputs,
            Some(reservation),
        );
    } else {
        CompactionCoordinator::settle_incomplete_authoritative_publication(
            event_loop,
            &[],
            &outputs,
            Some(reservation),
        );
    }
}

#[test]
fn should_settle_async_compaction_failure_according_to_owned_scratch() -> MidgeResult<()> {
    for ephemeral in [false, true] {
        for io_failure in [false, true] {
            // Arrange
            let directory = tempfile::tempdir()?;
            let (mut event_loop, hybrid) = event_loop_with_storage(&directory, ephemeral)?;
            let input = crate::sst::file_name(0, 0, 1);
            event_loop
                .state
                .manifest
                .files
                .push(crate::metadata::FileMeta {
                    name: input.clone(),
                    size_bytes: 100,
                    ..Default::default()
                });
            hybrid.reconcile_local_disk_usage(100, 0);
            event_loop
                .compaction_actor
                .prepare_for_completion_with_storage_test(
                    &mut event_loop.state,
                    std::slice::from_ref(&input),
                    Some(&hybrid),
                )?;
            let error = if io_failure {
                MidgeError::Io(std::io::Error::other("failed local writer"))
            } else {
                MidgeError::Aborted("compaction canceled".into())
            };
            event_loop.compaction_actor.set_worker_error_for_test(error);

            // Act
            CompactionCoordinator::complete(
                &mut event_loop,
                CompactionCompleteRequest {
                    request_id: 7_602,
                    input_ssts: vec![input.clone()],
                    output_ssts: Vec::new(),
                    cf_id: 0,
                    target_level: 1,
                    succeeded: false,
                },
            );

            // Assert
            // This injected worker failure created no scratch. The production
            // factory can prove that independently of the returned error type.
            let retained = 0;
            assert_eq!(
                hybrid.budget_snapshot().total_committed_bytes,
                100 + retained,
                "ephemeral={ephemeral}, io_failure={io_failure}"
            );
            assert!(event_loop.state.manifest_has_file(&input));
            assert_eq!(
                event_loop
                    .state
                    .active_compactions
                    .load(std::sync::atomic::Ordering::SeqCst),
                0
            );
        }
    }
    Ok(())
}

#[cfg(unix)]
#[test]
fn should_keep_compaction_allowance_when_published_output_metadata_fails() -> MidgeResult<()> {
    for complete in [false, true] {
        // Arrange
        let directory = tempfile::tempdir()?;
        let (mut event_loop, hybrid) = event_loop_with_storage(&directory, true)?;
        let output = published_output(&mut event_loop);
        let path = event_loop.state.sst_dir.join(&output);
        std::os::unix::fs::symlink(&output, &path)?;
        assert!(std::fs::metadata(&path).is_err());
        let reservation = hybrid
            .reserve_compaction_staging_with_token(STAGING_BYTES)
            .expect("reserve compaction window");

        // Act
        settle_publication(&mut event_loop, &output, reservation, complete);

        // Assert
        assert_eq!(
            hybrid.budget_snapshot().total_committed_bytes,
            STAGING_BYTES,
            "complete={complete}"
        );
        assert!(event_loop.state.manifest_has_file(&output));
        assert!(!event_loop.state.intent_log.is_empty());
    }
    Ok(())
}

#[test]
fn should_keep_compaction_allowance_when_published_output_is_not_a_file() -> MidgeResult<()> {
    for complete in [false, true] {
        // Arrange
        let directory = tempfile::tempdir()?;
        let (mut event_loop, hybrid) = event_loop_with_storage(&directory, true)?;
        let output = published_output(&mut event_loop);
        std::fs::create_dir(event_loop.state.sst_dir.join(&output))?;
        let reservation = hybrid
            .reserve_compaction_staging_with_token(STAGING_BYTES)
            .expect("reserve compaction window");

        // Act
        settle_publication(&mut event_loop, &output, reservation, complete);

        // Assert
        assert_eq!(
            hybrid.budget_snapshot().total_committed_bytes,
            STAGING_BYTES,
            "complete={complete}"
        );
        assert!(!event_loop.state.intent_log.is_empty());
    }
    Ok(())
}

#[test]
fn should_release_compaction_allowance_when_published_output_is_remote_only() -> MidgeResult<()> {
    for complete in [false, true] {
        // Arrange
        let directory = tempfile::tempdir()?;
        let (mut event_loop, hybrid) = event_loop_with_storage(&directory, true)?;
        let output = published_output(&mut event_loop);
        let reservation = hybrid
            .reserve_compaction_staging_with_token(STAGING_BYTES)
            .expect("reserve compaction window");

        // Act
        settle_publication(&mut event_loop, &output, reservation, complete);

        // Assert
        assert_eq!(
            hybrid.budget_snapshot().total_committed_bytes,
            0,
            "complete={complete}"
        );
        assert_eq!(event_loop.state.intent_log.is_empty(), complete);
        assert!(event_loop.state.manifest_has_file(&output));
    }
    Ok(())
}

#[test]
fn should_account_resident_outputs_when_compaction_publication_retains_inputs() -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let (mut event_loop, hybrid) = event_loop_with_storage(&directory, true)?;
    let output = published_output(&mut event_loop);
    std::fs::write(event_loop.state.sst_dir.join(&output), [0_u8; 300])?;
    hybrid.reconcile_local_disk_usage(100, 0);
    let reservation = hybrid
        .reserve_compaction_staging_with_token(STAGING_BYTES)
        .expect("reserve compaction window");

    // Act
    settle_publication(&mut event_loop, &output, reservation, false);

    // Assert
    assert_eq!(hybrid.budget_snapshot().total_committed_bytes, 400);
    assert!(!event_loop.state.intent_log.is_empty());
    Ok(())
}
