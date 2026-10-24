use super::*;
use crate::common::{MidgeResult, OperationDeadline};
use crate::lease::{CloudMetadataGeneration, CloudMetadataHead};
use crate::runtime::event_loop::tests::create_test_cloud_event_loop;
use std::time::Duration;

fn generation(event_loop: &EventLoop) -> CloudMetadataGeneration {
    let CloudMetadataHead::Committed(generation) = event_loop
        .authority
        .fencing
        .leader_store
        .as_ref()
        .unwrap()
        .read_committed_metadata(Duration::from_secs(5))
        .unwrap()
    else {
        panic!("reservation must be mirrored")
    };
    generation
}

#[test]
fn should_prewarm_first_flush_block_before_output_needs_its_name() -> MidgeResult<()> {
    // Arrange
    let mut event_loop = create_test_cloud_event_loop(Default::default())?;

    // Act: maintenance publishes a block while no output owns the control turn.
    event_loop.prefetch_flush_sst_names_if_due();
    let before = generation(&event_loop);
    let journal_before = event_loop
        .state
        .fs
        .metadata(&crate::io::FsPath::new(crate::metadata::files::JOURNAL))
        .map_err(crate::io::FsError::into_midge)?
        .len;
    let assigned = event_loop.reserve_flush_sst_seq(0)?;

    // Assert: output launch consumes the admitted block without metadata I/O.
    assert_eq!(assigned, 1);
    assert_eq!(event_loop.state.sst_names.cursor[&0], 2);
    assert_eq!(event_loop.state.sst_names.reserved_through[&0], 17);
    assert_eq!(generation(&event_loop), before);
    assert_eq!(
        event_loop
            .state
            .fs
            .metadata(&crate::io::FsPath::new(crate::metadata::files::JOURNAL))
            .map_err(crate::io::FsError::into_midge)?
            .len,
        journal_before
    );
    let cloud = event_loop
        .actors
        .cloud_coordinator
        .cloud_metadata_storage
        .as_ref()
        .unwrap();
    let bodies = crate::runtime::hybrid_persistence::read_committed_metadata_bodies_within(
        cloud,
        &before,
        &OperationDeadline::from_budget(Duration::from_secs(5)),
    )?;
    let durable = crate::runtime::hybrid_persistence::decode_generation_manifest(
        &before,
        &bodies,
        &OperationDeadline::from_budget(Duration::from_secs(5)),
    )?;
    assert_eq!(durable.next_sst_seqs[&0], 17);
    Ok(())
}

#[test]
fn should_reserve_next_flush_block_when_eight_names_remain() -> MidgeResult<()> {
    // Arrange
    let mut event_loop = create_test_cloud_event_loop(Default::default())?;
    event_loop.prefetch_flush_sst_names_if_due();
    for expected in 1..=8 {
        assert_eq!(event_loop.reserve_flush_sst_seq(0)?, expected);
    }
    let first = generation(&event_loop);

    // Act
    event_loop.prefetch_flush_sst_names_if_due();
    let prefetched = generation(&event_loop);
    let assigned = event_loop.reserve_flush_sst_seq(0)?;

    // Assert: prefetch extends the bound and preserves every unconsumed name.
    assert_eq!(assigned, 9);
    assert_eq!(event_loop.state.sst_names.reserved_through[&0], 33);
    assert_eq!(prefetched.id, first.id.checked_next().unwrap());
    assert_eq!(generation(&event_loop), prefetched);
    assert_eq!(event_loop.state.sst_names.cursor[&0], 10);
    Ok(())
}
