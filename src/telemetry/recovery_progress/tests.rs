//! Cargo regressions driven by actual planner/replay, HEAD inventory and
//! journal recovery work. No custom watchdog executable or clock is involved.

use crate::common::MidgeResult;
use crate::runtime::cloud_startup::recovery_progress_fixture::{
    prepare_recovery_progress_fixture, PreparedRecoveryProgressFixture,
    RecoveryProgressFixtureMode, RecoveryProgressFixtureResult,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::layer::{Layer, SubscriberExt};

#[path = "../../../benches/bench_support/stress_scenarios/recovery_progress.rs"]
mod workload_listener;

// Keep the production ProgressHandle adapter type-checked while the same
// scope/visitor is exercised below with an observable completion callback.
const _: fn(
    &cntryl_stress::ProgressHandle,
    &Arc<AtomicU64>,
    &str,
) -> workload_listener::RecoveryScope = workload_listener::RecoveryScope::enter;

fn with_listener<T>(operation: impl FnOnce() -> T) -> T {
    let subscriber = tracing_subscriber::registry()
        .with(workload_listener::RecoveryProgressLayer.with_filter(
            tracing_subscriber::filter::filter_fn(workload_listener::is_recovery_work),
        ))
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(std::io::sink)
                .with_filter(tracing_subscriber::EnvFilter::new("off")),
        );
    tracing::subscriber::with_default(subscriber, operation)
}

fn callback(completed: &Arc<AtomicU64>) -> Arc<dyn Fn() + Send + Sync> {
    let completed = Arc::clone(completed);
    Arc::new(move || {
        completed.fetch_add(1, Ordering::AcqRel);
    })
}

fn prepared(
    root: &std::path::Path,
    mode: RecoveryProgressFixtureMode,
) -> MidgeResult<PreparedRecoveryProgressFixture> {
    let fixture = prepare_recovery_progress_fixture(root, mode, Duration::ZERO)?;
    assert_eq!(fixture.mode(), mode);
    let _setup_elapsed = fixture.setup_elapsed();
    Ok(fixture)
}

fn verify_wal(result: &RecoveryProgressFixtureResult) {
    assert_eq!(result.mismatches, 0);
    assert_eq!(result.verified_records, result.expected_records);
    assert!(result.verified_records > 0);
    assert!(result.max_sequence >= result.verified_records);
    assert_eq!(result.max_epoch, 7);
    assert!(result.completed_range_reads >= 16);
    assert!(result.replay_completed_range_reads > 0);
    assert!(result.completed_range_bytes >= 1024 * 1024);
    assert!(result.maximum_range_bytes <= 64 * 1024);
    assert_eq!(result.local_wal_bytes, 0);
    assert_eq!(result.staged_wal_count, 0);
}

#[test]
fn should_report_completed_work_when_actual_remote_wal_planner_and_replay_recover_exact_rows(
) -> MidgeResult<()> {
    // Arrange
    let root = tempfile::tempdir()?;
    let fixture = prepared(root.path(), RecoveryProgressFixtureMode::DelayedRanges)?;
    let completed = Arc::new(AtomicU64::new(0));
    let phase = Arc::new(AtomicU64::new(1));

    // Act: all progress comes from the actual remote planner/replay path.
    let result = with_listener(|| {
        let _scope = workload_listener::RecoveryScope::enter_with_completion(
            callback(&completed),
            &phase,
            "recovery",
        );
        fixture.run()
    })?;

    // Assert
    verify_wal(&result);
    assert!(completed.load(Ordering::Acquire) > 0);
    Ok(())
}

#[test]
fn should_report_completed_work_when_actual_inventory_validates_authoritative_head_sizes(
) -> MidgeResult<()> {
    // Arrange
    let root = tempfile::tempdir()?;
    let fixture = prepared(root.path(), RecoveryProgressFixtureMode::MetadataInventory)?;
    let completed = Arc::new(AtomicU64::new(0));
    let phase = Arc::new(AtomicU64::new(1));

    // Act
    let result = with_listener(|| {
        let _scope = workload_listener::RecoveryScope::enter_with_completion(
            callback(&completed),
            &phase,
            "recovery",
        );
        fixture.run()
    })?;

    // Assert: real HEAD callbacks and exact manifest retention are required.
    assert_eq!(result.mismatches, 0);
    assert_eq!(
        result.retained_inventory_entries,
        result.expected_inventory_entries
    );
    assert_eq!(
        result.completed_inventory_heads,
        result.expected_inventory_entries
    );
    assert_eq!(
        result.completed_inventory_size_validations,
        result.expected_inventory_entries
    );
    assert!(result.expected_inventory_entries > 0);
    assert_eq!(result.local_wal_bytes, 0);
    assert_eq!(result.staged_wal_count, 0);
    assert!(completed.load(Ordering::Acquire) > 0);
    Ok(())
}

#[test]
fn should_credit_no_work_when_actual_first_recovery_io_remains_held_until_timeout(
) -> MidgeResult<()> {
    // Arrange: both actual first range-read and first authoritative HEAD
    // remain owned by the fixture transport until their finite timeout.
    for mode in [
        RecoveryProgressFixtureMode::HeldFirstRange,
        RecoveryProgressFixtureMode::HeldInventory,
    ] {
        let root = tempfile::tempdir()?;
        let fixture = prepared(root.path(), mode)?;
        let completed = Arc::new(AtomicU64::new(0));
        let phase = Arc::new(AtomicU64::new(1));

        // Act
        let result = with_listener(|| {
            let _scope = workload_listener::RecoveryScope::enter_with_completion(
                callback(&completed),
                &phase,
                "recovery",
            );
            fixture.run()
        });

        // Assert: independent transport observations cannot claim a response
        // or size validation, and a pending request cannot create progress.
        assert!(
            result.is_err(),
            "held first I/O unexpectedly recovered: {result:?}"
        );
        let observed: serde_json::Value = serde_json::from_slice(&std::fs::read(
            root.path().join("fixture-observations.json"),
        )?)
        .expect("decode independent recovery observations");
        assert_eq!(observed["held_requests"], 1);
        assert_eq!(observed["completed_range_reads"], 0);
        assert_eq!(observed["completed_inventory_heads"], 0);
        assert_eq!(observed["completed_inventory_size_validations"], 0);
        assert_eq!(completed.load(Ordering::Acquire), 0);
    }
    Ok(())
}

#[test]
fn should_report_completed_work_when_actual_journal_replay_restores_exact_edit_frontiers(
) -> MidgeResult<()> {
    // Arrange
    let root = tempfile::tempdir()?;
    let completed = Arc::new(AtomicU64::new(0));
    let phase = Arc::new(AtomicU64::new(1));

    // Act
    let result = with_listener(|| {
        let _scope = workload_listener::RecoveryScope::enter_with_completion(
            callback(&completed),
            &phase,
            "recovery",
        );
        crate::metadata::recovery_progress_fixture::run_journal_recovery_progress_fixture(
            root.path(),
        )
    })?;

    // Assert
    assert_eq!(result.mismatches, 0);
    assert_eq!(result.verified_edits, result.expected_edits);
    assert_eq!(result.manifest_edit_checkpoint_id, result.max_edit_id);
    assert_eq!(result.restored_cf_count, result.expected_edits);
    assert_eq!(result.restored_frontier_count, result.expected_edits + 1);
    assert!(result.completed_local_reads > 0);
    assert!(result.completed_local_read_bytes >= result.journal_bytes);
    assert_eq!(result.local_wal_bytes, 0);
    assert_eq!(result.staged_wal_count, 0);
    assert!(completed.load(Ordering::Acquire) > 0);
    Ok(())
}

#[test]
fn should_isolate_actual_recovery_work_from_background_threads_old_phases_and_ended_scopes(
) -> MidgeResult<()> {
    // Arrange
    let roots = (0..6)
        .map(|_| tempfile::tempdir())
        .collect::<Result<Vec<_>, _>>()?;
    let mut fixtures = roots
        .iter()
        .map(|root| prepared(root.path(), RecoveryProgressFixtureMode::DelayedRanges))
        .collect::<MidgeResult<Vec<_>>>()?;
    let completed = Arc::new(AtomicU64::new(0));
    let phase = Arc::new(AtomicU64::new(1));

    with_listener(|| -> MidgeResult<()> {
        // Act / Assert: sharing a subscriber does not transfer caller scope.
        let scope = workload_listener::RecoveryScope::enter_with_completion(
            callback(&completed),
            &phase,
            "recovery",
        );
        let background = fixtures.remove(0);
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        let result = std::thread::spawn(move || {
            tracing::dispatcher::with_default(&dispatch, || background.run())
        })
        .join()
        .expect("join actual background replay")?;
        verify_wal(&result);
        assert_eq!(completed.load(Ordering::Acquire), 0);
        verify_wal(&fixtures.remove(0).run()?);
        let credited = completed.load(Ordering::Acquire);
        assert!(credited > 0);
        {
            let _workload = workload_listener::RecoveryScope::enter_with_completion(
                callback(&completed),
                &phase,
                "workload",
            );
            verify_wal(&fixtures.remove(0).run()?);
            assert_eq!(completed.load(Ordering::Acquire), credited);
        }
        verify_wal(&fixtures.remove(0).run()?);
        let restored = completed.load(Ordering::Acquire);
        assert!(restored > credited);
        phase.store(3, Ordering::Release);
        verify_wal(&fixtures.remove(0).run()?);
        assert_eq!(completed.load(Ordering::Acquire), restored);
        drop(scope);
        verify_wal(&fixtures.remove(0).run()?);
        assert_eq!(completed.load(Ordering::Acquire), restored);
        Ok(())
    })
}
