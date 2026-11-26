//! Constructed boundary controls for the bounded accounting owner.
use super::*;

#[test]
fn should_distinguish_durable_snapshot_from_checkpoint_when_tail_fails() {
    // Arrange: the actual persistence test will hit the existing after-rename failpoint.
    // This bounded-owner control checks only its separate recording contract.
    let owner = Owner::new();
    let handle = owner.handle();
    let operation = owner.begin(
        OperationKind::Checkpoint,
        Origin::OrdinaryLocalFlush,
        Medium::Persistent,
    );
    operation.ledger().lock().issued(Payload::Snapshot, 37);
    operation.ledger().lock().returned(Payload::Snapshot, 37);

    // Act: snapshot durability was reached, journal truncation did not complete.
    operation.snapshot_durable(37);
    operation.finish(false);

    // Assert: attempted bytes and durable snapshot survive the checkpoint failure.
    let snapshot = handle.snapshot();
    let counters = &snapshot
        .bucket(Origin::OrdinaryLocalFlush, Medium::Persistent)
        .counters;
    assert_eq!(counters.issued_bytes[0], 37);
    assert_eq!(counters.returned_write_bytes[0], 37);
    assert_eq!(counters.snapshot_durable_bytes, 37);
    assert_eq!(counters.snapshot_durable_count, 1);
    assert_eq!(counters.checkpoint_complete_count, 0);
    assert_eq!(counters.operation_failures, 1);
}

#[test]
fn should_retain_only_metrics_when_accounting_owner_is_dropped() {
    // Arrange: this helper owns no runtime/Fs/lease; real Engine reopen is separate coverage.
    let owner = Owner::new();
    let handle = owner.handle();
    let before = handle.snapshot();

    // Act: one logical flush commits, then its engine-owned producer drops.
    owner.flush_committed(
        Origin::OrdinaryLocalFlush,
        Medium::Persistent,
        2048,
        Duration::from_millis(9),
    );
    drop(owner);

    // Assert: the retained handle still reads stable counters from the same owner.
    let delta = handle.snapshot().delta(&before).unwrap();
    let bucket = delta.bucket(Origin::OrdinaryLocalFlush, Medium::Persistent);
    assert_eq!(bucket.counters.flush_committed_count, 1);
    assert_eq!(bucket.counters.flush_committed_sst_bytes, 2048);
    assert_eq!(bucket.counters.flush_full_publication_elapsed_ns, 9_000_000);
    assert!(bucket.full_publication_latency.p95_bounds_ns().unwrap().0 >= 9_000_000);
}

#[test]
fn should_subtract_histogram_buckets_when_warmup_precedes_measurement() {
    // Arrange: a slower warmup cannot alter measured p95 via max/quantile subtraction.
    let mut before = LatencyHistogram::default();
    assert!(!before.record(50_000_000));
    let mut after = before.clone();

    // Act: measured operations fall on both sides of the exact 5ms gate boundary.
    for _ in 0..95 {
        assert!(!after.record(4_999_999));
    }
    for _ in 0..5 {
        assert!(!after.record(5_000_000));
    }
    let delta = after.subtract(&before).unwrap();

    // Assert: the measured p95 is below the gate, although warmup's maximum was high.
    assert_eq!(delta.p95_bounds_ns(), Some((4_750_000, 4_999_999)));
    assert_eq!(delta.counts.iter().sum::<u64>(), 100);
}

#[test]
fn should_reject_measurement_when_write_handle_escapes_its_operation() {
    // Arrange: operation-scoped File wrappers must not write after their result was folded.
    let owner = Owner::new();
    let handle = owner.handle();
    let before = handle.snapshot();
    let operation = owner.begin(
        OperationKind::Checkpoint,
        Origin::Recovery,
        Medium::Persistent,
    );
    let escaped = operation.ledger();

    // Act: an invalid future caller keeps a wrapped handle beyond the operation.
    operation.finish(true);
    escaped.lock().issued(Payload::Snapshot, 19);

    // Assert: bounded telemetry fails closed rather than losing that late work silently.
    let after = handle.snapshot();
    assert_eq!(after.late_operation_writes, 1);
    assert!(after.delta(&before).is_err());
}

#[test]
fn should_retain_late_invalidation_when_sealed_observation_counter_is_saturated() {
    // Arrange: deliberately preseed the private primitive boundary, not a workload.
    // Actual escaped mutations cannot practically accumulate u64::MAX observations.
    let owner = Owner::new();
    let handle = owner.handle();
    let before = handle.snapshot();
    let operation = owner.begin(
        OperationKind::JournalAppend,
        Origin::Recovery,
        Medium::Persistent,
    );
    let escaped = operation.ledger();
    operation.finish(true);
    escaped.lock().late.store(u64::MAX, Ordering::Relaxed);
    let saturated = handle.snapshot();
    assert_eq!(saturated.late_operation_writes, u64::MAX);
    assert!(saturated.delta(&before).is_err());

    // Act: the real sealed ledger observes one more mutation at its counter bound.
    escaped.lock().observe_mutation();
    let after = handle.snapshot();

    // Assert: escaped-operation invalidation remains sticky without false payload credit.
    assert_eq!(after.late_operation_writes, u64::MAX);
    assert!(after.delta(&before).is_err());
    let bucket = after.bucket(Origin::Recovery, Medium::Persistent);
    assert_eq!(bucket.counters.issued_bytes, [0; 3]);
    assert_eq!(bucket.counters.returned_write_bytes, [0; 3]);
    assert_eq!(bucket.counters.operation_failures, 0);
    assert_eq!(bucket.active_operations, 0);
}
