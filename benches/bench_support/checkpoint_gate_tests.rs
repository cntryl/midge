//! Constructed-counter gate-policy controls only; no provider/flush proof is claimed.
use super::checkpoint_gate::*;
use cntryl_midge::__internal::checkpoint::{Counters, Medium, Origin, Snapshot};
use cntryl_midge::{Engine, OpenOptions};
use std::time::Duration;

fn policy_snapshot() -> Snapshot {
    // The hidden bridge supplies a real owner's fixed bucket layout. All costs
    // below are then deliberately constructed for the policy, not measured.
    let directory = tempfile::tempdir().expect("create policy owner directory");
    let options = OpenOptions::local(directory.path())
        .build()
        .expect("build policy owner options");
    let mut engine = Engine::open(options).expect("open policy owner");
    let handle = cntryl_midge::__internal::checkpoint::metrics_handle(&engine);
    engine
        .shutdown(Duration::from_secs(30))
        .expect("join policy owner");
    let mut snapshot = handle.snapshot();
    snapshot.overflow = false;
    snapshot.late_operation_writes = 0;
    snapshot.incomplete_observations = 0;
    for bucket in &mut snapshot.buckets {
        bucket.counters = Counters::default();
        bucket.active_operations = 0;
        bucket.checkpoint_latency.counts.fill(0);
        bucket.full_publication_latency.counts.fill(0);
        bucket.committed_sst_size_log2.fill(0);
    }
    let ordinary = snapshot
        .buckets
        .iter_mut()
        .find(|bucket| {
            bucket.origin == Origin::OrdinaryLocalFlush && bucket.medium == Medium::Persistent
        })
        .expect("fixed ordinary persistent bucket");
    ordinary.counters.flush_committed_count = 1;
    ordinary.counters.flush_committed_sst_bytes = 10_000;
    ordinary.counters.flush_full_publication_elapsed_ns = 20_000_000;
    ordinary.committed_sst_size_log2[13] = 1;
    ordinary.full_publication_latency.counts[64] = 1;
    snapshot
}

#[test]
fn should_invalidate_measured_gate_when_a_forced_persistent_checkpoint_fails() {
    // Arrange: constructed ordinary counters satisfy the policy's denominators.
    let mut delta = policy_snapshot();
    assert!(evaluate(&delta, 1, 1).valid);
    let forced = delta
        .buckets
        .iter_mut()
        .find(|bucket| {
            bucket.origin == Origin::CompactionBeforeGc && bucket.medium == Medium::Persistent
        })
        .unwrap();
    forced.counters.operation_attempts = 1;
    forced.counters.operation_failures = 1;
    forced.counters.checkpoint_attempts = 1;
    forced.counters.snapshot_durable_count = 1;
    forced.checkpoint_latency.counts[0] = 1;

    // Act: safe snapshot durability does not imply truncation/completion.
    let verdict = evaluate(&delta, 1, 1);

    // Assert: forced cost invalidates evidence without entering ordinary ratios.
    assert!(!verdict.valid);
    assert_eq!(verdict.miss, None);
    assert!(verdict
        .invalid_reasons
        .iter()
        .any(|reason| reason.contains("CompactionBeforeGc: metadata persistence failed")));
    assert!(verdict
        .invalid_reasons
        .iter()
        .any(|reason| reason.contains("CompactionBeforeGc: checkpoint did not finish")));
    assert!(!verdict.snapshot_miss && !verdict.checkpoint_miss);
}

#[test]
fn should_reject_provisional_gate_when_original_owner_later_reports_escaped_work() {
    // Arrange: a constructed valid window remains distinct from lifetime costs.
    let delta = policy_snapshot();
    let mut verdict = evaluate(&delta, 1, 1);
    assert!(verdict.valid);
    let mut settled = delta.clone();
    settled.late_operation_writes = 2;

    // Act: post-shutdown sealing finds evidence the original window omitted.
    seal_after_shutdown(&mut verdict, delta.owner_id, &settled);

    // Assert: the provisional accept cannot survive; ratios stay unchanged.
    assert!(!verdict.valid);
    assert_eq!(verdict.miss, None);
    assert!(verdict
        .invalid_reasons
        .iter()
        .any(|reason| reason.contains("escaped")));
    assert!(!verdict.snapshot_miss && !verdict.checkpoint_miss);
}

#[test]
fn should_invalidate_final_owner_when_forced_checkpoint_fails_after_measured_window() {
    // Arrange: constructed policy counters, not actual provider failure proof.
    let measured = policy_snapshot();
    let mut verdict = evaluate(&measured, 1, 1);
    assert!(verdict.valid);
    let mut settled = measured.clone();
    let forced = settled
        .buckets
        .iter_mut()
        .find(|bucket| {
            bucket.origin == Origin::CompactionBeforeGc && bucket.medium == Medium::Persistent
        })
        .unwrap();
    forced.counters.operation_attempts = 1;
    forced.counters.operation_failures = 1;
    forced.counters.checkpoint_attempts = 1;
    forced.counters.snapshot_durable_count = 1;
    forced.checkpoint_latency.counts[0] = 1;
    // Act: both original and reopened owner use this final integrity check.
    seal_after_shutdown(&mut verdict, measured.owner_id, &settled);
    // Assert: later costs invalidate proof without changing measured ratios.
    assert!(!verdict.valid);
    assert_eq!(verdict.miss, None);
    assert!(!verdict.snapshot_miss && !verdict.checkpoint_miss);
    assert!(verdict
        .invalid_reasons
        .iter()
        .any(|reason| { reason.contains("CompactionBeforeGc: metadata persistence failed") }));
}
