//! Preregistered checkpoint measurement gates with native trust diagnostics retained.

use cntryl_midge::__internal::checkpoint::{Medium, Origin, Snapshot};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct Verdict {
    pub valid: bool,
    pub invalid_reasons: Vec<String>,
    pub snapshot_miss: bool,
    pub checkpoint_miss: bool,
    pub miss: Option<bool>,
    pub checkpoint_p95_lower_ns: Option<u64>,
    pub checkpoint_p95_upper_ns: Option<u64>,
}

pub fn evaluate(delta: &Snapshot, expected_flushes: u64, completed_compactions: u64) -> Verdict {
    let mut invalid = Vec::new();
    let bucket = delta.bucket(Origin::OrdinaryLocalFlush, Medium::Persistent);
    let counters = &bucket.counters;
    if delta.overflow || delta.late_operation_writes > 0 || delta.incomplete_observations > 0 {
        invalid.push("accounting overflow/escaped/incomplete operation".into());
    }
    if counters.flush_committed_count != expected_flushes {
        invalid.push("flush count differs from fixed construction".into());
    }
    check_persistent_operations(delta, &mut invalid);
    if counters.flush_committed_sst_bytes == 0 || counters.flush_full_publication_elapsed_ns == 0 {
        invalid.push("zero denominator".into());
    }
    if completed_compactions == 0 {
        invalid.push("no genuine compaction completed in measured ingestion".into());
    }
    if delta.buckets.iter().any(|bucket| {
        bucket.origin == Origin::Unclassified
            && (bucket.counters.operation_attempts > 0 || bucket.counters.flush_committed_count > 0)
    }) {
        invalid.push("unclassified measured metadata cost".into());
    }
    let p95 = bucket.checkpoint_latency.p95_bounds_ns();
    // 5ms is a histogram boundary: 250us bins cannot straddle this predicate.
    let snapshot_miss = u128::from(counters.issued_bytes[0]) * 100
        >= u128::from(counters.flush_committed_sst_bytes) * 5;
    let checkpoint_miss = u128::from(counters.checkpoint_elapsed_ns) * 100
        >= u128::from(counters.flush_full_publication_elapsed_ns) * 20
        && p95.is_some_and(|(lower, _)| lower >= 5_000_000);
    let valid = invalid.is_empty();
    Verdict {
        valid,
        invalid_reasons: invalid,
        snapshot_miss,
        checkpoint_miss,
        miss: valid.then_some(snapshot_miss || checkpoint_miss),
        checkpoint_p95_lower_ns: p95.map(|bounds| bounds.0),
        checkpoint_p95_upper_ns: p95.map(|bounds| bounds.1),
    }
}

fn check_persistent_operations(delta: &Snapshot, invalid: &mut Vec<String>) {
    for bucket in delta
        .buckets
        .iter()
        .filter(|bucket| bucket.medium == Medium::Persistent)
    {
        let counters = &bucket.counters;
        let reason = |message: &str| format!("{:?}: {message}", bucket.origin);
        if bucket.active_operations > 0 {
            invalid.push(reason("persistent metadata operation crosses end boundary"));
        }
        if counters.operation_failures > 0 || counters.abandoned_operations > 0 {
            invalid.push(reason("metadata persistence failed or was abandoned"));
        }
        if counters.checkpoint_attempts != counters.checkpoint_complete_count {
            invalid.push(reason(
                "checkpoint did not finish through journal truncation",
            ));
        }
        if counters.snapshot_durable_count != counters.checkpoint_complete_count {
            invalid.push(reason(
                "snapshot durability differs from checkpoint completion",
            ));
        }
        let histogram_count: u128 = bucket
            .checkpoint_latency
            .counts
            .iter()
            .map(|count| u128::from(*count))
            .sum();
        if histogram_count != u128::from(counters.checkpoint_attempts) {
            invalid.push(reason("checkpoint histogram does not cover every attempt"));
        }
        let publication_count: u128 = bucket
            .full_publication_latency
            .counts
            .iter()
            .map(|count| u128::from(*count))
            .sum();
        if publication_count != u128::from(counters.flush_committed_count) {
            invalid.push(reason(
                "publication histogram does not cover every committed flush",
            ));
        }
        let size_count: u128 = bucket
            .committed_sst_size_log2
            .iter()
            .map(|count| u128::from(*count))
            .sum();
        if size_count != u128::from(counters.flush_committed_count) {
            invalid.push(reason(
                "SST size distribution does not cover every committed flush",
            ));
        }
    }
}

/// Seal either original or reopened owner after all its engine work joins.
/// Shutdown/reopen costs never enter the measured ratios or histograms.
pub fn seal_after_shutdown(verdict: &mut Verdict, expected_owner: u64, settled: &Snapshot) {
    verdict
        .invalid_reasons
        .extend(settlement_invalid_reasons(expected_owner, settled));
    verdict.valid = verdict.invalid_reasons.is_empty();
    verdict.miss = verdict
        .valid
        .then_some(verdict.snapshot_miss || verdict.checkpoint_miss);
}

pub fn settlement_invalid_reasons(expected_owner: u64, settled: &Snapshot) -> Vec<String> {
    let mut invalid = Vec::new();
    if settled.owner_id != expected_owner {
        invalid.push("post-shutdown snapshot belongs to another owner".into());
    }
    if settled.overflow || settled.late_operation_writes > 0 || settled.incomplete_observations > 0
    {
        invalid.push("post-shutdown owner contains overflow/escaped/incomplete accounting".into());
    }
    if settled
        .buckets
        .iter()
        .any(|bucket| bucket.active_operations > 0)
    {
        invalid.push("metadata accounting operation remains active after owned shutdown".into());
    }
    check_persistent_operations(settled, &mut invalid);
    invalid
}
