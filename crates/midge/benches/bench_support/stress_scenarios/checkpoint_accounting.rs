//! Bounded phase snapshots for Tier 5/6; no byte inference or cadence gate.

use super::{write_atomic_json, WorkloadArtifacts};
use cntryl_midge::__internal::checkpoint::{metrics_handle, MetricsHandle, Snapshot};
use cntryl_midge::{Engine, MidgeError};
use serde_json::json;
use std::path::PathBuf;

/// The retained handle contains counters only, never an Engine/Fs/runtime/lease.
pub(super) struct CheckpointAccounting {
    handle: MetricsHandle,
    owner_id: u64,
    role: &'static str,
    directory: PathBuf,
}

impl CheckpointAccounting {
    pub(super) fn attach(
        engine: &Engine,
        artifacts: &WorkloadArtifacts,
        role: &'static str,
        predecessor: Option<u64>,
    ) -> Self {
        let handle = metrics_handle(engine);
        let owner_id = handle.snapshot().owner_id;
        let directory = artifacts.path.join("checkpoint-accounting").join(role);
        std::fs::create_dir_all(&directory).expect("create bounded accounting overlay directory");
        let trace = Self {
            handle,
            owner_id,
            role,
            directory,
        };
        let header = json!({
            "schema_version":"midge-soak-metadata-accounting.v1",
            "benchmark_workload":artifacts.benchmark,
            "git_commit":artifacts.git_commit,
            "process_id":std::process::id(),
            "owner_role":role,
            "owner_id":owner_id,
            "predecessor_owner_id":predecessor,
            "distinct_from_predecessor":predecessor.map(|previous| previous != owner_id),
            "observed_payload_boundary":"issued/returned delegated engine-Fs metadata arguments; durable confirmation recorded separately",
            "pre_state_startup_writes_covered":false,
            "pre_state_startup_exclusions":"FORMAT creation, pre-state loading/repair/hydration and independent bootstrap MockFs owner",
            "active_operations_scope":"metadata calls only; not a complete runtime idle gauge",
            "snapshot_boundary":"caller phase observation; not an event-loop atomic barrier",
            "device_write_amplification_measured":false,
            "cadence_or_ratio_release_claim":false,
        });
        write_atomic_json(&trace.directory.join("owner.json"), &header)
            .expect("persist original accounting owner identity");
        trace.capture("after-open-and-family-setup", artifacts, None);
        trace
    }

    pub(super) fn owner_id(&self) -> u64 {
        self.owner_id
    }

    pub(super) fn capture(
        &self,
        boundary: &'static str,
        artifacts: &WorkloadArtifacts,
        caller_result: Option<&Result<(), MidgeError>>,
    ) -> Snapshot {
        let snapshot = self.handle.snapshot();
        let observation = json!({
            "schema_version":"midge-soak-metadata-point.v1",
            "benchmark_workload":artifacts.benchmark,
            "git_commit":artifacts.git_commit,
            "process_id":std::process::id(),
            "owner_role":self.role,
            "owner_id":self.owner_id,
            "same_owner":snapshot.owner_id == self.owner_id,
            "boundary":boundary,
            "phase":artifacts.phase,
            "stage":artifacts.stage,
            "stage_index":artifacts.stage_index,
            "elapsed_ms":artifacts.started.elapsed().as_millis(),
            "caller_result":caller_result.map(|result| json!({
                "ok":result.is_ok(),
                "error":result.as_ref().err().map(|error| format!("{error:?}")),
            })),
            "snapshot":snapshot,
        });
        write_atomic_json(
            &self.directory.join(format!("{boundary}.json")),
            &observation,
        )
        .expect("persist actual bounded metadata snapshot");
        snapshot
    }

    /// An explicit same-owner counter/histogram delta; end gauges are samples.
    /// No origin is inferred from benchmark phase, and no bytes are estimated.
    pub(super) fn window(
        &self,
        label: &'static str,
        before: &Snapshot,
        artifacts: &WorkloadArtifacts,
        caller_result: Option<&Result<(), MidgeError>>,
    ) {
        let boundary = match label {
            "ingestion" => "after-ingestion",
            "explicit-flush" => "after-explicit-flush",
            _ => unreachable!("only two fixed accounting windows are registered"),
        };
        let after = self.capture(boundary, artifacts, caller_result);
        let computed = after.delta(before);
        let observation = json!({
            "schema_version":"midge-soak-metadata-window.v1",
            "benchmark_workload":artifacts.benchmark,
            "git_commit":artifacts.git_commit,
            "process_id":std::process::id(),
            "owner_role":self.role,
            "owner_id":self.owner_id,
            "window":label,
            "phase_at_end":artifacts.phase,
            "stage_at_end":artifacts.stage,
            "stage_index_at_end":artifacts.stage_index,
            "elapsed_ms":artifacts.started.elapsed().as_millis(),
            "before":before,
            "after":after,
            "delta":computed.as_ref().ok(),
            "delta_error":computed.as_ref().err(),
            "active_operations_are_end_gauges":true,
            "forced_costs_remain_in_explicit_origin_medium_buckets":true,
            "device_write_amplification_measured":false,
            "cadence_or_ratio_release_claim":false,
        });
        write_atomic_json(
            &self.directory.join(format!("window-{label}.json")),
            &observation,
        )
        .expect("persist same-owner diagnostic window without inference");
    }

    /// Called only after actual successful owned shutdown; uses no Engine.
    pub(super) fn finalized(&self, artifacts: &WorkloadArtifacts) {
        self.capture("final-after-owned-shutdown", artifacts, None);
    }

    /// Observe the original counter owner again after the distinct recovered
    /// engine's owned shutdown, keeping later costs outside ingestion windows.
    pub(super) fn after_reopened_shutdown(&self, artifacts: &WorkloadArtifacts) {
        self.capture("original-final-after-reopened-shutdown", artifacts, None);
    }
}
