//! Fixed-budget scheduler campaign; engine qualification is a separate layer.
use super::counterexample::{Counterexample, MODEL_VERSION};
use super::object_store::ObjectStore;
use super::scheduler::{Action, Scheduler};
use crate::common::OperationDeadline;
use crate::storage::{
    StorageBackend, StorageErrorKind, StorageEvent, StorageOutcome, StoragePrecondition,
    StorageRequest,
};
use std::collections::BTreeMap;
use std::sync::mpsc;
use std::time::Duration;

fn random(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut value = *state;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn request(key: &str) -> StorageRequest {
    StorageRequest::new(key, OperationDeadline::unbounded(), Duration::from_secs(5))
}

fn campaign(seed: u64, operations: usize) -> Result<(), (String, Scheduler, u64)> {
    let scheduler = Scheduler::manual();
    let store = ObjectStore::new(scheduler.clone());
    let mut state = seed;
    let mut commands = Vec::new();
    for index in 0..operations {
        let key = format!("immutable/{}", random(&mut state) % 32);
        let bytes = format!("seed-{seed}-operation-{index}").into_bytes();
        let (tx, rx) = mpsc::channel();
        store.submit_write_request(
            request(&key).with_precondition(StoragePrecondition::IfAbsent),
            bytes.clone(),
            tx,
        );
        let id = scheduler.pending().last().expect("submitted command").0.id;
        commands.push((id, key, bytes, rx));
    }
    // The first actual effect for each immutable key wins. Submission order,
    // callback order and callback loss do not change that expected body.
    for index in (1..commands.len()).rev() {
        let other =
            usize::try_from(random(&mut state) % u64::try_from(index + 1).unwrap()).unwrap();
        commands.swap(index, other);
    }
    let mut expected = BTreeMap::new();
    let mut successful = BTreeMap::new();
    let mut actions = Vec::new();
    for (id, key, bytes, _) in &commands {
        let winner = !expected.contains_key(key);
        expected.entry(key.clone()).or_insert_with(|| bytes.clone());
        successful.insert(*id, winner);
        let action = Action::Effect(*id);
        scheduler.apply(action.clone()).unwrap();
        actions.push(action);
    }
    // Responses are independently permuted, duplicated, or lost after effects.
    for index in (1..commands.len()).rev() {
        let other =
            usize::try_from(random(&mut state) % u64::try_from(index + 1).unwrap()).unwrap();
        commands.swap(index, other);
    }
    for (id, _, _, rx) in commands {
        let selection = random(&mut state) % 3;
        if selection == 2 {
            scheduler.apply(Action::Duplicate(id)).unwrap();
            actions.push(Action::Duplicate(id));
        }
        let action = if selection == 0 {
            Action::LoseResponse(id)
        } else {
            Action::Deliver(id)
        };
        scheduler.apply(action.clone()).unwrap();
        actions.push(action);
        for event in rx {
            let correct = match event {
                StorageEvent::WriteComplete {
                    result: StorageOutcome::Ok(()),
                    ..
                } => successful[&id],
                StorageEvent::WriteComplete {
                    result: StorageOutcome::Err(error),
                    ..
                } => !successful[&id] && error.kind() == StorageErrorKind::PreconditionFailed,
                _ => false,
            };
            if !correct {
                return Err((
                    format!("wrong frozen completion for operation {id}; actions {actions:?}"),
                    scheduler,
                    store.physical_bytes(),
                ));
            }
        }
    }
    for (key, bytes) in &expected {
        let (tx, rx) = mpsc::channel();
        store.submit_metadata_read_request(request(key), tx);
        let id = scheduler.pending().last().unwrap().0.id;
        scheduler.apply(Action::Effect(id)).unwrap();
        scheduler.apply(Action::Deliver(id)).unwrap();
        let (actual, _) = rx.recv().unwrap().unwrap();
        if actual != *bytes {
            return Err((
                format!("immutable key {key} changed"),
                scheduler,
                store.physical_bytes(),
            ));
        }
    }
    let expected_bytes: u64 = expected.values().map(|bytes| bytes.len() as u64).sum();
    if store.physical_bytes() != expected_bytes || !scheduler.pending().is_empty() {
        return Err((
            "resource accounting did not drain".into(),
            scheduler,
            store.physical_bytes(),
        ));
    }
    Ok(())
}

#[test]
fn should_preserve_immutable_objects_when_scheduler_runs_fixed_pr_campaign() {
    // Arrange
    let seed_count = 512;
    let operations = 256;
    // Act
    for seed in 0..seed_count {
        if let Err((error, scheduler, physical_bytes)) = campaign(seed, operations) {
            let trace = scheduler.trace();
            let actions = trace
                .iter()
                .filter_map(|event| match event {
                    super::scheduler::TraceEvent::Action(action) => Some(action.clone()),
                    _ => None,
                })
                .collect();
            let source_sha = std::process::Command::new("git")
                .args(["rev-parse", "HEAD"])
                .output()
                .expect("source identity");
            let example = Counterexample {
                model_version: MODEL_VERSION,
                source_sha: String::from_utf8(source_sha.stdout).unwrap().trim().into(),
                seed,
                transactions: Vec::new(),
                actions,
                trace,
                observed: Vec::new(),
                resource_counters: BTreeMap::from([
                    ("object_bytes".into(), physical_bytes),
                    ("pending_commands".into(), scheduler.pending().len() as u64),
                ]),
            };
            let root = std::env::var_os("MIDGE_SIM_ARTIFACT_DIR").map_or_else(
                || std::path::PathBuf::from("target/wal-simulation"),
                std::path::PathBuf::from,
            );
            std::fs::create_dir_all(&root).unwrap();
            let path = root.join(format!("scheduler-{seed}.json"));
            std::fs::write(&path, serde_json::to_vec_pretty(&example).unwrap()).unwrap();
            panic!("seed {seed}: {error}; counterexample {}", path.display());
        }
    }
    // Assert: each seed checked every delivered outcome, final body, and drain.
}
