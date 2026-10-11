//! The simulated object store against the probes that disqualified the
//! earlier draft (#810 review) and the provider contract's semantics.

use super::{Failure, Fate, Operation, RequestId, Schedule, SimulatedObjectStore};
use crate::common::{MidgeError, OperationDeadline};
use crate::storage::cloud::{
    BlockingCloud, CloudBackend, CloudError, CloudEvent, CloudOutcome, CloudStorage, ObjectMetadata,
};
use std::collections::BTreeMap;
use std::sync::{mpsc, Arc};
use std::time::Duration;

const ANSWER_WAIT: Duration = Duration::from_millis(50);

fn id(operation: Operation, key: &str, occurrence: u64) -> RequestId {
    RequestId {
        operation,
        key: key.to_string(),
        occurrence,
    }
}

fn scripted(fates: &[(RequestId, Fate)]) -> SimulatedObjectStore {
    SimulatedObjectStore::new(Schedule::Scripted(fates.iter().cloned().collect()))
}

fn put(
    store: &SimulatedObjectStore,
    key: &str,
    bytes: &[u8],
    headers: &[(&str, &str)],
) -> mpsc::Receiver<CloudEvent> {
    let (tx, rx) = mpsc::channel();
    let headers = headers
        .iter()
        .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
        .collect();
    store.submit_put(key, bytes.to_vec(), headers, tx);
    rx
}

fn delete(
    store: &SimulatedObjectStore,
    key: &str,
    headers: &[(&str, &str)],
) -> mpsc::Receiver<CloudEvent> {
    let (tx, rx) = mpsc::channel();
    let headers = headers
        .iter()
        .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
        .collect();
    store.submit_delete(key, headers, tx);
    rx
}

fn head(store: &SimulatedObjectStore, key: &str) -> CloudOutcome<ObjectMetadata> {
    let (tx, rx) = mpsc::channel();
    store.submit_head(key, tx);
    match rx.recv_timeout(ANSWER_WAIT).expect("head answer") {
        CloudEvent::Head { result, .. } => result,
        other => panic!("unexpected answer: {other:?}"),
    }
}

/// The outcome class of an answer.
#[allow(clippy::match_same_arms)] // The arms bind results of different types.
fn class(answer: &CloudEvent) -> &'static str {
    let outcome = match answer {
        CloudEvent::Put { result, .. } | CloudEvent::Delete { result, .. } => result.as_ref().err(),
        CloudEvent::Get { result, .. } => result.as_ref().err(),
        CloudEvent::GetWithMetadata { result, .. } => result.as_ref().err(),
        CloudEvent::GetRange { result, .. } => result.as_ref().err(),
        CloudEvent::List { result, .. } => result.as_ref().err(),
        CloudEvent::Head { result, .. } => result.as_ref().err(),
    };
    match outcome {
        None => "ok",
        Some(CloudError::NotFound(_)) => "not-found",
        Some(CloudError::PreconditionFailed(_)) => "precondition-failed",
        Some(CloudError::ServerError(_)) => "server-error",
        Some(CloudError::Timeout(_)) => "timeout",
        Some(CloudError::Transport(_)) => "transport",
        Some(CloudError::ConditionalConflict(_)) => "conflict",
        Some(CloudError::Protocol(_)) => "protocol",
        Some(_) => "other",
    }
}

fn answered(rx: &mpsc::Receiver<CloudEvent>) -> &'static str {
    class(&rx.recv_timeout(ANSWER_WAIT).expect("an answer"))
}

#[test]
fn should_land_late_put_when_its_client_already_gave_up() {
    // Arrange: the first put of the segment reaches the provider late.
    let store = scripted(&[(id(Operation::Put, "wal/1", 0), Fate::Late)]);
    let cloud = CloudStorage::new_with_timeout(
        Arc::new(store.clone()),
        String::new(),
        Duration::from_millis(100),
    );
    let deadline = OperationDeadline::from_budget(Duration::from_millis(100));

    // Act
    let result = BlockingCloud::new(&cloud, &deadline).put_with_headers(
        "wal/1",
        b"segment".to_vec(),
        Vec::new(),
    );
    let before_landing = store.object("wal/1");
    let landed = store.land_late_effects();

    // Assert
    assert!(matches!(result, Err(MidgeError::Timeout(_))), "{result:?}");
    assert_eq!(before_landing, None);
    assert_eq!(landed, 1);
    assert_eq!(store.object("wal/1"), Some(b"segment".to_vec()));
}

#[test]
fn should_evaluate_late_condition_when_effect_lands() {
    // Arrange: a late conditional create, then a create that wins.
    let store = scripted(&[(id(Operation::Put, "lease", 0), Fate::Late)]);
    let late = put(&store, "lease", b"stale holder", &[("If-None-Match", "*")]);
    let winner = put(&store, "lease", b"new holder", &[("If-None-Match", "*")]);

    // Act
    let landed = store.land_late_effects();

    // Assert: the late create lost its condition when it landed.
    assert_eq!(landed, 1);
    assert_eq!(answered(&late), "timeout");
    assert_eq!(answered(&winner), "ok");
    assert_eq!(store.object("lease"), Some(b"new holder".to_vec()));
}

#[test]
fn should_honor_stale_identity_when_identical_bytes_return() {
    // Arrange: S3 names a single upload by its content, so re-putting the
    // same bytes after a delete repeats the predecessor's identity.
    let store = SimulatedObjectStore::eager();
    answered(&put(&store, "wal/7", b"segment", &[]));
    let predecessor = head(&store, "wal/7").expect("predecessor identity");
    answered(&delete(&store, "wal/7", &[]));
    answered(&put(&store, "wal/7", b"segment", &[]));

    // Act
    let stale_delete = delete(&store, "wal/7", &[("If-Match", &predecessor.etag)]);

    // Assert: the stale delete removes the successor.
    assert_eq!(answered(&stale_delete), "ok");
    assert_eq!(store.object("wal/7"), None);
}

#[test]
fn should_reject_stale_identity_when_content_changed() {
    // Arrange
    let store = SimulatedObjectStore::eager();
    answered(&put(&store, "catalog", b"one", &[]));
    let stale = head(&store, "catalog").expect("identity");
    answered(&put(&store, "catalog", b"two", &[]));

    // Act
    let stale_put = put(&store, "catalog", b"three", &[("If-Match", &stale.etag)]);
    let stale_delete = delete(&store, "catalog", &[("If-Match", &stale.etag)]);

    // Assert
    assert_eq!(answered(&stale_put), "precondition-failed");
    assert_eq!(answered(&stale_delete), "precondition-failed");
    assert_eq!(store.object("catalog"), Some(b"two".to_vec()));
}

#[test]
fn should_replay_same_fates_when_submissions_interleave() {
    // Arrange: one seeded schedule, two submission orders.
    let schedule = || Schedule::Seeded {
        seed: 11,
        fates: vec![
            Fate::Normal,
            Fate::FailBefore(Failure::Server),
            Fate::FailAfter(Failure::Timeout),
            Fate::Late,
        ],
        scope: Vec::new(),
    };
    let first = SimulatedObjectStore::new(schedule());
    let second = SimulatedObjectStore::new(schedule());

    // Act
    for key in ["a", "b", "a", "c", "b", "a"] {
        drop(put(&first, key, b"x", &[]));
    }
    for key in ["c", "a", "b", "a", "a", "b"] {
        drop(put(&second, key, b"x", &[]));
    }

    // Assert
    let fates =
        |store: &SimulatedObjectStore| store.trace().into_iter().collect::<BTreeMap<_, _>>();
    assert_eq!(fates(&first), fates(&second));
    assert!(
        fates(&first).values().any(|fate| *fate != Fate::Normal),
        "the schedule is adversarial"
    );
}

#[test]
fn should_apply_effect_when_answer_is_lost() {
    // Arrange: the provider applied the put, but its answer never arrived, so
    // the executor reports a timeout.
    let store = scripted(&[(
        id(Operation::Put, "wal/2", 0),
        Fate::FailAfter(Failure::Timeout),
    )]);

    // Act
    let answer = put(&store, "wal/2", b"segment", &[]);

    // Assert
    assert_eq!(answered(&answer), "timeout");
    assert_eq!(store.object("wal/2"), Some(b"segment".to_vec()));
}

#[test]
fn should_not_apply_effect_when_request_fails_before() {
    // Arrange
    let store = scripted(&[(
        id(Operation::Put, "wal/3", 0),
        Fate::FailBefore(Failure::Transport),
    )]);

    // Act
    let answer = put(&store, "wal/3", b"segment", &[]);

    // Assert
    assert_eq!(answered(&answer), "transport");
    assert_eq!(store.object("wal/3"), None);
}

#[test]
fn should_apply_effect_when_request_fails_after() {
    // Arrange
    let store = scripted(&[(
        id(Operation::Put, "wal/4", 0),
        Fate::FailAfter(Failure::Server),
    )]);

    // Act
    let answer = put(&store, "wal/4", b"segment", &[]);

    // Assert
    assert_eq!(answered(&answer), "server-error");
    assert_eq!(store.object("wal/4"), Some(b"segment".to_vec()));
}

#[test]
fn should_classify_outcomes_like_providers_when_requests_are_conditional() {
    // Arrange: the scenarios the provider contract pins for every provider.
    let store = SimulatedObjectStore::eager();
    answered(&put(&store, "existing", b"one", &[]));
    let current = head(&store, "existing").expect("identity");
    let stale = ObjectMetadata {
        size: 3,
        etag: "\"stale\"".into(),
        generation: None,
    };
    let range = |key: &str, expected: &ObjectMetadata, end: u64| {
        let (tx, rx) = mpsc::channel();
        store.submit_get_range_with_identity(key, 0, end, expected.clone(), ANSWER_WAIT, tx);
        rx.recv_timeout(ANSWER_WAIT).expect("range answer")
    };
    let get = |key: &str| {
        let (tx, rx) = mpsc::channel();
        store.submit_get(key, tx);
        rx.recv_timeout(ANSWER_WAIT).expect("get answer")
    };

    // Act
    let outcomes = [
        (
            "create existing",
            answered(&put(&store, "existing", b"x", &[("If-None-Match", "*")])),
        ),
        (
            "update stale",
            answered(&put(&store, "existing", b"x", &[("If-Match", &stale.etag)])),
        ),
        (
            "update missing",
            answered(&put(
                &store,
                "missing",
                b"x",
                &[("If-Match", &current.etag)],
            )),
        ),
        ("read missing", class(&get("missing"))),
        (
            "head missing",
            match head(&store, "missing") {
                Err(CloudError::NotFound(_)) => "not-found",
                _ => "other",
            },
        ),
        ("delete missing", answered(&delete(&store, "missing", &[]))),
        (
            "conditional delete missing",
            answered(&delete(&store, "missing", &[("If-Match", &current.etag)])),
        ),
        ("range stale", class(&range("existing", &stale, 3))),
        ("range exact", class(&range("existing", &current, 3))),
        ("range past end", class(&range("existing", &current, 4))),
    ];

    // Assert
    assert_eq!(
        outcomes,
        [
            ("create existing", "precondition-failed"),
            ("update stale", "precondition-failed"),
            ("update missing", "precondition-failed"),
            ("read missing", "not-found"),
            ("head missing", "not-found"),
            ("delete missing", "ok"),
            ("conditional delete missing", "ok"),
            ("range stale", "precondition-failed"),
            ("range exact", "ok"),
            ("range past end", "protocol"),
        ]
    );
}

#[test]
fn should_list_every_key_under_prefix_when_listing() {
    // Arrange
    let store = SimulatedObjectStore::eager();
    for key in ["wal/2", "wal/1", "walrus", "sst/1"] {
        answered(&put(&store, key, b"x", &[]));
    }
    let (tx, rx) = mpsc::channel();

    // Act
    store.submit_list("wal/", tx);

    // Assert
    match rx.recv_timeout(ANSWER_WAIT).expect("list answer") {
        CloudEvent::List { result, .. } => {
            assert_eq!(result.expect("keys"), ["wal/1", "wal/2"]);
        }
        other => panic!("unexpected answer: {other:?}"),
    }
}

#[test]
fn should_release_reservation_when_answer_is_lost_or_late() {
    for fate in [Fate::FailAfter(Failure::Timeout), Fate::Late] {
        // Arrange: an admitted upload holds its reservation until answered.
        let store = scripted(&[(id(Operation::Put, "sst/1", 0), fate)]);
        let budget = crate::common::resource_budget::ResourceBudget::new(1 << 20);
        let reservation = Arc::new(budget.reserve(4096, "upload").expect("reserve"));
        let (tx, rx) = mpsc::channel();

        // Act
        store.submit_put_with_reservation(
            "sst/1",
            b"table".to_vec(),
            Vec::new(),
            Some(reservation),
            tx,
        );
        let answer = rx.recv_timeout(ANSWER_WAIT).expect("an answer");
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while budget.used() != 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }

        // Assert
        assert_eq!(class(&answer), "timeout", "{fate:?}");
        assert_eq!(budget.used(), 0, "{fate:?}");
    }
}

#[test]
fn should_resurrect_object_when_late_put_lands_after_late_delete() {
    // Arrange: a zombie put and a delete, both late.
    let store = SimulatedObjectStore::eager();
    answered(&put(&store, "wal/9", b"segment", &[]));
    store.set_schedule(Schedule::Scripted(
        [
            (id(Operation::Put, "wal/9", 1), Fate::Late),
            (id(Operation::Delete, "wal/9", 0), Fate::Late),
        ]
        .into_iter()
        .collect(),
    ));
    answered(&put(&store, "wal/9", b"zombie", &[]));
    answered(&delete(&store, "wal/9", &[]));

    // Act: the delete lands first, then the zombie put.
    let late = store.late_requests();
    let landed_delete = store.land_late(&id(Operation::Delete, "wal/9", 0));
    let landed_put = store.land_late(&id(Operation::Put, "wal/9", 1));

    // Assert
    assert_eq!(
        late,
        [
            id(Operation::Put, "wal/9", 1),
            id(Operation::Delete, "wal/9", 0)
        ]
    );
    assert!(landed_delete && landed_put);
    assert_eq!(store.object("wal/9"), Some(b"zombie".to_vec()));
    assert_eq!(store.late_requests(), Vec::new());
}

#[test]
fn should_name_requests_alike_when_generation_names_differ() {
    // Arrange: two runs publish generations under different random names.
    let runs = [
        "metadata/generations/15d115db-8ea6-45ba-bc78-be14dda6c277/manifest.journal",
        "metadata/generations/9c4de13d-4f53-4e19-8f26-ef2033386653/manifest.journal",
    ];

    // Act
    let names: Vec<RequestId> = runs
        .iter()
        .map(|key| {
            let store = SimulatedObjectStore::eager();
            answered(&put(&store, key, b"journal", &[]));
            store.trace().remove(0).0
        })
        .collect();

    // Assert
    assert_eq!(names[0], names[1]);
    assert_eq!(names[0].key, "metadata/generations/*/manifest.journal");
}

#[test]
fn should_leave_requests_outside_scope_eager_when_schedule_is_scoped() {
    // Arrange: only WAL traffic is adversarial.
    let store = SimulatedObjectStore::new(Schedule::Seeded {
        seed: 5,
        fates: vec![Fate::FailBefore(Failure::Server)],
        scope: vec!["wal/".into()],
    });

    // Act
    let lease = put(&store, "midge_primary_lease.json", b"lease", &[]);
    let segment = put(&store, "wal/1.wal", b"segment", &[]);

    // Assert
    assert_eq!(answered(&lease), "ok");
    assert_eq!(answered(&segment), "server-error");
}

#[test]
fn should_report_conflict_only_before_a_conditional_effect() {
    // Arrange
    let store = scripted(&[
        (
            id(Operation::Put, "catalog", 0),
            Fate::FailBefore(Failure::Conflict),
        ),
        (
            id(Operation::Put, "plain", 0),
            Fate::FailBefore(Failure::Conflict),
        ),
        (
            id(Operation::Put, "catalog", 1),
            Fate::FailAfter(Failure::Conflict),
        ),
    ]);

    // Act
    let conditional = answered(&put(&store, "catalog", b"one", &[("If-None-Match", "*")]));
    let unconditional = answered(&put(&store, "plain", b"one", &[]));
    let after_effect = answered(&put(&store, "catalog", b"two", &[("If-None-Match", "*")]));

    // Assert: a conflict promises nothing was committed, so it is never
    // reported for a mutation that applied or had no condition.
    assert_eq!(conditional, "conflict");
    assert_eq!(unconditional, "server-error");
    assert_eq!(after_effect, "server-error");
    assert_eq!(store.object("catalog"), Some(b"two".to_vec()));
}

#[test]
fn should_cut_crashed_process_off_without_effect_when_processes_restart() {
    // Arrange: two processes share the store.
    let store = SimulatedObjectStore::eager();
    let crashed = store.process();
    let other = store.process();

    // Act
    crashed.process_crash();
    let restarted = store.process();
    let from_crashed = put(&crashed.clone(), "k", b"zombie", &[]).recv_timeout(ANSWER_WAIT);
    let from_restarted = put(&restarted, "k", b"live", &[]).recv_timeout(ANSWER_WAIT);
    let from_other = head(&other, "k");

    // Assert
    assert_eq!(class(&from_crashed.expect("crashed answer")), "transport");
    assert_eq!(class(&from_restarted.expect("restarted answer")), "ok");
    assert!(from_other.is_ok(), "{from_other:?}");
    assert_eq!(store.object("k").as_deref(), Some(b"live".as_slice()));
    assert_eq!(
        store.trace(),
        vec![
            (id(Operation::Put, "k", 0), Fate::Normal),
            (id(Operation::Head, "k", 0), Fate::Normal),
        ]
    );
}

#[test]
fn should_land_crashed_process_late_effect_when_landed_after_the_crash() {
    // Arrange: a put that reached the provider before its process died.
    let store = scripted(&[(id(Operation::Put, "k", 0), Fate::Late)]);
    let crashed = store.process();
    let _ = put(&crashed, "k", b"zombie", &[]).recv_timeout(ANSWER_WAIT);

    // Act
    crashed.process_crash();
    let landed = store.land_late_effects();

    // Assert
    assert_eq!(landed, 1);
    assert_eq!(store.object("k").as_deref(), Some(b"zombie".as_slice()));
}
