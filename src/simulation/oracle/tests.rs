//! The oracle against the probes that disqualified the earlier draft's
//! subset semantics (#810 finding 8).

use super::{Commit, History, Key, Logging, Verdict};
use std::collections::{BTreeMap, BTreeSet};

const FAMILY: &str = "default";

fn key(name: &str) -> Key {
    (FAMILY.to_string(), name.as_bytes().to_vec())
}

fn write(name: &str, value: Option<&str>, logging: Logging, acknowledged: bool) -> Commit {
    Commit {
        family: FAMILY.to_string(),
        writes: vec![(
            name.as_bytes().to_vec(),
            value.map(|value| value.as_bytes().to_vec()),
        )],
        logging,
        acknowledged,
    }
}

fn put(name: &str, value: &str) -> Commit {
    write(name, Some(value), Logging::Wal, true)
}

fn observed(pairs: &[(&str, &str)]) -> BTreeMap<Key, Vec<u8>> {
    pairs
        .iter()
        .map(|(name, value)| (key(name), value.as_bytes().to_vec()))
        .collect()
}

fn families() -> BTreeSet<String> {
    BTreeSet::from([FAMILY.to_string()])
}

fn history() -> History {
    History::new(BTreeMap::new(), families())
}

#[test]
fn should_reject_recovery_hole_when_later_commit_survives_without_earlier() {
    // Arrange: k must survive; its delete and a put of j were not synced.
    let mut history = history();
    let first = history.commit(put("k", "1"));
    history.synced(first);
    history.commit(write("k", None, Logging::Wal, true));
    history.commit(put("j", "2"));

    // Act: j survived but the earlier delete of k did not.
    let verdict = history.check(&observed(&[("k", "1"), ("j", "2")]), &families());

    // Assert
    assert!(matches!(verdict, Verdict::Violation(_)), "{verdict:?}");
}

#[test]
fn should_accept_every_prefix_when_nothing_was_synced() {
    // Arrange
    let mut history = history();
    history.commit(put("a", "1"));
    history.commit(put("b", "2"));
    history.commit(write("a", None, Logging::Wal, true));

    // Act
    let verdicts: Vec<Verdict> = [
        observed(&[]),
        observed(&[("a", "1")]),
        observed(&[("a", "1"), ("b", "2")]),
        observed(&[("b", "2")]),
    ]
    .iter()
    .map(|state| history.check(state, &families()))
    .collect();

    // Assert
    assert_eq!(
        verdicts,
        [0, 1, 2, 3].map(|prefix| Verdict::Consistent { prefix })
    );
}

#[test]
fn should_require_earlier_buffered_commits_when_a_later_commit_is_synced() {
    // Arrange: a buffered put, then a synced put of another key.
    let mut history = history();
    history.commit(put("a", "1"));
    let synced = history.commit(put("b", "2"));
    history.synced(synced);

    // Act
    let lost_buffered = history.check(&observed(&[("b", "2")]), &families());

    // Assert
    assert!(
        matches!(lost_buffered, Verdict::Violation(_)),
        "{lost_buffered:?}"
    );
}

#[test]
fn should_allow_unlogged_commit_to_vanish_until_its_family_is_flushed() {
    // Arrange: a synced put, then a best-effort delete of the same key.
    let mut history = history();
    let put = history.commit(put("k", "1"));
    history.synced(put);
    history.commit(write("k", None, Logging::Unlogged, true));

    // Act
    let before_flush = history.check(&observed(&[("k", "1")]), &families());
    history.flushed(FAMILY);
    let after_flush = history.check(&observed(&[("k", "1")]), &families());

    // Assert
    assert!(
        matches!(before_flush, Verdict::Consistent { .. }),
        "{before_flush:?}"
    );
    assert!(
        matches!(after_flush, Verdict::Violation(_)),
        "{after_flush:?}"
    );
}

#[test]
fn should_require_every_acknowledged_commit_when_shut_down_cleanly() {
    // Arrange: the best-effort delete probe behind `best-effort-delete`.
    let mut history = history();
    history.commit(put("k", "1"));
    history.commit(write("k", None, Logging::Unlogged, true));

    // Act
    history.shut_down();
    let resurrected = history.check(&observed(&[("k", "1")]), &families());

    // Assert
    assert!(
        matches!(resurrected, Verdict::Violation(_)),
        "{resurrected:?}"
    );
}

#[test]
fn should_allow_either_outcome_when_commit_returned_an_error() {
    // Arrange
    let mut history = history();
    let put = history.commit(put("k", "1"));
    history.synced(put);
    history.commit(write("k", Some("2"), Logging::Wal, false));

    // Act
    let applied = history.check(&observed(&[("k", "2")]), &families());
    let not_applied = history.check(&observed(&[("k", "1")]), &families());
    let invented = history.check(&observed(&[("k", "3")]), &families());

    // Assert
    assert!(matches!(applied, Verdict::Consistent { .. }), "{applied:?}");
    assert!(
        matches!(not_applied, Verdict::Consistent { .. }),
        "{not_applied:?}"
    );
    assert!(matches!(invented, Verdict::Violation(_)), "{invented:?}");
}

#[test]
fn should_report_lost_family_when_acknowledged_creation_did_not_survive() {
    // Arrange
    let mut history = history();
    history.create_family("idle");

    // Act
    let verdict = history.check(&observed(&[]), &families());

    // Assert
    match verdict {
        Verdict::Violation(violation) => assert_eq!(violation.lost_families, ["idle"]),
        consistent @ Verdict::Consistent { .. } => {
            panic!("expected a violation, got {consistent:?}")
        }
    }
}

#[test]
fn should_judge_long_history_quickly_when_only_the_full_prefix_fits() {
    // Arrange: every prefix must be judged before the last one fits, the
    // worst case for the search.
    let mut history = history();
    let mut state = BTreeMap::new();
    for index in 0..2_000_u32 {
        let name = format!("k{index}");
        let value = index.to_string();
        history.commit(put(&name, &value));
        state.insert(key(&name), value.into_bytes());
    }

    // Act
    let started = std::time::Instant::now();
    let verdict = history.check(&state, &families());

    // Assert
    assert_eq!(verdict, Verdict::Consistent { prefix: 2_000 });
    assert!(started.elapsed() < std::time::Duration::from_secs(2));
}

#[test]
fn should_reject_key_the_history_never_wrote_when_it_reappears() {
    // Arrange: k was deleted and verified absent before this history began.
    let history = history();

    // Act
    let verdict = history.check(&observed(&[("k", "1")]), &families());

    // Assert
    assert!(matches!(verdict, Verdict::Violation(_)), "{verdict:?}");
}

#[test]
fn should_allow_flushed_family_to_outlive_earlier_family_when_nothing_was_synced() {
    // Arrange: a buffered put to the default family, then a buffered put to
    // another family that is flushed.
    let mut history = History::new(
        BTreeMap::new(),
        BTreeSet::from([FAMILY.to_string(), "other".to_string()]),
    );
    history.commit(put("k", "1"));
    history.commit(Commit {
        family: "other".to_string(),
        writes: vec![(b"j".to_vec(), Some(b"2".to_vec()))],
        logging: Logging::Wal,
        acknowledged: true,
    });
    history.flushed("other");

    // Act: the flushed family survived, the earlier unsynced write did not.
    let verdict = history.check(
        &BTreeMap::from([(("other".to_string(), b"j".to_vec()), b"2".to_vec())]),
        &BTreeSet::from([FAMILY.to_string(), "other".to_string()]),
    );

    // Assert: a flush promises its own family, not ordering across families.
    assert!(matches!(verdict, Verdict::Consistent { .. }), "{verdict:?}");
}

#[test]
fn should_round_trip_through_json_when_history_is_saved() {
    // Arrange
    let mut history = History::new(observed(&[("base", "0")]), families());
    let synced = history.commit(put("k", "1"));
    history.synced(synced);

    // Act
    let json = serde_json::to_string(&history).expect("encode history");
    let decoded: History = serde_json::from_str(&json).expect("decode history");

    // Assert
    assert_eq!(
        decoded.check(&observed(&[("base", "0"), ("k", "1")]), &families()),
        Verdict::Consistent { prefix: 1 }
    );
    assert_eq!(decoded.keys(), history.keys());
}

#[test]
fn should_not_promise_forgotten_commits_when_a_later_flush_completes() {
    // Arrange: an unpromised put is lost at a recovery that kept nothing.
    let mut history = history();
    history.commit(put("k", "v1"));
    history.forget_wal_from(0);

    // Act
    history.flushed(FAMILY);
    let verdict = history.check(&BTreeMap::new(), &families());

    // Assert
    assert_eq!(verdict, Verdict::Consistent { prefix: 0 });
}

#[test]
fn should_reject_forgotten_commit_when_it_survives_anyway() {
    // Arrange
    let mut history = history();
    history.commit(put("k", "v1"));
    history.forget_wal_from(0);

    // Act
    let verdict = history.check(&observed(&[("k", "v1")]), &families());

    // Assert
    assert!(matches!(verdict, Verdict::Violation(_)), "{verdict:?}");
}

#[test]
fn should_keep_promises_on_kept_commits_when_later_ones_are_forgotten() {
    // Arrange: a synced put survives a recovery that lost the put after it.
    let mut history = history();
    let synced = history.commit(put("a", "v1"));
    history.synced(synced);
    history.commit(put("b", "v2"));
    history.forget_wal_from(1);

    // Act
    let verdict = history.check(&BTreeMap::new(), &families());

    // Assert
    assert!(matches!(verdict, Verdict::Violation(_)), "{verdict:?}");
    assert_eq!(history.len(), 1);
}

#[test]
fn should_keep_promised_commits_when_forgetting_after_the_prefix() {
    // Arrange: a flushed put survives even though the WAL kept nothing.
    let mut history = history();
    history.commit(put("k", "v1"));
    history.flushed(FAMILY);
    history.forget_wal_from(0);

    // Act
    let verdict = history.check(&observed(&[("k", "v1")]), &families());

    // Assert
    assert_eq!(verdict, Verdict::Consistent { prefix: 0 });
    assert_eq!(history.len(), 1);
}
