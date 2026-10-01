use super::model::{Action, Intent, Model, Scan};
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::{Config, RngAlgorithm, RngSeed, TestRunner};
use serde::{Deserialize, Serialize};

pub const SEED: u64 = 0x4d49_4447_4530_3331;

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub enum Template {
    DurableRestart,
    HeldSnapshot,
    OrdinalIntents,
    BoundedScan,
    CompactionEquivalence,
    PinnedCompaction,
    BinaryBoundaries,
}

const TEMPLATES: [Template; 7] = [
    Template::DurableRestart,
    Template::HeldSnapshot,
    Template::OrdinalIntents,
    Template::BoundedScan,
    Template::CompactionEquivalence,
    Template::PinnedCompaction,
    Template::BinaryBoundaries,
];

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct History {
    pub ordinal: usize,
    pub template: Template,
    pub actions: Vec<Action>,
}

fn keys() -> BoxedStrategy<Vec<u8>> {
    prop_oneof![
        Just(Vec::new()),
        Just(vec![0]),
        Just(vec![0, 255]),
        Just(vec![255]),
        Just(vec![255, 255]),
        proptest::collection::vec(any::<u8>(), 0..=8),
    ]
    .boxed()
}

fn put(id: u8, key: Vec<u8>, byte: u8) -> Action {
    // A concrete compact value recipe keeps the corpus small. Replay always
    // expands exactly these bytes; the backend never affects generation.
    Action::Write(
        id,
        Intent::Put {
            key,
            byte,
            length: 1024,
        },
    )
}

fn palette() -> Vec<Vec<u8>> {
    vec![
        vec![],
        vec![0],
        vec![0, 0],
        vec![0, 255],
        vec![1],
        vec![127],
        vec![128],
        vec![255],
        vec![255, 0],
        vec![255, 255],
        vec![255, 255, 0],
        b"a".to_vec(),
        b"aa".to_vec(),
        b"b".to_vec(),
        b"z".to_vec(),
        b"zz".to_vec(),
    ]
}

fn scan(id: u8, reverse: bool) -> Action {
    Action::Scan(
        id,
        Scan {
            reverse,
            ..Scan::default()
        },
    )
}

fn bounded_scan_actions(template: Template) -> Vec<Action> {
    let mut actions = vec![Action::Begin(0)];
    if matches!(template, Template::BoundedScan) {
        actions.extend([
            Action::Write(
                0,
                Intent::DeleteRange {
                    start: vec![0],
                    end: vec![255, 255],
                },
            ),
            Action::Write(
                0,
                Intent::DeleteRange {
                    start: b"a".to_vec(),
                    end: b"z".to_vec(),
                },
            ),
            put(0, vec![255, 0], 96),
        ]);
    }
    for reverse in [false, true] {
        for prefix in [vec![], vec![0], vec![255], vec![255, 255]] {
            actions.push(Action::Scan(
                0,
                Scan {
                    start: Some(vec![0]),
                    end: None,
                    prefix: Some(prefix),
                    reverse,
                    limit: Some(3),
                },
            ));
        }
        actions.push(Action::Scan(
            0,
            Scan {
                start: Some(b"a".to_vec()),
                end: Some(b"z".to_vec()),
                reverse,
                limit: Some(0),
                prefix: None,
            },
        ));
    }
    actions.push(Action::Commit(0));
    actions
}

fn prefix(template: Template) -> Vec<Action> {
    let mut actions = vec![Action::Begin(0)];
    for (byte, key) in (0_u8..).zip(palette()) {
        actions.push(put(0, key, byte));
    }
    actions.extend([scan(0, false), Action::Commit(0)]);
    match template {
        Template::DurableRestart => actions.extend([
            Action::Restart,
            Action::Begin(0),
            scan(0, false),
            Action::Rollback(0),
        ]),
        Template::HeldSnapshot | Template::PinnedCompaction => {
            if matches!(template, Template::PinnedCompaction) {
                actions.push(Action::Flush);
            }
            actions.extend([
                Action::Begin(1),
                Action::Begin(0),
                put(0, b"a".to_vec(), 93),
                Action::Write(0, Intent::Delete { key: vec![255] }),
                Action::Commit(0),
                Action::Flush,
            ]);
            if matches!(template, Template::PinnedCompaction) {
                actions.push(Action::Compact);
            }
            actions.extend([scan(1, false), scan(1, true), Action::Rollback(1)]);
        }
        Template::OrdinalIntents => actions.extend([
            Action::Begin(0),
            Action::Begin(1),
            put(1, b"other".to_vec(), 94),
            Action::Commit(1),
            Action::Write(
                0,
                Intent::DeleteRange {
                    start: vec![],
                    end: vec![255],
                },
            ),
            put(0, b"a".to_vec(), 95),
            Action::Write(0, Intent::Delete { key: b"b".to_vec() }),
            Action::Write(
                0,
                Intent::DeleteRange {
                    start: b"a".to_vec(),
                    end: b"a".to_vec(),
                },
            ),
            Action::Read(0, b"a".to_vec()),
            scan(0, false),
            scan(0, true),
            Action::Commit(0),
        ]),
        Template::BoundedScan | Template::BinaryBoundaries => {
            actions.extend(bounded_scan_actions(template));
        }
        Template::CompactionEquivalence => actions.extend([
            Action::Flush,
            Action::Begin(0),
            Action::Write(
                0,
                Intent::DeleteRange {
                    start: vec![],
                    end: vec![255, 255, 255],
                },
            ),
            Action::Commit(0),
            Action::Flush,
            Action::Compact,
            Action::Begin(0),
            scan(0, false),
            Action::Rollback(0),
        ]),
    }
    actions
}

pub fn generate(count: usize, max_operations: usize) -> Vec<History> {
    assert!((48..=256).contains(&max_operations));
    let mut runner = TestRunner::new(Config {
        rng_seed: RngSeed::Fixed(SEED),
        rng_algorithm: RngAlgorithm::ChaCha,
        failure_persistence: None,
        ..Config::default()
    });
    let raw = proptest::collection::vec(
        (any::<u8>(), 0_u8..2, keys(), keys(), any::<u8>()),
        max_operations,
    );
    (0..count)
        .map(|ordinal| {
            let template = TEMPLATES[ordinal % TEMPLATES.len()];
            let mut actions = prefix(template);
            let mut model = Model::default();
            for action in &actions {
                model.apply(action).expect("legal template prefix");
            }
            let mut maintenance = actions
                .iter()
                .filter(|action| matches!(action, Action::Flush | Action::Compact))
                .count();
            let mut restarts = actions
                .iter()
                .filter(|action| matches!(action, Action::Restart))
                .count();
            for (code, id, key, other, byte) in raw
                .new_tree(&mut runner)
                .expect("generate history")
                .current()
            {
                if actions.len() == max_operations {
                    break;
                }
                let active = model.active(id);
                let action = match code % 12 {
                    0 if !active => Action::Begin(id),
                    1..=3 if active => put(id, key, byte),
                    4 if active => Action::Write(id, Intent::Delete { key }),
                    5 if active => {
                        let (start, end) = if key <= other {
                            (key, other)
                        } else {
                            (other, key)
                        };
                        Action::Write(id, Intent::DeleteRange { start, end })
                    }
                    6 if active => Action::Read(id, key),
                    7 if active => {
                        let (start, end) = if key <= other {
                            (key, other)
                        } else {
                            (other, key)
                        };
                        Action::Scan(
                            id,
                            Scan {
                                start: Some(start),
                                end: Some(end),
                                reverse: byte % 2 != 0,
                                prefix: None,
                                limit: Some(usize::from(byte % 9)),
                            },
                        )
                    }
                    8 if active => Action::Commit(id),
                    9 if active => Action::Rollback(id),
                    10 if maintenance < 4 => {
                        maintenance += 1;
                        if byte % 2 == 0 {
                            Action::Flush
                        } else {
                            Action::Compact
                        }
                    }
                    11 if restarts < 2 => {
                        restarts += 1;
                        Action::Restart
                    }
                    _ => continue,
                };
                model.apply(&action).expect("generated legal action");
                actions.push(action);
            }
            History {
                ordinal,
                template,
                actions,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_generate_reproducible_legal_histories_before_backend_selection() {
        // Arrange
        let first = generate(32, 64);

        // Act
        let second = generate(32, 64);

        // Assert
        assert_eq!(
            serde_json::to_vec(&first).unwrap(),
            serde_json::to_vec(&second).unwrap()
        );
        assert_eq!(first.len(), 32);
        for history in first {
            assert!(super::super::model::is_legal(&history.actions));
            assert!(history.actions.len() <= 64);
            assert!(
                history
                    .actions
                    .iter()
                    .filter(|a| matches!(a, Action::Restart))
                    .count()
                    <= 2
            );
            assert!(
                history
                    .actions
                    .iter()
                    .filter(|a| matches!(a, Action::Flush | Action::Compact))
                    .count()
                    <= 4
            );
        }
    }
}
