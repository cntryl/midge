//! Independent logical oracle. It deliberately does not use engine internals.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub type State = BTreeMap<Vec<u8>, Vec<u8>>;
pub type Rows = Vec<(Vec<u8>, Vec<u8>)>;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Intent {
    Put { key: Vec<u8>, byte: u8, length: u16 },
    Delete { key: Vec<u8> },
    DeleteRange { start: Vec<u8>, end: Vec<u8> },
}

impl Intent {
    pub fn apply(&self, state: &mut State) {
        match self {
            Self::Put { key, byte, length } => {
                state.insert(key.clone(), vec![*byte; usize::from(*length)]);
            }
            Self::Delete { key } => {
                state.remove(key);
            }
            Self::DeleteRange { start, end } => {
                state.retain(|key, _| key < start || key >= end);
            }
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Scan {
    pub start: Option<Vec<u8>>,
    pub end: Option<Vec<u8>>,
    pub prefix: Option<Vec<u8>>,
    pub reverse: bool,
    pub limit: Option<usize>,
}

impl Scan {
    pub fn evaluate(&self, state: &State) -> Rows {
        // Filter the explicit contract directly. Do not reuse Query's bound
        // normalization or prefix-successor implementation in the oracle.
        let mut rows: Rows = state
            .iter()
            .filter(|(key, _)| {
                self.start.as_ref().is_none_or(|start| *key >= start)
                    && self.end.as_ref().is_none_or(|end| *key < end)
                    && self
                        .prefix
                        .as_ref()
                        .is_none_or(|prefix| key.starts_with(prefix))
            })
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        if self.reverse {
            rows.reverse();
        }
        if let Some(limit) = self.limit {
            rows.truncate(limit);
        }
        rows
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Action {
    Begin(u8),
    Write(u8, Intent),
    Read(u8, Vec<u8>),
    Scan(u8, Scan),
    Commit(u8),
    Rollback(u8),
    Flush,
    Compact,
    Restart,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum Observation {
    None,
    Value(Option<Vec<u8>>),
    Rows(Rows),
}

struct Transaction {
    snapshot: State,
    intents: Vec<Intent>,
}

impl Transaction {
    fn view(&self) -> State {
        let mut state = self.snapshot.clone();
        for intent in &self.intents {
            intent.apply(&mut state);
        }
        state
    }
}

#[derive(Default)]
pub struct Model {
    pub committed: State,
    transactions: BTreeMap<u8, Transaction>,
}

impl Model {
    pub fn active(&self, id: u8) -> bool {
        self.transactions.contains_key(&id)
    }

    pub fn apply(&mut self, action: &Action) -> Result<Observation, String> {
        match action {
            Action::Begin(id) => {
                if *id >= 2 || self.active(*id) {
                    return Err("transaction slot unavailable".into());
                }
                self.transactions.insert(
                    *id,
                    Transaction {
                        snapshot: self.committed.clone(),
                        intents: Vec::new(),
                    },
                );
            }
            Action::Write(id, intent) => {
                if let Intent::DeleteRange { start, end } = intent {
                    if start > end {
                        return Err("reversed delete range".into());
                    }
                }
                self.transactions
                    .get_mut(id)
                    .ok_or("write without transaction")?
                    .intents
                    .push(intent.clone());
            }
            Action::Read(id, key) => {
                let tx = self
                    .transactions
                    .get(id)
                    .ok_or("read without transaction")?;
                return Ok(Observation::Value(tx.view().get(key).cloned()));
            }
            Action::Scan(id, scan) => {
                if matches!((&scan.start, &scan.end), (Some(start), Some(end)) if start > end) {
                    return Err("reversed scan bounds".into());
                }
                let tx = self
                    .transactions
                    .get(id)
                    .ok_or("scan without transaction")?;
                return Ok(Observation::Rows(scan.evaluate(&tx.view())));
            }
            Action::Commit(id) => {
                let tx = self
                    .transactions
                    .remove(id)
                    .ok_or("commit without transaction")?;
                // LastWriteWins applies accepted intents to CURRENT state.
                // Replacing current state with tx.view() loses intervening writes.
                for intent in &tx.intents {
                    intent.apply(&mut self.committed);
                }
            }
            Action::Rollback(id) => {
                self.transactions
                    .remove(id)
                    .ok_or("rollback without transaction")?;
            }
            Action::Restart => self.transactions.clear(),
            Action::Flush | Action::Compact => {}
        }
        Ok(Observation::None)
    }
}

pub fn is_legal(actions: &[Action]) -> bool {
    let mut model = Model::default();
    actions.iter().all(|action| model.apply(action).is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(key: &[u8], byte: u8) -> Intent {
        Intent::Put {
            key: key.to_vec(),
            byte,
            length: 1,
        }
    }

    #[test]
    fn should_preserve_intervening_unrelated_write_when_old_transaction_commits() {
        // Arrange
        let mut model = Model::default();
        for action in [
            Action::Begin(0),
            Action::Write(0, put(b"a", 1)),
            Action::Begin(1),
        ] {
            model.apply(&action).unwrap();
        }

        // Act
        for action in [
            Action::Write(1, put(b"b", 2)),
            Action::Commit(1),
            Action::Commit(0),
        ] {
            model.apply(&action).unwrap();
        }

        // Assert
        assert_eq!(model.committed.get(b"a".as_slice()), Some(&vec![1]));
        assert_eq!(model.committed.get(b"b".as_slice()), Some(&vec![2]));
    }

    #[test]
    fn should_apply_ordinal_intents_when_reading_or_committing_transactions() {
        // Arrange
        let mut model = Model::default();
        for action in [
            Action::Begin(0),
            Action::Begin(1),
            Action::Write(1, put(b"a", 9)),
            Action::Commit(1),
        ] {
            model.apply(&action).unwrap();
        }

        // Act
        let frozen = model.apply(&Action::Read(0, b"a".to_vec())).unwrap();
        model
            .apply(&Action::Write(
                0,
                Intent::DeleteRange {
                    start: b"a".to_vec(),
                    end: b"z".to_vec(),
                },
            ))
            .unwrap();
        model.apply(&Action::Write(0, put(b"b", 3))).unwrap();
        let own = model.apply(&Action::Scan(0, Scan::default())).unwrap();
        model.apply(&Action::Commit(0)).unwrap();

        // Assert
        assert_eq!(frozen, Observation::Value(None));
        assert_eq!(own, Observation::Rows(vec![(b"b".to_vec(), vec![3])]));
        assert_eq!(model.committed, BTreeMap::from([(b"b".to_vec(), vec![3])]));
    }

    #[test]
    fn should_discard_every_live_transaction_when_restarting() {
        // Arrange
        let mut model = Model::default();
        model.apply(&Action::Begin(0)).unwrap();
        model
            .apply(&Action::Write(0, put(b"uncommitted", 7)))
            .unwrap();

        // Act
        model.apply(&Action::Restart).unwrap();

        // Assert
        assert!(!model.active(0));
        assert_eq!(model.committed.len(), 0);
        assert!(model.apply(&Action::Commit(0)).is_err());
    }

    #[test]
    fn should_apply_binary_scan_contract_before_reverse_limit() {
        // Arrange
        let state = BTreeMap::from([
            (vec![255], vec![1]),
            (vec![255, 0], vec![2]),
            (vec![255, 255], vec![3]),
        ]);
        let scan = Scan {
            start: Some(vec![255, 0]),
            prefix: Some(vec![255]),
            reverse: true,
            limit: Some(1),
            ..Scan::default()
        };

        // Act
        let rows = scan.evaluate(&state);

        // Assert
        assert_eq!(rows, vec![(vec![255, 255], vec![3])]);
    }
}
