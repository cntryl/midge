//! Ordered transaction oracle, independent of WAL encodings and engine state.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Recovery obligations for one transaction under the selected failure domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Obligation {
    MustSurvive,
    MaySurvive,
    MustNotApply,
}

/// Logical effects checked by the oracle, including schema and TTL behavior.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Operation {
    CreateFamily {
        id: u32,
        name: String,
    },
    DropFamily {
        id: u32,
    },
    Put {
        family: u32,
        key: Vec<u8>,
        value: Vec<u8>,
        expires_at: Option<u64>,
    },
    Delete {
        family: u32,
        key: Vec<u8>,
    },
    DeleteRange {
        family: u32,
        start: Vec<u8>,
        end: Vec<u8>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transaction {
    pub id: u64,
    pub obligation: Obligation,
    pub operations: Vec<Operation>,
}

/// Full public observation, with stable ordering and no engine-owned counters.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct FamilyObservation {
    pub id: u32,
    pub name: String,
    pub values: Vec<(Vec<u8>, Vec<u8>)>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Value {
    bytes: Vec<u8>,
    expires_at: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Family {
    name: String,
    values: BTreeMap<Vec<u8>, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Database {
    families: BTreeMap<u32, Family>,
    used_family_ids: BTreeSet<u32>,
}

impl Default for Database {
    fn default() -> Self {
        Self {
            families: BTreeMap::from([(
                0,
                Family {
                    name: "default".into(),
                    values: BTreeMap::new(),
                },
            )]),
            used_family_ids: BTreeSet::from([0]),
        }
    }
}

impl Database {
    fn apply(&mut self, operation: &Operation) -> Result<(), String> {
        match operation {
            Operation::CreateFamily { id, name } => {
                if self.used_family_ids.contains(id)
                    || self.families.values().any(|family| family.name == *name)
                {
                    return Err("reused family ID or duplicate live name".into());
                }
                self.used_family_ids.insert(*id);
                self.families.insert(
                    *id,
                    Family {
                        name: name.clone(),
                        values: BTreeMap::new(),
                    },
                );
            }
            Operation::DropFamily { id } => {
                if *id == 0 || self.families.remove(id).is_none() {
                    return Err("invalid family drop".into());
                }
            }
            Operation::Put {
                family,
                key,
                value,
                expires_at,
            } => {
                self.families
                    .get_mut(family)
                    .ok_or("missing put family")?
                    .values
                    .insert(
                        key.clone(),
                        Value {
                            bytes: value.clone(),
                            expires_at: *expires_at,
                        },
                    );
            }
            Operation::Delete { family, key } => {
                self.families
                    .get_mut(family)
                    .ok_or("missing delete family")?
                    .values
                    .remove(key);
            }
            Operation::DeleteRange { family, start, end } => {
                if start > end {
                    return Err("reversed delete range".into());
                }
                self.families
                    .get_mut(family)
                    .ok_or("missing range family")?
                    .values
                    .retain(|key, _| key < start || key >= end);
            }
        }
        Ok(())
    }

    fn observe(&self, now: u64) -> Vec<FamilyObservation> {
        self.families
            .iter()
            .map(|(id, family)| FamilyObservation {
                id: *id,
                name: family.name.clone(),
                values: family
                    .values
                    .iter()
                    .filter(|(_, value)| value.expires_at.is_none_or(|expiry| now < expiry))
                    .map(|(key, value)| (key.clone(), value.bytes.clone()))
                    .collect(),
            })
            .collect()
    }
}

/// The bound limits ambiguous histories, never weakens the recovery obligation.
/// Exhaustion is an explicit inconclusive result rather than an accepted state.
#[derive(Debug, Clone)]
pub struct Oracle {
    transactions: Vec<Transaction>,
    max_candidates: usize,
}

impl Oracle {
    #[must_use]
    pub fn new(max_candidates: usize) -> Self {
        Self {
            transactions: Vec::new(),
            max_candidates,
        }
    }

    /// Append a transaction in authoritative logical order.
    ///
    /// # Errors
    /// Rejects duplicate transaction IDs.
    pub fn record(&mut self, transaction: Transaction) -> Result<(), String> {
        if self
            .transactions
            .iter()
            .any(|prior| prior.id == transaction.id)
        {
            return Err(format!("duplicate oracle transaction {}", transaction.id));
        }
        self.transactions.push(transaction);
        Ok(())
    }

    /// Check whether the complete observation comes from an allowed ordered
    /// history. Optional transactions are atomic; per-key mixtures are rejected.
    ///
    /// # Errors
    /// Returns a counterexample, invalid-history error, or explicit capacity error.
    pub fn verify(&self, observed: &[FamilyObservation], now: u64) -> Result<(), String> {
        if self.max_candidates == 0 {
            return Err("oracle candidate capacity is zero".into());
        }
        let mut candidates = BTreeSet::from([Database::default()]);
        for transaction in &self.transactions {
            if transaction.obligation == Obligation::MustNotApply {
                continue;
            }
            let mut next = BTreeSet::new();
            for candidate in &candidates {
                if transaction.obligation == Obligation::MaySurvive {
                    next.insert(candidate.clone());
                }
                let mut applied = candidate.clone();
                if transaction
                    .operations
                    .iter()
                    .all(|operation| applied.apply(operation).is_ok())
                {
                    next.insert(applied);
                }
                if next.len() > self.max_candidates {
                    return Err(format!(
                        "oracle capacity exhausted at transaction {}",
                        transaction.id
                    ));
                }
            }
            if next.is_empty() {
                return Err(format!(
                    "invalid ordered history at transaction {}",
                    transaction.id
                ));
            }
            candidates = next;
        }
        if candidates
            .iter()
            .any(|candidate| candidate.observe(now) == observed)
        {
            Ok(())
        } else {
            Err(format!(
                "recovered state violates transaction obligations: {observed:?}"
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(key: &[u8], value: &[u8]) -> Operation {
        Operation::Put {
            family: 0,
            key: key.into(),
            value: value.into(),
            expires_at: None,
        }
    }
    fn observation(values: &[(&[u8], &[u8])]) -> Vec<FamilyObservation> {
        vec![FamilyObservation {
            id: 0,
            name: "default".into(),
            values: values
                .iter()
                .map(|(key, value)| (key.to_vec(), value.to_vec()))
                .collect(),
        }]
    }

    #[test]
    fn should_reject_partial_optional_transaction_when_only_one_member_survives() {
        // Arrange
        let mut oracle = Oracle::new(16);
        oracle
            .record(Transaction {
                id: 1,
                obligation: Obligation::MaySurvive,
                operations: vec![put(b"a", b"1"), put(b"b", b"2")],
            })
            .unwrap();
        // Act
        // Assert
        assert!(oracle.verify(&observation(&[]), 0).is_ok());
        assert!(oracle
            .verify(&observation(&[(b"a", b"1"), (b"b", b"2")]), 0)
            .is_ok());
        assert!(oracle.verify(&observation(&[(b"a", b"1")]), 0).is_err());
    }

    #[test]
    fn should_reject_failed_transaction_effect_when_outcome_proves_not_applied() {
        // Arrange
        let mut oracle = Oracle::new(16);
        oracle
            .record(Transaction {
                id: 1,
                obligation: Obligation::MustSurvive,
                operations: vec![put(b"a", b"1")],
            })
            .unwrap();
        oracle
            .record(Transaction {
                id: 2,
                obligation: Obligation::MustNotApply,
                operations: vec![Operation::Delete {
                    family: 0,
                    key: b"a".into(),
                }],
            })
            .unwrap();
        // Act
        // Assert
        assert!(oracle.verify(&observation(&[(b"a", b"1")]), 0).is_ok());
        assert!(oracle.verify(&observation(&[]), 0).is_err());
    }

    #[test]
    fn should_match_visible_state_when_history_contains_ttl_range_delete_and_family_drop() {
        // Arrange
        let mut oracle = Oracle::new(16);
        oracle
            .record(Transaction {
                id: 1,
                obligation: Obligation::MustSurvive,
                operations: vec![
                    put(b"a", b"1"),
                    put(b"b", b"2"),
                    Operation::Put {
                        family: 0,
                        key: b"c".into(),
                        value: b"3".into(),
                        expires_at: Some(10),
                    },
                    Operation::CreateFamily {
                        id: 8,
                        name: "ephemeral".into(),
                    },
                    Operation::Put {
                        family: 8,
                        key: b"d".into(),
                        value: b"4".into(),
                        expires_at: None,
                    },
                ],
            })
            .unwrap();
        oracle
            .record(Transaction {
                id: 2,
                obligation: Obligation::MustSurvive,
                operations: vec![
                    Operation::DeleteRange {
                        family: 0,
                        start: b"a".into(),
                        end: b"b".into(),
                    },
                    Operation::DropFamily { id: 8 },
                ],
            })
            .unwrap();
        // Act
        // Assert
        assert!(oracle
            .verify(&observation(&[(b"b", b"2"), (b"c", b"3")]), 9)
            .is_ok());
        assert!(oracle.verify(&observation(&[(b"b", b"2")]), 10).is_ok());
        assert!(oracle
            .verify(&observation(&[(b"a", b"1"), (b"b", b"2")]), 10)
            .is_err());
    }

    #[test]
    fn should_report_inconclusive_when_ambiguous_history_exceeds_oracle_capacity() {
        // Arrange
        let mut oracle = Oracle::new(1);
        oracle
            .record(Transaction {
                id: 1,
                obligation: Obligation::MaySurvive,
                operations: vec![put(b"a", b"1")],
            })
            .unwrap();
        // Act
        let error = oracle.verify(&observation(&[]), 0).unwrap_err();
        // Assert
        assert!(error.contains("capacity exhausted"));
    }
}
