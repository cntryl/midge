//! The campaign oracle (#831): is a recovered state one the engine was
//! allowed to recover?
//!
//! Commits are recorded in the order the engine acknowledged them. midge has
//! one WAL for every family, and a WAL record survives only with every
//! earlier one, so the WAL commits that survive form a prefix of that order.
//! A commit that bypassed the WAL (best effort) or that returned an error may
//! or may not have survived on its own. A commit becomes required when
//! something promised it:
//!
//! - a synced commit makes every earlier WAL commit durable, which raises the
//!   shortest prefix recovery may keep;
//! - a completed flush makes its own family's commits durable in an SST. It
//!   promises nothing about other families, so it does not raise the prefix:
//!   the engine does not promise cross-family ordering for unsynced writes;
//! - a clean shutdown makes every acknowledged commit durable.
//!
//! A recovered state is consistent when one prefix length explains every key
//! at once: each key holds the value of its last required write, or of an
//! optional write after it. One prefix for all keys is what makes a recovery
//! hole (a later commit surviving an earlier lost one) a violation. Keys the
//! history never wrote must be absent, so a resurrection is caught however
//! long ago its delete was verified. A commit's keys are judged one by one,
//! so a commit torn across several keys is not detected; campaigns write one
//! key per commit.
//!
//! The check is incremental: moving the prefix by one commit changes only
//! that commit's keys, so judging every prefix costs one pass over the
//! history plus each key's writes once per change.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// A key in one column family.
pub type Key = (String, Vec<u8>);

/// Whether a commit went through the WAL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Logging {
    Wal,
    /// Best effort: memtable only until a flush.
    Unlogged,
}

/// One commit, in the order the engine returned it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Commit {
    pub family: String,
    /// Each written key, with its new value or `None` for a delete.
    pub writes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    pub logging: Logging,
    /// The engine reported success; otherwise the outcome is unknown.
    pub acknowledged: bool,
}

/// A key no allowed history explains.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mismatch {
    pub key: Key,
    /// What recovery returned.
    pub observed: Option<Vec<u8>>,
    /// What recovery could have returned.
    pub allowed: Vec<Option<Vec<u8>>>,
}

/// Why a recovered state is not one the history allows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Violation {
    /// The surviving WAL prefix that explains the most keys.
    pub prefix: usize,
    /// Keys no allowed history explains under that prefix.
    pub keys: Vec<Mismatch>,
    /// Column families the history requires but recovery lost.
    pub lost_families: Vec<String>,
}

/// The oracle's judgement of one recovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Verdict {
    /// Explained by keeping this many commits of the WAL order.
    Consistent {
        prefix: usize,
    },
    Violation(Violation),
}

/// What a commit's write may contribute under one candidate prefix.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Status {
    Applied,
    Optional,
    Excluded,
}

/// Maps keyed by [`Key`] serialize as lists of pairs, which JSON can hold.
mod pairs {
    use super::Key;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::BTreeMap;

    pub(super) fn serialize<S: Serializer>(
        map: &BTreeMap<Key, Vec<u8>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        map.iter().collect::<Vec<_>>().serialize(serializer)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<Key, Vec<u8>>, D::Error> {
        Ok(Vec::<(Key, Vec<u8>)>::deserialize(deserializer)?
            .into_iter()
            .collect())
    }
}

/// The commits since the last verified state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct History {
    #[serde(with = "pairs")]
    base: BTreeMap<Key, Vec<u8>>,
    families: BTreeSet<String>,
    commits: Vec<Commit>,
    /// Commits that must survive, whatever the prefix.
    required: BTreeSet<usize>,
    /// Required WAL commits that also bound the prefix from below.
    prefix_floor: BTreeSet<usize>,
}

impl History {
    /// Start from a verified state and the families it holds.
    #[must_use]
    pub fn new(base: BTreeMap<Key, Vec<u8>>, families: BTreeSet<String>) -> Self {
        Self {
            base,
            families,
            ..Self::default()
        }
    }

    /// Every key the history writes or starts with.
    #[must_use]
    pub fn keys(&self) -> BTreeSet<Key> {
        self.base
            .keys()
            .cloned()
            .chain(self.commits.iter().flat_map(|commit| {
                commit
                    .writes
                    .iter()
                    .map(|(key, _)| (commit.family.clone(), key.clone()))
            }))
            .collect()
    }

    /// Families the engine acknowledged creating.
    #[must_use]
    pub fn families(&self) -> &BTreeSet<String> {
        &self.families
    }

    /// A family's creation was acknowledged; it must survive.
    pub fn create_family(&mut self, family: &str) {
        self.families.insert(family.to_string());
    }

    /// Record a commit; returns its index.
    pub fn commit(&mut self, commit: Commit) -> usize {
        self.commits.push(commit);
        self.commits.len() - 1
    }

    /// The commit at `index` was synced: it and every earlier acknowledged
    /// WAL commit must survive.
    pub fn synced(&mut self, index: usize) {
        for (position, commit) in self.commits.iter().enumerate().take(index + 1) {
            if commit.acknowledged && commit.logging == Logging::Wal {
                self.required.insert(position);
                self.prefix_floor.insert(position);
            }
        }
    }

    /// A flush of `family` completed: its acknowledged commits so far must
    /// survive, in an SST, whatever the WAL keeps.
    pub fn flushed(&mut self, family: &str) {
        for (position, commit) in self.commits.iter().enumerate() {
            if commit.acknowledged && commit.family == family {
                self.required.insert(position);
            }
        }
    }

    /// The engine shut down cleanly: every acknowledged commit must survive.
    pub fn shut_down(&mut self) {
        for (position, commit) in self.commits.iter().enumerate() {
            if commit.acknowledged {
                self.required.insert(position);
                if commit.logging == Logging::Wal {
                    self.prefix_floor.insert(position);
                }
            }
        }
    }

    fn status(&self, index: usize, prefix: usize) -> Status {
        let commit = &self.commits[index];
        if self.required.contains(&index) {
            Status::Applied
        } else if commit.logging == Logging::Unlogged {
            Status::Optional
        } else if index >= prefix {
            Status::Excluded
        } else if commit.acknowledged {
            Status::Applied
        } else {
            Status::Optional
        }
    }

    /// The values `key` may hold under `prefix`, given the indices of the
    /// commits that write it.
    fn allowed(&self, key: &Key, writers: &[usize], prefix: usize) -> Vec<Option<Vec<u8>>> {
        let mut allowed = vec![self.base.get(key).cloned()];
        for &index in writers {
            let value = self.commits[index]
                .writes
                .iter()
                .filter(|(written, _)| *written == key.1)
                .map(|(_, value)| value.clone())
                .next_back()
                .flatten();
            match self.status(index, prefix) {
                Status::Applied => allowed = vec![value],
                Status::Optional => allowed.push(value),
                Status::Excluded => {}
            }
        }
        allowed
    }

    /// Judge a recovered state: every key recovery holds, and the families it
    /// reopened. Keys the history names but recovery lacks read as absent.
    #[must_use]
    pub fn check(&self, observed: &BTreeMap<Key, Vec<u8>>, families: &BTreeSet<String>) -> Verdict {
        let lost_families: Vec<String> = self.families.difference(families).cloned().collect();
        let mut writers: BTreeMap<Key, Vec<usize>> = self
            .keys()
            .into_iter()
            .chain(observed.keys().cloned())
            .filter(|key| !lost_families.contains(&key.0))
            .map(|key| (key, Vec::new()))
            .collect();
        for (index, commit) in self.commits.iter().enumerate() {
            for (key, _) in &commit.writes {
                if let Some(indices) = writers.get_mut(&(commit.family.clone(), key.clone())) {
                    if indices.last() != Some(&index) {
                        indices.push(index);
                    }
                }
            }
        }
        let fails = |key: &Key, prefix: usize| {
            !self
                .allowed(key, &writers[key], prefix)
                .contains(&observed.get(key).cloned())
        };
        let shortest = self.prefix_floor.iter().max().map_or(0, |index| index + 1);
        let mut failing: BTreeSet<Key> = writers
            .keys()
            .filter(|key| fails(key, shortest))
            .cloned()
            .collect();
        let mut best = (shortest, failing.len());
        for prefix in shortest..=self.commits.len() {
            if prefix > shortest {
                // Only the commit the prefix now takes in changes status.
                let entered = &self.commits[prefix - 1];
                for (key, _) in &entered.writes {
                    let key = (entered.family.clone(), key.clone());
                    if !writers.contains_key(&key) {
                        continue;
                    }
                    if fails(&key, prefix) {
                        failing.insert(key);
                    } else {
                        failing.remove(&key);
                    }
                }
            }
            if failing.is_empty() && lost_families.is_empty() {
                return Verdict::Consistent { prefix };
            }
            if failing.len() < best.1 {
                best = (prefix, failing.len());
            }
        }
        let prefix = best.0;
        Verdict::Violation(Violation {
            prefix,
            keys: writers
                .keys()
                .filter(|key| fails(key, prefix))
                .map(|key| Mismatch {
                    key: key.clone(),
                    observed: observed.get(key).cloned(),
                    allowed: self.allowed(key, &writers[key], prefix),
                })
                .collect(),
            lost_families,
        })
    }
}

#[cfg(test)]
mod tests;
