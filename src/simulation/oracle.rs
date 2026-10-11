//! The campaign oracle (#831): is a recovered state one the engine was
//! allowed to recover?
//!
//! Commits are recorded in the order the engine acknowledged them. A commit
//! logged in the WAL survives only with every earlier WAL record, so the WAL
//! commits that survive form a prefix of that order. A commit that bypassed
//! the WAL (best effort) or that returned an error may or may not have
//! survived on its own. A commit is *required* once something promised it:
//! a synced commit makes every earlier WAL commit durable, a completed flush
//! makes its family's earlier commits durable, and a clean shutdown makes
//! every acknowledged commit durable.
//!
//! A recovered state is consistent when one prefix length explains every key
//! at once: each key holds the value of its last required write, or of an
//! optional write after it. One prefix for all keys is what makes a recovery
//! hole (a later commit surviving an earlier lost one) a violation. The check
//! is polynomial: for each candidate prefix, each key's writes are scanned
//! once.

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

/// The commits since the last verified state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct History {
    base: BTreeMap<Key, Vec<u8>>,
    families: BTreeSet<String>,
    commits: Vec<Commit>,
    required: BTreeSet<usize>,
}

impl History {
    /// Start from a verified state and the families it holds.
    #[must_use]
    pub fn new(base: BTreeMap<Key, Vec<u8>>, families: BTreeSet<String>) -> Self {
        Self {
            base,
            families,
            commits: Vec::new(),
            required: BTreeSet::new(),
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
            }
        }
    }

    /// A flush of `family` completed: its acknowledged commits so far must
    /// survive.
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

    /// The values `key` may hold under `prefix`.
    fn allowed(&self, key: &Key, prefix: usize) -> Vec<Option<Vec<u8>>> {
        let mut allowed = vec![self.base.get(key).cloned()];
        for (index, commit) in self.commits.iter().enumerate() {
            if commit.family != key.0 {
                continue;
            }
            for (written, value) in &commit.writes {
                if *written != key.1 {
                    continue;
                }
                match self.status(index, prefix) {
                    Status::Applied => allowed = vec![value.clone()],
                    Status::Optional => allowed.push(value.clone()),
                    Status::Excluded => {}
                }
            }
        }
        allowed
    }

    /// Judge a recovered state: the value of every key the history names, and
    /// the families recovery reopened.
    #[must_use]
    pub fn check(&self, observed: &BTreeMap<Key, Vec<u8>>, families: &BTreeSet<String>) -> Verdict {
        let lost_families: Vec<String> = self.families.difference(families).cloned().collect();
        let shortest = self
            .required
            .iter()
            .filter(|index| self.commits[**index].logging == Logging::Wal)
            .max()
            .map_or(0, |index| index + 1);
        let keys = self.keys();
        let mut best: Option<Violation> = None;
        for prefix in shortest..=self.commits.len() {
            let failing: Vec<Mismatch> = keys
                .iter()
                .filter(|key| !lost_families.contains(&key.0))
                .filter_map(|key| {
                    let seen = observed.get(key).cloned();
                    let allowed = self.allowed(key, prefix);
                    (!allowed.contains(&seen)).then(|| Mismatch {
                        key: key.clone(),
                        observed: seen,
                        allowed,
                    })
                })
                .collect();
            if failing.is_empty() && lost_families.is_empty() {
                return Verdict::Consistent { prefix };
            }
            if best
                .as_ref()
                .is_none_or(|best| failing.len() < best.keys.len())
            {
                best = Some(Violation {
                    prefix,
                    keys: failing,
                    lost_families: lost_families.clone(),
                });
            }
        }
        Verdict::Violation(best.unwrap_or(Violation {
            prefix: shortest,
            keys: Vec::new(),
            lost_families,
        }))
    }
}

#[cfg(test)]
mod tests;
