//! Judging cloud recoveries two ways (#838).
//!
//! A recovery from a cache that survived its crash must keep what the
//! engine already served from it, so the local history is rebased on every
//! recovery. A takeover by a process with a fresh cache keeps only what the
//! cloud promised, and an asynchronous cloud write served from a surviving
//! cache never was, so the cloud history is rebased only by fresh-cache
//! takeovers and judges them alone. A recovery from a surviving cache still
//! removes the unpromised WAL commits it lost from the cloud history: they
//! left the WAL, so no later promise covers them.
//!
//! Every commit and promise goes to both histories in the same order, so the
//! local history's commits are always the cloud history's last ones, from
//! `offset` on.

use super::{Commit, History, Key, Verdict, Violation};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// What a recovery started from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Recovery {
    /// The crashed process's cache, as the crash left it.
    SurvivingCache,
    /// A new process's empty cache: only the cloud remains.
    FreshCache,
}

/// A commit's place in both histories.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitId {
    local: usize,
    cloud: usize,
}

/// The local and cloud histories of one campaign.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Histories {
    local: History,
    cloud: History,
    /// Where the local history's commits start in the cloud history.
    offset: usize,
}

impl Histories {
    /// Start from a verified state and the families it holds.
    #[must_use]
    pub fn new(base: BTreeMap<Key, Vec<u8>>, families: BTreeSet<String>) -> Self {
        Self {
            local: History::new(base.clone(), families.clone()),
            cloud: History::new(base, families),
            offset: 0,
        }
    }

    /// Record a commit in both histories.
    pub fn commit(&mut self, commit: Commit) -> CommitId {
        CommitId {
            local: self.local.commit(commit.clone()),
            cloud: self.cloud.commit(commit),
        }
    }

    /// See [`History::synced`].
    pub fn synced(&mut self, id: CommitId) {
        self.local.synced(id.local);
        self.cloud.synced(id.cloud);
    }

    /// See [`History::flushed`].
    pub fn flushed(&mut self, family: &str) {
        self.local.flushed(family);
        self.cloud.flushed(family);
    }

    /// See [`History::shut_down`].
    pub fn shut_down(&mut self) {
        self.local.shut_down();
        self.cloud.shut_down();
    }

    /// See [`History::create_family`].
    pub fn create_family(&mut self, family: &str) {
        self.local.create_family(family);
        self.cloud.create_family(family);
    }

    /// Every key the local history writes or starts with.
    #[must_use]
    pub fn local_keys(&self) -> BTreeSet<Key> {
        self.local.keys()
    }

    /// Judge a recovery, then rebase on what it served.
    ///
    /// # Errors
    ///
    /// Returns the violation when the recovered state is not one the
    /// recovery's history allows; the histories are then left unchanged.
    pub fn check(
        &mut self,
        recovery: Recovery,
        observed: BTreeMap<Key, Vec<u8>>,
        families: BTreeSet<String>,
    ) -> Result<(), Violation> {
        match recovery {
            Recovery::FreshCache => {
                if let Verdict::Violation(violation) = self.cloud.check(&observed, &families) {
                    return Err(violation);
                }
                self.cloud = History::new(observed.clone(), families.clone());
            }
            Recovery::SurvivingCache => match self.local.check(&observed, &families) {
                Verdict::Violation(violation) => return Err(violation),
                Verdict::Consistent { prefix } => self.cloud.forget_wal_from(self.offset + prefix),
            },
        }
        self.offset = self.cloud.len();
        self.local = History::new(observed, families);
        Ok(())
    }
}
