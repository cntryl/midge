//! The WAL segment ledger (#808): the single authority for which segment
//! files are history, their order, the next segment id, and the retirement
//! point (`log_start`).
//!
//! Segments are `wal/{id:020}.wal` files. The ledger's members are the
//! segments recovery replays, oldest first. The newest member is the one open
//! segment; every other member is sealed. Every segment below `log_start` is
//! retired, and only a prefix ever retires. Member ids strictly increase, and
//! they are consecutive except across a gap that [`LedgerEdit::SetAside`]
//! leaves.
//!
//! This module is pure: it performs no I/O. [`WalLedger::apply`] is its only
//! mutator, and it either applies an edit whole or rejects it and leaves the
//! ledger unchanged. `ledger_store` persists the ledger as a snapshot followed
//! by the edits applied to it.

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fmt;

/// A WAL segment id; segment `id` is the file `segment_file_name(id)`.
pub(crate) type SegmentId = u64;

/// One change to the ledger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum LedgerEdit {
    /// First edit of a new database: `holder` identity and the first open segment.
    Genesis { holder: String, open: SegmentId },
    /// Seal the open segment `seal` and open `open`, which must be `seal + 1`
    /// and equal to `next_segment_id`.
    Rotate { seal: SegmentId, open: SegmentId },
    /// Retire every segment below `to`. They must all be sealed, `to` must
    /// exceed the current log start, and `to` may not pass the open segment.
    AdvanceLogStart { to: SegmentId },
    /// Salvage: drop every member at or above `from` (it must be at or above
    /// log start), and open `open`, which must be at least `next_segment_id`.
    SetAside { from: SegmentId, open: SegmentId },
}

/// The WAL segment ledger. See the module documentation.
///
/// Every value is a state some sequence of edits reaches from
/// [`WalLedger::empty`]: deserializing an unreachable state fails.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(into = "LedgerImage", try_from = "LedgerImage")]
pub(crate) struct WalLedger {
    /// Identity of the database the ledger belongs to; empty until genesis.
    holder: String,
    /// Strictly increasing; the last is the open segment. Empty until genesis.
    members: VecDeque<SegmentId>,
    /// The id the next rotation opens: always one past the open segment.
    /// Zero until genesis.
    next_segment_id: SegmentId,
}

/// Why an edit cannot apply, or why a serialized ledger is not a reachable
/// state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LedgerError {
    /// Only `Genesis` applies to an uninitialized ledger.
    NotInitialized,
    /// `Genesis` applies only to an uninitialized ledger.
    AlreadyInitialized,
    /// `Genesis` needs a non-empty holder identity.
    EmptyHolder,
    /// `Rotate` must seal the open segment.
    NotOpenSegment {
        seal: SegmentId,
        open_segment: SegmentId,
    },
    /// `Rotate` must open the next segment id, one past the sealed one.
    NotNextSegment {
        open: SegmentId,
        next_segment_id: SegmentId,
    },
    /// `AdvanceLogStart` must move past the log start without passing the
    /// open segment.
    BadLogStart {
        to: SegmentId,
        log_start: SegmentId,
        open_segment: SegmentId,
    },
    /// `SetAside` must start at or above the log start and open an id that
    /// was never allocated.
    BadSetAside {
        from: SegmentId,
        open: SegmentId,
        log_start: SegmentId,
        next_segment_id: SegmentId,
    },
    /// The edit would open the last representable id, leaving no next id.
    SegmentIdsExhausted,
    /// A serialized ledger is not a state any sequence of edits reaches.
    UnreachableState(&'static str),
}

impl fmt::Display for LedgerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotInitialized => f.write_str("the WAL ledger is not initialized"),
            Self::AlreadyInitialized => f.write_str("the WAL ledger is already initialized"),
            Self::EmptyHolder => f.write_str("WAL ledger genesis needs a holder identity"),
            Self::NotOpenSegment { seal, open_segment } => write!(
                f,
                "cannot seal WAL segment {seal}: the open segment is {open_segment}"
            ),
            Self::NotNextSegment {
                open,
                next_segment_id,
            } => write!(
                f,
                "cannot open WAL segment {open}: the next segment id is {next_segment_id}"
            ),
            Self::BadLogStart {
                to,
                log_start,
                open_segment,
            } => write!(
                f,
                "cannot advance the WAL log start to {to}: it must exceed {log_start} \
                 and not pass the open segment {open_segment}"
            ),
            Self::BadSetAside {
                from,
                open,
                log_start,
                next_segment_id,
            } => write!(
                f,
                "cannot set aside WAL segments from {from} opening {open}: from must be \
                 at least the log start {log_start} and open at least the next segment \
                 id {next_segment_id}"
            ),
            Self::SegmentIdsExhausted => f.write_str("WAL segment ids are exhausted"),
            Self::UnreachableState(reason) => {
                write!(f, "the WAL ledger state is unreachable: {reason}")
            }
        }
    }
}

impl std::error::Error for LedgerError {}

impl From<LedgerError> for crate::common::MidgeError {
    fn from(error: LedgerError) -> Self {
        Self::Corruption(format!("WAL ledger: {error}"))
    }
}

impl WalLedger {
    /// An uninitialized ledger; only `Genesis` applies to it.
    pub(crate) fn empty() -> Self {
        Self {
            holder: String::new(),
            members: VecDeque::new(),
            next_segment_id: 0,
        }
    }

    /// Apply one edit, or return why it is invalid without changing anything.
    pub(crate) fn apply(&mut self, edit: &LedgerEdit) -> Result<(), LedgerError> {
        match edit {
            LedgerEdit::Genesis { holder, open } => self.genesis(holder, *open),
            LedgerEdit::Rotate { seal, open } => self.rotate(*seal, *open),
            LedgerEdit::AdvanceLogStart { to } => self.advance_log_start(*to),
            LedgerEdit::SetAside { from, open } => self.set_aside(*from, *open),
        }
    }

    // Each edit validates everything before its first mutation, so a
    // rejected edit leaves the ledger as it was.

    fn genesis(&mut self, holder: &str, open: SegmentId) -> Result<(), LedgerError> {
        if self.is_initialized() {
            return Err(LedgerError::AlreadyInitialized);
        }
        if holder.is_empty() {
            return Err(LedgerError::EmptyHolder);
        }
        let next = following(open)?;
        holder.clone_into(&mut self.holder);
        self.members = VecDeque::from([open]);
        self.next_segment_id = next;
        Ok(())
    }

    fn rotate(&mut self, seal: SegmentId, open: SegmentId) -> Result<(), LedgerError> {
        let open_segment = self.initialized_open_segment()?;
        if seal != open_segment {
            return Err(LedgerError::NotOpenSegment { seal, open_segment });
        }
        if open != self.next_segment_id || seal.checked_add(1) != Some(open) {
            return Err(LedgerError::NotNextSegment {
                open,
                next_segment_id: self.next_segment_id,
            });
        }
        let next = following(open)?;
        self.members.push_back(open);
        self.next_segment_id = next;
        Ok(())
    }

    fn advance_log_start(&mut self, to: SegmentId) -> Result<(), LedgerError> {
        let open_segment = self.initialized_open_segment()?;
        let log_start = self.log_start();
        if to <= log_start || to > open_segment {
            return Err(LedgerError::BadLogStart {
                to,
                log_start,
                open_segment,
            });
        }
        // `to` does not pass the open segment, so only sealed members retire.
        let retired = self.members.partition_point(|id| *id < to);
        self.members.drain(..retired);
        Ok(())
    }

    fn set_aside(&mut self, from: SegmentId, open: SegmentId) -> Result<(), LedgerError> {
        self.initialized_open_segment()?;
        let log_start = self.log_start();
        if from < log_start || open < self.next_segment_id {
            return Err(LedgerError::BadSetAside {
                from,
                open,
                log_start,
                next_segment_id: self.next_segment_id,
            });
        }
        let next = following(open)?;
        // Every member is below `next_segment_id`, so `open` exceeds the kept ones.
        let kept = self.members.partition_point(|id| *id < from);
        self.members.truncate(kept);
        self.members.push_back(open);
        self.next_segment_id = next;
        Ok(())
    }

    fn initialized_open_segment(&self) -> Result<SegmentId, LedgerError> {
        self.members
            .back()
            .copied()
            .ok_or(LedgerError::NotInitialized)
    }

    /// Whether `Genesis` has applied.
    pub(crate) fn is_initialized(&self) -> bool {
        !self.members.is_empty()
    }

    /// The holder identity `Genesis` recorded; empty when uninitialized.
    pub(crate) fn holder(&self) -> &str {
        &self.holder
    }

    /// The open segment, the newest member; zero when uninitialized.
    pub(crate) fn open_segment(&self) -> SegmentId {
        self.members.back().copied().unwrap_or(0)
    }

    /// The id the next rotation opens; zero when uninitialized.
    pub(crate) fn next_segment_id(&self) -> SegmentId {
        self.next_segment_id
    }

    /// The oldest member's id; every segment below it is retired. Zero when
    /// uninitialized.
    pub(crate) fn log_start(&self) -> SegmentId {
        self.members.front().copied().unwrap_or(0)
    }

    /// Members to replay, oldest first; the last is the open segment.
    pub(crate) fn members(&self) -> impl Iterator<Item = SegmentId> + '_ {
        self.members.iter().copied()
    }

    /// Sealed members, oldest first.
    pub(crate) fn sealed(&self) -> impl Iterator<Item = SegmentId> + '_ {
        self.members
            .iter()
            .copied()
            .take(self.members.len().saturating_sub(1))
    }

    /// Whether segment `id` is history recovery replays.
    pub(crate) fn is_member(&self, id: SegmentId) -> bool {
        self.members.binary_search(&id).is_ok()
    }
}

/// The id after `open`, which the ledger records as `next_segment_id`.
fn following(open: SegmentId) -> Result<SegmentId, LedgerError> {
    open.checked_add(1).ok_or(LedgerError::SegmentIdsExhausted)
}

/// The serialized form of a [`WalLedger`], validated on the way in.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LedgerImage {
    holder: String,
    members: Vec<SegmentId>,
    next_segment_id: SegmentId,
}

impl From<WalLedger> for LedgerImage {
    fn from(ledger: WalLedger) -> Self {
        Self {
            holder: ledger.holder,
            members: ledger.members.into(),
            next_segment_id: ledger.next_segment_id,
        }
    }
}

impl TryFrom<LedgerImage> for WalLedger {
    type Error = LedgerError;

    fn try_from(image: LedgerImage) -> Result<Self, Self::Error> {
        let LedgerImage {
            holder,
            members,
            next_segment_id,
        } = image;
        let Some(&open) = members.last() else {
            if holder.is_empty() && next_segment_id == 0 {
                return Ok(Self::empty());
            }
            return Err(LedgerError::UnreachableState(
                "an uninitialized ledger carries state",
            ));
        };
        if holder.is_empty() {
            return Err(LedgerError::UnreachableState("the holder is empty"));
        }
        if members.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(LedgerError::UnreachableState(
                "members are not strictly increasing",
            ));
        }
        if open.checked_add(1) != Some(next_segment_id) {
            return Err(LedgerError::UnreachableState(
                "the next segment id does not follow the open segment",
            ));
        }
        Ok(Self {
            holder,
            members: members.into(),
            next_segment_id,
        })
    }
}

#[cfg(test)]
mod tests;
