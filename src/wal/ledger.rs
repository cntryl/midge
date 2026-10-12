//! The WAL segment ledger (#808): the single authority for which segment
//! files are history, their order, how they chain, the next segment id, the
//! retirement point (`log_start`) and the sequence reservation.
//!
//! Segments are `wal/{id:020}.wal` files (`wal::format`). The ledger's
//! members are the segments recovery replays, oldest first. The newest
//! member is the one open segment; every other member is sealed, and the
//! ledger records what its footer seals ([`SealedFacts`]). Each member
//! records what its header holds ([`OpenFacts`]); its `prev` is the link of
//! the member before it, or the `anchor` for the first one. The anchor is
//! the link of the last retired segment, so a chain stays verifiable however
//! much of it retires. Every segment below `log_start` is retired, and only
//! a prefix ever retires. Member ids strictly increase; they skip ids across
//! a gap [`LedgerEdit::SetAside`] leaves and the ids [`LedgerEdit::Reopen`]
//! replaces.
//!
//! This module is pure: it performs no I/O. [`WalLedger::apply`] is its only
//! mutator, and it either applies an edit whole or rejects it and leaves the
//! ledger unchanged. `ledger_store` persists the ledger as a snapshot followed
//! by the edits applied to it.

use crate::wal::format::{ChainLink, SealedFacts};
pub(crate) use crate::wal::format::{OpenFacts, SegmentId};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fmt;

/// The identity of the database a ledger belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(into = "String", try_from = "String")]
pub(crate) struct HolderId(uuid::Uuid);

impl HolderId {
    /// A new, random identity.
    pub(crate) fn new() -> Self {
        Self(uuid::Uuid::new_v4())
    }

    /// The identity segment headers store as `bytes`; `None` for the nil
    /// identity.
    #[cfg(test)]
    pub(crate) fn from_bytes(bytes: [u8; 16]) -> Option<Self> {
        let uuid = uuid::Uuid::from_bytes(bytes);
        (!uuid.is_nil()).then_some(Self(uuid))
    }

    /// The identity as segment headers store it.
    pub(crate) fn bytes(self) -> [u8; 16] {
        *self.0.as_bytes()
    }
}

impl From<HolderId> for String {
    fn from(holder: HolderId) -> Self {
        holder.0.to_string()
    }
}

impl TryFrom<String> for HolderId {
    type Error = String;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        let uuid = uuid::Uuid::parse_str(&text)
            .map_err(|error| format!("invalid WAL ledger holder {text:?}: {error}"))?;
        if uuid.is_nil() {
            return Err("the WAL ledger holder is nil".to_string());
        }
        Ok(Self(uuid))
    }
}

/// One ledger member: its header facts and, once sealed, its footer facts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Member {
    pub(crate) open: OpenFacts,
    pub(crate) sealed: Option<SealedFacts>,
}

impl Member {
    pub(crate) fn id(&self) -> SegmentId {
        self.open.id
    }

    fn opened(open: OpenFacts) -> Self {
        Self { open, sealed: None }
    }
}

/// One change to the ledger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum LedgerEdit {
    /// Seal the open member with `sealed` and open `open`, which takes
    /// `next_segment_id` and links `sealed`.
    Rotate {
        sealed: SealedFacts,
        open: OpenFacts,
    },
    /// Replace the open member, which holds no record, with `open`: it takes
    /// `next_segment_id`, keeps the replaced member's `prev`, and may raise
    /// the epoch and first sequence.
    Reopen { open: OpenFacts },
    /// Retire every member below `to`. They must all be sealed, `to` must
    /// exceed the current log start, and `to` may not pass the open segment.
    /// The anchor becomes the last retired member's link.
    AdvanceLogStart { to: SegmentId },
    /// Salvage: drop every member at or above `from` (it must be at or above
    /// log start); `cut`, when present, reseals the last kept member with the
    /// prefix replay applied. Then open `open`, at least `next_segment_id`,
    /// linked to the last kept member, or the anchor when none is kept.
    SetAside {
        from: SegmentId,
        cut: Option<SealedFacts>,
        open: OpenFacts,
    },
    /// Drop every member and continue from `anchor`, a history this ledger
    /// did not write (cloud failback): open `open`, at least
    /// `next_segment_id`, linked to it.
    Rebase { anchor: ChainLink, open: OpenFacts },
    /// Raise the sequence reservation to `through` (#843): sequences up to
    /// it may be handed out. It must exceed the current reservation.
    Reserve { through: u64 },
}

/// The WAL segment ledger. See the module documentation.
///
/// Every value is a state some sequence of edits reaches from
/// [`WalLedger::genesis`]: deserializing an unreachable state fails.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(into = "LedgerImage", try_from = "LedgerImage")]
pub(crate) struct WalLedger {
    /// Identity of the database the ledger belongs to.
    holder: HolderId,
    /// The link the first member names: the last retired segment's, or the
    /// history this ledger started from.
    anchor: ChainLink,
    /// Strictly increasing and never empty; the last is the open segment
    /// and the only unsealed one.
    members: VecDeque<Member>,
    /// The id the next rotation opens: always one past the open segment.
    next_segment_id: SegmentId,
    /// Every sequence handed out is at or below this (#843).
    seq_reserved_through: u64,
}

/// Why an edit cannot apply, or why a serialized ledger is not a reachable
/// state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LedgerError {
    /// `Rotate` must seal the open segment.
    NotOpenSegment {
        seal: SegmentId,
        open_segment: SegmentId,
    },
    /// The edit must open the next segment id.
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
    /// The edit's facts break the chain: a link, epoch, first sequence or
    /// sealed fact disagrees with the member it follows.
    BrokenChain(String),
    /// `Reserve` must raise the reservation.
    ReservationNotRaised { through: u64, reserved_through: u64 },
    /// The edit would open the last representable id, leaving no next id.
    SegmentIdsExhausted,
    /// A serialized ledger is not a state any sequence of edits reaches.
    UnreachableState(&'static str),
}

impl fmt::Display for LedgerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
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
            Self::BrokenChain(reason) => {
                write!(f, "the WAL ledger edit breaks the chain: {reason}")
            }
            Self::ReservationNotRaised {
                through,
                reserved_through,
            } => write!(
                f,
                "cannot reserve WAL sequences through {through}: the reservation already reaches {reserved_through}"
            ),
            Self::SegmentIdsExhausted => f.write_str("WAL segment ids are exhausted"),
            Self::UnreachableState(reason) => {
                write!(f, "the WAL ledger state is unreachable: {reason}")
            }
        }
    }
}

impl std::error::Error for LedgerError {}

impl WalLedger {
    /// The ledger of a new history: identity `holder`, continuing `anchor`
    /// (`ChainLink::GENESIS` for a new database) in the segment `open`.
    pub(crate) fn genesis(
        holder: HolderId,
        anchor: ChainLink,
        open: OpenFacts,
    ) -> Result<Self, LedgerError> {
        opens_after(&open, anchor, None)?;
        Ok(Self {
            holder,
            anchor,
            members: VecDeque::from([Member::opened(open)]),
            next_segment_id: following(open.id)?,
            seq_reserved_through: 0,
        })
    }

    /// Apply one edit, or return why it is invalid without changing anything.
    pub(crate) fn apply(&mut self, edit: &LedgerEdit) -> Result<(), LedgerError> {
        match edit {
            LedgerEdit::Rotate { sealed, open } => self.rotate(sealed, *open),
            LedgerEdit::Reopen { open } => self.reopen(*open),
            LedgerEdit::AdvanceLogStart { to } => self.advance_log_start(*to),
            LedgerEdit::SetAside { from, cut, open } => self.set_aside(*from, cut.as_ref(), *open),
            LedgerEdit::Rebase { anchor, open } => self.rebase(*anchor, *open),
            LedgerEdit::Reserve { through } => self.reserve(*through),
        }
    }

    // Each edit validates everything before its first mutation, so a
    // rejected edit leaves the ledger as it was.

    fn rotate(&mut self, sealed: &SealedFacts, open: OpenFacts) -> Result<(), LedgerError> {
        let member = self.open_member();
        if sealed.link.id != member.id() {
            return Err(LedgerError::NotOpenSegment {
                seal: sealed.link.id,
                open_segment: member.id(),
            });
        }
        self.expect_next(open.id)?;
        seals(&member.open, sealed)?;
        opens_after(&open, sealed.link, Some(member.open.epoch))?;
        let next = following(open.id)?;
        if let Some(member) = self.members.back_mut() {
            member.sealed = Some(sealed.clone());
        }
        self.members.push_back(Member::opened(open));
        self.next_segment_id = next;
        Ok(())
    }

    fn reopen(&mut self, open: OpenFacts) -> Result<(), LedgerError> {
        let replaced = self.open_member().open;
        self.expect_next(open.id)?;
        opens_after(&open, replaced.prev, Some(replaced.epoch))?;
        let next = following(open.id)?;
        if let Some(member) = self.members.back_mut() {
            *member = Member::opened(open);
        }
        self.next_segment_id = next;
        Ok(())
    }

    fn advance_log_start(&mut self, to: SegmentId) -> Result<(), LedgerError> {
        let open_segment = self.open_segment();
        let log_start = self.log_start();
        if to <= log_start || to > open_segment {
            return Err(LedgerError::BadLogStart {
                to,
                log_start,
                open_segment,
            });
        }
        // `to` does not pass the open segment, so only sealed members retire.
        let retired = self.members.partition_point(|member| member.id() < to);
        let anchor = retired
            .checked_sub(1)
            .and_then(|last| self.members[last].sealed.as_ref())
            .map_or(self.anchor, |sealed| sealed.link);
        self.members.drain(..retired);
        self.anchor = anchor;
        Ok(())
    }

    fn set_aside(
        &mut self,
        from: SegmentId,
        cut: Option<&SealedFacts>,
        open: OpenFacts,
    ) -> Result<(), LedgerError> {
        let log_start = self.log_start();
        if from < log_start || open.id < self.next_segment_id {
            return Err(LedgerError::BadSetAside {
                from,
                open: open.id,
                log_start,
                next_segment_id: self.next_segment_id,
            });
        }
        let kept = self.members.partition_point(|member| member.id() < from);
        let prev = match (kept.checked_sub(1).map(|last| &self.members[last]), cut) {
            (Some(last), Some(cut)) => {
                if cut.link.id != last.id() {
                    return Err(LedgerError::BrokenChain(format!(
                        "the cut seals segment {} but the last kept member is {}",
                        cut.link.id,
                        last.id()
                    )));
                }
                seals(&last.open, cut)?;
                cut.link
            }
            (Some(last), None) => match &last.sealed {
                Some(sealed) => sealed.link,
                None => {
                    return Err(LedgerError::BrokenChain(format!(
                        "the kept member {} is unsealed and nothing cuts it",
                        last.id()
                    )))
                }
            },
            (None, Some(cut)) => {
                return Err(LedgerError::BrokenChain(format!(
                    "the cut seals segment {} but no member is kept",
                    cut.link.id
                )))
            }
            (None, None) => self.anchor,
        };
        opens_after(&open, prev, Some(self.newest_epoch()))?;
        let next = following(open.id)?;
        self.members.truncate(kept);
        if let (Some(cut), Some(last)) = (cut, self.members.back_mut()) {
            last.sealed = Some(cut.clone());
        }
        self.members.push_back(Member::opened(open));
        self.next_segment_id = next;
        Ok(())
    }

    fn rebase(&mut self, anchor: ChainLink, open: OpenFacts) -> Result<(), LedgerError> {
        if open.id < self.next_segment_id {
            return Err(LedgerError::NotNextSegment {
                open: open.id,
                next_segment_id: self.next_segment_id,
            });
        }
        opens_after(&open, anchor, Some(self.newest_epoch()))?;
        let next = following(open.id)?;
        self.members = VecDeque::from([Member::opened(open)]);
        self.anchor = anchor;
        self.next_segment_id = next;
        Ok(())
    }

    fn reserve(&mut self, through: u64) -> Result<(), LedgerError> {
        if through <= self.seq_reserved_through {
            return Err(LedgerError::ReservationNotRaised {
                through,
                reserved_through: self.seq_reserved_through,
            });
        }
        self.seq_reserved_through = through;
        Ok(())
    }

    fn expect_next(&self, open: SegmentId) -> Result<(), LedgerError> {
        if open == self.next_segment_id {
            Ok(())
        } else {
            Err(LedgerError::NotNextSegment {
                open,
                next_segment_id: self.next_segment_id,
            })
        }
    }

    /// Whether `edit` would apply, without applying it.
    ///
    /// # Errors
    ///
    /// The error [`Self::apply`] would return.
    pub(crate) fn check(&self, edit: &LedgerEdit) -> Result<(), LedgerError> {
        self.clone().apply(edit)
    }

    /// The holder identity genesis recorded.
    pub(crate) fn holder(&self) -> HolderId {
        self.holder
    }

    /// The link the first member names.
    pub(crate) fn anchor(&self) -> ChainLink {
        self.anchor
    }

    /// The open member, the newest.
    pub(crate) fn open_member(&self) -> &Member {
        self.members
            .back()
            .expect("a reachable ledger always has an open member")
    }

    /// The open segment, the newest member.
    pub(crate) fn open_segment(&self) -> SegmentId {
        self.open_member().id()
    }

    /// The id the next rotation opens.
    pub(crate) fn next_segment_id(&self) -> SegmentId {
        self.next_segment_id
    }

    /// The oldest member's id; every segment below it is retired.
    pub(crate) fn log_start(&self) -> SegmentId {
        self.members
            .front()
            .map_or(self.next_segment_id, Member::id)
    }

    /// Members to replay, oldest first; the last is the open segment.
    pub(crate) fn members(&self) -> impl Iterator<Item = SegmentId> + '_ {
        self.members.iter().map(Member::id)
    }

    /// Every member's facts, oldest first.
    pub(crate) fn member_facts(&self) -> impl Iterator<Item = &Member> + '_ {
        self.members.iter()
    }

    /// Sealed members, oldest first.
    pub(crate) fn sealed(&self) -> impl Iterator<Item = SegmentId> + '_ {
        self.members
            .iter()
            .take(self.members.len().saturating_sub(1))
            .map(Member::id)
    }

    /// The facts the ledger expects of member `id`.
    pub(crate) fn expected(&self, id: SegmentId) -> Option<&Member> {
        self.members
            .binary_search_by_key(&id, Member::id)
            .ok()
            .map(|index| &self.members[index])
    }

    /// Whether segment `id` is history recovery replays.
    pub(crate) fn is_member(&self, id: SegmentId) -> bool {
        self.expected(id).is_some()
    }

    /// The newest writer epoch that wrote this history: the open member's,
    /// since epochs never fall along the chain.
    pub(crate) fn newest_epoch(&self) -> u64 {
        self.members
            .iter()
            .map(|member| member.open.epoch)
            .max()
            .unwrap_or_default()
    }

    /// Every sequence handed out is at or below this.
    pub(crate) fn seq_reserved_through(&self) -> u64 {
        self.seq_reserved_through
    }
}

/// The id after `open`, which the ledger records as `next_segment_id`.
fn following(open: SegmentId) -> Result<SegmentId, LedgerError> {
    open.checked_add(1).ok_or(LedgerError::SegmentIdsExhausted)
}

/// Whether `open` may follow `prev`: it links it, starts past it, and does
/// not lower the epoch below `epoch`.
fn opens_after(open: &OpenFacts, prev: ChainLink, epoch: Option<u64>) -> Result<(), LedgerError> {
    crate::wal::format::follows(prev, epoch, open).map_err(LedgerError::BrokenChain)
}

/// Whether `sealed` can seal the member whose header is `open`.
fn seals(open: &OpenFacts, sealed: &SealedFacts) -> Result<(), LedgerError> {
    let consistent = if sealed.record_count == 0 {
        sealed.link.last_seq == open.prev.last_seq && sealed.max_seq_per_cf.is_empty()
    } else {
        sealed.link.last_seq >= open.first_seq
            && sealed
                .max_seq_per_cf
                .iter()
                .all(|(_, seq)| (open.first_seq..=sealed.link.last_seq).contains(seq))
    };
    if consistent && sealed.link.id == open.id {
        Ok(())
    } else {
        Err(LedgerError::BrokenChain(format!(
            "the facts {sealed:?} cannot seal segment {} starting at {}",
            open.id, open.first_seq
        )))
    }
}

/// The serialized form of a [`WalLedger`], validated on the way in.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LedgerImage {
    holder: HolderId,
    anchor: ChainLink,
    members: Vec<Member>,
    next_segment_id: SegmentId,
    seq_reserved_through: u64,
}

impl From<WalLedger> for LedgerImage {
    fn from(ledger: WalLedger) -> Self {
        Self {
            holder: ledger.holder,
            anchor: ledger.anchor,
            members: ledger.members.into(),
            next_segment_id: ledger.next_segment_id,
            seq_reserved_through: ledger.seq_reserved_through,
        }
    }
}

impl TryFrom<LedgerImage> for WalLedger {
    type Error = LedgerError;

    fn try_from(image: LedgerImage) -> Result<Self, Self::Error> {
        let LedgerImage {
            holder,
            anchor,
            members,
            next_segment_id,
            seq_reserved_through,
        } = image;
        let Some((open, sealed)) = members.split_last() else {
            return Err(LedgerError::UnreachableState(
                "the ledger has no open segment",
            ));
        };
        if open.sealed.is_some() || sealed.iter().any(|member| member.sealed.is_none()) {
            return Err(LedgerError::UnreachableState(
                "only the open segment is unsealed",
            ));
        }
        if open.id().checked_add(1) != Some(next_segment_id) {
            return Err(LedgerError::UnreachableState(
                "the next segment id does not follow the open segment",
            ));
        }
        let mut prev = anchor;
        let mut epoch = None;
        for member in &members {
            opens_after(&member.open, prev, epoch)
                .map_err(|_| LedgerError::UnreachableState("the members do not chain"))?;
            epoch = Some(member.open.epoch);
            if let Some(facts) = &member.sealed {
                seals(&member.open, facts).map_err(|_| {
                    LedgerError::UnreachableState("a member's facts do not seal it")
                })?;
                prev = facts.link;
            }
        }
        Ok(Self {
            holder,
            anchor,
            members: members.into(),
            next_segment_id,
            seq_reserved_through,
        })
    }
}

#[cfg(test)]
mod tests;
