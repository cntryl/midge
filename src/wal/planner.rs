//! Recovery planning (#844): which WAL segments recovery replays, in which
//! order and from where, what salvage sets aside, and how the local history
//! continues.
//!
//! [`lineage`] places a cache's segment ledger against the cloud catalog,
//! and [`plan`] admits the probed segments in chain order from the catalog's
//! anchor and decides; [`local`] plans a local WAL from its ledger alone.
//! They are pure: adapters probe the segments, then carry the plan out.
//!
//! ## Lineage
//!
//! The catalog's head `h_C` is the link of its newest entry, or its anchor;
//! the ledger's positions are its anchor and each sealed member's link.
//!
//! - **Fresh:** no ledger. The catalog replays; a new history starts linked
//!   to its head, at its next id.
//! - **Joined:** the ledger's chain holds `h_C`, and the catalog never
//!   handed out the id of the ledger's first member after it. Members at or
//!   below the catalog's anchor are retired history; members up to `h_C`
//!   replay from the catalog (salvage falls back to the local copy); later
//!   members are this cache's tail.
//! - **Behind:** `h_C` precedes the ledger's anchor and is not in its
//!   chain: the catalog lost history this cache retired. When the catalog's
//!   floor covers the ledger's anchor, the catalog set that history aside
//!   itself and the cache rebases; otherwise it is a hole.
//! - **Diverged:** anything else: another writer extended the catalog. The
//!   local history is set aside and the cache rebases onto the catalog.
//!
//! A cataloged segment whose link differs from this ledger's member at the
//! same id is never Joined, so it is a fork: the local tail is set aside.

use crate::common::MidgeError;
use crate::config::RecoveryPolicy;
use crate::wal::cloud_catalog::{CatalogEntry, WalPublicationCatalog};
use crate::wal::format::{Chain, ChainLink, SegmentHeader, SegmentId};
use crate::wal::ledger::{Member, WalLedger};
use std::collections::BTreeMap;

/// Where a cache's ledger stands against the catalog. See the module
/// documentation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Lineage {
    Fresh,
    Joined,
    Behind {
        /// The catalog's floor covers the ledger's anchor: the catalog set
        /// the lost history aside itself.
        covered: bool,
    },
    Diverged,
}

/// Place `ledger`, the cache's, if it has one, against `catalog`.
pub(crate) fn lineage(catalog: &WalPublicationCatalog, ledger: Option<&WalLedger>) -> Lineage {
    let Some(ledger) = ledger else {
        return Lineage::Fresh;
    };
    let head = catalog.head();
    let anchor = ledger.anchor();
    let in_chain = anchor == head
        || ledger
            .member_facts()
            .filter_map(|member| member.sealed.as_ref())
            .any(|sealed| sealed.link == head);
    // The ledger's first member after the head continues the catalog only
    // when the catalog never handed out its id: a writer may have published
    // that id and salvage set it aside since.
    let continues = ledger
        .members()
        .find(|id| *id > head.id)
        .unwrap_or_else(|| ledger.open_segment());
    if in_chain && catalog.next_segment_id() <= continues {
        Lineage::Joined
    } else if !in_chain && head.id < anchor.id {
        Lineage::Behind {
            covered: catalog.seq_floor() >= anchor.last_seq,
        }
    } else {
        Lineage::Diverged
    }
}

/// What reading one segment found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Probe {
    /// A sealed segment that verified whole: its header, and the link its
    /// footer seals.
    Sealed {
        header: SegmentHeader,
        link: ChainLink,
    },
    /// The open member, read as far as it verifies.
    Open(OpenProbe),
    /// The segment cannot be replayed: its file or object is gone or
    /// damaged. `max_verified` is the highest sequence a verified frame in
    /// it holds, zero when none.
    Damaged { reason: String, max_verified: u64 },
}

/// The open member as far as it verifies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OpenProbe {
    pub(crate) header: SegmentHeader,
    /// The file's length.
    pub(crate) len: u64,
    /// Where the verified prefix ends.
    pub(crate) valid_bytes: u64,
    /// The records the verified prefix holds.
    pub(crate) record_count: u64,
    /// The highest sequence the verified prefix holds, zero when none.
    pub(crate) max_sequence: u64,
    /// What ends the verified prefix.
    pub(crate) end: OpenEnd,
}

/// What ends an open member's verified prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OpenEnd {
    /// The end of the file: every byte verified.
    Whole,
    /// An incomplete frame a crash left behind.
    Torn,
    /// Damage only salvage may cut at; `suffix_max` is the highest sequence
    /// a verified frame past it holds, zero when none.
    Damaged { reason: String, suffix_max: u64 },
}

/// The probes [`plan`] reads: cataloged objects and local ledger members,
/// by segment id. A segment nobody probed counts as damaged.
#[derive(Debug, Clone, Default)]
pub(crate) struct Probes {
    pub(crate) remote: BTreeMap<SegmentId, Probe>,
    pub(crate) local: BTreeMap<SegmentId, Probe>,
}

/// Where replay reads a planned segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Source {
    /// The cataloged object.
    Remote,
    /// The cache's ledger member.
    Local,
}

/// What replay checks a segment's header and footer against, beyond the
/// chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Expectation {
    /// A ledger member: its header facts and the ledger's holder, and its
    /// footer facts once sealed.
    Member { member: Member, holder: [u8; 16] },
    /// A published object: the epoch and link before it its catalog entry
    /// names, and the link its footer must seal.
    Published(CatalogEntry),
}

impl Expectation {
    /// What `ledger` expects of its `member`, whose header carries the
    /// ledger's holder.
    pub(crate) fn member(ledger: &WalLedger, member: &Member) -> Self {
        Self::Member {
            member: member.clone(),
            holder: ledger.holder().bytes(),
        }
    }
}

/// One segment replay reads, in plan order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlannedSegment {
    pub(crate) id: SegmentId,
    pub(crate) source: Source,
    /// Sealed segments must end in their footer; only the last planned
    /// segment may be open.
    pub(crate) sealed: bool,
    pub(crate) expect: Expectation,
}

/// What a replayed segment holds, for the runtime's cloud obligations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SegmentFacts {
    pub(crate) max_sequence: u64,
    pub(crate) writer_epoch: u64,
}

/// How the cache's ledger continues once the plan is durable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HistoryAction {
    /// A new history: its first segment, at least `open_at_least`, links
    /// `anchor`.
    Genesis {
        anchor: ChainLink,
        open_at_least: SegmentId,
    },
    /// The ledger stands; members below `retire_local_below` are history the
    /// catalog already retired.
    Keep {
        retire_local_below: Option<SegmentId>,
    },
    /// Salvage: members from `from` on are set aside, and a new segment at
    /// least `open_at_least` follows the last kept one.
    SetAside {
        from: SegmentId,
        open_at_least: SegmentId,
    },
    /// Every member is set aside, and the history continues from `anchor`
    /// in a new segment at least `open_at_least`.
    Rebase {
        anchor: ChainLink,
        open_at_least: SegmentId,
    },
}

/// Cut the open member back to the `valid_bytes` replay applies, keeping a
/// copy of the original first when `retain`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OpenCut {
    pub(crate) id: SegmentId,
    pub(crate) valid_bytes: u64,
    pub(crate) retain: bool,
}

/// A recovery plan. See [`plan`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecoveryPlan {
    /// The link the first planned segment must name.
    pub(crate) anchor: ChainLink,
    /// The segments replay reads, in order.
    pub(crate) replay: Vec<PlannedSegment>,
    /// The planned cataloged segments: already published.
    pub(crate) remote_segments: BTreeMap<SegmentId, SegmentFacts>,
    /// The planned sealed local segments: still owed to the catalog.
    pub(crate) local_segments: BTreeMap<SegmentId, SegmentFacts>,
    /// Salvage dropped or cut damaged history.
    pub(crate) opened_in_salvage_mode: bool,
    /// Every sequence at or below it may be held by history this or an
    /// earlier open did not replay, or by the catalog's head; new writes
    /// start above it.
    pub(crate) max_unreplayed_sequence: u64,
    /// The sequence floor the catalog must hold before the ledger changes.
    pub(crate) catalog_floor: u64,
    /// The first cataloged segment salvage sets aside; the catalog drops it
    /// and every later one.
    pub(crate) retire_catalog_from: Option<SegmentId>,
    /// The entries from `retire_catalog_from` on, as planning saw them.
    pub(crate) set_aside_entries: Vec<CatalogEntry>,
    pub(crate) history: HistoryAction,
    pub(crate) open_cut: Option<OpenCut>,
}

impl RecoveryPlan {
    /// Whether the catalog must change before the ledger does: salvage sets
    /// cataloged segments aside, or the floor rises.
    pub(crate) fn changes_catalog(&self, catalog: &WalPublicationCatalog) -> bool {
        self.retire_catalog_from.is_some() || self.catalog_floor > catalog.seq_floor()
    }

    pub(crate) fn remote_max_sequences(&self) -> BTreeMap<u64, u64> {
        facts(&self.remote_segments, |facts| facts.max_sequence)
    }

    pub(crate) fn remote_writer_epochs(&self) -> BTreeMap<u64, u64> {
        facts(&self.remote_segments, |facts| facts.writer_epoch)
    }

    pub(crate) fn local_max_sequences(&self) -> BTreeMap<u64, u64> {
        facts(&self.local_segments, |facts| facts.max_sequence)
    }

    pub(crate) fn local_writer_epochs(&self) -> BTreeMap<u64, u64> {
        facts(&self.local_segments, |facts| facts.writer_epoch)
    }
}

fn facts(
    segments: &BTreeMap<SegmentId, SegmentFacts>,
    value: impl Fn(&SegmentFacts) -> u64,
) -> BTreeMap<u64, u64> {
    segments
        .iter()
        .map(|(id, facts)| (*id, value(facts)))
        .collect()
}

/// Why recovery cannot go on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PlanError {
    /// Strict recovery found history it cannot replay; the probe's text.
    Unreplayable(String),
    /// The catalog lost history this cache retired, and no floor covers it.
    CatalogBehind(String),
}

impl From<PlanError> for MidgeError {
    fn from(error: PlanError) -> Self {
        match error {
            PlanError::Unreplayable(reason) => {
                MidgeError::RecoveryFailed(format!("WAL source validation failed: {reason}"))
            }
            PlanError::CatalogBehind(reason) => MidgeError::Corruption(reason),
        }
    }
}

/// The plan for a local WAL: every member of `ledger`, in order, from its
/// anchor.
pub(crate) fn local(ledger: &WalLedger) -> RecoveryPlan {
    let replay = ledger
        .member_facts()
        .map(|member| PlannedSegment {
            id: member.id(),
            source: Source::Local,
            sealed: member.sealed.is_some(),
            expect: Expectation::member(ledger, member),
        })
        .collect();
    RecoveryPlan {
        anchor: ledger.anchor(),
        replay,
        remote_segments: BTreeMap::new(),
        local_segments: BTreeMap::new(),
        opened_in_salvage_mode: false,
        max_unreplayed_sequence: 0,
        catalog_floor: 0,
        retire_catalog_from: None,
        set_aside_entries: Vec::new(),
        history: HistoryAction::Keep {
            retire_local_below: None,
        },
        open_cut: None,
    }
}

/// The first planned position recovery cannot replay.
struct Hole {
    id: SegmentId,
    reason: String,
}

/// Plan cloud recovery over `catalog` and, when the cache has one, its
/// `ledger`, from `probes`. The planner derives their lineage from these
/// history facts, so a caller cannot supply a different classification.
///
/// Positions are ordered catalog entries, then (when Joined) the cache's
/// tail, and admitted in that order to one [`Chain`] from the catalog's
/// anchor. The first that is missing, damaged or breaks the chain is the
/// hole: strict recovery refuses it; salvage replays only what precedes it,
/// sets the rest aside and lifts the sequence floor above all of it.
///
/// # Errors
///
/// [`PlanError::Unreplayable`] for a hole under strict recovery, and
/// [`PlanError::CatalogBehind`] when strict recovery finds the catalog lost
/// history the cache retired.
pub(crate) fn plan(
    policy: RecoveryPolicy,
    catalog: &WalPublicationCatalog,
    ledger: Option<&WalLedger>,
    probes: &Probes,
) -> Result<RecoveryPlan, PlanError> {
    let lineage = lineage(catalog, ledger);
    let salvage = policy == RecoveryPolicy::Salvage;
    let mut planner = Planner {
        salvage,
        chain: Chain::new(Some(catalog.anchor())),
        replay: Vec::new(),
        remote: BTreeMap::new(),
        local: BTreeMap::new(),
        salvaged: false,
        open_cut: None,
        cut_floor: 0,
    };
    let joined = ledger.filter(|_| lineage == Lineage::Joined);
    let mut hole = planner.admit_catalog(catalog, joined, probes);
    if let (None, Some(ledger)) = (&hole, joined) {
        hole = planner.admit_tail(catalog.head(), ledger, probes)?;
    }
    if let Lineage::Behind { covered: false } = lineage {
        let reason = format!(
            "the cloud WAL catalog ends at segment {} but this cache's history already retired through segment {}, and no sequence floor covers it",
            catalog.head().id,
            ledger.map_or(0, |ledger| ledger.anchor().id)
        );
        if !salvage {
            return Err(PlanError::CatalogBehind(reason));
        }
        planner.salvaged = true;
    }
    if let Some(hole) = &hole {
        if !salvage {
            return Err(PlanError::Unreplayable(hole.reason.clone()));
        }
        planner.salvaged = true;
    }
    Ok(planner.finish(catalog, ledger, lineage, hole.as_ref(), probes))
}

struct Planner {
    salvage: bool,
    chain: Chain,
    replay: Vec<PlannedSegment>,
    remote: BTreeMap<SegmentId, SegmentFacts>,
    local: BTreeMap<SegmentId, SegmentFacts>,
    salvaged: bool,
    open_cut: Option<OpenCut>,
    /// The highest sequence past an open member's salvage cut.
    cut_floor: u64,
}

impl Planner {
    /// Admit the catalog's entries from its anchor; a damaged object of a
    /// member `joined` shares falls back to the local copy under salvage.
    /// Returns the hole, if any.
    fn admit_catalog(
        &mut self,
        catalog: &WalPublicationCatalog,
        joined: Option<&WalLedger>,
        probes: &Probes,
    ) -> Option<Hole> {
        for entry in catalog.entries() {
            let id = entry.id();
            let remote = published(entry, probes.remote.get(&id))
                .and_then(|header| admit(&mut self.chain, &header, id, entry.link()));
            match remote {
                Ok(()) => {
                    self.replay.push(PlannedSegment {
                        id,
                        source: Source::Remote,
                        sealed: true,
                        expect: Expectation::Published(entry.clone()),
                    });
                    self.remote.insert(
                        id,
                        SegmentFacts {
                            max_sequence: entry.max_sequence(),
                            writer_epoch: entry.epoch(),
                        },
                    );
                }
                Err(reason) => {
                    let fallback = joined
                        .filter(|_| self.salvage)
                        .and_then(|ledger| self.fall_back(ledger, entry, probes));
                    if fallback.is_none() {
                        return Some(Hole { id, reason });
                    }
                    self.salvaged = true;
                }
            }
        }
        None
    }

    /// Replay `ledger`'s copy of the cataloged `entry` whose object is
    /// unusable, when the copy is the same segment and admits.
    fn fall_back(
        &mut self,
        ledger: &WalLedger,
        entry: &CatalogEntry,
        probes: &Probes,
    ) -> Option<()> {
        let id = entry.id();
        let member = ledger.expected(id)?;
        let sealed = member.sealed.as_ref()?;
        if sealed.link != entry.link() {
            return None;
        }
        let header = sealed_member(ledger, member, probes.local.get(&id)).ok()?;
        admit(&mut self.chain, &header, id, sealed.link).ok()?;
        self.replay.push(PlannedSegment {
            id,
            source: Source::Local,
            sealed: true,
            expect: Expectation::member(ledger, member),
        });
        self.local.insert(
            id,
            SegmentFacts {
                max_sequence: sealed.link.last_seq,
                writer_epoch: header.epoch,
            },
        );
        Some(())
    }

    /// Admit the cache's tail: its members after the catalog's `head`.
    /// Returns the hole, if any.
    ///
    /// # Errors
    ///
    /// Damage to the open member that strict recovery may not cut.
    fn admit_tail(
        &mut self,
        head: ChainLink,
        ledger: &WalLedger,
        probes: &Probes,
    ) -> Result<Option<Hole>, PlanError> {
        for member in ledger.member_facts().filter(|member| member.id() > head.id) {
            let id = member.id();
            let probe = probes.local.get(&id);
            let Some(sealed) = &member.sealed else {
                return self.admit_open(ledger, member, probe);
            };
            let admitted = sealed_member(ledger, member, probe).and_then(|header| {
                admit(&mut self.chain, &header, id, sealed.link).map(|()| header)
            });
            match admitted {
                Ok(header) => {
                    self.replay.push(PlannedSegment {
                        id,
                        source: Source::Local,
                        sealed: true,
                        expect: Expectation::member(ledger, member),
                    });
                    self.local.insert(
                        id,
                        SegmentFacts {
                            max_sequence: sealed.link.last_seq,
                            writer_epoch: header.epoch,
                        },
                    );
                }
                Err(reason) => return Ok(Some(Hole { id, reason })),
            }
        }
        Ok(None)
    }

    /// Admit the open member: replay its verified prefix, cut back to it
    /// when bytes follow.
    fn admit_open(
        &mut self,
        ledger: &WalLedger,
        member: &Member,
        probe: Option<&Probe>,
    ) -> Result<Option<Hole>, PlanError> {
        let id = member.id();
        let open = match probe {
            Some(Probe::Open(open)) => open,
            Some(Probe::Damaged { reason, .. }) => {
                return Ok(Some(Hole {
                    id,
                    reason: reason.clone(),
                }))
            }
            Some(Probe::Sealed { .. }) | None => {
                return Ok(Some(Hole {
                    id,
                    reason: format!("open WAL segment {id} was not read"),
                }))
            }
        };
        if let Err(reason) = expected_header(ledger, member, &open.header).and_then(|()| {
            self.chain
                .admit(&open.header, id)
                .map_err(|error| error.to_string())
        }) {
            return Ok(Some(Hole { id, reason }));
        }
        let retain = match &open.end {
            OpenEnd::Whole => false,
            OpenEnd::Torn => open.record_count == 0,
            OpenEnd::Damaged { reason, suffix_max } => {
                if !self.salvage {
                    return Err(PlanError::Unreplayable(format!(
                        "active WAL failed validation: {reason}"
                    )));
                }
                self.salvaged = true;
                self.cut_floor = *suffix_max;
                true
            }
        };
        if open.valid_bytes != open.len {
            self.open_cut = Some(OpenCut {
                id,
                valid_bytes: open.valid_bytes,
                retain,
            });
        }
        if open.record_count > 0 {
            let mut expected = member.clone();
            if self.open_cut.is_some() {
                expected.durable_bytes = expected
                    .durable_bytes
                    .map(|bytes| bytes.min(open.valid_bytes));
            }
            self.replay.push(PlannedSegment {
                id,
                source: Source::Local,
                sealed: false,
                expect: Expectation::member(ledger, &expected),
            });
        }
        Ok(None)
    }

    fn finish(
        self,
        catalog: &WalPublicationCatalog,
        ledger: Option<&WalLedger>,
        lineage: Lineage,
        hole: Option<&Hole>,
        probes: &Probes,
    ) -> RecoveryPlan {
        // The catalog drops every entry at or after the hole; its head
        // becomes the link before it.
        let set_aside_entries: Vec<CatalogEntry> = hole.map_or_else(Vec::new, |hole| {
            catalog
                .entries()
                .iter()
                .filter(|entry| entry.id() >= hole.id)
                .cloned()
                .collect()
        });
        let retire_catalog_from = set_aside_entries.first().map(CatalogEntry::id);
        let head = catalog
            .entries()
            .iter()
            .take_while(|entry| retire_catalog_from.is_none_or(|from| entry.id() < from))
            .last()
            .map_or(catalog.anchor(), CatalogEntry::link);
        let open_at_least = catalog.next_segment_id();
        let entries_floor = set_aside_entries
            .iter()
            .map(CatalogEntry::max_sequence)
            .max()
            .unwrap_or(0);
        // Local members set aside: from the hole when Joined, every member
        // otherwise.
        let local_from = match lineage {
            Lineage::Joined => hole.map(|hole| hole.id),
            _ => Some(0),
        };
        let local_floor = ledger
            .zip(local_from)
            .map_or(0, |(ledger, from)| set_aside_maximum(ledger, from, probes));
        // The catalog's floor covers what it sets aside, and local history
        // that was owed to it; a takeover's tail stays below this ledger's
        // reservation instead.
        let owed_floor = match lineage {
            Lineage::Joined if hole.is_some() => local_floor,
            Lineage::Behind { covered: false } => {
                local_floor.max(ledger.map_or(0, |ledger| ledger.anchor().last_seq))
            }
            _ => 0,
        };
        let catalog_floor = catalog
            .seq_floor()
            .max(entries_floor)
            .max(owed_floor)
            .max(self.cut_floor);
        let max_unreplayed_sequence = catalog_floor.max(head.last_seq).max(local_floor);
        let history = match (lineage, ledger) {
            (Lineage::Fresh, _) | (_, None) => HistoryAction::Genesis {
                anchor: head,
                open_at_least,
            },
            (Lineage::Joined, Some(ledger)) => match hole {
                None => HistoryAction::Keep {
                    retire_local_below: (ledger.log_start() <= catalog.anchor().id)
                        .then(|| catalog.anchor().id.saturating_add(1)),
                },
                Some(hole) if kept_before(ledger, catalog.anchor().id, hole.id) => {
                    HistoryAction::SetAside {
                        from: hole.id,
                        open_at_least,
                    }
                }
                Some(_) => HistoryAction::Rebase {
                    anchor: head,
                    open_at_least,
                },
            },
            (Lineage::Behind { .. } | Lineage::Diverged, Some(_)) => HistoryAction::Rebase {
                anchor: head,
                open_at_least,
            },
        };
        let replay = match hole {
            Some(hole) => self
                .replay
                .into_iter()
                .filter(|segment| segment.id < hole.id)
                .collect(),
            None => self.replay,
        };
        RecoveryPlan {
            anchor: catalog.anchor(),
            replay,
            remote_segments: self.remote,
            local_segments: self.local,
            opened_in_salvage_mode: self.salvaged,
            max_unreplayed_sequence,
            catalog_floor,
            retire_catalog_from,
            set_aside_entries,
            history,
            open_cut: self.open_cut,
        }
    }
}

/// Whether `ledger` keeps a member before the hole at `hole` that the
/// catalog did not retire (it is above the catalog's anchor, `retired`).
fn kept_before(ledger: &WalLedger, retired: SegmentId, hole: SegmentId) -> bool {
    ledger.members().any(|id| id > retired && id < hole)
}

/// The highest sequence `ledger`'s members from `from` on may hold: what
/// the ledger sealed into each, and what verified frames of each hold.
fn set_aside_maximum(ledger: &WalLedger, from: SegmentId, probes: &Probes) -> u64 {
    ledger
        .member_facts()
        .filter(|member| member.id() >= from)
        .map(|member| {
            let recorded = member
                .sealed
                .as_ref()
                .map_or(0, |sealed| sealed.link.last_seq);
            let verified = match probes.local.get(&member.id()) {
                Some(Probe::Sealed { link, .. }) => link.last_seq,
                Some(Probe::Open(open)) => match &open.end {
                    OpenEnd::Damaged { suffix_max, .. } => open.max_sequence.max(*suffix_max),
                    OpenEnd::Whole | OpenEnd::Torn => open.max_sequence,
                },
                Some(Probe::Damaged { max_verified, .. }) => *max_verified,
                None => 0,
            };
            recorded.max(verified)
        })
        .max()
        .unwrap_or(0)
}

/// The header of cataloged `entry`'s object, when `probe` found it whole
/// and it is the segment the entry names.
fn published(entry: &CatalogEntry, probe: Option<&Probe>) -> Result<SegmentHeader, String> {
    match probe {
        Some(Probe::Sealed { header, link }) => {
            if header.id != entry.id()
                || header.epoch != entry.epoch()
                || header.prev != entry.prev()
                || *link != entry.link()
            {
                return Err(format!(
                    "cloud WAL '{}' is not the segment its catalog entry names",
                    entry.object_key()
                ));
            }
            Ok(*header)
        }
        Some(Probe::Damaged { reason, .. }) => Err(reason.clone()),
        Some(Probe::Open(_)) | None => Err(format!(
            "cloud WAL '{}' was not read whole",
            entry.object_key()
        )),
    }
}

/// The header of `ledger`'s sealed `member`, when `probe` found the file
/// whole and it is the segment the ledger recorded.
fn sealed_member(
    ledger: &WalLedger,
    member: &Member,
    probe: Option<&Probe>,
) -> Result<SegmentHeader, String> {
    match probe {
        Some(Probe::Sealed { header, link }) => {
            expected_header(ledger, member, header)?;
            if member.sealed.as_ref().map(|sealed| sealed.link) != Some(*link) {
                return Err(format!(
                    "WAL segment {} seals another link than the ledger recorded",
                    member.id()
                ));
            }
            Ok(*header)
        }
        Some(Probe::Damaged { reason, .. }) => Err(reason.clone()),
        Some(Probe::Open(_)) | None => Err(format!(
            "sealed WAL segment {} was not read whole",
            member.id()
        )),
    }
}

/// Whether `header` is the one `ledger` recorded for `member`.
fn expected_header(
    ledger: &WalLedger,
    member: &Member,
    header: &SegmentHeader,
) -> Result<(), String> {
    if header.holder != ledger.holder().bytes() {
        return Err(format!(
            "WAL segment {} header names another holder than this ledger's",
            member.id()
        ));
    }
    if header.open_facts() != member.open {
        return Err(format!(
            "WAL segment {} header {:?} is not the ledger's {:?}",
            member.id(),
            header.open_facts(),
            member.open
        ));
    }
    Ok(())
}

/// Admit `header`, planned as segment `id`, to `chain`, sealed by `link`.
fn admit(
    chain: &mut Chain,
    header: &SegmentHeader,
    id: SegmentId,
    link: ChainLink,
) -> Result<(), String> {
    chain.admit(header, id).map_err(|error| error.to_string())?;
    chain.seal(link);
    Ok(())
}

#[cfg(test)]
mod tests;
