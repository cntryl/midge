use super::*;
use crate::wal::format::segment::{sealed_footer, SegmentBuilder, FIXTURE_HOLDER};
use crate::wal::format::{OpenFacts, SealedFacts};
use crate::wal::ledger::{HolderId, LedgerEdit};
use bytes::Bytes;

/// A sealed segment a fixture built: its header, its footer's facts and its
/// bytes.
#[derive(Clone)]
struct Built {
    header: SegmentHeader,
    sealed: SealedFacts,
    bytes: Vec<u8>,
}

impl Built {
    fn link(&self) -> ChainLink {
        self.sealed.link
    }

    fn entry(&self) -> CatalogEntry {
        CatalogEntry::from_sealed_bytes(&self.bytes).expect("a built segment publishes")
    }

    fn probe(&self) -> Probe {
        Probe::Sealed {
            header: self.header,
            link: self.link(),
        }
    }
}

fn put(seq: u64) -> crate::wal::WalRecord {
    crate::wal::WalRecord::new(
        crate::wal::WalOpKind::Put,
        Bytes::from_static(b"key"),
        Some(Bytes::copy_from_slice(&seq.to_le_bytes())),
        seq,
    )
}

/// Segment `id` by `epoch` after `prev`, holding one record, sealed.
fn sealed(id: SegmentId, epoch: u64, prev: ChainLink) -> Built {
    let builder = SegmentBuilder::new(id, epoch, prev).record(&put(prev.last_seq + 1));
    let header = builder.header();
    let (bytes, _) = builder.seal();
    let sealed = sealed_footer(&bytes)
        .expect("a built segment scans")
        .expect("a built segment is sealed")
        .sealed_facts();
    Built {
        header,
        sealed,
        bytes,
    }
}

/// Segment `id` by `epoch` after `prev`, written by another holder than
/// the fixtures' ledgers.
fn foreign(id: SegmentId, epoch: u64, prev: ChainLink) -> Built {
    let mut header = SegmentBuilder::new(id, epoch, prev).header();
    header.holder = [0x42; 16];
    let (bytes, _) = SegmentBuilder::from_header(header)
        .record(&put(prev.last_seq + 1))
        .seal();
    let sealed = sealed_footer(&bytes)
        .expect("a built segment scans")
        .expect("a built segment is sealed")
        .sealed_facts();
    Built {
        header,
        sealed,
        bytes,
    }
}

/// Segments `ids`, in order, by `epoch`, chained after `anchor`.
fn chain(anchor: ChainLink, ids: impl IntoIterator<Item = SegmentId>, epoch: u64) -> Vec<Built> {
    let mut prev = anchor;
    ids.into_iter()
        .map(|id| {
            let built = sealed(id, epoch, prev);
            prev = built.link();
            built
        })
        .collect()
}

/// The ledger of a cache that sealed `members` after `anchor` and opened
/// `open`, empty, after them, by `epoch`.
fn ledger(anchor: ChainLink, members: &[Built], open: SegmentId, epoch: u64) -> WalLedger {
    let holder = HolderId::from_bytes(FIXTURE_HOLDER).expect("the fixture holder");
    let opening = |id: SegmentId, prev: ChainLink| OpenFacts {
        id,
        epoch,
        first_seq: prev.last_seq + 1,
        prev,
    };
    let first = members.first().map_or(open, |member| member.header.id);
    let mut ledger = WalLedger::genesis(
        holder,
        anchor,
        members.first().map_or_else(
            || opening(first, anchor),
            |member| member.header.open_facts(),
        ),
    )
    .expect("genesis");
    for (index, member) in members.iter().enumerate() {
        let next = members.get(index + 1).map_or_else(
            || opening(open, member.link()),
            |next| next.header.open_facts(),
        );
        ledger
            .apply(&LedgerEdit::Rotate {
                sealed: member.sealed.clone(),
                open: next,
            })
            .expect("rotate");
    }
    ledger
}

/// A catalog owned by epoch 9 that published `segments` after `anchor`,
/// starting from a pristine one when `anchor` is the genesis link.
fn catalog(segments: &[Built]) -> WalPublicationCatalog {
    let mut catalog = WalPublicationCatalog::empty(9).expect("catalog");
    let entries: Vec<CatalogEntry> = segments.iter().map(Built::entry).collect();
    catalog.append_run(9, &entries).expect("publish");
    catalog
}

/// The open member of `ledger`, holding `records` records through
/// `max_sequence` in `len` bytes, every one verified.
fn open_probe(ledger: &WalLedger, records: u64, max_sequence: u64) -> Probe {
    let open = ledger.open_member().open;
    Probe::Open(OpenProbe {
        header: open.header(ledger.holder().bytes()),
        len: 128,
        valid_bytes: 128,
        record_count: records,
        max_sequence,
        end: OpenEnd::Whole,
    })
}

/// Probes where every cataloged object and every ledger member reads whole.
fn whole(remote: &[Built], local: &[Built], ledger: &WalLedger) -> Probes {
    let mut probes = Probes::default();
    for built in remote {
        probes.remote.insert(built.header.id, built.probe());
    }
    for built in local {
        probes.local.insert(built.header.id, built.probe());
    }
    probes
        .local
        .insert(ledger.open_segment(), open_probe(ledger, 0, 0));
    probes
}

fn ids(plan: &RecoveryPlan) -> Vec<(SegmentId, Source)> {
    plan.replay
        .iter()
        .map(|segment| (segment.id, segment.source))
        .collect()
}

#[test]
fn should_plan_every_ledger_member_after_its_anchor() {
    // Arrange
    let members = chain(ChainLink::GENESIS, 1..=2, 7);
    let ledger = ledger(ChainLink::GENESIS, &members, 3, 7);

    // Act
    let plan = local(&ledger);

    // Assert
    assert_eq!(plan.anchor, ChainLink::GENESIS);
    assert_eq!(
        plan.replay
            .iter()
            .map(|segment| (segment.id, segment.sealed))
            .collect::<Vec<_>>(),
        vec![(1, true), (2, true), (3, false)]
    );
    assert!(plan
        .replay
        .iter()
        .all(|segment| matches!(segment.expect, Expectation::Member { .. })));
}

#[test]
fn should_classify_a_cache_without_a_ledger_as_fresh() {
    // Arrange
    let catalog = catalog(&chain(ChainLink::GENESIS, 1..=2, 7));

    // Act
    let lineage = lineage(&catalog, None);

    // Assert
    assert_eq!(lineage, Lineage::Fresh);
}

#[test]
fn should_join_a_ledger_whose_chain_holds_the_catalog_head() {
    // Arrange: this cache published 1 and 2 and holds them, then opened 3.
    let members = chain(ChainLink::GENESIS, 1..=2, 7);
    let ledger = ledger(ChainLink::GENESIS, &members, 3, 7);
    let catalog = catalog(&members);
    let probes = whole(&members, &[], &ledger);

    // Act
    let plan = plan(RecoveryPolicy::Strict, &catalog, Some(&ledger), &probes);

    // Assert: the shared members replay from the catalog, nothing is owed,
    // and the ledger stands.
    let plan = plan.expect("plan");
    assert_eq!(lineage(&catalog, Some(&ledger)), Lineage::Joined);
    assert_eq!(ids(&plan), vec![(1, Source::Remote), (2, Source::Remote)]);
    assert!(plan.local_segments.is_empty());
    assert_eq!(
        plan.history,
        HistoryAction::Keep {
            retire_local_below: None
        }
    );
    assert!(!plan.changes_catalog(&catalog));
}

#[test]
fn should_retire_members_at_or_below_the_catalog_anchor() {
    // Arrange: the catalog retired 1; the cache still holds it.
    let members = chain(ChainLink::GENESIS, 1..=2, 7);
    let ledger = ledger(ChainLink::GENESIS, &members, 3, 7);
    let mut catalog = catalog(&members);
    catalog
        .retire_prefix(
            9,
            &[members[0].entry()],
            crate::lease::SharedGenerationId::new(1).unwrap(),
        )
        .expect("retire");
    let probes = whole(&members[1..], &[], &ledger);

    // Act
    let plan = plan(RecoveryPolicy::Strict, &catalog, Some(&ledger), &probes).expect("plan");

    // Assert
    assert_eq!(ids(&plan), vec![(2, Source::Remote)]);
    assert_eq!(plan.anchor, members[0].link());
    assert!(plan.local_segments.is_empty());
    assert_eq!(
        plan.history,
        HistoryAction::Keep {
            retire_local_below: Some(2)
        }
    );
}

#[test]
fn should_replay_the_tail_after_the_catalog_head_from_the_cache() {
    // Arrange: the catalog holds 1; the cache sealed 2 without publishing it.
    let members = chain(ChainLink::GENESIS, 1..=2, 7);
    let ledger = ledger(ChainLink::GENESIS, &members, 3, 7);
    let catalog = catalog(&members[..1]);
    let mut probes = whole(&members[..1], &members[1..], &ledger);
    probes
        .local
        .insert(3, open_probe(&ledger, 1, members[1].link().last_seq + 1));

    // Act
    let plan = plan(RecoveryPolicy::Strict, &catalog, Some(&ledger), &probes).expect("plan");

    // Assert: the unpublished member is owed; the open member replays last.
    assert_eq!(
        ids(&plan),
        vec![(1, Source::Remote), (2, Source::Local), (3, Source::Local)]
    );
    assert_eq!(
        plan.local_segments.keys().copied().collect::<Vec<_>>(),
        vec![2]
    );
    assert!(!plan.replay[2].sealed);
}

#[test]
fn should_preserve_a_joined_tail_when_planning_from_the_history_facts() {
    // Arrange: the catalog has retired the shared member, and the cache's
    // unpublished tail still belongs to that history.
    let members = chain(ChainLink::GENESIS, 1..=2, 7);
    let ledger = ledger(ChainLink::GENESIS, &members, 3, 7);
    let mut catalog = catalog(&members[..1]);
    catalog
        .retire_prefix(
            9,
            &[members[0].entry()],
            crate::lease::SharedGenerationId::new(1).unwrap(),
        )
        .expect("retire");
    let probes = whole(&[], &members[1..], &ledger);

    // Act
    let plan = plan(RecoveryPolicy::Strict, &catalog, Some(&ledger), &probes).expect("plan");

    // Assert: an empty catalog still names shared history via its anchor.
    assert_eq!(ids(&plan), vec![(2, Source::Local)]);
    assert_eq!(
        plan.history,
        HistoryAction::Keep {
            retire_local_below: Some(2)
        }
    );
}

#[test]
fn should_rebase_onto_the_catalog_when_another_writer_published_the_same_id() {
    // Arrange: the cache sealed its own segment 1; the catalog holds another
    // writer's segment 1.
    let members = chain(ChainLink::GENESIS, [1], 7);
    let ledger = ledger(ChainLink::GENESIS, &members, 2, 7);
    let foreign = foreign(1, 8, ChainLink::GENESIS);
    let catalog = catalog(std::slice::from_ref(&foreign));
    let probes = whole(std::slice::from_ref(&foreign), &[], &ledger);

    // Act
    let lineage = lineage(&catalog, Some(&ledger));
    let plan = plan(RecoveryPolicy::Strict, &catalog, Some(&ledger), &probes).expect("plan");

    // Assert: a fork, not a hole: strict recovery replays the catalog and
    // continues after it.
    assert_eq!(lineage, Lineage::Diverged);
    assert_eq!(ids(&plan), vec![(1, Source::Remote)]);
    assert!(!plan.opened_in_salvage_mode);
    assert_eq!(
        plan.history,
        HistoryAction::Rebase {
            anchor: foreign.link(),
            open_at_least: 2
        }
    );
}

#[test]
fn should_rebase_strictly_when_the_catalog_set_this_history_aside() {
    // Arrange: the cache retired 1 to 3 after publishing them; the catalog
    // then set 2 and 3 aside with a floor covering them.
    let published = chain(ChainLink::GENESIS, 1..=3, 7);
    let tail = chain(published[2].link(), [4], 7);
    let ledger = ledger(published[2].link(), &tail, 5, 7);
    let mut catalog = catalog(&published);
    catalog
        .set_aside_suffix(9, Some(2), &[published[1].entry(), published[2].entry()], 5)
        .expect("set aside");
    let probes = whole(&published[..1], &tail, &ledger);

    // Act
    let lineage = lineage(&catalog, Some(&ledger));
    let plan = plan(RecoveryPolicy::Strict, &catalog, Some(&ledger), &probes).expect("plan");

    // Assert
    assert_eq!(lineage, Lineage::Behind { covered: true });
    assert_eq!(ids(&plan), vec![(1, Source::Remote)]);
    assert_eq!(
        plan.history,
        HistoryAction::Rebase {
            anchor: published[0].link(),
            open_at_least: 4
        }
    );
    assert!(plan.max_unreplayed_sequence >= 5);
    assert!(!plan.changes_catalog(&catalog));
}

#[test]
fn should_fail_strict_recovery_when_the_catalog_lost_retired_history() {
    // Arrange: as above, but the catalog lost 2 and 3 with no floor.
    let published = chain(ChainLink::GENESIS, 1..=3, 7);
    let tail = chain(published[2].link(), [4], 7);
    let ledger = ledger(published[2].link(), &tail, 5, 7);
    let catalog = catalog(&published[..1]);
    let probes = whole(&published[..1], &tail, &ledger);
    let lineage = lineage(&catalog, Some(&ledger));

    // Act
    let strict = plan(RecoveryPolicy::Strict, &catalog, Some(&ledger), &probes);
    let salvage = plan(RecoveryPolicy::Salvage, &catalog, Some(&ledger), &probes);

    // Assert: salvage rebases and persists a floor over the lost history.
    assert_eq!(lineage, Lineage::Behind { covered: false });
    assert!(
        matches!(strict, Err(PlanError::CatalogBehind(_))),
        "{strict:?}"
    );
    let salvage = salvage.expect("salvage plans");
    assert!(salvage.opened_in_salvage_mode);
    assert!(salvage.catalog_floor >= published[2].link().last_seq);
    assert!(salvage.changes_catalog(&catalog));
    assert!(matches!(salvage.history, HistoryAction::Rebase { .. }));
}

#[test]
fn should_stop_at_the_first_segment_whose_header_breaks_the_chain() {
    // Arrange: the cache's unpublished segment 2 was written by an older
    // epoch than the catalog's segment 1.
    let published = chain(ChainLink::GENESIS, [1], 8);
    let tail = chain(published[0].link(), [2], 7);
    let ledger = ledger(published[0].link(), &tail, 3, 7);
    let catalog = catalog(&published);
    let probes = whole(&published, &tail, &ledger);

    // Act
    let strict = plan(RecoveryPolicy::Strict, &catalog, Some(&ledger), &probes);
    let salvage =
        plan(RecoveryPolicy::Salvage, &catalog, Some(&ledger), &probes).expect("salvage plans");

    // Assert
    assert!(
        matches!(&strict, Err(PlanError::Unreplayable(reason)) if reason.contains("below epoch")),
        "{strict:?}"
    );
    assert_eq!(ids(&salvage), vec![(1, Source::Remote)]);
    assert_eq!(
        salvage.history,
        HistoryAction::Rebase {
            anchor: published[0].link(),
            open_at_least: 2
        }
    );
    assert!(salvage.catalog_floor >= tail[0].link().last_seq);
}

#[test]
fn should_fall_back_to_the_cached_copy_when_a_shared_object_is_damaged() {
    // Arrange
    let members = chain(ChainLink::GENESIS, 1..=2, 7);
    let ledger = ledger(ChainLink::GENESIS, &members, 3, 7);
    let catalog = catalog(&members);
    let mut probes = whole(&members, &members[1..], &ledger);
    probes.remote.insert(
        2,
        Probe::Damaged {
            reason: "damaged object".into(),
            max_verified: 0,
        },
    );

    // Act
    let strict = plan(RecoveryPolicy::Strict, &catalog, Some(&ledger), &probes);
    let salvage =
        plan(RecoveryPolicy::Salvage, &catalog, Some(&ledger), &probes).expect("salvage plans");

    // Assert
    assert!(matches!(strict, Err(PlanError::Unreplayable(_))));
    assert_eq!(ids(&salvage), vec![(1, Source::Remote), (2, Source::Local)]);
    assert!(salvage.opened_in_salvage_mode);
    assert_eq!(salvage.retire_catalog_from, None);
    assert_eq!(
        salvage.local_segments.keys().copied().collect::<Vec<_>>(),
        vec![2]
    );
}

#[test]
fn should_set_aside_the_catalog_suffix_from_a_lost_object() {
    // Arrange: object 2 is lost and the cache holds no copy; the cache
    // holds its tail 4 after the catalog's 3.
    let published = chain(ChainLink::GENESIS, 1..=3, 7);
    let tail = chain(published[2].link(), [4], 7);
    let ledger = ledger(published[2].link(), &tail, 5, 7);
    let catalog = catalog(&published);
    let mut probes = whole(&published, &tail, &ledger);
    probes.remote.insert(
        2,
        Probe::Damaged {
            reason: "lost object".into(),
            max_verified: 0,
        },
    );

    // Act
    let plan =
        plan(RecoveryPolicy::Salvage, &catalog, Some(&ledger), &probes).expect("salvage plans");

    // Assert: one catalog change drops 2 and 3 and lifts the floor over the
    // cache's tail; every member is set aside behind the new head.
    assert_eq!(ids(&plan), vec![(1, Source::Remote)]);
    assert_eq!(plan.retire_catalog_from, Some(2));
    assert_eq!(
        plan.set_aside_entries,
        vec![published[1].entry(), published[2].entry()]
    );
    assert_eq!(plan.catalog_floor, tail[0].link().last_seq);
    assert_eq!(
        plan.history,
        HistoryAction::Rebase {
            anchor: published[0].link(),
            open_at_least: 4
        }
    );
}

#[test]
fn should_raise_floor_to_the_catalog_head_when_every_entry_retired() {
    // Arrange: every published segment retired.
    let published = chain(ChainLink::GENESIS, 1..=2, 7);
    let mut catalog = catalog(&published);
    catalog
        .retire_prefix(
            9,
            &[published[0].entry(), published[1].entry()],
            crate::lease::SharedGenerationId::new(1).unwrap(),
        )
        .expect("retire");

    // Act
    let plan = plan(RecoveryPolicy::Strict, &catalog, None, &Probes::default()).expect("plan");

    // Assert: nothing replays, but new history starts after the head.
    assert_eq!(plan.replay, []);
    assert_eq!(plan.max_unreplayed_sequence, published[1].link().last_seq);
    assert_eq!(
        plan.history,
        HistoryAction::Genesis {
            anchor: published[1].link(),
            open_at_least: 3
        }
    );
    assert!(!plan.changes_catalog(&catalog));
}

#[test]
fn should_cut_a_torn_open_member_without_keeping_a_copy() {
    // Arrange: the open member holds a record, then a torn frame.
    let ledger = ledger(ChainLink::GENESIS, &[], 1, 7);
    let catalog = WalPublicationCatalog::empty(9).expect("catalog");
    let mut probes = Probes::default();
    let open = ledger.open_member().open;
    probes.local.insert(
        1,
        Probe::Open(OpenProbe {
            header: open.header(ledger.holder().bytes()),
            len: 200,
            valid_bytes: 160,
            record_count: 1,
            max_sequence: 1,
            end: OpenEnd::Torn,
        }),
    );

    // Act
    let plan = plan(RecoveryPolicy::Strict, &catalog, Some(&ledger), &probes).expect("plan");

    // Assert
    assert_eq!(
        plan.open_cut,
        Some(OpenCut {
            id: 1,
            valid_bytes: 160,
            retain: false
        })
    );
    assert_eq!(ids(&plan), vec![(1, Source::Local)]);
    assert!(!plan.opened_in_salvage_mode);
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config { cases: 128, rng_seed: proptest::test_runner::RngSeed::Fixed(0x4d49_4447_4538_3434), ..proptest::test_runner::Config::default() })]
    #[test]
    fn should_replay_only_the_chain_prefix_before_the_first_hole(published in 0_u64..6, unpublished in 0_u64..4, damaged in proptest::collection::btree_set(1_u64..10, 0..3)) {
        // Arrange: a cache that published its first segments and holds the
        // rest; some objects or files are damaged.
        let members = chain(ChainLink::GENESIS, 1..=published + unpublished, 7);
        let open = published + unpublished + 1;
        let ledger = ledger(ChainLink::GENESIS, &members, open, 7);
        let catalog = catalog(&members[..usize::try_from(published).unwrap()]);
        let mut probes = whole(&members[..usize::try_from(published).unwrap()], &members[usize::try_from(published).unwrap()..], &ledger);
        for id in &damaged {
            let damage = Probe::Damaged { reason: format!("segment {id} damaged"), max_verified: 0 };
            if *id <= published {
                probes.remote.insert(*id, damage);
            } else if *id < open {
                probes.local.insert(*id, damage);
            }
        }
        let hole = damaged.iter().copied().find(|id| *id < open);

        // Act
        let strict = plan(RecoveryPolicy::Strict, &catalog, Some(&ledger), &probes);
        let salvage = plan(RecoveryPolicy::Salvage, &catalog, Some(&ledger), &probes).unwrap();

        // Assert
        proptest::prop_assert_eq!(strict.is_err(), hole.is_some());
        let replayed: Vec<SegmentId> = salvage.replay.iter().map(|segment| segment.id).filter(|id| *id < open).collect();
        let expected: Vec<SegmentId> = (1..hole.unwrap_or(open)).collect();
        proptest::prop_assert_eq!(replayed, expected);
        proptest::prop_assert_eq!(salvage.opened_in_salvage_mode, hole.is_some());
    }
}
