use super::*;

const EPOCH: u64 = 1;

fn holder() -> HolderId {
    HolderId::try_from("6a0d1d10-4c6b-4f51-9a0e-0a5fd2e1b2c3".to_string()).expect("holder")
}

fn digest(id: SegmentId) -> [u8; 16] {
    [u8::try_from(id % 251).expect("small"); 16]
}

fn opening(id: SegmentId, prev: ChainLink) -> OpenFacts {
    OpenFacts {
        id,
        epoch: EPOCH,
        first_seq: prev.last_seq + 1,
        prev,
    }
}

/// Facts sealing `member` with `records` records, one sequence each.
fn sealing(member: &OpenFacts, records: u64) -> SealedFacts {
    let last_seq = if records == 0 {
        member.prev.last_seq
    } else {
        member.first_seq + records - 1
    };
    SealedFacts {
        link: ChainLink {
            id: member.id,
            last_seq,
            digest: digest(member.id),
        },
        record_count: records,
        data_len: 128 + 32 * records,
        max_seq_per_cf: if records == 0 {
            Vec::new()
        } else {
            vec![(0, last_seq)]
        },
    }
}

fn ledger_at(open: SegmentId) -> WalLedger {
    WalLedger::genesis(
        holder(),
        ChainLink::GENESIS,
        opening(open, ChainLink::GENESIS),
    )
    .expect("genesis")
}

/// The rotation that seals `ledger`'s open member with `records` records.
/// The facts of segment `id` opening after `prev` in `ledger`, written by
/// its newest epoch, so the chain's epochs never go backwards.
fn opening_in(ledger: &WalLedger, id: SegmentId, prev: ChainLink) -> OpenFacts {
    OpenFacts {
        epoch: ledger.newest_epoch().max(EPOCH),
        ..opening(id, prev)
    }
}

fn rotation(ledger: &WalLedger, records: u64) -> LedgerEdit {
    let sealed = sealing(&ledger.open_member().open, records);
    let open = opening_in(ledger, ledger.next_segment_id(), sealed.link);
    LedgerEdit::Rotate { sealed, open }
}

fn rotate(ledger: &mut WalLedger, records: u64) {
    let edit = rotation(ledger, records);
    ledger.apply(&edit).expect("rotation applies");
}

/// A ledger holding `first..=last`, each sealed member with two records,
/// with `last` open.
fn ledger_spanning(first: SegmentId, last: SegmentId) -> WalLedger {
    let mut ledger = ledger_at(first);
    while ledger.open_segment() < last {
        rotate(&mut ledger, 2);
    }
    ledger
}

/// A set-aside of every member from `from`, opening `open` linked to the
/// last kept member, or the anchor.
fn setting_aside(ledger: &WalLedger, from: SegmentId, open: SegmentId) -> LedgerEdit {
    let prev = ledger
        .member_facts()
        .filter(|member| member.id() < from)
        .last()
        .and_then(|member| member.sealed.as_ref())
        .map_or(ledger.anchor(), |sealed| sealed.link);
    LedgerEdit::SetAside {
        from,
        cut: None,
        open: opening_in(ledger, open, prev),
    }
}

fn sealed_facts(ledger: &WalLedger, id: SegmentId) -> Option<&SealedFacts> {
    ledger
        .expected(id)
        .and_then(|member| member.sealed.as_ref())
}

fn members(ledger: &WalLedger) -> Vec<SegmentId> {
    ledger.members().collect()
}

fn sealed(ledger: &WalLedger) -> Vec<SegmentId> {
    ledger.sealed().collect()
}

/// Apply `edit`, which must fail, and prove the ledger did not change.
fn rejected(ledger: &WalLedger, edit: &LedgerEdit) -> LedgerError {
    let mut attempt = ledger.clone();
    let error = attempt.apply(edit).expect_err("the edit must be rejected");
    assert_eq!(&attempt, ledger, "a rejected edit changed the ledger");
    error
}

fn assert_invariants(ledger: &WalLedger) {
    let members = members(ledger);
    assert!(
        members.windows(2).all(|pair| pair[0] < pair[1]),
        "members are not strictly increasing: {members:?}"
    );
    assert_eq!(members.last().copied(), Some(ledger.open_segment()));
    assert!(ledger.next_segment_id() > ledger.open_segment());
    assert_eq!(members.first().copied(), Some(ledger.log_start()));
    assert_eq!(sealed(ledger), members[..members.len() - 1]);
    assert!(members.iter().all(|id| ledger.is_member(*id)));
    let mut prev = ledger.anchor();
    for member in ledger.member_facts() {
        assert_eq!(
            member.open.prev,
            prev,
            "member {} breaks the chain",
            member.id()
        );
        if let Some(sealed) = &member.sealed {
            prev = sealed.link;
        }
    }
    let image = serde_json::to_vec(ledger).expect("serialize ledger");
    let decoded: WalLedger = serde_json::from_slice(&image).expect("reachable state decodes");
    assert_eq!(&decoded, ledger);
}

/// `SplitMix64`: a small seeded generator for property-style loops.
struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut mixed = self.0;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        mixed ^ (mixed >> 31)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }

    /// An id within a few of `around`, on either side.
    fn near(&mut self, around: SegmentId) -> SegmentId {
        (around + self.below(5)).saturating_sub(2)
    }
}

/// A random edit that is valid for `ledger`.
fn valid_edit(ledger: &WalLedger, random: &mut SplitMix64) -> LedgerEdit {
    let open = ledger.open_segment();
    let next = ledger.next_segment_id();
    let log_start = ledger.log_start();
    match random.below(6) {
        1 if log_start < open => LedgerEdit::AdvanceLogStart {
            to: log_start + 1 + random.below(open - log_start),
        },
        2 => setting_aside(
            ledger,
            log_start + random.below(open - log_start + 1),
            next + random.below(3),
        ),
        3 => LedgerEdit::Reserve {
            through: ledger.seq_reserved_through() + 1 + random.below(1 << 20),
        },
        4 => {
            let member = ledger.open_member().open;
            LedgerEdit::Reopen {
                open: OpenFacts {
                    id: next,
                    epoch: member.epoch + random.below(2),
                    first_seq: member.first_seq + random.below(3),
                    prev: member.prev,
                },
            }
        }
        _ => rotation(ledger, random.below(3)),
    }
}

/// A random edit near `ledger`'s current ids, valid or not.
fn arbitrary_edit(ledger: &WalLedger, random: &mut SplitMix64) -> LedgerEdit {
    let past_log_start = ledger.log_start() + random.below(4);
    match random.below(5) {
        0 => {
            let LedgerEdit::Rotate {
                mut sealed,
                mut open,
            } = rotation(ledger, random.below(3))
            else {
                unreachable!("a rotation")
            };
            sealed.link.id = random.near(sealed.link.id);
            open.id = random.near(open.id);
            if random.below(3) == 0 {
                open.prev.last_seq = random.near(open.prev.last_seq);
            }
            LedgerEdit::Rotate { sealed, open }
        }
        1 => LedgerEdit::AdvanceLogStart {
            to: random.near(past_log_start),
        },
        3 => LedgerEdit::Reserve {
            through: random.near(ledger.seq_reserved_through()),
        },
        _ => setting_aside(
            ledger,
            random.near(past_log_start),
            random.near(ledger.next_segment_id()),
        ),
    }
}

/// The members and next segment id a valid `edit` leaves, computed from
/// the definitions rather than the implementation.
fn modelled(ledger: &WalLedger, edit: &LedgerEdit) -> (Vec<SegmentId>, SegmentId) {
    let members = members(ledger);
    match edit {
        LedgerEdit::Rotate { open, .. } => {
            let mut members = members;
            members.push(open.id);
            (members, open.id + 1)
        }
        LedgerEdit::Reopen { open } => {
            let mut members = members;
            members.pop();
            members.push(open.id);
            (members, open.id + 1)
        }
        LedgerEdit::AdvanceLogStart { to } => (
            members.into_iter().filter(|id| id >= to).collect(),
            ledger.next_segment_id(),
        ),
        LedgerEdit::SetAside { from, open, .. } => {
            let mut members: Vec<_> = members.into_iter().filter(|id| id < from).collect();
            members.push(open.id);
            (members, open.id + 1)
        }
        LedgerEdit::Rebase { open, .. } => (vec![open.id], open.id + 1),
        LedgerEdit::Reserve { .. } => (members, ledger.next_segment_id()),
    }
}

#[test]
fn should_open_first_segment_when_ledger_starts() {
    // Arrange
    let open = opening(7, ChainLink::GENESIS);

    // Act
    let ledger = WalLedger::genesis(holder(), ChainLink::GENESIS, open).expect("genesis");

    // Assert
    assert_eq!(ledger.holder(), holder());
    assert_eq!(members(&ledger), [7]);
    assert_eq!(ledger.open_member().open, open);
    assert_eq!(ledger.next_segment_id(), 8);
    assert_eq!(ledger.anchor(), ChainLink::GENESIS);
    assert_invariants(&ledger);
}

#[test]
fn should_reject_genesis_when_open_does_not_link_the_anchor() {
    // Arrange
    let anchor = ChainLink {
        id: 4,
        last_seq: 9,
        digest: digest(4),
    };

    // Act
    let result = WalLedger::genesis(holder(), anchor, opening(5, ChainLink::GENESIS));

    // Assert
    assert!(matches!(result, Err(LedgerError::BrokenChain(_))));
}

#[test]
fn should_seal_open_segment_when_rotating() {
    // Arrange
    let mut ledger = ledger_at(1);
    let edit = rotation(&ledger, 3);

    // Act
    ledger.apply(&edit).expect("rotation applies");

    // Assert
    assert_eq!(members(&ledger), [1, 2]);
    assert_eq!(sealed(&ledger), [1]);
    let LedgerEdit::Rotate { sealed, open } = edit else {
        unreachable!("a rotation")
    };
    assert_eq!(sealed_facts(&ledger, 1), Some(&sealed));
    assert_eq!(ledger.open_member().open, open);
    assert_invariants(&ledger);
}

#[test]
fn should_reject_rotate_when_seal_is_not_open_segment() {
    // Arrange
    let ledger = ledger_spanning(1, 2);
    let LedgerEdit::Rotate { mut sealed, open } = rotation(&ledger, 1) else {
        unreachable!("a rotation")
    };
    sealed.link.id = 1;

    // Act
    let error = rejected(&ledger, &LedgerEdit::Rotate { sealed, open });

    // Assert
    assert_eq!(
        error,
        LedgerError::NotOpenSegment {
            seal: 1,
            open_segment: 2
        }
    );
}

#[test]
fn should_reject_rotate_when_open_is_not_next_segment() {
    // Arrange
    let ledger = ledger_at(1);
    let LedgerEdit::Rotate { sealed, mut open } = rotation(&ledger, 1) else {
        unreachable!("a rotation")
    };
    open.id = 3;

    // Act
    let error = rejected(&ledger, &LedgerEdit::Rotate { sealed, open });

    // Assert
    assert_eq!(
        error,
        LedgerError::NotNextSegment {
            open: 3,
            next_segment_id: 2
        }
    );
}

#[test]
fn should_reject_rotate_when_open_does_not_link_sealed_facts() {
    // Arrange: the next header names a different digest than the footer.
    let ledger = ledger_at(1);
    let LedgerEdit::Rotate { sealed, mut open } = rotation(&ledger, 2) else {
        unreachable!("a rotation")
    };
    open.prev.digest = [0xee; 16];

    // Act
    let error = rejected(&ledger, &LedgerEdit::Rotate { sealed, open });

    // Assert
    assert!(matches!(error, LedgerError::BrokenChain(_)), "{error:?}");
}

#[test]
fn should_reject_rotate_when_open_starts_at_or_below_sealed_last_sequence() {
    // Arrange
    let ledger = ledger_at(1);
    let LedgerEdit::Rotate { sealed, mut open } = rotation(&ledger, 2) else {
        unreachable!("a rotation")
    };
    open.first_seq = sealed.link.last_seq;

    // Act
    let error = rejected(&ledger, &LedgerEdit::Rotate { sealed, open });

    // Assert
    assert!(matches!(error, LedgerError::BrokenChain(_)), "{error:?}");
}

#[test]
fn should_reject_rotate_when_epoch_falls() {
    // Arrange
    let ledger = WalLedger::genesis(
        holder(),
        ChainLink::GENESIS,
        OpenFacts {
            epoch: 5,
            ..opening(1, ChainLink::GENESIS)
        },
    )
    .expect("genesis");
    let LedgerEdit::Rotate { sealed, mut open } = rotation(&ledger, 1) else {
        unreachable!("a rotation")
    };
    open.epoch = 4;

    // Act
    let error = rejected(&ledger, &LedgerEdit::Rotate { sealed, open });

    // Assert
    assert!(matches!(error, LedgerError::BrokenChain(_)), "{error:?}");
}

#[test]
fn should_retire_prefix_when_log_start_advances() {
    // Arrange
    let mut ledger = ledger_spanning(1, 4);

    // Act
    ledger
        .apply(&LedgerEdit::AdvanceLogStart { to: 3 })
        .expect("advance applies");

    // Assert
    assert_eq!(members(&ledger), [3, 4]);
    assert_eq!(ledger.log_start(), 3);
    assert_invariants(&ledger);
}

#[test]
fn should_keep_anchor_of_last_retired_segment_when_log_start_advances() {
    // Arrange
    let mut ledger = ledger_spanning(1, 4);
    let retired = sealed_facts(&ledger, 2).expect("segment 2 is sealed").link;

    // Act
    ledger
        .apply(&LedgerEdit::AdvanceLogStart { to: 3 })
        .expect("advance applies");

    // Assert
    assert_eq!(ledger.anchor(), retired);
    assert_eq!(
        ledger.expected(3).expect("member 3").open.prev,
        ledger.anchor()
    );
}

#[test]
fn should_retire_every_sealed_segment_when_log_start_reaches_open_segment() {
    // Arrange
    let mut ledger = ledger_spanning(1, 3);

    // Act
    ledger
        .apply(&LedgerEdit::AdvanceLogStart { to: 3 })
        .expect("advance applies");

    // Assert
    assert_eq!(members(&ledger), [3]);
    assert_eq!(sealed(&ledger), Vec::<SegmentId>::new());
    assert_invariants(&ledger);
}

#[test]
fn should_reject_log_start_when_target_does_not_exceed_current() {
    // Arrange
    let ledger = ledger_spanning(2, 4);

    // Act
    let errors = [1, 2].map(|to| rejected(&ledger, &LedgerEdit::AdvanceLogStart { to }));

    // Assert
    for (error, to) in errors.into_iter().zip([1, 2]) {
        assert_eq!(
            error,
            LedgerError::BadLogStart {
                to,
                log_start: 2,
                open_segment: 4
            }
        );
    }
}

#[test]
fn should_reject_log_start_when_target_passes_open_segment() {
    // Arrange
    let ledger = ledger_spanning(1, 3);

    // Act
    let error = rejected(&ledger, &LedgerEdit::AdvanceLogStart { to: 4 });

    // Assert
    assert_eq!(
        error,
        LedgerError::BadLogStart {
            to: 4,
            log_start: 1,
            open_segment: 3
        }
    );
}

#[test]
fn should_open_past_gap_when_setting_aside() {
    // Arrange
    let mut ledger = ledger_spanning(1, 4);

    // Act
    ledger
        .apply(&setting_aside(&ledger, 3, 9))
        .expect("set-aside applies");

    // Assert
    assert_eq!(members(&ledger), [1, 2, 9]);
    assert_eq!(ledger.next_segment_id(), 10);
    assert_eq!(
        ledger.open_member().open.prev,
        sealed_facts(&ledger, 2).expect("2 sealed").link
    );
    assert_invariants(&ledger);
}

#[test]
fn should_drop_every_member_when_set_aside_starts_at_log_start() {
    // Arrange
    let mut ledger = ledger_spanning(2, 4);
    ledger
        .apply(&LedgerEdit::AdvanceLogStart { to: 3 })
        .expect("advance applies");
    let anchor = ledger.anchor();

    // Act
    ledger
        .apply(&setting_aside(&ledger, 3, 5))
        .expect("set-aside applies");

    // Assert
    assert_eq!(members(&ledger), [5]);
    assert_eq!(ledger.open_member().open.prev, anchor);
    assert_invariants(&ledger);
}

#[test]
fn should_reseal_cut_member_when_setting_aside_after_it() {
    // Arrange: salvage keeps one record of the open segment 3.
    let mut ledger = ledger_spanning(1, 3);
    let cut = sealing(&ledger.open_member().open, 1);
    let open = opening(4, cut.link);

    // Act
    ledger
        .apply(&LedgerEdit::SetAside {
            from: 4,
            cut: Some(cut.clone()),
            open,
        })
        .expect("set-aside applies");

    // Assert
    assert_eq!(members(&ledger), [1, 2, 3, 4]);
    assert_eq!(sealed_facts(&ledger, 3), Some(&cut));
    assert_invariants(&ledger);
}

#[test]
fn should_reject_set_aside_when_kept_member_is_unsealed_without_cut() {
    // Arrange: from passes the open segment, which nothing seals.
    let ledger = ledger_spanning(1, 3);
    let open = opening(4, sealed_facts(&ledger, 2).expect("sealed").link);

    // Act
    let error = rejected(
        &ledger,
        &LedgerEdit::SetAside {
            from: 4,
            cut: None,
            open,
        },
    );

    // Assert
    assert!(matches!(error, LedgerError::BrokenChain(_)), "{error:?}");
}

#[test]
fn should_reject_set_aside_when_from_precedes_log_start() {
    // Arrange
    let mut ledger = ledger_spanning(1, 4);
    ledger
        .apply(&LedgerEdit::AdvanceLogStart { to: 2 })
        .expect("advance applies");

    // Act
    let error = rejected(&ledger, &setting_aside(&ledger, 1, 6));

    // Assert
    assert_eq!(
        error,
        LedgerError::BadSetAside {
            from: 1,
            open: 6,
            log_start: 2,
            next_segment_id: 5,
        }
    );
}

#[test]
fn should_reject_set_aside_when_open_reuses_allocated_id() {
    // Arrange
    let ledger = ledger_spanning(1, 5);

    // Act
    let error = rejected(&ledger, &setting_aside(&ledger, 2, 4));

    // Assert
    assert_eq!(
        error,
        LedgerError::BadSetAside {
            from: 2,
            open: 4,
            log_start: 1,
            next_segment_id: 6,
        }
    );
}

#[test]
fn should_rotate_past_gap_when_set_aside_applied() {
    // Arrange
    let mut ledger = ledger_spanning(1, 3);
    ledger
        .apply(&setting_aside(&ledger, 2, 8))
        .expect("set-aside applies");

    // Act
    rotate(&mut ledger, 1);

    // Assert
    assert_eq!(members(&ledger), [1, 8, 9]);
    assert_eq!(sealed(&ledger), [1, 8]);
    assert_eq!(ledger.next_segment_id(), 10);
    assert_invariants(&ledger);
}

#[test]
fn should_skip_gap_when_log_start_advances_into_it() {
    // Arrange
    let mut ledger = ledger_spanning(1, 3);
    ledger
        .apply(&setting_aside(&ledger, 3, 8))
        .expect("set-aside applies");
    rotate(&mut ledger, 1);

    // Act
    ledger
        .apply(&LedgerEdit::AdvanceLogStart { to: 5 })
        .expect("advance applies");

    // Assert
    assert_eq!(members(&ledger), [8, 9]);
    assert_eq!(ledger.log_start(), 8);
    assert!(!ledger.is_member(5));
    assert_eq!(ledger.anchor(), ledger.expected(8).expect("8").open.prev);
}

#[test]
fn should_replace_open_member_when_reopening() {
    // Arrange: a new epoch replaces the open segment that holds no record.
    let mut ledger = ledger_spanning(1, 2);
    let replaced = ledger.open_member().open;
    let open = OpenFacts {
        id: 3,
        epoch: EPOCH + 1,
        first_seq: replaced.first_seq + 100,
        prev: replaced.prev,
    };

    // Act
    ledger
        .apply(&LedgerEdit::Reopen { open })
        .expect("reopen applies");

    // Assert
    assert_eq!(members(&ledger), [1, 3]);
    assert_eq!(ledger.open_member().open, open);
    assert_eq!(ledger.next_segment_id(), 4);
    assert_invariants(&ledger);
}

#[test]
fn should_reject_reopen_when_prev_changes() {
    // Arrange
    let ledger = ledger_spanning(1, 2);
    let replaced = ledger.open_member().open;
    let open = OpenFacts {
        id: 3,
        prev: ChainLink::GENESIS,
        ..replaced
    };

    // Act
    let error = rejected(&ledger, &LedgerEdit::Reopen { open });

    // Assert
    assert!(matches!(error, LedgerError::BrokenChain(_)), "{error:?}");
}

#[test]
fn should_rebase_onto_anchor_when_failing_back() {
    // Arrange: another writer published through segment 9.
    let mut ledger = ledger_spanning(1, 3);
    let head = ChainLink {
        id: 9,
        last_seq: 900,
        digest: digest(9),
    };
    let open = opening(10, head);

    // Act
    ledger
        .apply(&LedgerEdit::Rebase { anchor: head, open })
        .expect("rebase applies");

    // Assert
    assert_eq!(members(&ledger), [10]);
    assert_eq!(ledger.anchor(), head);
    assert_eq!(ledger.next_segment_id(), 11);
    assert_invariants(&ledger);
}

#[test]
fn should_reject_rebase_when_open_reuses_allocated_id() {
    // Arrange
    let ledger = ledger_spanning(1, 3);
    let head = ChainLink {
        id: 2,
        last_seq: 900,
        digest: digest(2),
    };

    // Act
    let error = rejected(
        &ledger,
        &LedgerEdit::Rebase {
            anchor: head,
            open: opening(3, head),
        },
    );

    // Assert
    assert_eq!(
        error,
        LedgerError::NotNextSegment {
            open: 3,
            next_segment_id: 4
        }
    );
}

#[test]
fn should_reject_edit_when_segment_ids_are_exhausted() {
    // Arrange
    let last = ledger_at(u64::MAX - 1);

    // Act
    let errors = [
        WalLedger::genesis(
            holder(),
            ChainLink::GENESIS,
            opening(u64::MAX, ChainLink::GENESIS),
        )
        .expect_err("no id follows the last"),
        rejected(&last, &rotation(&last, 1)),
        rejected(&last, &setting_aside(&last, u64::MAX - 1, u64::MAX)),
    ];

    // Assert
    assert_eq!(errors, [const { LedgerError::SegmentIdsExhausted }; 3]);
}

#[test]
fn should_raise_reservation_when_reserving() {
    // Arrange
    let mut ledger = ledger_at(1);

    // Act
    ledger
        .apply(&LedgerEdit::Reserve { through: 1 << 20 })
        .expect("a raise applies");

    // Assert
    assert_eq!(ledger.seq_reserved_through(), 1 << 20);
    assert_invariants(&ledger);
}

#[test]
fn should_reject_reserve_when_it_does_not_raise_the_reservation() {
    // Arrange
    let mut ledger = ledger_at(1);
    ledger
        .apply(&LedgerEdit::Reserve { through: 100 })
        .expect("a raise applies");

    // Act
    let errors = [100, 99].map(|through| rejected(&ledger, &LedgerEdit::Reserve { through }));

    // Assert
    for (error, through) in errors.into_iter().zip([100, 99]) {
        assert_eq!(
            error,
            LedgerError::ReservationNotRaised {
                through,
                reserved_through: 100,
            }
        );
    }
}

#[test]
fn should_hold_invariants_when_random_valid_edits_apply() {
    // Arrange
    for seed in 0..64 {
        let mut random = SplitMix64(seed);
        let mut ledger = ledger_at(1 + random.below(10));

        // Act
        for step in 0..200 {
            let edit = valid_edit(&ledger, &mut random);
            let (expected_members, expected_next) = modelled(&ledger, &edit);
            let before = ledger.clone();
            ledger
                .apply(&edit)
                .unwrap_or_else(|error| panic!("seed {seed} step {step}: {edit:?}: {error}"));

            // Assert
            assert_invariants(&ledger);
            assert_eq!(members(&ledger), expected_members, "{edit:?}");
            assert_eq!(ledger.next_segment_id(), expected_next, "{edit:?}");
            assert!(ledger.log_start() >= before.log_start());
            assert_eq!(ledger.holder(), before.holder());
        }
    }
}

#[test]
fn should_leave_ledger_unchanged_when_random_edit_is_rejected() {
    // Arrange
    let mut accepted = 0;
    let mut refused = 0;
    for seed in 0..64 {
        let mut random = SplitMix64(seed ^ 0x5eed);
        let mut ledger = ledger_at(1 + random.below(10));

        // Act
        for _ in 0..200 {
            let edit = arbitrary_edit(&ledger, &mut random);
            let before = ledger.clone();
            let modelled = modelled(&ledger, &edit);
            let result = ledger.apply(&edit);

            // Assert
            if result.is_ok() {
                accepted += 1;
                assert_eq!((members(&ledger), ledger.next_segment_id()), modelled);
            } else {
                refused += 1;
                assert_eq!(ledger, before, "rejected {edit:?} changed the ledger");
            }
            assert_invariants(&ledger);
        }
    }
    assert!(accepted > 1000, "too few accepted edits: {accepted}");
    assert!(refused > 1000, "too few rejected edits: {refused}");
}

#[test]
fn should_round_trip_when_ledger_is_serialized() {
    // Arrange
    let mut ledger = ledger_spanning(2, 6);
    ledger
        .apply(&setting_aside(&ledger, 5, 11))
        .expect("set-aside applies");
    ledger
        .apply(&LedgerEdit::Reserve { through: 77 })
        .expect("reserve applies");

    // Act
    let image = serde_json::to_vec(&ledger).expect("serialize ledger");
    let decoded: WalLedger = serde_json::from_slice(&image).expect("deserialize ledger");

    // Assert
    assert_eq!(decoded, ledger);
}

#[test]
fn should_reject_snapshot_when_state_is_unreachable() {
    // Arrange: damage a reachable image one fact at a time.
    let ledger = ledger_spanning(2, 4);
    let image = serde_json::to_value(&ledger).expect("serialize");
    let damaged = |damage: &dyn Fn(&mut serde_json::Value)| {
        let mut image = image.clone();
        damage(&mut image);
        image
    };
    let images = [
        damaged(&|image| image["members"] = serde_json::json!([])),
        damaged(&|image| image["next_segment_id"] = serde_json::json!(9)),
        damaged(&|image| image["holder"] = serde_json::json!("")),
        damaged(&|image| {
            image["holder"] = serde_json::json!("00000000-0000-0000-0000-000000000000");
        }),
        damaged(&|image| image["anchor"]["last_seq"] = serde_json::json!(5)),
        damaged(&|image| {
            image["members"][1]["open"]["prev"]["digest"][0] = serde_json::json!(9);
        }),
        damaged(&|image| image["members"][2]["sealed"] = image["members"][1]["sealed"].clone()),
        damaged(&|image| image["members"][1]["sealed"] = serde_json::Value::Null),
        damaged(&|image| image["members"][0]["open"]["epoch"] = serde_json::json!(9)),
        damaged(&|image| image["extra"] = serde_json::json!(1)),
    ];

    // Act
    let results = images
        .iter()
        .map(|image| serde_json::from_value::<WalLedger>(image.clone()))
        .collect::<Vec<_>>();

    // Assert
    for (result, image) in results.iter().zip(&images) {
        assert!(result.is_err(), "unreachable state decoded: {image}");
    }
}

#[test]
fn should_classify_ledger_errors_when_converting() {
    // Arrange
    let refused = LedgerError::NotOpenSegment {
        seal: 3,
        open_segment: 4,
    };
    let unreachable = LedgerError::UnreachableState("the members do not chain");

    // Act
    let converted = [
        crate::common::MidgeError::from(refused.clone()),
        crate::common::MidgeError::from(LedgerError::SegmentIdsExhausted),
        crate::common::MidgeError::from(unreachable.clone()),
    ];

    // Assert: a refused edit is a caller's mistake, exhaustion a limit, and
    // only an unreachable stored state is damage.
    assert!(
        matches!(&converted[0], crate::common::MidgeError::Internal(message)
            if message.contains(&refused.to_string())),
        "{:?}",
        converted[0]
    );
    assert!(matches!(
        converted[1],
        crate::common::MidgeError::ResourceLimit(_)
    ));
    assert!(
        matches!(&converted[2], crate::common::MidgeError::Corruption(message)
            if message.contains(&unreachable.to_string())),
        "{:?}",
        converted[2]
    );
}
