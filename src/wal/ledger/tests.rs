use super::*;

fn ledger_at(open: SegmentId) -> WalLedger {
    WalLedger::genesis("holder-a", open).expect("genesis")
}

/// A ledger holding `first..=last`, with `last` open.
fn ledger_spanning(first: SegmentId, last: SegmentId) -> WalLedger {
    let mut ledger = ledger_at(first);
    for seal in first..last {
        ledger
            .apply(&LedgerEdit::Rotate {
                seal,
                open: seal + 1,
            })
            .expect("rotation applies");
    }
    ledger
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
    assert_ne!(ledger.holder(), "");
    assert!(
        members.windows(2).all(|pair| pair[0] < pair[1]),
        "members are not strictly increasing: {members:?}"
    );
    assert_eq!(members.last().copied(), Some(ledger.open_segment()));
    assert!(ledger.next_segment_id() > ledger.open_segment());
    assert_eq!(members.first().copied(), Some(ledger.log_start()));
    assert_eq!(sealed(ledger), members[..members.len() - 1]);
    assert!(members.iter().all(|id| ledger.is_member(*id)));
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
    match random.below(4) {
        1 if log_start < open => LedgerEdit::AdvanceLogStart {
            to: log_start + 1 + random.below(open - log_start),
        },
        2 => LedgerEdit::SetAside {
            from: log_start + random.below(open - log_start + 3),
            open: next + random.below(3),
        },
        3 => LedgerEdit::Reserve {
            through: ledger.seq_reserved_through() + 1 + random.below(1 << 20),
        },
        _ => LedgerEdit::Rotate {
            seal: open,
            open: next,
        },
    }
}

/// A random edit near `ledger`'s current ids, valid or not.
fn arbitrary_edit(ledger: &WalLedger, random: &mut SplitMix64) -> LedgerEdit {
    let past_log_start = ledger.log_start() + random.below(4);
    match random.below(4) {
        0 => LedgerEdit::Rotate {
            seal: random.near(ledger.open_segment()),
            open: random.near(ledger.next_segment_id()),
        },
        3 => LedgerEdit::Reserve {
            through: random.near(ledger.seq_reserved_through()),
        },
        1 => LedgerEdit::AdvanceLogStart {
            to: random.near(past_log_start),
        },
        _ => LedgerEdit::SetAside {
            from: random.near(past_log_start),
            open: random.near(ledger.next_segment_id()),
        },
    }
}

/// The members and next segment id a valid `edit` leaves, computed from
/// the definitions rather than the implementation.
fn modelled(ledger: &WalLedger, edit: &LedgerEdit) -> (Vec<SegmentId>, SegmentId) {
    let members = members(ledger);
    match edit {
        LedgerEdit::Rotate { open, .. } => {
            let mut members = members;
            members.push(*open);
            (members, open + 1)
        }
        LedgerEdit::AdvanceLogStart { to } => (
            members.into_iter().filter(|id| id >= to).collect(),
            ledger.next_segment_id(),
        ),
        LedgerEdit::SetAside { from, open } => {
            let mut members: Vec<_> = members.into_iter().filter(|id| id < from).collect();
            members.push(*open);
            (members, open + 1)
        }
        LedgerEdit::Reserve { .. } => (members, ledger.next_segment_id()),
    }
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
    let same = rejected(&ledger, &LedgerEdit::Reserve { through: 100 });
    let lower = rejected(&ledger, &LedgerEdit::Reserve { through: 99 });

    // Assert
    for error in [same, lower] {
        assert_eq!(
            error,
            LedgerError::ReservationNotRaised {
                through: error_through(&error),
                reserved_through: 100,
            }
        );
    }
}

fn error_through(error: &LedgerError) -> u64 {
    match error {
        LedgerError::ReservationNotRaised { through, .. } => *through,
        other => panic!("unexpected ledger error {other:?}"),
    }
}

#[test]
fn should_open_first_segment_when_ledger_starts() {
    // Arrange
    let holder = "holder-a";

    // Act
    let ledger = WalLedger::genesis(holder, 7).expect("genesis");

    // Assert
    assert_eq!(ledger.holder(), "holder-a");
    assert_eq!(members(&ledger), [7]);
    assert_eq!(sealed(&ledger), Vec::<SegmentId>::new());
    assert_eq!(ledger.open_segment(), 7);
    assert_eq!(ledger.log_start(), 7);
    assert_eq!(ledger.next_segment_id(), 8);
}

#[test]
fn should_reject_genesis_when_holder_is_empty() {
    // Arrange
    let holder = String::new();

    // Act
    let result = WalLedger::genesis(holder, 1);

    // Assert
    assert_eq!(result, Err(LedgerError::EmptyHolder));
}

#[test]
fn should_seal_open_segment_when_rotating() {
    // Arrange
    let mut ledger = ledger_at(3);

    // Act
    ledger
        .apply(&LedgerEdit::Rotate { seal: 3, open: 4 })
        .expect("rotation applies");

    // Assert
    assert_eq!(members(&ledger), [3, 4]);
    assert_eq!(sealed(&ledger), [3]);
    assert_eq!(ledger.open_segment(), 4);
    assert_eq!(ledger.log_start(), 3);
    assert_eq!(ledger.next_segment_id(), 5);
}

#[test]
fn should_reject_rotate_when_seal_is_not_open_segment() {
    // Arrange
    let ledger = ledger_spanning(3, 4);

    // Act
    let error = rejected(&ledger, &LedgerEdit::Rotate { seal: 3, open: 5 });

    // Assert
    assert_eq!(
        error,
        LedgerError::NotOpenSegment {
            seal: 3,
            open_segment: 4,
        }
    );
}

#[test]
fn should_reject_rotate_when_open_is_not_next_segment() {
    // Arrange
    let ledger = ledger_spanning(3, 4);

    // Act
    let errors = [4, 6, 0].map(|open| rejected(&ledger, &LedgerEdit::Rotate { seal: 4, open }));

    // Assert
    for (error, open) in errors.into_iter().zip([4, 6, 0]) {
        assert_eq!(
            error,
            LedgerError::NotNextSegment {
                open,
                next_segment_id: 5,
            }
        );
    }
}

#[test]
fn should_retire_prefix_when_log_start_advances() {
    // Arrange
    let mut ledger = ledger_spanning(1, 5);

    // Act
    ledger
        .apply(&LedgerEdit::AdvanceLogStart { to: 3 })
        .expect("advance applies");

    // Assert
    assert_eq!(members(&ledger), [3, 4, 5]);
    assert_eq!(sealed(&ledger), [3, 4]);
    assert_eq!(ledger.log_start(), 3);
    assert_eq!(ledger.open_segment(), 5);
    assert_eq!(ledger.next_segment_id(), 6);
    assert!(!ledger.is_member(2));
}

#[test]
fn should_retire_every_sealed_segment_when_log_start_reaches_open_segment() {
    // Arrange
    let mut ledger = ledger_spanning(1, 5);

    // Act
    ledger
        .apply(&LedgerEdit::AdvanceLogStart { to: 5 })
        .expect("advance applies");

    // Assert
    assert_eq!(members(&ledger), [5]);
    assert_eq!(sealed(&ledger), Vec::<SegmentId>::new());
    assert_eq!(ledger.log_start(), 5);
}

#[test]
fn should_reject_log_start_when_target_does_not_exceed_current() {
    // Arrange
    let mut ledger = ledger_spanning(1, 5);
    ledger
        .apply(&LedgerEdit::AdvanceLogStart { to: 3 })
        .expect("advance applies");

    // Act
    let errors = [3, 2, 0].map(|to| rejected(&ledger, &LedgerEdit::AdvanceLogStart { to }));

    // Assert
    for (error, to) in errors.into_iter().zip([3, 2, 0]) {
        assert_eq!(
            error,
            LedgerError::BadLogStart {
                to,
                log_start: 3,
                open_segment: 5,
            }
        );
    }
}

#[test]
fn should_reject_log_start_when_target_passes_open_segment() {
    // Arrange
    let ledger = ledger_spanning(1, 5);

    // Act
    let error = rejected(&ledger, &LedgerEdit::AdvanceLogStart { to: 6 });

    // Assert
    assert_eq!(
        error,
        LedgerError::BadLogStart {
            to: 6,
            log_start: 1,
            open_segment: 5,
        }
    );
}

#[test]
fn should_open_past_gap_when_setting_aside() {
    // Arrange
    let mut ledger = ledger_spanning(1, 5);

    // Act
    ledger
        .apply(&LedgerEdit::SetAside { from: 3, open: 9 })
        .expect("set-aside applies");

    // Assert
    assert_eq!(members(&ledger), [1, 2, 9]);
    assert_eq!(sealed(&ledger), [1, 2]);
    assert_eq!(ledger.open_segment(), 9);
    assert_eq!(ledger.log_start(), 1);
    assert_eq!(ledger.next_segment_id(), 10);
    assert!(!ledger.is_member(3));
    assert!(!ledger.is_member(5));
}

#[test]
fn should_drop_every_member_when_set_aside_starts_at_log_start() {
    // Arrange
    let mut ledger = ledger_spanning(1, 5);

    // Act
    ledger
        .apply(&LedgerEdit::SetAside { from: 1, open: 6 })
        .expect("set-aside applies");

    // Assert
    assert_eq!(members(&ledger), [6]);
    assert_eq!(ledger.log_start(), 6);
    assert_eq!(ledger.next_segment_id(), 7);
}

#[test]
fn should_seal_open_segment_when_set_aside_starts_past_it() {
    // Arrange
    let mut ledger = ledger_spanning(1, 2);

    // Act
    ledger
        .apply(&LedgerEdit::SetAside { from: 3, open: 5 })
        .expect("set-aside applies");

    // Assert
    assert_eq!(members(&ledger), [1, 2, 5]);
    assert_eq!(sealed(&ledger), [1, 2]);
}

#[test]
fn should_reject_set_aside_when_from_precedes_log_start() {
    // Arrange
    let mut ledger = ledger_spanning(1, 5);
    ledger
        .apply(&LedgerEdit::AdvanceLogStart { to: 3 })
        .expect("advance applies");

    // Act
    let error = rejected(&ledger, &LedgerEdit::SetAside { from: 2, open: 6 });

    // Assert
    assert_eq!(
        error,
        LedgerError::BadSetAside {
            from: 2,
            open: 6,
            log_start: 3,
            next_segment_id: 6,
        }
    );
}

#[test]
fn should_reject_set_aside_when_open_reuses_allocated_id() {
    // Arrange
    let ledger = ledger_spanning(1, 5);

    // Act
    let error = rejected(&ledger, &LedgerEdit::SetAside { from: 4, open: 5 });

    // Assert
    assert_eq!(
        error,
        LedgerError::BadSetAside {
            from: 4,
            open: 5,
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
        .apply(&LedgerEdit::SetAside { from: 2, open: 8 })
        .expect("set-aside applies");

    // Act
    ledger
        .apply(&LedgerEdit::Rotate { seal: 8, open: 9 })
        .expect("rotation applies");

    // Assert
    assert_eq!(members(&ledger), [1, 8, 9]);
    assert_eq!(sealed(&ledger), [1, 8]);
    assert_eq!(ledger.next_segment_id(), 10);
}

#[test]
fn should_skip_gap_when_log_start_advances_into_it() {
    // Arrange
    let mut ledger = ledger_spanning(1, 3);
    ledger
        .apply(&LedgerEdit::SetAside { from: 3, open: 8 })
        .expect("set-aside applies");
    ledger
        .apply(&LedgerEdit::Rotate { seal: 8, open: 9 })
        .expect("rotation applies");

    // Act
    ledger
        .apply(&LedgerEdit::AdvanceLogStart { to: 5 })
        .expect("advance applies");

    // Assert
    assert_eq!(members(&ledger), [8, 9]);
    assert_eq!(ledger.log_start(), 8);
    assert!(!ledger.is_member(5));
}

#[test]
fn should_reject_edit_when_segment_ids_are_exhausted() {
    // Arrange
    let last = ledger_at(u64::MAX - 1);

    // Act
    let errors = [
        WalLedger::genesis("holder-a", u64::MAX).expect_err("no id follows the last"),
        rejected(
            &last,
            &LedgerEdit::Rotate {
                seal: u64::MAX - 1,
                open: u64::MAX,
            },
        ),
        rejected(
            &last,
            &LedgerEdit::SetAside {
                from: u64::MAX - 1,
                open: u64::MAX,
            },
        ),
    ];

    // Assert
    assert_eq!(errors, [const { LedgerError::SegmentIdsExhausted }; 3]);
}

#[test]
fn should_hold_invariants_when_random_valid_edits_apply() {
    // Arrange
    for seed in 0..64 {
        let mut random = SplitMix64(seed);
        let mut ledger = ledger_at(random.below(10));

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
        let mut ledger = ledger_at(random.below(10));

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
        .apply(&LedgerEdit::SetAside { from: 5, open: 11 })
        .expect("set-aside applies");

    // Act
    let image = serde_json::to_vec(&ledger).expect("serialize ledger");
    let decoded: WalLedger = serde_json::from_slice(&image).expect("deserialize ledger");

    // Assert
    assert_eq!(decoded, ledger);
}

#[test]
fn should_reject_snapshot_when_state_is_unreachable() {
    // Arrange
    let images = [
        r#"{"holder":"h","members":[3,3],"next_segment_id":4,"seq_reserved_through":0}"#,
        r#"{"holder":"h","members":[4,3],"next_segment_id":5,"seq_reserved_through":0}"#,
        r#"{"holder":"h","members":[3],"next_segment_id":3,"seq_reserved_through":0}"#,
        r#"{"holder":"h","members":[3],"next_segment_id":9,"seq_reserved_through":0}"#,
        r#"{"holder":"","members":[3],"next_segment_id":4,"seq_reserved_through":0}"#,
        r#"{"holder":"h","members":[],"next_segment_id":0,"seq_reserved_through":0}"#,
        r#"{"holder":"","members":[],"next_segment_id":1,"seq_reserved_through":0}"#,
        r#"{"holder":"h","members":[3],"next_segment_id":4,"seq_reserved_through":0,"extra":1}"#,
    ];

    // Act
    let results = images.map(serde_json::from_str::<WalLedger>);

    // Assert
    for (result, image) in results.iter().zip(images) {
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
    let unreachable = LedgerError::UnreachableState("the holder is empty");

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
