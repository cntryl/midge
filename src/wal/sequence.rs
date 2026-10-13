//! Sequence reservations (#843).
//!
//! Every sequence the engine hands out lies at or below the reservation the
//! WAL ledger made durable (`LedgerEdit::Reserve`). A process that crashes
//! after handing out sequences it never wrote, or whose unsynced records a
//! power loss drops, leaves the reservation behind, and the next process
//! starts above it, so no sequence is ever handed out twice.
//!
//! This module is pure: callers persist reservations through the ledger.

use std::ops::RangeInclusive;

/// How far past the latest allocation one reservation reaches.
pub(crate) const RESERVATION_BLOCK: u64 = 1 << 20;

/// What allocating `count` sequences after `last` needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Allocation {
    /// The reservation covers these sequences; they may be handed out.
    Ready(RangeInclusive<u64>),
    /// The reservation must first reach `through`.
    NeedsReservation { through: u64 },
}

/// Allocate `count` (at least one) sequences after `last` under the
/// reservation `reserved_through`. `None` when the sequence space is
/// exhausted. `u64::MAX` is the persisted dropped-family sentinel, so
/// `u64::MAX - 1` is the last sequence a live family may use.
pub(crate) fn allocate(last: u64, reserved_through: u64, count: u64) -> Option<Allocation> {
    let first = last.checked_add(1)?;
    let end = last.checked_add(count.max(1))?;
    if end == u64::MAX {
        return None;
    }
    Some(if end <= reserved_through {
        Allocation::Ready(first..=end)
    } else {
        Allocation::NeedsReservation { through: end }
    })
}

/// The reservation to persist before allocating `count` sequences after
/// `last`, if one is due: when less than half a block would remain past the
/// allocation, reserve a whole block past it. A large allocation reserves
/// once, for itself and a block beyond.
pub(crate) fn refill_due(last: u64, reserved_through: u64, count: u64) -> Option<u64> {
    let end = last.saturating_add(count.max(1));
    let remaining = reserved_through.checked_sub(end);
    remaining
        .is_none_or(|remaining| remaining < RESERVATION_BLOCK / 2)
        .then(|| end.saturating_add(RESERVATION_BLOCK))
}

/// The last sequence that may be considered handed out when a process
/// opens: everything recovery saw, every compaction generation, and every
/// sequence an earlier process reserved, which it may have handed out.
pub(crate) fn opening_last(state_sequence: u64, compaction_generation: u64, reserved: u64) -> u64 {
    state_sequence.max(compaction_generation).max(reserved)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_require_reservation_when_allocation_passes_reserved_through() {
        // Arrange
        let (last, reserved) = (10, 12);

        // Act
        let within = allocate(last, reserved, 2);
        let past = allocate(last, reserved, 3);

        // Assert
        assert_eq!(within, Some(Allocation::Ready(11..=12)));
        assert_eq!(past, Some(Allocation::NeedsReservation { through: 13 }));
    }

    #[test]
    fn should_refill_when_half_the_block_is_used() {
        // Arrange
        let reserved = RESERVATION_BLOCK;
        let half = RESERVATION_BLOCK / 2;

        // Act
        let early = refill_due(0, reserved, half);
        let due = refill_due(0, reserved, half + 1);

        // Assert
        assert_eq!(early, None);
        assert_eq!(due, Some(half + 1 + RESERVATION_BLOCK));
    }

    #[test]
    fn should_reserve_once_for_a_transaction_larger_than_the_remaining_block() {
        // Arrange
        let (last, reserved) = (100, 100 + RESERVATION_BLOCK);
        let count = 3 * RESERVATION_BLOCK;

        // Act
        let through = refill_due(last, reserved, count).expect("a refill is due");
        let allocation = allocate(last, through, count);

        // Assert
        assert_eq!(through, last + count + RESERVATION_BLOCK);
        assert_eq!(allocation, Some(Allocation::Ready(101..=last + count)));
    }

    #[test]
    fn should_open_above_everything_an_earlier_process_reserved() {
        // Arrange
        let (sequence, generation, reserved) = (40, 70, 1_000);

        // Act
        let last = opening_last(sequence, generation, reserved);

        // Assert
        assert_eq!(last, 1_000);
        assert_eq!(opening_last(2_000, generation, reserved), 2_000);
    }

    #[test]
    fn should_report_exhaustion_when_sequence_space_runs_out() {
        // Arrange
        let last = u64::MAX - 1;

        // Act
        let allocation = allocate(last, u64::MAX, 2);

        // Assert
        assert_eq!(allocation, None);
    }

    #[test]
    fn should_refuse_allocation_when_it_would_use_the_dropped_family_sentinel() {
        // Arrange
        let last = u64::MAX - 1;

        // Act
        let allocation = allocate(last, u64::MAX, 1);

        // Assert
        assert_eq!(allocation, None);
    }

    #[test]
    fn should_allocate_the_last_live_sequence_when_it_precedes_the_dropped_sentinel() {
        // Arrange
        let last = u64::MAX - 2;

        // Act
        let allocation = allocate(last, u64::MAX, 1);

        // Assert
        assert_eq!(
            allocation,
            Some(Allocation::Ready((u64::MAX - 1)..=(u64::MAX - 1)))
        );
    }
}
