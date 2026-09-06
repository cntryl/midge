//! Replay-local cursors into durably published name reservations.

use crate::common::MidgeResult;
use crate::wal::recovery::streaming::StreamingReplayLimits;
use std::collections::HashMap;

pub(super) struct Names {
    count: u64,
    ranges: HashMap<u32, std::ops::Range<u64>>,
}

impl Names {
    pub(super) fn new(limits: StreamingReplayLimits) -> Self {
        Self {
            count: u64::try_from(
                (limits.max_memtable_encoded_bytes / limits.target_memtable_encoded_bytes.max(1))
                    .max(1),
            )
            .unwrap_or(u64::MAX),
            ranges: HashMap::new(),
        }
    }

    pub(super) fn take(
        &mut self,
        cf_id: u32,
        publish: impl FnOnce(u64) -> MidgeResult<std::ops::Range<u64>>,
    ) -> MidgeResult<u64> {
        let range = self.ranges.entry(cf_id).or_insert(0..0);
        if range.is_empty() {
            // A failed publication must never install a usable cursor.
            *range = publish(self.count)?;
        }
        range.next().ok_or_else(|| {
            crate::common::MidgeError::Internal("empty recovery SST name reservation".into())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_amortize_publications_when_checkpoints_consume_reserved_names() {
        // Arrange
        let mut names = Names::new(StreamingReplayLimits {
            max_frame_bytes: 16,
            max_pending_txn_bytes: 16,
            max_memtable_encoded_bytes: 64,
            target_memtable_encoded_bytes: 16,
        });
        let mut publications = 0;
        let mut high_water = 1;

        // Act
        let observed: Vec<_> = (0..10)
            .map(|_| {
                names
                    .take(0, |count| {
                        publications += 1;
                        let start = high_water;
                        high_water += count;
                        Ok(start..high_water)
                    })
                    .unwrap()
            })
            .collect();

        // Assert
        assert_eq!(observed, (1..=10).collect::<Vec<_>>());
        assert_eq!(publications, 3);
        assert_eq!(high_water, 13);
    }

    #[test]
    fn should_discard_unused_names_when_recovery_restarts_after_a_failed_publication() {
        // Arrange
        let limits = StreamingReplayLimits {
            max_frame_bytes: 16,
            max_pending_txn_bytes: 16,
            max_memtable_encoded_bytes: 64,
            target_memtable_encoded_bytes: 16,
        };
        let mut names = Names::new(limits);
        let mut high_water = 1;

        // Act
        let failed = names.take(0, |count| {
            high_water += count;
            Err(crate::common::MidgeError::Internal(
                "mirror failed after local reservation".into(),
            ))
        });
        drop(names);
        let mut restarted = Names::new(limits);
        let first = restarted
            .take(0, |count| {
                let start = high_water;
                high_water += count;
                Ok(start..high_water)
            })
            .unwrap();
        let second_family = restarted.take(1, |count| Ok(1..1 + count)).unwrap();
        let next = restarted
            .take(0, |_| panic!("range must already be durable"))
            .unwrap();

        // Assert
        assert!(failed.is_err());
        assert_eq!((first, next, second_family), (5, 6, 1));
        assert_eq!(high_water, 9);
    }
}
