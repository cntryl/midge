//! How WAL segments chain (#843): each segment's header names the link of
//! the segment before it, and [`Chain`] admits a sequence of segments only
//! when every link, epoch and first sequence agrees.

use super::segment::SegmentHeader;
use crate::common::{MidgeError, MidgeResult};
use crate::wal::ledger::SegmentId;
use serde::{Deserialize, Serialize};

/// An xxh3-128 digest, little-endian.
pub type Digest128 = [u8; 16];

/// What a sealed segment contributes to the chain: its id, the last
/// sequence it holds and the digest of its bytes before the footer. The
/// next segment's header names it as `prev`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainLink {
    pub id: SegmentId,
    pub last_seq: u64,
    pub digest: Digest128,
}

impl ChainLink {
    /// The link before the first segment of a new history.
    pub const GENESIS: Self = Self {
        id: 0,
        last_seq: 0,
        digest: [0; 16],
    };
}

/// Admits WAL segments in replay order. See [`Chain::admit`].
#[derive(Debug, Clone)]
pub(crate) struct Chain {
    /// The link the next segment must name; `None` before the first
    /// segment when the anchor is unknown.
    expected_prev: Option<ChainLink>,
    /// The newest epoch admitted so far.
    epoch: Option<u64>,
    /// Whether the last admitted segment is still unsealed: nothing may
    /// follow it.
    open: bool,
}

impl Chain {
    /// A chain whose first segment must name `anchor`, or any link when
    /// `anchor` is `None`.
    pub(crate) fn new(anchor: Option<ChainLink>) -> Self {
        Self {
            expected_prev: anchor,
            epoch: None,
            open: false,
        }
    }

    /// Admit `header`, planned as segment `id`: the header names `id`, links
    /// the previous segment (or the anchor), does not lower the epoch, and
    /// starts past the previous segment's last sequence. Only a sealed
    /// segment may be followed.
    ///
    /// # Errors
    ///
    /// `Corruption` naming what disagrees.
    pub(crate) fn admit(&mut self, header: &SegmentHeader, id: SegmentId) -> MidgeResult<()> {
        if self.open {
            return Err(corruption(format!(
                "WAL segment {id} follows a segment that was never sealed"
            )));
        }
        if header.id != id {
            return Err(corruption(format!(
                "WAL segment {id} holds the header of segment {}",
                header.id
            )));
        }
        if let Some(expected) = self.expected_prev {
            if header.prev != expected {
                return Err(corruption(format!(
                    "WAL segment {id} links segment {} (last sequence {}), not segment {} (last sequence {}) before it",
                    header.prev.id, header.prev.last_seq, expected.id, expected.last_seq
                )));
            }
        }
        if let Some(epoch) = self.epoch {
            if header.epoch < epoch {
                return Err(corruption(format!(
                    "WAL segment {id} has writer epoch {} below epoch {epoch} before it",
                    header.epoch
                )));
            }
        }
        if header.first_seq <= header.prev.last_seq {
            return Err(corruption(format!(
                "WAL segment {id} starts at sequence {}, not past its predecessor's {}",
                header.first_seq, header.prev.last_seq
            )));
        }
        self.epoch = Some(header.epoch);
        self.open = true;
        Ok(())
    }

    /// The admitted segment sealed with `link`; the next must name it.
    pub(crate) fn seal(&mut self, link: ChainLink) {
        self.expected_prev = Some(link);
        self.open = false;
    }
}

fn corruption(message: String) -> MidgeError {
    MidgeError::Corruption(message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(id: SegmentId, epoch: u64, first_seq: u64, prev: ChainLink) -> SegmentHeader {
        SegmentHeader {
            id,
            epoch,
            holder: [7; 16],
            first_seq,
            prev,
        }
    }

    fn link(id: SegmentId, last_seq: u64) -> ChainLink {
        ChainLink {
            id,
            last_seq,
            digest: [u8::try_from(id).expect("small id"); 16],
        }
    }

    #[test]
    fn should_admit_segments_that_link_in_order() {
        // Arrange
        let mut chain = Chain::new(Some(ChainLink::GENESIS));

        // Act
        let first = chain.admit(&header(1, 1, 1, ChainLink::GENESIS), 1);
        chain.seal(link(1, 5));
        let second = chain.admit(&header(2, 1, 6, link(1, 5)), 2);

        // Assert
        assert!(first.is_ok() && second.is_ok());
    }

    #[test]
    fn should_reject_segment_that_links_a_different_predecessor() {
        // Arrange
        let mut chain = Chain::new(Some(ChainLink::GENESIS));
        chain
            .admit(&header(1, 1, 1, ChainLink::GENESIS), 1)
            .expect("first");
        chain.seal(link(1, 5));

        // Act
        let result = chain.admit(&header(2, 1, 6, link(9, 5)), 2);

        // Assert
        assert!(matches!(result, Err(MidgeError::Corruption(_))));
    }

    #[test]
    fn should_reject_segment_whose_epoch_regresses() {
        // Arrange
        let mut chain = Chain::new(None);
        chain
            .admit(&header(1, 2, 1, ChainLink::GENESIS), 1)
            .expect("first");
        chain.seal(link(1, 5));

        // Act
        let result = chain.admit(&header(2, 1, 6, link(1, 5)), 2);

        // Assert
        assert!(
            matches!(&result, Err(MidgeError::Corruption(message)) if message.contains("epoch")),
            "{result:?}"
        );
    }

    #[test]
    fn should_reject_segment_that_names_another_id() {
        // Arrange
        let mut chain = Chain::new(None);

        // Act
        let result = chain.admit(&header(4, 1, 1, ChainLink::GENESIS), 3);

        // Assert
        assert!(matches!(result, Err(MidgeError::Corruption(_))));
    }

    #[test]
    fn should_reject_segment_after_an_unsealed_one() {
        // Arrange
        let mut chain = Chain::new(None);
        chain
            .admit(&header(1, 1, 1, ChainLink::GENESIS), 1)
            .expect("first");

        // Act
        let result = chain.admit(&header(2, 1, 1, ChainLink::GENESIS), 2);

        // Assert
        assert!(matches!(result, Err(MidgeError::Corruption(_))));
    }

    #[test]
    fn should_reject_segment_that_starts_at_or_below_its_predecessor() {
        // Arrange
        let mut chain = Chain::new(None);

        // Act
        let result = chain.admit(&header(2, 1, 5, link(1, 5)), 2);

        // Assert
        assert!(matches!(result, Err(MidgeError::Corruption(_))));
    }
}
