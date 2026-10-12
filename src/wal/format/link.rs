//! How WAL segments chain (#843): each segment's header names the link of
//! the segment before it, and [`Chain`] admits a sequence of segments only
//! when every link, epoch and first sequence agrees.

use super::segment::SegmentHeader;
use crate::common::{MidgeError, MidgeResult};
use serde::{Deserialize, Serialize};

/// A WAL segment id; segment `id` is the file `segment_file_name(id)`.
pub type SegmentId = u64;

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

/// What a segment's header fixes besides its holder: its id, the writer
/// epoch that created it, the least sequence its records may hold, and the
/// link of the segment before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OpenFacts {
    pub(crate) id: SegmentId,
    /// The writer epoch that created the segment.
    pub(crate) epoch: u64,
    /// Every record in the segment is at least this.
    pub(crate) first_seq: u64,
    /// The link of the segment before it.
    pub(crate) prev: ChainLink,
}

impl OpenFacts {
    /// The header of a segment with these facts, written for `holder`.
    pub(crate) fn header(self, holder: [u8; 16]) -> SegmentHeader {
        SegmentHeader {
            id: self.id,
            epoch: self.epoch,
            holder,
            first_seq: self.first_seq,
            prev: self.prev,
        }
    }
}

/// The chain rule (#843): whether a segment with header facts `open` may
/// follow the segment whose link is `prev`, after segments written by
/// `min_epoch`. It must link `prev`, start past `prev`'s last sequence,
/// carry a greater id, and not lower the epoch.
///
/// # Errors
///
/// Why it may not, naming both segments.
pub(crate) fn follows(
    prev: ChainLink,
    min_epoch: Option<u64>,
    open: &OpenFacts,
) -> Result<(), String> {
    if open.prev != prev {
        return Err(format!(
            "segment {} links segment {} (last sequence {}), not segment {} (last sequence {}) before it",
            open.id, open.prev.id, open.prev.last_seq, prev.id, prev.last_seq
        ));
    }
    if open.first_seq <= prev.last_seq {
        return Err(format!(
            "segment {} starts at sequence {}, not past {}",
            open.id, open.first_seq, prev.last_seq
        ));
    }
    if let Some(epoch) = min_epoch.filter(|epoch| open.epoch < *epoch) {
        return Err(format!(
            "segment {} has epoch {} below epoch {epoch}",
            open.id, open.epoch
        ));
    }
    if open.id <= prev.id {
        return Err(format!(
            "segment {} does not follow segment {}",
            open.id, prev.id
        ));
    }
    Ok(())
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
        // Before the first segment an unknown anchor is whatever it links.
        let prev = self.expected_prev.unwrap_or(header.prev);
        follows(prev, self.epoch, &header.open_facts())
            .map_err(|reason| corruption(format!("WAL {reason}")))?;
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
