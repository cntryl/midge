//! Contiguous catalog publication (#841, stage 2 of #808).
//!
//! A writer seals WAL segments in order but uploads them concurrently, so
//! their uploads finish in any order. Recovery replays what the catalog
//! publishes, so the catalog may only ever publish a prefix of what a writer
//! sealed: a segment published while an earlier one is not lets a takeover
//! recover later writes without the earlier ones.
//!
//! A verified upload therefore only marks its segment uploaded. These pure
//! decisions choose what one publication adds: the longest run of the
//! segments a writer owes the catalog, oldest first, whose segments are all
//! acknowledged ([`uploaded_run`] over an [`AckSet`]), appended past the
//! catalog's head ([`additions`]).

use super::cloud_catalog::CatalogEntry;
use super::format::{ChainLink, SegmentId};
use std::collections::BTreeMap;

/// What the writer knows the cloud holds of one segment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Ack {
    /// A verified upload the catalog does not publish yet.
    Uploaded(CatalogEntry),
    /// The verified sealed summary the catalog publishes.
    Published(CatalogEntry),
}

impl Ack {
    /// The last sequence the acknowledged segment holds.
    fn max_sequence(&self) -> u64 {
        match self {
            Self::Uploaded(entry) | Self::Published(entry) => entry.max_sequence(),
        }
    }
}

/// The writer's acknowledged segments: verified uploads waiting to be
/// published, and segments the catalog publishes.
///
/// An upload only ever becomes published, and a published segment only
/// leaves once pruning retires it from the catalog: withdrawing an upload
/// never takes back a publication.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct AckSet {
    acks: BTreeMap<SegmentId, Ack>,
}

impl AckSet {
    /// The segments recovery found published, each with the last sequence
    /// it holds.
    pub(crate) fn recovered(published: BTreeMap<SegmentId, CatalogEntry>) -> Self {
        Self {
            acks: published
                .into_iter()
                .map(|(segment_id, entry)| (segment_id, Ack::Published(entry)))
                .collect(),
        }
    }

    /// Acknowledge `segment_id` as published through `max_sequence`, as
    /// recovery would.
    #[cfg(test)]
    pub(crate) fn acknowledge(&mut self, segment_id: SegmentId, max_sequence: u64) {
        let record = crate::wal::WalRecord::new(
            crate::wal::WalOpKind::Put,
            bytes::Bytes::from_static(b"key"),
            Some(bytes::Bytes::from_static(b"value")),
            max_sequence,
        );
        let (bytes, _) =
            crate::wal::format::segment::SegmentBuilder::new(segment_id, 1, ChainLink::GENESIS)
                .record(&record)
                .seal();
        self.acks.insert(
            segment_id,
            Ack::Published(
                CatalogEntry::from_sealed_bytes(&bytes).expect("fixture segment validates"),
            ),
        );
    }

    /// Acknowledge a verified upload. A segment the catalog already
    /// publishes stays published.
    pub(crate) fn record_upload(&mut self, entry: CatalogEntry) {
        if !matches!(self.acks.get(&entry.id()), Some(Ack::Published(_))) {
            self.acks.insert(entry.id(), Ack::Uploaded(entry));
        }
    }

    /// Withdraw `segment_id`'s verified upload, so no run publishes past it
    /// until it is verified again. Returns whether an upload was withdrawn.
    pub(crate) fn withdraw(&mut self, segment_id: SegmentId) -> bool {
        if !matches!(self.acks.get(&segment_id), Some(Ack::Uploaded(_))) {
            return false;
        }
        self.acks.remove(&segment_id);
        true
    }

    /// The catalog now publishes `run`: each member's upload becomes a
    /// publication.
    pub(crate) fn mark_published(&mut self, run: &[OwedSegment]) {
        for segment in run {
            if let Some(ack) = self.acks.get_mut(&segment.segment_id) {
                if let Ack::Uploaded(entry) = ack {
                    *ack = Ack::Published(entry.clone());
                }
            }
        }
    }

    /// The catalog no longer publishes `segment_id`: pruning retired it.
    pub(crate) fn retire_pruned(&mut self, segment_id: SegmentId) {
        self.acks.remove(&segment_id);
    }

    /// The published segments, ascending, each with the last sequence it
    /// holds.
    pub(crate) fn published(&self) -> impl Iterator<Item = (SegmentId, u64)> + '_ {
        self.acks.iter().filter_map(|(segment_id, ack)| match ack {
            Ack::Published(entry) => Some((*segment_id, entry.max_sequence())),
            Ack::Uploaded(_) => None,
        })
    }

    /// The verified summary of the oldest still-authoritative remote segment.
    pub(crate) fn oldest_published_facts(&self) -> Option<&crate::wal::format::SealedFacts> {
        self.acks.values().find_map(|ack| match ack {
            Ack::Published(entry) => Some(entry.facts()),
            Ack::Uploaded(_) => None,
        })
    }

    /// What publishing `run` adds: its verified uploads, oldest first.
    /// Segments the catalog already publishes add nothing.
    pub(crate) fn unpublished(&self, run: &[OwedSegment]) -> Vec<CatalogEntry> {
        run.iter()
            .filter_map(|segment| match self.acks.get(&segment.segment_id) {
                Some(Ack::Uploaded(entry)) => Some(entry.clone()),
                Some(Ack::Published(_)) | None => None,
            })
            .collect()
    }

    /// How many verified uploads wait to be published, and their total
    /// size in bytes.
    pub(crate) fn held(&self) -> (usize, u64) {
        self.acks
            .values()
            .filter_map(|ack| match ack {
                Ack::Uploaded(entry) => Some(entry.size_bytes()),
                Ack::Published(_) => None,
            })
            .fold((0, 0), |(count, bytes), size| {
                (count + 1, bytes.saturating_add(size))
            })
    }

    /// Whether `segment_id` is acknowledged, uploaded or published.
    #[cfg(test)]
    pub(crate) fn contains(&self, segment_id: SegmentId) -> bool {
        self.acks.contains_key(&segment_id)
    }

    /// The last sequence acknowledged segment `segment_id` holds.
    pub(crate) fn max_sequence(&self, segment_id: SegmentId) -> Option<u64> {
        self.acks.get(&segment_id).map(Ack::max_sequence)
    }

    /// How many segments are acknowledged.
    pub(crate) fn len(&self) -> usize {
        self.acks.len()
    }
}

/// A sealed segment its writer has not yet seen published.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OwedSegment {
    pub(crate) segment_id: u64,
    /// The last sequence the writer sealed into it.
    pub(crate) max_sequence: u64,
}

/// Why a publication cannot go ahead.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// Segments must be listed oldest first, each once.
    Unordered { previous: u64, next: u64 },
    /// A verified upload holds other sequences than its writer sealed.
    SequenceMismatch {
        segment_id: u64,
        sealed: u64,
        uploaded: u64,
    },
    /// The catalog publishes a different object for this segment.
    Conflict { segment_id: u64 },
    /// The catalog already publishes a later segment, so adding this one
    /// would publish out of order.
    BehindHead { segment_id: u64, head: u64 },
    /// The segment does not link the catalog's head, or lowers its epoch:
    /// it continues another history (#844).
    Unchained { segment_id: u64, head: ChainLink },
    /// Another writer epoch owns the catalog.
    EpochMismatch { catalog: u64, writer: u64 },
}

impl Refusal {
    /// The segment the refusal is about, if it is about one.
    pub(crate) fn segment_id(&self) -> Option<u64> {
        match self {
            Self::Unordered { next, .. } => Some(*next),
            Self::SequenceMismatch { segment_id, .. }
            | Self::Conflict { segment_id }
            | Self::BehindHead { segment_id, .. }
            | Self::Unchained { segment_id, .. } => Some(*segment_id),
            Self::EpochMismatch { .. } => None,
        }
    }
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unordered { previous, next } => write!(
                f,
                "cloud WAL segment {next} is listed after segment {previous}, not oldest first"
            ),
            Self::SequenceMismatch {
                segment_id,
                sealed,
                uploaded,
            } => write!(
                f,
                "cloud WAL segment {segment_id} ack max sequence {uploaded} does not match inflight max sequence {sealed}"
            ),
            Self::Conflict { segment_id } => write!(
                f,
                "cloud WAL catalog publishes a different object for segment {segment_id}"
            ),
            Self::BehindHead { segment_id, head } => write!(
                f,
                "cloud WAL segment {segment_id} cannot be published behind catalog head {head}"
            ),
            Self::Unchained { segment_id, head } => write!(
                f,
                "cloud WAL segment {segment_id} does not link the catalog head, segment {} (last sequence {})",
                head.id, head.last_seq
            ),
            Self::EpochMismatch { catalog, writer } => write!(
                f,
                "cloud WAL catalog mutation requires fencing epoch {catalog}, writer epoch {writer} rejected"
            ),
        }
    }
}

impl From<Refusal> for crate::common::MidgeError {
    /// A refusal no retry can fix: the catalog contradicts the writer, the
    /// writer's own bookkeeping is inconsistent, or the writer is fenced.
    fn from(refusal: Refusal) -> Self {
        match refusal {
            Refusal::Conflict { .. } | Refusal::BehindHead { .. } | Refusal::Unchained { .. } => {
                Self::Corruption(format!("cloud WAL publication refused: {refusal}"))
            }
            Refusal::Unordered { .. } | Refusal::SequenceMismatch { .. } => {
                Self::Internal(format!("cloud WAL publication refused: {refusal}"))
            }
            Refusal::EpochMismatch { .. } => Self::Fenced(refusal.to_string()),
        }
    }
}

/// The longest run of `owed`, oldest first, whose every segment is
/// acknowledged: uploaded and verified, or already published.
///
/// The run stops at the first owed segment `acks` does not hold, so a later
/// upload never joins it while an earlier segment is missing.
///
/// # Errors
///
/// Refuses `owed` that is not strictly ascending, and an acknowledgement
/// whose last sequence differs from the one its writer sealed.
pub(crate) fn uploaded_run(
    owed: &[OwedSegment],
    acks: &AckSet,
) -> Result<Vec<OwedSegment>, Refusal> {
    ascending(owed.iter().map(|segment| segment.segment_id))?;
    let mut run = Vec::new();
    for segment in owed {
        let Some(uploaded_max) = acks.max_sequence(segment.segment_id) else {
            break;
        };
        if uploaded_max != segment.max_sequence {
            return Err(Refusal::SequenceMismatch {
                segment_id: segment.segment_id,
                sealed: segment.max_sequence,
                uploaded: uploaded_max,
            });
        }
        run.push(*segment);
    }
    Ok(run)
}

/// What publishing `run` adds to a catalog whose head is `head` and which
/// publishes `published`, oldest first.
///
/// A segment the catalog already publishes identically, as after a
/// publication whose acknowledgement was lost, adds nothing. Every other
/// segment must chain onto the head (#844): link it, pass its id, and not
/// lower the epoch of the newest published segment, so the catalog stays
/// one history.
///
/// # Errors
///
/// Refuses a run that is not strictly ascending, a segment the catalog
/// publishes with another object, a segment behind the catalog's head, and
/// one that does not chain onto it.
pub(crate) fn additions(
    head: ChainLink,
    published: &[CatalogEntry],
    run: &[CatalogEntry],
) -> Result<Vec<CatalogEntry>, Refusal> {
    ascending(run.iter().map(CatalogEntry::id))?;
    let mut head = head;
    let mut epoch = published.last().map(CatalogEntry::epoch);
    let mut added = Vec::new();
    for entry in run {
        let segment_id = entry.id();
        match published.binary_search_by_key(&segment_id, CatalogEntry::id) {
            Ok(index) if published[index] == *entry => continue,
            Ok(_) => return Err(Refusal::Conflict { segment_id }),
            Err(_) => {}
        }
        if segment_id <= head.id {
            return Err(Refusal::BehindHead {
                segment_id,
                head: head.id,
            });
        }
        if entry.prev() != head
            || entry.max_sequence() <= head.last_seq
            || epoch.is_some_and(|epoch| entry.epoch() < epoch)
        {
            return Err(Refusal::Unchained { segment_id, head });
        }
        head = entry.link();
        epoch = Some(entry.epoch());
        added.push(entry.clone());
    }
    Ok(added)
}

fn ascending(segment_ids: impl Iterator<Item = u64>) -> Result<(), Refusal> {
    let mut previous: Option<u64> = None;
    for next in segment_ids {
        if let Some(previous) = previous.filter(|previous| *previous >= next) {
            return Err(Refusal::Unordered { previous, next });
        }
        previous = Some(next);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owed(segments: &[(u64, u64)]) -> Vec<OwedSegment> {
        segments
            .iter()
            .map(|&(segment_id, max_sequence)| OwedSegment {
                segment_id,
                max_sequence,
            })
            .collect()
    }

    /// Segment `segment_id` of a chain written by `epoch`: segment `n`
    /// holds a put at `10 * n` and links segment `n - 1`.
    fn chained(epoch: u64, segment_id: u64) -> CatalogEntry {
        let mut prev = ChainLink::GENESIS;
        let mut bytes = Vec::new();
        for id in 1..=segment_id {
            let record = crate::wal::WalRecord::new(
                crate::wal::WalOpKind::Put,
                bytes::Bytes::from_static(b"key"),
                Some(bytes::Bytes::from_static(b"value")),
                id * 10,
            );
            (bytes, prev) = crate::wal::format::segment::SegmentBuilder::new(id, epoch, prev)
                .record(&record)
                .seal();
        }
        CatalogEntry::from_sealed_bytes(&bytes).expect("a chained segment publishes")
    }

    fn segment(segment_id: u64) -> CatalogEntry {
        chained(1, segment_id)
    }

    fn catalog(segment_ids: &[u64]) -> Vec<CatalogEntry> {
        segment_ids
            .iter()
            .map(|&segment_id| segment(segment_id))
            .collect()
    }

    fn head(published: &[CatalogEntry]) -> ChainLink {
        published
            .last()
            .map_or(ChainLink::GENESIS, CatalogEntry::link)
    }

    /// An acknowledgement set holding a verified upload of each segment.
    fn uploads(segment_ids: &[u64]) -> AckSet {
        let mut acks = AckSet::default();
        for &segment_id in segment_ids {
            acks.record_upload(segment(segment_id));
        }
        acks
    }

    #[test]
    fn should_preserve_verified_family_maxima_after_publication_retires_the_local_copy() {
        // Arrange
        let entry = segment(1);
        let expected = entry.facts().clone();
        let mut acks = AckSet::default();
        acks.record_upload(entry);
        // Act
        acks.mark_published(&owed(&[(1, 10)]));
        // Assert
        assert_eq!(acks.oldest_published_facts(), Some(&expected));
        acks.retire_pruned(1);
        assert_eq!(acks.oldest_published_facts(), None);
    }

    #[test]
    fn should_select_nothing_when_oldest_owed_segment_is_not_uploaded() {
        // Arrange
        let owed = owed(&[(1, 10), (2, 20), (3, 30)]);
        let acks = uploads(&[2, 3]);

        // Act
        let run = uploaded_run(&owed, &acks);

        // Assert
        assert_eq!(run, Ok(Vec::new()));
    }

    #[test]
    fn should_stop_run_when_a_middle_segment_is_not_uploaded() {
        // Arrange
        let owed = owed(&[(1, 10), (2, 20), (3, 30), (4, 40)]);
        let acks = uploads(&[1, 2, 4]);

        // Act
        let run = uploaded_run(&owed, &acks);

        // Assert
        assert_eq!(run, Ok(owed[..2].to_vec()));
    }

    #[test]
    fn should_select_every_owed_segment_when_all_are_uploaded() {
        // Arrange
        let owed = owed(&[(5, 50), (6, 60)]);
        let acks = uploads(&[5, 6, 7]);

        // Act
        let run = uploaded_run(&owed, &acks);

        // Assert
        assert_eq!(run, Ok(owed));
    }

    #[test]
    fn should_join_published_segments_to_the_run_when_recovery_found_them_published() {
        // Arrange
        let owed = owed(&[(1, 10), (2, 20), (3, 30)]);
        let mut acks = AckSet::recovered(BTreeMap::from([(1, segment(1))]));
        acks.record_upload(segment(2));

        // Act
        let run = uploaded_run(&owed, &acks);

        // Assert
        assert_eq!(run, Ok(owed[..2].to_vec()));
    }

    #[test]
    fn should_refuse_run_when_upload_sequence_differs_from_sealed_segment() {
        // Arrange
        let owed = owed(&[(1, 10), (2, 20)]);
        let mut acks = AckSet::default();
        acks.acknowledge(1, 10);
        acks.acknowledge(2, 21);

        // Act
        let run = uploaded_run(&owed, &acks);

        // Assert
        assert_eq!(
            run,
            Err(Refusal::SequenceMismatch {
                segment_id: 2,
                sealed: 20,
                uploaded: 21,
            })
        );
    }

    #[test]
    fn should_refuse_run_when_owed_segments_are_not_ascending() {
        // Arrange
        let owed = owed(&[(2, 20), (1, 10)]);
        let acks = uploads(&[1, 2]);

        // Act
        let run = uploaded_run(&owed, &acks);

        // Assert
        assert_eq!(
            run,
            Err(Refusal::Unordered {
                previous: 2,
                next: 1,
            })
        );
    }

    #[test]
    fn should_append_run_past_head_when_catalog_publishes_a_prefix() {
        // Arrange
        let published = catalog(&[1, 2]);
        let run = [segment(3), segment(4)];

        // Act
        let added = additions(head(&published), &published, &run);

        // Assert
        assert_eq!(added, Ok(run.to_vec()));
    }

    #[test]
    fn should_add_nothing_when_catalog_already_publishes_the_run() {
        // Arrange
        let published = catalog(&[1, 2, 3]);
        let run = [segment(2), segment(3)];

        // Act
        let added = additions(head(&published), &published, &run);

        // Assert
        assert_eq!(added, Ok(Vec::new()));
    }

    #[test]
    fn should_add_only_unpublished_tail_when_run_starts_inside_catalog() {
        // Arrange
        let published = catalog(&[1, 2]);
        let run = [segment(2), segment(3)];

        // Act
        let added = additions(head(&published), &published, &run);

        // Assert
        assert_eq!(added, Ok(vec![segment(3)]));
    }

    #[test]
    fn should_append_run_when_catalog_retired_every_segment() {
        // Arrange: the catalog retired segments through 6.
        let anchor = segment(6).link();
        let run = [segment(7)];

        // Act
        let added = additions(anchor, &[], &run);

        // Assert
        assert_eq!(added, Ok(vec![segment(7)]));
    }

    #[test]
    fn should_refuse_segment_behind_head_when_catalog_publishes_a_later_one() {
        // Arrange
        let published = catalog(&[3, 4]);
        let run = [segment(2)];

        // Act
        let added = additions(head(&published), &published, &run);

        // Assert
        assert_eq!(
            added,
            Err(Refusal::BehindHead {
                segment_id: 2,
                head: 4,
            })
        );
    }

    #[test]
    fn should_refuse_segment_when_catalog_publishes_a_different_object() {
        // Arrange
        let published = catalog(&[1, 2]);
        let different = chained(2, 2);

        // Act
        let added = additions(head(&published), &published, &[different, segment(3)]);

        // Assert
        assert_eq!(added, Err(Refusal::Conflict { segment_id: 2 }));
    }

    #[test]
    fn should_refuse_run_that_does_not_link_the_head() {
        // Arrange: the catalog's segment 1 is another history's.
        let published = vec![chained(2, 1)];
        let run = [segment(2)];

        // Act
        let added = additions(head(&published), &published, &run);

        // Assert
        assert_eq!(
            added,
            Err(Refusal::Unchained {
                segment_id: 2,
                head: published[0].link(),
            })
        );
    }

    #[test]
    fn should_refuse_run_when_it_is_not_ascending() {
        // Arrange
        let published = catalog(&[]);
        let run = [segment(3), segment(3)];

        // Act
        let added = additions(head(&published), &published, &run);

        // Assert
        assert_eq!(
            added,
            Err(Refusal::Unordered {
                previous: 3,
                next: 3,
            })
        );
    }

    #[test]
    fn should_keep_published_acknowledgement_when_an_upload_is_withdrawn() {
        // Arrange
        let mut acks = uploads(&[1, 2]);
        acks.mark_published(&owed(&[(1, 10)]));

        // Act
        let withdrew_published = acks.withdraw(1);
        let withdrew_upload = acks.withdraw(2);

        // Assert
        assert!(!withdrew_published);
        assert!(withdrew_upload);
        assert_eq!(acks.published().collect::<Vec<_>>(), vec![(1, 10)]);
        assert!(!acks.contains(2));
    }
}
