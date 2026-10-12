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

use super::cloud_catalog::PublishedWalSegment;
use super::format::SegmentId;
use std::collections::BTreeMap;

/// What the writer knows the cloud holds of one segment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Ack {
    /// A verified upload the catalog does not publish yet.
    Uploaded(PublishedWalSegment),
    /// A segment the catalog publishes, holding sequences through
    /// `max_sequence`.
    Published { max_sequence: u64 },
}

impl Ack {
    /// The last sequence the acknowledged segment holds.
    fn max_sequence(&self) -> u64 {
        match self {
            Self::Uploaded(entry) => entry.max_sequence,
            Self::Published { max_sequence } => *max_sequence,
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
    pub(crate) fn recovered(published: BTreeMap<SegmentId, u64>) -> Self {
        Self {
            acks: published
                .into_iter()
                .map(|(segment_id, max_sequence)| (segment_id, Ack::Published { max_sequence }))
                .collect(),
        }
    }

    /// Acknowledge a verified upload. A segment the catalog already
    /// publishes stays published.
    pub(crate) fn record_upload(&mut self, entry: PublishedWalSegment) {
        if !matches!(
            self.acks.get(&entry.segment_id),
            Some(Ack::Published { .. })
        ) {
            self.acks.insert(entry.segment_id, Ack::Uploaded(entry));
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
                    *ack = Ack::Published {
                        max_sequence: entry.max_sequence,
                    };
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
            Ack::Published { max_sequence } => Some((*segment_id, *max_sequence)),
            Ack::Uploaded(_) => None,
        })
    }

    /// What publishing `run` adds: its verified uploads, oldest first.
    /// Segments the catalog already publishes add nothing.
    pub(crate) fn unpublished(&self, run: &[OwedSegment]) -> Vec<PublishedWalSegment> {
        run.iter()
            .filter_map(|segment| match self.acks.get(&segment.segment_id) {
                Some(Ack::Uploaded(entry)) => Some(entry.clone()),
                Some(Ack::Published { .. }) | None => None,
            })
            .collect()
    }

    /// How many verified uploads wait to be published, and their total
    /// size in bytes.
    pub(crate) fn held(&self) -> (usize, u64) {
        self.acks
            .values()
            .filter_map(|ack| match ack {
                Ack::Uploaded(entry) => Some(entry.size_bytes),
                Ack::Published { .. } => None,
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
            | Self::BehindHead { segment_id, .. } => Some(*segment_id),
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
            Refusal::Conflict { .. } | Refusal::BehindHead { .. } => {
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

/// What publishing `run` adds to a catalog that publishes `published`.
///
/// A segment the catalog already publishes identically, as after a
/// publication whose acknowledgement was lost, adds nothing. Every other
/// segment must land past the catalog's head, so the catalog stays a prefix
/// of what its writers sealed.
///
/// # Errors
///
/// Refuses a run that is not strictly ascending, a segment the catalog
/// publishes with another object, and a segment behind the catalog's head.
pub(crate) fn additions(
    published: &BTreeMap<u64, PublishedWalSegment>,
    run: &[PublishedWalSegment],
) -> Result<Vec<PublishedWalSegment>, Refusal> {
    ascending(run.iter().map(|segment| segment.segment_id))?;
    let mut head = published.keys().next_back().copied();
    let mut added = Vec::new();
    for segment in run {
        match published.get(&segment.segment_id) {
            Some(existing) if existing == segment => continue,
            Some(_) => {
                return Err(Refusal::Conflict {
                    segment_id: segment.segment_id,
                })
            }
            None => {}
        }
        if let Some(head) = head.filter(|head| *head > segment.segment_id) {
            return Err(Refusal::BehindHead {
                segment_id: segment.segment_id,
                head,
            });
        }
        head = Some(segment.segment_id);
        added.push(segment.clone());
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

    fn segment(segment_id: u64) -> PublishedWalSegment {
        PublishedWalSegment {
            segment_id,
            writer_epoch: 1,
            max_sequence: segment_id * 10,
            size_bytes: 64,
            content_crc32c: u32::try_from(segment_id).unwrap(),
            object_key: crate::wal::segment_object_key(segment_id, 1),
        }
    }

    fn catalog(segment_ids: &[u64]) -> BTreeMap<u64, PublishedWalSegment> {
        segment_ids
            .iter()
            .map(|&segment_id| (segment_id, segment(segment_id)))
            .collect()
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
        let mut acks = AckSet::recovered(BTreeMap::from([(1, 10)]));
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
        let acks = AckSet::recovered(BTreeMap::from([(1, 10), (2, 21)]));

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
        let added = additions(&published, &run);

        // Assert
        assert_eq!(added, Ok(run.to_vec()));
    }

    #[test]
    fn should_add_nothing_when_catalog_already_publishes_the_run() {
        // Arrange
        let published = catalog(&[1, 2, 3]);
        let run = [segment(2), segment(3)];

        // Act
        let added = additions(&published, &run);

        // Assert
        assert_eq!(added, Ok(Vec::new()));
    }

    #[test]
    fn should_add_only_unpublished_tail_when_run_starts_inside_catalog() {
        // Arrange
        let published = catalog(&[1, 2]);
        let run = [segment(2), segment(3)];

        // Act
        let added = additions(&published, &run);

        // Assert
        assert_eq!(added, Ok(vec![segment(3)]));
    }

    #[test]
    fn should_append_run_when_catalog_is_empty() {
        // Arrange
        let published = catalog(&[]);
        let run = [segment(7)];

        // Act
        let added = additions(&published, &run);

        // Assert
        assert_eq!(added, Ok(vec![segment(7)]));
    }

    #[test]
    fn should_refuse_segment_behind_head_when_catalog_publishes_a_later_one() {
        // Arrange
        let published = catalog(&[1, 4]);
        let run = [segment(2), segment(3)];

        // Act
        let added = additions(&published, &run);

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
        let mut different = segment(2);
        different.content_crc32c ^= 1;

        // Act
        let added = additions(&published, &[different, segment(3)]);

        // Assert
        assert_eq!(added, Err(Refusal::Conflict { segment_id: 2 }));
    }

    #[test]
    fn should_refuse_run_when_it_is_not_ascending() {
        // Arrange
        let published = catalog(&[]);
        let run = [segment(3), segment(3)];

        // Act
        let added = additions(&published, &run);

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
