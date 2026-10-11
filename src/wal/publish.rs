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
//! segments a writer owes the catalog, oldest first, whose uploads are all
//! verified ([`uploaded_run`]), appended past the catalog's head
//! ([`additions`]).

use super::cloud_catalog::PublishedWalSegment;
use std::collections::BTreeMap;

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

/// The longest run of `owed`, oldest first, whose every segment is uploaded.
///
/// `uploaded` maps a segment to the last sequence its verified upload holds.
/// The run stops at the first owed segment without a verified upload, so a
/// later upload never joins it while an earlier segment is missing.
///
/// # Errors
///
/// Refuses `owed` that is not strictly ascending, and an upload whose last
/// sequence differs from the one its writer sealed.
pub(crate) fn uploaded_run(
    owed: &[OwedSegment],
    uploaded: &BTreeMap<u64, u64>,
) -> Result<Vec<OwedSegment>, Refusal> {
    ascending(owed.iter().map(|segment| segment.segment_id))?;
    let mut run = Vec::new();
    for segment in owed {
        let Some(&uploaded_max) = uploaded.get(&segment.segment_id) else {
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
            object_key: crate::wal::cloud_segment_object_key(segment_id, 1),
        }
    }

    fn catalog(segment_ids: &[u64]) -> BTreeMap<u64, PublishedWalSegment> {
        segment_ids
            .iter()
            .map(|&segment_id| (segment_id, segment(segment_id)))
            .collect()
    }

    #[test]
    fn should_select_nothing_when_oldest_owed_segment_is_not_uploaded() {
        // Arrange
        let owed = owed(&[(1, 10), (2, 20), (3, 30)]);
        let uploaded = BTreeMap::from([(2, 20), (3, 30)]);

        // Act
        let run = uploaded_run(&owed, &uploaded);

        // Assert
        assert_eq!(run, Ok(Vec::new()));
    }

    #[test]
    fn should_stop_run_when_a_middle_segment_is_not_uploaded() {
        // Arrange
        let owed = owed(&[(1, 10), (2, 20), (3, 30), (4, 40)]);
        let uploaded = BTreeMap::from([(1, 10), (2, 20), (4, 40)]);

        // Act
        let run = uploaded_run(&owed, &uploaded);

        // Assert
        assert_eq!(run, Ok(owed[..2].to_vec()));
    }

    #[test]
    fn should_select_every_owed_segment_when_all_are_uploaded() {
        // Arrange
        let owed = owed(&[(5, 50), (6, 60)]);
        let uploaded = BTreeMap::from([(5, 50), (6, 60), (7, 70)]);

        // Act
        let run = uploaded_run(&owed, &uploaded);

        // Assert
        assert_eq!(run, Ok(owed));
    }

    #[test]
    fn should_refuse_run_when_upload_sequence_differs_from_sealed_segment() {
        // Arrange
        let owed = owed(&[(1, 10), (2, 20)]);
        let uploaded = BTreeMap::from([(1, 10), (2, 21)]);

        // Act
        let run = uploaded_run(&owed, &uploaded);

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
        let uploaded = BTreeMap::from([(1, 10), (2, 20)]);

        // Act
        let run = uploaded_run(&owed, &uploaded);

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
}
