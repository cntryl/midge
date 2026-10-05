//! Budgeted sequence-interval lookup over one immutable manifest snapshot.

use crate::common::resource_budget::{ResourceBudget, ResourceReservation};
use crate::common::{DeadlineScope, MidgeResult};
use crate::metadata::FileMeta;
use crate::wal::WalRecord;
use std::cell::Cell;

struct Node {
    file_index: usize,
    subtree_last_sequence: u64,
}

pub(super) struct CandidateIndex {
    nodes: Vec<Node>,
    _reservation: ResourceReservation,
}

impl CandidateIndex {
    pub(super) fn allocation_bytes(file_count: usize) -> Option<usize> {
        file_count.checked_mul(std::mem::size_of::<Node>())
    }

    pub(super) fn new(
        files: &[FileMeta],
        budget: &ResourceBudget,
        scope: Option<&DeadlineScope>,
    ) -> MidgeResult<Option<Self>> {
        let Some(bytes) = Self::allocation_bytes(files.len()) else {
            return Ok(None);
        };
        let Ok(reservation) = budget.reserve(bytes, "recovery manifest candidate index") else {
            return Ok(None);
        };
        let mut nodes = Vec::new();
        if nodes.try_reserve_exact(files.len()).is_err() {
            return Ok(None);
        }
        for (file_index, file) in files.iter().enumerate() {
            super::check_scope(scope)?;
            // The existing point predicate can never select missing bounds.
            let (Some(first), Some(last)) = (file.smallest_seq, file.largest_seq) else {
                continue;
            };
            if first <= last && file.smallest_key.is_some() && file.largest_key.is_some() {
                nodes.push(Node {
                    file_index,
                    subtree_last_sequence: last,
                });
            }
        }
        nodes.sort_unstable_by_key(|node| {
            let file = &files[node.file_index];
            (file.cf_id, file.smallest_seq, node.file_index)
        });
        super::check_scope(scope)?;
        Self::augment(&mut nodes);
        super::check_scope(scope)?;
        Ok(Some(Self {
            nodes,
            _reservation: reservation,
        }))
    }

    fn augment(nodes: &mut [Node]) -> u64 {
        if nodes.is_empty() {
            return 0;
        }
        let middle = nodes.len() / 2;
        let (left, rest) = nodes.split_at_mut(middle);
        let (root, right) = rest.split_first_mut().expect("nonempty interval subtree");
        root.subtree_last_sequence = root
            .subtree_last_sequence
            .max(Self::augment(left))
            .max(Self::augment(right));
        root.subtree_last_sequence
    }

    pub(super) fn visit(
        &self,
        files: &[FileMeta],
        record: &WalRecord,
        scope: Option<&DeadlineScope>,
        scanned: &Cell<u64>,
        visitor: &mut impl FnMut(&FileMeta) -> MidgeResult<bool>,
    ) -> MidgeResult<bool> {
        Self::visit_nodes(&self.nodes, files, record, scope, scanned, visitor)
    }

    fn visit_nodes(
        nodes: &[Node],
        files: &[FileMeta],
        record: &WalRecord,
        scope: Option<&DeadlineScope>,
        scanned: &Cell<u64>,
        visitor: &mut impl FnMut(&FileMeta) -> MidgeResult<bool>,
    ) -> MidgeResult<bool> {
        let Some(root) = nodes.get(nodes.len() / 2) else {
            return Ok(true);
        };
        super::check_scope(scope)?;
        scanned.set(scanned.get().saturating_add(1));
        if root.subtree_last_sequence < record.seq {
            return Ok(true);
        }
        let middle = nodes.len() / 2;
        let file = &files[root.file_index];
        if record.cf_id <= file.cf_id
            && !Self::visit_nodes(&nodes[..middle], files, record, scope, scanned, visitor)?
        {
            return Ok(false);
        }
        let first = file.smallest_seq.expect("indexed sequence lower bound");
        let last = file.largest_seq.expect("indexed sequence upper bound");
        if file.cf_id == record.cf_id
            && first <= record.seq
            && record.seq <= last
            && !visitor(file)?
        {
            return Ok(false);
        }
        if record.cf_id > file.cf_id || record.cf_id == file.cf_id && first <= record.seq {
            return Self::visit_nodes(&nodes[middle + 1..], files, record, scope, scanned, visitor);
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{MidgeError, OperationDeadline};
    use crate::runtime::hybrid_persistence::file_covers_wal_point_record;
    use crate::wal::WalOpKind;
    use bytes::Bytes;
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;
    use std::time::Duration;

    // These tests exercise selection and its resource contract. Actual SST
    // identity, unreadable-object and proof-state tests live in the parent.
    fn file(index: usize, cf_id: u32, first: u64, last: u64) -> FileMeta {
        FileMeta {
            name: format!("candidate-{index}.sst"),
            level: 0,
            size_bytes: 1,
            content_crc32c: Some(7),
            cf_id,
            sst_seq: u64::try_from(index).unwrap(),
            smallest_key: Some(Vec::new()),
            largest_key: Some(b"z".to_vec()),
            smallest_seq: Some(first),
            largest_seq: Some(last),
            key_bounds_complete: true,
            sublevel: 0,
            read_count: Arc::new(AtomicU64::new(0)),
        }
    }

    fn record(cf_id: u32, seq: u64, key: &[u8]) -> WalRecord {
        WalRecord::new_cf(
            cf_id,
            WalOpKind::Put,
            Bytes::copy_from_slice(key),
            Some(Bytes::from_static(b"value")),
            seq,
            7,
        )
    }

    fn matching_names(
        index: &CandidateIndex,
        files: &[FileMeta],
        record: &WalRecord,
    ) -> Vec<String> {
        let mut names = Vec::new();
        assert!(index
            .visit(files, record, None, &Cell::new(0), &mut |file| {
                if file_covers_wal_point_record(file, record) {
                    names.push(file.name.clone());
                }
                Ok(true)
            })
            .unwrap());
        names.sort_unstable();
        names
    }

    #[test]
    fn should_preserve_all_linear_matches_when_intervals_overlap_across_families() {
        // Arrange: deliberately unsorted, nested, disjoint and same-start
        // intervals exercise every pruning branch against the old predicate.
        let mut files: Vec<_> = (0..96)
            .map(|index| {
                let first = u64::try_from(index * 3 % 101).unwrap();
                let mut file = file(index, u32::try_from(index % 4).unwrap(), first, first + 37);
                if index % 5 == 0 {
                    file.smallest_key = Some(b"b".to_vec());
                    file.largest_key = Some(b"m".to_vec());
                }
                file
            })
            .collect();
        files.extend([
            file(96, 0, 0, u64::MAX),
            file(97, 0, 7, 7),
            file(98, 1, u64::MAX, u64::MAX),
        ]);
        files.reverse();
        let budget = ResourceBudget::new(CandidateIndex::allocation_bytes(files.len()).unwrap());
        let index = CandidateIndex::new(&files, &budget, None).unwrap().unwrap();

        // Act: compare complete match sets, including inclusive endpoints,
        // empty keys and the maximum sequence without incrementing it.
        for cf_id in 0..6 {
            for seq in (0..=150).chain([u64::MAX]) {
                for key in [b"".as_slice(), b"b", b"m", b"z", b"zz"] {
                    let record = record(cf_id, seq, key);
                    let mut expected: Vec<_> = files
                        .iter()
                        .filter(|file| file_covers_wal_point_record(file, &record))
                        .map(|file| file.name.clone())
                        .collect();
                    expected.sort_unstable();
                    let actual = matching_names(&index, &files, &record);

                    // Assert: no matching overlap may disappear.
                    assert_eq!(actual, expected, "cf={cf_id}, seq={seq}, key={key:?}");
                }
            }
        }
    }

    #[test]
    fn should_keep_unproved_candidates_when_metadata_cannot_establish_coverage() {
        // Arrange: CRC and completeness are proof concerns, whereas missing
        // or inverted bounds cannot pass the unchanged candidate predicate.
        let mut files: Vec<_> = (0..8).map(|index| file(index, 0, 7, 7)).collect();
        files[0].content_crc32c = None;
        files[1].key_bounds_complete = false;
        files[2].smallest_seq = None;
        files[3].largest_seq = None;
        files[4].smallest_key = None;
        files[5].largest_key = None;
        files[6].smallest_seq = Some(8);
        files[7].smallest_key = Some(b"z".to_vec());
        files[7].largest_key = Some(b"a".to_vec());
        let budget = ResourceBudget::new(1024);
        let index = CandidateIndex::new(&files, &budget, None).unwrap().unwrap();

        // Act
        let actual = matching_names(&index, &files, &record(0, 7, b"key"));

        // Assert: later proof failures must still be observable.
        assert_eq!(actual, [files[0].name.clone(), files[1].name.clone()]);
    }

    #[test]
    fn should_stop_visiting_when_an_overlapping_candidate_rejects_proof() {
        // Arrange: all five intervals match; false belongs to the third
        // actually visited overlap rather than an unrelated interval.
        let files: Vec<_> = (0..5).map(|index| file(index, 0, 7, 7)).collect();
        let budget = ResourceBudget::new(1024);
        let index = CandidateIndex::new(&files, &budget, None).unwrap().unwrap();
        let mut visited = Vec::new();

        // Act
        let accepted = index
            .visit(
                &files,
                &record(0, 7, b"key"),
                None,
                &Cell::new(0),
                &mut |file| {
                    visited.push(file.name.clone());
                    Ok(visited.len() < 3)
                },
            )
            .unwrap();

        // Assert
        assert!(!accepted);
        assert_eq!(
            visited,
            files[..3]
                .iter()
                .map(|file| file.name.clone())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn should_propagate_typed_timeout_when_candidate_visitor_fails() {
        // Arrange
        let files = [file(0, 0, 7, 7)];
        let budget = ResourceBudget::new(1024);
        let index = CandidateIndex::new(&files, &budget, None).unwrap().unwrap();

        // Act
        let result = index.visit(
            &files,
            &record(0, 7, b"key"),
            None,
            &Cell::new(0),
            &mut |_| {
                Err(MidgeError::Timeout(
                    "actual candidate callback deadline".into(),
                ))
            },
        );

        // Assert
        assert!(matches!(result, Err(MidgeError::Timeout(_))));
    }

    #[test]
    fn should_reject_cancelled_lookup_when_index_is_already_built() {
        // Arrange
        let files = [file(0, 0, 7, 7)];
        let budget = ResourceBudget::new(1024);
        let index = CandidateIndex::new(&files, &budget, None).unwrap().unwrap();
        let scope = DeadlineScope::new(OperationDeadline::unbounded());
        scope.cancel();
        let mut visited = 0;

        // Act
        let result = index.visit(
            &files,
            &record(0, 7, b"key"),
            Some(&scope),
            &Cell::new(0),
            &mut |_| {
                visited += 1;
                Ok(true)
            },
        );

        // Assert
        assert!(matches!(result, Err(MidgeError::Timeout(_))));
        assert_eq!(visited, 0);
    }

    #[test]
    fn should_escape_timeout_when_scope_is_cancelled_during_candidate_traversal() {
        // Arrange
        let files: Vec<_> = (0..7).map(|index| file(index, 0, 7, 7)).collect();
        let budget = ResourceBudget::new(1024);
        let index = CandidateIndex::new(&files, &budget, None).unwrap().unwrap();
        let scope = DeadlineScope::new(OperationDeadline::unbounded());
        let mut visited = 0;

        // Act
        let result = index.visit(
            &files,
            &record(0, 7, b"key"),
            Some(&scope),
            &Cell::new(0),
            &mut |_| {
                visited += 1;
                scope.cancel();
                Ok(true)
            },
        );

        // Assert: cancelled traversals cannot become a cached coverage answer.
        assert!(matches!(result, Err(MidgeError::Timeout(_))));
        assert!(visited > 0 && visited < files.len());
    }

    #[test]
    fn should_release_index_charge_when_expired_construction_returns_timeout() {
        // Arrange
        let files = [file(0, 0, 7, 7)];
        let budget = ResourceBudget::new(1024);
        let scope = DeadlineScope::new(OperationDeadline::from_budget(Duration::ZERO));

        // Act
        let result = CandidateIndex::new(&files, &budget, Some(&scope));

        // Assert
        assert!(matches!(result, Err(MidgeError::Timeout(_))));
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn should_preserve_exact_charge_when_index_is_dropped_or_admission_fails() {
        // Arrange
        let files: Vec<_> = (0..16).map(|index| file(index, 0, 7, 7)).collect();
        let bytes = CandidateIndex::allocation_bytes(files.len()).unwrap();
        let denied = ResourceBudget::new(bytes - 1);
        let accepted = ResourceBudget::new(bytes);

        // Act
        let missing = CandidateIndex::new(&files, &denied, None).unwrap();
        let index = CandidateIndex::new(&files, &accepted, None)
            .unwrap()
            .unwrap();
        let charged = accepted.used();
        drop(index);

        // Assert: callers can use the exact legacy fallback with no leaked
        // reservation, and successful indexing cannot escape the shared cap.
        assert!(missing.is_none());
        assert_eq!(denied.used(), 0);
        assert_eq!(charged, bytes);
        assert_eq!(accepted.used(), 0);
        assert_eq!(accepted.peak(), bytes);
        assert_eq!(CandidateIndex::allocation_bytes(0), Some(0));
        assert_eq!(CandidateIndex::allocation_bytes(usize::MAX), None);
    }
}
