//! Experimental key-interval selector over the same immutable manifest.
use crate::common::resource_budget::{ResourceBudget, ResourceReservation};
use crate::common::{DeadlineScope, MidgeResult};
use crate::metadata::FileMeta;
use crate::wal::WalRecord;
use std::cell::Cell;

struct Node {
    file: usize,
    largest_key_file: usize,
}

pub(super) struct KeyIndex {
    nodes: Vec<Node>,
    _reservation: ResourceReservation,
}
impl KeyIndex {
    pub(super) fn new(
        files: &[FileMeta],
        budget: &ResourceBudget,
        scope: Option<&DeadlineScope>,
    ) -> MidgeResult<Option<Self>> {
        let Some(bytes) = files.len().checked_mul(std::mem::size_of::<Node>()) else {
            return Ok(None);
        };
        let Ok(reservation) = budget.reserve(bytes, "experimental recovery key index") else {
            return Ok(None);
        };
        let mut nodes = Vec::new();
        if nodes.try_reserve_exact(files.len()).is_err() {
            return Ok(None);
        }
        for (file, meta) in files.iter().enumerate() {
            super::check_scope(scope)?;
            if matches!((meta.smallest_seq, meta.largest_seq), (Some(first), Some(last)) if first <= last)
                && matches!((&meta.smallest_key, &meta.largest_key), (Some(first), Some(last)) if first <= last)
            {
                nodes.push(Node {
                    file,
                    largest_key_file: file,
                });
            }
        }
        nodes.sort_unstable_by(|a, b| {
            let a_meta = &files[a.file];
            let b_meta = &files[b.file];
            (a_meta.cf_id, &a_meta.smallest_key, a.file).cmp(&(
                b_meta.cf_id,
                &b_meta.smallest_key,
                b.file,
            ))
        });
        Self::augment(&mut nodes, files);
        super::check_scope(scope)?;
        Ok(Some(Self {
            nodes,
            _reservation: reservation,
        }))
    }

    fn augment(nodes: &mut [Node], files: &[FileMeta]) -> Option<usize> {
        if nodes.is_empty() {
            return None;
        }
        let middle = nodes.len() / 2;
        let (left, rest) = nodes.split_at_mut(middle);
        let (root, right) = rest.split_first_mut().expect("nonempty key subtree");
        for candidate in [Self::augment(left, files), Self::augment(right, files)]
            .into_iter()
            .flatten()
        {
            if files[candidate].largest_key > files[root.largest_key_file].largest_key {
                root.largest_key_file = candidate;
            }
        }
        Some(root.largest_key_file)
    }

    pub(super) fn visit(
        &self,
        files: &[FileMeta],
        record: &WalRecord,
        scope: Option<&DeadlineScope>,
        scanned: &Cell<u64>,
        visitor: &mut impl FnMut(&FileMeta) -> MidgeResult<bool>,
    ) -> MidgeResult<bool> {
        Self::walk(&self.nodes, files, record, scope, scanned, visitor)
    }

    fn walk(
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
        if files[root.largest_key_file]
            .largest_key
            .as_ref()
            .expect("indexed upper key")
            .as_slice()
            < record.key.as_ref()
        {
            return Ok(true);
        }
        let middle = nodes.len() / 2;
        let file = &files[root.file];
        if record.cf_id <= file.cf_id
            && !Self::walk(&nodes[..middle], files, record, scope, scanned, visitor)?
        {
            return Ok(false);
        }
        let first = file
            .smallest_key
            .as_ref()
            .expect("indexed lower key")
            .as_slice();
        let last = file
            .largest_key
            .as_ref()
            .expect("indexed upper key")
            .as_slice();
        if file.cf_id == record.cf_id
            && first <= record.key.as_ref()
            && record.key.as_ref() <= last
            && !visitor(file)?
        {
            return Ok(false);
        }
        if record.cf_id > file.cf_id || record.cf_id == file.cf_id && first <= record.key.as_ref() {
            return Self::walk(&nodes[middle + 1..], files, record, scope, scanned, visitor);
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
    #[test]
    fn should_match_shared_candidates_when_key_and_sequence_intervals_overlap() {
        // Arrange
        let mut files: Vec<_> = (0..96_u32)
            .map(|index| FileMeta {
                name: format!("file-{index}"),
                cf_id: index % 3,
                smallest_seq: Some(u64::from(index % 7)),
                largest_seq: Some(u64::from(index % 7 + 8)),
                smallest_key: Some(vec![u8::try_from(index % 32).unwrap()]),
                largest_key: Some(vec![u8::try_from(index % 32 + 16).unwrap()]),
                ..FileMeta::default()
            })
            .collect();
        files[0].smallest_key = None;
        files[1].largest_seq = None;
        files[2].largest_key = Some(Vec::new());
        files[3].smallest_seq = Some(999);
        files.reverse();
        let budget = ResourceBudget::new(8192);
        let index = KeyIndex::new(&files, &budget, None).unwrap().unwrap();
        // Act
        for cf in 0..4 {
            for seq in 0..17 {
                for key in [Vec::new(), vec![0], vec![7], vec![31], vec![48], vec![255]] {
                    let record = WalRecord::new_cf(
                        cf,
                        WalOpKind::Put,
                        Bytes::from(key),
                        Some(Bytes::from_static(b"v")),
                        seq,
                        1,
                    );
                    let mut actual = Vec::new();
                    assert!(index
                        .visit(&files, &record, None, &Cell::new(0), &mut |file| {
                            if file_covers_wal_point_record(file, &record) {
                                actual.push(file.name.clone());
                            }
                            Ok(true)
                        })
                        .unwrap());
                    let mut expected: Vec<_> = files
                        .iter()
                        .filter(|file| file_covers_wal_point_record(file, &record))
                        .map(|file| file.name.clone())
                        .collect();
                    actual.sort();
                    expected.sort();
                    // Assert
                    assert_eq!(actual, expected);
                }
            }
        }
        drop(index);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn should_preserve_resource_contract_when_key_index_scope_expires() {
        // Arrange
        let files = [FileMeta {
            cf_id: 0,
            smallest_seq: Some(1),
            largest_seq: Some(9),
            smallest_key: Some(Vec::new()),
            largest_key: Some(vec![255]),
            ..FileMeta::default()
        }];
        let budget = ResourceBudget::new(1024);
        let index = KeyIndex::new(&files, &budget, None).unwrap().unwrap();
        let record = WalRecord::new_cf(
            0,
            WalOpKind::Put,
            Bytes::from_static(b"key"),
            Some(Bytes::from_static(b"v")),
            7,
            1,
        );
        let scope = DeadlineScope::new(OperationDeadline::unbounded());
        scope.cancel();
        // Act
        let result = index.visit(&files, &record, Some(&scope), &Cell::new(0), &mut |_| {
            panic!("cancelled visitor")
        });
        drop(index);
        let expired = KeyIndex::new(&files, &budget, Some(&scope));
        let denied = KeyIndex::new(&files, &ResourceBudget::new(0), None).unwrap();
        // Assert
        assert!(matches!(result, Err(MidgeError::Timeout(_))));
        assert!(matches!(expired, Err(MidgeError::Timeout(_))));
        assert!(denied.is_none());
        assert_eq!(budget.used(), 0);
    }
}
