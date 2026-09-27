//! Complete-bound L1+ layout defects shared by read selection and repair.

use crate::metadata::FileMeta;
use std::collections::BTreeMap;

struct EndpointGroup {
    count: usize,
    // At most two independent files can share an endpoint without already
    // forming a three-file boundary. Once connected, retain one representative.
    representatives: Vec<usize>,
}

fn root(parents: &mut [usize], index: usize) -> usize {
    if parents[index] != index {
        parents[index] = root(parents, parents[index]);
    }
    parents[index]
}

fn connect(parents: &mut [usize], left: usize, right: usize) {
    let left = root(parents, left);
    let right = root(parents, right);
    if left != right {
        parents[right] = left;
    }
}

/// Return the complete connected repair components in key order. Pairwise
/// equality alone remains legal; strict overlap or three files covering one
/// endpoint connects the files. Incomplete legacy bounds are maintained by the
/// separate bound-backfill path.
pub(crate) fn repair_components(files: &[&FileMeta]) -> Vec<Vec<usize>> {
    let mut order: Vec<_> = (0..files.len())
        .filter(|&index| {
            let file = files[index];
            file.key_bounds_complete
                && file
                    .smallest_key
                    .as_ref()
                    .zip(file.largest_key.as_ref())
                    .is_some_and(|(smallest, largest)| smallest <= largest)
        })
        .collect();
    order.sort_by(|&left, &right| {
        files[left]
            .smallest_key
            .cmp(&files[right].smallest_key)
            .then_with(|| files[left].largest_key.cmp(&files[right].largest_key))
            .then_with(|| files[left].name.cmp(&files[right].name))
    });

    let mut parents: Vec<_> = (0..files.len()).collect();
    let mut active = BTreeMap::<&[u8], EndpointGroup>::new();
    let mut active_count = 0usize;
    let mut found = false;
    for &index in &order {
        let start = files[index]
            .smallest_key
            .as_deref()
            .expect("complete start");
        let end = files[index].largest_key.as_deref().expect("complete end");
        while active
            .first_key_value()
            .is_some_and(|(bound, _)| *bound < start)
        {
            let (_, expired) = active.pop_first().expect("active endpoint");
            active_count -= expired.count;
        }

        // All intervals strictly covering this start are already connected:
        // each new interval met an earlier active one when it was inserted.
        if let Some((&furthest, group)) = active.last_key_value() {
            if furthest > start {
                connect(&mut parents, index, group.representatives[0]);
                found = true;
            }
        }
        if active_count >= 2 {
            // Including this file, at least three intervals cover the point.
            // Only equality-ended files may still be disconnected from the
            // strict-overlap component.
            if let Some(equal) = active.get(start) {
                for &representative in &equal.representatives {
                    connect(&mut parents, index, representative);
                }
            }
            found = true;
        }

        let group = active.entry(end).or_insert_with(|| EndpointGroup {
            count: 0,
            representatives: Vec::new(),
        });
        group.count += 1;
        active_count += 1;
        if group.count <= 2 {
            group.representatives.push(index);
        } else {
            for &representative in &group.representatives {
                connect(&mut parents, index, representative);
            }
            group.representatives.clear();
            group.representatives.push(index);
        }
    }
    if !found {
        return Vec::new();
    }

    let mut groups = BTreeMap::<usize, Vec<usize>>::new();
    for index in order {
        groups
            .entry(root(&mut parents, index))
            .or_default()
            .push(index);
    }
    let mut components: Vec<_> = groups
        .into_values()
        .filter(|group| group.len() > 1)
        .collect();
    components.sort_by(|left, right| {
        files[left[0]]
            .smallest_key
            .cmp(&files[right[0]].smallest_key)
            .then_with(|| files[left[0]].name.cmp(&files[right[0]].name))
    });
    components
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_match_reference_repair_components_for_small_layouts() {
        // Arrange: enumerate every four-file layout over four endpoint values.
        let intervals: Vec<_> = (0_u8..4)
            .flat_map(|start| (start..4).map(move |end| (start, end)))
            .collect();

        // Act
        for a in &intervals {
            for b in &intervals {
                for c in &intervals {
                    for d in &intervals {
                        let files: Vec<_> = [a, b, c, d]
                            .iter()
                            .enumerate()
                            .map(|(index, (start, end))| FileMeta {
                                name: format!("{index}.sst"),
                                level: 1,
                                size_bytes: 1,
                                content_crc32c: None,
                                cf_id: 0,
                                smallest_key: Some(vec![*start]),
                                largest_key: Some(vec![*end]),
                                smallest_seq: None,
                                largest_seq: None,
                                key_bounds_complete: true,
                                sst_seq: 0,
                                sublevel: 0,
                                read_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(
                                    0,
                                )),
                            })
                            .collect();
                        let refs: Vec<_> = files.iter().collect();
                        let mut expected_parent = [0, 1, 2, 3];
                        for left in 0..4 {
                            for right in left + 1..4 {
                                let (left_start, left_end) = [a, b, c, d][left];
                                let (right_start, right_end) = [a, b, c, d][right];
                                if left_start < right_end && right_start < left_end {
                                    connect(&mut expected_parent, left, right);
                                }
                            }
                        }
                        for point in 0..4 {
                            let covering: Vec<_> = [a, b, c, d]
                                .iter()
                                .enumerate()
                                .filter_map(|(index, (start, end))| {
                                    (start <= &point && &point <= end).then_some(index)
                                })
                                .collect();
                            if covering.len() >= 3 {
                                for &index in &covering[1..] {
                                    connect(&mut expected_parent, covering[0], index);
                                }
                            }
                        }
                        let mut expected = BTreeMap::<usize, Vec<usize>>::new();
                        for index in 0..4 {
                            expected
                                .entry(root(&mut expected_parent, index))
                                .or_default()
                                .push(index);
                        }
                        let mut expected: Vec<_> = expected
                            .into_values()
                            .filter(|component| component.len() > 1)
                            .collect();
                        let mut actual = repair_components(&refs);
                        for component in &mut actual {
                            component.sort_unstable();
                        }
                        expected.sort();
                        actual.sort();

                        // Assert
                        assert_eq!(actual, expected, "layout: {a:?} {b:?} {c:?} {d:?}");
                    }
                }
            }
        }
    }
}
