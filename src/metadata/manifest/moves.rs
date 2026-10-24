//! Logical level moves preserve the complete original immutable identity.
use super::{invalid_transition, FileMeta, MidgeResult};
use std::collections::HashSet;

fn bounds(file: &FileMeta) -> Option<(&[u8], &[u8])> {
    file.smallest_key
        .as_deref()
        .zip(file.largest_key.as_deref())
        .filter(|(lower, upper)| file.key_bounds_complete && lower <= upper)
}

fn overlaps(left: &FileMeta, right: &FileMeta) -> bool {
    bounds(left)
        .zip(bounds(right))
        .is_none_or(|((ll, lu), (rl, ru))| ll <= ru && rl <= lu)
}

/// Validate the original selection against today's complete authority. Returns
/// true only if the entire move is already reflected (idempotent replay).
pub(crate) fn validate_sst_move(
    files: &[FileMeta],
    captured: &[FileMeta],
    target: u32,
) -> MidgeResult<bool> {
    let Some(first) = captured.first() else {
        return Err(invalid_transition("empty SST move"));
    };
    if first.level.checked_add(1) != Some(target) {
        return Err(invalid_transition(
            "SST move must promote one logical level",
        ));
    }
    let mut names = HashSet::new();
    let mut moved = 0;
    for file in captured {
        if file.cf_id != first.cf_id
            || file.level != first.level
            || !names.insert(file.name.as_str())
            || bounds(file).is_none()
            || file.content_crc32c.is_none()
        {
            return Err(invalid_transition(
                "SST move lacks complete source identity and bounds",
            ));
        }
        let current = files
            .iter()
            .find(|current| current.name == file.name)
            .ok_or_else(|| invalid_transition("SST move source is no longer live"))?;
        let mut original = current.clone();
        if current.level == target {
            moved += 1;
            original.level = file.level;
        }
        if !original.same_identity(file)
            || original.sst_seq != file.sst_seq
            || original.sublevel != file.sublevel
            || original.earliest_expiration_millis != file.earliest_expiration_millis
        {
            return Err(invalid_transition(
                "SST move source identity or ordering changed",
            ));
        }
    }
    if moved != 0 && moved != captured.len() {
        return Err(invalid_transition("SST move is only partially reflected"));
    }
    for (index, file) in captured.iter().enumerate() {
        if captured[..index].iter().any(|other| overlaps(file, other)) {
            return Err(invalid_transition("SST move sources require a merge"));
        }
        for other in files
            .iter()
            .filter(|other| other.cf_id == first.cf_id && !names.contains(other.name.as_str()))
        {
            if other.level == target && overlaps(file, other) {
                return Err(invalid_transition(
                    "SST move target acquired conflicting coverage",
                ));
            }
            if first.level == 0
                && other.level == 0
                && overlaps(file, other)
                && other
                    .smallest_seq
                    .zip(file.largest_seq)
                    .is_none_or(|(remaining_first, selected_last)| remaining_first <= selected_last)
            {
                return Err(invalid_transition(
                    "SST move would pass older overlapping L0 coverage",
                ));
            }
            if first.level > 0 && other.level == first.level && overlaps(file, other) {
                return Err(invalid_transition(
                    "SST move source level requires overlap repair",
                ));
            }
        }
    }
    Ok(moved != 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{Manifest, ManifestEdit};

    fn file(level: u32, generation: u64, lower: &[u8], upper: &[u8]) -> FileMeta {
        FileMeta {
            name: crate::cloud_layout::file_name(0, level, generation),
            level,
            sst_seq: generation,
            size_bytes: 4096,
            content_crc32c: Some(42),
            smallest_key: Some(lower.to_vec()),
            largest_key: Some(upper.to_vec()),
            smallest_seq: Some(generation),
            largest_seq: Some(generation),
            key_bounds_complete: true,
            ..Default::default()
        }
    }

    fn rejected_unchanged(mut manifest: Manifest, captured: Vec<FileMeta>) {
        let before =
            crate::metadata::codec::encode(crate::metadata::codec::MANIFEST, &manifest).unwrap();
        assert!(manifest
            .apply_edit(&ManifestEdit::MoveSsts {
                captured,
                target_level: 1
            })
            .is_err());
        assert_eq!(
            crate::metadata::codec::encode(crate::metadata::codec::MANIFEST, &manifest).unwrap(),
            before
        );
        assert!(manifest.retired_ssts.is_empty());
    }

    #[test]
    fn should_reject_hot_newer_l0_move_when_an_older_overlapping_source_remains() {
        // Arrange: target empty is insufficient; the older upper-level value
        // cannot remain above the hot newer version.
        let older = file(0, 1, b"key", b"key");
        let newer = file(0, 2, b"key", b"key");
        newer
            .read_count
            .store(100, std::sync::atomic::Ordering::Relaxed);
        let manifest = Manifest {
            files: vec![older, newer.clone()],
            ..Default::default()
        };
        // Act and Assert
        rejected_unchanged(manifest, vec![newer]);
    }

    #[test]
    fn should_reject_incomplete_range_bounds_and_target_conflicts_without_retiring_sources() {
        // Arrange
        let source = file(0, 1, b"a", b"z");
        let target = file(1, 2, b"m", b"n");
        let conflicting = Manifest {
            files: vec![source.clone(), target],
            ..Default::default()
        };
        // Act and Assert: complete bounds include range-tombstone endpoints.
        rejected_unchanged(conflicting, vec![source.clone()]);
        let mut unknown = source;
        unknown.key_bounds_complete = false;
        rejected_unchanged(
            Manifest {
                files: vec![unknown.clone()],
                ..Default::default()
            },
            vec![unknown],
        );
    }

    #[test]
    fn should_preserve_identity_and_retirement_ledger_when_replaying_an_atomic_move_twice(
    ) -> MidgeResult<()> {
        // Arrange
        let first = file(0, 1, b"a", b"b");
        let second = file(0, 2, b"x", b"z");
        let mut manifest = Manifest {
            files: vec![first.clone(), second.clone()],
            ..Default::default()
        };
        let edit = ManifestEdit::MoveSsts {
            captured: vec![first, second],
            target_level: 1,
        };
        // Act
        manifest.apply_edit(&edit)?;
        manifest.apply_edit(&edit)?;
        // Assert
        assert_eq!(manifest.files.len(), 2);
        assert!(manifest
            .files
            .iter()
            .all(|file| file.level == 1 && file.content_crc32c == Some(42)));
        assert!(manifest.retired_ssts.is_empty());
        Ok(())
    }

    #[test]
    fn should_reject_changed_source_order_and_partial_move_without_any_mutation() {
        // Arrange
        let first = file(0, 1, b"a", b"b");
        let second = file(0, 2, b"x", b"z");
        let mut partial = first.clone();
        partial.level = 1;
        // Act and Assert
        rejected_unchanged(
            Manifest {
                files: vec![partial, second.clone()],
                ..Default::default()
            },
            vec![first.clone(), second],
        );
        let mut changed = first.clone();
        changed.sublevel = 8;
        rejected_unchanged(
            Manifest {
                files: vec![changed],
                ..Default::default()
            },
            vec![first],
        );
    }
}
