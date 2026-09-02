//! Immutable SST candidate index shared by read snapshots.

use crate::metadata::{FileMeta, Manifest};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

#[derive(Debug)]
struct LevelReadView {
    ordered: Vec<Arc<FileMeta>>,
    fallback: Vec<Arc<FileMeta>>,
}

/// Candidate files for one non-zero level.
pub(crate) struct LevelRangeCandidates {
    /// Complete-bound, non-overlapping files in key order.
    pub(crate) ordered: Vec<Arc<FileMeta>>,
    /// Files whose bounds cannot safely participate in the ordered index.
    pub(crate) fallback: Vec<Arc<FileMeta>>,
}

/// SST candidates intersecting one range query.
pub(crate) struct RangeCandidates {
    /// L0 files remain independent because they may overlap arbitrarily.
    pub(crate) l0: Vec<Arc<FileMeta>>,
    /// At most one ordered cursor is needed for each non-zero level.
    pub(crate) levels: Vec<LevelRangeCandidates>,
}

/// Immutable per-column-family SST catalog optimized for read selection.
///
/// L0 is kept in newest-first order. Complete-bound files in L1+ are kept in
/// key order and searched with partition points. Legacy files remain in a
/// conservative fallback bucket until maintenance verifies their real SST
/// bounds. If complete files reveal a true leveled overlap, the whole level is
/// quarantined into that same fallback rather than relying on a false index.
#[derive(Debug)]
pub(crate) struct SstReadView {
    l0: Vec<Arc<FileMeta>>,
    levels: BTreeMap<u32, LevelReadView>,
    pinned_sst_names: Arc<HashSet<String>>,
}

impl SstReadView {
    pub(crate) fn new(
        cf_id: crate::types::ColumnFamilyId,
        files: impl IntoIterator<Item = FileMeta>,
    ) -> Self {
        Self::from_shared(
            files
                .into_iter()
                .filter(|file| file.cf_id == cf_id)
                .map(Arc::new),
        )
    }

    fn from_shared(files: impl IntoIterator<Item = Arc<FileMeta>>) -> Self {
        let mut l0 = Vec::new();
        let mut level_files = BTreeMap::<u32, Vec<Arc<FileMeta>>>::new();
        let mut pinned_sst_names = HashSet::new();

        for file in files {
            pinned_sst_names.insert(file.name.clone());
            if file.level == 0 {
                l0.push(file);
            } else {
                level_files.entry(file.level).or_default().push(file);
            }
        }

        l0.sort_by(|left, right| {
            right
                .sst_seq
                .cmp(&left.sst_seq)
                .then_with(|| right.largest_seq.cmp(&left.largest_seq))
                .then_with(|| right.name.cmp(&left.name))
        });

        let levels = level_files
            .into_iter()
            .map(|(level, files)| (level, Self::build_level(level, files)))
            .collect();

        Self {
            l0,
            levels,
            pinned_sst_names: Arc::new(pinned_sst_names),
        }
    }

    fn build_level(level: u32, files: Vec<Arc<FileMeta>>) -> LevelReadView {
        let (mut ordered, mut fallback): (Vec<_>, Vec<_>) = files.into_iter().partition(|file| {
            file.key_bounds_complete
                && file
                    .smallest_key
                    .as_ref()
                    .zip(file.largest_key.as_ref())
                    .is_some_and(|(smallest, largest)| smallest <= largest)
        });
        ordered.sort_by(|left, right| {
            left.smallest_key
                .cmp(&right.smallest_key)
                .then_with(|| left.largest_key.cmp(&right.largest_key))
                .then_with(|| left.name.cmp(&right.name))
        });

        let has_true_overlap = ordered.windows(2).any(|pair| {
            pair[0]
                .largest_key
                .as_ref()
                .expect("complete largest bound")
                > pair[1]
                    .smallest_key
                    .as_ref()
                    .expect("complete smallest bound")
        });
        // Equality at one adjacent boundary is conservative and supported,
        // but three files sharing one point would exceed the two-candidate
        // leveled bound and is therefore quarantined as an overlap.
        let has_three_way_boundary = ordered.windows(3).any(|window| {
            window[0]
                .largest_key
                .as_ref()
                .expect("complete largest bound")
                >= window[2]
                    .smallest_key
                    .as_ref()
                    .expect("complete smallest bound")
        });
        let quarantined = has_true_overlap || has_three_way_boundary;
        if quarantined {
            tracing::error!(
                level,
                file_count = ordered.len(),
                "quarantining complete-bound SST level with overlapping key coverage"
            );
            fallback.append(&mut ordered);
            fallback.sort_by(|left, right| left.name.cmp(&right.name));
        }

        LevelReadView { ordered, fallback }
    }

    pub(crate) fn point_candidates(&self, key: &[u8]) -> Vec<Arc<FileMeta>> {
        let mut candidates = Vec::with_capacity(self.l0.len() + self.levels.len() * 2);
        candidates.extend(self.l0.iter().cloned());

        for level in self.levels.values() {
            let first = level.ordered.partition_point(|file| {
                file.largest_key.as_deref().expect("indexed largest bound") < key
            });
            candidates.extend(
                level.ordered[first..]
                    .iter()
                    .take_while(|file| {
                        file.smallest_key
                            .as_deref()
                            .expect("indexed smallest bound")
                            <= key
                    })
                    .filter(|file| {
                        file.largest_key.as_deref().expect("indexed largest bound") >= key
                    })
                    .cloned(),
            );
            candidates.extend(level.fallback.iter().cloned());
        }
        candidates
    }

    pub(crate) fn range_candidates(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> RangeCandidates {
        let levels = self
            .levels
            .iter()
            .map(|(&_level_number, level)| {
                let first = start.map_or(0, |start_key| {
                    level.ordered.partition_point(|file| {
                        file.largest_key.as_deref().expect("indexed largest bound") < start_key
                    })
                });
                let last = end.map_or(level.ordered.len(), |end_key| {
                    level.ordered.partition_point(|file| {
                        file.smallest_key
                            .as_deref()
                            .expect("indexed smallest bound")
                            < end_key
                    })
                });
                LevelRangeCandidates {
                    ordered: if first < last {
                        level.ordered[first..last].to_vec()
                    } else {
                        Vec::new()
                    },
                    fallback: level.fallback.clone(),
                }
            })
            .filter(|level| !level.ordered.is_empty() || !level.fallback.is_empty())
            .collect();

        RangeCandidates {
            l0: self.l0.clone(),
            levels,
        }
    }

    pub(crate) fn pinned_sst_names(&self) -> Arc<HashSet<String>> {
        Arc::clone(&self.pinned_sst_names)
    }

    pub(crate) fn file_count(&self) -> usize {
        self.pinned_sst_names.len()
    }

    #[cfg(test)]
    pub(crate) fn is_level_quarantined(&self, level: u32) -> bool {
        self.levels.get(&level).is_some_and(|view| {
            view.ordered.is_empty()
                && !view.fallback.is_empty()
                && view.fallback.iter().all(|file| file.key_bounds_complete)
        })
    }
}

/// Event-loop-owned cache. It rebuilds once after a manifest edit and lets all
/// later snapshot publications clone only one `Arc` per column family.
pub(crate) struct SstReadViewCache {
    dirty: bool,
    views: HashMap<crate::types::ColumnFamilyId, Arc<SstReadView>>,
    live_names: Arc<HashSet<String>>,
}

impl Default for SstReadViewCache {
    fn default() -> Self {
        Self::new()
    }
}

impl SstReadViewCache {
    pub(crate) fn new() -> Self {
        Self {
            dirty: true,
            views: HashMap::new(),
            live_names: Arc::new(HashSet::new()),
        }
    }

    pub(crate) fn invalidate(&mut self) {
        self.dirty = true;
    }

    fn refresh_if_dirty(&mut self, manifest: &Manifest) {
        if self.dirty {
            let mut grouped = HashMap::<u32, Vec<Arc<FileMeta>>>::new();
            let mut live_names = HashSet::with_capacity(manifest.files.len());
            for file in &manifest.files {
                live_names.insert(file.name.clone());
                grouped
                    .entry(file.cf_id)
                    .or_default()
                    .push(Arc::new(file.clone()));
            }
            self.views = grouped
                .into_iter()
                .map(|(id, files)| (id, Arc::new(SstReadView::from_shared(files))))
                .collect();
            self.live_names = Arc::new(live_names);
            self.dirty = false;
        }
    }

    pub(crate) fn view_for(
        &mut self,
        manifest: &Manifest,
        cf_id: crate::types::ColumnFamilyId,
    ) -> Arc<SstReadView> {
        self.refresh_if_dirty(manifest);

        self.views
            .entry(cf_id)
            .or_insert_with(|| Arc::new(SstReadView::new(cf_id, Vec::new())))
            .clone()
    }

    pub(crate) fn live_names(&mut self, manifest: &Manifest) -> Arc<HashSet<String>> {
        self.refresh_if_dirty(manifest);
        Arc::clone(&self.live_names)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn complete_file(level: u32, index: usize) -> FileMeta {
        let start = u64::try_from(index).expect("test index").to_be_bytes();
        let end = u64::try_from(index + 1).expect("test index").to_be_bytes();
        FileMeta {
            name: format!("{level}-{index}.sst"),
            level,
            cf_id: 7,
            sst_seq: u64::try_from(index).expect("test sequence"),
            smallest_key: Some(start.to_vec()),
            largest_key: Some(end.to_vec()),
            key_bounds_complete: true,
            ..Default::default()
        }
    }

    #[test]
    fn should_select_at_most_two_adjacent_files_per_lower_level_for_point_read() {
        // Arrange
        let files = (0..100_000)
            .map(|index| complete_file(1, index))
            .collect::<Vec<_>>();
        let view = SstReadView::new(7, files);
        let key = 50_000_u64.to_be_bytes();

        // Act
        let candidates = view.point_candidates(&key);

        // Assert
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0].name, "1-49999.sst");
        assert_eq!(candidates[1].name, "1-50000.sst");
    }

    #[test]
    fn should_keep_legacy_file_in_conservative_fallback_bucket() {
        // Arrange
        let mut legacy = complete_file(2, 9);
        legacy.key_bounds_complete = false;
        legacy.smallest_key = Some(b"narrow".to_vec());
        legacy.largest_key = Some(b"narrow".to_vec());
        let view = SstReadView::new(7, [legacy]);

        // Act
        let candidates = view.point_candidates(b"outside-advisory-bounds");

        // Assert
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].name, "2-9.sst");
    }

    #[test]
    fn should_quarantine_lower_level_when_complete_bounds_truly_overlap() {
        // Arrange
        let mut first = complete_file(1, 1);
        first.smallest_key = Some(b"a".to_vec());
        first.largest_key = Some(b"z".to_vec());
        let mut second = complete_file(1, 2);
        second.smallest_key = Some(b"m".to_vec());
        second.largest_key = Some(b"zz".to_vec());

        // Act
        let view = SstReadView::new(7, [first, second]);

        // Assert
        assert!(view.is_level_quarantined(1));
        assert_eq!(view.point_candidates(b"0").len(), 2);
    }

    #[test]
    fn should_reuse_same_view_until_manifest_is_invalidated() {
        // Arrange
        let mut manifest = Manifest::default();
        manifest.files.push(complete_file(1, 1));
        let mut cache = SstReadViewCache::new();

        // Act
        let first = cache.view_for(&manifest, 7);
        let second = cache.view_for(&manifest, 7);
        cache.invalidate();
        let third = cache.view_for(&manifest, 7);

        // Assert
        assert!(Arc::ptr_eq(&first, &second));
        assert!(!Arc::ptr_eq(&second, &third));
    }
}
