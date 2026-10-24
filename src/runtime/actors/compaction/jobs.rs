//! Loop-owned compaction input and publication-range reservations.
use crate::common::{MidgeError, MidgeResult};
use crate::compaction::strategy::CompactionPurpose;
use crate::compaction::CompactionPlan;
use crate::metadata::FileMeta;
use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};

pub(super) struct CompactionJobs {
    limit: usize,
    state: Mutex<JobState>,
}

#[derive(Default)]
struct JobState {
    next_id: u64,
    active: BTreeMap<u64, ReservedJob>,
}

struct ReservedJob {
    cf_id: u32,
    source_level: u32,
    target_level: u32,
    exclusive: bool,
    inputs: HashSet<String>,
    metadata: Vec<FileMeta>,
    bounds: Option<(Vec<u8>, Vec<u8>)>,
}

pub(super) struct CompactionJobGuard {
    jobs: Arc<CompactionJobs>,
    id: u64,
}

impl CompactionJobs {
    pub(super) fn available(&self, plan: &CompactionPlan, files: &[FileMeta]) -> bool {
        let candidate = ReservedJob::capture(plan, files);
        let state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active.len() < self.limit && !state.active.values().any(|job| job.conflicts(&candidate))
    }

    pub(super) fn active_count(&self) -> usize {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner).active.len()
    }

    pub(super) fn source_available(&self, file: &FileMeta, target_level: u32) -> bool {
        let state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active.len() < self.limit && state.active.values().all(|job| {
            if job.exclusive || job.inputs.contains(&file.name) { return false; }
            if job.cf_id != file.cf_id { return true; }
            if job.source_level == 0 && file.level == 0 { return false; }
            let shared = [file.level, target_level].iter().any(|level| *level == job.source_level || *level == job.target_level);
            if !shared { return true; }
            job.bounds.as_ref().zip(file.smallest_key.as_ref().zip(file.largest_key.as_ref()))
                .is_some_and(|((lower, upper), (first, last))| file.key_bounds_complete && (upper < first || last < lower))
        })
    }
    pub(super) fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit: limit.max(1),
            state: Mutex::new(JobState::default()),
        })
    }

    pub(super) fn reserve(
        self: &Arc<Self>,
        plan: &CompactionPlan,
        files: &[FileMeta],
    ) -> MidgeResult<CompactionJobGuard> {
        let candidate = ReservedJob::capture(plan, files);
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.active.len() >= self.limit
            || state.active.values().any(|job| job.conflicts(&candidate))
        {
            return Err(MidgeError::Busy(
                "compaction input or publication range is reserved".into(),
            ));
        }
        let id = state
            .next_id
            .checked_add(1)
            .ok_or_else(|| MidgeError::ResourceLimit("compaction job identity exhausted".into()))?;
        state.next_id = id;
        state.active.insert(id, candidate);
        Ok(CompactionJobGuard {
            jobs: Arc::clone(self),
            id,
        })
    }
}

impl ReservedJob {
    fn capture(plan: &CompactionPlan, files: &[FileMeta]) -> Self {
        let inputs: HashSet<_> = plan.input_files.iter().cloned().collect();
        let metadata: Vec<_> = files
            .iter()
            .filter(|file| inputs.contains(&file.name))
            .cloned()
            .collect();
        let bounds = metadata
            .iter()
            .try_fold(None::<(Vec<u8>, Vec<u8>)>, |bounds, file| {
                let (lower, upper) = file
                    .smallest_key
                    .as_ref()
                    .zip(file.largest_key.as_ref())
                    .filter(|(lower, upper)| file.key_bounds_complete && lower <= upper)?;
                Some(Some(bounds.map_or_else(
                    || (lower.clone(), upper.clone()),
                    |(smallest, largest)| (smallest.min(lower.clone()), largest.max(upper.clone())),
                )))
            })
            .flatten();
        let bounds = (metadata.len() == inputs.len()).then_some(bounds).flatten();
        Self {
            cf_id: plan.cf_id,
            source_level: plan.source_level,
            target_level: plan.target_level,
            exclusive: !matches!(plan.purpose, CompactionPurpose::Merge),
            inputs,
            metadata,
            bounds,
        }
    }

    fn conflicts(&self, other: &Self) -> bool {
        // Repairs/fragments may need whole-component scratch and remain
        // exclusive until a narrower budget/layout proof is provided.
        if self.exclusive || other.exclusive {
            return true;
        }
        if !self.inputs.is_disjoint(&other.inputs) {
            return true;
        }
        if self.cf_id != other.cf_id {
            return false;
        }
        if self.source_level == 0 && other.source_level == 0 {
            return true;
        }
        let shared_level = [self.source_level, self.target_level]
            .iter()
            .any(|level| *level == other.source_level || *level == other.target_level);
        shared_level
            && self.bounds.as_ref().zip(other.bounds.as_ref()).is_none_or(
                |((lower, upper), (other_lower, other_upper))| {
                    lower <= other_upper && other_lower <= upper
                },
            )
    }

    fn validate_inputs(&self, files: &[FileMeta]) -> MidgeResult<()> {
        if self.metadata.len() != self.inputs.len()
            || self.metadata.iter().any(|captured| {
                captured.cf_id != self.cf_id
                    || !files.iter().any(|current| {
                        current.same_identity(captured)
                            && current.sst_seq == captured.sst_seq
                            && current.sublevel == captured.sublevel
                    })
            })
        {
            return Err(MidgeError::Aborted(
                "compaction input identity or ordering changed; replan".into(),
            ));
        }
        Ok(())
    }
}

impl CompactionJobGuard {
    pub(super) fn validate_inputs(&self, files: &[FileMeta]) -> MidgeResult<()> {
        let state = self
            .jobs
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state
            .active
            .get(&self.id)
            .ok_or_else(|| {
                MidgeError::Aborted("compaction job reservation is no longer live".into())
            })?
            .validate_inputs(files)
    }
}

impl Drop for CompactionJobGuard {
    fn drop(&mut self) {
        self.jobs
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active
            .remove(&self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(name: &str, cf_id: u32, level: u32, lower: &[u8], upper: &[u8]) -> FileMeta {
        FileMeta {
            name: name.into(),
            cf_id,
            level,
            key_bounds_complete: true,
            smallest_key: Some(lower.to_vec()),
            largest_key: Some(upper.to_vec()),
            ..Default::default()
        }
    }

    fn plan(file: &FileMeta, target: u32) -> CompactionPlan {
        let mut plan = CompactionPlan::new(file.cf_id, file.level, target);
        plan.input_files.push(file.name.clone());
        plan.source_files.push(file.name.clone());
        plan
    }

    #[test]
    fn should_reserve_disjoint_levels_while_rejecting_conflicting_publication_ranges(
    ) -> MidgeResult<()> {
        // Arrange
        let jobs = CompactionJobs::new(4);
        let files = [
            file("first", 0, 1, b"a", b"z"),
            file("second", 0, 3, b"a", b"z"),
            file("conflict", 0, 2, b"m", b"n"),
        ];
        let first = jobs.reserve(&plan(&files[0], 2), &files)?;

        // Act
        let second = jobs.reserve(&plan(&files[1], 4), &files)?;
        let conflict = jobs.reserve(&plan(&files[2], 3), &files);

        // Assert: names differ, but source/target publication ranges conflict.
        assert!(matches!(conflict, Err(MidgeError::Busy(_))));
        first.validate_inputs(&files)?;
        second.validate_inputs(&files)?;
        drop(first);
        drop(second);
        jobs.reserve(&plan(&files[2], 3), &files)?;
        Ok(())
    }

    #[test]
    fn should_allow_complete_disjoint_key_ranges_but_reserve_unknown_bounds_conservatively(
    ) -> MidgeResult<()> {
        // Arrange
        let jobs = CompactionJobs::new(4);
        let mut files = [
            file("left", 0, 1, b"a", b"m"),
            file("right", 0, 1, b"x", b"z"),
        ];
        let _left = jobs.reserve(&plan(&files[0], 2), &files)?;

        // Act
        let right = jobs.reserve(&plan(&files[1], 2), &files)?;
        drop(right);
        files[1].key_bounds_complete = false;
        let unknown = jobs.reserve(&plan(&files[1], 2), &files);

        // Assert
        assert!(matches!(unknown, Err(MidgeError::Busy(_))));
        Ok(())
    }

    #[test]
    fn should_keep_one_l0_job_per_family_even_when_inputs_are_disjoint() -> MidgeResult<()> {
        // Arrange
        let jobs = CompactionJobs::new(4);
        let files = [
            file("left", 0, 0, b"a", b"m"),
            file("right", 0, 0, b"x", b"z"),
            file("other", 1, 0, b"x", b"z"),
        ];
        let _left = jobs.reserve(&plan(&files[0], 1), &files)?;

        // Act / Assert
        assert!(matches!(
            jobs.reserve(&plan(&files[1], 1), &files),
            Err(MidgeError::Busy(_))
        ));
        jobs.reserve(&plan(&files[2], 1), &files)?;
        Ok(())
    }

    #[test]
    fn should_reject_changed_l0_ordering_identity_before_publication() -> MidgeResult<()> {
        // Arrange
        let jobs = CompactionJobs::new(4);
        let mut files = [file("input", 0, 0, b"a", b"z")];
        let reservation = jobs.reserve(&plan(&files[0], 1), &files)?;

        // Act
        files[0].sst_seq = 7;

        // Assert
        assert!(matches!(
            reservation.validate_inputs(&files),
            Err(MidgeError::Aborted(_))
        ));
        Ok(())
    }
}
