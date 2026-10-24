use super::merge::{CompactionEvent, CompactionEventCursor};
use crate::common::resource_budget::ResourceBudget;
use crate::common::MidgeResult;
use crate::metadata::FileMeta;

/// Soft overlap budget for one output's intersection with its next level.
/// A single oversized grandparent or inseparable overlapping grandparent set
/// may exceed the target. Equal user-key groups are never divided.
#[derive(Debug, Clone, Default)]
pub struct GrandparentOverlap {
    pub max_bytes: u64,
    starts: Vec<(Vec<u8>, u128)>,
    ends: Vec<(Vec<u8>, u128)>,
    unknown_bytes: u128,
}

impl GrandparentOverlap {
    pub(crate) fn from_files<'a>(
        files: impl Iterator<Item = &'a FileMeta>,
        max_bytes: u64,
    ) -> Self {
        let mut policy = Self {
            max_bytes,
            ..Self::default()
        };
        for file in files {
            if let Some((lower, upper)) = file
                .smallest_key
                .as_ref()
                .zip(file.largest_key.as_ref())
                .filter(|(lower, upper)| file.key_bounds_complete && lower <= upper)
            {
                policy
                    .starts
                    .push((lower.clone(), u128::from(file.size_bytes)));
                policy
                    .ends
                    .push((upper.clone(), u128::from(file.size_bytes)));
            } else {
                policy.unknown_bytes += u128::from(file.size_bytes);
            }
        }
        Self::prefix_sums(&mut policy.starts);
        Self::prefix_sums(&mut policy.ends);
        policy
    }

    fn prefix_sums(endpoints: &mut [(Vec<u8>, u128)]) {
        endpoints.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        let mut total = 0;
        for (_, bytes) in endpoints {
            total += *bytes;
            *bytes = total;
        }
    }

    fn bytes_between(&self, lower: &[u8], upper: &[u8]) -> u64 {
        let starts = self
            .starts
            .partition_point(|(key, _)| key.as_slice() <= upper);
        let ends = self.ends.partition_point(|(key, _)| key.as_slice() < lower);
        let started = starts
            .checked_sub(1)
            .map_or(0, |index| self.starts[index].1);
        let ended = ends.checked_sub(1).map_or(0, |index| self.ends[index].1);
        u64::try_from(started - ended + self.unknown_bytes).unwrap_or(u64::MAX)
    }

    pub(super) fn boundary_cursor(
        &self,
        budget: &ResourceBudget,
    ) -> MidgeResult<Option<CompactionEventCursor<'static>>> {
        if self.max_bytes == 0 || self.starts.is_empty() {
            return Ok(None);
        }
        let retained = self.starts.iter().fold(0_usize, |total, (key, _)| {
            total
                .saturating_add(key.len())
                .saturating_add(std::mem::size_of::<CompactionEvent>())
        });
        let reservation = budget.reserve(retained, "grandparent partition boundaries")?;
        let mut boundaries: Vec<_> = self.starts.iter().map(|(key, _)| key.clone()).collect();
        boundaries.sort_unstable();
        boundaries.dedup();
        Ok(Some(Box::new(boundaries.into_iter().map(move |key| {
            let _keep_reservation = &reservation;
            Ok(CompactionEvent::Boundary(key))
        }))))
    }

    pub(super) fn tracker<'a>(&'a self, budget: &'a ResourceBudget) -> OverlapTracker<'a> {
        OverlapTracker {
            policy: self,
            budget,
            lower: None,
            previous_bytes: 0,
        }
    }
}

pub(super) struct OverlapTracker<'a> {
    policy: &'a GrandparentOverlap,
    budget: &'a ResourceBudget,
    lower: Option<(Vec<u8>, crate::common::resource_budget::ResourceReservation)>,
    previous_bytes: u64,
}

impl OverlapTracker<'_> {
    pub(super) fn should_roll(&self, key: &[u8]) -> bool {
        let Some((lower, _reservation)) = &self.lower else {
            return false;
        };
        let prospective = self.policy.bytes_between(lower, key);
        self.policy.max_bytes != 0
            && prospective > self.policy.max_bytes
            && prospective > self.previous_bytes
    }

    pub(super) fn reset(&mut self) {
        self.lower = None;
        self.previous_bytes = 0;
    }

    pub(super) fn observe(&mut self, key: &[u8]) -> MidgeResult<()> {
        if self.lower.is_none() {
            let reservation = self
                .budget
                .reserve(key.len(), "grandparent partition lower bound")?;
            self.lower = Some((key.to_vec(), reservation));
        }
        let (lower, _reservation) = self.lower.as_ref().expect("observed output key");
        self.previous_bytes = self.policy.bytes_between(lower, key);
        Ok(())
    }
}
