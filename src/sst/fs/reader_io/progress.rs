//! Bounds and retained-key accounting shared by immutable SST summaries.

use super::{MidgeResult, SstFileSummary};
use crate::common::resource_budget::{ResourceBudget, ResourceReservation};

#[derive(Default)]
pub(crate) struct SstSummaryProgress {
    pub(crate) summary: Option<SstFileSummary>,
    minimum_reservation: Option<ResourceReservation>,
    maximum_reservation: Option<ResourceReservation>,
}

impl SstSummaryProgress {
    pub(super) fn observe(
        &mut self,
        size: u64,
        key: &[u8],
        seq: u64,
        budget: Option<&ResourceBudget>,
    ) -> MidgeResult<()> {
        if self
            .summary
            .as_ref()
            .is_none_or(|summary| key < summary.smallest_key.as_slice())
        {
            let reservation = budget
                .map(|budget| budget.reserve(key.len(), "SST resume minimum key"))
                .transpose()?;
            if let Some(summary) = &mut self.summary {
                summary.smallest_key = key.to_vec();
            } else {
                let maximum = budget
                    .map(|budget| budget.reserve(key.len(), "SST resume maximum key"))
                    .transpose()?;
                self.summary = Some(SstFileSummary {
                    size_bytes: size,
                    smallest_key: key.to_vec(),
                    largest_key: key.to_vec(),
                    smallest_seq: seq,
                    largest_seq: seq,
                });
                self.maximum_reservation = maximum;
            }
            self.minimum_reservation = reservation;
        }
        let summary = self.summary.as_mut().expect("observed summary entry");
        if key > summary.largest_key.as_slice() {
            let reservation = budget
                .map(|budget| budget.reserve(key.len(), "SST resume maximum key"))
                .transpose()?;
            summary.largest_key = key.to_vec();
            self.maximum_reservation = reservation;
        }
        summary.smallest_seq = summary.smallest_seq.min(seq);
        summary.largest_seq = summary.largest_seq.max(seq);
        Ok(())
    }
}
