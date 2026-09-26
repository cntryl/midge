//! Process-local scan checkpoints for immutable SST verification.

use super::{
    Arc, Fs, MidgeError, MidgeResult, SstFileIo, SstFileSummary, SstRawVersionScan,
    SstScanLifecycle,
};
use crate::common::resource_budget::{ResourceBudget, ResourceReservation};
use crate::sst::traits::RawSstVersion;
#[cfg(test)]
use crate::types::EntryType;

#[derive(Clone, Copy, Default)]
pub(crate) struct SstCursorPosition {
    block: Option<usize>,
    offset: usize,
    complete: bool,
}

#[derive(Default)]
pub(crate) struct SstSummaryProgress {
    pub(crate) summary: Option<SstFileSummary>,
    cursor: SstCursorPosition,
    ranges_checked: bool,
    minimum_reservation: Option<ResourceReservation>,
    maximum_reservation: Option<ResourceReservation>,
}

impl SstFileIo {
    pub(crate) fn visit_raw_versions_with_progress(
        self,
        budget: ResourceBudget,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        progress: &mut SstCursorPosition,
        visitor: &mut dyn FnMut(RawSstVersion) -> MidgeResult<()>,
        checkpoint: &mut dyn FnMut() -> MidgeResult<()>,
    ) -> MidgeResult<()> {
        if progress.complete {
            return Ok(());
        }
        let mut cursor = SstRawVersionScan::new(Arc::new(self), start, end, Some(budget))?;
        cursor.initialize()?;
        if let Some(block) = progress.block {
            if block < cursor.first_block || block > cursor.last_block {
                return Err(MidgeError::Corruption(
                    "SST resume block is outside pinned cursor bounds".into(),
                ));
            }
            cursor.next_block = block;
            cursor.lifecycle = SstScanLifecycle::Active;
        }
        while let Some(version) = cursor.next_version()? {
            let block = if matches!(cursor.lifecycle, SstScanLifecycle::Exhausted) {
                cursor.last_block
            } else {
                cursor.next_block.saturating_sub(1)
            };
            if progress.block == Some(block) && cursor.decoder.offset <= progress.offset {
                continue;
            }
            visitor(version)?;
            progress.block = Some(block);
            progress.offset = cursor.decoder.offset;
            checkpoint()?;
        }
        progress.complete = true;
        Ok(())
    }

    pub(crate) fn summarize_with_fs_progress<'a>(
        path: &str,
        fs: Arc<dyn Fs>,
        budget: &ResourceBudget,
        progress: &'a mut SstSummaryProgress,
        checkpoint: &mut dyn FnMut() -> MidgeResult<()>,
    ) -> MidgeResult<&'a SstFileSummary> {
        if !progress.cursor.complete {
            let reader = Self::open_for_compaction(path, fs, budget.clone())?;
            let size = reader.fs.metadata(&reader.path)?.len;
            if !progress.ranges_checked {
                for tombstone in &reader.range_tombstones {
                    progress.observe(size, &tombstone.start, tombstone.seq, Some(budget))?;
                    progress.observe(size, &tombstone.end, tombstone.seq, Some(budget))?;
                }
                progress.ranges_checked = true;
                if !reader.range_tombstones.is_empty() {
                    checkpoint()?;
                }
            }
            // Keep the resume boundary unchanged until a whole version has
            // been incorporated, including both retained key reservations.
            let mut cursor = progress.cursor;
            let result = reader.visit_raw_versions_with_progress(
                budget.clone(),
                None,
                None,
                &mut cursor,
                &mut |version| progress.observe(size, &version.key, version.seq, Some(budget)),
                checkpoint,
            );
            progress.cursor = cursor;
            result?;
        }
        progress
            .summary
            .as_ref()
            .ok_or_else(|| MidgeError::Corruption("SST contains no publishable entries".into()))
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::SstFactory;

    #[test]
    fn should_resume_at_unacknowledged_version_after_visitor_failure() -> MidgeResult<()> {
        // Arrange
        let directory = tempfile::tempdir()?;
        let fs = Arc::new(crate::io::RealFs::new(directory.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs.clone(), 4096);
        let mut writer = factory.create()?;
        for sequence in (1..=3).rev() {
            writer.add_with_meta(
                b"shared-key",
                Some(b"value"),
                sequence,
                EntryType::Put,
                None,
            )?;
        }
        crate::sst::fs::finish_writer_to_path(writer, &directory.path().join("resume.sst"))?;
        let budget = ResourceBudget::new(128 * 1024);
        let mut progress = SstCursorPosition::default();
        let mut observed = Vec::new();
        let reader = SstFileIo::open_for_compaction("resume.sst", fs.clone(), budget.clone())?;
        // Act
        let first = reader.visit_raw_versions_with_progress(
            budget.clone(),
            None,
            None,
            &mut progress,
            &mut |version| {
                if version.seq == 2 {
                    return Err(MidgeError::Timeout("interrupted visitor".into()));
                }
                observed.push(version.seq);
                Ok(())
            },
            &mut || Ok(()),
        );
        let reader = SstFileIo::open_for_compaction("resume.sst", fs, budget.clone())?;
        reader.visit_raw_versions_with_progress(
            budget,
            None,
            None,
            &mut progress,
            &mut |version| {
                observed.push(version.seq);
                Ok(())
            },
            &mut || Ok(()),
        )?;
        // Assert
        assert!(matches!(first, Err(MidgeError::Timeout(_))));
        assert_eq!(observed, vec![3, 2, 1]);
        assert!(progress.complete);
        Ok(())
    }
}
