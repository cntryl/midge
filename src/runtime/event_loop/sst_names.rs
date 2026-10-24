use super::EventLoop;
use crate::metadata::accounting::Origin;

/// SST names one journal append and mirror reserve at a time.
const SST_NAME_RESERVATION_BLOCK: u64 = 16;
const SST_NAME_PREFETCH_REMAINING: u64 = SST_NAME_RESERVATION_BLOCK / 2;

impl EventLoop {
    /// Reserve the next flush block while no publication owns the control
    /// turn. Completion, rather than this journal append, admits its names.
    pub(super) fn prefetch_flush_sst_names_if_due(&mut self) {
        if self.actors.cloud_coordinator.hybrid_storage.is_none()
            || self.cloud_maintenance_control_blocked()
            || self.compaction_compute_owned()
            || self
                .state
                .sst_names
                .prefetch_retry_at
                .is_some_and(|at| std::time::Instant::now() < at)
        {
            return;
        }
        let mut families = std::iter::once(0).chain(
            self.state
                .manifest
                .column_families
                .iter()
                .filter(|family| family.deleted_at.is_none())
                .map(|family| family.id),
        );
        let candidate = families.find_map(|cf_id| self.flush_name_prefetch_candidate(cf_id));
        let Some((cf_id, cursor, bound)) = candidate else {
            return;
        };
        // A prewarm must retain the current first usable name before its
        // journal raises next_sst_seqs. Otherwise the later flush would start
        // at that new durable bound and skip the entire prefetched block.
        self.state.sst_names.cursor.entry(cf_id).or_insert(cursor);
        let result =
            self.begin_sst_name_reservation(cf_id, bound.max(cursor), None, Origin::Administration);
        if let Err(error) = result {
            self.state.sst_names.prefetch_retry_at =
                Some(std::time::Instant::now() + std::time::Duration::from_secs(1));
            tracing::debug!(%error, cf_id, "SST name prefetch retained for retry");
        } else {
            self.state.sst_names.prefetch_retry_at = None;
        }
    }

    fn flush_name_prefetch_candidate(
        &self,
        cf_id: crate::types::ColumnFamilyId,
    ) -> Option<(crate::types::ColumnFamilyId, u64, u64)> {
        let cursor = self
            .state
            .sst_names
            .cursor
            .get(&cf_id)
            .copied()
            .unwrap_or_else(|| {
                self.state
                    .manifest
                    .next_sst_seqs
                    .get(&cf_id)
                    .copied()
                    .unwrap_or(1)
            });
        let bound = self
            .state
            .sst_names
            .reserved_through
            .get(&cf_id)
            .copied()
            .unwrap_or(0);
        (bound.saturating_sub(cursor) <= SST_NAME_PREFETCH_REMAINING)
            .then_some((cf_id, cursor, bound))
    }

    /// Persist an SST filename allocation before the object can exist
    /// remotely. After a crash between upload and publication, a replacement
    /// must never reuse the orphan's name: immutable publication rejects an
    /// existing object with different bytes, which would wedge it forever.
    ///
    /// A name is covered only once its reservation is both journaled and
    /// mirrored by this session (`SstNameAllocation::reserved_through`).
    /// `manifest.next_sst_seqs` alone is not enough: a failed mirror leaves it
    /// raised locally while cloud metadata never saw it. Otherwise reserve a
    /// block of names with one journal append and one mirror, so the next
    /// flushes reserve nothing. The journal replays on open, so no snapshot is
    /// needed (#491).
    pub(super) fn reserve_sst_name_durably(
        &mut self,
        cf_id: crate::types::ColumnFamilyId,
        sst_seq: u64,
    ) -> crate::common::MidgeResult<()> {
        self.reserve_sst_name_with_origin(cf_id, sst_seq, None, Origin::CompactionBeforeGc)
    }

    pub(super) fn reserve_sst_name_durably_within(
        &mut self,
        cf_id: crate::types::ColumnFamilyId,
        sst_seq: u64,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<()> {
        self.reserve_sst_name_with_origin(
            cf_id,
            sst_seq,
            Some(deadline),
            Origin::CompactionBeforeGc,
        )
    }

    pub(super) fn reserve_sst_name_durably_for(
        &mut self,
        cf_id: crate::types::ColumnFamilyId,
        sst_seq: u64,
        origin: Origin,
    ) -> crate::common::MidgeResult<()> {
        self.reserve_sst_name_with_origin(cf_id, sst_seq, None, origin)
    }

    fn reserve_sst_name_with_origin(
        &mut self,
        cf_id: crate::types::ColumnFamilyId,
        sst_seq: u64,
        deadline: Option<&crate::common::OperationDeadline>,
        origin: Origin,
    ) -> crate::common::MidgeResult<()> {
        if self.begin_sst_name_reservation(cf_id, sst_seq, deadline, origin)? {
            return Err(crate::common::MidgeError::Busy(
                "SST filename reservation publication pending".into(),
            ));
        }
        Ok(())
    }

    /// Retains a pending mirror before any output is admitted remotely.
    pub(super) fn begin_sst_name_reservation(
        &mut self,
        cf_id: crate::types::ColumnFamilyId,
        sst_seq: u64,
        deadline: Option<&crate::common::OperationDeadline>,
        origin: Origin,
    ) -> crate::common::MidgeResult<bool> {
        check_reservation_deadline(deadline)?;
        let reserved_through = self
            .state
            .sst_names
            .reserved_through
            .get(&cf_id)
            .copied()
            .unwrap_or(0);
        if sst_seq < reserved_through {
            return Ok(false);
        }
        // The mirror below publishes local metadata files; never publish
        // them while memory is known to be behind disk (#500). Reload first,
        // so the counter read next is the reloaded one.
        self.state.retry_metadata_reload()?;
        check_reservation_deadline(deadline)?;
        let durable_next = self
            .state
            .manifest
            .next_sst_seqs
            .get(&cf_id)
            .copied()
            .unwrap_or(1);
        // Never lower the counter: the edit replays as a max, and memory must
        // match what the journal replays.
        let next_seq = sst_seq
            .checked_add(SST_NAME_RESERVATION_BLOCK)
            .ok_or_else(|| {
                crate::common::MidgeError::ResourceLimit("SST filename allocation exhausted".into())
            })?
            .max(durable_next);
        let edit_id = self.state.manifest_store.append_for(
            origin,
            &crate::metadata::ManifestEdit::BumpNextSstSeq { cf_id, next_seq },
        )?;
        self.state.manifest.set_next_sst_seq(cf_id, next_seq);
        self.state.manifest.note_applied_journal_edit(edit_id);
        let mirror_deadline = deadline
            .copied()
            .unwrap_or_else(|| self.event_loop_cloud_deadline());
        if self.start_metadata_mirror(
            super::metadata_mirror::MirrorContinuation::SstReservation { cf_id, next_seq },
            mirror_deadline,
        )? {
            return Ok(true);
        }
        check_reservation_deadline(deadline)?;
        self.state
            .sst_names
            .reserved_through
            .insert(cf_id, next_seq);
        Ok(false)
    }
}

#[cfg(test)]
mod tests;

fn check_reservation_deadline(
    deadline: Option<&crate::common::OperationDeadline>,
) -> crate::common::MidgeResult<()> {
    if deadline.is_some_and(crate::common::OperationDeadline::is_expired) {
        return Err(crate::common::MidgeError::Timeout(
            "SST filename reservation exceeded the caller deadline".into(),
        ));
    }
    Ok(())
}
