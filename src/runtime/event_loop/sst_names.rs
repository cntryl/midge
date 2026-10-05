use super::EventLoop;

/// SST names one journal append and mirror reserve at a time.
const SST_NAME_RESERVATION_BLOCK: u64 = 16;

impl EventLoop {
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
        self.reserve_sst_name_with_origin(cf_id, sst_seq, None)
    }

    pub(super) fn reserve_sst_name_durably_within(
        &mut self,
        cf_id: crate::types::ColumnFamilyId,
        sst_seq: u64,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<()> {
        self.reserve_sst_name_with_origin(cf_id, sst_seq, Some(deadline))
    }

    fn reserve_sst_name_with_origin(
        &mut self,
        cf_id: crate::types::ColumnFamilyId,
        sst_seq: u64,
        deadline: Option<&crate::common::OperationDeadline>,
    ) -> crate::common::MidgeResult<()> {
        check_reservation_deadline(deadline)?;
        let reserved_through = self
            .state
            .sst_names
            .reserved_through
            .get(&cf_id)
            .copied()
            .unwrap_or(0);
        if sst_seq < reserved_through {
            return Ok(());
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
        let edit_id = self
            .state
            .manifest_store
            .append(&crate::metadata::ManifestEdit::BumpNextSstSeq { cf_id, next_seq })?;
        self.state.manifest.set_next_sst_seq(cf_id, next_seq);
        self.state.manifest.note_applied_journal_edit(edit_id);
        if let Some(deadline) = deadline {
            self.mirror_metadata_to_authoritative_cloud_within(deadline)?;
        } else {
            self.mirror_metadata_to_authoritative_cloud()?;
        }
        check_reservation_deadline(deadline)?;
        self.state
            .sst_names
            .reserved_through
            .insert(cf_id, next_seq);
        Ok(())
    }
}

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
