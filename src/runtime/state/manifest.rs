//! Manifest publication and compaction/flush intent transitions.

use super::{MidgeResult, PublicationPhase, RuntimeState};
use crate::metadata::accounting::Origin;

#[derive(Clone, Copy)]
pub(crate) struct FlushManifestPublication<'a> {
    pub(crate) origin: Origin,
    pub(crate) cf_id: crate::types::ColumnFamilyId,
    pub(crate) sequence: u64,
    pub(crate) file_meta: &'a crate::runtime::FileMeta,
    pub(crate) next_sst_seq: u64,
    pub(crate) require_snapshot: bool,
}

fn flush_record_is_bounded(file_meta: &crate::runtime::FileMeta) -> bool {
    // Bound deferred-record encoding before cloning metadata or allocating
    // JSON. JSON escapes consume at most six bytes per name byte; key
    // bounds are hex. The fixed allowance covers numeric fields, both
    // frontier edits, the batch envelope, framing and the fsync marker.
    let record_bound = file_meta
        .name
        .len()
        .saturating_mul(6)
        .saturating_add(
            file_meta
                .smallest_key
                .as_ref()
                .map_or(0, |key| key.len().saturating_mul(2)),
        )
        .saturating_add(
            file_meta
                .largest_key
                .as_ref()
                .map_or(0, |key| key.len().saturating_mul(2)),
        )
        .saturating_add(1_536);
    record_bound <= crate::metadata::store::LOCAL_FLUSH_RECORD_BYTES
}

impl RuntimeState {
    #[cfg(test)]
    pub fn append_intent(&mut self, entry: crate::runtime::IntentLogEntry) -> MidgeResult<()> {
        let mut proposed = self.intent_log.clone();
        proposed.push(entry);
        self.persist_intent_entries(&proposed)?;
        self.intent_log = proposed;
        Ok(())
    }

    /// Re-read the manifest and intent log after a local metadata write had
    /// an uncertain result. No other runtime writer may advance either file.
    pub(crate) fn reload_persisted_metadata(&mut self) -> MidgeResult<()> {
        if self.is_memory_mode() {
            return Ok(());
        }
        // Strict: only startup may salvage-heal (see RuntimeState::load_manifest).
        // Load both before assigning either: a manifest fresher than its
        // intents is as unsafe to publish from as two stale files.
        let loaded = crate::metadata::ManifestPersistence::load_with_fs_and_policy(
            &self.fs,
            crate::config::RecoveryPolicy::Strict,
        )
        .and_then(|manifest| {
            crate::runtime::IntentPersistence::load_with_fs_and_policy(
                &self.fs,
                crate::config::RecoveryPolicy::Strict,
            )
            .map(|intents| (manifest, intents))
        });
        match loaded {
            Ok((manifest, intents)) => {
                self.manifest.replace(manifest);
                self.intent_log = intents;
                self.recovery.metadata = super::MetadataSync::Current;
                Ok(())
            }
            Err(error) => {
                self.recovery.metadata = super::MetadataSync::ReloadRequired;
                self.mark_persistence_anomaly();
                Err(crate::common::MidgeError::Internal(error))
            }
        }
    }

    /// Retries a reload that failed earlier. A no-op once memory is current.
    pub(crate) fn retry_metadata_reload(&mut self) -> MidgeResult<()> {
        if self.recovery.metadata == super::MetadataSync::ReloadRequired {
            self.reload_persisted_metadata().map_err(|error| {
                crate::common::MidgeError::Fenced(format!(
                    "manifest and intent log are still behind disk; refusing to publish: {error}"
                ))
            })?;
        }
        Ok(())
    }

    /// Refuses a publication from memory while memory is known to be behind
    /// disk; see `MetadataSync::ReloadRequired`.
    pub(crate) fn ensure_metadata_current(&self) -> MidgeResult<()> {
        if self.recovery.metadata == super::MetadataSync::ReloadRequired {
            return Err(crate::common::MidgeError::Fenced(
                "manifest and intent log could not be reloaded after a failed publication; \
                 refusing to publish from stale metadata"
                    .into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn fence_metadata_until_reloaded(&mut self) {
        self.recovery.metadata = super::MetadataSync::ReloadRequired;
        self.mark_persistence_anomaly();
    }

    pub(super) fn persist_intent_entries(
        &self,
        intents: &[crate::runtime::IntentLogEntry],
    ) -> MidgeResult<()> {
        self.ensure_metadata_current()?;
        if !self.is_memory_mode() {
            crate::runtime::IntentPersistence::save(&self.db_path, intents)
                .map_err(crate::common::MidgeError::Internal)?;
        }
        Ok(())
    }

    pub fn record_compaction_publication_intent(
        &mut self,
        cf_id: crate::types::ColumnFamilyId,
        removed: Vec<String>,
        added: Vec<crate::runtime::FileMeta>,
    ) -> MidgeResult<Vec<String>> {
        if added.iter().any(|file_meta| file_meta.cf_id != cf_id) {
            return Err(crate::common::MidgeError::Corruption(format!(
                "compaction output metadata does not belong to column family {cf_id}"
            )));
        }
        let same_inputs = |existing_cf_id: crate::types::ColumnFamilyId,
                           existing_removed: &[String]| {
            existing_cf_id == cf_id
                && Self::same_file_name_set(
                    existing_removed.iter().map(String::as_str),
                    removed.iter().map(String::as_str),
                )
        };
        let same_outputs = |existing_added: &[crate::runtime::FileMeta]| {
            !existing_added.is_empty()
                && !added.is_empty()
                && Self::same_file_name_set(
                    existing_added.iter().map(|meta| meta.name.as_str()),
                    added.iter().map(|meta| meta.name.as_str()),
                )
        };

        for entry in &self.intent_log {
            let crate::runtime::IntentLogEntry::CompactionPublish {
                phase,
                cf_id: existing_cf_id,
                removed: existing_removed,
                added: existing_added,
            } = entry
            else {
                continue;
            };
            let inputs_match = same_inputs(*existing_cf_id, existing_removed);
            let outputs_match = same_outputs(existing_added);
            if *phase == PublicationPhase::ManifestPublished && (inputs_match || outputs_match) {
                return Err(crate::common::MidgeError::Busy(
                    "cannot replace a manifest-published compaction intent".to_string(),
                ));
            }
            if outputs_match && !inputs_match {
                return Err(crate::common::MidgeError::Corruption(
                    "compaction output identity is already owned by different inputs".to_string(),
                ));
            }
        }

        let mut proposed = self.intent_log.clone();
        let mut superseded_outputs = Vec::new();
        proposed.retain(|entry| {
            let crate::runtime::IntentLogEntry::CompactionPublish {
                phase: PublicationPhase::OutputDurable,
                cf_id: existing_cf_id,
                removed: existing_removed,
                added: existing_added,
            } = entry
            else {
                return true;
            };
            let replace =
                same_inputs(*existing_cf_id, existing_removed) || same_outputs(existing_added);
            if replace {
                superseded_outputs.extend(
                    existing_added
                        .iter()
                        .map(|meta| meta.name.clone())
                        .filter(|name| !added.iter().any(|meta| meta.name == *name)),
                );
            }
            !replace
        });
        proposed.push(crate::runtime::IntentLogEntry::CompactionPublish {
            phase: PublicationPhase::OutputDurable,
            cf_id,
            removed,
            added,
        });
        self.persist_intent_entries(&proposed)?;
        self.intent_log = proposed;
        superseded_outputs.sort();
        superseded_outputs.dedup();
        Ok(superseded_outputs)
    }

    #[cfg(test)]
    pub fn transition_flush_publication_intent(
        &mut self,
        sst_name: &str,
        phase: PublicationPhase,
    ) -> MidgeResult<()> {
        let mut proposed = self.intent_log.clone();
        for entry in &mut proposed {
            if let crate::runtime::IntentLogEntry::FlushPublish {
                phase: current_phase,
                file_meta,
                ..
            } = entry
            {
                if file_meta.name == sst_name {
                    *current_phase = phase;
                }
            }
        }
        self.persist_intent_entries(&proposed)?;
        self.intent_log = proposed;
        Ok(())
    }

    pub(crate) fn record_flush_publication_intent(
        &mut self,
        cf_id: crate::types::ColumnFamilyId,
        sequence: u64,
        file_meta: &crate::runtime::FileMeta,
    ) -> MidgeResult<()> {
        let mut proposed = self.intent_log.clone();
        proposed.retain(|entry| {
            !matches!(
                entry,
                crate::runtime::IntentLogEntry::FlushPublish { file_meta: existing, .. }
                    if existing.name == file_meta.name
            )
        });
        proposed.push(crate::runtime::IntentLogEntry::FlushPublish {
            phase: PublicationPhase::OutputDurable,
            cf_id,
            sequence,
            file_meta: file_meta.clone(),
        });
        self.persist_intent_entries(&proposed)?;
        self.intent_log = proposed;
        Ok(())
    }

    pub fn clear_flush_publication_intent(&mut self, sst_name: &str) -> MidgeResult<()> {
        let mut proposed = self.intent_log.clone();
        proposed.retain(|entry| {
            !matches!(
                entry,
                crate::runtime::IntentLogEntry::FlushPublish { file_meta, .. }
                    if file_meta.name == sst_name
            )
        });
        self.persist_intent_entries(&proposed)?;
        self.intent_log = proposed;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn commit_flush_publication(
        &mut self,
        cf_id: crate::types::ColumnFamilyId,
        sequence: u64,
        file_meta: &crate::runtime::FileMeta,
        next_sst_seq: u64,
        require_snapshot: bool,
    ) -> MidgeResult<()> {
        self.commit_flush_publication_for(FlushManifestPublication {
            origin: Origin::Unclassified,
            cf_id,
            sequence,
            file_meta,
            next_sst_seq,
            require_snapshot,
        })
    }

    fn prepare_local_flush_checkpoint(
        &mut self,
        origin: Origin,
        bounded_record: bool,
        forced_path: bool,
    ) -> MidgeResult<()> {
        if !forced_path
            && (self
                .manifest_store
                .local_checkpoint_due(self.manifest.edit_checkpoint_id)
                || !bounded_record)
        {
            // A previous failed checkpoint cannot allow another journal edit
            // to grow the deferred tail. Preserve authority and backpressure.
            let checkpoint = self
                .manifest_store
                .save_snapshot_for(origin, &self.manifest)?;
            if !checkpoint.caller_was_current {
                self.fence_metadata_until_reloaded();
                return Err(crate::common::MidgeError::Fenced(
                    "local flush checkpoint caller is behind durable authority".into(),
                ));
            }
            self.manifest.adopt_checkpoint(checkpoint);
        }
        Ok(())
    }

    fn append_flush_manifest_edits(
        &self,
        publication: FlushManifestPublication<'_>,
        file_meta: crate::metadata::FileMeta,
        next_sst_seq: u64,
        include_frontier: bool,
    ) -> MidgeResult<u64> {
        let next = crate::metadata::ManifestEdit::BumpNextSstSeq {
            cf_id: publication.cf_id,
            next_seq: next_sst_seq,
        };
        let add = crate::metadata::ManifestEdit::AddSst(file_meta);
        if include_frontier {
            self.manifest_store.append_batch_for(
                publication.origin,
                &[
                    next,
                    add,
                    crate::metadata::ManifestEdit::BumpWalSeq {
                        seq: publication.sequence,
                    },
                ],
            )
        } else {
            // Healthy cheap flushes retain the original two-edit payload;
            // their forced snapshot makes the remaining frontier durable.
            self.manifest_store
                .append_batch_for(publication.origin, &[next, add])
        }
    }

    fn checkpoint_flush_publication(
        &mut self,
        publication: FlushManifestPublication<'_>,
        next_sst_seq: u64,
        frontiers_journaled: bool,
    ) -> MidgeResult<()> {
        match self
            .manifest_store
            .save_snapshot_for(publication.origin, &self.manifest)
        {
            Ok(written) => self.manifest.adopt_checkpoint(written),
            Err(error) if !publication.require_snapshot => {
                self.mark_persistence_anomaly();
                if !frontiers_journaled {
                    // No flush ACK until both counter and frontier survive
                    // without this failed snapshot. A fallback append must
                    // not release the store's checkpoint retry pressure.
                    let edit_id = self
                        .manifest_store
                        .append_batch_for(
                            publication.origin,
                            &[
                                crate::metadata::ManifestEdit::BumpNextSstSeq {
                                    cf_id: publication.cf_id,
                                    next_seq: next_sst_seq,
                                },
                                crate::metadata::ManifestEdit::BumpWalSeq {
                                    seq: publication.sequence,
                                },
                            ],
                        )
                        .inspect_err(|_| {
                            self.fence_metadata_until_reloaded();
                        })?;
                    self.manifest.note_applied_journal_edit(edit_id);
                }
                tracing::warn!(%error, "manifest journal is durable but checkpoint save failed");
            }
            Err(error) => return Err(error),
        }
        Ok(())
    }

    pub(crate) fn commit_flush_publication_for(
        &mut self,
        publication: FlushManifestPublication<'_>,
    ) -> MidgeResult<()> {
        let FlushManifestPublication {
            origin,
            cf_id,
            sequence,
            file_meta,
            next_sst_seq,
            require_snapshot,
        } = publication;
        self.ensure_metadata_current()?;
        let ordinary_local = origin == Origin::OrdinaryLocalFlush && !require_snapshot;
        let bounded_record = flush_record_is_bounded(file_meta);
        let forced_path = ordinary_local
            && bounded_record
            && self
                .manifest_store
                .can_use_forced_flush_path(self.manifest.edit_checkpoint_id, file_meta.size_bytes);
        if ordinary_local {
            self.prepare_local_flush_checkpoint(origin, bounded_record, forced_path)?;
        }
        let manifest_meta: crate::metadata::FileMeta = file_meta.into();
        let next_sst_seq = self
            .manifest
            .next_sst_seqs
            .get(&cf_id)
            .copied()
            .unwrap_or(1)
            .max(next_sst_seq);
        let mut appended = false;
        match self
            .manifest
            .files
            .iter()
            .find(|file| file.name == manifest_meta.name)
        {
            Some(existing) if existing.same_identity(&manifest_meta) => {}
            Some(_) => {
                return Err(crate::common::MidgeError::Corruption(format!(
                    "flush output conflicts with existing SST {}",
                    manifest_meta.name
                )));
            }
            None => {
                crate::failpoints::fail_point!("midge::flush_worker::before_manifest_persist");
                let edit_id = self.append_flush_manifest_edits(
                    publication,
                    manifest_meta.clone(),
                    next_sst_seq,
                    !forced_path,
                )?;
                self.manifest.add_file(manifest_meta);
                self.manifest.note_applied_journal_edit(edit_id);
                appended = true;
                crate::failpoints::fail_point!("midge::flush_worker::after_manifest_journal");
            }
        }
        self.manifest.advance_next_sst_seq(cf_id, next_sst_seq);
        self.manifest.advance_persisted_sequence(sequence);
        self.clear_flush_publication_intent(&file_meta.name)?;
        if ordinary_local
            && !forced_path
            && bounded_record
            && appended
            && self
                .manifest_store
                .local_checkpoint_is_costly(file_meta.size_bytes)
            && !self
                .manifest_store
                .local_checkpoint_due(self.manifest.edit_checkpoint_id)
        {
            return Ok(());
        }
        self.checkpoint_flush_publication(publication, next_sst_seq, appended && !forced_path)
    }

    pub fn transition_compaction_publication_intent(
        &mut self,
        input_ssts: &[String],
        output_ssts: &[String],
        phase: PublicationPhase,
    ) -> MidgeResult<()> {
        let mut proposed = self.intent_log.clone();
        let mut matched = 0usize;
        for entry in &mut proposed {
            if let crate::runtime::IntentLogEntry::CompactionPublish {
                phase: current_phase,
                removed,
                added,
                ..
            } = entry
            {
                if Self::same_file_name_set(
                    removed.iter().map(String::as_str),
                    input_ssts.iter().map(String::as_str),
                ) && Self::same_file_name_set(
                    added.iter().map(|meta| meta.name.as_str()),
                    output_ssts.iter().map(String::as_str),
                ) {
                    matched = matched.saturating_add(1);
                    *current_phase = phase;
                }
            }
        }
        if matched != 1 {
            return Err(crate::common::MidgeError::Corruption(format!(
                "expected exactly one compaction publication intent transition, found {matched}"
            )));
        }
        self.persist_intent_entries(&proposed)?;
        self.intent_log = proposed;
        Ok(())
    }

    pub fn clear_compaction_publication_intent(
        &mut self,
        input_ssts: &[String],
        output_ssts: &[String],
    ) -> MidgeResult<()> {
        let mut proposed = self.intent_log.clone();
        let before = proposed.len();
        proposed.retain(|entry| {
            !matches!(
                entry,
                crate::runtime::IntentLogEntry::CompactionPublish { removed, added, .. }
                    if Self::same_file_name_set(
                        removed.iter().map(String::as_str),
                        input_ssts.iter().map(String::as_str),
                    ) && Self::same_file_name_set(
                        added.iter().map(|meta| meta.name.as_str()),
                        output_ssts.iter().map(String::as_str),
                    )
            )
        });
        let removed_count = before.saturating_sub(proposed.len());
        if removed_count != 1 {
            return Err(crate::common::MidgeError::Corruption(format!(
                "expected exactly one compaction publication intent clear, found {removed_count}"
            )));
        }
        crate::failpoints::fail_point!(
            "midge::compaction::inject_no_space_on_intent_clear",
            |_| Err(crate::common::MidgeError::NoSpace(
                "failpoint: no space while clearing compaction publication intent".to_string()
            ))
        );
        self.persist_intent_entries(&proposed)?;
        self.intent_log = proposed;
        Ok(())
    }

    pub(crate) fn has_compaction_publication_intent(
        &self,
        input_ssts: &[String],
        output_ssts: &[String],
    ) -> bool {
        self.intent_log.iter().any(|entry| {
            matches!(
                entry,
                crate::runtime::IntentLogEntry::CompactionPublish { removed, added, .. }
                    if Self::same_file_name_set(
                        removed.iter().map(String::as_str),
                        input_ssts.iter().map(String::as_str),
                    ) && Self::same_file_name_set(
                        added.iter().map(|meta| meta.name.as_str()),
                        output_ssts.iter().map(String::as_str),
                    )
            )
        })
    }

    pub(super) fn same_file_name_set<'a, I, J>(left: I, right: J) -> bool
    where
        I: Iterator<Item = &'a str>,
        J: Iterator<Item = &'a str>,
    {
        let mut left_names: Vec<_> = left.collect();
        let mut right_names: Vec<_> = right.collect();
        left_names.sort_unstable();
        right_names.sort_unstable();
        left_names == right_names
    }

    pub(crate) fn manifest_has_file(&self, sst_name: &str) -> bool {
        self.manifest.files.iter().any(|file| file.name == sst_name)
    }

    pub(super) fn insert_manifest_file_if_missing(
        &mut self,
        file_meta: &crate::runtime::FileMeta,
    ) -> bool {
        if self.manifest_has_file(&file_meta.name) {
            return false;
        }

        self.manifest.add_file(file_meta.into());
        true
    }

    pub(super) fn apply_compaction_to_manifest(
        &mut self,
        removed: &[String],
        added: &[crate::runtime::FileMeta],
    ) -> bool {
        let mut changed = false;
        self.manifest.retain_files(|file| {
            let keep = !removed.contains(&file.name);
            if !keep {
                changed = true;
            }
            keep
        });

        for file_meta in added {
            changed |= self.insert_manifest_file_if_missing(file_meta);
        }

        changed
    }

    pub(super) fn append_manifest_add_sst(
        &self,
        file_meta: &crate::runtime::FileMeta,
    ) -> MidgeResult<()> {
        if self.is_memory_mode() {
            return Ok(());
        }

        self.manifest_store
            .append_for(
                Origin::Recovery,
                &crate::metadata::ManifestEdit::AddSst(file_meta.into()),
            )
            .map(|_| ())
    }

    pub(super) fn append_manifest_compaction_batch(
        &self,
        removed: &[String],
        added: &[crate::runtime::FileMeta],
    ) -> MidgeResult<()> {
        if self.is_memory_mode() {
            return Ok(());
        }

        let mut edits = Vec::with_capacity(removed.len() + added.len());
        for name in removed {
            edits.push(crate::metadata::ManifestEdit::RemoveSst { name: name.clone() });
        }
        for file_meta in added {
            edits.push(crate::metadata::ManifestEdit::AddSst(file_meta.into()));
        }

        if edits.is_empty() {
            return Ok(());
        }

        self.manifest_store
            .append_batch_for(Origin::Recovery, &edits)
            .map(|_| ())
    }

    pub(super) fn persist_manifest_checkpoint(&mut self) -> MidgeResult<()> {
        if self.is_memory_mode() {
            return Ok(());
        }
        self.retry_metadata_reload()?;

        let checkpoint = self
            .manifest_store
            .save_snapshot_for(Origin::Recovery, &self.manifest)?;
        self.manifest.adopt_checkpoint(checkpoint);
        Ok(())
    }
}

#[cfg(test)]
mod checkpoint_bound_tests {
    use super::flush_record_is_bounded;

    #[test]
    fn should_bound_framed_flush_record_before_deferring_large_key_metadata() {
        // Arrange
        for key_bytes in [0, 16, 512, 1024, 65_536] {
            let meta = crate::runtime::FileMeta {
                name: "000000_00_18446744073709551615.sst".into(),
                level: u32::MAX,
                size_bytes: u64::MAX,
                content_crc32c: Some(u32::MAX),
                cf_id: u32::MAX,
                smallest_key: Some(vec![255; key_bytes]),
                largest_key: Some(vec![255; key_bytes]),
                smallest_seq: Some(u64::MAX),
                largest_seq: Some(u64::MAX),
                key_bounds_complete: true,
            };

            // Act
            let eligible = flush_record_is_bounded(&meta);

            // Assert: oversized bounds cannot enter the deferred path, while
            // eligible worst-width fields fit including both TLV frames.
            if key_bytes >= 1024 {
                assert!(!eligible);
                continue;
            }
            assert!(eligible);
            let batch = crate::metadata::ManifestEdit::Batch(vec![
                crate::metadata::ManifestEdit::BumpNextSstSeq {
                    cf_id: u32::MAX,
                    next_seq: u64::MAX,
                },
                crate::metadata::ManifestEdit::AddSst((&meta).into()),
                crate::metadata::ManifestEdit::BumpWalSeq { seq: u64::MAX },
            ]);
            let envelope = serde_json::json!({"edit_id": u64::MAX, "edit": batch});
            let marker = crate::metadata::journal::FsyncMarker {
                last_persisted_sequence: u64::MAX,
                ts_millis: u64::MAX,
            };
            let framed_bytes = serde_json::to_vec(&envelope).unwrap().len()
                + serde_json::to_vec(&marker).unwrap().len()
                + 18;
            assert!(
                framed_bytes <= crate::metadata::store::LOCAL_FLUSH_RECORD_BYTES,
                "{key_bytes} key bytes produced {framed_bytes} framed bytes"
            );
        }
    }
}
