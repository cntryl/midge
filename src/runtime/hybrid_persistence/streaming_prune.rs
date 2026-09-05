//! Exact WAL retirement with memory bounded independently of segment length.

use super::{
    exact_state_sequence, file_may_contain_record_key, verify_sst_summary_matches_manifest,
    CloudWalPruneBatchResults, CloudWalPruneGuard, DataCoverageRecord, ExactCoverageState,
    FileMeta, GuardedObjectProof, HybridStorage, Manifest, MidgeError, MidgeResult,
    PublishedWalSegment, RemoteObjectProof, StorageBackend, StorageObjectMetadata,
    ValidatedWalObject, ValidatedWalPruneCandidate, WalPublicationCatalog,
};
use crate::common::resource_budget::{ResourceBudget, ResourceReservation};
use crate::io::{Fs, FsPath, OpenMode, OpenOptions};
use crate::wal::recovery::streaming::{visit_sealed_wal_records_from, StreamingReplayLimits};
use std::collections::BTreeMap;
use std::sync::Arc;

mod progress;
pub(super) use progress::Progress;
use progress::{CrcProgress, RecordProgress, SegmentProgress, SstProgress};

pub(super) struct StreamedValidation {
    pub results: CloudWalPruneBatchResults,
    pub candidates: Vec<ValidatedWalPruneCandidate>,
    pub coverage: Vec<bool>,
    pub dependencies: Vec<GuardedObjectProof>,
    // Keep proof storage charged through the catalog CAS and delete scheduling.
    pub reservations: Vec<Arc<ResourceReservation>>,
}

struct Coverage<'a> {
    storage: &'a HybridStorage,
    manifest: &'a Manifest,
    deadline: &'a crate::common::OperationDeadline,
    budget: ResourceBudget,
    progress: &'a mut Progress,
    window: usize,
}

pub(super) fn validate(
    storage: &HybridStorage,
    candidates: &[(u64, u64)],
    catalog: &WalPublicationCatalog,
    guard: &CloudWalPruneGuard,
    deadline: &crate::common::OperationDeadline,
) -> MidgeResult<StreamedValidation> {
    let mut progress = guard.progress.0.lock();
    progress.prepare(storage, guard, catalog, deadline)?;
    let budget = progress.budget.clone();
    let unit = budget.limit() / 8;
    let limits = StreamingReplayLimits {
        max_frame_bytes: unit,
        max_pending_txn_bytes: unit,
        max_memtable_encoded_bytes: unit,
        target_memtable_encoded_bytes: unit,
    };
    // Encoded frame, decoded record/batch, and one copied coverage operation
    // coexist. Their parsers reject oversized declarations before allocation.
    let _workspace = budget.reserve(unit.saturating_mul(4), "WAL retirement frame workspace")?;
    let mut coverage = Coverage {
        storage,
        manifest: &guard.manifest,
        deadline,
        budget,
        progress: &mut progress,
        window: unit.max(1),
    };
    let mut output = StreamedValidation {
        results: Vec::new(),
        candidates: Vec::new(),
        coverage: Vec::new(),
        dependencies: Vec::new(),
        reservations: Vec::new(),
    };
    let mut blocked_by = None;
    for &(segment_id, expected_sequence) in candidates {
        let Some(entry) = catalog.segments.get(&segment_id) else {
            output.results.push((segment_id, Ok(())));
            continue;
        };
        if let Some(older) = blocked_by {
            output.results.push((
                segment_id,
                Err(MidgeError::Busy(format!(
                "cloud WAL segment {segment_id} cannot retire past authoritative segment {older}"
            ))),
            ));
            continue;
        }
        let result = if entry.max_sequence == expected_sequence {
            validate_segment(&mut coverage, entry, limits)
        } else {
            Err(MidgeError::Corruption(format!(
                "cloud WAL catalog segment {segment_id} max sequence does not match {expected_sequence}"
            )))
        };
        match result {
            Ok((proof, covered, reservation)) => {
                output.candidates.push(ValidatedWalPruneCandidate {
                    segment_id,
                    entry: entry.clone(),
                    validated: ValidatedWalObject {
                        proof,
                        data_records: Vec::new(),
                    },
                });
                output.coverage.push(covered);
                output.reservations.push(Arc::new(reservation));
                if !covered {
                    blocked_by = Some(segment_id);
                }
            }
            Err(error) => {
                output.results.push((segment_id, Err(error)));
                blocked_by = Some(segment_id);
            }
        }
    }
    for (name, proof) in &coverage.progress.ssts {
        if proof.complete {
            let key = crate::sst::object_key(name);
            output.reservations.push(Arc::clone(&proof.reservation));
            output.dependencies.push(GuardedObjectProof::range_identity(
                storage.remote_sst_backend(),
                key,
                proof.metadata.clone(),
            ));
        }
    }
    Ok(output)
}

fn pinned_fs(
    storage: &HybridStorage,
    backend: Arc<dyn StorageBackend>,
    key: String,
    metadata: StorageObjectMetadata,
    deadline: &crate::common::OperationDeadline,
) -> Arc<dyn Fs> {
    Arc::new(
        crate::storage::remote_sst::RemoteSstFs::for_object(
            Arc::new(crate::io::MockFs::new()),
            backend,
            key,
            metadata,
            storage.storage_io_timeout(),
        )
        .with_deadline(*deadline),
    )
}

fn proof_bytes(key: &str, metadata: &StorageObjectMetadata) -> usize {
    // Account for the cached proof, tree/vector bookkeeping, and the temporary
    // clones made by dependency verification and conditional delete scheduling.
    1024_usize
        .saturating_add(key.len().saturating_mul(8))
        .saturating_add(metadata.etag.len().saturating_mul(8))
        .saturating_add(
            metadata
                .generation
                .as_ref()
                .map_or(0, String::len)
                .saturating_mul(8),
        )
}

fn validate_segment(
    coverage: &mut Coverage<'_>,
    entry: &PublishedWalSegment,
    limits: StreamingReplayLimits,
) -> MidgeResult<(RemoteObjectProof, bool, ResourceReservation)> {
    let metadata = coverage
        .storage
        .remote_range_metadata_within(&entry.object_key, coverage.deadline)?;
    if metadata.size != entry.size_bytes {
        return Err(MidgeError::Corruption(format!(
            "cloud WAL segment {} size differs from catalog",
            entry.segment_id
        )));
    }
    let previous = coverage.progress.segment.take();
    let mut segment = match previous {
        Some(progress) if progress.entry == *entry && progress.metadata.same_version(&metadata) => {
            progress
        }
        _ => SegmentProgress {
            entry: entry.clone(),
            metadata: metadata.clone(),
            crc: CrcProgress::default(),
            prefix: crate::wal::recovery::VerifiedWalPrefix::default(),
            operation: 0,
            record: RecordProgress::default(),
            _reservation: coverage.budget.reserve(
                proof_bytes(&entry.object_key, &metadata),
                "resumable WAL proof",
            )?,
        },
    };
    let result = validate_segment_progress(coverage, &mut segment, limits);
    coverage.progress.segment = Some(segment);
    result?;
    Ok((
        RemoteObjectProof::from_validated_ranges(entry.object_key.clone(), metadata.clone()),
        true,
        coverage.budget.reserve(
            proof_bytes(&entry.object_key, &metadata),
            "WAL retirement target proof",
        )?,
    ))
}

fn validate_segment_progress(
    coverage: &mut Coverage<'_>,
    segment: &mut SegmentProgress,
    limits: StreamingReplayLimits,
) -> MidgeResult<()> {
    let entry = &segment.entry;
    let fs = pinned_fs(
        coverage.storage,
        coverage.storage.remote_wal_backend(),
        entry.object_key.clone(),
        segment.metadata.clone(),
        coverage.deadline,
    );
    segment.crc.verify(
        fs.as_ref(),
        &entry.object_key,
        segment.metadata.size,
        entry.content_crc32c,
        coverage.window,
    )?;
    let path = FsPath::new(&entry.object_key);
    let file = fs.open(
        &path,
        OpenOptions {
            mode: OpenMode::ReadOnly,
            create: false,
            create_new: false,
            truncate: false,
        },
    )?;
    let _read_window = coverage
        .budget
        .reserve(coverage.window, "WAL retirement read window")?;
    let file = crate::io::buffered_read::BufferedReadFile::new(file, coverage.window)?;
    let SegmentProgress {
        prefix,
        operation,
        record,
        ..
    } = segment;
    visit_sealed_wal_records_from(&file, &path, limits, prefix, &mut |frame| {
        if !coverage.covers_frame(
            frame,
            limits.max_pending_txn_bytes,
            operation,
            record,
            entry.segment_id,
        )? {
            return Err(MidgeError::Busy(
                "WAL frame is not exactly covered by committed SSTs".into(),
            ));
        }
        *operation = 0;
        *record = RecordProgress::default();
        Ok(())
    })?;
    if prefix.max_sequence != entry.max_sequence || prefix.writer_epoch != entry.writer_epoch {
        return Err(MidgeError::Corruption(format!(
            "cloud WAL segment {} contents differ from catalog sequence or epoch",
            entry.segment_id
        )));
    }
    Ok(())
}

impl Coverage<'_> {
    fn covers_frame(
        &mut self,
        record: &crate::wal::WalRecord,
        max_decoded_bytes: usize,
        operation: &mut usize,
        progress: &mut RecordProgress,
        segment_id: u64,
    ) -> MidgeResult<bool> {
        if record.op.is_transaction_batch() {
            let payload = record
                .value
                .as_ref()
                .ok_or_else(|| MidgeError::Corruption("WAL batch missing payload".into()))?;
            let batch = crate::wal::encoding::decode_txn_batch_payload_bounded(
                record,
                payload,
                max_decoded_bytes,
            )?;
            for (index, op) in batch.records.into_iter().enumerate().skip(*operation) {
                if !self.covers_record(
                    &DataCoverageRecord {
                        cf_id: op.cf_id,
                        op: op.op,
                        key: op.key.to_vec(),
                        value: op.value.map(|value| value.to_vec()),
                        expiration: op.expiration,
                        range_end: op.range_end.map(|key| key.to_vec()),
                        seq: op.seq,
                    },
                    progress,
                    segment_id,
                )? {
                    return Ok(false);
                }
                *operation = index + 1;
                *progress = RecordProgress::default();
            }
            Ok(true)
        } else if record.op.is_transaction_marker() {
            Ok(true)
        } else {
            self.covers_record(
                &DataCoverageRecord {
                    cf_id: record.cf_id,
                    op: record.op,
                    key: record.key.to_vec(),
                    value: record.value.as_ref().map(|value| value.to_vec()),
                    expiration: record.expiration,
                    range_end: record.range_end.as_ref().map(|key| key.to_vec()),
                    seq: record.seq,
                },
                progress,
                segment_id,
            )
        }
    }

    fn covers_record(
        &mut self,
        record: &DataCoverageRecord,
        progress: &mut RecordProgress,
        segment_id: u64,
    ) -> MidgeResult<bool> {
        while progress.file_index < self.manifest.files.len() {
            let file = &self.manifest.files[progress.file_index];
            if !file.key_bounds_complete || file_may_contain_record_key(file, record) {
                let fs = self.verified_source(file, segment_id)?;
                if file_may_contain_record_key(file, record) {
                    let reader = crate::sst::fs::SstFileIo::open_for_compaction(
                        &file.name,
                        fs,
                        self.budget.clone(),
                    )?;
                    let mut end = record.key.clone();
                    end.push(0);
                    reader.visit_raw_versions_with_progress(
                        self.budget.clone(),
                        Some(record.key.clone()),
                        Some(end),
                        &mut progress.cursor,
                        &mut |version| {
                            if version.key != record.key {
                                return Ok(());
                            }
                            let held = self.budget.reserve(
                                version.value.as_ref().map_or(0, Vec::len),
                                "WAL exact coverage state",
                            )?;
                            let raw = if version.is_tombstone {
                                crate::sst::types::KeyState::Tombstone(version.seq)
                            } else {
                                crate::sst::types::KeyState::Value(
                                    bytes::Bytes::from(version.value.unwrap_or_default()),
                                    version.seq,
                                    version.expiration,
                                    crate::wal::WalOpKind::Put.to_wire_format(),
                                )
                            };
                            let retain = progress
                                .state
                                .state
                                .as_ref()
                                .and_then(exact_state_sequence)
                                .is_none_or(|sequence| version.seq > sequence);
                            progress.state.observe(raw);
                            if retain {
                                progress.held_value = Some(held);
                            }
                            Ok(())
                        },
                    )?;
                }
            }
            progress.file_index += 1;
            progress.cursor = crate::sst::fs::reader_io::SstCursorPosition::default();
        }
        Ok(progress.state.exactly_covers(record))
    }

    fn verified_source(&mut self, file: &FileMeta, segment_id: u64) -> MidgeResult<Arc<dyn Fs>> {
        let key = crate::sst::object_key(&file.name);
        let mut proof = if let Some(proof) = self.progress.ssts.remove(&file.name) {
            proof
        } else {
            let metadata = self
                .storage
                .remote_range_metadata_within(&key, self.deadline)?;
            if file.size_bytes != 0 && file.size_bytes != metadata.size {
                return Err(MidgeError::Corruption(format!(
                    "cloud SST '{}' size differs from manifest",
                    file.name
                )));
            }
            let bytes = proof_bytes(&key, &metadata)
                .saturating_mul(2)
                .saturating_add(file.smallest_key.as_ref().map_or(0, Vec::len))
                .saturating_add(file.largest_key.as_ref().map_or(0, Vec::len));
            SstProgress {
                expected: file.clone(),
                metadata,
                crc: CrcProgress::default(),
                summary: crate::sst::fs::reader_io::SstSummaryProgress::default(),
                complete: false,
                last_used_segment: segment_id,
                // Admit both cached work and its publication dependency before
                // proving a row. Exporting a completed batch must not discover
                // an extra allocation that strands its already-covered prefix.
                reservation: Arc::new(
                    self.budget
                        .reserve(bytes, "resumable SST proof and dependency")?,
                ),
            }
        };
        proof.last_used_segment = segment_id;
        let fs = pinned_fs(
            self.storage,
            self.storage.remote_sst_backend(),
            key,
            proof.metadata.clone(),
            self.deadline,
        );
        let result = self.validate_sst_progress(file, fs.clone(), &mut proof);
        self.progress.ssts.insert(file.name.clone(), proof);
        result?;
        Ok(fs)
    }

    fn validate_sst_progress(
        &self,
        file: &FileMeta,
        fs: Arc<dyn Fs>,
        proof: &mut SstProgress,
    ) -> MidgeResult<()> {
        if proof.complete {
            return Ok(());
        }
        if let Some(crc) = file.content_crc32c {
            proof.crc.verify(
                fs.as_ref(),
                &file.name,
                proof.metadata.size,
                crc,
                self.window,
            )?;
        }
        let summary = crate::sst::fs::SstFileIo::summarize_with_fs_progress(
            &file.name,
            fs,
            &self.budget,
            &mut proof.summary,
        )?;
        verify_sst_summary_matches_manifest(&file.name, summary, file)
            .map_err(MidgeError::Corruption)?;
        proof.complete = true;
        Ok(())
    }
}
