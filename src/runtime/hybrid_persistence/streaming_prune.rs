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
use crate::sst::traits::SstStateReader;
use crate::wal::recovery::streaming::{visit_sealed_wal_records, StreamingReplayLimits};
use std::collections::BTreeMap;
use std::sync::Arc;

pub(super) struct StreamedValidation {
    pub results: CloudWalPruneBatchResults,
    pub candidates: Vec<ValidatedWalPruneCandidate>,
    pub coverage: Vec<bool>,
    pub dependencies: Vec<GuardedObjectProof>,
    // Keep proof storage charged through the catalog CAS and delete scheduling.
    pub reservations: Vec<ResourceReservation>,
}

struct VerifiedSst {
    metadata: StorageObjectMetadata,
    dependency: GuardedObjectProof,
    reservation: ResourceReservation,
}

struct Coverage<'a> {
    storage: &'a HybridStorage,
    manifest: &'a Manifest,
    deadline: &'a crate::common::OperationDeadline,
    budget: ResourceBudget,
    verified: BTreeMap<usize, VerifiedSst>,
    window: usize,
}

pub(super) fn validate(
    storage: &HybridStorage,
    candidates: &[(u64, u64)],
    catalog: &WalPublicationCatalog,
    guard: &CloudWalPruneGuard,
    deadline: &crate::common::OperationDeadline,
) -> MidgeResult<StreamedValidation> {
    let budget = ResourceBudget::new(guard.memory_limit());
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
        verified: BTreeMap::new(),
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
                output.reservations.push(reservation);
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
    for verified in coverage.verified.into_values() {
        output.dependencies.push(verified.dependency);
        output.reservations.push(verified.reservation);
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

fn validate_crc(
    fs: &dyn Fs,
    name: &str,
    length: u64,
    expected: u32,
    window: usize,
) -> MidgeResult<()> {
    let file = fs.open(
        &FsPath::new(name),
        OpenOptions {
            mode: OpenMode::ReadOnly,
            create: false,
            create_new: false,
            truncate: false,
        },
    )?;
    let mut offset = 0;
    let mut crc = 0;
    while offset < length {
        let count = (length - offset).min(window as u64);
        let bytes = file.read_at(offset, count)?;
        if bytes.len() as u64 != count {
            return Err(MidgeError::Corruption(format!(
                "cloud object '{name}' CRC range was truncated"
            )));
        }
        crc = crc32c::crc32c_append(crc, &bytes);
        offset += count;
    }
    if crc != expected {
        return Err(MidgeError::Corruption(format!(
            "cloud object '{name}' crc32c {crc:08x} does not match published {expected:08x}"
        )));
    }
    Ok(())
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
    let reservation = coverage.budget.reserve(
        proof_bytes(&entry.object_key, &metadata),
        "WAL retirement target proof",
    )?;
    let fs = pinned_fs(
        coverage.storage,
        coverage.storage.remote_wal_backend(),
        entry.object_key.clone(),
        metadata.clone(),
        coverage.deadline,
    );
    validate_crc(
        fs.as_ref(),
        &entry.object_key,
        metadata.size,
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
    let mut covered = true;
    let prefix = visit_sealed_wal_records(&file, &path, limits, &mut |record| {
        // Continue validating all frames even when semantic coverage is absent.
        if covered {
            covered = coverage.covers_frame(record, limits.max_pending_txn_bytes)?;
        }
        Ok(())
    })?;
    if prefix.max_sequence != entry.max_sequence || prefix.writer_epoch != entry.writer_epoch {
        return Err(MidgeError::Corruption(format!(
            "cloud WAL segment {} contents differ from catalog sequence or epoch",
            entry.segment_id
        )));
    }
    Ok((
        RemoteObjectProof::from_validated_ranges(entry.object_key.clone(), metadata),
        covered,
        reservation,
    ))
}

impl Coverage<'_> {
    fn covers_frame(
        &mut self,
        record: &crate::wal::WalRecord,
        max_decoded_bytes: usize,
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
            for op in batch.records {
                if !self.covers_record(&DataCoverageRecord {
                    cf_id: op.cf_id,
                    op: op.op,
                    key: op.key.to_vec(),
                    value: op.value.map(|value| value.to_vec()),
                    expiration: op.expiration,
                    range_end: op.range_end.map(|key| key.to_vec()),
                    seq: op.seq,
                })? {
                    return Ok(false);
                }
            }
            Ok(true)
        } else if record.op.is_transaction_marker() {
            Ok(true)
        } else {
            self.covers_record(&DataCoverageRecord {
                cf_id: record.cf_id,
                op: record.op,
                key: record.key.to_vec(),
                value: record.value.as_ref().map(|value| value.to_vec()),
                expiration: record.expiration,
                range_end: record.range_end.as_ref().map(|key| key.to_vec()),
                seq: record.seq,
            })
        }
    }

    fn covers_record(&mut self, record: &DataCoverageRecord) -> MidgeResult<bool> {
        let mut state = ExactCoverageState::default();
        let mut retained_state = None;
        for (index, file) in self.manifest.files.iter().enumerate() {
            if file.key_bounds_complete && !file_may_contain_record_key(file, record) {
                continue;
            }
            let fs = self.verified_source(index, file)?;
            if !file_may_contain_record_key(file, record) {
                continue;
            }
            let reader = crate::sst::fs::SstFileIo::open_for_compaction(
                &file.name,
                fs,
                self.budget.clone(),
            )?;
            let mut end = record.key.clone();
            end.push(0);
            let cursor = Box::new(reader).raw_version_cursor_with_budget(
                Some(record.key.clone()),
                Some(end),
                Some(self.budget.clone()),
            )?;
            for version in cursor {
                let version = version?;
                if version.key != record.key {
                    continue;
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
                let should_retain = state
                    .state
                    .as_ref()
                    .and_then(exact_state_sequence)
                    .is_none_or(|seq| version.seq > seq);
                state.observe(raw);
                if should_retain {
                    retained_state = Some(held);
                }
            }
        }
        let covered = state.exactly_covers(record);
        drop(retained_state);
        Ok(covered)
    }

    fn verified_source(&mut self, index: usize, file: &FileMeta) -> MidgeResult<Arc<dyn Fs>> {
        let key = crate::sst::object_key(&file.name);
        let backend = self.storage.remote_sst_backend();
        if let Some(verified) = self.verified.get(&index) {
            return Ok(pinned_fs(
                self.storage,
                backend,
                key,
                verified.metadata.clone(),
                self.deadline,
            ));
        }
        let metadata = self
            .storage
            .remote_range_metadata_within(&key, self.deadline)?;
        if file.size_bytes != 0 && file.size_bytes != metadata.size {
            return Err(MidgeError::Corruption(format!(
                "cloud SST '{}' size differs from manifest",
                file.name
            )));
        }
        let reservation = self
            .budget
            .reserve(proof_bytes(&key, &metadata), "WAL retirement SST proof")?;
        let fs = pinned_fs(
            self.storage,
            Arc::clone(&backend),
            key.clone(),
            metadata.clone(),
            self.deadline,
        );
        if let Some(crc) = file.content_crc32c {
            validate_crc(fs.as_ref(), &file.name, metadata.size, crc, self.window)?;
        }
        let summary = crate::sst::fs::SstFileIo::summarize_with_fs_for_compaction(
            &file.name,
            Arc::clone(&fs),
            self.budget.clone(),
        )?;
        verify_sst_summary_matches_manifest(&file.name, &summary, file)
            .map_err(MidgeError::Corruption)?;
        self.verified.insert(
            index,
            VerifiedSst {
                dependency: GuardedObjectProof::range_identity(backend, key, metadata.clone()),
                metadata,
                reservation,
            },
        );
        Ok(fs)
    }
}
