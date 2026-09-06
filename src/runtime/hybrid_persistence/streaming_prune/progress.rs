//! Reusable, process-local proof work. Provider identities are the invalidation boundary.

use super::{
    Arc, BTreeMap, CloudWalPruneGuard, ExactCoverageState, FileMeta, Fs, FsPath, HybridStorage,
    Manifest, MidgeError, MidgeResult, OpenMode, OpenOptions, PublishedWalSegment, ResourceBudget,
    ResourceReservation, StorageObjectMetadata, ValidatedWalPruneCandidate, WalPublicationCatalog,
};
use crate::sst::fs::reader_io::{SstCursorPosition, SstSummaryProgress};

/// A cooperative work target, separate from provider and shutdown deadlines.
/// Call only after acknowledging a resumable unit so setup cannot consume
/// every attempt without allowing any saved progress.
#[derive(Clone, Copy)]
pub(super) struct WorkQuantum(Option<crate::common::OperationDeadline>);

impl WorkQuantum {
    pub(super) fn new(duration: Option<std::time::Duration>) -> Self {
        Self(duration.map(crate::common::OperationDeadline::from_budget))
    }

    pub(super) fn checkpoint(self) -> MidgeResult<()> {
        if self.0.is_some_and(|deadline| deadline.is_expired()) {
            return Err(MidgeError::Busy(
                "cloud WAL proof yielded after acknowledged progress".into(),
            ));
        }
        Ok(())
    }
}

pub(in crate::runtime::hybrid_persistence) struct Progress {
    pub(super) budget: ResourceBudget,
    pub(super) manifest: Option<Arc<Manifest>>,
    manifest_memory: Option<Arc<ResourceReservation>>,
    metadata: Option<super::super::CloudMetadataPruneGuard>,
    pub(super) segment: Option<SegmentProgress>,
    pub(super) ssts: BTreeMap<String, SstProgress>,
}
impl Default for Progress {
    fn default() -> Self {
        Self {
            budget: ResourceBudget::new(0),
            manifest: None,
            manifest_memory: None,
            metadata: None,
            segment: None,
            ssts: BTreeMap::new(),
        }
    }
}

pub(super) struct SegmentProgress {
    pub(super) entry: PublishedWalSegment,
    pub(super) metadata: StorageObjectMetadata,
    pub(super) crc: CrcProgress,
    pub(super) prefix: crate::wal::recovery::VerifiedWalPrefix,
    pub(super) operation: usize,
    pub(super) record: RecordProgress,
    pub(super) _reservation: ResourceReservation,
}

#[derive(Default)]
pub(super) struct RecordProgress {
    pub(super) file_index: usize,
    pub(super) state: ExactCoverageState,
    pub(super) held_value: Option<ResourceReservation>,
    pub(super) cursor: SstCursorPosition,
}

pub(super) struct SstProgress {
    pub(super) expected: FileMeta,
    pub(super) metadata: StorageObjectMetadata,
    pub(super) crc: CrcProgress,
    pub(super) summary: SstSummaryProgress,
    pub(super) complete: bool,
    pub(super) last_used_segment: u64,
    // Shared with a completed batch until catalog CAS and delete scheduling end.
    pub(super) reservation: Arc<ResourceReservation>,
}

#[derive(Default)]
pub(super) struct CrcProgress {
    offset: u64,
    checksum: u32,
}
impl CrcProgress {
    pub(super) fn verify(
        &mut self,
        fs: &dyn Fs,
        name: &str,
        length: u64,
        expected: u32,
        window: usize,
        checkpoint: &mut dyn FnMut() -> MidgeResult<()>,
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
        while self.offset < length {
            let count = (length - self.offset).min(window as u64);
            let bytes = file.read_at(self.offset, count)?;
            if bytes.len() as u64 != count {
                return Err(MidgeError::Corruption(format!(
                    "cloud object '{name}' CRC range was truncated"
                )));
            }
            self.checksum = crc32c::crc32c_append(self.checksum, &bytes);
            self.offset += count;
            // A completed checksum is checked before yielding its final chunk.
            // Resuming at EOF then performs no new work and can move forward.
            if self.offset == length && self.checksum != expected {
                return Err(MidgeError::Corruption(format!(
                    "cloud object '{name}' crc32c {:08x} does not match published {expected:08x}",
                    self.checksum
                )));
            }
            checkpoint()?;
        }
        if self.checksum != expected {
            return Err(MidgeError::Corruption(format!(
                "cloud object '{name}' crc32c {:08x} does not match published {expected:08x}",
                self.checksum
            )));
        }
        Ok(())
    }
}

pub(in crate::runtime::hybrid_persistence) fn same_file(left: &FileMeta, right: &FileMeta) -> bool {
    left.name == right.name
        && left.cf_id == right.cf_id
        && left.size_bytes == right.size_bytes
        && left.content_crc32c == right.content_crc32c
        && left.smallest_key == right.smallest_key
        && left.largest_key == right.largest_key
        && left.smallest_seq == right.smallest_seq
        && left.largest_seq == right.largest_seq
        && left.key_bounds_complete == right.key_bounds_complete
}

impl Progress {
    pub(in crate::runtime::hybrid_persistence) fn retain_snapshot(
        &mut self,
        guard: &CloudWalPruneGuard,
    ) {
        self.manifest = Some(Arc::clone(&guard.manifest));
        self.manifest_memory.clone_from(&guard.manifest_memory);
        self.metadata.clone_from(&guard.metadata);
    }

    pub(in crate::runtime::hybrid_persistence) fn snapshot(
        &self,
        budget: &ResourceBudget,
    ) -> Option<CloudWalPruneGuard> {
        let memory = self.manifest_memory.as_ref()?;
        if !memory.belongs_to(budget) {
            return None;
        }
        Some(CloudWalPruneGuard {
            manifest: Arc::clone(self.manifest.as_ref()?),
            manifest_memory: Some(Arc::clone(memory)),
            metadata: self.metadata.clone(),
            ..CloudWalPruneGuard::default()
        })
    }

    pub(in crate::runtime::hybrid_persistence) fn retained_bytes(&self) -> usize {
        self.budget.used()
    }

    pub(super) fn prepare(
        &mut self,
        storage: &HybridStorage,
        guard: &CloudWalPruneGuard,
        catalog: &WalPublicationCatalog,
        deadline: &crate::common::OperationDeadline,
    ) -> MidgeResult<()> {
        if self.budget.limit()
            != storage
                .maintenance_memory()
                .map_or(guard.memory_limit(), |budget| budget.limit())
        {
            *self = Self {
                budget: storage
                    .maintenance_memory()
                    .unwrap_or_else(|| ResourceBudget::new(guard.memory_limit())),
                ..Self::default()
            };
        }
        // Only the authoritative oldest segment can resume. A prior batch may
        // have lost its CAS or been retired by another caller; do not let its
        // unrelated proof cache consume the next segment's workspace.
        if self.segment.as_ref().is_some_and(|segment| {
            catalog.segments.first_key_value().map(|(_, entry)| entry) != Some(&segment.entry)
        }) {
            self.segment = None;
            self.ssts.clear();
        }
        let manifest_changed = self.manifest.as_ref().is_none_or(|old| {
            old.files.len() != guard.manifest.files.len()
                || !old
                    .files
                    .iter()
                    .zip(&guard.manifest.files)
                    .all(|(left, right)| same_file(left, right))
        });
        if manifest_changed {
            if !self.appended_files_preserve_coverage(&guard.manifest) {
                self.reset_semantic_progress();
            }
            self.ssts.retain(|_, proof| {
                guard
                    .manifest
                    .files
                    .iter()
                    .any(|file| same_file(&proof.expected, file))
            });
            self.retain_snapshot(guard);
        }
        // A finished prefix may skip these SSTs entirely on this attempt. HEAD
        // checks must therefore precede reuse, not just subsequent row reads.
        let mut changed = Vec::new();
        for (name, proof) in &self.ssts {
            let actual =
                storage.remote_range_metadata_within(&crate::sst::object_key(name), deadline)?;
            if !actual.same_version(&proof.metadata) {
                changed.push(name.clone());
            }
        }
        if !changed.is_empty() {
            self.reset_semantic_progress();
            for name in changed {
                self.ssts.remove(&name);
            }
        }
        Ok(())
    }

    fn appended_files_preserve_coverage(&self, manifest: &Manifest) -> bool {
        let (Some(previous), Some(segment)) = (&self.manifest, &self.segment) else {
            return false;
        };
        if manifest.files.len() < previous.files.len()
            || !previous
                .files
                .iter()
                .zip(&manifest.files)
                .all(|(left, right)| same_file(left, right))
        {
            return false;
        }
        manifest.files[previous.files.len()..].iter().all(|added| {
            added.key_bounds_complete
                && added.content_crc32c.is_some()
                && added.smallest_key.is_some()
                && added.largest_key.is_some()
                && added
                    .smallest_seq
                    .is_some_and(|sequence| sequence > segment.entry.max_sequence)
                && self.ssts.values().all(|proof| {
                    if added.cf_id != proof.expected.cf_id {
                        return true;
                    }
                    let (Some(minimum), Some(maximum), Some(summary)) = (
                        &added.smallest_key,
                        &added.largest_key,
                        &proof.summary.summary,
                    ) else {
                        return false;
                    };
                    // A later equal-sequence conflicting value could invalidate
                    // even newer coverage of an old WAL row. Only proven
                    // disjoint additions preserve skipped frame semantics.
                    proof.complete
                        && (maximum < &summary.smallest_key || minimum > &summary.largest_key)
                })
        })
    }

    pub(in crate::runtime::hybrid_persistence) fn discard_proofs(&mut self) {
        self.segment = None;
        self.ssts.clear();
        self.manifest = None;
        self.manifest_memory = None;
        self.metadata = None;
    }

    pub(in crate::runtime::hybrid_persistence) fn after_retirement(
        &mut self,
        retired: &[ValidatedWalPruneCandidate],
    ) {
        if retired.is_empty() {
            return;
        }
        if self.segment.as_ref().is_some_and(|segment| {
            retired
                .iter()
                .any(|candidate| candidate.entry == segment.entry)
        }) {
            self.segment = None;
        }
        // All batch dependencies remain available until the catalog CAS has
        // succeeded. Then only an unfinished segment can still need them.
        self.ssts.retain(|_, proof| {
            self.segment
                .as_ref()
                .is_some_and(|segment| proof.last_used_segment == segment.entry.segment_id)
        });
        if self.segment.is_none() {
            self.manifest = None;
            self.manifest_memory = None;
            self.metadata = None;
        }
    }

    fn reset_semantic_progress(&mut self) {
        if let Some(segment) = &mut self.segment {
            segment.prefix = crate::wal::recovery::VerifiedWalPrefix::default();
            segment.operation = 0;
            segment.record = RecordProgress::default();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn old_coverage() -> (Progress, FileMeta) {
        let budget = ResourceBudget::new(64 * 1024);
        let file = FileMeta {
            name: "old.sst".into(),
            smallest_key: Some(b"k".to_vec()),
            largest_key: Some(b"k".to_vec()),
            smallest_seq: Some(10),
            largest_seq: Some(10),
            content_crc32c: Some(1),
            key_bounds_complete: true,
            ..FileMeta::default()
        };
        let mut summary = SstSummaryProgress::default();
        summary.summary = Some(crate::sst::fs::SstFileSummary {
            size_bytes: 1,
            smallest_key: b"k".to_vec(),
            largest_key: b"k".to_vec(),
            smallest_seq: 10,
            largest_seq: 10,
        });
        let proof = SstProgress {
            expected: file.clone(),
            metadata: StorageObjectMetadata::content_crc(4, b"test"),
            crc: CrcProgress::default(),
            summary,
            complete: true,
            last_used_segment: 1,
            reservation: Arc::new(budget.reserve(1024, "test SST proof").expect("proof")),
        };
        let progress = Progress {
            segment: Some(SegmentProgress {
                entry: PublishedWalSegment::from_validated_bytes(1, 1, 1, b"test"),
                metadata: StorageObjectMetadata::content_crc(4, b"test"),
                crc: CrcProgress::default(),
                prefix: crate::wal::recovery::VerifiedWalPrefix::default(),
                operation: 0,
                record: RecordProgress::default(),
                _reservation: budget.reserve(1024, "test WAL proof").expect("proof"),
            }),
            manifest_memory: None,
            metadata: None,
            manifest: Some(Arc::new(Manifest {
                files: vec![file.clone()],
                ..Manifest::default()
            })),
            ssts: BTreeMap::from([(file.name.clone(), proof)]),
            budget,
        };
        (progress, file)
    }

    #[test]
    fn should_reuse_manifest_admission_when_cleanup_resumes_without_new_acknowledgements() {
        // Arrange
        let budget = ResourceBudget::new(8192);
        let manifest = Manifest::default();
        let progress = crate::runtime::hybrid_persistence::CloudWalPruneProgress::default();
        let guard =
            CloudWalPruneGuard::admitted_local_snapshot(&manifest, &budget, &progress).unwrap();
        assert!(budget.used() > budget.limit() / 2);
        {
            let mut retained = progress.0.lock();
            retained.manifest = Some(Arc::clone(&guard.manifest));
            retained.manifest_memory.clone_from(&guard.manifest_memory);
        }
        drop(guard);
        let retained_bytes = budget.used();

        // Act
        let resumed = CloudWalPruneGuard::admitted_local_snapshot(&manifest, &budget, &progress);

        // Assert
        assert!(resumed.is_ok(), "a retained snapshot must fit on retry");
        assert_eq!(budget.used(), retained_bytes);
    }

    #[test]
    fn should_release_stale_snapshot_before_admitting_changed_manifest() {
        // Arrange
        let budget = ResourceBudget::new(12 * 1024);
        let progress = crate::runtime::hybrid_persistence::CloudWalPruneProgress::default();
        let mut manifest = Manifest::default();
        let first =
            CloudWalPruneGuard::admitted_local_snapshot(&manifest, &budget, &progress).unwrap();
        progress.0.lock().retain_snapshot(&first);
        drop(first);
        manifest.files.push(FileMeta {
            name: "replacement.sst".into(),
            ..FileMeta::default()
        });

        // Act
        let replacement =
            CloudWalPruneGuard::admitted_local_snapshot(&manifest, &budget, &progress).unwrap();

        // Assert
        assert_eq!(replacement.manifest.files[0].name, "replacement.sst");
        assert!(progress.0.lock().manifest.is_none());
        drop(replacement);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn should_not_reuse_snapshot_admission_from_another_pool() {
        // Arrange
        let budget = ResourceBudget::new(8192);
        let replacement_budget = ResourceBudget::new(8192);
        let progress = crate::runtime::hybrid_persistence::CloudWalPruneProgress::default();
        let manifest = Manifest::default();
        let first =
            CloudWalPruneGuard::admitted_local_snapshot(&manifest, &budget, &progress).unwrap();
        progress.0.lock().retain_snapshot(&first);
        drop(first);

        // Act
        let replacement =
            CloudWalPruneGuard::admitted_local_snapshot(&manifest, &replacement_budget, &progress)
                .unwrap();

        // Assert
        assert_eq!(budget.used(), 0);
        assert!(replacement_budget.used() > 0);
        drop(replacement);
        assert_eq!(replacement_budget.used(), 0);
    }

    #[test]
    fn should_release_retained_manifest_charge_when_last_wal_proof_is_retired() {
        // Arrange
        let (mut progress, _) = old_coverage();
        progress.manifest_memory = Some(Arc::new(
            progress
                .budget
                .reserve(1024, "test retained manifest")
                .unwrap(),
        ));
        let entry = progress.segment.as_ref().unwrap().entry.clone();
        let candidate = ValidatedWalPruneCandidate {
            segment_id: entry.segment_id,
            validated: super::super::ValidatedWalObject {
                proof: super::super::RemoteObjectProof::from_validated_ranges(
                    entry.object_key.clone(),
                    StorageObjectMetadata::content_crc(4, b"test"),
                ),
                data_records: Vec::new(),
            },
            entry,
        };

        // Act
        progress.after_retirement(&[candidate]);

        // Assert
        assert!(progress.manifest.is_none());
        assert_eq!(progress.budget.used(), 0);
    }

    #[test]
    fn should_invalidate_old_wal_coverage_when_appended_newer_states_can_conflict() {
        // Arrange
        let (progress, file) = old_coverage();
        let mut manifest = Manifest {
            files: vec![
                file.clone(),
                FileMeta {
                    name: "new.sst".into(),
                    ..file
                },
            ],
            ..Manifest::default()
        };
        // Act
        let overlapping = progress.appended_files_preserve_coverage(&manifest);
        manifest.files[1].smallest_key = Some(b"z".to_vec());
        manifest.files[1].largest_key = Some(b"z".to_vec());
        let disjoint = progress.appended_files_preserve_coverage(&manifest);
        manifest.files[1].key_bounds_complete = false;
        let unknown = progress.appended_files_preserve_coverage(&manifest);
        // Assert
        assert!(
            !overlapping,
            "seq10 can conflict with old seq10 coverage of WAL seq1"
        );
        assert!(disjoint);
        assert!(!unknown);
    }
}
