//! Retiring WAL segments whose records the manifest's SSTs already hold.
//!
//! Local retention lives here; cloud retention, with its resumable streaming
//! prover, lives in [`cloud`].

use super::EventLoop;

mod cloud;

/// What one local WAL prune pass did.
#[derive(Debug, Default)]
struct LocalWalPruneOutcome {
    removed: Vec<u64>,
    anomaly: bool,
    lease_lost: bool,
}

/// WAL bytes one local prune pass may read for coverage proofs before it
/// defers the remaining segments to later passes. A pass always reads at
/// least one segment, so proofs keep making progress (#548).
const LOCAL_WAL_PROOF_BYTES_PER_PASS: u64 = 16 * 1024 * 1024;

fn local_wal_proof_byte_budget() -> u64 {
    #[cfg(test)]
    if let Some(budget) = LOCAL_WAL_PROOF_BYTE_BUDGET.with(std::cell::Cell::get) {
        return budget;
    }
    LOCAL_WAL_PROOF_BYTES_PER_PASS
}

#[cfg(test)]
thread_local! {
    /// Whole-file reads of local WAL segments, for tests that bound a prune pass.
    pub(super) static RUNTIME_FILE_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    /// Replaces the local WAL proof byte budget for one test thread.
    pub(super) static LOCAL_WAL_PROOF_BYTE_BUDGET: std::cell::Cell<Option<u64>> =
        const { std::cell::Cell::new(None) };
}

impl EventLoop {
    pub(super) fn prune_local_wal_segments_covered_by_manifest(&mut self) {
        if self.state.is_memory_mode() || self.wal_actor.is_cloud_async() {
            return;
        }
        if let Err(error) = self.validate_runtime_lease_for_wal_prune() {
            tracing::warn!(%error, "retaining local WAL because the writer lease is no longer valid");
            return;
        }
        if !self.seal_active_wal_for_prune() {
            return;
        }

        let wal_dir = crate::io::FsPath::new("wal");
        let mut sealed_segments = match self.state.fs.list_dir(&wal_dir) {
            Ok(entries) => entries
                .into_iter()
                .filter(|entry| !entry.is_dir)
                .filter_map(|entry| {
                    crate::wal::parse_segment_id(&entry.name).map(|segment_id| {
                        (
                            segment_id,
                            crate::io::FsPath::new(format!("wal/{}", entry.name)),
                        )
                    })
                })
                .filter(|(segment_id, _)| *segment_id < self.state.wal.current_segment_id)
                .collect::<Vec<_>>(),
            Err(error) => {
                self.state.mark_persistence_anomaly();
                tracing::warn!(%error, "retaining local WAL because sealed segments could not be listed");
                return;
            }
        };
        sealed_segments.sort_by_key(|(segment_id, _)| *segment_id);
        // Memtables flush in FIFO order per column family, so a segment below
        // the oldest unflushed memtable's first segment holds only data that
        // is already in published SSTs, including deletes, which the
        // per-record proof below cannot certify.
        let flushed_floor = self.state.wal_recovery_floor_segment();
        let mut proofs = std::mem::take(&mut self.state.wal.local_segment_proofs);
        let mut proven = std::mem::take(&mut self.state.wal.proven_coverage_ssts);
        let mut cursor = self.state.wal.local_prune_cursor;
        let outcome = self.retire_sealed_local_wal(
            &sealed_segments,
            flushed_floor,
            &mut proofs,
            &mut proven,
            &mut cursor,
        );
        self.state.wal.proven_coverage_ssts = proven;
        self.state.wal.local_prune_cursor = cursor;
        // Forget proofs for segments that are gone.
        let listed: std::collections::HashSet<u64> = sealed_segments
            .iter()
            .map(|(segment_id, _)| *segment_id)
            .collect();
        let removed: std::collections::HashSet<u64> = outcome.removed.iter().copied().collect();
        proofs.retain(|segment_id, _| listed.contains(segment_id) && !removed.contains(segment_id));
        self.state.wal.local_segment_proofs = proofs;
        if outcome.anomaly {
            self.state.mark_persistence_anomaly();
        }
        if outcome.lease_lost {
            return;
        }

        if let Err(error) = self.state.fs.sync_dir(
            &crate::io::FsPath::new("wal"),
            crate::io::Durability::Durable,
        ) {
            self.state.mark_persistence_anomaly();
            tracing::warn!(%error, "failed to sync local WAL directory after pruning");
        }
    }

    /// Retires sealed local WAL segments. A segment below the recovery floor
    /// goes whole; one at or above it goes only when every record is exactly
    /// in the manifest's SSTs. Removal order does not matter: a proof never
    /// removes a segment holding a delete, so no retained value loses the
    /// tombstone that hides it.
    ///
    /// Every segment is considered, not just a prefix: an idle column family
    /// can keep the first one uncovered forever while later ones are covered.
    /// A failed proof is remembered in `proofs` and not repeated until the
    /// blocking family's SSTs change, so a pass reads only segments whose
    /// answer could differ (#490). Proof reads stop once a pass has read its
    /// byte budget; the next pass resumes after `cursor`, the last segment
    /// read (#548). Deferring a proof only retains a segment longer.
    fn retire_sealed_local_wal(
        &self,
        sealed_segments: &[(u64, crate::io::FsPath)],
        flushed_floor: Option<u64>,
        proofs: &mut std::collections::HashMap<
            u64,
            crate::runtime::hybrid_persistence::FailedWalProof,
        >,
        proven: &mut crate::runtime::hybrid_persistence::ProvenSstIdentities,
        cursor: &mut u64,
    ) -> LocalWalPruneOutcome {
        use crate::runtime::hybrid_persistence::{
            coverage_fingerprint, FailedWalProof, VerifiedManifestWalCoverage,
        };
        let mut outcome = LocalWalPruneOutcome::default();
        // One prover per pass, built only if a proof is needed, so each
        // covering SST is opened once per pass and read in full only the
        // first time this runtime proves its identity.
        let mut coverage = None;
        let mut identity_cache = Some(proven);
        // Each family's fingerprint once per pass, however many segments ask.
        let mut fingerprints = std::collections::HashMap::new();
        let mut fingerprint_of = |cf_id: u32| {
            *fingerprints
                .entry(cf_id)
                .or_insert_with(|| coverage_fingerprint(&self.state.manifest, cf_id))
        };
        let budget = local_wal_proof_byte_budget();
        let mut bytes_read = 0_u64;
        let mut segments_read = 0_usize;
        let resume_at = sealed_segments.partition_point(|(segment_id, _)| *segment_id <= *cursor);
        let (before, after) = sealed_segments.split_at(resume_at);
        for (segment_id, path) in after.iter().chain(before) {
            let segment_id = *segment_id;
            if outcome.lease_lost {
                return outcome;
            }
            if flushed_floor.is_some_and(|floor| segment_id < floor) {
                self.remove_local_wal_segment(segment_id, path, "flushed", &mut outcome);
                continue;
            }
            if proofs
                .get(&segment_id)
                .is_some_and(|proof| proof.still_fails(&mut fingerprint_of))
            {
                continue;
            }
            if segments_read > 0 && bytes_read >= budget {
                continue;
            }
            segments_read += 1;
            *cursor = segment_id;
            let bytes = match self.read_runtime_file(path) {
                Ok(bytes) => bytes,
                Err(error) => {
                    outcome.anomaly = true;
                    tracing::warn!(segment_id, %error, "retaining unreadable local WAL segment");
                    continue;
                }
            };
            bytes_read = bytes_read.saturating_add(bytes.len() as u64);
            if bytes.is_empty() {
                self.remove_local_wal_segment(segment_id, path, "empty", &mut outcome);
                continue;
            }
            let readback = match crate::wal::cloud_segment::inspect_local_bytes(&path.0, &bytes) {
                Ok(readback) => readback,
                Err(error) => {
                    outcome.anomaly = true;
                    tracing::warn!(segment_id, %error, "retaining invalid local WAL segment");
                    continue;
                }
            };
            let verifier = match (&mut coverage, identity_cache.take()) {
                (Some(built), _) => &*built,
                (slot @ None, Some(cache)) => slot.insert(VerifiedManifestWalCoverage::open(
                    std::sync::Arc::clone(&self.state.fs),
                    crate::cloud_layout::CloudObjectLayout::SST_PREFIX,
                    &self.state.manifest,
                    cache,
                )),
                (None, None) => unreachable!("the prover is built exactly once"),
            };
            match verifier.first_uncovered(&readback.data_records) {
                None => {
                    self.remove_local_wal_segment(
                        segment_id,
                        path,
                        "exactly covered",
                        &mut outcome,
                    );
                }
                Some(reason) => match FailedWalProof::remember(reason, &mut fingerprint_of) {
                    Some(proof) => {
                        proofs.insert(segment_id, proof);
                    }
                    // Unverifiable: prove it again next pass.
                    None => {
                        proofs.remove(&segment_id);
                    }
                },
            }
        }
        outcome
    }

    /// Removes one sealed segment. Every removal is preceded by a fresh
    /// writer-lease check, so a fenced writer stops deleting at once. Reads
    /// and skipped segments mutate nothing and need no check.
    fn remove_local_wal_segment(
        &self,
        segment_id: u64,
        path: &crate::io::FsPath,
        why: &'static str,
        outcome: &mut LocalWalPruneOutcome,
    ) {
        if let Err(error) = self.validate_runtime_lease_for_wal_prune() {
            tracing::warn!(segment_id, %error, "stopped local WAL pruning after lease validation failed");
            outcome.lease_lost = true;
            return;
        }
        match self.state.fs.remove_file(path) {
            Ok(()) => {
                tracing::debug!(segment_id, why, "removed local WAL segment");
                outcome.removed.push(segment_id);
            }
            Err(crate::io::FsError::NotFound(_)) => outcome.removed.push(segment_id),
            Err(error) => {
                outcome.anomaly = true;
                tracing::warn!(segment_id, why, %error, "failed to remove local WAL segment");
            }
        }
    }

    pub(super) fn read_runtime_file(
        &self,
        path: &crate::io::FsPath,
    ) -> crate::io::FsResult<bytes::Bytes> {
        #[cfg(test)]
        RUNTIME_FILE_READS.with(|reads| reads.set(reads.get() + 1));
        let length = self.state.fs.metadata(path)?.len;
        let file = self.state.fs.open(
            path,
            crate::io::OpenOptions {
                mode: crate::io::OpenMode::ReadOnly,
                create: false,
                create_new: false,
                truncate: false,
            },
        )?;
        file.read_at(0, length)
    }

    /// Seals the active WAL segment so it can be retired later. In local
    /// mode this is the only place segments are sealed, so it must happen
    /// even while some memtable is unflushed, or `wal.log` would grow without
    /// bound. An empty active segment has nothing to seal (#490): rotating it
    /// would only create an empty sealed file for the loop below to delete.
    ///
    /// The on-disk length is a sound emptiness test because an append returns
    /// only after the writer thread has written its bytes, and appends run on
    /// this thread, so none is in flight here. A failed stat seals as before.
    /// Returns false when pruning must stop.
    pub(super) fn seal_active_wal_for_prune(&mut self) -> bool {
        let active = crate::io::FsPath::new(format!("wal/{}", crate::wal::ACTIVE_FILE_NAME));
        if self
            .state
            .fs
            .metadata(&active)
            .is_ok_and(|metadata| metadata.len == 0)
        {
            return true;
        }
        if let Err(error) = self.sync_local_wal_before_prune_rotation() {
            self.state.mark_persistence_anomaly();
            tracing::warn!(%error, "retaining local WAL because buffered records could not be synced");
            return false;
        }
        if let Err(error) = self.rotate_local_wal_transition() {
            self.state.mark_persistence_anomaly();
            tracing::warn!(%error, "retaining local WAL because the active segment could not be sealed");
            return false;
        }
        true
    }

    pub(super) fn validate_runtime_lease_for_wal_prune(&self) -> crate::common::MidgeResult<()> {
        self.check_lease_health()?;
        if let Some(store) = &self.fencing.leader_store {
            store
                .validate_epoch(
                    self.fencing.leader_holder_id.as_deref().unwrap_or_default(),
                    self.fencing.writer_epoch,
                )
                .map_err(|error| error.into_validation_error("local WAL prune"))?;
        }
        Ok(())
    }
}
