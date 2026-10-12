//! Retiring WAL segments whose records the manifest's SSTs already hold.
//!
//! Local retention lives here; cloud retention, with its resumable streaming
//! prover, lives in [`cloud`].

use super::EventLoop;

mod cloud;

/// What one local WAL prune pass found.
#[derive(Debug, Default)]
struct LocalWalPruneOutcome {
    /// One past the leading run of retirable sealed segments.
    retire_below: Option<u64>,
    anomaly: bool,
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
        let sealed: Vec<u64> = self.state.wal.history.ledger().sealed().collect();
        // Memtables flush in FIFO order per column family, so a segment below
        // the oldest unflushed memtable's first segment holds only data that
        // is already in published SSTs, including deletes, which the
        // per-record proof below cannot certify.
        let flushed_floor = self.state.wal_recovery_floor_segment();
        let mut proofs = std::mem::take(&mut self.state.wal.local_segment_proofs);
        let mut proven = std::mem::take(&mut self.state.wal.proven_coverage_ssts);
        let outcome =
            self.retirable_local_wal_prefix(&sealed, flushed_floor, &mut proofs, &mut proven);
        self.state.wal.proven_coverage_ssts = proven;
        if outcome.anomaly {
            self.state.mark_persistence_anomaly();
        }
        if let Some(to) = outcome.retire_below {
            proofs.retain(|segment_id, _| *segment_id >= to);
            self.retire_local_wal_below(to);
        }
        self.state.wal.local_segment_proofs = proofs;
    }

    /// Finds the leading run of sealed segments that may retire: each is
    /// below the recovery floor, empty, or has every record exactly in the
    /// manifest's SSTs. Only a prefix retires (#842), so the first segment
    /// that may not stops the run, and an idle column family's segment holds
    /// every later one until a flush covers it.
    ///
    /// A failed proof is remembered in `proofs` and not repeated until the
    /// blocking family's SSTs change, so a pass reads only segments whose
    /// answer could differ (#490). Proof reads stop once a pass has read its
    /// byte budget, which only retains segments longer (#548).
    fn retirable_local_wal_prefix(
        &self,
        sealed: &[u64],
        flushed_floor: Option<u64>,
        proofs: &mut std::collections::HashMap<
            u64,
            crate::runtime::hybrid_persistence::FailedWalProof,
        >,
        proven: &mut crate::runtime::hybrid_persistence::ProvenSstIdentities,
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
        for &segment_id in sealed {
            if flushed_floor.is_some_and(|floor| segment_id < floor) {
                outcome.retire_below = Some(segment_id + 1);
                continue;
            }
            let still_fails = proofs
                .get(&segment_id)
                .is_some_and(|proof| proof.still_fails(&mut fingerprint_of));
            if still_fails || (segments_read > 0 && bytes_read >= budget) {
                break;
            }
            segments_read += 1;
            let path = crate::wal::segment_key(segment_id);
            let bytes = match self.read_runtime_file(&path) {
                Ok(bytes) => bytes,
                Err(error) => {
                    outcome.anomaly = true;
                    tracing::warn!(segment_id, %error, "retaining unreadable local WAL segment");
                    break;
                }
            };
            bytes_read = bytes_read.saturating_add(bytes.len() as u64);
            if !bytes.is_empty() {
                let readback = match crate::wal::cloud_segment::inspect_local_bytes(&path.0, &bytes)
                {
                    Ok(readback) => readback,
                    Err(error) => {
                        outcome.anomaly = true;
                        tracing::warn!(segment_id, %error, "retaining invalid local WAL segment");
                        break;
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
                if let Some(reason) = verifier.first_uncovered(&readback.data_records) {
                    match FailedWalProof::remember(reason, &mut fingerprint_of) {
                        Some(proof) => {
                            proofs.insert(segment_id, proof);
                        }
                        // Unverifiable: prove it again next pass.
                        None => {
                            proofs.remove(&segment_id);
                        }
                    }
                    break;
                }
            }
            outcome.retire_below = Some(segment_id + 1);
        }
        outcome
    }

    /// Retires every sealed segment below `to` through the ledger, after a
    /// fresh writer-lease check, so a fenced writer stops retiring at once.
    fn retire_local_wal_below(&mut self, to: u64) {
        if let Err(error) = self.validate_runtime_lease_for_wal_prune() {
            tracing::warn!(%error, "stopped local WAL pruning after lease validation failed");
            return;
        }
        self.retire_wal_below_or_retain(to);
    }

    /// Retire every WAL segment below `to`, or retain the WAL and record the
    /// anomaly when the ledger cannot. A file left behind is an anomaly too:
    /// it holds disk space until the next open sweeps it.
    pub(super) fn retire_wal_below_or_retain(
        &mut self,
        to: u64,
    ) -> Option<crate::runtime::wal_history::RetiredWal> {
        match self.wal_actor.retire_wal_below(&mut self.state, to) {
            Ok(retired) => {
                if !retired.left.is_empty() {
                    self.state.mark_persistence_anomaly();
                }
                tracing::debug!(?retired, "retired WAL segments");
                Some(retired)
            }
            Err(error) => {
                self.state.mark_persistence_anomaly();
                tracing::warn!(%error, to, "retaining WAL because the ledger could not retire it");
                None
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
    /// even while some memtable is unflushed, or the open segment would grow
    /// without bound. An empty active segment has nothing to seal (#490): rotating it
    /// would only create an empty sealed file for the loop below to delete.
    ///
    /// The on-disk length is a sound emptiness test because an append returns
    /// only after the writer thread has written its bytes, and appends run on
    /// this thread, so none is in flight here. A failed stat seals as before.
    /// Returns false when pruning must stop.
    pub(super) fn seal_active_wal_for_prune(&mut self) -> bool {
        let active = crate::wal::segment_key(self.state.wal.open_segment());
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
