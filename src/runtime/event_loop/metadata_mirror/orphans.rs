//! Pin-safe loop capture for a permanently unselectable-key inventory pass.

use super::{EventLoop, MirrorContinuation};
use crate::common::{MidgeError, MidgeResult, OperationDeadline};
use crate::runtime::hybrid_persistence::{
    sweep_sst_orphans_within, visit_intent_sources, InventoryCursor, OrphanSweepContext,
    OrphanSweepProgress, SstOrphanProtection,
};
use std::collections::HashSet;
use std::sync::Arc;

#[cfg(test)]
mod tests;

#[derive(Clone)]
pub(in crate::runtime::event_loop) struct OrphanSweepTask {
    protection: Arc<SstOrphanProtection>,
    cursor: InventoryCursor,
    wal_cursor: InventoryCursor,
    progress: Arc<parking_lot::Mutex<Option<OrphanSweepProgress>>>,
}

impl OrphanSweepTask {
    pub(super) fn take_progress(&self) -> Option<OrphanSweepProgress> {
        self.progress.lock().take()
    }

    pub(super) fn execute(
        &self,
        storage: &crate::storage::HybridStorage,
        cloud: &crate::storage::cloud::CloudStorage,
        lock: &crate::runtime::MetadataPublicationLock,
        fence: &super::super::fencing::RuntimeFence,
        deadline: &OperationDeadline,
    ) -> MidgeResult<()> {
        let context = OrphanSweepContext {
            storage,
            metadata: cloud,
            authority: crate::runtime::hybrid_persistence::CloudMetadataMirrorAuthority {
                store: fence
                    .leader_store
                    .as_deref()
                    .ok_or_else(|| MidgeError::Fenced("orphan sweep has no writer store".into()))?,
                holder_id: fence.leader_holder_id.as_deref().unwrap_or_default(),
                writer_epoch: fence.writer_epoch,
            },
            publication_lock: lock,
            deadline,
        };
        let mut result =
            sweep_sst_orphans_within(context, &self.protection, self.cursor.clone(), |deadline| {
                fence.validate_within(deadline)
            })?;
        let remaining = 8usize.saturating_sub(usize::try_from(result.probes).unwrap_or(usize::MAX));
        if remaining != 0 {
            match crate::runtime::hybrid_persistence::sweep_retired_wal_within(
                context,
                self.wal_cursor.clone(),
                remaining,
                |deadline| fence.validate_within(deadline),
            ) {
                Ok(wal) => {
                    result.pages += wal.pages;
                    result.probes += wal.probes;
                    result.deleted += wal.deleted;
                    result.retained += wal.retained;
                    result.unavailable += wal.unavailable;
                    result.wal_cursor = Some(wal.cursor);
                }
                Err(error @ MidgeError::Fenced(_)) => return Err(error),
                Err(error) => {
                    tracing::debug!(%error, "retired WAL discovery retained its uncertain cursor");
                }
            }
        }
        *self.progress.lock() = Some(result);
        Ok(())
    }
}

impl EventLoop {
    pub(super) fn periodic_retention_continuation(&self) -> MirrorContinuation {
        match self.capture_sst_orphan_task() {
            Ok(Some(task)) => MirrorContinuation::OrphanSweep(task),
            Err(error) => {
                tracing::debug!(%error, "orphan source capture deferred; metadata retention still runs");
                MirrorContinuation::Retention
            }
            Ok(None) => MirrorContinuation::Retention,
        }
    }

    fn capture_sst_orphan_task(&self) -> MidgeResult<Option<OrphanSweepTask>> {
        let Some(storage) = &self.actors.cloud_coordinator.hybrid_storage else {
            return Ok(None);
        };
        if !storage.has_object_inventory()
            || self.state.sst_names.startup_floor.is_empty()
            || self.state.sst_names.startup_floor_epoch != Some(self.authority.fencing.writer_epoch)
            || self.authority.fencing.writer_epoch == 0
            || self.publication.publication_gate.is_active()
        {
            return Ok(None);
        }
        self.check_lease_health()?;
        let Some(mut names) = self.state.try_get_pinned_sst_names() else {
            return Ok(None);
        };
        let job_names = self.actors.compaction_actor.protected_sst_names();
        let mut generations = self
            .actors
            .compaction_actor
            .protected_output_generations()
            .into_iter()
            .collect::<HashSet<_>>();
        let pipeline = &self.actors.cloud_coordinator.metadata_mirror;
        for (plan, _) in &pipeline.pending_compactions {
            generations.insert((plan.cf_id, plan.output_seq));
        }
        let budget = storage.maintenance_memory().ok_or_else(|| {
            MidgeError::ResourceLimit("orphan capture has no maintenance memory".into())
        })?;
        let mut bytes = 4096usize
            .saturating_add(self.state.sst_names.startup_floor.len().saturating_mul(128))
            .saturating_add(generations.len().saturating_mul(64));
        let mut charge = |name: &str| {
            bytes = bytes.saturating_add(name.len().saturating_add(128).saturating_mul(2));
        };
        for name in &names {
            charge(name);
        }
        for name in &job_names {
            charge(name);
        }
        self.visit_runtime_sst_sources(&mut charge)?;
        let memory = budget.reserve(bytes, "orphan sweep protected source capture")?;
        names.extend(job_names);
        self.visit_runtime_sst_sources(|name| {
            names.insert(name.to_owned());
        })?;
        let protection = SstOrphanProtection::new(
            self.state.writer_epoch,
            self.state.sst_names.startup_floor.clone(),
            names,
            generations,
            memory,
        );
        Ok(Some(OrphanSweepTask {
            protection: Arc::new(protection),
            cursor: pipeline.orphan_cursor.clone(),
            wal_cursor: pipeline.wal_orphan_cursor.clone(),
            progress: Arc::default(),
        }))
    }

    fn visit_runtime_sst_sources(&self, mut visit: impl FnMut(&str)) -> MidgeResult<()> {
        for file in &self.state.manifest.files {
            visit(&file.name);
        }
        for name in self.state.manifest.retired_ssts.keys() {
            visit(name);
        }
        for family in &self.state.manifest.column_families {
            for name in &family.dropped_sst_names {
                visit(name);
            }
        }
        for name in &self.state.compaction.compacting_ssts {
            visit(name);
        }
        for intent in &self.state.intent_log {
            visit_intent_sources(intent, &mut visit);
        }
        for family in self.state.column_families.values() {
            for flush in &family.immutable_flushes {
                if let Some(name) = &flush.sst_name {
                    visit(name);
                }
            }
        }
        let pipeline = &self.actors.cloud_coordinator.metadata_mirror;
        for (plan, _) in &pipeline.pending_compactions {
            for name in &plan.input_files {
                visit(name);
            }
        }
        if let Some(id) = pipeline.pending_flush {
            let (_, flush) = self.state.immutable_flush_by_id(id).ok_or_else(|| {
                MidgeError::Fenced("pending mirror flush lost its admitted source".into())
            })?;
            if let Some(name) = &flush.sst_name {
                visit(name);
            }
        }
        Ok(())
    }
}
