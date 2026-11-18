//! Bounded independent job owners sharing admission and compute workers.
use super::{jobs, pool, CompactionJobActor, CompactionStorage};
use crate::common::resource_budget::ResourceBudget;
use crate::common::{MidgeError, MidgeResult, OperationDeadline};
use crate::compaction::{CompactionPlan, LeveledCompactionConfig};
use crate::runtime::{RuntimeMsg, RuntimeState};
use crate::sst::SstFactory;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::Ordering;
use std::sync::Arc;

// Charge cursor state as well as its owned key/name bytes to the compaction
// pool, with an additional allowance for map and allocation overhead.
const CURSOR_ENTRY_OVERHEAD: usize = 128;

fn cursor_charge(state: &RuntimeState, plan: &CompactionPlan) -> MidgeResult<Option<usize>> {
    Ok(
        if plan.source_level > 0
            && matches!(
                plan.purpose,
                crate::compaction::strategy::CompactionPurpose::Merge
            )
        {
            let file = plan
                .source_files
                .last()
                .and_then(|name| state.manifest.files.iter().find(|file| file.name == *name))
                .ok_or_else(|| {
                    MidgeError::Corruption("compaction cursor source is missing".into())
                })?;
            let key = file.smallest_key.as_ref().ok_or_else(|| {
                MidgeError::Corruption("compaction cursor source has no lower bound".into())
            })?;
            Some(
                key.len()
                    .checked_add(file.name.len())
                    .and_then(|size| {
                        size.checked_add(
                            std::mem::size_of::<crate::compaction::strategy::SelectionCursor>()
                                + CURSOR_ENTRY_OVERHEAD,
                        )
                    })
                    .ok_or_else(|| {
                        MidgeError::ResourceLimit("compaction cursor size overflow".into())
                    })?,
            )
        } else {
            None
        },
    )
}

/// Each selected operation owns all its mutable compute/publication state.
/// Shared admission prevents conflicting jobs from occupying different slots.
pub struct CompactionActor {
    slots: Vec<CompactionJobActor>,
    selected: usize,
    jobs: Arc<jobs::CompactionJobs>,
    pool: Option<Arc<pool::CompactionPool>>,
    memory: ResourceBudget,
    global_memory_limit: usize,
    target_sst_size: usize,
    last_scheduled_cf: Option<u32>,
    cursors: crate::compaction::strategy::CompactionCursors,
    hybrid_memory_attached: bool,
}

impl Deref for CompactionActor {
    type Target = CompactionJobActor;
    fn deref(&self) -> &Self::Target {
        &self.slots[self.selected]
    }
}
impl DerefMut for CompactionActor {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.slots[self.selected]
    }
}

impl CompactionActor {
    #[cfg(test)]
    pub fn new(factory: Arc<dyn SstFactory>) -> Self {
        Self::new_with_config(factory, LeveledCompactionConfig::default())
    }

    pub fn new_with_config(factory: Arc<dyn SstFactory>, config: LeveledCompactionConfig) -> Self {
        let core = CompactionJobActor::new_with_config(factory, config);
        let jobs = Arc::clone(&core.jobs);
        Self {
            global_memory_limit: core.compaction_memory_limit,
            target_sst_size: core.target_sst_size,
            memory: ResourceBudget::new(core.compaction_memory_limit),
            slots: vec![core],
            selected: 0,
            jobs,
            pool: None,
            last_scheduled_cf: None,
            cursors: crate::compaction::strategy::CompactionCursors::new(),
            hybrid_memory_attached: false,
        }
    }

    pub(crate) fn active_count(&self) -> usize {
        self.slots
            .iter()
            .filter(|slot| slot.compaction_running)
            .count()
    }

    pub(crate) fn has_capacity(&self) -> bool {
        self.active_count() < self.slots.len()
    }
    pub(crate) fn can_admit_plan(
        &self,
        plan: &CompactionPlan,
        files: &[crate::metadata::FileMeta],
    ) -> bool {
        self.has_capacity() && self.jobs.available(plan, files)
    }
    pub(crate) fn concurrency_limit(&self) -> usize {
        self.slots.len()
    }

    pub(crate) fn configure_concurrency(&mut self, limit: usize) -> MidgeResult<()> {
        if !(1..=64).contains(&limit) {
            return Err(MidgeError::InvalidArgument(
                "compaction concurrency must be in 1..=64".into(),
            ));
        }
        if self.active_count() != 0 {
            return Err(MidgeError::Busy(
                "cannot resize active compaction owners".into(),
            ));
        }
        let factory = Arc::clone(&self.slots[0].sst_factory);
        let config = self.slots[0].compactor.config.clone();
        // Every owner reports rolls to the one engine counter; a rebuilt slot
        // must not fall back to a private one.
        let rolls = Arc::clone(&self.slots[0].grandparent_overlap_rolls);
        self.pool = None;
        self.jobs = jobs::CompactionJobs::new(limit);
        self.slots = (0..limit)
            .map(|_| {
                let worker_factory = factory
                    .fork_for_worker()
                    .unwrap_or_else(|| Arc::clone(&factory));
                let mut core = CompactionJobActor::new_with_config(worker_factory, config.clone());
                core.jobs = Arc::clone(&self.jobs);
                core.set_grandparent_overlap_roll_counter(Arc::clone(&rolls));
                core.set_execution_limits(self.target_sst_size, self.global_memory_limit / limit);
                core
            })
            .collect();
        self.selected = 0;
        Ok(())
    }

    pub(crate) fn set_execution_limits(&mut self, target: usize, memory: usize) {
        if self.active_count() != 0 {
            tracing::warn!("active compaction owners retain their original memory limits");
            return;
        }
        self.target_sst_size = target.max(1);
        self.global_memory_limit = memory;
        self.cursors = crate::compaction::strategy::CompactionCursors::new();
        self.memory = ResourceBudget::new(memory);
        self.hybrid_memory_attached = false;
        let quota = memory / self.slots.len();
        for slot in &mut self.slots {
            slot.set_execution_limits(target, quota);
        }
    }

    pub(crate) fn compaction_memory_limit(&self) -> usize {
        self.global_memory_limit / self.slots.len()
    }
    pub(crate) fn total_memory_limit(&self) -> usize {
        self.global_memory_limit
    }
    #[cfg(all(test, feature = "failpoints"))]
    pub(in crate::runtime) fn shared_memory_usage(&self) -> (usize, usize) {
        (self.memory.used(), self.memory.peak())
    }
    #[cfg(all(test, feature = "failpoints"))]
    pub(in crate::runtime) fn cursor_memory_usage(&self) -> usize {
        self.cursors
            .values()
            .map(|cursor| cursor.reservation.bytes())
            .sum()
    }
    #[cfg(all(test, feature = "failpoints"))]
    pub(in crate::runtime) fn shared_memory_budget(&self) -> ResourceBudget {
        self.memory.clone()
    }
    pub(crate) fn target_sst_size(&self) -> usize {
        self.target_sst_size
    }
    pub(crate) fn memory_limit_for_plan(&self, plan: &CompactionPlan) -> usize {
        if matches!(
            plan.purpose,
            crate::compaction::strategy::CompactionPurpose::Merge
        ) {
            self.compaction_memory_limit()
        } else {
            self.global_memory_limit
        }
    }

    pub(crate) fn configure_work_target(&mut self, storage: Option<&dyn CompactionStorage>) {
        for slot in &mut self.slots {
            slot.configure_work_target(storage);
        }
    }

    pub fn check_compaction(
        &mut self,
        state: &RuntimeState,
    ) -> MidgeResult<Option<CompactionPlan>> {
        self.pick(state, true)
    }
    pub fn check_manual_compaction(
        &mut self,
        state: &RuntimeState,
    ) -> MidgeResult<Option<CompactionPlan>> {
        self.pick(state, false)
    }
    fn pick(
        &mut self,
        state: &RuntimeState,
        background: bool,
    ) -> MidgeResult<Option<CompactionPlan>> {
        let cursor_count = self.cursors.len();
        self.cursors
            .retain(|(cf_id, _), _| state.get_cf(*cf_id).is_some());
        if self.cursors.len() != cursor_count {
            self.cursors.shrink_to_fit();
        }
        let Some(index) = self.slots.iter().position(|slot| !slot.compaction_running) else {
            return Ok(None);
        };
        self.selected = index;
        let slot = &mut self.slots[index];
        slot.last_scheduled_cf = self.last_scheduled_cf;
        let picked = if background {
            slot.check_compaction(state, &self.cursors)?
        } else {
            slot.check_manual_compaction(state, &self.cursors)?
        };
        if let Some(plan) = &picked {
            self.last_scheduled_cf = Some(plan.cf_id);
        }
        Ok(picked)
    }

    pub fn run_compaction(
        &mut self,
        state: &mut RuntimeState,
        plan: &CompactionPlan,
        storage: Option<&Arc<dyn CompactionStorage>>,
        tx: Option<crossbeam::channel::Sender<RuntimeMsg>>,
        deadline: Option<OperationDeadline>,
    ) -> MidgeResult<Vec<String>> {
        let index = self
            .slots
            .iter()
            .position(|slot| !slot.compaction_running)
            .ok_or_else(|| MidgeError::Busy("compaction owners are full".into()))?;
        if !self.jobs.available(plan, &state.manifest.files) {
            return Err(MidgeError::Busy(
                "compaction publication range is reserved".into(),
            ));
        }
        self.selected = index;
        if !self.hybrid_memory_attached {
            if let Some(parent) = storage.and_then(|storage| storage.maintenance_memory()) {
                self.cursors = crate::compaction::strategy::CompactionCursors::new();
                self.memory = parent.child(self.global_memory_limit);
                self.hybrid_memory_attached = true;
            }
        }
        let limit = self.memory_limit_for_plan(plan);
        self.slots[index].compaction_memory_limit = limit;
        self.slots[index].memory_budget = Some(self.memory.child(limit));
        if tx.is_some() {
            if self.pool.is_none() {
                self.pool = Some(pool::CompactionPool::new(self.slots.len())?);
            }
            self.slots[index].worker_pool.clone_from(&self.pool);
        }
        let cursor_charge = cursor_charge(state, plan)?;
        let cursor_id = (plan.cf_id, plan.source_level);
        let mut new_reservation = None;
        let mut old_charge = None;
        if let Some(charge) = cursor_charge {
            if let Some(cursor) = self.cursors.get_mut(&cursor_id) {
                let previous = cursor.reservation.bytes();
                cursor
                    .reservation
                    .resize(previous.max(charge), "compaction selection cursor")?;
                old_charge = Some(previous);
            } else {
                new_reservation = Some(self.memory.reserve(charge, "compaction selection cursor")?);
            }
        }
        let result = self.slots[index].run_compaction(state, plan, storage, tx, deadline);
        if let Some(charge) = cursor_charge {
            if result.is_ok() {
                // Admission only marks input files in the manifest; publication happens later.
                let file = state
                    .manifest
                    .files
                    .iter()
                    .find(|file| Some(&file.name) == plan.source_files.last())
                    .expect("admitted compaction retains its manifest inputs until publication");
                let key = file
                    .smallest_key
                    .as_ref()
                    .expect("cursor lower bound was checked before admission");
                if let Some(cursor) = self.cursors.get_mut(&cursor_id) {
                    cursor.key = Vec::new();
                    cursor.name = String::new();
                    cursor.key.clone_from(key);
                    cursor.name.clone_from(&file.name);
                    cursor
                        .reservation
                        .resize(charge, "compaction selection cursor")
                        .expect("shrinking cursor reservation succeeds");
                } else {
                    self.cursors.insert(
                        cursor_id,
                        crate::compaction::strategy::SelectionCursor {
                            key: key.clone(),
                            name: file.name.clone(),
                            reservation: new_reservation
                                .expect("new cursor reservation was admitted"),
                        },
                    );
                }
            } else if let Some(previous) = old_charge {
                self.cursors
                    .get_mut(&cursor_id)
                    .expect("existing cursor remains present")
                    .reservation
                    .resize(previous, "compaction selection cursor")
                    .expect("rolling back cursor growth succeeds");
            }
        }
        result
    }

    pub(crate) fn select_job(&mut self, id: u64) -> bool {
        let matching = self
            .slots
            .iter()
            .position(|slot| slot.compaction_running && slot.active_job_id == Some(id));
        #[cfg(test)]
        let matching = matching.or_else(|| {
            (self.active_count() == 1)
                .then(|| {
                    self.slots
                        .iter()
                        .position(|slot| slot.compaction_running && slot.active_job_id.is_none())
                })
                .flatten()
        });
        if let Some(index) = matching {
            self.selected = index;
            true
        } else {
            false
        }
    }

    pub(crate) fn select_completion(
        &mut self,
        id: u64,
        inputs: &[String],
        outputs: &[String],
        cf_id: u32,
        target: u32,
        succeeded: bool,
    ) -> bool {
        let matching = self
            .slots
            .iter()
            .position(|slot| slot.compaction_running && slot.active_job_id == Some(id));
        #[cfg(test)]
        let matching = matching.or_else(|| {
            self.slots.iter().position(|slot| {
                slot.compaction_running
                    && slot.active_job_id.is_none()
                    && slot
                        .active_output_generation
                        .is_some_and(|(cf, level, _)| cf == cf_id && level == target)
                    && slot.active_input_ssts.len() == inputs.len()
                    && inputs
                        .iter()
                        .all(|name| slot.active_input_ssts.contains(name))
            })
        });
        let Some(index) = matching else {
            return false;
        };
        let slot = &mut self.slots[index];
        if slot.active_job_id.is_some() {
            if !slot.poll_worker_result() {
                return false;
            }
            if slot.active_input_ssts != inputs
                || slot.completed_outputs.as_deref() != Some(outputs)
                || slot.worker_succeeded != Some(succeeded)
                || !slot
                    .active_output_generation
                    .is_some_and(|(cf, level, _)| cf == cf_id && level == target)
            {
                return false;
            }
            slot.completion_emitted = true;
        }
        self.selected = index;
        true
    }

    pub(crate) fn has_ready_compute(&self) -> bool {
        self.slots.iter().any(|slot| {
            !slot.completion_emitted
                && (slot.completed_outputs.is_some()
                    || slot
                        .worker_handle
                        .as_ref()
                        .is_some_and(pool::CompactionTask::is_ready))
        })
    }

    pub(crate) fn has_pending_compute(&self) -> bool {
        self.slots.iter().any(|slot| slot.worker_handle.is_some())
    }

    pub(crate) fn poll_compute_completion(&mut self) -> Option<RuntimeMsg> {
        for slot in self
            .slots
            .iter_mut()
            .filter(|slot| slot.compaction_running && !slot.completion_emitted)
        {
            if !slot.poll_worker_result() {
                continue;
            }
            let Some(id) = slot.active_job_id else {
                continue;
            };
            let Some((cf_id, target_level, _)) = slot.active_output_generation else {
                continue;
            };
            slot.completion_emitted = true;
            return Some(RuntimeMsg::CompactionComplete {
                request_id: id,
                input_ssts: slot.active_input_ssts.clone(),
                output_ssts: slot.completed_outputs.clone().unwrap_or_default(),
                cf_id,
                target_level,
                succeeded: slot.worker_succeeded.unwrap_or(false),
            });
        }
        None
    }

    pub(crate) fn protected_sst_names(&self) -> Vec<String> {
        let mut names = std::collections::BTreeSet::new();
        for slot in self.slots.iter().filter(|slot| slot.compaction_running) {
            names.extend(slot.active_input_ssts.iter().cloned());
            names.extend(slot.prepared_outputs.lock().keys().cloned());
        }
        names.into_iter().collect()
    }

    pub(crate) fn protected_output_generations(&self) -> Vec<(u32, u64)> {
        self.slots
            .iter()
            .filter(|slot| slot.compaction_running)
            .filter_map(|slot| {
                slot.active_output_generation
                    .map(|(cf, _, generation)| (cf, generation))
            })
            .collect()
    }

    pub fn cancel_and_join_worker(
        &mut self,
        state: &mut RuntimeState,
        storage: Option<&Arc<dyn CompactionStorage>>,
    ) {
        for slot in &self.slots {
            slot.worker_cancel.store(true, Ordering::Release);
        }
        for slot in &mut self.slots {
            slot.cancel_and_join_worker(state, storage);
        }
    }

    #[cfg(test)]
    pub fn set_l0_file_count_threshold(&mut self, threshold: usize) {
        for slot in &mut self.slots {
            slot.set_l0_file_count_threshold(threshold);
        }
    }
}

impl Drop for CompactionActor {
    fn drop(&mut self) {
        // Signal every worker before any selected owner starts joining.
        for slot in &self.slots {
            slot.worker_cancel.store(true, Ordering::Release);
        }
    }
}

impl CompactionJobActor {
    fn poll_worker_result(&mut self) -> bool {
        if self.completed_outputs.is_some() {
            return true;
        }
        let Some(result) = self
            .worker_handle
            .as_ref()
            .and_then(pool::CompactionTask::try_join)
        else {
            return false;
        };
        self.worker_handle = None;
        let outputs = if let Ok(outputs) = result {
            outputs
        } else {
            super::store_compaction_worker_error(
                &self.worker_error,
                Some(&super::compaction_worker_panic_error()),
            );
            Vec::new()
        };
        self.worker_succeeded = Some(
            self.worker_error
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_none(),
        );
        self.completed_outputs = Some(outputs);
        true
    }
}
