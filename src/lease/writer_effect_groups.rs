//! Counted writer lifetimes under one continuously held filesystem fence.
//!
//! A logical root can reserve jobs while it lives. Jobs keep the fence through
//! queueing, execution, and rollback. Once the creating root leaves, unrelated
//! roots cannot extend this cohort; already admitted roots may finish their jobs.

use super::traits::{scope_lease_error, LeaseError};
use crate::common::{DeadlineScope, OperationDeadline};
use crate::io::FsLock;
use parking_lot::{Condvar, Mutex};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;
use std::thread::ThreadId;
use std::time::Duration;

/// Optional authority for grouping physical writer jobs under one epoch proof.
pub trait WriterEffectGroups: Send + Sync {
    /// Admit a lexical, thread-bound root under the expected writer epoch.
    ///
    /// # Errors
    /// Returns timeout, storage, or fencing errors before admitting the root.
    fn begin_group(
        &self,
        holder: &str,
        epoch: u64,
        timeout: Duration,
    ) -> Result<WriterEffectGroup, LeaseError> {
        self.begin_group_with_context(holder, epoch, timeout, &WriterEffectGroupContext::default())
    }

    /// Preserve an existing caller scope while giving acquisition its own budget.
    ///
    /// # Errors
    /// Returns timeout, storage, or fencing errors before admitting the root.
    #[doc(hidden)]
    fn begin_group_with_context(
        &self,
        holder: &str,
        epoch: u64,
        timeout: Duration,
        context: &WriterEffectGroupContext,
    ) -> Result<WriterEffectGroup, LeaseError>;

    /// Execute one physical substep of an activated job with a fresh I/O budget.
    ///
    /// # Errors
    /// Returns an error before the callback when authority or scope is invalid.
    fn run_active(
        &self,
        holder: &str,
        epoch: u64,
        active: &ActiveWriterEffect,
        timeout: Duration,
        operation: &mut dyn FnMut(),
    ) -> Result<(), LeaseError>;
}

/// Opaque incoming operation scope, separate from an acquisition-stage timeout.
#[derive(Clone, Default)]
pub struct WriterEffectGroupContext {
    scopes: Vec<DeadlineScope>,
}

impl WriterEffectGroupContext {
    pub(super) fn with_scope(&self, scope: DeadlineScope) -> Self {
        let mut context = self.clone();
        context.scopes.push(scope);
        context
    }

    pub(super) fn check(&self, operation: &str) -> Result<(), LeaseError> {
        for scope in &self.scopes {
            scope.check(operation).map_err(scope_lease_error)?;
        }
        Ok(())
    }

    pub(super) fn timeout(&self, timeout: Duration) -> Duration {
        self.scopes
            .iter()
            .fold(timeout, |remaining, scope| scope.clamp(remaining))
    }

    pub(super) fn scopes(&self) -> &[DeadlineScope] {
        &self.scopes
    }
}

#[derive(Clone, PartialEq, Eq)]
struct WriterKey {
    holder: String,
    epoch: u64,
}

struct Grant {
    generation: u64,
    key: WriterKey,
    creator: u64,
    creator_live: bool,
    roots: HashMap<u64, ThreadId>,
    jobs: usize,
    active_owners: HashMap<ThreadId, usize>,
    native: Box<dyn FsLock>,
}

enum Phase {
    Idle,
    Admitting { generation: u64, thread: ThreadId },
    Active(Grant),
    Releasing(u64),
    Transition,
}

struct State {
    phase: Phase,
    next_id: u64,
    transition_waiters: usize,
}

pub(super) struct WriterEffectCoordinator {
    state: Mutex<State>,
    changed: Condvar,
}

impl Default for WriterEffectCoordinator {
    fn default() -> Self {
        Self {
            state: Mutex::new(State {
                phase: Phase::Idle,
                next_id: 0,
                transition_waiters: 0,
            }),
            changed: Condvar::new(),
        }
    }
}

impl State {
    fn next_id(&mut self) -> Result<u64, LeaseError> {
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or_else(|| LeaseError::Internal("writer-effect identities exhausted".into()))?;
        Ok(self.next_id)
    }
}

impl WriterEffectCoordinator {
    #[cfg(test)]
    pub(super) fn wait_for_transition_waiters(&self, expected: usize) {
        let deadline = OperationDeadline::from_budget(Duration::from_secs(2));
        let mut state = self.state.lock();
        while state.transition_waiters != expected {
            assert!(!deadline.is_expired(), "transition intent was not observed");
            self.changed.wait_for(&mut state, deadline.remaining());
        }
    }

    pub(super) fn begin(
        self: &Arc<Self>,
        holder: &str,
        epoch: u64,
        timeout: Duration,
        context: WriterEffectGroupContext,
        acquire: impl FnOnce() -> Result<Box<dyn FsLock>, LeaseError>,
    ) -> Result<WriterEffectGroup, LeaseError> {
        let key = WriterKey {
            holder: holder.into(),
            epoch,
        };
        let deadline = effect_deadline(timeout, &context)?;
        let (generation, root) = self.admit_root(&key, deadline, &context)?;
        if let Some(generation) = generation {
            let mut admission = AdmissionPermit {
                coordinator: Arc::clone(self),
                generation,
                committed: false,
            };
            let acquired = acquire().and_then(|native| {
                check_deadline(deadline, &context)?;
                Ok(native)
            });
            let mut state = self.state.lock();
            match acquired {
                Ok(native) => {
                    state.phase = Phase::Active(Grant {
                        generation,
                        key: key.clone(),
                        creator: root,
                        creator_live: true,
                        roots: HashMap::from([(root, std::thread::current().id())]),
                        jobs: 0,
                        active_owners: HashMap::new(),
                        native,
                    });
                    admission.committed = true;
                }
                Err(error) => {
                    drop(state);
                    drop(admission);
                    return Err(error);
                }
            }
            self.changed.notify_all();
        }
        let generation = {
            let state = self.state.lock();
            let Phase::Active(grant) = &state.phase else {
                return Err(LeaseError::Internal(
                    "admitted writer cohort disappeared".into(),
                ));
            };
            grant.generation
        };
        Ok(WriterEffectGroup {
            coordinator: Arc::clone(self),
            generation,
            root,
            key,
            context,
            _thread_bound: PhantomData,
        })
    }

    fn admit_root(
        &self,
        key: &WriterKey,
        deadline: OperationDeadline,
        context: &WriterEffectGroupContext,
    ) -> Result<(Option<u64>, u64), LeaseError> {
        let thread = std::thread::current().id();
        let mut state = self.state.lock();
        loop {
            check_deadline(deadline, context)?;
            let pending = state.transition_waiters != 0;
            match &state.phase {
                Phase::Idle if !pending => {
                    let root = state.next_id()?;
                    let generation = state.next_id()?;
                    state.phase = Phase::Admitting { generation, thread };
                    return Ok((Some(generation), root));
                }
                Phase::Active(grant) if grant.key == *key && grant.creator_live && !pending => {
                    grant
                        .native
                        .validate_held()
                        .map_err(|error| native_error(&error))?;
                    check_deadline(deadline, context)?;
                    let root = state.next_id()?;
                    let Phase::Active(grant) = &mut state.phase else {
                        unreachable!()
                    };
                    grant.roots.insert(root, thread);
                    return Ok((None, root));
                }
                Phase::Active(grant) if grant.key != *key => {
                    return Err(LeaseError::RenewalFailed(
                        "requested writer does not match the continuously held epoch proof".into(),
                    ));
                }
                Phase::Active(grant)
                    if grant.active_owners.contains_key(&thread)
                        || grant.roots.values().any(|owner| *owner == thread) =>
                {
                    return Err(LeaseError::RenewalFailed(
                        "nested independent writer root cannot wait for its own cohort; reserve a child job from the admitted root".into(),
                    ));
                }
                Phase::Admitting { thread: owner, .. } if *owner == thread => {
                    return Err(LeaseError::Internal(
                        "recursive writer fence admission".into(),
                    ));
                }
                _ => self.changed.wait_for(
                    &mut state,
                    deadline.remaining().min(Duration::from_millis(1)),
                ),
            };
        }
    }

    pub(super) fn transition(
        self: &Arc<Self>,
        timeout: Duration,
        context: &WriterEffectGroupContext,
    ) -> Result<TransitionPermit, LeaseError> {
        let deadline = effect_deadline(timeout, context)?;
        let thread = std::thread::current().id();
        let mut state = self.state.lock();
        if matches!(&state.phase, Phase::Active(grant) if grant.active_owners.contains_key(&thread) || grant.roots.values().any(|owner| *owner == thread))
        {
            return Err(LeaseError::RenewalFailed(
                "lease transition cannot wait for its own writer effect".into(),
            ));
        }
        state.transition_waiters += 1;
        self.changed.notify_all();
        drop(state);
        let mut pending = TransitionPermit {
            coordinator: Arc::clone(self),
            acquired: false,
        };
        let mut state = self.state.lock();
        loop {
            check_deadline(deadline, context)?;
            if matches!(state.phase, Phase::Idle) {
                state.phase = Phase::Transition;
                pending.acquired = true;
                return Ok(pending);
            }
            self.changed.wait_for(
                &mut state,
                deadline.remaining().min(Duration::from_millis(1)),
            );
        }
    }

    fn release(&self, generation: u64, root: Option<u64>) {
        let mut state = self.state.lock();
        let Phase::Active(grant) = &mut state.phase else {
            return;
        };
        if grant.generation != generation {
            return;
        }
        if let Some(root) = root {
            grant.roots.remove(&root);
            if root == grant.creator {
                grant.creator_live = false;
            }
        } else {
            grant.jobs -= 1;
        }
        if !grant.roots.is_empty() || grant.jobs != 0 {
            self.changed.notify_all();
            return;
        }
        let Phase::Active(grant) =
            std::mem::replace(&mut state.phase, Phase::Releasing(generation))
        else {
            unreachable!()
        };
        drop(state);
        let releasing = ReleasePermit {
            coordinator: self,
            generation,
        };
        // Clear all authority proof before native unlock, and never invoke an
        // external guard destructor while holding the coordinator mutex.
        drop(grant);
        drop(releasing);
    }
}

struct ReleasePermit<'a> {
    coordinator: &'a WriterEffectCoordinator,
    generation: u64,
}

impl Drop for ReleasePermit<'_> {
    fn drop(&mut self) {
        let mut state = self.coordinator.state.lock();
        if matches!(state.phase, Phase::Releasing(current) if current == self.generation) {
            state.phase = Phase::Idle;
            self.coordinator.changed.notify_all();
        }
    }
}

struct AdmissionPermit {
    coordinator: Arc<WriterEffectCoordinator>,
    generation: u64,
    committed: bool,
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let mut state = self.coordinator.state.lock();
        if matches!(state.phase, Phase::Admitting { generation, .. } if generation == self.generation)
        {
            state.phase = Phase::Idle;
            self.coordinator.changed.notify_all();
        }
    }
}

fn native_error(error: &crate::io::FsError) -> LeaseError {
    LeaseError::RenewalFailed(format!("writer storage fence is no longer held: {error}"))
}

fn effect_deadline(
    timeout: Duration,
    context: &WriterEffectGroupContext,
) -> Result<OperationDeadline, LeaseError> {
    context.check("writer effect admission")?;
    let timeout = context.timeout(timeout);
    if timeout.is_zero() {
        return Err(LeaseError::Timeout(
            "writer effect deadline exhausted".into(),
        ));
    }
    Ok(OperationDeadline::from_budget(timeout))
}

fn check_deadline(
    deadline: OperationDeadline,
    context: &WriterEffectGroupContext,
) -> Result<(), LeaseError> {
    context.check("writer effect admission")?;
    if deadline.is_expired() {
        return Err(LeaseError::Timeout(
            "writer effect deadline exhausted waiting for fence".into(),
        ));
    }
    Ok(())
}

/// One admitted lexical writer operation, bound to its caller's thread.
///
/// Roots cannot be cloned or sent to another thread; reserve tickets to transfer
/// accounted jobs to a worker.
pub struct WriterEffectGroup {
    coordinator: Arc<WriterEffectCoordinator>,
    generation: u64,
    root: u64,
    key: WriterKey,
    context: WriterEffectGroupContext,
    // Roots are lexical caller ownership. Only accounted job tickets cross
    // threads, so local self-transition detection cannot follow stale owners.
    _thread_bound: PhantomData<Rc<()>>,
}

impl WriterEffectGroup {
    /// Account for one queued job before handing it to another thread.
    ///
    /// # Errors
    /// Returns timeout or fencing errors when the root cannot admit a job.
    pub fn reserve_ticket(&self, timeout: Duration) -> Result<WriterEffectTicket, LeaseError> {
        effect_deadline(timeout, &self.context)?;
        let mut state = self.coordinator.state.lock();
        let Phase::Active(grant) = &mut state.phase else {
            return Err(LeaseError::RenewalFailed(
                "writer cohort is no longer active".into(),
            ));
        };
        if grant.generation != self.generation
            || grant.key != self.key
            || !grant.roots.contains_key(&self.root)
        {
            return Err(LeaseError::RenewalFailed(
                "writer group identity no longer matches its fence".into(),
            ));
        }
        grant
            .native
            .validate_held()
            .map_err(|error| native_error(&error))?;
        grant.jobs = grant
            .jobs
            .checked_add(1)
            .ok_or_else(|| LeaseError::Internal("writer jobs exhausted".into()))?;
        Ok(WriterEffectTicket {
            permit: JobPermit {
                coordinator: Arc::clone(&self.coordinator),
                generation: self.generation,
                key: self.key.clone(),
                context: self.context.clone(),
            },
        })
    }
}

impl Drop for WriterEffectGroup {
    fn drop(&mut self) {
        self.coordinator.release(self.generation, Some(self.root));
    }
}

struct JobPermit {
    coordinator: Arc<WriterEffectCoordinator>,
    generation: u64,
    key: WriterKey,
    context: WriterEffectGroupContext,
}

impl Drop for JobPermit {
    fn drop(&mut self) {
        self.coordinator.release(self.generation, None);
    }
}

/// Ownership of one queued job. Activation consumes this ticket exactly once.
pub struct WriterEffectTicket {
    permit: JobPermit,
}

impl WriterEffectTicket {
    /// Start this job on its worker, consuming its transferable ticket.
    ///
    /// # Errors
    /// Returns timeout or fencing errors if the queued authority is invalid.
    pub fn activate(self) -> Result<ActiveWriterEffect, LeaseError> {
        self.permit.context.check("queued writer job activation")?;
        validate_permit(&self.permit)?;
        let thread = std::thread::current().id();
        let mut state = self.permit.coordinator.state.lock();
        let Phase::Active(grant) = &mut state.phase else {
            return Err(LeaseError::RenewalFailed(
                "writer job fence is no longer active".into(),
            ));
        };
        *grant.active_owners.entry(thread).or_default() += 1;
        drop(state);
        Ok(ActiveWriterEffect {
            permit: self.permit,
            thread,
            _thread_bound: PhantomData,
        })
    }
}

/// One thread-bound running job, retaining its fence through cleanup substeps.
pub struct ActiveWriterEffect {
    permit: JobPermit,
    thread: ThreadId,
    _thread_bound: PhantomData<Rc<()>>,
}

impl ActiveWriterEffect {
    pub(super) fn run(
        &self,
        coordinator: &Arc<WriterEffectCoordinator>,
        holder: &str,
        epoch: u64,
        timeout: Duration,
        operation: &mut dyn FnMut(),
    ) -> Result<(), LeaseError> {
        if !Arc::ptr_eq(coordinator, &self.permit.coordinator)
            || self.permit.key.holder != holder
            || self.permit.key.epoch != epoch
        {
            return Err(LeaseError::RenewalFailed(
                "writer job belongs to a different authority".into(),
            ));
        }
        let deadline = effect_deadline(timeout, &self.permit.context)?;
        validate_permit(&self.permit)?;
        check_deadline(deadline, &self.permit.context)?;
        operation();
        Ok(())
    }
}

fn validate_permit(permit: &JobPermit) -> Result<(), LeaseError> {
    let state = permit.coordinator.state.lock();
    let Phase::Active(grant) = &state.phase else {
        return Err(LeaseError::RenewalFailed(
            "writer job fence is no longer active".into(),
        ));
    };
    if grant.generation != permit.generation || grant.key != permit.key {
        return Err(LeaseError::RenewalFailed(
            "writer job fence generation changed".into(),
        ));
    }
    grant
        .native
        .validate_held()
        .map_err(|error| native_error(&error))
}

impl Drop for ActiveWriterEffect {
    fn drop(&mut self) {
        let mut state = self.permit.coordinator.state.lock();
        if let Phase::Active(grant) = &mut state.phase {
            let count = grant
                .active_owners
                .get_mut(&self.thread)
                .expect("active writer job is counted");
            *count -= 1;
            if *count == 0 {
                grant.active_owners.remove(&self.thread);
            }
        }
    }
}

pub(super) struct TransitionPermit {
    coordinator: Arc<WriterEffectCoordinator>,
    acquired: bool,
}

impl Drop for TransitionPermit {
    fn drop(&mut self) {
        let mut state = self.coordinator.state.lock();
        state.transition_waiters -= 1;
        if self.acquired {
            state.phase = Phase::Idle;
        }
        self.coordinator.changed.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::MockFs;
    use crate::lease::fs_leader_store::FsLeaderStore;
    use crate::lease::LeaderStore;

    #[test]
    fn should_preserve_other_transition_intents_when_one_waiter_cancels() {
        // Arrange
        let budget = Duration::from_secs(2);
        let store = Arc::new(FsLeaderStore::new(
            Arc::new(MockFs::new()),
            crate::lease::LeaseClock::default(),
        ));
        let epoch = store.acquire_leadership("writer").unwrap().epoch;
        let root = store.begin_group("writer", epoch, budget).unwrap();
        let coordinator = Arc::clone(&root.coordinator);
        let cancel = DeadlineScope::new(OperationDeadline::from_budget(budget));
        let first_context = WriterEffectGroupContext::default().with_scope(cancel.clone());
        let first_coordinator = Arc::clone(&coordinator);
        let first =
            std::thread::spawn(move || first_coordinator.transition(budget, &first_context));
        coordinator.wait_for_transition_waiters(1);
        let second_coordinator = Arc::clone(&coordinator);
        let second = std::thread::spawn(move || {
            second_coordinator.transition(budget, &WriterEffectGroupContext::default())
        });
        coordinator.wait_for_transition_waiters(2);

        // Act: dropping one owned pending intent must not reopen admissions.
        cancel.cancel();
        assert!(matches!(first.join().unwrap(), Err(LeaseError::Timeout(_))));
        coordinator.wait_for_transition_waiters(1);
        let independent_store = Arc::clone(&store);
        let independent = std::thread::spawn(move || {
            matches!(
                independent_store.begin_group("writer", epoch, Duration::from_millis(10)),
                Err(LeaseError::Timeout(_))
            )
        })
        .join()
        .unwrap();
        let ticket = root.reserve_ticket(budget).unwrap();
        drop(root);
        let active = ticket.activate().unwrap();
        let mut ran = false;
        store
            .run_active("writer", epoch, &active, budget, &mut || ran = true)
            .unwrap();
        drop(active);
        let transition = second.join().unwrap().unwrap();

        // Assert
        assert!(independent);
        assert!(ran);
        drop(transition);
        store.begin_group("writer", epoch, budget).unwrap();
    }
}
