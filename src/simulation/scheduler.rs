//! Storage effects and response delivery are separate scheduler actions.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

type Delivery = Box<dyn Fn() + Send>;
type Effect = Box<dyn FnOnce() -> (Delivery, String) + Send>;

/// Immutable identity supplied with a scheduled operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Command {
    pub id: u64,
    pub epoch: u64,
    pub kind: String,
    pub key: String,
    pub expected_identity: Option<String>,
}

/// A replayable scheduling action, independent of the worker's response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Action {
    Effect(u64),
    Deliver(u64),
    Duplicate(u64),
    LoseResponse(u64),
    Cancel(u64),
}

/// Recorded inputs and observed effect results for a counterexample.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TraceEvent {
    Issued(Command),
    Action(Action),
    EffectResult { id: u64, result: String },
}

struct Pending {
    command: Command,
    effect: Option<Effect>,
    delivery: Option<Delivery>,
}

#[derive(Default)]
struct State {
    next_id: u64,
    pending: BTreeMap<u64, Pending>,
    trace: Vec<TraceEvent>,
}

/// Manual schedulers retain responses until explicitly delivered or lost.
/// Eager schedulers execute the identical effect and completion paths immediately.
#[derive(Clone)]
pub struct Scheduler {
    state: Arc<Mutex<State>>,
    eager: bool,
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::manual()
    }
}

impl Scheduler {
    #[must_use]
    pub fn manual() -> Self {
        Self {
            state: Arc::default(),
            eager: false,
        }
    }

    #[must_use]
    pub fn eager() -> Self {
        Self {
            state: Arc::default(),
            eager: true,
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn submit(
        &self,
        epoch: u64,
        kind: &str,
        key: String,
        expected_identity: Option<String>,
        effect: Effect,
    ) -> u64 {
        let id = {
            let mut state = self.lock();
            state.next_id = state
                .next_id
                .checked_add(1)
                .expect("scheduler ID exhaustion");
            let id = state.next_id;
            let command = Command {
                id,
                epoch,
                kind: kind.into(),
                key,
                expected_identity,
            };
            state.trace.push(TraceEvent::Issued(command.clone()));
            state.pending.insert(
                id,
                Pending {
                    command,
                    effect: Some(effect),
                    delivery: None,
                },
            );
            id
        };
        if self.eager {
            self.apply(Action::Effect(id)).expect("new effect exists");
            self.apply(Action::Deliver(id))
                .expect("effect produced completion");
        }
        id
    }

    /// Commands awaiting an effect or completion, ordered by operation ID.
    #[must_use]
    pub fn pending(&self) -> Vec<(Command, bool)> {
        self.lock()
            .pending
            .values()
            .map(|pending| (pending.command.clone(), pending.delivery.is_some()))
            .collect()
    }

    #[must_use]
    pub fn trace(&self) -> Vec<TraceEvent> {
        self.lock().trace.clone()
    }

    /// Execute one effect, deliver/duplicate its frozen result, or abandon work.
    ///
    /// # Errors
    /// Returns an error for an unknown ID or an action in the wrong phase.
    pub fn apply(&self, action: Action) -> Result<(), String> {
        match action {
            Action::Effect(id) => {
                let effect = {
                    let mut state = self.lock();
                    let pending = state
                        .pending
                        .get_mut(&id)
                        .ok_or_else(|| format!("unknown operation {id}"))?;
                    let effect = pending
                        .effect
                        .take()
                        .ok_or_else(|| format!("effect {id} already executed"))?;
                    state.trace.push(TraceEvent::Action(Action::Effect(id)));
                    effect
                };
                let (delivery, result) = effect();
                let mut state = self.lock();
                state.trace.push(TraceEvent::EffectResult { id, result });
                if let Some(pending) = state.pending.get_mut(&id) {
                    pending.delivery = Some(delivery);
                }
            }
            Action::Deliver(id) | Action::LoseResponse(id) => {
                let pending = {
                    let mut state = self.lock();
                    let pending = state
                        .pending
                        .get(&id)
                        .ok_or_else(|| format!("unknown operation {id}"))?;
                    if pending.delivery.is_none() {
                        return Err(format!("effect {id} has not completed"));
                    }
                    state.trace.push(TraceEvent::Action(action.clone()));
                    state
                        .pending
                        .remove(&id)
                        .ok_or_else(|| format!("unknown operation {id}"))?
                };
                if matches!(action, Action::Deliver(_)) {
                    pending
                        .delivery
                        .ok_or_else(|| format!("missing completion {id}"))?();
                }
            }
            Action::Duplicate(id) => {
                let mut state = self.lock();
                let pending = state
                    .pending
                    .get(&id)
                    .ok_or_else(|| format!("unknown operation {id}"))?;
                let delivery = pending
                    .delivery
                    .as_ref()
                    .ok_or_else(|| format!("effect {id} has not completed"))?;
                // Deliveries only send frozen values to unbounded channels; they
                // cannot invoke another scheduler operation under this lock.
                delivery();
                state.trace.push(TraceEvent::Action(action));
            }
            Action::Cancel(id) => {
                let mut state = self.lock();
                let pending = state
                    .pending
                    .get(&id)
                    .ok_or_else(|| format!("unknown operation {id}"))?;
                if pending.effect.is_none() {
                    return Err(format!(
                        "effect {id} has already started; lose its response instead"
                    ));
                }
                state.pending.remove(&id);
                state.trace.push(TraceEvent::Action(action));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    #[test]
    fn should_keep_effect_applied_when_response_is_lost() {
        // Arrange
        let scheduler = Scheduler::manual();
        let value = Arc::new(Mutex::new(0));
        let changed = value.clone();
        let (tx, rx) = mpsc::channel();
        let id = scheduler.submit(
            7,
            "write",
            "root".into(),
            Some("v1".into()),
            Box::new(move || {
                *changed.lock().unwrap() = 2;
                (
                    Box::new(move || {
                        let _ = tx.send(2);
                    }),
                    "applied".into(),
                )
            }),
        );
        // Act
        scheduler.apply(Action::Effect(id)).unwrap();
        assert!(rx.try_recv().is_err());
        scheduler.apply(Action::LoseResponse(id)).unwrap();
        // Assert
        assert_eq!(*value.lock().unwrap(), 2);
        assert!(rx.recv().is_err());
        assert_eq!(scheduler.pending(), Vec::new());
    }

    #[test]
    fn should_duplicate_completion_without_executing_effect_twice() {
        // Arrange
        let scheduler = Scheduler::manual();
        let value = Arc::new(Mutex::new(0));
        let changed = value.clone();
        let (tx, rx) = mpsc::channel();
        let id = scheduler.submit(
            0,
            "write",
            "key".into(),
            None,
            Box::new(move || {
                *changed.lock().unwrap() += 1;
                (
                    Box::new(move || {
                        let _ = tx.send(1);
                    }),
                    "applied".into(),
                )
            }),
        );
        // Act
        scheduler.apply(Action::Effect(id)).unwrap();
        scheduler.apply(Action::Duplicate(id)).unwrap();
        scheduler.apply(Action::Deliver(id)).unwrap();
        // Assert
        assert_eq!(rx.iter().collect::<Vec<_>>(), vec![1, 1]);
        assert_eq!(*value.lock().unwrap(), 1);
        assert!(scheduler.apply(Action::Effect(id)).is_err());
    }
}
