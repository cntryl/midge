//! Versioned object storage using the production callback contract.

use super::scheduler::Scheduler;
use crate::storage::{
    MetadataReadCallback, RangeReadCallback, StorageBackend, StorageCallback, StorageError,
    StorageEvent, StorageObjectMetadata, StorageOutcome, StoragePrecondition, StorageRequest,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct Object {
    bytes: Vec<u8>,
    metadata: StorageObjectMetadata,
}

#[derive(Default)]
struct State {
    next_version: u64,
    objects: BTreeMap<String, Object>,
}

/// Cloneable provider view. Views share objects and have independently fenced
/// command epochs. Conditions are evaluated when effects land, never on submit.
#[derive(Clone)]
pub struct ObjectStore {
    state: Arc<Mutex<State>>,
    scheduler: Scheduler,
    epoch: u64,
}

impl ObjectStore {
    #[must_use]
    pub fn new(scheduler: Scheduler) -> Self {
        Self {
            state: Arc::default(),
            scheduler,
            epoch: 0,
        }
    }

    #[must_use]
    pub fn with_epoch(&self, epoch: u64) -> Self {
        Self {
            epoch,
            ..self.clone()
        }
    }

    #[must_use]
    pub fn physical_bytes(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .objects
            .values()
            .map(|object| object.bytes.len() as u64)
            .sum()
    }

    fn submit<T: Clone + CompletionSummary + Send + 'static>(
        &self,
        kind: &str,
        request: StorageRequest,
        callback: std::sync::mpsc::Sender<T>,
        operation: impl FnOnce(&mut State, &StorageRequest) -> T + Send + 'static,
    ) {
        let state = self.state.clone();
        let expected = match &request.precondition {
            StoragePrecondition::IfMatch(metadata) => Some(
                metadata
                    .generation
                    .as_ref()
                    .unwrap_or(&metadata.etag)
                    .clone(),
            ),
            StoragePrecondition::IfAbsent => Some("absent".into()),
            StoragePrecondition::None => None,
        };
        self.scheduler.submit(
            self.epoch,
            kind,
            request.key.clone(),
            expected,
            Box::new(move || {
                let result = operation(
                    &mut state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner),
                    &request,
                );
                let description = result.summary();
                (
                    Box::new(move || {
                        // Retain the request's reservation through terminal delivery or
                        // response loss, including a delayed or duplicated callback.
                        let _request = &request;
                        let _ = callback.send(result.clone());
                    }),
                    description,
                )
            }),
        );
    }
}

trait CompletionSummary {
    fn summary(&self) -> String;
}

impl<T> CompletionSummary for Result<T, StorageError> {
    fn summary(&self) -> String {
        match self {
            Ok(_) => "Ok".into(),
            Err(error) => format!("Err({:?}): {error}", error.kind()),
        }
    }
}

impl CompletionSummary for StorageEvent {
    fn summary(&self) -> String {
        match self {
            StorageEvent::WriteComplete { result, .. }
            | StorageEvent::DeleteComplete { result, .. } => format!("{result:?}"),
            StorageEvent::HeadComplete { result, .. } => format!("{result:?}"),
            _ => "unexpected simulation completion".into(),
        }
    }
}

fn check_budget(request: &StorageRequest) -> Result<(), StorageError> {
    if request.remaining_timeout().is_zero() {
        Err(StorageError::timeout(
            "simulated request deadline expired before effect",
        ))
    } else {
        Ok(())
    }
}

fn check_read(request: &StorageRequest) -> Result<(), StorageError> {
    check_budget(request)?;
    if request.precondition != StoragePrecondition::None {
        return Err(StorageError::protocol(
            "metadata reads and HEAD reject mutation preconditions",
        ));
    }
    Ok(())
}

fn check_match(
    object: Option<&Object>,
    condition: &StoragePrecondition,
) -> Result<(), StorageError> {
    let satisfied = match condition {
        StoragePrecondition::None => true,
        StoragePrecondition::IfAbsent => object.is_none(),
        StoragePrecondition::IfMatch(expected) => object.is_some_and(|object| {
            if let Some(generation) = expected
                .generation
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                object.metadata.generation.as_deref().map(str::trim) == Some(generation)
            } else {
                let etag = expected.etag.trim().trim_matches('"');
                !etag.is_empty() && etag == object.metadata.etag.trim().trim_matches('"')
            }
        }),
    };
    if satisfied {
        Ok(())
    } else {
        Err(StorageError::precondition_failed(
            "simulated object version changed",
        ))
    }
}

fn outcome<T: Clone>(result: Result<T, StorageError>) -> StorageOutcome<T> {
    match result {
        Ok(value) => StorageOutcome::Ok(value),
        Err(error) => StorageOutcome::Err(error),
    }
}

impl StorageBackend for ObjectStore {
    fn submit_write_request(
        &self,
        request: StorageRequest,
        data: Vec<u8>,
        callback: StorageCallback,
    ) {
        self.submit("write", request, callback, move |state, request| {
            let result = (|| {
                check_budget(request)?;
                check_match(state.objects.get(&request.key), &request.precondition)?;
                state.next_version = state
                    .next_version
                    .checked_add(1)
                    .ok_or_else(|| StorageError::protocol("simulated version exhaustion"))?;
                let metadata = StorageObjectMetadata {
                    size: data.len() as u64,
                    etag: format!("simulated-{}", state.next_version),
                    generation: Some(state.next_version.to_string()),
                };
                state.objects.insert(
                    request.key.clone(),
                    Object {
                        bytes: data,
                        metadata,
                    },
                );
                Ok(())
            })();
            StorageEvent::WriteComplete {
                key: request.key.clone(),
                result: outcome(result),
            }
        });
    }

    fn submit_delete_request(&self, request: StorageRequest, callback: StorageCallback) {
        self.submit("delete", request, callback, |state, request| {
            let result = (|| {
                check_budget(request)?;
                if request.precondition == StoragePrecondition::IfAbsent {
                    return Err(StorageError::precondition_failed(
                        "delete cannot enforce absence",
                    ));
                }
                if state.objects.contains_key(&request.key) {
                    check_match(state.objects.get(&request.key), &request.precondition)?;
                    state.objects.remove(&request.key);
                }
                Ok(())
            })();
            StorageEvent::DeleteComplete {
                key: request.key.clone(),
                result: outcome(result),
            }
        });
    }

    fn submit_head_request(&self, request: StorageRequest, callback: StorageCallback) {
        self.submit("head", request, callback, |state, request| {
            let result = check_read(request).and_then(|()| {
                state
                    .objects
                    .get(&request.key)
                    .map(|object| object.metadata.clone())
                    .ok_or_else(|| StorageError::not_found(&request.key))
            });
            StorageEvent::HeadComplete {
                key: request.key.clone(),
                result: outcome(result),
            }
        });
    }

    fn submit_range_head_request(&self, request: StorageRequest, callback: StorageCallback) {
        self.submit_head_request(request, callback);
    }

    fn submit_metadata_read_request(
        &self,
        request: StorageRequest,
        callback: MetadataReadCallback,
    ) {
        self.submit("read", request, callback, |state, request| {
            check_read(request)?;
            let object = state
                .objects
                .get(&request.key)
                .ok_or_else(|| StorageError::not_found(&request.key))?;
            Ok((object.bytes.clone(), object.metadata.clone()))
        });
    }

    fn submit_range_read_request(
        &self,
        request: StorageRequest,
        range: std::ops::Range<u64>,
        callback: RangeReadCallback,
    ) {
        self.submit("range", request, callback, move |state, request| {
            check_budget(request)?;
            let StoragePrecondition::IfMatch(expected) = &request.precondition else {
                return Err(StorageError::protocol(
                    "range read requires object identity",
                ));
            };
            let object = state
                .objects
                .get(&request.key)
                .ok_or_else(|| StorageError::not_found(&request.key))?;
            if !expected.same_version(&object.metadata) {
                return Err(StorageError::precondition_failed(
                    "simulated range metadata changed",
                ));
            }
            let start = usize::try_from(range.start)
                .map_err(|_| StorageError::protocol("range overflow"))?;
            let end =
                usize::try_from(range.end).map_err(|_| StorageError::protocol("range overflow"))?;
            object
                .bytes
                .get(start..end)
                .map(<[u8]>::to_vec)
                .ok_or_else(|| StorageError::protocol("short or reversed range"))
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::OperationDeadline;
    use crate::simulation::scheduler::Action;
    use crate::storage::StorageErrorKind;
    use std::sync::mpsc;
    use std::time::Duration;

    fn request() -> StorageRequest {
        StorageRequest::new(
            "root",
            OperationDeadline::unbounded(),
            Duration::from_secs(5),
        )
    }

    #[test]
    fn should_reject_delayed_stale_effect_when_successor_cas_lands_first() {
        // Arrange
        let scheduler = Scheduler::eager();
        let store = ObjectStore::new(scheduler);
        let (tx, rx) = mpsc::channel();
        store.submit_write_request(request(), b"initial".to_vec(), tx);
        rx.recv().unwrap();
        let (tx, rx) = mpsc::channel();
        store.submit_metadata_read_request(request(), tx);
        let (_, original) = rx.recv().unwrap().unwrap();
        let scheduler = Scheduler::manual();
        let store = ObjectStore {
            scheduler: scheduler.clone(),
            ..store
        };
        let (old_tx, old_rx) = mpsc::channel();
        store.with_epoch(1).submit_write_request(
            request().with_precondition(StoragePrecondition::IfMatch(original.clone())),
            b"stale".to_vec(),
            old_tx,
        );
        let (new_tx, new_rx) = mpsc::channel();
        store.with_epoch(2).submit_write_request(
            request().with_precondition(StoragePrecondition::IfMatch(original)),
            b"successor".to_vec(),
            new_tx,
        );
        // Act
        scheduler.apply(Action::Effect(2)).unwrap();
        scheduler.apply(Action::LoseResponse(2)).unwrap();
        scheduler.apply(Action::Effect(1)).unwrap();
        scheduler.apply(Action::Deliver(1)).unwrap();
        // Assert
        assert!(new_rx.recv().is_err());
        assert!(
            matches!(old_rx.recv().unwrap(), StorageEvent::WriteComplete { result: StorageOutcome::Err(error), .. } if error.kind() == StorageErrorKind::PreconditionFailed)
        );
        let eager = ObjectStore {
            scheduler: Scheduler::eager(),
            ..store
        };
        let (tx, rx) = mpsc::channel();
        eager.submit_metadata_read_request(request(), tx);
        assert_eq!(rx.recv().unwrap().unwrap().0, b"successor");
        assert_eq!(eager.physical_bytes(), 9);
    }
}
