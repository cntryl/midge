//! A simulated object store with S3 semantics and an adversarial schedule
//! (#830), qualified against the review of the earlier draft (#810).
//!
//! It plugs into the engine as a [`CloudBackend`] through
//! `object_store_for_testing`, so the production `CloudStorage` adapter, its
//! deadlines and its conditional headers all run against it.
//!
//! - **Effects and responses are separate.** A schedule gives every request
//!   a [`Fate`]: applied and answered, failed before or after its effect,
//!   answered twice, or never answered. A `Late` request is not applied until
//!   the test lands it, after its client has given up, as a request that
//!   reached the provider applies whatever the client's deadline. Conditions
//!   are evaluated when an effect lands, never when it is submitted.
//! - **Identity is S3's.** An `ETag` is a digest of the content, so deleting an
//!   object and putting identical bytes again repeats its identity, and a
//!   stale `If-Match` then succeeds against the successor. Conditional
//!   requests compare identities only, never sizes.
//! - **Replay is stable.** A request is named by its operation, its key and
//!   how many earlier requests of that operation named that key, so a seeded
//!   or scripted schedule gives each request the same fate however engine
//!   threads interleave.

use crate::storage::cloud::{CloudBackend, CloudCallback, CloudError, CloudEvent, ObjectMetadata};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

/// The kind of request a [`RequestId`] names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Operation {
    Put,
    Get,
    Head,
    Delete,
    List,
    RangeRead,
}

impl Operation {
    fn mutates(self) -> bool {
        matches!(self, Self::Put | Self::Delete)
    }
}

/// A stable name for one request: its operation, its key and how many
/// earlier requests of that operation named that key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RequestId {
    pub operation: Operation,
    pub key: String,
    pub occurrence: u64,
}

/// What happens to one request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Fate {
    /// Applied, and answered once.
    Normal,
    /// Not applied; answered with a server error.
    FailBefore,
    /// Applied; answered with a server error anyway.
    FailAfter,
    /// Applied; never answered, so the client times out.
    LoseResponse,
    /// Applied; answered twice.
    Duplicate,
    /// Not applied until [`SimulatedObjectStore::land_late_effects`]; never
    /// answered. A read has no effect, so a late read only loses its answer.
    Late,
}

/// How the store decides each request's fate.
#[derive(Debug, Clone)]
pub enum Schedule {
    /// Every request is applied and answered at once.
    Eager,
    /// Each request draws its fate from `fates`, by `seed` and its id.
    Seeded { seed: u64, fates: Vec<Fate> },
    /// Listed requests get their fate; the rest are eager.
    Scripted(BTreeMap<RequestId, Fate>),
}

impl Schedule {
    fn fate(&self, id: &RequestId) -> Fate {
        match self {
            Self::Eager => Fate::Normal,
            Self::Seeded { seed, fates } if !fates.is_empty() => {
                let index = digest(seed.to_le_bytes().iter().copied().chain(
                    format!("{:?}\0{}\0{}", id.operation, id.key, id.occurrence).into_bytes(),
                )) % fates.len() as u64;
                fates[usize::try_from(index).expect("an index fits usize")]
            }
            Self::Seeded { .. } => Fate::Normal,
            Self::Scripted(fates) => fates.get(id).copied().unwrap_or(Fate::Normal),
        }
    }
}

/// FNV-1a over `bytes`, finished with SplitMix64.
fn digest(bytes: impl IntoIterator<Item = u8>) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash = (hash ^ (hash >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    hash = (hash ^ (hash >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    hash ^ (hash >> 31)
}

/// The `ETag` of `bytes`: a digest of the content, as S3 reports for a single
/// upload.
fn etag(bytes: &[u8]) -> String {
    let low = digest(bytes.iter().copied());
    let high = digest(
        bytes
            .iter()
            .copied()
            .chain((bytes.len() as u64).to_le_bytes()),
    );
    format!("\"{high:016x}{low:016x}\"")
}

fn metadata(bytes: &[u8]) -> ObjectMetadata {
    ObjectMetadata {
        size: bytes.len() as u64,
        etag: etag(bytes),
        generation: None,
    }
}

/// Whether `stored` carries the identity a conditional header names.
fn identity_matches(stored: &[u8], expected: &str) -> bool {
    etag(stored).trim_matches('"') == expected.trim().trim_matches('"')
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(header, _)| header.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

/// One request's effect, evaluated when it lands.
enum Effect {
    Put {
        key: String,
        bytes: Vec<u8>,
        headers: Vec<(String, String)>,
    },
    Delete {
        key: String,
        headers: Vec<(String, String)>,
    },
    Get {
        key: String,
        with_metadata: bool,
    },
    Head {
        key: String,
    },
    List {
        prefix: String,
    },
    RangeRead {
        key: String,
        start: u64,
        end: Option<u64>,
        expected: Option<ObjectMetadata>,
    },
}

impl Effect {
    /// Apply this effect to `objects` and describe its answer.
    fn land(&self, objects: &mut BTreeMap<String, Vec<u8>>) -> CloudEvent {
        match self {
            Self::Put {
                key,
                bytes,
                headers,
            } => {
                let existing = objects.get(key);
                let result = if header(headers, "if-none-match") == Some("*") && existing.is_some()
                {
                    Err(CloudError::PreconditionFailed(format!("{key} exists")))
                } else if let Some(expected) = header(headers, "if-match") {
                    match existing {
                        Some(stored) if identity_matches(stored, expected) => Ok(()),
                        _ => Err(CloudError::PreconditionFailed(format!(
                            "{key} is not at {expected}"
                        ))),
                    }
                } else if header(headers, "x-goog-if-generation-match").is_some() {
                    Err(CloudError::PreconditionFailed(
                        "the simulated store reports no generations".into(),
                    ))
                } else {
                    Ok(())
                };
                if result.is_ok() {
                    objects.insert(key.clone(), bytes.clone());
                }
                CloudEvent::Put {
                    key: key.clone(),
                    result,
                }
            }
            Self::Delete { key, headers } => {
                let result = match (objects.get(key), header(headers, "if-match")) {
                    // Deleting an absent object succeeds, conditional or not.
                    (None, _) => Ok(()),
                    (Some(stored), Some(expected)) if !identity_matches(stored, expected) => Err(
                        CloudError::PreconditionFailed(format!("{key} is not at {expected}")),
                    ),
                    (Some(_), _) => {
                        objects.remove(key);
                        Ok(())
                    }
                };
                CloudEvent::Delete {
                    key: key.clone(),
                    result,
                }
            }
            Self::Get { key, with_metadata } => {
                let stored = objects
                    .get(key)
                    .cloned()
                    .ok_or_else(|| CloudError::NotFound(key.clone()));
                if *with_metadata {
                    CloudEvent::GetWithMetadata {
                        key: key.clone(),
                        result: stored.map(|bytes| {
                            let metadata = metadata(&bytes);
                            (bytes, metadata)
                        }),
                    }
                } else {
                    CloudEvent::Get {
                        key: key.clone(),
                        result: stored,
                    }
                }
            }
            Self::Head { key } => CloudEvent::Head {
                key: key.clone(),
                result: objects
                    .get(key)
                    .map(|bytes| metadata(bytes))
                    .ok_or_else(|| CloudError::NotFound(key.clone())),
            },
            Self::List { prefix } => CloudEvent::List {
                prefix: prefix.clone(),
                result: Ok(objects
                    .keys()
                    .filter(|key| key.starts_with(prefix.as_str()))
                    .cloned()
                    .collect()),
            },
            Self::RangeRead {
                key,
                start,
                end,
                expected,
            } => {
                let result = (|| {
                    let stored = objects
                        .get(key)
                        .ok_or_else(|| CloudError::NotFound(key.clone()))?;
                    if let Some(expected) = expected {
                        if !metadata(stored).same_version(expected) {
                            return Err(CloudError::PreconditionFailed(format!(
                                "{key} changed version"
                            )));
                        }
                    }
                    let start = usize::try_from(*start)
                        .map_err(|error| CloudError::Protocol(error.to_string()))?;
                    let end = end.map_or(Ok(stored.len()), |end| {
                        usize::try_from(end)
                            .map_err(|error| CloudError::Protocol(error.to_string()))
                    })?;
                    stored
                        .get(start..end)
                        .map(<[u8]>::to_vec)
                        .ok_or_else(|| CloudError::Protocol(format!("{key} is shorter than {end}")))
                })();
                CloudEvent::GetRange {
                    key: key.clone(),
                    start: *start,
                    end: *end,
                    result,
                }
            }
        }
    }
}

/// A server error answering `event`'s request, keeping its shape.
fn server_error(event: CloudEvent) -> CloudEvent {
    let error = || CloudError::ServerError("simulated provider failure".into());
    match event {
        CloudEvent::Put { key, .. } => CloudEvent::Put {
            key,
            result: Err(error()),
        },
        CloudEvent::Get { key, .. } => CloudEvent::Get {
            key,
            result: Err(error()),
        },
        CloudEvent::GetWithMetadata { key, .. } => CloudEvent::GetWithMetadata {
            key,
            result: Err(error()),
        },
        CloudEvent::GetRange {
            key, start, end, ..
        } => CloudEvent::GetRange {
            key,
            start,
            end,
            result: Err(error()),
        },
        CloudEvent::Delete { key, .. } => CloudEvent::Delete {
            key,
            result: Err(error()),
        },
        CloudEvent::List { prefix, .. } => CloudEvent::List {
            prefix,
            result: Err(error()),
        },
        CloudEvent::Head { key, .. } => CloudEvent::Head {
            key,
            result: Err(error()),
        },
    }
}

struct LateEffect {
    id: RequestId,
    effect: Effect,
    /// Held so the client waits for its own deadline rather than seeing a
    /// closed channel.
    callback: CloudCallback,
}

struct StoreState {
    objects: BTreeMap<String, Vec<u8>>,
    schedule: Schedule,
    occurrences: BTreeMap<(Operation, String), u64>,
    late: Vec<LateEffect>,
    /// Answers that never arrive, held open for the same reason.
    unanswered: Vec<CloudCallback>,
    trace: Vec<(RequestId, Fate)>,
}

/// A simulated object store; clones share one store.
#[derive(Clone)]
pub struct SimulatedObjectStore {
    state: Arc<Mutex<StoreState>>,
}

impl SimulatedObjectStore {
    #[must_use]
    pub fn new(schedule: Schedule) -> Self {
        Self {
            state: Arc::new(Mutex::new(StoreState {
                objects: BTreeMap::new(),
                schedule,
                occurrences: BTreeMap::new(),
                late: Vec::new(),
                unanswered: Vec::new(),
                trace: Vec::new(),
            })),
        }
    }

    /// A store that applies and answers every request at once.
    #[must_use]
    pub fn eager() -> Self {
        Self::new(Schedule::Eager)
    }

    fn lock(&self) -> MutexGuard<'_, StoreState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Decide the fate of later requests by `schedule`.
    pub fn set_schedule(&self, schedule: Schedule) {
        self.lock().schedule = schedule;
    }

    /// Apply every late effect, in request order, as requests that reached
    /// the provider after their clients gave up. Returns how many landed.
    pub fn land_late_effects(&self) -> usize {
        let mut state = self.lock();
        let mut late = std::mem::take(&mut state.late);
        late.sort_by(|left, right| left.id.cmp(&right.id));
        for effect in &late {
            effect.effect.land(&mut state.objects);
        }
        let landed = late.len();
        state
            .unanswered
            .extend(late.into_iter().map(|effect| effect.callback));
        landed
    }

    /// Every request so far with its fate, in arrival order.
    #[must_use]
    pub fn trace(&self) -> Vec<(RequestId, Fate)> {
        self.lock().trace.clone()
    }

    /// The bytes stored at `key`, as the provider names it.
    #[must_use]
    pub fn object(&self, key: &str) -> Option<Vec<u8>> {
        self.lock().objects.get(key).cloned()
    }

    /// Every stored key, as the provider names it.
    #[must_use]
    pub fn keys(&self) -> Vec<String> {
        self.lock().objects.keys().cloned().collect()
    }

    fn submit(&self, operation: Operation, key: &str, effect: Effect, callback: CloudCallback) {
        let mut state = self.lock();
        let occurrence = state
            .occurrences
            .entry((operation, key.to_string()))
            .or_default();
        let id = RequestId {
            operation,
            key: key.to_string(),
            occurrence: *occurrence,
        };
        *occurrence += 1;
        let mut fate = state.schedule.fate(&id);
        if !operation.mutates() {
            // A read has no effect to apply before or after its answer.
            fate = match fate {
                Fate::FailAfter => Fate::FailBefore,
                Fate::Late => Fate::LoseResponse,
                other => other,
            };
        }
        state.trace.push((id.clone(), fate));
        match fate {
            Fate::Late => state.late.push(LateEffect {
                id,
                effect,
                callback,
            }),
            Fate::FailBefore => {
                let answer = server_error(effect.land(&mut BTreeMap::new()));
                drop(state);
                let _ = callback.send(answer);
            }
            Fate::Normal | Fate::Duplicate => {
                let answer = effect.land(&mut state.objects);
                drop(state);
                if fate == Fate::Duplicate {
                    let _ = callback.send(answer.clone());
                }
                let _ = callback.send(answer);
            }
            Fate::FailAfter => {
                let answer = server_error(effect.land(&mut state.objects));
                drop(state);
                let _ = callback.send(answer);
            }
            Fate::LoseResponse => {
                effect.land(&mut state.objects);
                state.unanswered.push(callback);
            }
        }
    }
}

impl CloudBackend for SimulatedObjectStore {
    fn submit_get_range_with_identity(
        &self,
        key: &str,
        start: u64,
        end: u64,
        expected: ObjectMetadata,
        _timeout: std::time::Duration,
        callback: CloudCallback,
    ) {
        self.submit(
            Operation::RangeRead,
            key,
            Effect::RangeRead {
                key: key.to_string(),
                start,
                end: Some(end),
                expected: Some(expected),
            },
            callback,
        );
    }

    fn submit_put(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        self.submit(
            Operation::Put,
            key,
            Effect::Put {
                key: key.to_string(),
                bytes: data,
                headers,
            },
            callback,
        );
    }

    fn submit_get(&self, key: &str, callback: CloudCallback) {
        self.submit(
            Operation::Get,
            key,
            Effect::Get {
                key: key.to_string(),
                with_metadata: false,
            },
            callback,
        );
    }

    fn submit_get_with_metadata(&self, key: &str, callback: CloudCallback) {
        self.submit(
            Operation::Get,
            key,
            Effect::Get {
                key: key.to_string(),
                with_metadata: true,
            },
            callback,
        );
    }

    #[cfg(test)]
    fn submit_get_range(&self, key: &str, start: u64, end: Option<u64>, callback: CloudCallback) {
        self.submit(
            Operation::RangeRead,
            key,
            Effect::RangeRead {
                key: key.to_string(),
                start,
                end,
                expected: None,
            },
            callback,
        );
    }

    fn submit_delete(&self, key: &str, headers: Vec<(String, String)>, callback: CloudCallback) {
        self.submit(
            Operation::Delete,
            key,
            Effect::Delete {
                key: key.to_string(),
                headers,
            },
            callback,
        );
    }

    fn submit_list(&self, prefix: &str, callback: CloudCallback) {
        self.submit(
            Operation::List,
            prefix,
            Effect::List {
                prefix: prefix.to_string(),
            },
            callback,
        );
    }

    fn submit_head(&self, key: &str, callback: CloudCallback) {
        self.submit(
            Operation::Head,
            key,
            Effect::Head {
                key: key.to_string(),
            },
            callback,
        );
    }
}

#[cfg(test)]
mod tests;
