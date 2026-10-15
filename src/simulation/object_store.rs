//! A simulated object store with S3 semantics and an adversarial schedule
//! (#830), qualified against the reviews of the earlier draft (#810).
//!
//! It plugs into the engine as a [`CloudBackend`] through
//! `object_store_for_testing`, so the production `CloudStorage` adapter, its
//! deadlines, admission reservations and conditional headers all run against
//! it.
//!
//! - **Effects and answers are separate.** A schedule gives every request a
//!   [`Fate`]: applied and answered, failed before or after its effect with a
//!   chosen error class, or late. A late request is not applied until the
//!   test lands it, after its client was told it timed out, as a request that
//!   reached the provider applies whatever the client's deadline. Conditions
//!   are evaluated when an effect lands, never when it is submitted. Every
//!   request is answered, as the executor answers `Timeout` at a deadline, so
//!   admission reservations are always released.
//! - **Identity is S3's.** An `ETag` is a digest of the content, so deleting an
//!   object and putting identical bytes again repeats its identity, and a
//!   stale `If-Match` then succeeds against the successor. Conditional
//!   requests compare identities only, never sizes.
//! - **Replay is stable.** A request is named by its operation, its key and
//!   how many earlier requests of that operation named that key. Random
//!   generation names in keys are replaced by `*`, so names repeat across
//!   runs. Requests the engine issues to one key from several threads, such
//!   as timer-driven lease reads, are still numbered by arrival, so a seeded
//!   schedule can be scoped to key prefixes and leave them eager.
//!
//! - **A crashed process is cut off.** Each process uses its own view of the
//!   store ([`SimulatedObjectStore::process`]). Once a view's process
//!   crashes ([`SimulatedObjectStore::process_crash`]), requests through it
//!   fail at once without effect and without a request id, as a dead process
//!   sends nothing; other processes keep their views, and the dead one's
//!   late effects can still land (#835).
//!
//! Listing returns every key in one page; real providers page at about a
//! thousand keys.

use crate::storage::cloud::{CloudBackend, CloudCallback, CloudError, CloudEvent, ObjectMetadata};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

/// The kind of request a [`RequestId`] names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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

    fn name(self) -> &'static str {
        match self {
            Self::Put => "put",
            Self::Get => "get",
            Self::Head => "head",
            Self::Delete => "delete",
            Self::List => "list",
            Self::RangeRead => "range-read",
        }
    }
}

/// A stable name for one request: its operation, its key with random
/// generation names replaced by `*`, and how many earlier requests of that
/// operation named that key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RequestId {
    pub operation: Operation,
    pub key: String,
    pub occurrence: u64,
}

/// Whether `segment` is a random generation name (a hyphenated UUID).
fn is_generation_name(segment: &str) -> bool {
    let groups: Vec<&str> = segment.split('-').collect();
    groups.iter().map(|group| group.len()).eq([8, 4, 4, 4, 12])
        && groups
            .iter()
            .all(|group| group.bytes().all(|byte| byte.is_ascii_hexdigit()))
}

/// `key` with random generation names replaced by `*`.
fn stable_key(key: &str) -> String {
    key.split('/')
        .map(|segment| {
            if is_generation_name(segment) {
                "*"
            } else {
                segment
            }
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// The text every failed request reports, so a caller can tell a refusal
/// the store's faults caused from one the engine made on its own.
pub const FAILURE_MESSAGE: &str = "simulated provider failure";

/// The error class a failed request reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Failure {
    /// A retryable server failure (5xx).
    Server,
    /// The executor's deadline expired: the answer was lost.
    Timeout,
    /// The connection failed.
    Transport,
    /// A conditional mutation was definitely rejected by a conflict without
    /// its precondition being decided. Only a failure before the effect can
    /// report it; after the effect it is reported as a server failure.
    Conflict,
}

/// What happens to one request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Fate {
    /// Applied, and answered.
    Normal,
    /// Not applied; answered with the failure.
    FailBefore(Failure),
    /// Applied; answered with the failure anyway.
    FailAfter(Failure),
    /// Not applied until landed; answered with a timeout at once. A read has
    /// no effect, so a late read only times out.
    Late,
}

/// How the store decides each request's fate.
#[derive(Debug, Clone)]
pub enum Schedule {
    /// Every request is applied and answered at once.
    Eager,
    /// Requests whose key starts with one of `scope` and whose operation is
    /// one of `operations` (every key, or every operation, when empty) draw
    /// their fate from `fates` by `seed` and their id; the rest are eager.
    Seeded {
        seed: u64,
        fates: Vec<Fate>,
        scope: Vec<String>,
        operations: Vec<Operation>,
    },
    /// Listed requests get their fate; the rest are eager.
    Scripted(BTreeMap<RequestId, Fate>),
}

impl Schedule {
    fn fate(&self, id: &RequestId) -> Fate {
        match self {
            Self::Seeded {
                seed,
                fates,
                scope,
                operations,
            } if !fates.is_empty()
                && (scope.is_empty()
                    || scope
                        .iter()
                        .any(|prefix| id.key.starts_with(prefix.as_str())))
                && (operations.is_empty() || operations.contains(&id.operation)) =>
            {
                let key = format!("{}\0{}\0{}", id.operation.name(), id.key, id.occurrence);
                let index = digest(0, seed.to_le_bytes().into_iter().chain(key.into_bytes()))
                    % fates.len() as u64;
                fates[usize::try_from(index).expect("an index fits usize")]
            }
            Self::Scripted(fates) => fates.get(id).copied().unwrap_or(Fate::Normal),
            Self::Eager | Self::Seeded { .. } => Fate::Normal,
        }
    }
}

/// FNV-1a from `basis` over `bytes`, finished with `SplitMix64`.
fn digest(basis: u64, bytes: impl IntoIterator<Item = u8>) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325 ^ basis;
    for byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash = (hash ^ (hash >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    hash = (hash ^ (hash >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    hash ^ (hash >> 31)
}

/// The `ETag` of `bytes`: a 128-bit digest of the content, as S3 reports for a
/// single upload.
fn etag(bytes: &[u8]) -> String {
    let low = digest(0, bytes.iter().copied());
    let high = digest(0x9e37_79b9_7f4a_7c15, bytes.iter().copied());
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

type Objects = BTreeMap<String, Vec<u8>>;

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
    /// Whether this is a mutation guarded by a precondition.
    fn conditional(&self) -> bool {
        match self {
            Self::Put { headers, .. } | Self::Delete { headers, .. } => {
                header(headers, "if-match").is_some() || header(headers, "if-none-match").is_some()
            }
            _ => false,
        }
    }

    /// Apply this effect to `objects` and describe its answer.
    fn land(&self, objects: &mut Objects) -> CloudEvent {
        match self {
            Self::Put {
                key,
                bytes,
                headers,
            } => land_put(objects, key, bytes, headers),
            Self::Delete { key, headers } => land_delete(objects, key, headers),
            Self::Get { key, with_metadata } => land_get(objects, key, *with_metadata),
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
            } => CloudEvent::GetRange {
                key: key.clone(),
                start: *start,
                end: *end,
                result: read_range(objects, key, *start, *end, expected.as_ref()),
            },
        }
    }
}

fn land_put(
    objects: &mut Objects,
    key: &str,
    bytes: &[u8],
    headers: &[(String, String)],
) -> CloudEvent {
    let existing = objects.get(key);
    let result = if header(headers, "if-none-match") == Some("*") && existing.is_some() {
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
        objects.insert(key.to_string(), bytes.to_vec());
    }
    CloudEvent::Put {
        key: key.to_string(),
        result,
    }
}

fn land_delete(objects: &mut Objects, key: &str, headers: &[(String, String)]) -> CloudEvent {
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
        key: key.to_string(),
        result,
    }
}

fn land_get(objects: &Objects, key: &str, with_metadata: bool) -> CloudEvent {
    let stored = objects
        .get(key)
        .cloned()
        .ok_or_else(|| CloudError::NotFound(key.to_string()));
    if with_metadata {
        CloudEvent::GetWithMetadata {
            key: key.to_string(),
            result: stored.map(|bytes| {
                let metadata = metadata(&bytes);
                (bytes, metadata)
            }),
        }
    } else {
        CloudEvent::Get {
            key: key.to_string(),
            result: stored,
        }
    }
}

fn read_range(
    objects: &Objects,
    key: &str,
    start: u64,
    end: Option<u64>,
    expected: Option<&ObjectMetadata>,
) -> Result<Vec<u8>, CloudError> {
    let stored = objects
        .get(key)
        .ok_or_else(|| CloudError::NotFound(key.to_string()))?;
    if expected.is_some_and(|expected| !metadata(stored).same_version(expected)) {
        return Err(CloudError::PreconditionFailed(format!(
            "{key} changed version"
        )));
    }
    let start = usize::try_from(start).map_err(|error| CloudError::Protocol(error.to_string()))?;
    let end = end.map_or(Ok(stored.len()), |end| {
        usize::try_from(end).map_err(|error| CloudError::Protocol(error.to_string()))
    })?;
    stored
        .get(start..end)
        .map(<[u8]>::to_vec)
        .ok_or_else(|| CloudError::Protocol(format!("{key} is shorter than {end}")))
}

/// `failure` reported in the shape of `event`'s answer.
fn failed(event: CloudEvent, failure: Failure) -> CloudEvent {
    let error = || {
        let message = FAILURE_MESSAGE.to_string();
        match failure {
            Failure::Server => CloudError::ServerError(message),
            Failure::Timeout => CloudError::Timeout(message),
            Failure::Transport => CloudError::Transport(message),
            Failure::Conflict => CloudError::ConditionalConflict(message),
        }
    };
    match event {
        CloudEvent::PutReceipt { key, .. } => CloudEvent::PutReceipt {
            key,
            result: Err(error()),
        },
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

struct StoreState {
    objects: Objects,
    schedule: Schedule,
    occurrences: BTreeMap<(Operation, String), u64>,
    /// Late effects not yet landed, in arrival order.
    late: Vec<(RequestId, Effect)>,
    trace: Vec<(RequestId, Fate)>,
}

/// What a store holds at one point, so a campaign can replay from it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreImage {
    /// Every stored object, as the provider names it.
    pub objects: BTreeMap<String, Vec<u8>>,
    /// How many requests each operation has made to each key, so request
    /// ids, and the fates a seeded schedule draws for them, continue.
    occurrences: Vec<((Operation, String), u64)>,
    /// Requests held back and not yet landed, in arrival order.
    late: Vec<(RequestId, HeldEffect)>,
}

/// A held-back request, as an image keeps it. Only mutations are held: a
/// late read only times out.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum HeldEffect {
    Put {
        key: String,
        bytes: Vec<u8>,
        headers: Vec<(String, String)>,
    },
    Delete {
        key: String,
        headers: Vec<(String, String)>,
    },
}

impl HeldEffect {
    fn held(effect: &Effect) -> Option<Self> {
        match effect {
            Effect::Put {
                key,
                bytes,
                headers,
            } => Some(Self::Put {
                key: key.clone(),
                bytes: bytes.clone(),
                headers: headers.clone(),
            }),
            Effect::Delete { key, headers } => Some(Self::Delete {
                key: key.clone(),
                headers: headers.clone(),
            }),
            _ => None,
        }
    }

    fn effect(&self) -> Effect {
        match self.clone() {
            Self::Put {
                key,
                bytes,
                headers,
            } => Effect::Put {
                key,
                bytes,
                headers,
            },
            Self::Delete { key, headers } => Effect::Delete { key, headers },
        }
    }
}

/// A simulated object store; clones share one store and one process view.
#[derive(Clone)]
pub struct SimulatedObjectStore {
    state: Arc<Mutex<StoreState>>,
    /// Whether the process using this view is still running.
    alive: Arc<AtomicBool>,
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
                trace: Vec::new(),
            })),
            alive: Arc::new(AtomicBool::new(true)),
        }
    }

    /// A view of the store for a new process.
    #[must_use]
    pub fn process(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            alive: Arc::new(AtomicBool::new(true)),
        }
    }

    /// The process using this view crashes: later requests through this view
    /// and its clones fail with a transport error, without effect. Other
    /// views are unaffected.
    pub fn process_crash(&self) {
        self.alive.store(false, Ordering::SeqCst);
    }

    /// A store that applies and answers every request at once.
    #[must_use]
    pub fn eager() -> Self {
        Self::new(Schedule::Eager)
    }

    /// A store holding what `image` captured, deciding later fates by
    /// `schedule`.
    #[must_use]
    pub fn from_image(schedule: Schedule, image: &StoreImage) -> Self {
        let store = Self::new(schedule);
        {
            let mut state = store.lock();
            state.objects.clone_from(&image.objects);
            state.occurrences = image.occurrences.iter().cloned().collect();
            state.late = image
                .late
                .iter()
                .map(|(id, held)| (id.clone(), held.effect()))
                .collect();
        }
        store
    }

    /// What the store holds now.
    #[must_use]
    pub fn image(&self) -> StoreImage {
        let state = self.lock();
        StoreImage {
            objects: state.objects.clone(),
            occurrences: state
                .occurrences
                .iter()
                .map(|(id, count)| (id.clone(), *count))
                .collect(),
            late: state
                .late
                .iter()
                .filter_map(|(id, effect)| Some((id.clone(), HeldEffect::held(effect)?)))
                .collect(),
        }
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

    /// Late requests not yet landed, in arrival order.
    #[must_use]
    pub fn late_requests(&self) -> Vec<RequestId> {
        self.lock().late.iter().map(|(id, _)| id.clone()).collect()
    }

    /// Land one late request's effect, evaluating its condition now. Returns
    /// whether `id` was late and unlanded.
    #[must_use = "a test should prove the effect was late"]
    pub fn land_late(&self, id: &RequestId) -> bool {
        let mut state = self.lock();
        let Some(position) = state.late.iter().position(|(late, _)| late == id) else {
            return false;
        };
        let (_, effect) = state.late.remove(position);
        effect.land(&mut state.objects);
        true
    }

    /// Land every late effect in request order. Returns how many landed.
    #[must_use = "a test should prove late effects existed"]
    pub fn land_late_effects(&self) -> usize {
        let mut ids = self.late_requests();
        ids.sort();
        ids.iter().filter(|id| self.land_late(id)).count()
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

    fn submit(&self, operation: Operation, key: &str, effect: Effect, callback: &CloudCallback) {
        if !self.alive.load(Ordering::SeqCst) {
            let _ = callback.send(failed(effect.land(&mut Objects::new()), Failure::Transport));
            return;
        }
        let mut state = self.lock();
        let stable = stable_key(key);
        let occurrence = state
            .occurrences
            .entry((operation, stable.clone()))
            .or_default();
        let id = RequestId {
            operation,
            key: stable,
            occurrence: *occurrence,
        };
        *occurrence += 1;
        let mut fate = state.schedule.fate(&id);
        if !operation.mutates() {
            // A read has no effect to apply before or after its answer.
            fate = match fate {
                Fate::FailAfter(failure) => Fate::FailBefore(failure),
                Fate::Late => Fate::FailBefore(Failure::Timeout),
                other => other,
            };
        }
        if !effect.conditional() {
            fate = match fate {
                Fate::FailBefore(Failure::Conflict) => Fate::FailBefore(Failure::Server),
                other => other,
            };
        }
        if let Fate::FailAfter(Failure::Conflict) = fate {
            // A conflict reports that nothing was committed.
            fate = Fate::FailAfter(Failure::Server);
        }
        state.trace.push((id.clone(), fate));
        let answer = match fate {
            Fate::Normal => effect.land(&mut state.objects),
            Fate::FailBefore(failure) => failed(effect.land(&mut Objects::new()), failure),
            Fate::FailAfter(failure) => failed(effect.land(&mut state.objects), failure),
            Fate::Late => {
                let answer = failed(effect.land(&mut Objects::new()), Failure::Timeout);
                state.late.push((id, effect));
                answer
            }
        };
        drop(state);
        let _ = callback.send(answer);
    }
}

impl crate::storage::cloud::CloudReadCapability for SimulatedObjectStore {
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
            &callback,
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
            &callback,
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
            &callback,
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
            &callback,
        );
    }
}

impl crate::storage::cloud::CloudWriteCapability for SimulatedObjectStore {
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
            &callback,
        );
    }
}

impl crate::storage::cloud::CloudDeleteCapability for SimulatedObjectStore {
    fn submit_delete(&self, key: &str, headers: Vec<(String, String)>, callback: CloudCallback) {
        self.submit(
            Operation::Delete,
            key,
            Effect::Delete {
                key: key.to_string(),
                headers,
            },
            &callback,
        );
    }
}

impl crate::storage::cloud::CloudListCapability for SimulatedObjectStore {
    fn submit_list(&self, prefix: &str, callback: CloudCallback) {
        self.submit(
            Operation::List,
            prefix,
            Effect::List {
                prefix: prefix.to_string(),
            },
            &callback,
        );
    }
}

impl crate::storage::cloud::CloudHeadCapability for SimulatedObjectStore {
    fn submit_head(&self, key: &str, callback: CloudCallback) {
        self.submit(
            Operation::Head,
            key,
            Effect::Head {
                key: key.to_string(),
            },
            &callback,
        );
    }
}

impl CloudBackend for SimulatedObjectStore {}

#[cfg(test)]
mod tests;
