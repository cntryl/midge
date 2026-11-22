//! Local FORMAT 10 immutable metadata objects selected by one durable head.
//!
//! This is the common publication boundary for manifest and intent history.
//! Callers hold their existing logical validation and locking; this module
//! owns canonical object naming, bounded wire decoding, and selector order.

use crate::common::{MidgeError, MidgeResult};
use crate::io::staging;
use crate::io::{Durability, Fs, FsError, FsPath, OpenMode, OpenOptions};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use std::sync::Arc;

pub(crate) const HEAD_PATH: &str = "metadata/HEAD";
const HEAD_TEMP: &str = "metadata/HEAD.tmp";
const HEAD_MAGIC: &[u8; 8] = b"MDGEHD10";
const OBJECT_MAGIC: &[u8; 8] = b"MDGEOB10";
const WIRE_VERSION: u16 = 10;
pub(crate) const MAX_HEAD_BYTES: usize = 64 * 1024;
pub(crate) const MAX_OBJECTS: usize = 16;
pub(crate) const MAX_CLOSURE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_OBJECT_BYTES: u64 = MAX_CLOSURE_BYTES;
type StreamSegments = Vec<(u64, u64, Vec<u8>)>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Stream {
    Manifest,
    Intent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObjectRef {
    name: String,
    head_sequence: u64,
    len: u64,
    crc32c: u32,
    sha256: [u8; 32],
    predecessor: [u8; 32],
    first_id: u64,
    last_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StreamHead {
    base: ObjectRef,
    segments: Vec<ObjectRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingObject {
    stream: Stream,
    object: ObjectRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HeadBody {
    version: u16,
    sequence: u64,
    generation: String,
    previous_generation: Option<String>,
    source_digest: Option<[u8; 32]>,
    predecessor_head: [u8; 32],
    manifest: StreamHead,
    intent: StreamHead,
    pending_objects: Vec<PendingObject>,
    manifest_base_edit_id: u64,
    intent_base_sequence: u64,
    intent_base_checksum: u32,
    highest_manifest_id: u64,
    highest_intent_sequence: u64,
}

/// Validated selected v10 metadata closure for one local database.
pub(crate) struct LocalMetadataAuthority {
    fs: Arc<dyn Fs>,
    head: HeadBody,
    head_digest: [u8; 32],
    fenced: bool,
}

pub(crate) struct CloudMetadataStream<'a> {
    pub(crate) checkpoint: u64,
    pub(crate) base: &'a [u8],
    pub(crate) segments: Vec<(u64, u64, &'a [u8])>,
}

impl LocalMetadataAuthority {
    /// Verify the exact committed cloud closure before using any of its bytes.
    pub(crate) fn validate_cloud_bodies(
        bodies: &std::collections::BTreeMap<String, Vec<u8>>,
    ) -> MidgeResult<u64> {
        Self::verified_cloud_head(bodies).map(|head| head.highest_manifest_id)
    }

    pub(crate) fn read_cloud_stream(
        bodies: &std::collections::BTreeMap<String, Vec<u8>>,
        stream: Stream,
    ) -> MidgeResult<CloudMetadataStream<'_>> {
        let head = Self::verified_cloud_head(bodies)?;
        let selected = stream_head(&head, stream);
        let payload = |object: &ObjectRef| {
            bodies
                .get(&object_path(&head.generation, &object.name))
                .map(|bytes| &bytes[62..bytes.len() - 4])
                .ok_or_else(|| {
                    MidgeError::Corruption("verified cloud metadata body is missing".into())
                })
        };
        let base = payload(&selected.base)?;
        let segments = selected
            .segments
            .iter()
            .map(|object| Ok((object.first_id, object.last_id, payload(object)?)))
            .collect::<MidgeResult<Vec<_>>>()?;
        Ok(CloudMetadataStream {
            checkpoint: match stream {
                Stream::Manifest => head.manifest_base_edit_id,
                Stream::Intent => head.intent_base_sequence,
            },
            base,
            segments,
        })
    }

    fn verified_cloud_head(
        bodies: &std::collections::BTreeMap<String, Vec<u8>>,
    ) -> MidgeResult<HeadBody> {
        if bodies.get(super::files::FORMAT).map(Vec::as_slice) != Some(b"midge-format-version=10\n")
        {
            return Err(MidgeError::CompatibilityError(
                "cloud FORMAT 10 closure has an invalid format marker".into(),
            ));
        }
        let encoded_head = bodies
            .get(HEAD_PATH)
            .ok_or_else(|| MidgeError::Corruption("cloud FORMAT 10 HEAD is missing".into()))?;
        let (head, _) = decode_head(encoded_head)?;
        validate_head(&head)?;
        let expected_len = 4 + head.manifest.segments.len() + head.intent.segments.len();
        if bodies.len() != expected_len {
            return Err(MidgeError::Corruption(
                "cloud FORMAT 10 descriptor differs from the objects selected by HEAD".into(),
            ));
        }
        for (stream, selected) in [
            (Stream::Manifest, &head.manifest),
            (Stream::Intent, &head.intent),
        ] {
            for object in std::iter::once(&selected.base).chain(&selected.segments) {
                let path = object_path(&head.generation, &object.name);
                let bytes = bodies.get(&path).ok_or_else(|| {
                    MidgeError::Corruption(format!("cloud FORMAT 10 object '{path}' is missing"))
                })?;
                verify_object_bytes(stream, object, bytes)?;
            }
        }
        Ok(head)
    }

    /// Validate a selected local head without retaining an open writer owner.
    pub(crate) fn validate(fs: &dyn Fs) -> MidgeResult<()> {
        load_selected_head(fs).map(|_| ())
    }

    /// Load the selected head and verify every selected immutable object.
    pub(crate) fn open(fs: Arc<dyn Fs>) -> MidgeResult<Self> {
        let (head, head_digest) = load_selected_head(fs.as_ref())?;
        Ok(Self {
            fs,
            head,
            head_digest,
            fenced: false,
        })
    }

    /// Publish the initial empty-history bases and select them with one head.
    #[cfg(test)]
    pub(crate) fn initialize(
        fs: Arc<dyn Fs>,
        generation: &str,
        manifest_base: &[u8],
        intent_base: &[u8],
    ) -> MidgeResult<Self> {
        Self::initialize_with_state(fs, generation, manifest_base, 0, intent_base, 0, 0)
    }

    #[cfg(test)]
    pub(crate) fn initialize_with_state(
        fs: Arc<dyn Fs>,
        generation: &str,
        manifest_base: &[u8],
        manifest_base_edit_id: u64,
        intent_base: &[u8],
        intent_base_sequence: u64,
        intent_base_checksum: u32,
    ) -> MidgeResult<Self> {
        validate_identifier(generation)?;
        if fs
            .exists(&FsPath::new(HEAD_PATH))
            .map_err(FsError::into_midge)?
        {
            return Err(MidgeError::CompatibilityError(
                "FORMAT 10 HEAD is already selected".into(),
            ));
        }
        fs.create_dir_all(&FsPath::new(format!(
            "metadata/generations/{generation}/manifest"
        )))
        .map_err(|error| recovery_error("create manifest generation", error))?;
        fs.create_dir_all(&FsPath::new(format!(
            "metadata/generations/{generation}/intent"
        )))
        .map_err(|error| recovery_error("create intent generation", error))?;
        sync_directory_chain(
            fs.as_ref(),
            &format!("metadata/generations/{generation}/manifest"),
        )?;
        sync_directory_chain(
            fs.as_ref(),
            &format!("metadata/generations/{generation}/intent"),
        )?;

        let manifest = write_base(fs.as_ref(), generation, Stream::Manifest, manifest_base)?;
        let intent = write_base(fs.as_ref(), generation, Stream::Intent, intent_base)?;
        let head = HeadBody {
            version: WIRE_VERSION,
            sequence: 1,
            generation: generation.to_owned(),
            previous_generation: None,
            source_digest: None,
            predecessor_head: [0; 32],
            manifest: StreamHead {
                base: manifest,
                segments: Vec::new(),
            },
            intent: StreamHead {
                base: intent,
                segments: Vec::new(),
            },
            pending_objects: Vec::new(),
            manifest_base_edit_id,
            intent_base_sequence,
            intent_base_checksum,
            highest_manifest_id: manifest_base_edit_id,
            highest_intent_sequence: intent_base_sequence,
        };
        publish_head(&fs, &head)?;
        Self::open(fs)
    }

    /// Stage or rebuild the digest-bound local migration candidate while FORMAT 9 remains selected.
    pub(crate) fn stage_upgrade_candidate(
        fs: Arc<dyn Fs>,
        source_digest: [u8; 32],
        manifest_base: &[u8],
        manifest_base_edit_id: u64,
        intent_base: &[u8],
        intent_base_sequence: u64,
        intent_base_checksum: u32,
    ) -> MidgeResult<Self> {
        if crate::metadata::format::validate_format_marker(fs.as_ref(), std::path::Path::new("."))?
            != crate::metadata::format::CURRENT_FORMAT_VERSION
        {
            return Err(MidgeError::CompatibilityError(
                "FORMAT 10 candidate staging requires a currently selected FORMAT 9 source".into(),
            ));
        }
        let generation = "upgrade-candidate";
        if fs
            .exists(&FsPath::new(HEAD_PATH))
            .map_err(FsError::into_midge)?
        {
            let bytes = read_bounded(
                fs.as_ref(),
                HEAD_PATH,
                MAX_HEAD_BYTES as u64,
                "FORMAT 10 head",
            )
            .map_err(|error| recovery_error("read existing FORMAT 10 upgrade candidate", error))?;
            let (existing, _) = decode_head(&bytes)?;
            validate_head(&existing)?;
            if existing.sequence != 1 || existing.generation != generation {
                return Err(MidgeError::CompatibilityError(
                    "refusing to replace a used FORMAT 10 generation while FORMAT 9 is selected"
                        .into(),
                ));
            }
        }
        let generation_path = FsPath::new(format!("metadata/generations/{generation}"));
        if fs.exists(&generation_path).map_err(FsError::into_midge)? {
            fs.remove_dir_all(&generation_path).map_err(|error| {
                recovery_error("remove prior unselected FORMAT 10 upgrade candidate", error)
            })?;
            fs.sync_dir(&FsPath::new("metadata/generations"), Durability::Durable)
                .map_err(|error| {
                    recovery_error("sync prior FORMAT 10 upgrade candidate removal", error)
                })?;
        }
        for stream in [Stream::Manifest, Stream::Intent] {
            let path = format!(
                "metadata/generations/{generation}/{}",
                stream_directory(stream)
            );
            fs.create_dir_all(&FsPath::new(&path))
                .map_err(|error| recovery_error("create FORMAT 10 upgrade candidate", error))?;
            sync_directory_chain(fs.as_ref(), &path)?;
        }
        let manifest = write_base(fs.as_ref(), generation, Stream::Manifest, manifest_base)?;
        let intent = write_base(fs.as_ref(), generation, Stream::Intent, intent_base)?;
        let head = HeadBody {
            version: WIRE_VERSION,
            sequence: 1,
            generation: generation.to_owned(),
            previous_generation: None,
            source_digest: Some(source_digest),
            predecessor_head: [0; 32],
            manifest: StreamHead {
                base: manifest,
                segments: Vec::new(),
            },
            intent: StreamHead {
                base: intent,
                segments: Vec::new(),
            },
            pending_objects: Vec::new(),
            manifest_base_edit_id,
            intent_base_sequence,
            intent_base_checksum,
            highest_manifest_id: manifest_base_edit_id,
            highest_intent_sequence: intent_base_sequence,
        };
        validate_head(&head)?;
        if crate::metadata::format::validate_format_marker(fs.as_ref(), std::path::Path::new("."))?
            != crate::metadata::format::CURRENT_FORMAT_VERSION
        {
            return Err(MidgeError::CompatibilityError(
                "FORMAT 9 selection changed while staging FORMAT 10 candidate".into(),
            ));
        }
        publish_head(&fs, &head)?;
        Self::open(fs)
    }

    /// Append one immutable logical operation and select it by replacing HEAD.
    /// The old head remains authoritative if any step before the replacement fails.
    pub(crate) fn append(
        &mut self,
        stream: Stream,
        first_id: u64,
        last_id: u64,
        payload: &[u8],
    ) -> MidgeResult<()> {
        if self.fenced {
            return Err(MidgeError::RecoveryFailed(
                "FORMAT 10 metadata authority is fenced after an unresolved HEAD publication"
                    .into(),
            ));
        }
        if first_id == 0 || first_id > last_id || first_id <= self.high_water(stream) {
            return Err(MidgeError::Corruption(
                "invalid FORMAT 10 segment identity range".into(),
            ));
        }
        self.prepare_for_write()?;
        let current = stream_head(&self.head, stream);
        let predecessor = current
            .segments
            .last()
            .map_or(current.base.sha256, |item| item.sha256);
        let pending_sequence = self.next_head_sequence()?;
        let object_sequence = pending_sequence
            .checked_add(1)
            .ok_or_else(|| MidgeError::ResourceLimit("FORMAT 10 head sequence exhausted".into()))?;
        let segment_name = segment_name(object_sequence, first_id, last_id);
        let next = ObjectRef {
            name: format!("{}/{}", stream_directory(stream), segment_name),
            head_sequence: object_sequence,
            len: 0,
            crc32c: 0,
            sha256: [0; 32],
            predecessor,
            first_id,
            last_id,
        };
        let encoded = encode_object(stream, predecessor, first_id, last_id, payload)?;
        let next = complete_ref(next, &encoded);
        let mut pending_head = self.head.clone();
        pending_head.pending_objects.push(PendingObject {
            stream,
            object: next.clone(),
        });
        pending_head.predecessor_head = self.head_digest();
        pending_head.sequence = pending_sequence;
        self.publish_candidate(&pending_head)?;

        let mut candidate = self.head.clone();
        let selected = stream_head_mut(&mut candidate, stream);
        selected.segments.push(next.clone());
        candidate.pending_objects.clear();
        match stream {
            Stream::Manifest => candidate.highest_manifest_id = last_id,
            Stream::Intent => candidate.highest_intent_sequence = last_id,
        }
        candidate.predecessor_head = self.head_digest();
        candidate.sequence = object_sequence;
        validate_head(&candidate)?;

        let path = object_path(&candidate.generation, &next.name);
        write_immutable(self.fs.as_ref(), &path, &encoded)?;
        sync_directory_chain(self.fs.as_ref(), &parent_path(&path))?;
        verify_object(
            self.fs.as_ref(),
            &candidate.generation,
            stream,
            &next,
            false,
        )?;
        crate::failpoints::fail_point!("midge::format10::after_object_sync", |_| {
            Err(MidgeError::Internal(
                "failpoint after immutable object sync".into(),
            ))
        });

        self.publish_candidate(&candidate)
    }

    pub(crate) fn prepare_for_write(&mut self) -> MidgeResult<()> {
        if self.fenced {
            return Err(MidgeError::RecoveryFailed(
                "FORMAT 10 metadata authority is fenced after an unresolved HEAD publication"
                    .into(),
            ));
        }
        self.cleanup_previous_generation()?;
        self.cleanup_pending_objects()?;
        self.cleanup_unselected_generation(self.next_head_sequence()?)
    }

    /// Replace the manifest base while retaining the selected intent stream.
    pub(crate) fn checkpoint_manifest(
        &mut self,
        manifest_base: &[u8],
        checkpoint_edit_id: u64,
    ) -> MidgeResult<()> {
        self.prepare_for_write()?;
        if checkpoint_edit_id > self.head.highest_manifest_id.saturating_add(1) {
            return Err(MidgeError::Corruption(
                "FORMAT 10 manifest checkpoint is ahead of selected history".into(),
            ));
        }
        let sequence = self.next_head_sequence()?;
        let generation = generation_name(sequence);
        self.cleanup_previous_generation()?;
        let encoded_base = encode_object(Stream::Manifest, [0; 32], 0, 0, manifest_base)?;
        preflight_replacement(&self.head.intent, encoded_base.len())?;
        self.create_generation(&generation)?;
        let manifest = write_base(
            self.fs.as_ref(),
            &generation,
            Stream::Manifest,
            manifest_base,
        )?;
        let intent = copy_stream_to_generation(
            self.fs.as_ref(),
            &self.head.generation,
            &generation,
            Stream::Intent,
            &self.head.intent,
        )?;
        crate::failpoints::fail_point!("midge::format10::after_checkpoint_objects_sync", |_| {
            Err(MidgeError::Internal(
                "failpoint after FORMAT 10 checkpoint object sync".into(),
            ))
        });
        let candidate = HeadBody {
            version: WIRE_VERSION,
            sequence,
            generation,
            previous_generation: Some(self.head.generation.clone()),
            source_digest: self.head.source_digest,
            predecessor_head: self.head_digest(),
            manifest: StreamHead {
                base: manifest,
                segments: Vec::new(),
            },
            intent,
            pending_objects: Vec::new(),
            manifest_base_edit_id: checkpoint_edit_id,
            intent_base_sequence: self.head.intent_base_sequence,
            intent_base_checksum: self.head.intent_base_checksum,
            highest_manifest_id: checkpoint_edit_id,
            highest_intent_sequence: self.head.highest_intent_sequence,
        };
        self.publish_candidate(&candidate)
    }

    /// Replace the intent chain with a new complete base while retaining manifest history.
    pub(crate) fn compact_intent(
        &mut self,
        intent_base: &[u8],
        sequence: u64,
        checksum: u32,
    ) -> MidgeResult<()> {
        self.prepare_for_write()?;
        if sequence < self.head.highest_intent_sequence {
            return Err(MidgeError::Corruption(
                "FORMAT 10 intent compaction regresses its sequence".into(),
            ));
        }
        let sequence = self.next_head_sequence()?;
        let generation = generation_name(sequence);
        self.cleanup_previous_generation()?;
        let encoded_base = encode_object(Stream::Intent, [0; 32], 0, 0, intent_base)?;
        preflight_replacement(&self.head.manifest, encoded_base.len())?;
        self.create_generation(&generation)?;
        let manifest = copy_stream_to_generation(
            self.fs.as_ref(),
            &self.head.generation,
            &generation,
            Stream::Manifest,
            &self.head.manifest,
        )?;
        let intent = write_base(self.fs.as_ref(), &generation, Stream::Intent, intent_base)?;
        crate::failpoints::fail_point!("midge::format10::after_checkpoint_objects_sync", |_| {
            Err(MidgeError::Internal(
                "failpoint after FORMAT 10 intent compaction object sync".into(),
            ))
        });
        let candidate = HeadBody {
            version: WIRE_VERSION,
            sequence,
            generation,
            previous_generation: Some(self.head.generation.clone()),
            source_digest: self.head.source_digest,
            predecessor_head: self.head_digest(),
            manifest,
            intent: StreamHead {
                base: intent,
                segments: Vec::new(),
            },
            pending_objects: Vec::new(),
            manifest_base_edit_id: self.head.manifest_base_edit_id,
            intent_base_sequence: sequence,
            intent_base_checksum: checksum,
            highest_manifest_id: self.head.highest_manifest_id,
            highest_intent_sequence: sequence,
        };
        self.publish_candidate(&candidate)
    }

    fn create_generation(&self, generation: &str) -> MidgeResult<()> {
        validate_identifier(generation)?;
        if generation == self.head.generation
            || self.head.previous_generation.as_deref() == Some(generation)
        {
            return Err(MidgeError::Corruption(
                "FORMAT 10 replacement generation collides with selected retention state".into(),
            ));
        }
        let root = FsPath::new(format!("metadata/generations/{generation}"));
        if self.fs.exists(&root).map_err(FsError::into_midge)? {
            self.fs.remove_dir_all(&root).map_err(|error| {
                recovery_error("remove unselected FORMAT 10 staging generation", error)
            })?;
            self.fs
                .sync_dir(&FsPath::new("metadata/generations"), Durability::Durable)
                .map_err(|error| {
                    recovery_error("sync removed FORMAT 10 staging generation", error)
                })?;
        }
        for stream in [Stream::Manifest, Stream::Intent] {
            let path = format!(
                "metadata/generations/{generation}/{}",
                stream_directory(stream)
            );
            self.fs
                .create_dir_all(&FsPath::new(&path))
                .map_err(|error| recovery_error("create replacement metadata generation", error))?;
            sync_directory_chain(self.fs.as_ref(), &path)?;
        }
        Ok(())
    }

    fn next_head_sequence(&self) -> MidgeResult<u64> {
        self.head
            .sequence
            .checked_add(1)
            .ok_or_else(|| MidgeError::ResourceLimit("FORMAT 10 head sequence exhausted".into()))
    }

    fn publish_candidate(&mut self, candidate: &HeadBody) -> MidgeResult<()> {
        validate_head(candidate)?;
        let bytes = encode_head(candidate)?;
        let prior_bytes = encode_head(&self.head)?;
        if let Err(error) = staging::stage_bytes_typed(
            &self.fs,
            &FsPath::new(HEAD_TEMP),
            &FsPath::new(HEAD_PATH),
            &bytes,
            || Ok(()),
        ) {
            match read_bounded(
                self.fs.as_ref(),
                HEAD_PATH,
                MAX_HEAD_BYTES as u64,
                "FORMAT 10 head",
            ) {
                Ok(current) if current.as_ref() == bytes.as_slice() => {
                    if self
                        .fs
                        .sync_dir(&FsPath::new("metadata"), Durability::Durable)
                        .is_err()
                        || read_bounded(
                            self.fs.as_ref(),
                            HEAD_PATH,
                            MAX_HEAD_BYTES as u64,
                            "FORMAT 10 head",
                        )
                        .map_or(true, |again| again.as_ref() != bytes.as_slice())
                        || verify_head_objects(self.fs.as_ref(), candidate).is_err()
                    {
                        self.fenced = true;
                        return Err(MidgeError::RecoveryFailed(format!(
                            "FORMAT 10 HEAD publication outcome is unresolved: {error}"
                        )));
                    }
                }
                Ok(current) if current.as_ref() == prior_bytes.as_slice() => return Err(error),
                _ => {
                    self.fenced = true;
                    return Err(MidgeError::RecoveryFailed(format!(
                        "FORMAT 10 HEAD publication outcome is unresolved: {error}"
                    )));
                }
            }
        }
        let selected = match read_bounded(
            self.fs.as_ref(),
            HEAD_PATH,
            MAX_HEAD_BYTES as u64,
            "FORMAT 10 head",
        ) {
            Ok(selected) => selected,
            Err(error) => {
                self.fenced = true;
                return Err(MidgeError::RecoveryFailed(format!(
                    "FORMAT 10 HEAD readback after publication is unresolved: {error}"
                )));
            }
        };
        if selected.as_ref() != bytes.as_slice() {
            self.fenced = true;
            return Err(MidgeError::RecoveryFailed(
                "FORMAT 10 HEAD changed after durable publication".into(),
            ));
        }
        let (head, digest) = match decode_head(&selected)
            .and_then(|(head, digest)| validate_head(&head).map(|()| (head, digest)))
            .and_then(|(head, digest)| {
                verify_head_objects(self.fs.as_ref(), &head).map(|()| (head, digest))
            }) {
            Ok(selected) => selected,
            Err(error) => {
                self.fenced = true;
                return Err(MidgeError::RecoveryFailed(format!(
                    "FORMAT 10 selected HEAD is invalid after publication: {error}"
                )));
            }
        };
        self.head = head;
        self.head_digest = digest;
        if let Err(error) = self.cleanup_previous_generation() {
            tracing::warn!(%error, "prior FORMAT 10 generation remains within retained closure budget");
        }
        Ok(())
    }

    fn cleanup_previous_generation(&mut self) -> MidgeResult<()> {
        let Some(previous) = self.head.previous_generation.as_deref() else {
            return Ok(());
        };
        if previous == self.head.generation {
            return Err(MidgeError::Corruption(
                "FORMAT 10 previous generation equals selected generation".into(),
            ));
        }
        let path = FsPath::new(format!("metadata/generations/{previous}"));
        if self.fs.exists(&path).map_err(FsError::into_midge)? {
            self.fs.remove_dir_all(&path).map_err(|error| {
                recovery_error("collect unpinned prior FORMAT 10 generation", error)
            })?;
            self.fs
                .sync_dir(&FsPath::new("metadata/generations"), Durability::Durable)
                .map_err(|error| {
                    recovery_error("sync prior FORMAT 10 generation collection", error)
                })?;
        }
        Ok(())
    }

    fn cleanup_pending_objects(&mut self) -> MidgeResult<()> {
        if self.head.pending_objects.is_empty() {
            return Ok(());
        }
        for pending in &self.head.pending_objects {
            let path = object_path(&self.head.generation, &pending.object.name);
            remove_file_if_present(
                self.fs.as_ref(),
                &path,
                "remove unselected FORMAT 10 pending object",
            )?;
            self.fs
                .sync_dir(&FsPath::new(parent_path(&path)), Durability::Durable)
                .map_err(|error| recovery_error("sync pending FORMAT 10 object removal", error))?;
        }
        let mut candidate = self.head.clone();
        candidate.pending_objects.clear();
        candidate.predecessor_head = self.head_digest();
        candidate.sequence = self.next_head_sequence()?;
        self.publish_candidate(&candidate)
    }

    fn cleanup_unselected_generation(&self, sequence: u64) -> MidgeResult<()> {
        let candidate = generation_name(sequence);
        if candidate == self.head.generation
            || self.head.previous_generation.as_deref() == Some(candidate.as_str())
        {
            return Err(MidgeError::Corruption(
                "FORMAT 10 next generation collides with selected retention state".into(),
            ));
        }
        let path = FsPath::new(format!("metadata/generations/{candidate}"));
        if self.fs.exists(&path).map_err(FsError::into_midge)? {
            self.fs.remove_dir_all(&path).map_err(|error| {
                recovery_error("remove unselected FORMAT 10 candidate generation", error)
            })?;
            self.fs
                .sync_dir(&FsPath::new("metadata/generations"), Durability::Durable)
                .map_err(|error| {
                    recovery_error("sync unselected FORMAT 10 candidate removal", error)
                })?;
        }
        Ok(())
    }

    pub(crate) fn high_water(&self, stream: Stream) -> u64 {
        match stream {
            Stream::Manifest => self.head.highest_manifest_id,
            Stream::Intent => self.head.highest_intent_sequence,
        }
    }

    pub(crate) fn append_would_exceed_limits(
        &self,
        stream: Stream,
        first_id: u64,
        last_id: u64,
        payload: &[u8],
    ) -> MidgeResult<bool> {
        let current = stream_head(&self.head, stream);
        let count = 2_usize
            .checked_add(self.head.manifest.segments.len())
            .and_then(|count| count.checked_add(self.head.intent.segments.len()))
            .and_then(|count| count.checked_add(self.head.pending_objects.len()))
            .ok_or_else(|| MidgeError::ResourceLimit("FORMAT 10 object count overflow".into()))?;
        if count >= MAX_OBJECTS {
            return Ok(true);
        }
        let predecessor = current
            .segments
            .last()
            .map_or(current.base.sha256, |object| object.sha256);
        let encoded = encode_object(stream, predecessor, first_id, last_id, payload)?;
        let existing = stream_bytes(&self.head);
        let candidate = existing
            .checked_add(u64::try_from(encoded.len()).unwrap_or(u64::MAX))
            .ok_or_else(|| MidgeError::ResourceLimit("FORMAT 10 closure size overflow".into()))?;
        Ok(candidate > MAX_CLOSURE_BYTES)
    }

    pub(crate) fn base_id(&self, stream: Stream) -> u64 {
        match stream {
            Stream::Manifest => self.head.manifest_base_edit_id,
            Stream::Intent => self.head.intent_base_sequence,
        }
    }

    pub(crate) fn head_digest(&self) -> [u8; 32] {
        self.head_digest
    }

    /// Names of the immutable objects selected by HEAD, plus HEAD itself.
    pub(crate) fn selected_relative_paths(&self) -> Vec<String> {
        relative_paths(&self.head)
    }

    /// Verify and enumerate the exact selected names through a borrowed root.
    /// Callers capturing those bodies hold their writer lock through capture.
    pub(crate) fn selected_relative_paths_from(fs: &dyn Fs) -> MidgeResult<Vec<String>> {
        let (head, _) = load_selected_head(fs)?;
        Ok(relative_paths(&head))
    }

    /// Return the selected base payload and each selected logical append.
    pub(crate) fn read_stream(&self, stream: Stream) -> MidgeResult<(Vec<u8>, StreamSegments)> {
        let selected = stream_head(&self.head, stream);
        let base = read_object_payload(
            self.fs.as_ref(),
            &self.head.generation,
            stream,
            &selected.base,
            true,
        )?;
        let mut segments = Vec::with_capacity(selected.segments.len());
        for object in &selected.segments {
            segments.push((
                object.first_id,
                object.last_id,
                read_object_payload(
                    self.fs.as_ref(),
                    &self.head.generation,
                    stream,
                    object,
                    false,
                )?,
            ));
        }
        Ok((base, segments))
    }

    #[cfg(test)]
    fn selected_ids(&self, stream: Stream) -> Vec<(u64, u64)> {
        stream_head(&self.head, stream)
            .segments
            .iter()
            .map(|item| (item.first_id, item.last_id))
            .collect()
    }
}

fn stream_head(head: &HeadBody, stream: Stream) -> &StreamHead {
    match stream {
        Stream::Manifest => &head.manifest,
        Stream::Intent => &head.intent,
    }
}

fn stream_head_mut(head: &mut HeadBody, stream: Stream) -> &mut StreamHead {
    match stream {
        Stream::Manifest => &mut head.manifest,
        Stream::Intent => &mut head.intent,
    }
}

fn stream_bytes(head: &HeadBody) -> u64 {
    [&head.manifest, &head.intent]
        .into_iter()
        .flat_map(|stream| std::iter::once(&stream.base).chain(stream.segments.iter()))
        .fold(0_u64, |total, object| total.saturating_add(object.len))
}

fn stream_directory(stream: Stream) -> &'static str {
    match stream {
        Stream::Manifest => "manifest",
        Stream::Intent => "intent",
    }
}

fn segment_name(sequence: u64, first: u64, last: u64) -> String {
    format!("{sequence:020}-{first:020}-{last:020}.seg")
}

fn generation_name(sequence: u64) -> String {
    format!("g-{sequence:020}")
}

fn validate_identifier(identifier: &str) -> MidgeResult<()> {
    if identifier.is_empty()
        || identifier.len() > 64
        || !identifier
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-')
    {
        return Err(MidgeError::Corruption(
            "invalid FORMAT 10 generation identifier".into(),
        ));
    }
    Ok(())
}

/// Validate the path of an immutable FORMAT 10 body in a remote descriptor.
/// The selected HEAD additionally checks the exact referenced object set.
pub(crate) fn closure_path_parts(path: &str) -> MidgeResult<(&str, &str)> {
    let invalid = || MidgeError::Corruption("invalid FORMAT 10 metadata body path".into());
    let (generation, body) = path
        .strip_prefix("metadata/generations/")
        .and_then(|path| path.split_once('/'))
        .ok_or_else(invalid)?;
    validate_identifier(generation)?;
    if matches!(body, "manifest/base" | "intent/base") {
        return Ok((generation, body));
    }
    let (stream, segment) = body.split_once('/').ok_or_else(invalid)?;
    let segment = segment.strip_suffix(".seg").ok_or_else(invalid)?;
    if !matches!(stream, "manifest" | "intent")
        || segment.split('-').count() != 3
        || !segment.split('-').all(|number| {
            number.len() == 20
                && number.bytes().all(|byte| byte.is_ascii_digit())
                && number.parse::<u64>().is_ok_and(|number| number != 0)
        })
    {
        return Err(invalid());
    }
    Ok((generation, body))
}

fn object_path(generation: &str, relative: &str) -> String {
    format!("metadata/generations/{generation}/{relative}")
}

fn relative_paths(head: &HeadBody) -> Vec<String> {
    let mut paths = vec![HEAD_PATH.to_owned()];
    for selected in [&head.manifest, &head.intent] {
        paths.push(object_path(&head.generation, &selected.base.name));
        paths.extend(
            selected
                .segments
                .iter()
                .map(|object| object_path(&head.generation, &object.name)),
        );
    }
    paths
}

fn load_selected_head(fs: &dyn Fs) -> MidgeResult<(HeadBody, [u8; 32])> {
    let bytes = read_bounded(fs, HEAD_PATH, MAX_HEAD_BYTES as u64, "FORMAT 10 head")
        .map_err(|error| recovery_error("read FORMAT 10 head", error))?;
    let (head, digest) = decode_head(&bytes)?;
    validate_head(&head)?;
    verify_stream(fs, &head.generation, Stream::Manifest, &head.manifest)?;
    verify_stream(fs, &head.generation, Stream::Intent, &head.intent)?;
    Ok((head, digest))
}

fn parent_path(path: &str) -> String {
    path.rsplit_once('/')
        .map_or_else(|| ".".to_owned(), |(parent, _)| parent.to_owned())
}

fn write_base(
    fs: &dyn Fs,
    generation: &str,
    stream: Stream,
    payload: &[u8],
) -> MidgeResult<ObjectRef> {
    let encoded = encode_object(stream, [0; 32], 0, 0, payload)?;
    let name = format!("{}/base", stream_directory(stream));
    let path = object_path(generation, &name);
    write_immutable(fs, &path, &encoded)?;
    sync_directory_chain(fs, &parent_path(&path))?;
    let reference = complete_ref(
        ObjectRef {
            name,
            head_sequence: 0,
            len: 0,
            crc32c: 0,
            sha256: [0; 32],
            predecessor: [0; 32],
            first_id: 0,
            last_id: 0,
        },
        &encoded,
    );
    verify_object(fs, generation, stream, &reference, true)?;
    Ok(reference)
}

pub(crate) fn sync_directory_chain(fs: &dyn Fs, directory: &str) -> MidgeResult<()> {
    let mut path = std::path::PathBuf::new();
    let mut components = Vec::new();
    for component in std::path::Path::new(directory).components() {
        let std::path::Component::Normal(component) = component else {
            return Err(MidgeError::Corruption(
                "non-canonical FORMAT 10 directory path".into(),
            ));
        };
        path.push(component);
        components.push(path.to_string_lossy().into_owned());
    }
    for directory in components {
        fs.sync_dir(&FsPath::new(directory), Durability::Durable)
            .map_err(|error| recovery_error("sync FORMAT 10 directory chain", error))?;
    }
    Ok(())
}

fn read_bounded(
    fs: &dyn Fs,
    path: &str,
    maximum: u64,
    context: &str,
) -> Result<bytes::Bytes, FsError> {
    let path = FsPath::new(path);
    let metadata = fs.metadata(&path)?;
    if metadata.is_dir || metadata.len > maximum {
        return Err(FsError::Corruption(format!(
            "{context} length {} exceeds bound {maximum}",
            metadata.len
        )));
    }
    let file = fs.open_read(&path)?;
    let length = file.len()?;
    if length > maximum || length != metadata.len {
        return Err(FsError::Corruption(format!(
            "{context} length changed or exceeds bound"
        )));
    }
    file.read_at(0, length)
}

fn write_immutable(fs: &dyn Fs, path: &str, bytes: &[u8]) -> MidgeResult<()> {
    let temp = format!("{path}.tmp");
    remove_file_if_present(fs, &temp, "stale immutable metadata staging object")?;
    remove_file_if_present(fs, path, "unselected immutable metadata object")?;
    let file = fs
        .open(
            &FsPath::new(&temp),
            OpenOptions {
                mode: OpenMode::ReadWrite,
                create: true,
                create_new: true,
                truncate: false,
            },
        )
        .map_err(|error| recovery_error("create immutable metadata staging object", error))?;
    let mut file = file;
    file.write_at(0, bytes::Bytes::copy_from_slice(bytes))
        .map_err(|error| recovery_error("write immutable metadata object", error))?;
    file.sync(Durability::Durable)
        .map_err(|error| recovery_error("sync immutable metadata object", error))?;
    drop(file);
    fs.rename_atomic(&FsPath::new(&temp), &FsPath::new(path))
        .map_err(|error| recovery_error("publish immutable metadata object", error))?;
    Ok(())
}

fn remove_file_if_present(fs: &dyn Fs, path: &str, context: &str) -> MidgeResult<()> {
    let path = FsPath::new(path);
    if fs.exists(&path).map_err(FsError::into_midge)? {
        fs.remove_file(&path)
            .map_err(|error| recovery_error(context, error))?;
    }
    Ok(())
}

fn encode_object(
    stream: Stream,
    predecessor: [u8; 32],
    first_id: u64,
    last_id: u64,
    payload: &[u8],
) -> MidgeResult<Vec<u8>> {
    let payload_len = u32::try_from(payload.len()).map_err(|_| {
        MidgeError::ResourceLimit("FORMAT 10 object payload exceeds wire bound".into())
    })?;
    let total = 8_usize
        .checked_add(1 + 1 + 8 + 8 + 32 + 4 + payload.len() + 4)
        .ok_or_else(|| MidgeError::ResourceLimit("FORMAT 10 object size overflow".into()))?;
    if u64::try_from(total).unwrap_or(u64::MAX) > MAX_OBJECT_BYTES {
        return Err(MidgeError::ResourceLimit(
            "FORMAT 10 object exceeds 64 MiB".into(),
        ));
    }
    let mut bytes = Vec::with_capacity(total);
    bytes.extend_from_slice(OBJECT_MAGIC);
    bytes.push(match stream {
        Stream::Manifest => 1,
        Stream::Intent => 2,
    });
    bytes.push(0);
    bytes.extend_from_slice(&first_id.to_be_bytes());
    bytes.extend_from_slice(&last_id.to_be_bytes());
    bytes.extend_from_slice(&predecessor);
    bytes.extend_from_slice(&payload_len.to_be_bytes());
    bytes.extend_from_slice(payload);
    let checksum = crc32c::crc32c(&bytes);
    bytes.extend_from_slice(&checksum.to_be_bytes());
    Ok(bytes)
}

fn complete_ref(mut reference: ObjectRef, bytes: &[u8]) -> ObjectRef {
    reference.len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    reference.crc32c = crc32c::crc32c(bytes);
    reference.sha256 = sha2::Sha256::digest(bytes).into();
    reference
}

fn encode_head(head: &HeadBody) -> MidgeResult<Vec<u8>> {
    let body = serde_json::to_vec(head)
        .map_err(|error| MidgeError::Internal(format!("encode FORMAT 10 head: {error}")))?;
    let body_len = u32::try_from(body.len())
        .map_err(|_| MidgeError::ResourceLimit("FORMAT 10 head exceeds wire bound".into()))?;
    let mut bytes = Vec::with_capacity(8 + 2 + 4 + body.len() + 4);
    bytes.extend_from_slice(HEAD_MAGIC);
    bytes.extend_from_slice(&WIRE_VERSION.to_be_bytes());
    bytes.extend_from_slice(&body_len.to_be_bytes());
    bytes.extend_from_slice(&body);
    let checksum = crc32c::crc32c(&bytes);
    bytes.extend_from_slice(&checksum.to_be_bytes());
    if bytes.len() > MAX_HEAD_BYTES {
        return Err(MidgeError::ResourceLimit(
            "FORMAT 10 head exceeds 64 KiB".into(),
        ));
    }
    Ok(bytes)
}

fn decode_head(bytes: &[u8]) -> MidgeResult<(HeadBody, [u8; 32])> {
    if bytes.len() < 18 || bytes.len() > MAX_HEAD_BYTES || &bytes[..8] != HEAD_MAGIC {
        return Err(MidgeError::Corruption(
            "invalid FORMAT 10 head framing".into(),
        ));
    }
    let version = u16::from_be_bytes([bytes[8], bytes[9]]);
    let len =
        usize::try_from(u32::from_be_bytes(bytes[10..14].try_into().map_err(
            |_| MidgeError::Corruption("invalid FORMAT 10 head length".into()),
        )?))
        .unwrap_or(usize::MAX);
    let expected = 14_usize
        .checked_add(len)
        .and_then(|value| value.checked_add(4))
        .ok_or_else(|| MidgeError::Corruption("FORMAT 10 head length overflow".into()))?;
    if version != WIRE_VERSION || expected != bytes.len() {
        return Err(MidgeError::Corruption(
            "unsupported or inconsistent FORMAT 10 head version/length".into(),
        ));
    }
    let checksum_at = bytes.len() - 4;
    let checksum = u32::from_be_bytes(
        bytes[checksum_at..]
            .try_into()
            .map_err(|_| MidgeError::Corruption("invalid FORMAT 10 head checksum".into()))?,
    );
    if crc32c::crc32c(&bytes[..checksum_at]) != checksum {
        return Err(MidgeError::Corruption(
            "FORMAT 10 head checksum mismatch".into(),
        ));
    }
    let head: HeadBody = serde_json::from_slice(&bytes[14..checksum_at])
        .map_err(|error| MidgeError::Corruption(format!("invalid FORMAT 10 head body: {error}")))?;
    Ok((head, sha2::Sha256::digest(bytes).into()))
}

fn validate_head(head: &HeadBody) -> MidgeResult<()> {
    if head.version != WIRE_VERSION || head.sequence == 0 {
        return Err(MidgeError::Corruption(
            "invalid FORMAT 10 head version or sequence".into(),
        ));
    }
    if (head.sequence == 1 && head.predecessor_head != [0; 32])
        || (head.sequence > 1 && head.predecessor_head == [0; 32])
    {
        return Err(MidgeError::Corruption(
            "FORMAT 10 head predecessor does not match its sequence".into(),
        ));
    }
    validate_identifier(&head.generation)?;
    if head.generation == "upgrade-candidate" && head.source_digest.is_none() {
        return Err(MidgeError::Corruption(
            "FORMAT 10 upgrade candidate is missing its source digest".into(),
        ));
    }
    validate_previous_generation(head)?;
    let object_count = 2_usize
        .checked_add(head.manifest.segments.len())
        .and_then(|n| n.checked_add(head.intent.segments.len()))
        .and_then(|n| n.checked_add(head.pending_objects.len()))
        .ok_or_else(|| MidgeError::ResourceLimit("FORMAT 10 object count overflow".into()))?;
    if object_count > MAX_OBJECTS {
        return Err(MidgeError::ResourceLimit(
            "FORMAT 10 closure exceeds 16 objects".into(),
        ));
    }
    let mut total = validate_selected_streams(head)?;
    total = validate_pending_objects(head, total)?;
    if total > MAX_CLOSURE_BYTES {
        return Err(MidgeError::ResourceLimit(
            "FORMAT 10 closure and pending object exceed 64 MiB".into(),
        ));
    }
    validate_high_water_marks(head)
}

fn validate_previous_generation(head: &HeadBody) -> MidgeResult<()> {
    if let Some(previous) = head.previous_generation.as_deref() {
        validate_identifier(previous)?;
        if previous == head.generation {
            return Err(MidgeError::Corruption(
                "FORMAT 10 previous generation equals selected generation".into(),
            ));
        }
    }
    Ok(())
}

fn validate_selected_streams(head: &HeadBody) -> MidgeResult<u64> {
    let mut total = 0_u64;
    for (stream, selected) in [
        (Stream::Manifest, &head.manifest),
        (Stream::Intent, &head.intent),
    ] {
        validate_ref(&head.generation, stream, &selected.base, true)?;
        total = total
            .checked_add(selected.base.len)
            .ok_or_else(|| MidgeError::ResourceLimit("FORMAT 10 closure size overflow".into()))?;
        let mut predecessor = selected.base.sha256;
        let mut last_id = match stream {
            Stream::Manifest => head.manifest_base_edit_id,
            Stream::Intent => head.intent_base_sequence,
        };
        let mut last_sequence = 0;
        for object in &selected.segments {
            validate_ref(&head.generation, stream, object, false)?;
            if object.predecessor != predecessor
                || object.first_id <= last_id
                || object.head_sequence <= last_sequence
                || object.head_sequence > head.sequence
            {
                return Err(MidgeError::Corruption(
                    "FORMAT 10 segment chain is inconsistent".into(),
                ));
            }
            predecessor = object.sha256;
            last_id = object.last_id;
            last_sequence = object.head_sequence;
            total = total.checked_add(object.len).ok_or_else(|| {
                MidgeError::ResourceLimit("FORMAT 10 closure size overflow".into())
            })?;
        }
    }
    Ok(total)
}

fn validate_pending_objects(head: &HeadBody, mut total: u64) -> MidgeResult<u64> {
    if head.pending_objects.len() > 1 {
        return Err(MidgeError::Corruption(
            "FORMAT 10 head contains multiple pending objects".into(),
        ));
    }
    for pending in &head.pending_objects {
        validate_ref(&head.generation, pending.stream, &pending.object, false)?;
        let expected_sequence = head
            .sequence
            .checked_add(1)
            .ok_or_else(|| MidgeError::ResourceLimit("FORMAT 10 head sequence exhausted".into()))?;
        let selected = stream_head(head, pending.stream);
        let predecessor = selected
            .segments
            .last()
            .map_or(selected.base.sha256, |object| object.sha256);
        let high_water = match pending.stream {
            Stream::Manifest => head.highest_manifest_id,
            Stream::Intent => head.highest_intent_sequence,
        };
        if pending.object.head_sequence != expected_sequence
            || pending.object.predecessor != predecessor
            || pending.object.first_id <= high_water
        {
            return Err(MidgeError::Corruption(
                "FORMAT 10 pending object does not follow selected stream history".into(),
            ));
        }
        total = total
            .checked_add(pending.object.len)
            .ok_or_else(|| MidgeError::ResourceLimit("FORMAT 10 closure size overflow".into()))?;
    }
    Ok(total)
}

fn validate_high_water_marks(head: &HeadBody) -> MidgeResult<()> {
    if head.highest_manifest_id
        != head
            .manifest
            .segments
            .last()
            .map_or(head.manifest_base_edit_id, |item| item.last_id)
        || head.highest_intent_sequence
            != head
                .intent
                .segments
                .last()
                .map_or(head.intent_base_sequence, |item| item.last_id)
    {
        return Err(MidgeError::Corruption(
            "FORMAT 10 head high-water mark disagrees with selected streams".into(),
        ));
    }
    Ok(())
}

fn validate_ref(
    generation: &str,
    stream: Stream,
    object: &ObjectRef,
    base: bool,
) -> MidgeResult<()> {
    let prefix = format!("{}/", stream_directory(stream));
    let canonical_segment = object.name.strip_prefix(&prefix).is_some_and(|name| {
        let mut parts = name.split('-');
        let sequence = parts.next().unwrap_or_default();
        let first = parts.next().unwrap_or_default();
        let tail = parts.next().unwrap_or_default();
        let last = tail.strip_suffix(".seg").unwrap_or_default();
        sequence == format!("{:020}", object.head_sequence)
            && object.head_sequence > 0
            && first == format!("{:020}", object.first_id)
            && last == format!("{:020}", object.last_id)
            && parts.next().is_none()
    });
    let expected_base = format!("{}/base", stream_directory(stream));
    if (base && object.name != expected_base)
        || (!base && !canonical_segment)
        || object.len == 0
        || object.len > MAX_OBJECT_BYTES
        || (base && (object.first_id != 0 || object.last_id != 0 || object.predecessor != [0; 32]))
        || (!base && (object.first_id == 0 || object.first_id > object.last_id))
    {
        return Err(MidgeError::Corruption(format!(
            "invalid FORMAT 10 object reference in generation {generation}"
        )));
    }
    Ok(())
}

fn verify_stream(
    fs: &dyn Fs,
    generation: &str,
    stream: Stream,
    selected: &StreamHead,
) -> MidgeResult<()> {
    verify_object(fs, generation, stream, &selected.base, true)?;
    for object in &selected.segments {
        verify_object(fs, generation, stream, object, false)?;
    }
    Ok(())
}

fn preflight_replacement(retained: &StreamHead, replacement_base_len: usize) -> MidgeResult<()> {
    let object_count = 2_usize
        .checked_add(retained.segments.len())
        .ok_or_else(|| {
            MidgeError::ResourceLimit("FORMAT 10 closure object count overflow".into())
        })?;
    let bytes = u64::try_from(replacement_base_len)
        .unwrap_or(u64::MAX)
        .checked_add(retained.base.len)
        .and_then(|total| {
            retained
                .segments
                .iter()
                .try_fold(total, |total, object| total.checked_add(object.len))
        })
        .ok_or_else(|| MidgeError::ResourceLimit("FORMAT 10 closure size overflow".into()))?;
    if object_count > MAX_OBJECTS || bytes > MAX_CLOSURE_BYTES {
        return Err(MidgeError::ResourceLimit(
            "FORMAT 10 replacement closure exceeds its object or byte limit".into(),
        ));
    }
    Ok(())
}

fn copy_stream_to_generation(
    fs: &dyn Fs,
    old_generation: &str,
    new_generation: &str,
    stream: Stream,
    selected: &StreamHead,
) -> MidgeResult<StreamHead> {
    let mut copied = StreamHead {
        base: copy_object_to_generation(
            fs,
            old_generation,
            new_generation,
            stream,
            &selected.base,
            true,
        )?,
        segments: Vec::with_capacity(selected.segments.len()),
    };
    for object in &selected.segments {
        copied.segments.push(copy_object_to_generation(
            fs,
            old_generation,
            new_generation,
            stream,
            object,
            false,
        )?);
    }
    Ok(copied)
}

fn copy_object_to_generation(
    fs: &dyn Fs,
    old_generation: &str,
    new_generation: &str,
    stream: Stream,
    object: &ObjectRef,
    base: bool,
) -> MidgeResult<ObjectRef> {
    verify_object(fs, old_generation, stream, object, base)?;
    let old_path = object_path(old_generation, &object.name);
    let bytes = read_bounded(fs, &old_path, MAX_OBJECT_BYTES, "selected FORMAT 10 object")
        .map_err(|error| {
            recovery_error("read selected FORMAT 10 object for generation copy", error)
        })?;
    let new_path = object_path(new_generation, &object.name);
    write_immutable(fs, &new_path, &bytes)?;
    sync_directory_chain(fs, &parent_path(&new_path))?;
    verify_object(fs, new_generation, stream, object, base)?;
    Ok(object.clone())
}

fn verify_head_objects(fs: &dyn Fs, head: &HeadBody) -> MidgeResult<()> {
    validate_head(head)?;
    verify_stream(fs, &head.generation, Stream::Manifest, &head.manifest)?;
    verify_stream(fs, &head.generation, Stream::Intent, &head.intent)
}

fn verify_object(
    fs: &dyn Fs,
    generation: &str,
    stream: Stream,
    object: &ObjectRef,
    base: bool,
) -> MidgeResult<()> {
    validate_ref(generation, stream, object, base)?;
    let path = object_path(generation, &object.name);
    let bytes = read_bounded(fs, &path, MAX_OBJECT_BYTES, "selected FORMAT 10 object")
        .map_err(|error| recovery_error("read selected FORMAT 10 object", error))?;
    verify_object_bytes(stream, object, &bytes)
}

fn verify_object_bytes(stream: Stream, object: &ObjectRef, bytes: &[u8]) -> MidgeResult<()> {
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != object.len
        || crc32c::crc32c(bytes) != object.crc32c
        || <[u8; 32]>::from(sha2::Sha256::digest(bytes)) != object.sha256
    {
        return Err(MidgeError::Corruption(
            "selected FORMAT 10 object does not match HEAD".into(),
        ));
    }
    if bytes.len() < 66
        || &bytes[..8] != OBJECT_MAGIC
        || bytes[8]
            != match stream {
                Stream::Manifest => 1,
                Stream::Intent => 2,
            }
        || bytes[9] != 0
    {
        return Err(MidgeError::Corruption(
            "selected FORMAT 10 object framing is invalid".into(),
        ));
    }
    let payload_len = u32::from_be_bytes(
        bytes[58..62]
            .try_into()
            .map_err(|_| MidgeError::Corruption("invalid FORMAT 10 object length".into()))?,
    ) as usize;
    let expected = 62_usize
        .checked_add(payload_len)
        .and_then(|value| value.checked_add(4))
        .ok_or_else(|| MidgeError::Corruption("FORMAT 10 object length overflow".into()))?;
    let first = u64::from_be_bytes(
        bytes[10..18]
            .try_into()
            .map_err(|_| MidgeError::Corruption("invalid FORMAT 10 object identity".into()))?,
    );
    let last = u64::from_be_bytes(
        bytes[18..26]
            .try_into()
            .map_err(|_| MidgeError::Corruption("invalid FORMAT 10 object identity".into()))?,
    );
    let predecessor: [u8; 32] = bytes[26..58]
        .try_into()
        .map_err(|_| MidgeError::Corruption("invalid FORMAT 10 predecessor".into()))?;
    if expected != bytes.len()
        || first != object.first_id
        || last != object.last_id
        || predecessor != object.predecessor
        || crc32c::crc32c(&bytes[..bytes.len() - 4])
            != u32::from_be_bytes(
                bytes[bytes.len() - 4..].try_into().map_err(|_| {
                    MidgeError::Corruption("invalid FORMAT 10 object checksum".into())
                })?,
            )
    {
        return Err(MidgeError::Corruption(
            "selected FORMAT 10 object frame is corrupt".into(),
        ));
    }
    Ok(())
}

fn read_object_payload(
    fs: &dyn Fs,
    generation: &str,
    stream: Stream,
    object: &ObjectRef,
    base: bool,
) -> MidgeResult<Vec<u8>> {
    verify_object(fs, generation, stream, object, base)?;
    let path = object_path(generation, &object.name);
    let bytes = read_bounded(fs, &path, MAX_OBJECT_BYTES, "selected FORMAT 10 object")
        .map_err(|error| recovery_error("read selected FORMAT 10 object", error))?;
    let payload_len =
        usize::try_from(u32::from_be_bytes(bytes[58..62].try_into().map_err(
            |_| MidgeError::Corruption("invalid FORMAT 10 object payload length".into()),
        )?))
        .unwrap_or(usize::MAX);
    let end = 62_usize
        .checked_add(payload_len)
        .ok_or_else(|| MidgeError::Corruption("FORMAT 10 object payload length overflow".into()))?;
    Ok(bytes[62..end].to_vec())
}

fn publish_head(fs: &Arc<dyn Fs>, head: &HeadBody) -> MidgeResult<()> {
    let bytes = encode_head(head)?;
    staging::stage_bytes_typed(
        fs,
        &FsPath::new(HEAD_TEMP),
        &FsPath::new(HEAD_PATH),
        &bytes,
        || Ok(()),
    )?;
    let selected = read_bounded(
        fs.as_ref(),
        HEAD_PATH,
        MAX_HEAD_BYTES as u64,
        "FORMAT 10 head",
    )
    .map_err(|error| recovery_error("read back FORMAT 10 HEAD", error))?;
    if selected.as_ref() != bytes.as_slice() {
        return Err(MidgeError::Corruption(
            "FORMAT 10 HEAD readback differs from published candidate".into(),
        ));
    }
    Ok(())
}

fn recovery_error(context: &str, error: FsError) -> MidgeError {
    match error {
        FsError::Timeout(message) => MidgeError::Timeout(message),
        other => MidgeError::RecoveryFailed(format!("{context}: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::traits::{DirEntry, Metadata};
    use crate::io::{Durability, File, FsPath, FsResult, OpenOptions, ReadFs, RealFs};
    use std::sync::atomic::{AtomicBool, Ordering};

    struct FailHeadDirectorySyncFs {
        inner: RealFs,
        armed: AtomicBool,
        head_replaced: AtomicBool,
    }

    impl ReadFs for FailHeadDirectorySyncFs {
        fn open_read(&self, path: &FsPath) -> FsResult<Box<dyn crate::io::ReadFile + '_>> {
            self.inner.open_read(path)
        }

        fn exists(&self, path: &FsPath) -> FsResult<bool> {
            self.inner.exists(path)
        }

        fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
            self.inner.metadata(path)
        }

        fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
            self.inner.list_dir(path)
        }
    }

    impl Fs for FailHeadDirectorySyncFs {
        fn open(&self, path: &FsPath, options: OpenOptions) -> FsResult<Box<dyn File + '_>> {
            self.inner.open(path, options)
        }

        fn remove_file(&self, path: &FsPath) -> FsResult<()> {
            self.inner.remove_file(path)
        }

        fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
            self.inner.create_dir_all(path)
        }

        fn remove_dir_all(&self, path: &FsPath) -> FsResult<()> {
            self.inner.remove_dir_all(path)
        }

        fn sync_dir(&self, path: &FsPath, durability: Durability) -> FsResult<()> {
            if self.head_replaced.load(Ordering::SeqCst) && path.0 == "metadata" {
                return Err(crate::io::FsError::Io(
                    "injected HEAD directory sync failure".into(),
                ));
            }
            self.inner.sync_dir(path, durability)
        }

        fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
            self.inner.rename_atomic(from, to)?;
            if self.armed.load(Ordering::SeqCst) && to.0 == HEAD_PATH {
                self.head_replaced.store(true, Ordering::SeqCst);
            }
            Ok(())
        }
    }

    fn real_fs(path: &std::path::Path) -> Arc<dyn Fs> {
        Arc::new(RealFs::new(path).expect("real filesystem"))
    }

    #[test]
    fn should_fence_writer_when_replaced_head_cannot_be_resynced() {
        // Arrange
        #[cfg(feature = "failpoints")]
        let _test_guard = crate::failpoints::test_failpoint_guard();
        let directory = tempfile::tempdir().expect("temporary database");
        let failing_fs = Arc::new(FailHeadDirectorySyncFs {
            inner: RealFs::new(directory.path()).expect("real filesystem"),
            armed: AtomicBool::new(false),
            head_replaced: AtomicBool::new(false),
        });
        let fs: Arc<dyn Fs> = failing_fs.clone();
        let mut authority = LocalMetadataAuthority::initialize(
            Arc::clone(&fs),
            "generation-outcome",
            b"manifest checkpoint",
            b"intent checkpoint",
        )
        .expect("initialize authority");
        failing_fs.armed.store(true, Ordering::SeqCst);

        // Act
        let first = authority.append(Stream::Manifest, 1, 1, b"uncertain selector");
        let second = authority.append(Stream::Manifest, 1, 1, b"must not proceed");
        let selected_head = LocalMetadataAuthority::open(fs).expect("read visible selector");

        // Assert
        assert!(first.is_err(), "directory resync failure must be reported");
        assert!(
            matches!(second, Err(MidgeError::RecoveryFailed(_))),
            "an unresolved selector outcome must fence later writes"
        );
        assert_eq!(selected_head.head.sequence, 2);
        assert_eq!(
            selected_head.selected_ids(Stream::Manifest),
            Vec::<(u64, u64)>::new()
        );
        assert_eq!(selected_head.head.pending_objects.len(), 1);
    }

    #[test]
    fn should_reopen_only_head_selected_local_metadata_segments() {
        // Arrange
        #[cfg(feature = "failpoints")]
        let _test_guard = crate::failpoints::test_failpoint_guard();
        let directory = tempfile::tempdir().expect("temporary database");
        let fs = real_fs(directory.path());
        let mut authority = LocalMetadataAuthority::initialize(
            Arc::clone(&fs),
            "generation-a",
            b"manifest checkpoint",
            b"intent checkpoint",
        )
        .expect("initialize authority");

        // Act
        authority
            .append(Stream::Manifest, 1, 1, b"manifest edit 1")
            .expect("append manifest");
        authority
            .append(Stream::Intent, 1, 1, b"intent edit 1")
            .expect("append intent");
        let reopened = LocalMetadataAuthority::open(fs).expect("reopen selected closure");

        // Assert
        assert_eq!(reopened.selected_ids(Stream::Manifest), vec![(1, 1)]);
        assert_eq!(reopened.selected_ids(Stream::Intent), vec![(1, 1)]);
        assert_eq!(reopened.head.sequence, 5);
    }

    #[test]
    fn should_reject_selected_object_corruption_without_falling_back_to_prior_head() {
        // Arrange
        #[cfg(feature = "failpoints")]
        let _test_guard = crate::failpoints::test_failpoint_guard();
        let directory = tempfile::tempdir().expect("temporary database");
        let fs = real_fs(directory.path());
        let mut authority = LocalMetadataAuthority::initialize(
            Arc::clone(&fs),
            "generation-b",
            b"manifest checkpoint",
            b"intent checkpoint",
        )
        .expect("initialize authority");
        authority
            .append(Stream::Manifest, 1, 1, b"durable edit")
            .expect("append manifest");
        let selected = authority.head.manifest.segments[0].clone();
        let selected_path = directory
            .path()
            .join(object_path(&authority.head.generation, &selected.name));
        let mut bytes = std::fs::read(&selected_path).expect("read selected segment");
        bytes[62] ^= 1;
        std::fs::write(&selected_path, bytes).expect("corrupt selected segment");

        // Act
        let result = LocalMetadataAuthority::open(fs);

        // Assert
        assert!(
            matches!(result, Err(MidgeError::Corruption(_))),
            "selected corruption must fail closed"
        );
    }

    #[test]
    fn should_reject_zero_extended_selected_history_damage_at_every_wire_region() {
        // Arrange
        #[cfg(feature = "failpoints")]
        let _test_guard = crate::failpoints::test_failpoint_guard();
        let regions = ["header", "body", "crc"];
        for selected_position in [0, 2] {
            for region in regions {
                let (directory, fs) = v10_database();
                let store = crate::metadata::store::ManifestStore::new_with_format(
                    Arc::clone(&fs),
                    crate::metadata::accounting::Owner::new(),
                    crate::metadata::accounting::Medium::Persistent,
                    crate::metadata::format::CURRENT_FORMAT_VERSION + 1,
                );
                for id in 2..=4 {
                    store
                        .append(&crate::metadata::ManifestEdit::CreateColumnFamily {
                            id,
                            name: format!("family-{id}"),
                            created_at: u64::from(id),
                        })
                        .expect("append selected history");
                }
                let authority =
                    LocalMetadataAuthority::open(Arc::clone(&fs)).expect("open selected history");
                let selected = authority.head.manifest.segments[selected_position].clone();
                let selected_path = directory
                    .path()
                    .join(object_path(&authority.head.generation, &selected.name));
                let original = std::fs::read(&selected_path).expect("read selected segment");
                assert_eq!(u64::try_from(original.len()).unwrap(), selected.len);
                let cut = match region {
                    "header" => 12,
                    "body" => 62 + (original.len() - 66) / 2,
                    "crc" => original.len() - 2,
                    _ => unreachable!("known wire region"),
                };
                let mut damaged = original[..cut].to_vec();
                damaged.resize(original.len(), 0);
                std::fs::write(&selected_path, damaged).expect("write zero-extended damage");
                assert_eq!(
                    std::fs::metadata(&selected_path)
                        .expect("stat damaged object")
                        .len(),
                    selected.len,
                    "damage retains the advertised object extent"
                );

                // Act
                let loaded = crate::metadata::ManifestPersistence::load_with_fs_and_policy_typed(
                    &fs,
                    crate::config::RecoveryPolicy::Strict,
                );

                // Assert
                assert!(
                    matches!(loaded, Err(MidgeError::Corruption(_))),
                    "selected {region} damage at history position {selected_position} must fail closed"
                );
            }
        }
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_keep_previous_head_selected_when_append_stops_after_object_sync() {
        // Arrange
        let _test_guard = crate::failpoints::test_failpoint_guard();
        let scenario = fail::FailScenario::setup();
        let directory = tempfile::tempdir().expect("temporary database");
        let fs = real_fs(directory.path());
        let mut authority = LocalMetadataAuthority::initialize(
            Arc::clone(&fs),
            "generation-c",
            b"manifest checkpoint",
            b"intent checkpoint",
        )
        .expect("initialize authority");
        fail::cfg("midge::format10::after_object_sync", "return").expect("configure failpoint");

        // Act
        let append = authority.append(Stream::Manifest, 1, 1, b"not selected");
        fail::remove("midge::format10::after_object_sync");
        scenario.teardown();
        let reopened = LocalMetadataAuthority::open(fs).expect("reopen previous head");

        // Assert
        assert!(append.is_err());
        assert_eq!(
            reopened.selected_ids(Stream::Manifest),
            [] as [(u64, u64); 0]
        );
        assert_eq!(reopened.head.sequence, 2);
        assert_eq!(reopened.head.pending_objects.len(), 1);
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_allow_new_append_after_unselected_immutable_is_cleaned() {
        // Arrange
        let _test_guard = crate::failpoints::test_failpoint_guard();
        let scenario = fail::FailScenario::setup();
        let (_directory, fs) = v10_database();
        let store = crate::metadata::store::ManifestStore::new_with_format(
            Arc::clone(&fs),
            crate::metadata::accounting::Owner::new(),
            crate::metadata::accounting::Medium::Persistent,
            crate::metadata::format::CURRENT_FORMAT_VERSION + 1,
        );
        store
            .append(&crate::metadata::ManifestEdit::CreateColumnFamily {
                id: 2,
                name: "selected-before-cut".into(),
                created_at: 1,
            })
            .expect("append initial selected edit");
        fail::cfg("midge::format10::after_object_sync", "return").expect("configure failpoint");

        // Act
        let interrupted = store.append(&crate::metadata::ManifestEdit::CreateColumnFamily {
            id: 3,
            name: "unselected-after-cut".into(),
            created_at: 2,
        });
        fail::remove("midge::format10::after_object_sync");
        scenario.teardown();
        let reopened_before_retry =
            crate::metadata::ManifestPersistence::load_with_fs_and_policy_typed(
                &fs,
                crate::config::RecoveryPolicy::Strict,
            )
            .expect("read selected history after interruption");
        let retry = store.append(&crate::metadata::ManifestEdit::CreateColumnFamily {
            id: 4,
            name: "selected-after-retry".into(),
            created_at: 3,
        });
        let reopened_after_retry =
            crate::metadata::ManifestPersistence::load_with_fs_and_policy_typed(
                &fs,
                crate::config::RecoveryPolicy::Strict,
            )
            .expect("read selected history after retry");

        // Assert
        assert!(interrupted.is_err());
        assert!(
            retry.is_ok(),
            "a proven unselected object must not block a later append"
        );
        assert_eq!(reopened_before_retry.edit_checkpoint_id, 1);
        assert!(reopened_before_retry
            .column_families
            .iter()
            .any(|family| family.id == 2));
        assert!(!reopened_before_retry
            .column_families
            .iter()
            .any(|family| family.id == 3));
        assert_eq!(reopened_after_retry.edit_checkpoint_id, 2);
        assert!(reopened_after_retry
            .column_families
            .iter()
            .any(|family| family.id == 2));
        assert!(reopened_after_retry
            .column_families
            .iter()
            .any(|family| family.id == 4));
        assert!(!reopened_after_retry
            .column_families
            .iter()
            .any(|family| family.id == 3));
    }

    #[test]
    fn should_reject_head_that_exceeds_encoded_limit() {
        // Arrange
        #[cfg(feature = "failpoints")]
        let _test_guard = crate::failpoints::test_failpoint_guard();
        let directory = tempfile::tempdir().expect("temporary database");
        let fs = real_fs(directory.path());
        let authority = LocalMetadataAuthority::initialize(
            Arc::clone(&fs),
            "generation-d",
            b"manifest checkpoint",
            b"intent checkpoint",
        )
        .expect("initialize authority");
        let mut head = authority.head.clone();
        head.generation = "x".repeat(70_000);

        // Act
        let encoded = encode_head(&head);

        // Assert
        assert!(matches!(encoded, Err(MidgeError::ResourceLimit(_))));
    }

    fn v10_database() -> (tempfile::TempDir, Arc<dyn Fs>) {
        let directory = tempfile::tempdir().expect("temporary database");
        let fs = real_fs(directory.path());
        let manifest = crate::metadata::codec::encode(
            crate::metadata::codec::MANIFEST,
            &crate::metadata::Manifest::default(),
        )
        .expect("encode empty manifest base");
        let intents = serde_json::to_vec(&Vec::<crate::runtime::IntentLogEntry>::new())
            .expect("encode empty intent base");
        LocalMetadataAuthority::initialize(Arc::clone(&fs), "generation-e", &manifest, &intents)
            .expect("initialize FORMAT 10 authority");
        std::fs::write(
            directory.path().join(crate::metadata::files::FORMAT),
            b"midge-format-version=10\n",
        )
        .expect("write FORMAT 10 marker");
        (directory, fs)
    }

    #[test]
    fn should_rebuild_digest_bound_candidate_after_interrupted_source_change() {
        // Arrange
        #[cfg(feature = "failpoints")]
        let _test_guard = crate::failpoints::test_failpoint_guard();
        let directory = tempfile::tempdir().expect("temporary database");
        let fs = real_fs(directory.path());
        crate::metadata::ensure_or_create_format_marker(&fs, directory.path())
            .expect("create FORMAT 9 marker");
        std::fs::write(
            directory.path().join("manifest.journal"),
            b"preserved v9 source",
        )
        .expect("write legacy source bytes");
        let manifest = crate::metadata::codec::encode(
            crate::metadata::codec::MANIFEST,
            &crate::metadata::Manifest::default(),
        )
        .expect("encode manifest base");
        let intents = serde_json::to_vec(&Vec::<crate::runtime::IntentLogEntry>::new())
            .expect("encode intent base");

        // Act
        LocalMetadataAuthority::stage_upgrade_candidate(
            Arc::clone(&fs),
            [1; 32],
            &manifest,
            0,
            &intents,
            0,
            0,
        )
        .expect("stage first candidate");
        let candidate = LocalMetadataAuthority::stage_upgrade_candidate(
            Arc::clone(&fs),
            [2; 32],
            &manifest,
            0,
            &intents,
            0,
            0,
        )
        .expect("rebuild candidate for revalidated source");

        // Assert
        assert_eq!(candidate.head.source_digest, Some([2; 32]));
        assert_eq!(candidate.head.generation, "upgrade-candidate");
        assert_eq!(
            std::fs::read(directory.path().join(crate::metadata::files::FORMAT))
                .expect("read marker"),
            b"midge-format-version=9\n"
        );
        assert_eq!(
            std::fs::read(directory.path().join("manifest.journal")).expect("read v9 source"),
            b"preserved v9 source"
        );
    }

    #[test]
    fn should_route_metadata_writes_through_selected_head() {
        // Arrange
        #[cfg(feature = "failpoints")]
        let _test_guard = crate::failpoints::test_failpoint_guard();
        let (directory, fs) = v10_database();
        let store = crate::metadata::store::ManifestStore::new_with_format(
            Arc::clone(&fs),
            crate::metadata::accounting::Owner::new(),
            crate::metadata::accounting::Medium::Persistent,
            crate::metadata::format::CURRENT_FORMAT_VERSION + 1,
        );
        let edit = crate::metadata::ManifestEdit::CreateColumnFamily {
            id: 7,
            name: "seven".into(),
            created_at: 1,
        };

        // Act
        store.append(&edit).expect("append manifest edit");
        let selected = LocalMetadataAuthority::open(Arc::clone(&fs)).expect("open appended head");
        assert_eq!(selected.high_water(Stream::Manifest), 1);
        let intents = vec![crate::runtime::IntentLogEntry::SeqnoAllocated { seqno: 9, cf_id: 7 }];
        crate::runtime::IntentPersistence::save_with_fs(&fs, &intents)
            .expect("append intent state");
        let manifest = crate::metadata::ManifestPersistence::load_with_fs_and_policy_typed(
            &fs,
            crate::config::RecoveryPolicy::Strict,
        )
        .expect("reload manifest");
        let recovered_intents = crate::runtime::IntentPersistence::load_with_fs_and_policy_typed(
            &fs,
            crate::config::RecoveryPolicy::Strict,
        )
        .expect("reload intent state");

        // Assert
        assert_eq!(manifest.edit_checkpoint_id, 1);
        assert_eq!(recovered_intents.len(), 1);
        assert!(matches!(
            recovered_intents[0],
            crate::runtime::IntentLogEntry::SeqnoAllocated { seqno: 9, cf_id: 7 }
        ));
        assert!(!directory
            .path()
            .join(crate::metadata::files::JOURNAL)
            .exists());
        assert!(!directory
            .path()
            .join(crate::metadata::files::INTENT_LOG)
            .exists());
    }

    #[test]
    fn should_reject_state_invalid_manifest_batch_before_format_10_publication() {
        // Arrange
        #[cfg(feature = "failpoints")]
        let _test_guard = crate::failpoints::test_failpoint_guard();
        let (_directory, fs) = v10_database();
        let store = crate::metadata::store::ManifestStore::new_with_format(
            Arc::clone(&fs),
            crate::metadata::accounting::Owner::new(),
            crate::metadata::accounting::Medium::Persistent,
            crate::metadata::format::CURRENT_FORMAT_VERSION + 1,
        );
        let invalid = [
            crate::metadata::ManifestEdit::CreateColumnFamily {
                id: 2,
                name: "must-not-commit".into(),
                created_at: 1,
            },
            crate::metadata::ManifestEdit::AdvanceFlushFrontier {
                cf_id: 999,
                freeze_sequence: 1,
            },
        ];

        // Act
        let rejected = store.append_batch(&invalid);
        let after_rejection =
            LocalMetadataAuthority::open(Arc::clone(&fs)).expect("open after rejected batch");
        let persisted = crate::metadata::ManifestPersistence::load_with_fs_and_policy_typed(
            &fs,
            crate::config::RecoveryPolicy::Strict,
        )
        .expect("persisted source remains readable");
        // Assert
        assert!(
            rejected.is_err(),
            "state-invalid batch was acknowledged: {rejected:?}"
        );
        assert_eq!(after_rejection.high_water(Stream::Manifest), 0);
        assert_eq!(persisted.edit_checkpoint_id, 0);
        assert!(!persisted
            .column_families
            .iter()
            .any(|family| family.name == "must-not-commit"));
    }

    #[test]
    fn should_apply_manifest_batch_as_one_format_10_edit() {
        // Arrange
        #[cfg(feature = "failpoints")]
        let _test_guard = crate::failpoints::test_failpoint_guard();
        let (_directory, fs) = v10_database();
        let store = crate::metadata::store::ManifestStore::new_with_format(
            Arc::clone(&fs),
            crate::metadata::accounting::Owner::new(),
            crate::metadata::accounting::Medium::Persistent,
            crate::metadata::format::CURRENT_FORMAT_VERSION + 1,
        );

        // Act
        let accepted = store
            .append_batch(&[
                crate::metadata::ManifestEdit::CreateColumnFamily {
                    id: 2,
                    name: "batch-a".into(),
                    created_at: 2,
                },
                crate::metadata::ManifestEdit::CreateColumnFamily {
                    id: 3,
                    name: "batch-b".into(),
                    created_at: 3,
                },
            ])
            .expect("append valid batch");
        let authority = LocalMetadataAuthority::open(Arc::clone(&fs)).expect("open selected batch");
        let reopened = crate::metadata::ManifestPersistence::load_with_fs_and_policy_typed(
            &fs,
            crate::config::RecoveryPolicy::Strict,
        )
        .expect("load committed batch");

        // Assert
        assert_eq!(accepted, 1, "a batch consumes one edit ID");
        assert_eq!(authority.high_water(Stream::Manifest), 1);
        assert_eq!(reopened.edit_checkpoint_id, 1);
        assert!(reopened.column_families.iter().any(|family| family.id == 2));
        assert!(reopened.column_families.iter().any(|family| family.id == 3));
    }

    #[test]
    fn should_checkpoint_manifest_to_a_new_generation_before_closure_limit() {
        // Arrange
        #[cfg(feature = "failpoints")]
        let _test_guard = crate::failpoints::test_failpoint_guard();
        let (directory, fs) = v10_database();
        let original = LocalMetadataAuthority::open(Arc::clone(&fs)).expect("open initial head");
        let original_generation = original.head.generation.clone();
        let store = crate::metadata::store::ManifestStore::new_with_format(
            Arc::clone(&fs),
            crate::metadata::accounting::Owner::new(),
            crate::metadata::accounting::Medium::Persistent,
            crate::metadata::format::CURRENT_FORMAT_VERSION + 1,
        );

        // Act
        for id in 1..=15 {
            store
                .append(&crate::metadata::ManifestEdit::CreateColumnFamily {
                    id: id + 1,
                    name: format!("family-{id}"),
                    created_at: u64::from(id),
                })
                .expect("append or checkpoint manifest edit");
        }
        let reopened = LocalMetadataAuthority::open(Arc::clone(&fs)).expect("open rolled head");
        let manifest = crate::metadata::ManifestPersistence::load_with_fs_and_policy_typed(
            &fs,
            crate::config::RecoveryPolicy::Strict,
        )
        .expect("load rolled manifest");

        // Assert
        assert_ne!(reopened.head.generation, original_generation);
        assert!(reopened.head.sequence >= 16);
        assert!(reopened.head.manifest.segments.len() < MAX_OBJECTS - 2);
        assert_eq!(reopened.high_water(Stream::Manifest), 15);
        assert_eq!(manifest.edit_checkpoint_id, 15);
        assert!(directory.path().join("metadata/HEAD").is_file());
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_leave_manifest_edit_unselected_when_immutable_publication_is_interrupted() {
        // Arrange
        let _test_guard = crate::failpoints::test_failpoint_guard();
        let scenario = fail::FailScenario::setup();
        let (_directory, fs) = v10_database();
        let store = crate::metadata::store::ManifestStore::new_with_format(
            Arc::clone(&fs),
            crate::metadata::accounting::Owner::new(),
            crate::metadata::accounting::Medium::Persistent,
            crate::metadata::format::CURRENT_FORMAT_VERSION + 1,
        );
        fail::cfg("midge::format10::after_object_sync", "return").expect("configure failpoint");

        // Act
        let append = store.append(&crate::metadata::ManifestEdit::CreateColumnFamily {
            id: 8,
            name: "eight".into(),
            created_at: 2,
        });
        fail::remove("midge::format10::after_object_sync");
        scenario.teardown();
        let manifest = crate::metadata::ManifestPersistence::load_with_fs_and_policy_typed(
            &fs,
            crate::config::RecoveryPolicy::Strict,
        )
        .expect("reload previous manifest head");

        // Assert
        assert!(append.is_err());
        assert_eq!(manifest.edit_checkpoint_id, 0);
        assert!(!manifest.column_families.iter().any(|family| family.id == 8));
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_keep_old_generation_selected_when_checkpoint_is_interrupted() {
        // Arrange
        let _test_guard = crate::failpoints::test_failpoint_guard();
        let scenario = fail::FailScenario::setup();
        let (_directory, fs) = v10_database();
        let mut authority = LocalMetadataAuthority::open(Arc::clone(&fs)).expect("open head");
        let original = authority.head.clone();
        let base = crate::metadata::codec::encode(
            crate::metadata::codec::MANIFEST,
            &crate::metadata::Manifest::default(),
        )
        .expect("encode checkpoint base");
        fail::cfg("midge::format10::after_checkpoint_objects_sync", "return")
            .expect("configure failpoint");

        // Act
        let result = authority.checkpoint_manifest(&base, 0);
        fail::remove("midge::format10::after_checkpoint_objects_sync");
        scenario.teardown();
        let reopened = LocalMetadataAuthority::open(fs).expect("reopen old head");

        // Assert
        assert!(result.is_err());
        assert_eq!(reopened.head, original);
    }
}
